//! Settings → Team: the org-shared-workspace management surface.
//!
//! - Workspaces: every membership listed; switching (or creating/deleting)
//!   emits an intent to the shell. The shell owns the destructive preflight,
//!   auth RPC and runtime handoff so Settings cannot bypass the same warning
//!   used by the avatar-menu fast switch.
//! - Members: the team roster with invite-by-email, role changes, and
//!   removal. Admin-ness is enforced edge-side (live WorkOS membership); the
//!   UI only hides what would be rejected anyway.

use gpui::{
    AnyElement, Context, Entity, EventEmitter, SharedString, Subscription, Task, Window, div,
    prelude::*, px,
};
use gpui_tokio::Tokio;

use zeron_proto::WorkspaceScope;
use zeron_rpc::methods;

use crate::composer::{ComposerInput, ComposerInputEvent};
use crate::popover::{self, Loadable};
use crate::settings::widgets;
use crate::state::{AppState, OrgRow, org_name_valid, parse_orgs, sort_memberships};
use crate::theme::Theme;

const TEAM_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// One roster row (tolerant mirror of the engine's ListMembers reply).
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberRow {
    pub membership_id: String,
    pub user_id: String,
    pub email: String,
    #[serde(default)]
    pub name: Option<String>,
    pub role: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamMutationKind {
    Select,
    Create,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamMutation {
    Select {
        organization_id: String,
        label: SharedString,
    },
    Create {
        name: String,
    },
    Delete {
        organization_id: String,
        label: SharedString,
    },
}

impl TeamMutation {
    pub fn kind(&self) -> TeamMutationKind {
        match self {
            Self::Select { .. } => TeamMutationKind::Select,
            Self::Create { .. } => TeamMutationKind::Create,
            Self::Delete { .. } => TeamMutationKind::Delete,
        }
    }

    pub fn label(&self) -> SharedString {
        match self {
            Self::Select { label, .. } | Self::Delete { label, .. } => label.clone(),
            Self::Create { name } => name.clone().into(),
        }
    }

    pub fn requested_organization_id(&self) -> Option<&str> {
        match self {
            Self::Select {
                organization_id, ..
            } => Some(organization_id),
            Self::Create { .. } | Self::Delete { .. } => None,
        }
    }

    pub fn rpc(&self) -> (&'static str, serde_json::Value) {
        match self {
            Self::Select {
                organization_id, ..
            } => (
                methods::SELECT_ORG,
                serde_json::json!({ "organizationId": organization_id }),
            ),
            Self::Create { name } => (methods::CREATE_ORG, serde_json::json!({ "name": name })),
            Self::Delete {
                organization_id, ..
            } => (
                methods::DELETE_ORG,
                serde_json::json!({ "organizationId": organization_id }),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TeamPageEvent {
    /// Shell must run the shared destructive preflight before issuing this RPC.
    RequestRuntimeChange(TeamMutation),
}

impl EventEmitter<TeamPageEvent> for TeamPage {}

#[derive(Debug, Clone)]
enum SuccessAction {
    Refresh,
    Invite,
}

pub struct TeamPage {
    state: Entity<AppState>,
    orgs: Loadable<Vec<OrgRow>>,
    members: Loadable<Vec<MemberRow>>,
    invite: Entity<ComposerInput>,
    invite_admin: bool,
    new_org: Option<Entity<ComposerInput>>,
    confirm_delete: bool,
    confirm_remove: Option<MemberRow>,
    busy: bool,
    error: Option<SharedString>,
    info: Option<SharedString>,
    load_task: Option<Task<()>>,
    action_task: Option<Task<()>>,
    _subs: Vec<Subscription>,
}

impl TeamPage {
    pub fn new(state: Entity<AppState>, cx: &mut Context<Self>) -> Self {
        let invite = cx.new(|cx| ComposerInput::new("teammate@example.com", cx));
        let mut subs = vec![cx.observe(&state, |_: &mut Self, _, cx| cx.notify())];
        subs.push(cx.subscribe(&invite, |this: &mut Self, _, event, cx| {
            if matches!(event, ComposerInputEvent::Submitted) {
                this.submit_invite(cx);
            }
        }));
        let mut page = Self {
            state,
            orgs: Loadable::Idle,
            members: Loadable::Idle,
            invite,
            invite_admin: false,
            new_org: None,
            confirm_delete: false,
            confirm_remove: None,
            busy: false,
            error: None,
            info: None,
            load_task: None,
            action_task: None,
            _subs: subs,
        };
        page.refresh(cx);
        page
    }

    fn current_org_id(&self, cx: &Context<Self>) -> Option<String> {
        match self.state.read(cx).auth.as_ref()? {
            zeron_proto::AuthState::SignedIn { org_id, .. } => org_id.clone(),
            _ => None,
        }
    }

    fn my_role(&self, cx: &Context<Self>) -> String {
        let current = self.current_org_id(cx);
        self.orgs
            .ready()
            .into_iter()
            .flatten()
            .find(|o| Some(&o.organization_id) == current.as_ref())
            .map(|o| o.role.clone())
            .unwrap_or_else(|| "member".into())
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.busy || self.load_task.is_some() {
            return;
        }
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            self.orgs = Loadable::Error("Engine not connected".into());
            self.members = Loadable::Error("Engine not connected".into());
            cx.notify();
            return;
        };
        let org_id = self.current_org_id(cx);
        self.error = None;
        self.orgs = Loadable::Loading;
        self.members = if org_id.is_some() {
            Loadable::Loading
        } else {
            Loadable::Ready(Vec::new())
        };
        self.load_task = Some(cx.spawn(async move |this, cx| {
            let orgs = engine
                .client()
                .call(methods::LIST_ORGS, serde_json::json!({}))
                .await;
            let members = match &org_id {
                Some(org_id) => Some(
                    engine
                        .client()
                        .call(
                            methods::LIST_MEMBERS,
                            serde_json::json!({ "organizationId": org_id }),
                        )
                        .await,
                ),
                None => None,
            };
            this.update(cx, |page, cx| {
                page.load_task = None;
                page.orgs = match orgs {
                    Ok(value) => Loadable::Ready(sort_memberships(parse_orgs(&value))),
                    Err(err) => Loadable::Error(format!("Loading workspaces failed: {err}")),
                };
                match members {
                    Some(Ok(value)) => {
                        page.members = Loadable::Ready(
                            value
                                .get("members")
                                .and_then(|m| serde_json::from_value(m.clone()).ok())
                                .unwrap_or_default(),
                        );
                    }
                    Some(Err(err)) => {
                        page.members = Loadable::Error(format!("Loading members failed: {err}"));
                    }
                    None => page.members = Loadable::Ready(Vec::new()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    /// Fire one non-profile-changing auth RPC. Team profile mutations are
    /// emitted to the shell instead so they cannot bypass its preflight.
    fn run(
        &mut self,
        method: &'static str,
        params: serde_json::Value,
        success: SuccessAction,
        cx: &mut Context<Self>,
    ) {
        if self.busy || self.load_task.is_some() {
            return;
        }
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            self.error = Some("Engine not connected".into());
            cx.notify();
            return;
        };
        self.busy = true;
        self.error = None;
        self.info = None;
        let rpc = Tokio::spawn(cx, async move {
            match tokio::time::timeout(TEAM_RPC_TIMEOUT, engine.client().call(method, params)).await
            {
                Ok(result) => result,
                Err(_) => Err(zeron_rpc::RpcError::Transport(format!(
                    "Team request timed out after {} seconds",
                    TEAM_RPC_TIMEOUT.as_secs()
                ))),
            }
        });
        self.action_task = Some(cx.spawn(async move |this, cx| {
            let result = match rpc.await {
                Ok(result) => result,
                Err(error) => Err(zeron_rpc::RpcError::Transport(format!(
                    "Team request task failed: {error}"
                ))),
            };
            this.update(cx, |page, cx| {
                page.action_task = None;
                page.busy = false;
                match result {
                    Ok(value) => match success {
                        SuccessAction::Invite => {
                            if value.get("invited").and_then(|v| v.as_bool()) == Some(true) {
                                page.info = Some(
                                    "Invitation sent — they can join after signing up.".into(),
                                );
                            } else if value.get("added").and_then(|v| v.as_bool()) == Some(true) {
                                page.info = Some("Member added.".into());
                            }
                            page.invite.update(cx, |input, cx| input.set_text("", cx));
                            page.refresh(cx);
                        }
                        SuccessAction::Refresh => page.refresh(cx),
                    },
                    Err(err) => page.error = Some(format!("{err}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn submit_invite(&mut self, cx: &mut Context<Self>) {
        if self.busy || self.load_task.is_some() {
            return;
        }
        let email = self.invite.read(cx).text().trim().to_string();
        let Some(org_id) = self.current_org_id(cx) else {
            return;
        };
        if !email.contains('@') || email.len() < 5 {
            self.error = Some("Enter a valid email address.".into());
            cx.notify();
            return;
        }
        let role = if self.invite_admin { "admin" } else { "member" };
        self.info = None;
        self.run(
            methods::INVITE_MEMBER,
            serde_json::json!({ "organizationId": org_id, "email": email, "role": role }),
            SuccessAction::Invite,
            cx,
        );
    }

    fn submit_new_org(&mut self, cx: &mut Context<Self>) {
        if self.busy || self.load_task.is_some() {
            return;
        }
        let Some(input) = self.new_org.as_ref() else {
            return;
        };
        let name = input.read(cx).text().trim().to_string();
        if !org_name_valid(&name) {
            self.error = Some("Workspace names must be 1-64 characters.".into());
            cx.notify();
            return;
        }
        // CREATE_ORG also selects the new org. The shell must approve the
        // destructive handoff before this intent can become an RPC.
        self.error = None;
        self.info = None;
        cx.emit(TeamPageEvent::RequestRuntimeChange(TeamMutation::Create {
            name,
        }));
    }

    fn render_workspaces(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        let current = self.current_org_id(cx);
        let orgs = self.orgs.clone();
        let busy = self.busy || self.load_task.is_some();
        let mut card = widgets::section_card(theme);
        match orgs {
            Loadable::Idle | Loadable::Loading => {
                card = card.child(widgets::card_row(theme, true).child(div().flex_1().child(
                    popover::skeleton_rows("team-orgs-skeleton", theme, 3, cx.entity_id(), cx),
                )));
            }
            Loadable::Error(message) => {
                card = card.child(
                    widgets::card_row(theme, true).child(
                        popover::error_row(theme, &message).child(
                            widgets::ghost_action(theme)
                                .id("team-orgs-retry")
                                .hover(|s| widgets::ghost_hover(theme, s))
                                .child("Retry")
                                .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                        ),
                    ),
                );
            }
            Loadable::Ready(orgs) if orgs.is_empty() => {
                card = card.child(
                    widgets::card_row(theme, true).child(
                        div()
                            .text_size(px(12.5))
                            .text_color(theme.text_muted)
                            .child("You don't belong to any workspaces yet."),
                    ),
                );
            }
            Loadable::Ready(orgs) => {
                for (ix, org) in orgs.into_iter().enumerate() {
                    let is_current = Some(&org.organization_id) == current.as_ref();
                    let mut row = widgets::card_row(theme, ix == 0)
                        // Generated crest, seeded by the organization id so
                        // renaming a Team does not change its badge.
                        .child(crate::avatar::team_avatar(&org.organization_id, 32.0))
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .child(widgets::row_title(theme, org.name.clone()))
                                .child(widgets::meta_line(
                                    theme,
                                    vec![
                                        div()
                                            .child(SharedString::from(if org.role == "admin" {
                                                "Admin"
                                            } else {
                                                "Member"
                                            }))
                                            .into_any_element(),
                                    ],
                                )),
                        );
                    if is_current {
                        row = row.child(widgets::badge_active(theme, "Current"));
                    } else {
                        let org_id = org.organization_id.clone();
                        let org_name: SharedString = org.name.into();
                        row = row.child(
                            widgets::ghost_action(theme)
                                .id(SharedString::from(format!("switch-org-{ix}")))
                                .hover(|s| widgets::ghost_hover(theme, s))
                                .when(busy, |button| button.opacity(0.45))
                                .when(!busy, |button| {
                                    button.on_click(cx.listener(move |this, _, _, cx| {
                                        this.error = None;
                                        this.info = None;
                                        cx.emit(TeamPageEvent::RequestRuntimeChange(
                                            TeamMutation::Select {
                                                organization_id: org_id.clone(),
                                                label: org_name.clone(),
                                            },
                                        ));
                                    }))
                                })
                                .child("Switch"),
                        );
                    }
                    card = card.child(row);
                }
            }
        }
        // New-workspace affordance: inline input when armed, button otherwise.
        let first = self.orgs.ready().is_none_or(|orgs| orgs.is_empty());
        card = card.child(match &self.new_org {
            Some(input) => widgets::card_row(theme, first)
                .child(div().flex_1().child(input.clone()))
                .child(
                    widgets::ghost_action(theme)
                        .id("new-org-create")
                        .hover(|s| widgets::ghost_hover(theme, s))
                        .when(busy, |button| button.opacity(0.45))
                        .when(!busy, |button| {
                            button.on_click(cx.listener(|this, _, _, cx| this.submit_new_org(cx)))
                        })
                        .child("Create"),
                ),
            None => widgets::card_row(theme, first).child(
                widgets::ghost_action(theme)
                    .id("new-org")
                    .hover(|s| widgets::ghost_hover(theme, s))
                    .when(busy, |button| button.opacity(0.45))
                    .when(!busy, |button| {
                        button.on_click(cx.listener(|this, _, _, cx| {
                            let input = cx.new(|cx| ComposerInput::new("Workspace name", cx));
                            let sub = cx.subscribe(&input, |this: &mut Self, _, event, cx| {
                                if matches!(event, ComposerInputEvent::Submitted) {
                                    this.submit_new_org(cx);
                                }
                            });
                            this._subs.push(sub);
                            this.new_org = Some(input);
                            this.error = None;
                            cx.notify();
                        }))
                    })
                    .child("New workspace…"),
            ),
        });
        card.into_any_element()
    }

    fn render_members(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        let my_role = self.my_role(cx);
        let is_admin = my_role == "admin";
        let busy = self.busy || self.load_task.is_some();
        let me = self
            .state
            .read(cx)
            .auth_user()
            .map(|u| u.id.clone())
            .unwrap_or_default();
        let org_id = self.current_org_id(cx).unwrap_or_default();
        let members = self.members.clone();
        let mut card = widgets::section_card(theme);
        match members {
            Loadable::Idle | Loadable::Loading => {
                card = card.child(widgets::card_row(theme, true).child(div().flex_1().child(
                    popover::skeleton_rows("team-members-skeleton", theme, 4, cx.entity_id(), cx),
                )));
            }
            Loadable::Error(message) => {
                card = card.child(
                    widgets::card_row(theme, true).child(
                        popover::error_row(theme, &message).child(
                            widgets::ghost_action(theme)
                                .id("team-members-retry")
                                .hover(|s| widgets::ghost_hover(theme, s))
                                .child("Retry")
                                .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                        ),
                    ),
                );
            }
            Loadable::Ready(members) if members.is_empty() => {
                card = card.child(
                    widgets::card_row(theme, true).child(
                        div()
                            .text_size(px(12.5))
                            .text_color(theme.text_muted)
                            .child("No members found in this workspace."),
                    ),
                );
            }
            Loadable::Ready(members) => {
                for (ix, member) in members.into_iter().enumerate() {
                    let title = member.name.clone().unwrap_or_else(|| member.email.clone());
                    let mut row = widgets::card_row(theme, ix == 0).child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .child(widgets::row_title(theme, title))
                            .child(widgets::meta_line(
                                theme,
                                vec![
                                    div()
                                        .child(SharedString::from(member.email.clone()))
                                        .into_any_element(),
                                ],
                            )),
                    );
                    row = row.child(if member.role == "admin" {
                        widgets::badge_active(theme, "Admin")
                    } else {
                        widgets::badge(theme, "Member")
                    });
                    if member.user_id == me {
                        row = row.child(widgets::badge(theme, "You"));
                    } else if is_admin {
                        let flip_to = if member.role == "admin" {
                            "member"
                        } else {
                            "admin"
                        };
                        let flip_label = if member.role == "admin" {
                            "Make member"
                        } else {
                            "Make admin"
                        };
                        let mid = member.membership_id.clone();
                        let org_for_role = org_id.clone();
                        row = row.child(
                            widgets::ghost_action(theme)
                                .id(SharedString::from(format!("role-{ix}")))
                                .hover(|s| widgets::ghost_hover(theme, s))
                                .when(busy, |button| button.opacity(0.45))
                                .when(!busy, |button| {
                                    button.on_click(cx.listener(move |this, _, _, cx| {
                                        this.run(
                                            methods::SET_MEMBER_ROLE,
                                            serde_json::json!({
                                                "organizationId": org_for_role.clone(),
                                                "membershipId": mid.clone(),
                                                "role": flip_to,
                                            }),
                                            SuccessAction::Refresh,
                                            cx,
                                        );
                                    }))
                                })
                                .child(flip_label),
                        );
                        let member_for_remove = member.clone();
                        row = row.child(
                            widgets::ghost_action(theme)
                                .id(SharedString::from(format!("remove-{ix}")))
                                .text_color(theme.danger)
                                .hover(|s| {
                                    s.bg(theme.danger.opacity(0.09))
                                        .text_color(theme.danger_muted)
                                })
                                .when(busy, |button| button.opacity(0.45))
                                .when(!busy, |button| {
                                    button.on_click(cx.listener(move |this, _, _, cx| {
                                        this.confirm_remove = Some(member_for_remove.clone());
                                        this.error = None;
                                        cx.notify();
                                    }))
                                })
                                .child("Remove"),
                        );
                    }
                    card = card.child(row);
                }
            }
        }
        if is_admin {
            let role_label = if self.invite_admin {
                "as Admin"
            } else {
                "as Member"
            };
            let first = self
                .members
                .ready()
                .is_none_or(|members| members.is_empty());
            card = card.child(
                widgets::card_row(theme, first)
                    .child(div().flex_1().child(self.invite.clone()))
                    .child(
                        widgets::ghost_action(theme)
                            .id("invite-role")
                            .hover(|s| widgets::ghost_hover(theme, s))
                            .when(busy, |button| button.opacity(0.45))
                            .when(!busy, |button| {
                                button.on_click(cx.listener(|this, _, _, cx| {
                                    this.invite_admin = !this.invite_admin;
                                    this.error = None;
                                    cx.notify();
                                }))
                            })
                            .child(SharedString::from(role_label)),
                    )
                    .child(
                        widgets::ghost_action(theme)
                            .id("invite-send")
                            .hover(|s| widgets::ghost_hover(theme, s))
                            .when(busy, |button| button.opacity(0.45))
                            .when(!busy, |button| {
                                button
                                    .on_click(cx.listener(|this, _, _, cx| this.submit_invite(cx)))
                            })
                            .child("Invite"),
                    ),
            );
        }
        card.into_any_element()
    }

    fn render_danger(&mut self, theme: &Theme, cx: &mut Context<Self>) -> Option<AnyElement> {
        if self.my_role(cx) != "admin" {
            return None;
        }
        self.current_org_id(cx)?;
        let busy = self.busy || self.load_task.is_some();
        Some(
            widgets::section_card(theme)
                .child(
                    widgets::card_row(theme, true)
                        .child(
                            div()
                                .flex_1()
                                .child(widgets::row_title(theme, "Danger zone"))
                                .child(widgets::meta_line(
                                    theme,
                                    vec![div()
                                        .child(SharedString::from(
                                            "Deleting a workspace removes it for every member. Sessions stay in each device's local store.",
                                        ))
                                        .into_any_element()],
                                )),
                        )
                        .child(
                            widgets::ghost_action(theme)
                                .id("delete-org")
                                .text_color(theme.danger)
                                .hover(|s| {
                                    s.bg(theme.danger.opacity(0.09))
                                        .text_color(theme.danger_muted)
                                })
                                .when(busy, |button| button.opacity(0.45))
                                .when(!busy, |button| {
                                    button.on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_delete = true;
                                        this.error = None;
                                        cx.notify();
                                    }))
                                })
                                .child("Delete workspace…"),
                        ),
                )
                .into_any_element(),
        )
    }

    fn render_confirmation(
        &mut self,
        viewport: gpui::Size<gpui::Pixels>,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        if let Some(member) = self.confirm_remove.clone() {
            let org_id = self.current_org_id(cx)?;
            let membership_id = member.membership_id.clone();
            let display_name = member.name.unwrap_or_else(|| member.email.clone());
            let body = format!(
                "Remove {display_name} ({}) from this workspace? They won't be able to renew access. An already-open connection may remain active until it reconnects or its session expires.",
                member.email
            );
            let card = popover::dialog_card(theme)
                .child(popover::dialog_title(theme, "Remove member?"))
                .child(div().mt(px(6.0)).child(popover::dialog_body(theme, body)))
                .child(
                    div()
                        .mt(px(16.0))
                        .flex()
                        .flex_row()
                        .justify_end()
                        .gap(px(8.0))
                        .child(
                            popover::btn_ghost(theme, "Cancel", "team-remove-cancel")
                                .id("team-remove-cancel")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirm_remove = None;
                                    cx.notify();
                                })),
                        )
                        .child(
                            popover::btn_danger(theme, "Remove member")
                                .id("team-remove-confirm")
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.confirm_remove = None;
                                    this.run(
                                        methods::REMOVE_MEMBER,
                                        serde_json::json!({
                                            "organizationId": org_id.clone(),
                                            "membershipId": membership_id.clone(),
                                        }),
                                        SuccessAction::Refresh,
                                        cx,
                                    );
                                })),
                        ),
                )
                .into_any_element();
            return Some(popover::modal("team-remove-member-dialog", viewport, card));
        }

        if self.confirm_delete {
            let org_id = self.current_org_id(cx)?;
            let org_name = self
                .orgs
                .ready()
                .and_then(|orgs| orgs.iter().find(|org| org.organization_id == org_id))
                .map(|org| org.name.clone())
                .unwrap_or_else(|| "this workspace".into());
            let body = format!(
                "Delete {org_name} for every member? Memberships cannot renew access, though already-open connections may remain active until they reconnect or expire. This cannot be undone."
            );
            let card = popover::dialog_card(theme)
                .child(popover::dialog_title(theme, "Delete workspace?"))
                .child(div().mt(px(6.0)).child(popover::dialog_body(theme, body)))
                .child(
                    div()
                        .mt(px(16.0))
                        .flex()
                        .flex_row()
                        .justify_end()
                        .gap(px(8.0))
                        .child(
                            popover::btn_ghost(theme, "Cancel", "team-delete-cancel")
                                .id("team-delete-cancel")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirm_delete = false;
                                    cx.notify();
                                })),
                        )
                        .child(
                            popover::btn_danger(theme, "Delete workspace")
                                .id("team-delete-confirm")
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.confirm_delete = false;
                                    this.error = None;
                                    this.info = None;
                                    cx.emit(TeamPageEvent::RequestRuntimeChange(
                                        TeamMutation::Delete {
                                            organization_id: org_id.clone(),
                                            label: org_name.clone().into(),
                                        },
                                    ));
                                })),
                        ),
                )
                .into_any_element();
            return Some(popover::modal(
                "team-delete-workspace-dialog",
                viewport,
                card,
            ));
        }

        None
    }
}

impl Render for TeamPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = Theme::of(cx).clone();
        let scope = self.state.read(cx).workspace_scope;
        let signed_in = matches!(
            self.state.read(cx).auth,
            Some(zeron_proto::AuthState::SignedIn { .. })
        );
        let interaction_blocked = self.busy || self.load_task.is_some();

        let mut column = widgets::page_column()
            .child(widgets::page_header(&theme, "Team", None))
            .child(widgets::page_subtitle(
                &theme,
                "Workspaces are shared: every member sees and can drive every session in the workspace.",
            ));

        if scope == Some(WorkspaceScope::Local) || !signed_in {
            return column.child(
                div()
                    .mt(px(16.0))
                    .text_size(px(13.0))
                    .text_color(theme.text_muted)
                    .child("Sign in to create or join a shared workspace."),
            );
        }

        if let Some(error) = &self.error {
            column = column.child(widgets::error_strip(&theme, error.clone()));
        }
        if let Some(info) = &self.info {
            column = column.child(
                div()
                    .mt(px(12.0))
                    .px(px(14.0))
                    .py(px(10.0))
                    .rounded(px(10.0))
                    .bg(theme.success.opacity(0.08))
                    .text_size(px(12.5))
                    .text_color(theme.success_muted)
                    .child(info.clone()),
            );
        }

        column = column
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .justify_between()
                    .child(widgets::field_label(&theme, "Workspaces"))
                    .child(
                        widgets::ghost_action(&theme)
                            .id("team-refresh")
                            .hover(|s| widgets::ghost_hover(&theme, s))
                            .when(interaction_blocked, |button| button.opacity(0.45))
                            .when(!interaction_blocked, |button| {
                                button.on_click(cx.listener(|this, _, _, cx| this.refresh(cx)))
                            })
                            .child("Refresh"),
                    ),
            )
            .child(self.render_workspaces(&theme, cx))
            .child(
                div()
                    .mt(px(28.0))
                    .child(widgets::field_label(&theme, "Members")),
            )
            .child(self.render_members(&theme, cx));
        if let Some(danger) = self.render_danger(&theme, cx) {
            column = column.child(danger);
        }
        if self.busy {
            column = column.child(
                div()
                    .mt(px(8.0))
                    .text_size(px(12.0))
                    .text_color(theme.text_muted)
                    .child("Working…"),
            );
        }
        if let Some(confirmation) = self.render_confirmation(window.viewport_size(), &theme, cx) {
            column = column.child(confirmation);
        }
        column
    }
}
