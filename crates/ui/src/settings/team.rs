//! Settings → Team: the org-shared-workspace management surface.
//!
//! - Workspaces: every membership listed; switching (or creating/deleting)
//!   selects the org on the engine and RESTARTS the app — the workspace
//!   storage boundary is captured once at engine startup by design
//!   (ARCHITECTURE.md "Local-first workspace profiles"), so a profile change
//!   is always a process restart, automated here.
//! - Members: the team roster with invite-by-email, role changes, and
//!   removal. Admin-ness is enforced edge-side (live WorkOS membership); the
//!   UI only hides what would be rejected anyway.

use gpui::{
    AnyElement, Context, Entity, SharedString, Subscription, Task, Window, div, prelude::*, px,
};

use zeron_proto::WorkspaceScope;
use zeron_rpc::methods;

use crate::composer::{ComposerInput, ComposerInputEvent};
use crate::settings::widgets;
use crate::state::{AppState, OrgRow, org_name_valid, parse_orgs, sort_memberships};
use crate::theme::Theme;

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

/// Relaunch the app: spawn a detached copy and quit. The 1s delay lets THIS
/// process release the data-dir instance lock and the IPC port first.
// ponytail: sh-based relaunch (Linux/macOS — the packaged targets); a Windows
// build would want a proper detached-spawn here.
fn restart_app(cx: &mut gpui::App) {
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("sleep 1; exec '{}'", exe.display()))
            .spawn();
    }
    cx.quit();
}

pub struct TeamPage {
    state: Entity<AppState>,
    orgs: Vec<OrgRow>,
    members: Vec<MemberRow>,
    invite: Entity<ComposerInput>,
    invite_admin: bool,
    new_org: Option<Entity<ComposerInput>>,
    /// Two-step delete: first click arms, second confirms.
    confirm_delete: bool,
    busy: bool,
    error: Option<SharedString>,
    info: Option<SharedString>,
    task: Option<Task<()>>,
    _subs: Vec<Subscription>,
}

impl TeamPage {
    pub fn new(state: Entity<AppState>, cx: &mut Context<Self>) -> Self {
        let invite = cx.new(|cx| ComposerInput::new("teammate@example.com", cx));
        let mut subs = vec![cx.observe(&state, |_, _, cx| cx.notify())];
        subs.push(cx.subscribe(&invite, |this: &mut Self, _, event, cx| {
            if matches!(event, ComposerInputEvent::Submitted) {
                this.submit_invite(cx);
            }
        }));
        let mut page = Self {
            state,
            orgs: Vec::new(),
            members: Vec::new(),
            invite,
            invite_admin: false,
            new_org: None,
            confirm_delete: false,
            busy: false,
            error: None,
            info: None,
            task: None,
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
            .iter()
            .find(|o| Some(&o.organization_id) == current.as_ref())
            .map(|o| o.role.clone())
            .unwrap_or_else(|| "member".into())
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        let org_id = self.current_org_id(cx);
        self.task = Some(cx.spawn(async move |this, cx| {
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
                match orgs {
                    Ok(value) => page.orgs = sort_memberships(parse_orgs(&value)),
                    Err(err) => {
                        page.error = Some(format!("Loading workspaces failed: {err}").into())
                    }
                }
                match members {
                    Some(Ok(value)) => {
                        page.members = value
                            .get("members")
                            .and_then(|m| serde_json::from_value(m.clone()).ok())
                            .unwrap_or_default();
                    }
                    Some(Err(err)) => {
                        page.error = Some(format!("Loading members failed: {err}").into());
                    }
                    None => page.members = Vec::new(),
                }
                cx.notify();
            })
            .ok();
        }));
    }

    /// Fire an auth RPC, then either restart the app (workspace boundary
    /// changed) or refresh the page data.
    fn run(
        &mut self,
        method: &'static str,
        params: serde_json::Value,
        restart: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        self.busy = true;
        self.error = None;
        self.task = Some(cx.spawn(async move |this, cx| {
            let result = engine.client().call(method, params).await;
            this.update(cx, |page, cx| {
                page.busy = false;
                match result {
                    Ok(value) => {
                        if restart {
                            page.info = Some("Switching workspace — restarting…".into());
                            cx.notify();
                            restart_app(cx);
                            return;
                        }
                        // Invite outcome feedback (added now vs emailed).
                        if value.get("invited").and_then(|v| v.as_bool()) == Some(true) {
                            page.info =
                                Some("Invitation sent — they can join after signing up.".into());
                        } else if value.get("added").and_then(|v| v.as_bool()) == Some(true) {
                            page.info = Some("Member added.".into());
                        }
                        page.refresh(cx);
                    }
                    Err(err) => page.error = Some(format!("{err}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn submit_invite(&mut self, cx: &mut Context<Self>) {
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
        self.invite.update(cx, |input, cx| input.set_text("", cx));
        self.info = None;
        self.run(
            methods::INVITE_MEMBER,
            serde_json::json!({ "organizationId": org_id, "email": email, "role": role }),
            false,
            cx,
        );
    }

    fn submit_new_org(&mut self, cx: &mut Context<Self>) {
        let Some(input) = self.new_org.take() else {
            return;
        };
        let name = input.read(cx).text().trim().to_string();
        if !org_name_valid(&name) {
            self.error = Some("Workspace names must be 1-64 characters.".into());
            cx.notify();
            return;
        }
        // CreateOrg also selects the new org — the restart lands in it.
        self.run(
            methods::CREATE_ORG,
            serde_json::json!({ "name": name }),
            true,
            cx,
        );
    }

    fn render_workspaces(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        let current = self.current_org_id(cx);
        let mut card = widgets::section_card(theme);
        for (ix, org) in self.orgs.clone().into_iter().enumerate() {
            let is_current = Some(&org.organization_id) == current.as_ref();
            let mut row = widgets::card_row(theme, ix == 0).child(
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
                row = row.child(
                    widgets::ghost_action(theme)
                        .id(SharedString::from(format!("switch-org-{ix}")))
                        .child("Switch")
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.run(
                                methods::SELECT_ORG,
                                serde_json::json!({ "organizationId": org_id.clone() }),
                                true,
                                cx,
                            );
                        })),
                );
            }
            card = card.child(row);
        }
        // New-workspace affordance: inline input when armed, button otherwise.
        card = card.child(match &self.new_org {
            Some(input) => widgets::card_row(theme, self.orgs.is_empty())
                .child(div().flex_1().child(input.clone()))
                .child(
                    widgets::ghost_action(theme)
                        .id("new-org-create")
                        .child("Create")
                        .on_click(cx.listener(|this, _, _, cx| this.submit_new_org(cx))),
                ),
            None => widgets::card_row(theme, self.orgs.is_empty()).child(
                widgets::ghost_action(theme)
                    .id("new-org")
                    .child("New workspace…")
                    .on_click(cx.listener(|this, _, _, cx| {
                        let input = cx.new(|cx| ComposerInput::new("Workspace name", cx));
                        let sub = cx.subscribe(&input, |this: &mut Self, _, event, cx| {
                            if matches!(event, ComposerInputEvent::Submitted) {
                                this.submit_new_org(cx);
                            }
                        });
                        this._subs.push(sub);
                        this.new_org = Some(input);
                        cx.notify();
                    })),
            ),
        });
        card.into_any_element()
    }

    fn render_members(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        let my_role = self.my_role(cx);
        let is_admin = my_role == "admin";
        let me = self
            .state
            .read(cx)
            .auth_user()
            .map(|u| u.id.clone())
            .unwrap_or_default();
        let org_id = self.current_org_id(cx).unwrap_or_default();
        let mut card = widgets::section_card(theme);
        for (ix, member) in self.members.clone().into_iter().enumerate() {
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
                        .child(flip_label)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.run(
                                methods::SET_MEMBER_ROLE,
                                serde_json::json!({
                                    "organizationId": org_for_role.clone(),
                                    "membershipId": mid.clone(),
                                    "role": flip_to,
                                }),
                                false,
                                cx,
                            );
                        })),
                );
                let mid = member.membership_id.clone();
                let org_for_remove = org_id.clone();
                row = row.child(
                    widgets::ghost_action(theme)
                        .id(SharedString::from(format!("remove-{ix}")))
                        .child("Remove")
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.run(
                                methods::REMOVE_MEMBER,
                                serde_json::json!({
                                    "organizationId": org_for_remove.clone(),
                                    "membershipId": mid.clone(),
                                }),
                                false,
                                cx,
                            );
                        })),
                );
            }
            card = card.child(row);
        }
        if is_admin {
            let role_label = if self.invite_admin {
                "as Admin"
            } else {
                "as Member"
            };
            card = card.child(
                widgets::card_row(theme, self.members.is_empty())
                    .child(div().flex_1().child(self.invite.clone()))
                    .child(
                        widgets::ghost_action(theme)
                            .id("invite-role")
                            .child(SharedString::from(role_label))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.invite_admin = !this.invite_admin;
                                cx.notify();
                            })),
                    )
                    .child(
                        widgets::ghost_action(theme)
                            .id("invite-send")
                            .child("Invite")
                            .on_click(cx.listener(|this, _, _, cx| this.submit_invite(cx))),
                    ),
            );
        }
        card.into_any_element()
    }

    fn render_danger(&mut self, theme: &Theme, cx: &mut Context<Self>) -> Option<AnyElement> {
        if self.my_role(cx) != "admin" {
            return None;
        }
        let org_id = self.current_org_id(cx)?;
        let label = if self.confirm_delete {
            "Click again to permanently delete this workspace"
        } else {
            "Delete workspace…"
        };
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
                                .child(SharedString::from(label))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if !this.confirm_delete {
                                        this.confirm_delete = true;
                                        cx.notify();
                                        return;
                                    }
                                    this.confirm_delete = false;
                                    this.run(
                                        methods::DELETE_ORG,
                                        serde_json::json!({ "organizationId": org_id.clone() }),
                                        true,
                                        cx,
                                    );
                                })),
                        ),
                )
                .into_any_element(),
        )
    }
}

impl Render for TeamPage {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = Theme::of(cx).clone();
        let scope = self.state.read(cx).workspace_scope;
        let signed_in = matches!(
            self.state.read(cx).auth,
            Some(zeron_proto::AuthState::SignedIn { .. })
        );

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
            column = column.child(
                div()
                    .mt(px(8.0))
                    .text_size(px(12.0))
                    .text_color(theme.danger)
                    .child(error.clone()),
            );
        }
        if let Some(info) = &self.info {
            column = column.child(
                div()
                    .mt(px(8.0))
                    .text_size(px(12.0))
                    .text_color(theme.text_muted)
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
                            .child("Refresh")
                            .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                    ),
            )
            .child(self.render_workspaces(&theme, cx))
            .child(widgets::field_label(&theme, "Members"))
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
        column
    }
}
