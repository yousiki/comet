//! The right-pane "Files" surface: a lazy directory tree over `ListFolders`
//! (dotfiles shown, `.git` excluded engine-side) beside a read-only file
//! viewer over `ReadWorkingDirectoryFile`.
//!
//! - roots at the selected chat's cwd and follows chat switches (the same
//!   dynamic resolution the Diff surface uses); remote chats relay-forward
//!   both RPCs to the host device via `targetDeviceId`;
//! - the tree is a flattened row list virtualized with gpui `list()` at a
//!   constant row height; expand/collapse recomputes the flatten and splices
//!   only the changed middle so the scroll anchor never jumps;
//! - the viewer reuses the diff pane's line model: one row per line, analytic
//!   21px height, mono no-wrap, line-number gutter, whole-file tree-sitter
//!   highlight computed on the background executor and applied as paint-only
//!   runs (files past the highlighter's 1 MiB limit render plain).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use gpui::{
    AnyElement, App, Context, Entity, ListAlignment, ListState, SharedString, Subscription, Task,
    Window, div, font, list, prelude::*, px,
};

use zeron_proto::{FolderListing, WorkingDirectoryFileText};
use zeron_rpc::methods;
use zeron_syntax::HighlightedDocument;

use crate::changes::Changes;
use crate::icons;
use crate::markdown::render;
use crate::popover::{self, Loadable};
use crate::state::{AppState, rpc_error_notice};
use crate::theme::{self, Theme};

/// Tree column width (Zed's project panel default neighborhood).
const TREE_WIDTH: f32 = 220.0;
const TREE_ROW_HEIGHT: f32 = 24.0;
/// Viewer line height — matches the diff pane so the two read alike.
const LINE_HEIGHT: f32 = 21.0;
const TEXT_SIZE: f32 = 12.0;

/// One visible tree row: a flatten of the loaded listings under the root.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeRow {
    /// Absolute path on the chat's host device.
    path: String,
    name: SharedString,
    depth: usize,
    is_dir: bool,
}

pub struct FileExplorer {
    state: Entity<AppState>,
    /// Resolved Working Directory root (the selected chat's cwd) + relay target; both
    /// re-checked on every state change so chat switches re-root the tree.
    root: Option<String>,
    target: Option<String>,
    chat_id: Option<String>,
    started: bool,

    // Tree.
    listings: HashMap<String, Loadable<FolderListing>>,
    expanded: HashSet<String>,
    rows: Vec<TreeRow>,
    tree_list: ListState,
    load_tasks: HashMap<String, Task<()>>,

    // Viewer.
    open_path: Option<String>,
    file: Loadable<WorkingDirectoryFileText>,
    lines: Vec<SharedString>,
    highlights: Option<Arc<HighlightedDocument>>,
    viewer_list: ListState,
    _file_task: Option<Task<()>>,
    _highlight_task: Option<Task<()>>,

    _observe: Subscription,
}

impl FileExplorer {
    pub fn new(state: Entity<AppState>, cx: &mut Context<Self>) -> Self {
        let observe = cx.observe(&state, |this: &mut Self, _, cx| {
            if this.started {
                this.ensure_content(cx);
            }
        });
        Self {
            state,
            root: None,
            target: None,
            chat_id: None,
            started: false,
            listings: HashMap::new(),
            expanded: HashSet::new(),
            rows: Vec::new(),
            tree_list: ListState::new(0, ListAlignment::Top, px(1024.0)),
            load_tasks: HashMap::new(),
            open_path: None,
            file: Loadable::Idle,
            lines: Vec::new(),
            highlights: None,
            viewer_list: ListState::new(0, ListAlignment::Top, px(1024.0)),
            _file_task: None,
            _highlight_task: None,
            _observe: observe,
        }
    }

    /// Surface-tab title: the Working Directory folder's name.
    pub fn tab_title(&self) -> SharedString {
        self.root
            .as_deref()
            .and_then(|root| root.trim_end_matches('/').rsplit('/').next())
            .filter(|name| !name.is_empty())
            .map(|name| SharedString::from(name.to_string()))
            .unwrap_or_else(|| SharedString::from("Files"))
    }

    /// The chat's host device when it differs from the connected engine's own
    /// (same rule as the Diff surface) — the filesystem lives on the host.
    fn desired_target(&self, cx: &App) -> Option<String> {
        let state = self.state.read(cx);
        let device = state.selected_chat_row()?.device_id.clone();
        (state.local_device_id.as_deref() != Some(device.as_str())).then_some(device)
    }

    /// Idempotent "make my data current": resolve the root from the selected
    /// chat; on root/target change reset everything and load the root listing.
    pub fn ensure_content(&mut self, cx: &mut Context<Self>) {
        self.started = true;
        let (root, chat_id) = {
            let state = self.state.read(cx);
            let chat = state.selected_chat_row();
            (
                chat.and_then(|chat| chat.cwd.clone()),
                chat.map(|chat| chat.id.clone()),
            )
        };
        let target = self.desired_target(cx);
        if self.root == root && self.target == target {
            // Same Working Directory — just make sure the root listing was requested.
            if let Some(root) = self.root.clone()
                && !self.listings.contains_key(&root)
            {
                self.request_listing(root, cx);
            }
            self.chat_id = chat_id;
            return;
        }
        self.root = root.clone();
        self.target = target;
        self.chat_id = chat_id;
        self.listings.clear();
        self.expanded.clear();
        self.load_tasks.clear();
        self.open_path = None;
        self.file = Loadable::Idle;
        self.lines = Vec::new();
        self.highlights = None;
        self._file_task = None;
        self._highlight_task = None;
        self.viewer_list.reset(0);
        self.apply_rows(Vec::new());
        if let Some(root) = root {
            self.request_listing(root, cx);
        }
        cx.notify();
    }

    /// Re-request the root and every expanded directory (header refresh).
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let Some(root) = self.root.clone() else {
            return;
        };
        let mut dirs: Vec<String> = self.expanded.iter().cloned().collect();
        dirs.push(root);
        for dir in dirs {
            self.request_listing(dir, cx);
        }
        if let Some(path) = self.open_path.clone() {
            self.open_file(path, cx);
        }
    }

    // ── Tree ────────────────────────────────────────────────────────────────

    /// ListFolders (dotfiles included) on the chat's host device.
    fn request_listing(&mut self, path: String, cx: &mut Context<Self>) {
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        let target = self.target.clone();
        self.listings.insert(path.clone(), Loadable::Loading);
        self.rebuild_rows();
        let task_path = path.clone();
        let task = cx.spawn(async move |this, cx| {
            let mut params = serde_json::Map::new();
            params.insert("path".into(), serde_json::Value::String(path.clone()));
            params.insert("showHidden".into(), serde_json::Value::Bool(true));
            if let Some(target) = &target {
                params.insert(
                    "targetDeviceId".into(),
                    serde_json::Value::String(target.clone()),
                );
            }
            let result = engine
                .client()
                .call(methods::LIST_FOLDERS, serde_json::Value::Object(params))
                .await;
            this.update(cx, |this, cx| {
                this.load_tasks.remove(&path);
                let slot = match result {
                    Ok(value) => match serde_json::from_value::<FolderListing>(value) {
                        Ok(listing) => Loadable::Ready(listing),
                        Err(err) => Loadable::Error(err.to_string()),
                    },
                    Err(err) => Loadable::Error(rpc_error_notice(&err)),
                };
                this.listings.insert(path, slot);
                this.rebuild_rows();
                cx.notify();
            })
            .ok();
        });
        self.load_tasks.insert(task_path, task);
    }

    fn toggle_dir(&mut self, path: String, cx: &mut Context<Self>) {
        if self.expanded.contains(&path) {
            // Collapse forgets the whole subtree — cached listings, in-flight
            // loads, and nested expansion alike. Re-expanding re-fetches one
            // fresh level, so cached content can never go stale behind a
            // collapsed chevron and the cache stays bounded to the root plus
            // currently expanded directories (exactly the set the header
            // refresh re-requests).
            let subtree_prefix = format!("{path}/");
            let in_subtree = |dir: &str| dir == path || dir.starts_with(&subtree_prefix);
            self.expanded.retain(|dir| !in_subtree(dir));
            self.listings.retain(|dir, _| !in_subtree(dir));
            self.load_tasks.retain(|dir, _| !in_subtree(dir));
        } else {
            self.expanded.insert(path.clone());
            self.request_listing(path, cx);
            return; // request_listing already rebuilt.
        }
        self.rebuild_rows();
        cx.notify();
    }

    /// Recompute the flatten and splice only the changed middle into the
    /// ListState — splicing (vs reset) keeps the scroll anchor in place.
    fn rebuild_rows(&mut self) {
        let mut rows = Vec::new();
        if let Some(root) = &self.root {
            self.flatten_into(root, 0, &mut rows);
        }
        self.apply_rows(rows);
    }

    fn flatten_into(&self, dir: &str, depth: usize, out: &mut Vec<TreeRow>) {
        let Some(Loadable::Ready(listing)) = self.listings.get(dir) else {
            return;
        };
        for entry in &listing.entries {
            let path = join_path(dir, &entry.name);
            out.push(TreeRow {
                name: SharedString::from(entry.name.clone()),
                depth,
                is_dir: entry.is_dir,
                path: path.clone(),
            });
            if entry.is_dir && self.expanded.contains(&path) {
                self.flatten_into(&path, depth + 1, out);
            }
        }
    }

    fn apply_rows(&mut self, new_rows: Vec<TreeRow>) {
        let old = &self.rows;
        let prefix = old
            .iter()
            .zip(new_rows.iter())
            .take_while(|(a, b)| a == b)
            .count();
        let suffix = old[prefix..]
            .iter()
            .rev()
            .zip(new_rows[prefix..].iter().rev())
            .take_while(|(a, b)| a == b)
            .count();
        self.tree_list
            .splice(prefix..old.len() - suffix, new_rows.len() - suffix - prefix);
        self.rows = new_rows;
    }

    // ── Viewer ──────────────────────────────────────────────────────────────

    fn open_file(&mut self, path: String, cx: &mut Context<Self>) {
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        let Some(chat_id) = self.chat_id.clone() else {
            return;
        };
        let target = self.target.clone();
        self.open_path = Some(path.clone());
        self.file = Loadable::Loading;
        self.lines = Vec::new();
        self.highlights = None;
        self._highlight_task = None;
        self.viewer_list.reset(0);
        cx.notify();
        self._file_task = Some(cx.spawn(async move |this, cx| {
            let mut params = serde_json::Map::new();
            params.insert("chatId".into(), serde_json::Value::String(chat_id));
            params.insert("path".into(), serde_json::Value::String(path.clone()));
            if let Some(target) = &target {
                params.insert(
                    "targetDeviceId".into(),
                    serde_json::Value::String(target.clone()),
                );
            }
            let result = engine
                .client()
                .call(
                    methods::READ_WORKING_DIRECTORY_FILE,
                    serde_json::Value::Object(params),
                )
                .await;
            this.update(cx, |this, cx| {
                // A newer click wins.
                if this.open_path.as_deref() != Some(path.as_str()) {
                    return;
                }
                match result {
                    Ok(value) => match serde_json::from_value::<WorkingDirectoryFileText>(value) {
                        Ok(file) => this.set_file(path, file, cx),
                        Err(err) => this.file = Loadable::Error(err.to_string()),
                    },
                    Err(err) => this.file = Loadable::Error(rpc_error_notice(&err)),
                }
                cx.notify();
            })
            .ok();
        }));
    }

    fn set_file(&mut self, path: String, file: WorkingDirectoryFileText, cx: &mut Context<Self>) {
        if let Some(text) = &file.text {
            self.lines = text
                .lines()
                .map(|l| SharedString::from(l.to_string()))
                .collect();
            self.viewer_list
                .reset_with_uniform_height(self.lines.len(), px(LINE_HEIGHT));
            self.spawn_highlight(path, text.clone(), cx);
        }
        self.file = Loadable::Ready(file);
    }

    /// Whole-file tree-sitter highlight on the background executor; paint-only
    /// runs, so a late arrival just recolors. Language detection (extension,
    /// known filename, shebang) happens inside `highlight`; unsupported or
    /// oversized files (the highlighter caps at 1 MiB) stay plain.
    fn spawn_highlight(&mut self, path: String, source: String, cx: &mut Context<Self>) {
        let request_path = path.clone();
        self._highlight_task = Some(cx.spawn(async move |this, cx| {
            let doc = cx
                .background_executor()
                .spawn(async move {
                    zeron_syntax::highlight(zeron_syntax::HighlightRequest {
                        source: &source,
                        path: Some(&request_path),
                        fence_tag: None,
                    })
                    .ok()
                    .map(Arc::new)
                })
                .await;
            this.update(cx, |this, cx| {
                if this.open_path.as_deref() == Some(path.as_str()) {
                    this.highlights = doc;
                    cx.notify();
                }
            })
            .ok();
        }));
    }

    // ── Chrome ──────────────────────────────────────────────────────────────

    /// The 36px header-row contents the shell mounts above the pane: the open
    /// file's Working Directory-relative path (else the root path) + refresh.
    pub fn render_header_controls(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = Theme::of(cx).clone();
        let label = match (&self.open_path, &self.root) {
            (Some(open), Some(root)) => open
                .strip_prefix(root.as_str())
                .map(|rel| rel.trim_start_matches('/').to_string())
                .unwrap_or_else(|| open.clone()),
            _ => self.root.clone().unwrap_or_default(),
        };
        div()
            .size_full()
            .flex()
            .flex_row()
            .items_center()
            .gap(px(8.0))
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .font_family(theme.font_mono.clone())
                    .text_size(px(11.0))
                    .text_color(theme.text_muted)
                    .child(SharedString::from(label)),
            )
            .child(
                Changes::header_button("file-explorer-refresh", icons::REFRESH, &theme).on_click(
                    cx.listener(|this, _, _, cx| {
                        cx.stop_propagation();
                        this.refresh(cx);
                    }),
                ),
            )
            .into_any_element()
    }

    fn render_tree_row(
        &mut self,
        ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(row) = self.rows.get(ix).cloned() else {
            return div().into_any_element();
        };
        let theme = Theme::of(cx).clone();
        let selected = self.open_path.as_deref() == Some(row.path.as_str());
        let expanded = row.is_dir && self.expanded.contains(&row.path);
        let icon_path = if row.is_dir {
            icons::FOLDER
        } else {
            icons::DOCUMENT
        };
        let chevron = row.is_dir.then(|| {
            icons::icon(if expanded {
                icons::ALT_ARROW_DOWN
            } else {
                icons::ALT_ARROW_RIGHT
            })
            .size(px(12.0))
            .flex_none()
            .text_color(theme.text_faint)
        });
        let path = row.path.clone();
        let is_dir = row.is_dir;
        div()
            .id(("fx-tree-row", ix))
            .h(px(TREE_ROW_HEIGHT))
            .w_full()
            .flex_none()
            .flex()
            .flex_row()
            .items_center()
            .gap(px(4.0))
            // Chevron-less files get the chevron's 12px as extra padding so
            // names in one directory align.
            .pl(px(6.0
                + row.depth as f32 * 14.0
                + if row.is_dir { 0.0 } else { 12.0 }))
            .pr(px(6.0))
            .cursor_pointer()
            .when(selected, |el| el.bg(theme::card_selected_bg()))
            .when(!selected, |el| el.hover(|s| s.bg(theme::ink(0.05))))
            .on_click(cx.listener(move |this, _, _, cx| {
                if is_dir {
                    this.toggle_dir(path.clone(), cx);
                } else {
                    this.open_file(path.clone(), cx);
                }
            }))
            .children(chevron)
            .child(
                icons::icon(icon_path)
                    .size(px(14.0))
                    .flex_none()
                    .text_color(theme.text_muted.opacity(0.8)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .text_size(px(12.5))
                    .text_color(if selected {
                        theme.text
                    } else {
                        theme.text_muted
                    })
                    .child(row.name),
            )
            .into_any_element()
    }

    fn render_viewer_row(
        &mut self,
        ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(line) = self.lines.get(ix) else {
            return div().into_any_element();
        };
        let theme = Theme::of(cx).clone();
        let empty: &[zeron_syntax::HighlightSpan] = &[];
        let spans = self
            .highlights
            .as_ref()
            .and_then(|doc| doc.lines.get(ix))
            .map_or(empty, |spans| spans.as_slice());
        let mono = font(theme.font_mono.clone());
        let runs = render::runs_for_syntax_line_with_plain(
            line,
            spans,
            &mono,
            theme.text.opacity(0.92),
            &theme,
        );
        let gutter_px = self.gutter_px();
        div()
            .h(px(LINE_HEIGHT))
            .w_full()
            .flex_none()
            .flex()
            .flex_row()
            .items_center()
            .child(
                div()
                    .w(px(gutter_px))
                    .flex_none()
                    .font_family(theme.font_mono.clone())
                    .text_size(px(11.0))
                    .text_color(theme.text_faint.opacity(0.8))
                    .flex()
                    .justify_end()
                    .pr(px(8.0))
                    .child(SharedString::from((ix + 1).to_string())),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .pl(px(10.0))
                    .font_family(theme.font_mono.clone())
                    .text_size(px(TEXT_SIZE))
                    .whitespace_nowrap()
                    .child(gpui::StyledText::new(line.clone()).with_runs(runs)),
            )
            .into_any_element()
    }

    /// Gutter fitted to the largest line number (the diff pane's formula).
    fn gutter_px(&self) -> f32 {
        let digits = (self.lines.len().max(1) as u32).ilog10() + 1;
        (digits as f32 * 6.6 + 8.0 + 6.0).max(36.0)
    }

    fn centered_note(theme: &Theme, copy: &str) -> AnyElement {
        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .text_size(px(12.0))
            .text_color(theme.text_faint)
            .child(SharedString::from(copy.to_string()))
            .into_any_element()
    }

    fn render_viewer(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        if self.open_path.is_none() {
            return Self::centered_note(theme, "Select a file");
        }
        match &self.file {
            Loadable::Idle => Self::centered_note(theme, "Select a file"),
            Loadable::Loading => div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .child(crate::loaders::gradient_spinner(
                    "file-explorer-loading",
                    theme,
                    3.0,
                    cx.entity_id(),
                    cx,
                ))
                .into_any_element(),
            Loadable::Error(message) => {
                let message = message.clone();
                div()
                    .flex_1()
                    .p(px(8.0))
                    .child(popover::error_row(theme, &message))
                    .into_any_element()
            }
            Loadable::Ready(file) if file.binary => Self::centered_note(theme, "Binary file"),
            Loadable::Ready(file) if file.truncated => {
                Self::centered_note(theme, "File too large to preview (2 MiB limit)")
            }
            Loadable::Ready(_) => list(
                self.viewer_list.clone(),
                cx.processor(Self::render_viewer_row),
            )
            .flex_1()
            .with_sizing_behavior(gpui::ListSizingBehavior::Auto)
            .into_any_element(),
        }
    }

    fn render_tree(&mut self, theme: &Theme, cx: &mut Context<Self>) -> AnyElement {
        let root_slot = self.root.as_ref().and_then(|root| self.listings.get(root));
        match root_slot {
            Some(Loadable::Loading) if self.rows.is_empty() => {
                popover::skeleton_rows("file-explorer-tree-skeleton", theme, 8, cx.entity_id(), cx)
            }
            Some(Loadable::Error(message)) => {
                let message = message.clone();
                div()
                    .p(px(8.0))
                    .child(popover::error_row(theme, &message))
                    .into_any_element()
            }
            _ => list(self.tree_list.clone(), cx.processor(Self::render_tree_row))
                .size_full()
                .with_sizing_behavior(gpui::ListSizingBehavior::Auto)
                .into_any_element(),
        }
    }
}

/// Join a directory and a child name without doubling separators.
fn join_path(dir: &str, name: &str) -> String {
    format!("{}/{}", dir.trim_end_matches('/'), name)
}

impl Render for FileExplorer {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = Theme::of(cx).clone();
        if self.root.is_none() {
            return div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .text_size(px(12.0))
                .text_color(theme.text_faint)
                .child(SharedString::from("This session has no working directory"));
        }
        let tree = self.render_tree(&theme, cx);
        let viewer = self.render_viewer(&theme, cx);
        div()
            .size_full()
            .flex()
            .flex_row()
            .min_h_0()
            .child(
                div()
                    .w(px(TREE_WIDTH))
                    .flex_none()
                    .h_full()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(tree),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .child(viewer),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_path_never_doubles_separators() {
        assert_eq!(join_path("/a/b", "c"), "/a/b/c");
        assert_eq!(join_path("/a/b/", "c"), "/a/b/c");
        assert_eq!(join_path("/", "c"), "/c");
    }
}
