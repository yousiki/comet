//! Registry integration: two `EngineCore`s with distinct data directories and
//! device IDs share one per-Organization registry through an in-memory server
//! that speaks the RegistryRoom JSON protocol. A live variant against a real
//! edge runs behind `#[ignore]` (`ZERON_EDGE_WS`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use zeron_doc::{CommandBasedOn, SessionCommandEntry, SessionCommandPayload, SessionCommandStatus};
use zeron_engine::{EngineCore, HarnessRegistry};
use zeron_harness::{Harness, HarnessError, RunControls};
use zeron_proto::{
    AgentEvent, ChatConfig, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel,
    SessionStatus, SteeringMode,
};
use zeron_rpc::methods;

const VIEWER: &str = "viewer-device";

/// Scripted harness: emits SessionStarted + text + Done with a per-event delay (so
/// `Working` is observable across the bridge).
struct ScriptedHarness {
    id: HarnessId,
    text: &'static str,
    step_delay: Duration,
}

#[async_trait]
impl Harness for ScriptedHarness {
    fn id(&self) -> HarnessId {
        self.id
    }
    fn display_name(&self) -> &str {
        "Scripted"
    }
    fn supports_steering(&self) -> bool {
        false
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[]
    }
    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(vec![])
    }
    async fn run(
        &self,
        _request: RunRequest,
        _controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<AgentEvent, HarnessError>>(16);
        let harness = self.id;
        let text = self.text;
        let delay = self.step_delay;
        tokio::spawn(async move {
            let script = vec![
                AgentEvent::SessionStarted {
                    harness,
                    model: "scripted-1".into(),
                    tools: vec![],
                    cwd: "/tmp".into(),
                    session_id: "hs-1".into(),
                    assistant_message_id: "a-1".into(),
                },
                AgentEvent::TextDelta { text: text.into() },
                AgentEvent::Done {
                    status: DoneStatus::Completed,
                    result: None,
                    error: None,
                    session_id: Some("hs-1".into()),
                },
            ];
            for event in script {
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
                tokio::time::sleep(delay).await;
            }
        });
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }
}

fn registry() -> Arc<HarnessRegistry> {
    let registry = HarnessRegistry::new();
    registry.register(Arc::new(ScriptedHarness {
        id: HarnessId::Mock,
        text: "Hello",
        step_delay: Duration::from_millis(60),
    }));
    registry.register(Arc::new(ScriptedHarness {
        id: HarnessId::Cursor,
        text: "From cursor",
        step_delay: Duration::from_millis(10),
    }));
    Arc::new(registry)
}

/// Assemble an engine with a fixed device id under its own data dir (offline).
fn assemble(dir: &std::path::Path, device_id: &str) -> EngineCore {
    std::fs::create_dir_all(dir).expect("create data dir");
    std::fs::write(dir.join("device-id"), device_id).expect("write device id");
    EngineCore::assemble(dir, registry(), HarnessId::Mock, None).expect("engine core assembles")
}

/// The in-process room: an in-memory registry server speaking the DO's JSON
/// WS protocol (what the RegistryRoom DO does over the wire), with both
/// engines' hosts wired to it via the test seam.
async fn bridge(
    a: &EngineCore,
    b: &EngineCore,
) -> zeron_sync::registry::mock_server::MockRegistryServer {
    let server = zeron_sync::registry::mock_server::MockRegistryServer::start().await;
    a.registry.connect_registry_url(&server.url());
    b.registry.connect_registry_url(&server.url());
    server
}

async fn wait_for<F>(mut predicate: F, what: &str)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn run_request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: None,
        reasoning: None,
        model_options: Default::default(),
        cwd: "/tmp".into(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        attachments: Vec::new(),
        mcp_servers: Vec::new(),
        worktree: None,
        resume: None,
    }
}

/// Queue a run command into a chat doc the way a remote viewer would (ledger rule 1).
fn queue_run(core: &EngineCore, chat_id: &str, command_id: &str, message_id: &str) {
    queue_run_with(
        core,
        chat_id,
        command_id,
        message_id,
        run_request("go do it"),
    );
}

fn queue_run_with(
    core: &EngineCore,
    chat_id: &str,
    command_id: &str,
    message_id: &str,
    request: RunRequest,
) {
    let handle = core.doc_host.open(chat_id).expect("open chat");
    let now = chrono::Utc::now().timestamp_millis();
    handle
        .doc()
        .queue_command(&SessionCommandEntry {
            id: command_id.into(),
            payload: SessionCommandPayload::Run {
                request,
                message_id: message_id.into(),
            },
            issued_by: VIEWER.into(),
            issued_at: now,
            user_id: None,
            origin: None,
            based_on: None::<CommandBasedOn>,
            expires_at: None,
            status: SessionCommandStatus::Pending,
            resolution: None,
        })
        .expect("queue command");
}

#[tokio::test]
async fn two_engines_share_a_registry() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = assemble(dir_a.path(), "dev-a");
    let b = assemble(dir_b.path(), "dev-b");
    let link = bridge(&a, &b).await;

    // Device rows from BOTH engines appear on both sides.
    for core in [&a, &b] {
        wait_for(
            || {
                let ids: Vec<String> = core
                    .registry
                    .read_devices()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|d| d.id)
                    .collect();
                ids == ["dev-a", "dev-b"]
            },
            "both device rows",
        )
        .await;
    }

    // CreateSpace + CreateChat on A (Mutate over the real RPC surface), hosted
    // by dev-a via the space.
    let client_a = zeron_rpc::memory_client(a.rpc_service());
    let client_b = zeron_rpc::memory_client(b.rpc_service());
    client_a
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createSpace", "spaceId": "space-1", "deviceId": "dev-a", "path": "/tmp"
            }),
        )
        .await
        .expect("create space");
    client_a
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createChat", "chatId": "chat-1", "spaceId": "space-1"
            }),
        )
        .await
        .expect("create chat");
    // The space row crosses to B alongside the chat row.
    wait_for(
        || {
            b.registry
                .read_spaces()
                .unwrap_or_default()
                .iter()
                .any(|s| s.id == "space-1" && s.device_id == "dev-a" && s.path == "/tmp")
        },
        "space row on B",
    )
    .await;
    wait_for(
        || b.registry.chat("chat-1").ok().flatten().is_some(),
        "chat row on B",
    )
    .await;

    // Run on A: B's registry view shows the session Working, then Idle.
    queue_run(&a, "chat-1", "cmd-run-1", "m-1");
    let b_status = |wanted: SessionStatus| {
        let ws = b.registry.clone();
        move || {
            ws.read_sessions()
                .unwrap_or_default()
                .iter()
                .any(|s| s.chat_id == "chat-1" && s.device_id == "dev-a" && s.status == wanted)
        }
    };
    wait_for(b_status(SessionStatus::Working), "Working on B").await;
    wait_for(b_status(SessionStatus::Idle), "Idle on B").await;

    // Sidebar freshness crossed too: the chat row's preview settles on the
    // assistant's final text (first-120-chars policy).
    wait_for(
        || {
            b.registry
                .chat("chat-1")
                .ok()
                .flatten()
                .and_then(|c| c.last_message_preview)
                .as_deref()
                == Some("Hello")
        },
        "assistant preview on B",
    )
    .await;

    // Rename + archive from B (LWW from any device) become visible on A.
    client_b
        .call(
            methods::MUTATE,
            serde_json::json!({ "op": "renameChat", "chatId": "chat-1", "title": "Renamed from B" }),
        )
        .await
        .expect("rename chat");
    client_b
        .call(
            methods::MUTATE,
            serde_json::json!({ "op": "setChatArchived", "chatId": "chat-1", "archived": true }),
        )
        .await
        .expect("archive chat");
    wait_for(
        || {
            a.registry
                .chat("chat-1")
                .ok()
                .flatten()
                .is_some_and(|c| c.title.as_deref() == Some("Renamed from B") && c.archived)
        },
        "rename + archive on A",
    )
    .await;

    // Device rename from B visible on A.
    client_b
        .call(
            methods::MUTATE,
            serde_json::json!({ "op": "renameDevice", "deviceId": "dev-b", "name": "B's VPS" }),
        )
        .await
        .expect("rename device");
    wait_for(
        || {
            a.registry
                .read_devices()
                .unwrap_or_default()
                .iter()
                .any(|d| d.id == "dev-b" && d.name == "B's VPS")
        },
        "device rename on A",
    )
    .await;

    drop(link);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn claim_on_first_command_creates_the_chat_row() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = assemble(dir_a.path(), "dev-a");
    let b = assemble(dir_b.path(), "dev-b");
    let link = bridge(&a, &b).await;

    // No CreateChat: the first run command claims the chat under A's device id.
    queue_run(&a, "chat-claimed", "cmd-claim-1", "m-1");
    wait_for(
        || {
            b.registry
                .chat("chat-claimed")
                .ok()
                .flatten()
                .is_some_and(|c| c.device_id == "dev-a" && c.cwd.as_deref() == Some("/tmp"))
        },
        "claimed chat row on B",
    )
    .await;

    drop(link);
    a.shutdown().await;
    b.shutdown().await;
}

/// A first command whose cwd is a linked WORKTREE must attribute the chat to
/// the parent checkout's space — claiming at the worktree path minted a
/// phantom sidebar space named after the worktree folder.
#[tokio::test]
async fn claim_resolves_a_worktree_cwd_to_the_repo_root_space() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble(dir.path(), "dev-a");
    let client = zeron_rpc::memory_client(core.rpc_service());

    // A checkout with a linked worktree — fs layout only; the claim path
    // reads `.git` without spawning git.
    let repo = tempfile::tempdir().unwrap();
    let root = repo.path().join("proj");
    let wt = repo.path().join("clever-ember");
    std::fs::create_dir_all(root.join(".git/worktrees/clever-ember")).unwrap();
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(
        wt.join(".git"),
        format!(
            "gitdir: {}\n",
            root.join(".git/worktrees/clever-ember").display()
        ),
    )
    .unwrap();

    // The project's space already exists (the normal state — sessions are
    // created FROM a space).
    client
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createSpace", "spaceId": "space-proj", "deviceId": "dev-a",
                "path": root.to_string_lossy(),
            }),
        )
        .await
        .expect("create space");

    let request = RunRequest {
        cwd: wt.to_string_lossy().into_owned(),
        ..run_request("go do it")
    };
    queue_run_with(&core, "chat-wt", "cmd-wt-1", "m-1", request);
    wait_for(
        || {
            core.registry
                .chat("chat-wt")
                .ok()
                .flatten()
                .is_some_and(|c| c.space_id.as_deref() == Some("space-proj"))
        },
        "worktree chat attributed to the project space",
    )
    .await;
    let spaces = core.registry.read_spaces().unwrap_or_default();
    assert_eq!(
        spaces.len(),
        1,
        "no phantom space for the worktree: {spaces:?}"
    );
    core.shutdown().await;
}

/// The claimed row records the harness the run actually dispatched on (the
/// request carries the picked harness) — without it the sidebar renders no
/// harness glyph and later dispatches silently fall back to the default.
#[tokio::test]
async fn claimed_chat_row_records_the_run_harness() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble(dir.path(), "dev-a");

    let request = RunRequest {
        harness: Some(HarnessId::Cursor),
        ..run_request("go do it")
    };
    queue_run_with(&core, "chat-glyph", "cmd-glyph-1", "m-1", request);
    wait_for(
        || {
            core.registry
                .chat("chat-glyph")
                .ok()
                .flatten()
                .and_then(|c| c.config)
                .is_some_and(|c| c.harness == HarnessId::Cursor)
        },
        "claimed row carries the dispatched harness",
    )
    .await;
    core.shutdown().await;
}

#[tokio::test]
async fn non_host_engine_leaves_remote_chats_commands_alone() {
    let dir_a = tempfile::tempdir().unwrap();
    let a = assemble(dir_a.path(), "dev-a");

    // The registry says dev-b hosts this chat (via its dev-b space); a run
    // command in A's local copy of the session doc must NOT execute on A
    // (is_host gating).
    a.registry
        .create_space("space-remote", "dev-b", "/tmp/remote", None, false)
        .expect("create remote space row");
    a.registry
        .create_chat("chat-remote", Some("space-remote"), None, None, None)
        .expect("create remote-hosted chat row");
    queue_run(&a, "chat-remote", "cmd-remote-1", "m-1");

    tokio::time::sleep(Duration::from_millis(400)).await;
    let handle = a.doc_host.open("chat-remote").expect("open chat");
    let commands = handle.doc().read_commands().expect("read commands");
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].status,
        SessionCommandStatus::Pending,
        "command must stay pending"
    );
    let entries = handle.doc().read_entries().expect("read entries");
    assert!(
        entries.is_empty(),
        "non-host must not write entries: {entries:#?}"
    );
    assert!(a.sessions.session_status("chat-remote").is_none());

    a.shutdown().await;
}

#[tokio::test]
async fn chat_config_selects_the_run_harness() {
    let dir_a = tempfile::tempdir().unwrap();
    let a = assemble(dir_a.path(), "dev-a"); // default harness = Mock ("Hello")

    a.registry
        .create_space("space-cfg", "dev-a", "/tmp/cfg", None, false)
        .expect("create space");
    a.registry
        .create_chat(
            "chat-cfg",
            Some("space-cfg"),
            None,
            Some(ChatConfig {
                harness: HarnessId::Cursor,
                model: None,
                reasoning: None,
                model_options: Default::default(),
                sandbox: SandboxLevel::WorkspaceWrite,
            }),
            None,
        )
        .expect("create configured chat");
    queue_run(&a, "chat-cfg", "cmd-cfg-1", "m-1");

    // The configured harness (Cursor, "From cursor") ran — not the default Mock.
    let handle = a.doc_host.open("chat-cfg").expect("open chat");
    wait_for(
        || {
            handle.doc().read_entries().unwrap_or_default().iter().any(|e| {
                e.parts.iter().any(
                    |p| matches!(p, zeron_doc::MessagePart::Text { text, .. } if text == "From cursor"),
                )
            })
        },
        "configured-harness output",
    )
    .await;

    a.shutdown().await;
}

/// Live-edge variant: the same convergence through a real registry room. Requires
/// the TS edge (`wrangler dev` in `edge/` with AUTH_MODE=dev):
///
/// ```sh
/// ZERON_EDGE_WS=ws://127.0.0.1:8787 cargo test -p zeron-engine -- --ignored
/// ```
#[tokio::test]
#[ignore = "requires a live edge: set ZERON_EDGE_WS (e.g. ws://127.0.0.1:8787)"]
async fn two_engines_converge_through_a_real_registry_room() {
    use zeron_engine::doc_host::EdgeConfig;

    let base = std::env::var("ZERON_EDGE_WS")
        .expect("set ZERON_EDGE_WS to the edge origin, e.g. ws://127.0.0.1:8787");
    let organization_id = format!("org-{}", uuid::Uuid::new_v4().simple());

    let assemble_live = |dir: &std::path::Path, device_id: &str, user: &str| {
        std::fs::create_dir_all(dir).expect("create data dir");
        std::fs::write(dir.join("device-id"), device_id).expect("write device id");
        // The dev-mode `user@organization` bearer carries the Organization claim.
        let edge = Some(EdgeConfig::with_static_token(
            base.clone(),
            format!("{user}@{organization_id}"),
        ));
        EngineCore::assemble_with_identity(
            dir,
            registry(),
            HarnessId::Mock,
            edge,
            &organization_id,
            user,
        )
        .expect("engine core assembles")
    };

    // Legacy workspace docs were per-user (`ws3/{organizationId}/{userId}`): convergence is across
    // ONE user's devices — two engines, same user, different device ids.
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = assemble_live(dir_a.path(), "dev-live-a", "alice");
    let b = assemble_live(dir_b.path(), "dev-live-b", "alice");

    // Both device rows converge through the real room.
    for core in [&a, &b] {
        wait_for(
            || {
                let ids: Vec<String> = core
                    .registry
                    .read_devices()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|d| d.id)
                    .collect();
                ids == ["dev-live-a", "dev-live-b"]
            },
            "both device rows through the edge",
        )
        .await;
    }

    // A rename from B lands on A.
    b.registry
        .rename_device("dev-live-a", "renamed by b")
        .expect("rename");
    wait_for(
        || {
            a.registry
                .read_devices()
                .unwrap_or_default()
                .iter()
                .any(|d| d.id == "dev-live-a" && d.name == "renamed by b")
        },
        "device rename through the edge",
    )
    .await;

    a.shutdown().await;
    b.shutdown().await;
}
