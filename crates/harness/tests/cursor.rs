//! CursorHarness integration tests against the fake shim in
//! `tests/fixtures/fake-cursor-shim.sh` (no node/@cursor/sdk involved).

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use zeron_harness::{CancellationToken, CursorHarness, Harness, RunControls, SteerMessage};
use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, McpServerConfig, RunRequest, SandboxLevel, ToolCall,
};

fn fixture_path() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake-cursor-shim.sh");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755));
    }
    path
}

fn harness() -> CursorHarness {
    CursorHarness::new().with_executable(fixture_path())
}

fn request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: None,
        reasoning: None,
        model_options: serde_json::Map::new(),
        cwd: String::new(),
        sandbox: SandboxLevel::DangerFullAccess,
        auto_approve: true,
        attachments: Vec::new(),
        resume: None,
        mcp_servers: Vec::new(),
    }
}

fn mcp_server() -> McpServerConfig {
    McpServerConfig {
        name: "zeron".into(),
        command: "/opt/zeron bin/comet".into(),
        args: vec!["mcp-bridge".into(), "--literal=$HOME".into()],
        env: vec![
            ("ZERON_CHAT_ID".into(), "chat-a".into()),
            ("ZERON_IPC_PORT".into(), "12345".into()),
        ],
    }
}

fn controls() -> (RunControls, mpsc::Sender<SteerMessage>, CancellationToken) {
    let (steer_tx, steer_rx) = mpsc::channel(8);
    let token = CancellationToken::new();
    let controls = RunControls {
        request_input: Box::new(move |_| {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Vec::new());
            rx
        }),
        steering: steer_rx,
        interrupt: token.clone(),
    };
    (controls, steer_tx, token)
}

/// Collect events until the first Done (the session parks afterwards).
async fn run_to_first_done(
    harness: &CursorHarness,
    req: RunRequest,
    controls: RunControls,
) -> Vec<AgentEvent> {
    let mut stream = harness.run(req, controls).await.expect("run starts");
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(ev) = stream.next().await {
            let ev = ev.expect("stream event");
            let done = matches!(ev, AgentEvent::Done { .. });
            events.push(ev);
            if done {
                break;
            }
        }
        events
    })
    .await
    .expect("run finished in time")
}

#[tokio::test]
async fn happy_path_maps_shim_frames_and_tags_subagents() {
    let (controls, _steer, _token) = controls();
    let events = run_to_first_done(&harness(), request("scenario:happy"), controls).await;

    assert!(events.iter().any(|e| matches!(
        e,
        AgentEvent::SessionStarted { harness, model, session_id, .. }
            if *harness == HarnessId::Cursor && model == "composer-2.5" && session_id == "agent-1"
    )));
    assert!(events.contains(&AgentEvent::ReasoningDelta {
        text: "planning".into()
    }));
    assert!(events.contains(&AgentEvent::TextDelta {
        text: "Hello from cursor".into()
    }));
    assert!(events.contains(&AgentEvent::ToolCall {
        id: "c1".into(),
        call: ToolCall::Exec {
            command: "ls -la".into()
        },
    }));
    assert!(events.contains(&AgentEvent::ToolResult {
        id: "c1".into(),
        is_error: false,
        output: None,
        diff: None,
    }));

    // The task spawn is a parent chip; its interior arrives tagged, never bare.
    assert!(events.iter().any(|e| matches!(
        e,
        AgentEvent::ToolCall { id, call: ToolCall::Unknown { name, .. } }
            if id == "task1" && name == "Agent: scan repo"
    )));
    assert!(events.contains(&AgentEvent::Subagent {
        parent_tool_use_id: "task1".into(),
        event: Box::new(AgentEvent::TextDelta {
            text: "sub scanning".into()
        }),
    }));
    assert!(events.contains(&AgentEvent::Subagent {
        parent_tool_use_id: "task1".into(),
        event: Box::new(AgentEvent::ToolCall {
            id: "s1".into(),
            call: ToolCall::Search {
                pattern: "todo".into(),
                path: None,
            },
        }),
    }));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, AgentEvent::ToolCall { id, .. } if id == "s1")),
        "subagent tool leaked into the parent feed: {events:?}"
    );
    // The task tool's end doubles as the subagent's tagged terminal — the
    // SDK has no separate frame for it, and without this the chip stays
    // "running" forever.
    assert!(events.iter().any(|e| matches!(
        e,
        AgentEvent::Subagent { parent_tool_use_id, event }
            if parent_tool_use_id == "task1"
                && matches!(event.as_ref(), AgentEvent::Done { status: DoneStatus::Completed, .. })
    )));

    assert!(events.contains(&AgentEvent::Usage {
        input_tokens: 11,
        output_tokens: 5
    }));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            session_id: Some(id),
            ..
        }) if id == "agent-1"
    ));
}

#[tokio::test]
async fn steer_after_done_becomes_the_next_turn() {
    let (controls, steer, _token) = controls();
    let harness = harness();
    let mut stream = harness
        .run(request("scenario:happy"), controls)
        .await
        .expect("run starts");

    let events = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        let mut dones = 0;
        while let Some(ev) = stream.next().await {
            let ev = ev.expect("stream event");
            if matches!(ev, AgentEvent::Done { .. }) {
                dones += 1;
                if dones == 1 {
                    steer
                        .send(SteerMessage {
                            prompt: "follow up".into(),
                            message_id: None,
                        })
                        .await
                        .expect("steer sent");
                }
            }
            let done = dones >= 2;
            events.push(ev);
            if done {
                break;
            }
        }
        events
    })
    .await
    .expect("both turns finished in time");

    assert!(
        events
            .iter()
            .any(|e| matches!(e, AgentEvent::Steered { .. })),
        "{events:?}"
    );
    assert!(events.contains(&AgentEvent::TextDelta {
        text: "second turn".into()
    }));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AgentEvent::Done { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn interrupt_maps_to_interrupted_done() {
    let harness = CursorHarness::new()
        .with_executable(fixture_path())
        .with_graces(Duration::from_millis(200), Duration::from_millis(500));
    let (controls, _steer, token) = controls();
    let mut stream = harness
        .run(request("scenario:interrupt"), controls)
        .await
        .expect("run starts");

    let events = tokio::time::timeout(Duration::from_secs(10), async move {
        let mut events = Vec::new();
        while let Some(ev) = stream.next().await {
            let ev = ev.expect("stream event");
            if matches!(ev, AgentEvent::TextDelta { .. }) {
                token.cancel();
            }
            let done = matches!(ev, AgentEvent::Done { .. });
            events.push(ev);
            if done {
                break;
            }
        }
        events
    })
    .await
    .expect("interrupt completed in time");

    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Interrupted,
            ..
        })
    ));
}

#[tokio::test]
async fn fatal_frame_surfaces_the_auth_fix_as_an_errored_done() {
    let (controls, _steer, _token) = controls();
    let events = run_to_first_done(&harness(), request("scenario:fatal"), controls).await;
    match events.last() {
        Some(AgentEvent::Done {
            status: DoneStatus::Errored,
            error: Some(message),
            ..
        }) => {
            assert!(message.contains("CURSOR_API_KEY"), "{message}");
        }
        other => panic!("expected errored done, got {other:?}"),
    }
}

#[tokio::test]
async fn mcp_config_is_forwarded_to_cursor_sdk_create_or_resume_options() {
    let (controls, _steer, _token) = controls();
    let mut req = request("scenario:mcp");
    req.resume = Some("agent-existing".into());
    req.mcp_servers = vec![mcp_server()];
    let events = run_to_first_done(&harness(), req, controls).await;

    assert!(events.contains(&AgentEvent::ToolCall {
        id: "mcp-1".into(),
        call: ToolCall::Mcp {
            server: "zeron".into(),
            tool: "send_to_session".into(),
            input: Some(serde_json::json!({ "target_chat_id": "chat-b" })),
        },
    }));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn shim_crash_mid_run_reports_stderr_tail() {
    let (controls, _steer, _token) = controls();
    let events = run_to_first_done(&harness(), request("scenario:crash"), controls).await;
    match events.last() {
        Some(AgentEvent::Done {
            status: DoneStatus::Errored,
            error: Some(message),
            ..
        }) => {
            assert!(
                message.contains("shim exploded") || message.contains("exit code 3"),
                "{message}"
            );
        }
        other => panic!("expected errored done, got {other:?}"),
    }
}
