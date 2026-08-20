//! `RegistryHost::is_host` fail-closed matrix (organization-shared registry): an
//! engine that has never synced its registry room must NOT believe it hosts a
//! chat it has no row for — in a shared registry that window would
//! double-execute a teammate's commands. Edge-less hosts keep the original
//! claim-anything behavior (local-only chats must still run).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use zeron_doc::SessionCommandPayload;
use zeron_engine::{EngineCore, HarnessRegistry, RegistryHost, RegistryHostConfig};
use zeron_harness::{Harness, HarnessError, RunControls};
use zeron_proto::{
    AgentEvent, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel, SteeringMode,
};
use zeron_sync::DocsStore;

struct CountingHarness {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl Harness for CountingHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Mock
    }

    fn display_name(&self) -> &str {
        "Counting"
    }

    fn supports_steering(&self) -> bool {
        false
    }

    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }

    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[ReasoningLevel::Medium]
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(Vec::new())
    }

    async fn run(
        &self,
        request: RunRequest,
        _controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        if request.prompt == "run after registry readiness" {
            self.runs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(futures::stream::empty().boxed())
    }
}

fn open_host(dir: &std::path::Path, edge: Option<zeron_engine::EdgeConfig>) -> RegistryHost {
    let store = Arc::new(DocsStore::open(dir).expect("store opens"));
    RegistryHost::open(
        store,
        RegistryHostConfig {
            device_id: "dev-me".into(),
            device_name: "test".into(),
            platform: "linux".into(),
            organization_id: "org-test".into(),
            user_id: "alice".into(),
            edge,
        },
    )
    .expect("registry host opens")
}

struct GatedRegistryEdge {
    url: String,
    fetch_release: watch::Sender<bool>,
    fetch_started: watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl GatedRegistryEdge {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind edge");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (fetch_release, release_rx) = watch::channel(false);
        let (started_tx, fetch_started) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve_registry_request(
                    stream,
                    started_tx.clone(),
                    release_rx.clone(),
                ));
            }
        });
        Self {
            url,
            fetch_release,
            fetch_started,
            task,
        }
    }

    async fn wait_for_fetch(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !*self.fetch_started.borrow() {
                self.fetch_started
                    .changed()
                    .await
                    .expect("edge task stays alive");
            }
        })
        .await
        .expect("registry pull did not start");
    }

    fn release_fetch(&self) {
        self.fetch_release.send(true).unwrap();
    }
}

impl Drop for GatedRegistryEdge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<(String, String, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    Some((method, target, String::from_utf8_lossy(&body).into_owned()))
}

async fn respond(stream: &mut tokio::net::TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn serve_registry_request(
    mut stream: tokio::net::TcpStream,
    fetch_started: watch::Sender<bool>,
    mut fetch_release: watch::Receiver<bool>,
) {
    let Some((method, target, body)) = read_request(&mut stream).await else {
        return;
    };
    if method == "POST" && target.starts_with("/registry/org-test/push?") {
        let Ok(body) = serde_json::from_str::<serde_json::Value>(&body) else {
            respond(&mut stream, "400 Bad Request", r#"{"error":"bad json"}"#).await;
            return;
        };
        let Some(batch) = body["batch"].as_str() else {
            respond(&mut stream, "400 Bad Request", r#"{"error":"no batch"}"#).await;
            return;
        };
        let ack = serde_json::json!({ "batch": batch, "seq": 11, "applied": 1 });
        respond(&mut stream, "200 OK", &ack.to_string()).await;
    } else if method == "GET" && target.starts_with("/registry/org-test/rows?") {
        let _ = fetch_started.send(true);
        while !*fetch_release.borrow() {
            if fetch_release.changed().await.is_err() {
                return;
            }
        }
        let clock = "0000000001000-000000-dev-b";
        let pull = serde_json::json!({
            "seq": 11,
            "full": false,
            "gcFloor": 0,
            "rows": [{
                "kind": "chats",
                "id": "foreign-chat",
                "seq": 10,
                "deleted": false,
                "fields": {
                    "id": "foreign-chat",
                    "deviceId": "dev-b",
                    "createdAt": 1_000,
                },
                "clocks": {
                    "id": clock,
                    "deviceId": clock,
                    "createdAt": clock,
                },
            }],
        });
        respond(&mut stream, "200 OK", &pull.to_string()).await;
    } else {
        // The WebSocket side is intentionally unavailable: this test isolates
        // the HTTP local-first path and its remote-readiness transition.
        respond(
            &mut stream,
            "503 Service Unavailable",
            r#"{"error":"ws unavailable"}"#,
        )
        .await;
    }
}

#[tokio::test]
async fn edgeless_host_claims_unknown_chats() {
    let dir = tempfile::tempdir().unwrap();
    let host = open_host(dir.path(), None);
    assert!(host.is_host("never-seen-chat"));
}

#[tokio::test]
async fn edged_host_is_fail_closed_before_first_registry_sync() {
    let dir = tempfile::tempdir().unwrap();
    // Unroutable edge: the registry join keeps failing, synced_once stays false.
    let edge = zeron_engine::EdgeConfig::with_static_token("http://127.0.0.1:1", "alice");
    let host = open_host(dir.path(), Some(edge));
    assert!(
        !host.is_host("never-seen-chat"),
        "unknown chat must not be claimable before the first registry sync"
    );
}

#[tokio::test]
async fn edged_host_opens_claim_gate_only_after_remote_pull() {
    let dir = tempfile::tempdir().unwrap();
    let mut edge = GatedRegistryEdge::start().await;
    let config = zeron_engine::EdgeConfig::with_static_token(&edge.url, "alice");
    let host = open_host(dir.path(), Some(config));
    // Subscribe synchronously after open (before its spawned join task runs).
    // The first publish proves the local-first client has been installed.
    let mut devices = host.watch_devices();

    edge.wait_for_fetch().await;
    tokio::time::timeout(Duration::from_secs(5), devices.changed())
        .await
        .expect("registry client was not installed")
        .expect("host stays alive");
    assert!(
        !host.is_host("unknown-before-pull"),
        "HTTP connect return and push ack must not open the ownership gate"
    );

    edge.release_fetch();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !host.is_host("unknown-after-pull") || host.chat("foreign-chat").unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("successful remote pull did not open the ownership gate");
    let foreign = host.chat("foreign-chat").unwrap().unwrap();
    assert_eq!(foreign.device_id, "dev-b");
    assert!(!host.is_host("foreign-chat"));
}

#[tokio::test(flavor = "multi_thread")]
async fn first_registry_sync_redrains_commands_parked_by_claim_gate() {
    let dir = tempfile::tempdir().unwrap();
    let mut edge = GatedRegistryEdge::start().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = HarnessRegistry::new();
    registry.register(Arc::new(CountingHarness { runs: runs.clone() }));
    let config = zeron_engine::EdgeConfig::with_static_token(&edge.url, "alice");
    let core = EngineCore::assemble_with_identity(
        dir.path(),
        Arc::new(registry),
        HarnessId::Mock,
        Some(config),
        "org-test",
        "alice",
    )
    .expect("engine assembles");

    core.doc_host
        .queue_command(
            "pending-before-sync",
            SessionCommandPayload::Run {
                request: RunRequest {
                    prompt: "run after registry readiness".into(),
                    harness: Some(HarnessId::Mock),
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
                },
                message_id: "m-before-sync".into(),
            },
        )
        .expect("command queues locally");

    edge.wait_for_fetch().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "fail-closed ownership must park the command before remote state lands"
    );

    edge.release_fetch();
    tokio::time::timeout(Duration::from_secs(5), async {
        while runs.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first registry sync did not re-drain the parked command");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the readiness retry must not double-dispatch"
    );
    core.shutdown().await;
}

#[tokio::test]
async fn row_ownership_is_authoritative_regardless_of_sync_state() {
    let dir = tempfile::tempdir().unwrap();
    let edge = zeron_engine::EdgeConfig::with_static_token("http://127.0.0.1:1", "alice");
    let host = open_host(dir.path(), Some(edge));
    host.create_space("space-1", "dev-me", "/tmp/repo", None, true)
        .expect("space");
    host.create_chat("chat-own", Some("space-1"), None, None, None)
        .expect("own chat");
    host.create_space("space-2", "dev-bob", "/tmp/other", None, true)
        .expect("space");
    host.create_chat("chat-foreign", Some("space-2"), None, None, None)
        .expect("foreign chat");
    assert!(host.is_host("chat-own"));
    assert!(!host.is_host("chat-foreign"));
}

/// Subagent docs (`{parent}--sub--{suffix}`) have no registry row of their
/// own: ownership follows the PARENT chat's row, and a row-less sub doc is
/// never claimable. Without this an edged host claimed every sub doc it merely
/// viewed. A top-level chat whose id merely CONTAINS "--sub--" keeps its own
/// row's authority.
#[tokio::test]
async fn subagent_docs_inherit_parent_ownership_and_never_self_claim() {
    let dir = tempfile::tempdir().unwrap();
    let edge = zeron_engine::EdgeConfig::with_static_token("http://127.0.0.1:1", "alice");
    let host = open_host(dir.path(), Some(edge));
    host.create_space("space-1", "dev-me", "/tmp/repo", None, true)
        .expect("space");
    host.create_chat("parent-own", Some("space-1"), None, None, None)
        .expect("own parent");
    host.create_space("space-2", "dev-bob", "/tmp/other", None, true)
        .expect("space");
    host.create_chat("parent-foreign", Some("space-2"), None, None, None)
        .expect("foreign parent");

    // Sub docs resolve to their parent's device.
    assert!(host.is_host("parent-own--sub--call_abc"));
    assert!(!host.is_host("parent-foreign--sub--call_xyz"));
    // Nested sub docs resolve to the nearest registered ancestor (here the
    // top-level parent, since the middle level has no row).
    assert!(host.is_host("parent-own--sub--call_abc--sub--nested"));
    assert!(!host.is_host("parent-foreign--sub--call_a--sub--call_b"));
    // A sub doc whose parent has no row is NOT claimable (even edged-and-synced
    // would still fail-closed here) — the row-less-claim path is off for subs.
    assert!(!host.is_host("ghost-parent--sub--call_1"));

    // A real top-level chat that merely contains the delimiter keeps its own
    // row's authority — the exact row wins over ancestor resolution.
    host.create_chat("weird--sub--name", Some("space-1"), None, None, None)
        .expect("weird own chat");
    assert!(host.is_host("weird--sub--name"));
    host.create_chat("weird--sub--foreign", Some("space-2"), None, None, None)
        .expect("weird foreign chat");
    assert!(!host.is_host("weird--sub--foreign"));
    // A subagent OF that delimiter-containing real chat resolves to the real
    // chat's row (nearest ancestor), not the stripped-to-first-segment "weird".
    assert!(host.is_host("weird--sub--name--sub--call_z"));
}
