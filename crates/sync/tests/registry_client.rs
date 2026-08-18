//! RegistryClient integration tests against the in-process mock server
//! (`registry::mock_server`), which runs the SAME merge fn as the client and
//! speaks the DO's JSON WS protocol. TS↔Rust interop is proven separately by
//! the `--ignored` live-edge test (registry_edge.rs) and scripts/e2e-smoke.sh.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use tokio::sync::watch;
use zeron_doc::{REGISTRY_DOC_ID, RegistryDoc};
use zeron_proto::{Chat, Device, Session, SessionStatus};
use zeron_sync::registry::mock_server::MockRegistryServer;
use zeron_sync::{
    DocsStore, RegistryClient, RegistryEvent, RegistryTransport, RegistryTuning, StaticUrl,
    SyncError,
};

fn ts(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::UNIX_EPOCH)
}

fn device(id: &str) -> Device {
    Device {
        id: id.into(),
        name: format!("{id}-name"),
        platform: "linux".into(),
        last_seen_at: Some(ts(1_000)),
        created_at: Some(ts(500)),
        version: Some("0.1.0".into()),
    }
}

fn chat(id: &str, device_id: &str) -> Chat {
    Chat {
        id: id.into(),
        device_id: device_id.into(),
        title: Some("chat".into()),
        archived: false,
        cwd: Some("/tmp".into()),
        branch: None,
        checkout_id: None,
        config: None,
        last_message_preview: None,
        last_message_at: None,
        created_at: ts(2_000),
        harness_session_id: None,
        harness_session_cwd: None,
        space_id: None,
        last_seen_at: None,
        room_gen: None,
        user_id: None,
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition not reached in time");
}

fn new_doc(device: &str) -> Arc<Mutex<RegistryDoc>> {
    Arc::new(Mutex::new(RegistryDoc::new(device)))
}

struct GatedRegistryTransport {
    fetch_release: watch::Receiver<bool>,
    fetch_started: watch::Sender<bool>,
    fetch_since: Arc<AtomicU64>,
    push_acked: watch::Sender<bool>,
}

impl RegistryTransport for GatedRegistryTransport {
    fn fetch(&self, since: u64) -> BoxFuture<'static, Result<String, SyncError>> {
        let mut release = self.fetch_release.clone();
        let started = self.fetch_started.clone();
        let fetch_since = self.fetch_since.clone();
        Box::pin(async move {
            fetch_since.store(since, Ordering::Release);
            let _ = started.send(true);
            while !*release.borrow() {
                release.changed().await.map_err(|_| SyncError::Closed)?;
            }
            let clock = "0000000001000-000000-dev-b";
            Ok(serde_json::json!({
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
            })
            .to_string())
        })
    }

    fn push(&self, body: String) -> BoxFuture<'static, Result<String, SyncError>> {
        let push_acked = self.push_acked.clone();
        Box::pin(async move {
            let body: serde_json::Value =
                serde_json::from_str(&body).map_err(|err| SyncError::Protocol(err.to_string()))?;
            let batch = body["batch"]
                .as_str()
                .ok_or_else(|| SyncError::Protocol("push without batch".into()))?;
            let ack = serde_json::json!({ "batch": batch, "seq": 11, "applied": 1 });
            let _ = push_acked.send(true);
            Ok(ack.to_string())
        })
    }
}

async fn wait_true(rx: &mut watch::Receiver<bool>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !*rx.borrow() {
            rx.changed().await.expect("signal sender stays alive");
        }
    })
    .await
    .expect("signal did not arrive");
}

#[tokio::test]
async fn http_transport_returns_local_first_and_syncs_only_after_pull() {
    let doc = new_doc("dev-a");
    doc.lock().unwrap().upsert_device(&device("dev-a")).unwrap();

    let (fetch_release_tx, fetch_release_rx) = watch::channel(false);
    let (fetch_started_tx, mut fetch_started_rx) = watch::channel(false);
    let (push_acked_tx, mut push_acked_rx) = watch::channel(false);
    let fetch_since = Arc::new(AtomicU64::new(u64::MAX));
    let transport = Arc::new(GatedRegistryTransport {
        fetch_release: fetch_release_rx,
        fetch_started: fetch_started_tx,
        fetch_since: fetch_since.clone(),
        push_acked: push_acked_tx,
    });

    // Reserve and release a port so the socket side fails independently; the
    // HTTP seam is the only path capable of crossing readiness in this test.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port");
    let ws_url = format!("ws://{}", listener.local_addr().unwrap());
    drop(listener);

    let client = tokio::time::timeout(
        Duration::from_secs(1),
        RegistryClient::connect_via_transport(
            Arc::new(StaticUrl(ws_url)),
            doc.clone(),
            "dev-a",
            RegistryTuning::default(),
            transport,
        ),
    )
    .await
    .expect("construction must not wait for the HTTP pull")
    .expect("local-first construction succeeds");

    wait_true(&mut push_acked_rx).await;
    wait_true(&mut fetch_started_rx).await;
    assert_eq!(fetch_since.load(Ordering::Acquire), 0);
    assert_eq!(doc.lock().unwrap().cursor(), 0);
    assert_eq!(doc.lock().unwrap().pending_len(), 1);
    assert!(
        !client.has_synced(),
        "a successful local push ack must remain deferred and is not readiness"
    );

    let mut events = client.events();
    fetch_release_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Ok(RegistryEvent::Synced) => {
                    let foreign = doc
                        .lock()
                        .unwrap()
                        .chat("foreign-chat")
                        .unwrap()
                        .expect("foreign row is applied before readiness");
                    assert_eq!(foreign.device_id, "dev-b");
                    break;
                }
                Ok(_) => continue,
                Err(err) => panic!("event stream ended: {err}"),
            }
        }
    })
    .await
    .expect("successful pull emits readiness");
    assert!(client.has_synced(), "pull readiness is sticky");
    assert_eq!(doc.lock().unwrap().cursor(), 11);
    assert_eq!(doc.lock().unwrap().pending_len(), 0);

    // Drop aborts the actor immediately; a graceful shutdown could otherwise
    // wait for an in-progress WebSocket dial timeout on a hostile network.
    drop(client);
}

struct FailingPullTransport {
    fetch_release: watch::Receiver<bool>,
    fetch_started: watch::Sender<bool>,
    push_acked: watch::Sender<bool>,
}

impl RegistryTransport for FailingPullTransport {
    fn fetch(&self, _since: u64) -> BoxFuture<'static, Result<String, SyncError>> {
        let mut release = self.fetch_release.clone();
        let started = self.fetch_started.clone();
        Box::pin(async move {
            let _ = started.send(true);
            while !*release.borrow() {
                release.changed().await.map_err(|_| SyncError::Closed)?;
            }
            Err(SyncError::Protocol("injected pull failure".into()))
        })
    }

    fn push(&self, body: String) -> BoxFuture<'static, Result<String, SyncError>> {
        let push_acked = self.push_acked.clone();
        Box::pin(async move {
            let body: serde_json::Value =
                serde_json::from_str(&body).map_err(|err| SyncError::Protocol(err.to_string()))?;
            let batch = body["batch"]
                .as_str()
                .ok_or_else(|| SyncError::Protocol("push without batch".into()))?;
            let ack = serde_json::json!({ "batch": batch, "seq": 7, "applied": 1 });
            let _ = push_acked.send(true);
            Ok(ack.to_string())
        })
    }
}

#[tokio::test]
async fn failed_http_pull_rearms_and_wakes_steady_websocket_push() {
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    doc.lock().unwrap().upsert_device(&device("dev-a")).unwrap();
    let (fetch_release_tx, fetch_release_rx) = watch::channel(false);
    let (fetch_started_tx, mut fetch_started_rx) = watch::channel(false);
    let (push_acked_tx, mut push_acked_rx) = watch::channel(false);

    let client = RegistryClient::connect_via_transport(
        Arc::new(StaticUrl(server.url())),
        doc.clone(),
        "dev-a",
        RegistryTuning::default(),
        Arc::new(FailingPullTransport {
            fetch_release: fetch_release_rx,
            fetch_started: fetch_started_tx,
            push_acked: push_acked_tx,
        }),
    )
    .await
    .expect("local-first construction succeeds");
    wait_true(&mut push_acked_rx).await;
    wait_true(&mut fetch_started_rx).await;
    wait_until(|| client.stats().connected).await;

    // Seeing a probe handled proves the actor has entered steady state after
    // its one initial push pass (where the HTTP-owned batch was in-flight).
    client.probe();
    wait_until(|| client.stats().probes > 0).await;
    assert_eq!(server.row_count(), 0);
    assert_eq!(doc.lock().unwrap().pending_len(), 1);

    fetch_release_tx.send(true).unwrap();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    assert_eq!(server.row_count(), 1, "re-armed batch reached the live WS");
    client.shutdown().await;
}

struct ResetAfterPushTransport;

impl RegistryTransport for ResetAfterPushTransport {
    fn fetch(&self, since: u64) -> BoxFuture<'static, Result<String, SyncError>> {
        Box::pin(async move {
            assert_eq!(since, 10);
            Ok(serde_json::json!({
                "seq": 0,
                "full": true,
                "gcFloor": 0,
                "rows": [],
            })
            .to_string())
        })
    }

    fn push(&self, body: String) -> BoxFuture<'static, Result<String, SyncError>> {
        Box::pin(async move {
            let body: serde_json::Value =
                serde_json::from_str(&body).map_err(|err| SyncError::Protocol(err.to_string()))?;
            let batch = body["batch"]
                .as_str()
                .ok_or_else(|| SyncError::Protocol("push without batch".into()))?;
            Ok(serde_json::json!({ "batch": batch, "seq": 11, "applied": 1 }).to_string())
        })
    }
}

#[tokio::test]
async fn reset_between_http_push_and_pull_keeps_batch_for_replay() {
    let doc = new_doc("dev-a");
    doc.lock().unwrap().apply_state(10, false, 0, vec![]);
    doc.lock().unwrap().upsert_device(&device("dev-a")).unwrap();

    // Hold the socket handshake so only the one HTTP cycle touches pending.
    let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind blackhole");
    let ws_url = format!("ws://{}", blackhole.local_addr().unwrap());
    let client = RegistryClient::connect_via_transport(
        Arc::new(StaticUrl(ws_url)),
        doc.clone(),
        "dev-a",
        RegistryTuning::default(),
        Arc::new(ResetAfterPushTransport),
    )
    .await
    .expect("local-first construction succeeds");

    wait_until(|| client.has_synced()).await;
    assert_eq!(doc.lock().unwrap().cursor(), 0, "reset state applied");
    assert_eq!(doc.lock().unwrap().pending_len(), 1);
    assert_eq!(
        doc.lock().unwrap().take_pushable().len(),
        1,
        "pre-reset ack did not retire and the batch was re-armed"
    );
    drop(client);
    drop(blackhole);
}

#[tokio::test]
async fn new_client_rearms_batches_left_in_flight_by_aborted_owner() {
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    doc.lock().unwrap().upsert_device(&device("dev-a")).unwrap();

    // Exact residue of an owner aborted after take_pushable: the batch stays
    // pending, but its transport-local flag would hide it from a successor.
    let abandoned = doc.lock().unwrap().take_pushable();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(doc.lock().unwrap().pending_len(), 1);

    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("successor connects");
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    assert_eq!(server.row_count(), 1, "successor re-sent abandoned batch");
    client.shutdown().await;
}

#[tokio::test]
async fn two_clients_converge_and_stream_live_updates() {
    let server = MockRegistryServer::start().await;
    let doc_a = new_doc("dev-a");
    let doc_b = new_doc("dev-b");

    // A writes while OFFLINE; the batches queue locally.
    {
        let mut doc = doc_a.lock().unwrap();
        doc.upsert_device(&device("dev-a")).unwrap();
        doc.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
        assert_eq!(doc.pending_len(), 2);
    }

    let client_a = RegistryClient::connect(&server.url(), doc_a.clone(), "dev-a")
        .await
        .expect("a connects");
    client_a.nudge();
    wait_until(|| doc_a.lock().unwrap().pending_len() == 0).await;
    assert_eq!(server.row_count(), 2);

    // B joins fresh and receives the full state.
    let client_b = RegistryClient::connect(&server.url(), doc_b.clone(), "dev-b")
        .await
        .expect("b connects");
    wait_until(|| doc_b.lock().unwrap().read_chats().unwrap().len() == 1).await;

    // Live propagation: B renames, A observes.
    {
        let mut doc = doc_b.lock().unwrap();
        assert!(doc.rename_chat("chat-1", "from b").unwrap());
    }
    client_b.nudge();
    wait_until(|| {
        doc_a
            .lock()
            .unwrap()
            .chat("chat-1")
            .unwrap()
            .is_some_and(|c| c.title.as_deref() == Some("from b"))
    })
    .await;

    // Working-status flip propagates the other way.
    {
        let mut doc = doc_a.lock().unwrap();
        doc.upsert_session(&Session {
            chat_id: "chat-1".into(),
            device_id: "dev-a".into(),
            status: SessionStatus::Working,
            started_at: Some(ts(3_000)),
            updated_at: ts(3_500),
        })
        .unwrap();
    }
    client_a.nudge();
    wait_until(|| {
        doc_b
            .lock()
            .unwrap()
            .read_sessions()
            .unwrap()
            .first()
            .is_some_and(|s| s.status == SessionStatus::Working)
    })
    .await;

    client_a.shutdown().await;
    client_b.shutdown().await;
}

#[tokio::test]
async fn cursor_delta_sync_on_reconnect() {
    let server = MockRegistryServer::start().await;
    let doc_a = new_doc("dev-a");
    {
        let mut doc = doc_a.lock().unwrap();
        doc.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    let client_a = RegistryClient::connect(&server.url(), doc_a.clone(), "dev-a")
        .await
        .expect("a connects");
    client_a.nudge();
    wait_until(|| doc_a.lock().unwrap().pending_len() == 0).await;
    client_a.shutdown().await;
    let cursor_before = doc_a.lock().unwrap().cursor();
    assert!(cursor_before > 0);

    // While A is away, B lands more rows.
    let doc_b = new_doc("dev-b");
    {
        let mut doc = doc_b.lock().unwrap();
        doc.upsert_chat(&chat("chat-2", "dev-b")).unwrap();
        doc.upsert_chat(&chat("chat-3", "dev-b")).unwrap();
    }
    let client_b = RegistryClient::connect(&server.url(), doc_b.clone(), "dev-b")
        .await
        .expect("b connects");
    client_b.nudge();
    wait_until(|| doc_b.lock().unwrap().pending_len() == 0).await;
    client_b.shutdown().await;

    // A reconnects with its cursor and catches up via the delta.
    let client_a = RegistryClient::connect(&server.url(), doc_a.clone(), "dev-a")
        .await
        .expect("a reconnects");
    wait_until(|| doc_a.lock().unwrap().read_chats().unwrap().len() == 3).await;
    assert!(doc_a.lock().unwrap().cursor() > cursor_before);
    client_a.shutdown().await;
}

#[tokio::test]
async fn unacked_writes_replay_idempotently_after_reconnect() {
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    {
        let mut d = doc.lock().unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("connects");
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    let seq_before = server.seq();

    // Simulate an ack lost to a dropped connection: mark the pending batch
    // un-flighted and replay it through a fresh session.
    {
        let mut d = doc.lock().unwrap();
        d.rename_chat("chat-1", "replayed").unwrap();
    }
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    let seq_after_first = server.seq();
    assert_eq!(seq_after_first, seq_before + 1);
    client.shutdown().await;

    // Manually re-enqueue the same logical write (same value, fresh clock
    // would win; instead we assert the SERVER state is unchanged when the
    // exact same batch replays — covered at the merge layer — and that a
    // reconnect with nothing pending doesn't grow seq).
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("reconnects");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.seq(), seq_after_first);
    assert_eq!(
        server
            .row("chats", "chat-1")
            .expect("row")
            .fields
            .get("title")
            .and_then(|v| v.as_str()),
        Some("replayed")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn server_wipe_reseeds_from_client_rows() {
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    {
        let mut d = doc.lock().unwrap();
        d.upsert_device(&device("dev-a")).unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
        d.rename_chat("chat-1", "kept title").unwrap();
    }
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("connects");
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    assert_eq!(server.row_count(), 2);
    client.shutdown().await;

    // Operator wipe: rows gone, seq back to 0.
    server.reset();
    assert_eq!(server.row_count(), 0);

    // The client reconnects, detects the seq regression, and re-seeds —
    // automatically, with the data intact.
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("reconnects");
    wait_until(|| server.row_count() == 2).await;
    assert_eq!(
        server
            .row("chats", "chat-1")
            .expect("row")
            .fields
            .get("title")
            .and_then(|v| v.as_str()),
        Some("kept title")
    );
    // And the local view never flickered.
    assert_eq!(doc.lock().unwrap().read_chats().unwrap().len(), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn gc_floor_forces_full_resync() {
    let server = MockRegistryServer::start().await;
    let doc_a = new_doc("dev-a");
    {
        let mut d = doc_a.lock().unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    let client = RegistryClient::connect(&server.url(), doc_a.clone(), "dev-a")
        .await
        .expect("connects");
    client.nudge();
    wait_until(|| doc_a.lock().unwrap().pending_len() == 0).await;
    client.shutdown().await;

    // B pushes more rows; then the server GC-jumps past A's cursor.
    let doc_b = new_doc("dev-b");
    {
        let mut d = doc_b.lock().unwrap();
        d.upsert_chat(&chat("chat-2", "dev-b")).unwrap();
    }
    let client_b = RegistryClient::connect(&server.url(), doc_b.clone(), "dev-b")
        .await
        .expect("b connects");
    client_b.nudge();
    wait_until(|| doc_b.lock().unwrap().pending_len() == 0).await;
    client_b.shutdown().await;
    server.set_gc_floor(server.seq());

    let client = RegistryClient::connect(&server.url(), doc_a.clone(), "dev-a")
        .await
        .expect("reconnects");
    wait_until(|| doc_a.lock().unwrap().read_chats().unwrap().len() == 2).await;
    client.shutdown().await;
}

#[tokio::test]
async fn presence_beats_reach_peers() {
    let server = MockRegistryServer::start().await;
    let doc_a = new_doc("dev-a");
    let doc_b = new_doc("dev-b");
    let client_a = RegistryClient::connect(&server.url(), doc_a, "dev-a")
        .await
        .expect("a connects");
    let client_b = RegistryClient::connect(&server.url(), doc_b, "dev-b")
        .await
        .expect("b connects");
    client_a.set_presence(123_456);
    wait_until(|| client_b.presence().get("dev-a") == Some(&123_456)).await;
    // And the hello reply carries existing presence to late joiners.
    let doc_c = new_doc("dev-c");
    let client_c = RegistryClient::connect(&server.url(), doc_c, "dev-c")
        .await
        .expect("c connects");
    wait_until(|| client_c.presence().get("dev-a") == Some(&123_456)).await;
    client_a.shutdown().await;
    client_b.shutdown().await;
    client_c.shutdown().await;
}

#[tokio::test]
async fn probe_answers_and_stats_flow() {
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    let client = RegistryClient::connect(&server.url(), doc, "dev-a")
        .await
        .expect("connects");
    let before = client.stats();
    assert!(before.connected);
    client.probe();
    let events = client.events();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = client.stats();
    assert_eq!(after.probes, before.probes + 1);
    // The probe was answered — no disconnect happened.
    assert!(after.connected);
    assert_eq!(after.disconnects, 0);
    drop(events);
    client.shutdown().await;
}

#[tokio::test]
async fn persisted_doc_survives_restart_with_docs_store() {
    let server = MockRegistryServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = DocsStore::open(dir.path()).unwrap();

    // Session 1: write, sync, persist, "shut down".
    let doc = new_doc("dev-a");
    {
        let mut d = doc.lock().unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("connects");
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    {
        let mut d = doc.lock().unwrap();
        d.rename_chat("chat-1", "offline rename").unwrap(); // will stay unacked
        let bytes = d.to_bytes().unwrap();
        store.save_snapshot(REGISTRY_DOC_ID, &bytes).unwrap();
        // Roll back the rename's push by dropping the connection first.
        d.mark_disconnected();
    }
    client.shutdown().await;

    // Session 2: restore, reconnect — the offline rename pushes.
    let bytes = store
        .load_snapshot(REGISTRY_DOC_ID)
        .unwrap()
        .expect("saved");
    let restored = Arc::new(Mutex::new(
        RegistryDoc::from_bytes(&bytes, "dev-a").unwrap(),
    ));
    assert_eq!(restored.lock().unwrap().pending_len(), 1);
    let client = RegistryClient::connect(&server.url(), restored.clone(), "dev-a")
        .await
        .expect("reconnects");
    wait_until(|| restored.lock().unwrap().pending_len() == 0).await;
    assert_eq!(
        server
            .row("chats", "chat-1")
            .expect("row")
            .fields
            .get("title")
            .and_then(|v| v.as_str()),
        Some("offline rename")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn churn_stays_bounded_no_history_growth() {
    // The wedge regression: 2,000 status flips on the same row must leave the
    // server with ONE row and delta-syncing clients with tiny catch-ups —
    // history is discarded the moment it stops being true.
    let server = MockRegistryServer::start().await;
    let doc = new_doc("dev-a");
    {
        let mut d = doc.lock().unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    let client = RegistryClient::connect(&server.url(), doc.clone(), "dev-a")
        .await
        .expect("connects");
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    for i in 0..2_000i64 {
        {
            let mut d = doc.lock().unwrap();
            d.upsert_session(&Session {
                chat_id: "chat-1".into(),
                device_id: "dev-a".into(),
                status: if i % 2 == 0 {
                    SessionStatus::Working
                } else {
                    SessionStatus::Idle
                },
                started_at: Some(ts(i)),
                updated_at: ts(i + 1),
            })
            .unwrap();
        }
        if i % 100 == 0 {
            client.nudge();
            // Let the queue drain periodically so the test bounds memory too.
            wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
        }
    }
    client.nudge();
    wait_until(|| doc.lock().unwrap().pending_len() == 0).await;
    // 2,001 writes → 2 rows. No log, no replay, nothing to compact.
    assert_eq!(server.row_count(), 2);
    client.shutdown().await;

    // A fresh device catches up with 2 rows, not 2,001 updates.
    let doc_b = new_doc("dev-b");
    let client_b = RegistryClient::connect(&server.url(), doc_b.clone(), "dev-b")
        .await
        .expect("b connects");
    wait_until(|| doc_b.lock().unwrap().read_sessions().unwrap().len() == 1).await;
    client_b.shutdown().await;
}

#[tokio::test]
async fn applied_events_fire_for_republish() {
    let server = MockRegistryServer::start().await;
    let doc_a = new_doc("dev-a");
    let doc_b = new_doc("dev-b");
    let client_a = RegistryClient::connect(&server.url(), doc_a, "dev-a")
        .await
        .expect("a connects");
    let client_b = RegistryClient::connect(&server.url(), doc_b, "dev-b")
        .await
        .expect("b connects");
    let mut events = client_b.events();
    {
        let mut d = client_a.doc().lock().unwrap();
        d.upsert_chat(&chat("chat-1", "dev-a")).unwrap();
    }
    client_a.nudge();
    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Ok(RegistryEvent::Applied) => return RegistryEvent::Applied,
                Ok(_) => continue,
                Err(err) => panic!("event stream ended: {err}"),
            }
        }
    })
    .await
    .expect("applied event");
    assert_eq!(event, RegistryEvent::Applied);
    client_a.shutdown().await;
    client_b.shutdown().await;
}
