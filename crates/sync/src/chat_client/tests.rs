//! ChatClient behavior against a hand-driven server end (channel pipes — no
//! WebSocket): handshake precision, backfill, push/ack retirement, and the
//! reconnect re-push path. Virtual clock (`start_paused`) so backoff and
//! deadlines cost nothing.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::*;
use crate::chat_frames::{decode, encode, frame_type};

// ── plumbing: linked pipes + scripted connector ─────────────────────────────

struct ServerEnd {
    tx: mpsc::Sender<Vec<u8>>,
    rx: mpsc::Receiver<Vec<u8>>,
}

fn pipe_pair() -> (BinPipe, ServerEnd) {
    let (c2s_tx, c2s_rx) = mpsc::channel(64);
    let (s2c_tx, s2c_rx) = mpsc::channel(64);
    (
        BinPipe {
            tx: c2s_tx,
            rx: s2c_rx,
        },
        ServerEnd {
            tx: s2c_tx,
            rx: c2s_rx,
        },
    )
}

struct ChanConnector {
    pipes: Mutex<VecDeque<BinPipe>>,
}

impl BinConnector for ChanConnector {
    fn connect(&self) -> BoxFuture<'static, Result<BinPipe, SyncError>> {
        let pipe = lock(&self.pipes).pop_front();
        Box::pin(async move { pipe.ok_or(SyncError::Closed) })
    }
}

struct ErrorConnector {
    error: SyncError,
}

impl BinConnector for ErrorConnector {
    fn connect(&self) -> BoxFuture<'static, Result<BinPipe, SyncError>> {
        let error = self.error.clone();
        Box::pin(async move { Err(error) })
    }
}

struct ErrorTransport {
    error: SyncError,
}

#[derive(Default)]
struct ScriptedTransport {
    push_results: Mutex<VecDeque<Result<String, SyncError>>>,
    fetch_results: Mutex<VecDeque<Result<Vec<u8>, SyncError>>>,
    pushes: Mutex<Vec<(String, Vec<u8>)>>,
    fetch_after: Mutex<Vec<u64>>,
}

impl ChatTransport for ScriptedTransport {
    fn fetch_rows(&self, after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        lock(&self.fetch_after).push(after);
        let result = lock(&self.fetch_results)
            .pop_front()
            .unwrap_or(Err(SyncError::Closed));
        Box::pin(async move { result })
    }

    fn push(
        &self,
        batch_id: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>> {
        lock(&self.pushes).push((batch_id, bytes));
        let result = lock(&self.push_results)
            .pop_front()
            .unwrap_or(Err(SyncError::Closed));
        Box::pin(async move { result })
    }
}

struct GatedPullFailureTransport {
    release: Mutex<Option<oneshot::Receiver<()>>>,
    fetch_after: Mutex<Vec<u64>>,
}

struct GatedRowsTransport {
    release: Mutex<Option<oneshot::Receiver<()>>>,
    started: Mutex<Option<oneshot::Sender<()>>>,
    body: Vec<u8>,
    push_results: Mutex<VecDeque<Result<String, SyncError>>>,
}

impl ChatTransport for GatedRowsTransport {
    fn fetch_rows(&self, _after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        let release = lock(&self.release).take().expect("one pull only");
        let started = lock(&self.started).take().expect("one pull only");
        let body = self.body.clone();
        Box::pin(async move {
            let _ = started.send(());
            let _ = release.await;
            Ok(body)
        })
    }

    fn push(
        &self,
        _batch_id: String,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>> {
        let result = lock(&self.push_results)
            .pop_front()
            .unwrap_or(Err(SyncError::Closed));
        Box::pin(async move { result })
    }
}

impl ChatTransport for GatedPullFailureTransport {
    fn fetch_rows(&self, after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        lock(&self.fetch_after).push(after);
        let release = lock(&self.release).take().expect("one pull only");
        Box::pin(async move {
            let _ = release.await;
            Err(SyncError::Protocol("injected pull failure".into()))
        })
    }

    fn push(
        &self,
        batch_id: String,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>> {
        Box::pin(async move {
            Ok(serde_json::json!({"batchId": batch_id, "seq": 1, "dup": false}).to_string())
        })
    }
}

impl ChatTransport for ErrorTransport {
    fn fetch_rows(&self, _after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        let error = self.error.clone();
        Box::pin(async move { Err(error) })
    }

    fn push(
        &self,
        _batch_id: String,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>> {
        let error = self.error.clone();
        Box::pin(async move { Err(error) })
    }
}

// ── sink + fetcher doubles ──────────────────────────────────────────────────

#[derive(Default)]
struct RecordingSink {
    rows: Mutex<Vec<(Vec<u8>, u64)>>,
    checkpoints: Mutex<Vec<(Vec<u8>, u64)>>,
    cursor_advances: Mutex<Vec<u64>>,
    cursor_resets: Mutex<Vec<u64>>,
    frontier_contained: std::sync::atomic::AtomicBool,
    /// Global apply order across rows and checkpoints — the overlap test
    /// pins "checkpoint imports before any row that buffered during it".
    ops: Mutex<Vec<String>>,
}

impl ChatDocSink for RecordingSink {
    fn apply_row(&self, bytes: &[u8], cursor: u64) {
        lock(&self.rows).push((bytes.to_vec(), cursor));
        lock(&self.ops).push(format!("row@{cursor}"));
    }
    fn apply_checkpoint(&self, bytes: &[u8], cursor: u64) -> Result<(), String> {
        lock(&self.checkpoints).push((bytes.to_vec(), cursor));
        lock(&self.ops).push(format!("ckpt@{cursor}"));
        Ok(())
    }
    fn contains_frontier(&self, _frontier: &[u8]) -> bool {
        self.frontier_contained
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    fn advance_cursor(&self, cursor: u64) {
        lock(&self.cursor_advances).push(cursor);
    }
    fn reset_cursor(&self, cursor: u64) {
        lock(&self.cursor_resets).push(cursor);
    }
}

struct FixedFetcher {
    bytes: Vec<u8>,
    calls: Arc<std::sync::atomic::AtomicU64>,
}

impl CheckpointFetcher for FixedFetcher {
    fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = self.bytes.clone();
        Box::pin(async move { Ok(bytes) })
    }
}

// ── server-side script helpers ──────────────────────────────────────────────

async fn expect_kind(end: &mut ServerEnd, kind: u8) -> wire::WireFrame {
    loop {
        let bytes = end.rx.recv().await.expect("client hung up");
        let frame = decode(&bytes).expect("client sent undecodable frame");
        if frame.kind == kind {
            return frame;
        }
        panic!("expected frame {kind:#x}, got {:#x}", frame.kind);
    }
}

async fn send(end: &ServerEnd, kind: u8, header: serde_json::Value, payload: &[u8]) {
    end.tx.send(encode(kind, &header, payload)).await.unwrap();
}

fn http_rows_body(frames: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut body = Vec::new();
    for frame in frames {
        body.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        body.extend_from_slice(&frame);
    }
    body
}

fn http_rows_response(head_seq: u64, rows: &[(u64, &str, &[u8])]) -> Vec<u8> {
    let mut frames = vec![encode(
        frame_type::STATE,
        &serde_json::json!({
            "headSeq": head_seq,
            "seqFloor": 0,
            "checkpointSeq": 0,
            "checkpointSize": 0,
            "rowCount": rows.len(),
            "rowBytes": rows.iter().map(|(_, _, bytes)| bytes.len()).sum::<usize>(),
        }),
        &[],
    )];
    for (seq, batch_id, payload) in rows {
        frames.push(encode(
            frame_type::ROW,
            &serde_json::json!({"seq": seq, "device": "dev-b", "batchId": batch_id}),
            payload,
        ));
    }
    frames.push(encode(
        frame_type::ROWS_DONE,
        &serde_json::json!({"headSeq": head_seq}),
        &[],
    ));
    http_rows_body(frames)
}

/// Answer hello with `state`, then serve the rows request with `rows`.
/// Returns the observed `after` from the rows request. `expect_exclude`
/// pins the F1 rule: the process's FIRST backfill must redownload own rows
/// (false), same-process reconnects skip them (true).
async fn serve_join(
    end: &mut ServerEnd,
    state: serde_json::Value,
    frontier: &[u8],
    rows: Vec<(u64, &str, Vec<u8>)>,
    expect_exclude: bool,
) -> u64 {
    let hello = expect_kind(end, frame_type::HELLO).await;
    assert!(hello.header["device"].is_string());
    let head_seq = state["headSeq"].as_u64().unwrap();
    send(end, frame_type::STATE, state, frontier).await;
    let req = expect_kind(end, frame_type::ROWS_REQ).await;
    assert_eq!(req.header["excludeOwn"], expect_exclude);
    let after = req.header["after"].as_u64().unwrap();
    for (seq, device, bytes) in rows {
        send(
            end,
            frame_type::ROW,
            serde_json::json!({"seq": seq, "device": device, "batchId": format!("b{seq}")}),
            &bytes,
        )
        .await;
    }
    send(
        end,
        frame_type::ROWS_DONE,
        serde_json::json!({"headSeq": head_seq}),
        &[],
    )
    .await;
    after
}

fn connector(pipes: Vec<BinPipe>) -> Arc<ChanConnector> {
    Arc::new(ChanConnector {
        pipes: Mutex::new(pipes.into_iter().collect()),
    })
}

fn fetcher(bytes: &[u8]) -> (Arc<FixedFetcher>, Arc<std::sync::atomic::AtomicU64>) {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    (
        Arc::new(FixedFetcher {
            bytes: bytes.to_vec(),
            calls: calls.clone(),
        }),
        calls,
    )
}

fn frame_actor(shared: Arc<Mutex<Shared>>, sink: Arc<RecordingSink>) -> Actor {
    let (fetch, _) = fetcher(b"");
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (nudge, nudge_rx) = mpsc::channel(1);
    let (_probe, probe_rx) = mpsc::channel(1);
    let (_redial, redial_rx) = mpsc::channel(1);
    let (_presence, presence_rx) = mpsc::channel(1);
    let (events, _) = broadcast::channel(1);
    Actor {
        shared,
        sink,
        fetcher: fetch,
        device_id: "dev-a".into(),
        connector: Arc::new(ErrorConnector {
            error: SyncError::Closed,
        }),
        tuning: ChatTuning::default(),
        events,
        shutdown: shutdown_rx,
        nudge,
        nudge_rx,
        probe_rx,
        redial_rx,
        presence_rx,
        flags: Arc::new(Flags::default()),
        cursor_amnesty_done: std::sync::atomic::AtomicBool::new(false),
        transport: None,
        sync_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        resumed: true,
    }
}

fn ws_http_error(status: u16) -> WsError {
    let response = tokio_tungstenite::tungstenite::http::Response::builder()
        .status(status)
        .body(Some(Vec::new()))
        .unwrap();
    WsError::Http(response)
}

// ── plan_catch_up (pure) ────────────────────────────────────────────────────

#[test]
fn websocket_403_is_typed_without_reclassifying_other_handshake_failures() {
    assert!(matches!(
        map_ws_connect_error(ws_http_error(403)),
        SyncError::AccessDenied(_)
    ));
    assert!(matches!(
        map_ws_connect_error(ws_http_error(401)),
        SyncError::WebSocket(_)
    ));
    assert!(matches!(
        map_ws_connect_error(ws_http_error(500)),
        SyncError::WebSocket(_)
    ));
}

#[tokio::test]
async fn access_denied_signal_is_sticky_and_broadcasts_once() {
    let flags = Flags::default();
    let (events, _) = broadcast::channel(4);
    let mut receiver = events.subscribe();

    signal_access_denied(&flags, &events);
    assert!(
        flags
            .access_denied
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(receiver.recv().await.unwrap(), ChatEvent::AccessDenied);

    signal_access_denied(&flags, &events);
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test(start_paused = true)]
async fn local_first_websocket_denial_is_visible_to_a_late_subscriber() {
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let client = ChatClient::connect_with_transport(
        Arc::new(ErrorConnector {
            error: SyncError::AccessDenied("websocket handshake HTTP 403".into()),
        }),
        sink,
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
        Some(Arc::new(ErrorTransport {
            error: SyncError::Closed,
        })),
    )
    .await
    .expect("local-first construction succeeds before network I/O");

    for _ in 0..16 {
        if client.access_denied() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        client.access_denied(),
        "403 verdict must latch on the client"
    );

    // Subscribe deliberately after the one-shot event. The accessor is the
    // race-free source of truth for this pre-subscription case.
    let _late_receiver = client.events();
    assert!(client.access_denied());
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn pending_updates_can_be_recovered_before_parking_the_client() {
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let client = ChatClient::connect_with_transport(
        Arc::new(ErrorConnector {
            error: SyncError::Closed,
        }),
        sink,
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
        Some(Arc::new(ErrorTransport {
            error: SyncError::Closed,
        })),
    )
    .await
    .unwrap();
    client.enqueue_update(vec![1, 2]);
    client.enqueue_update(vec![3, 4]);

    assert_eq!(client.into_pending_updates(), vec![vec![1, 2], vec![3, 4]]);
}

#[tokio::test]
async fn offline_push_pulls_from_the_pre_ack_cursor_before_retiring() {
    let transport = Arc::new(ScriptedTransport::default());
    lock(&transport.push_results).push_back(Ok(
        serde_json::json!({"batchId": "local", "seq": 2, "dup": false}).to_string(),
    ));
    lock(&transport.fetch_results).push_back(Ok(http_rows_response(
        2,
        &[(1, "foreign", b"foreign-row"), (2, "local", b"local-row")],
    )));
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 0,
        pending: VecDeque::from([PendingPush {
            batch_id: "local".into(),
            bytes: b"local-row".to_vec(),
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, mut nudge_rx) = mpsc::channel(1);

    offline_sync_once(
        transport.clone(),
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge.clone(),
    )
    .await;

    assert_eq!(*lock(&transport.fetch_after), vec![0]);
    assert_eq!(
        *lock(&sink.rows),
        vec![(b"foreign-row".to_vec(), 1), (b"local-row".to_vec(), 2)],
        "the foreign predecessor must land before the local ack retires"
    );
    let shared = lock(&shared);
    assert_eq!(shared.cursor, 2);
    assert!(shared.pending.is_empty());
    drop(shared);
    assert_eq!(*lock(&sink.cursor_advances), vec![2]);
    assert!(matches!(
        nudge_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn offline_push_followed_by_server_reset_keeps_pending_and_resets_cursor() {
    let transport = Arc::new(ScriptedTransport::default());
    lock(&transport.push_results).push_back(Ok(
        serde_json::json!({"batchId": "pre-reset", "seq": 42, "dup": false}).to_string(),
    ));
    // This GET was correctly issued with after=41. Once the room reports
    // head=1, it cannot contain rows for the new namespace in this response;
    // the self-nudge retries from the exact reset cursor.
    lock(&transport.fetch_results).push_back(Ok(http_rows_response(1, &[])));
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 41,
        pending: VecDeque::from([PendingPush {
            batch_id: "pre-reset".into(),
            bytes: vec![9],
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, mut nudge_rx) = mpsc::channel(1);
    let flags = Arc::new(Flags::default());

    offline_sync_once(
        transport.clone(),
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        flags.clone(),
        nudge,
    )
    .await;

    assert_eq!(*lock(&transport.fetch_after), vec![41]);
    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1, "pre-reset ack stays replayable");
    drop(shared);
    assert_eq!(*lock(&sink.cursor_resets), vec![0]);
    assert!(lock(&sink.cursor_advances).is_empty());
    assert_eq!(
        flags
            .server_resets
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    nudge_rx
        .recv()
        .await
        .expect("reset must schedule a fresh pull/push");
}

#[tokio::test]
async fn websocket_ack_racing_a_failed_http_push_is_staged_until_reset_pull() {
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 0,
        pending: VecDeque::from([PendingPush {
            batch_id: "racing".into(),
            bytes: b"must-replay".to_vec(),
        }]),
        ..Shared::default()
    }));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedRowsTransport {
        release: Mutex::new(Some(release_rx)),
        started: Mutex::new(Some(started_tx)),
        body: http_rows_response(0, &[]),
        push_results: Mutex::new(VecDeque::from([Err(SyncError::Protocol(
            "injected push failure".into(),
        ))])),
    });
    let sink = Arc::new(RecordingSink::default());
    let actor = frame_actor(shared.clone(), sink.clone());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let task = tokio::spawn(offline_sync_once(
        transport,
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    ));

    started_rx.await.unwrap();
    // Exact race under audit: HTTP could not confirm its POST, while the old
    // socket's ack lands after the round captured its replay snapshot and
    // before the GET exposes the replacement room's empty head.
    let ack = decode(&encode(
        frame_type::ACK,
        &serde_json::json!({"batchId": "racing", "seq": 1, "dup": false}),
        &[],
    ))
    .unwrap();
    assert!(actor.handle_frame(ack, 0));
    {
        let shared = lock(&shared);
        assert_eq!(shared.cursor, 0, "a staged ack is not a pull frontier");
        assert_eq!(shared.pending.len(), 1, "keep the only replay copy");
        assert_eq!(shared.offline_ws_acks.get("racing"), Some(&1));
    }
    assert!(lock(&sink.cursor_advances).is_empty());
    release_tx.send(()).unwrap();
    task.await.unwrap();

    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    assert_eq!(shared.pending[0].batch_id, "racing");
    assert_eq!(shared.pending[0].bytes, b"must-replay");
    assert!(shared.offline_snapshot_ids.is_empty());
    assert!(shared.offline_ws_acks.is_empty());
    assert!(shared.permanent_rejections.is_empty());
    drop(shared);
    assert!(lock(&sink.cursor_advances).is_empty());
    assert_eq!(*lock(&sink.cursor_resets), vec![0]);
}

#[tokio::test]
async fn aba_ack_reset_reanchors_from_zero_instead_of_the_captured_cursor() {
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 10,
        pending: VecDeque::from([PendingPush {
            batch_id: "aba-race".into(),
            bytes: b"must-replay".to_vec(),
        }]),
        ..Shared::default()
    }));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedRowsTransport {
        release: Mutex::new(Some(release_rx)),
        started: Mutex::new(Some(started_tx)),
        body: http_rows_response(
            12,
            &[(11, "new-11", b"new-row-11"), (12, "new-12", b"new-row-12")],
        ),
        push_results: Mutex::new(VecDeque::from([Err(SyncError::Protocol(
            "injected push failure".into(),
        ))])),
    });
    let sink = Arc::new(RecordingSink::default());
    let actor = frame_actor(shared.clone(), sink.clone());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let task = tokio::spawn(offline_sync_once(
        transport,
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    ));

    started_rx.await.unwrap();
    let ack = decode(&encode(
        frame_type::ACK,
        &serde_json::json!({"batchId": "aba-race", "seq": 13, "dup": false}),
        &[],
    ))
    .unwrap();
    assert!(actor.handle_frame(ack, 0));
    assert_eq!(lock(&shared).offline_ws_acks.get("aba-race"), Some(&13));
    release_tx.send(()).unwrap();
    task.await.unwrap();

    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    assert_eq!(shared.pending[0].batch_id, "aba-race");
    assert_eq!(shared.pending[0].bytes, b"must-replay");
    drop(shared);
    assert!(
        lock(&sink.rows).is_empty(),
        "rows 11..12 cannot fill the new incarnation's missing 1..10"
    );
    assert_eq!(*lock(&sink.cursor_resets), vec![0]);
}

#[tokio::test]
async fn ack_sequence_reused_by_a_foreign_batch_forces_zero_reanchor() {
    let transport = Arc::new(ScriptedTransport::default());
    lock(&transport.push_results).push_back(Ok(
        serde_json::json!({"batchId": "old-local", "seq": 13, "dup": false}).to_string(),
    ));
    lock(&transport.fetch_results).push_back(Ok(http_rows_response(
        14,
        &[
            (11, "new-11", b"new-row-11"),
            (12, "new-12", b"new-row-12"),
            (13, "foreign", b"foreign-row-13"),
            (14, "new-14", b"new-row-14"),
        ],
    )));
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 10,
        pending: VecDeque::from([PendingPush {
            batch_id: "old-local".into(),
            bytes: b"must-replay".to_vec(),
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);

    offline_sync_once(
        transport.clone(),
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    )
    .await;

    assert_eq!(*lock(&transport.fetch_after), vec![10]);
    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    assert_eq!(shared.pending[0].batch_id, "old-local");
    drop(shared);
    assert!(lock(&sink.rows).is_empty());
    assert_eq!(*lock(&sink.cursor_resets), vec![0]);
}

#[tokio::test]
async fn contained_checkpoint_does_not_prove_a_staged_ack_identity() {
    let body = http_rows_body([
        encode(
            frame_type::STATE,
            &serde_json::json!({
                "headSeq": 14,
                "seqFloor": 12,
                "checkpointSeq": 12,
                "checkpointSize": 128,
                "rowCount": 2,
                "rowBytes": 20,
            }),
            b"contained-frontier",
        ),
        encode(
            frame_type::ROW,
            &serde_json::json!({"seq": 13, "device": "dev-b", "batchId": "new-13"}),
            b"new-row-13",
        ),
        encode(
            frame_type::ROW,
            &serde_json::json!({"seq": 14, "device": "dev-b", "batchId": "new-14"}),
            b"new-row-14",
        ),
        encode(
            frame_type::ROWS_DONE,
            &serde_json::json!({"headSeq": 14}),
            &[],
        ),
    ]);
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 10,
        pending: VecDeque::from([PendingPush {
            batch_id: "pruned-old-local".into(),
            bytes: b"must-replay".to_vec(),
        }]),
        ..Shared::default()
    }));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedRowsTransport {
        release: Mutex::new(Some(release_rx)),
        started: Mutex::new(Some(started_tx)),
        body,
        push_results: Mutex::new(VecDeque::from([Err(SyncError::Protocol(
            "injected push failure".into(),
        ))])),
    });
    let sink = Arc::new(RecordingSink::default());
    sink.frontier_contained
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let actor = frame_actor(shared.clone(), sink.clone());
    let (fetch, fetch_calls) = fetcher(b"unused-checkpoint");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let task = tokio::spawn(offline_sync_once(
        transport,
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    ));

    started_rx.await.unwrap();
    let ack = decode(&encode(
        frame_type::ACK,
        &serde_json::json!({
            "batchId": "pruned-old-local",
            "seq": 8,
            "dup": true,
        }),
        &[],
    ))
    .unwrap();
    assert!(actor.handle_frame(ack, 0));
    release_tx.send(()).unwrap();
    task.await.unwrap();

    let shared = lock(&shared);
    assert_eq!(shared.cursor, 12, "only the trusted checkpoint may anchor");
    assert_eq!(
        shared.pending.len(),
        1,
        "missing batch identity must replay"
    );
    assert_eq!(shared.pending[0].batch_id, "pruned-old-local");
    assert!(shared.offline_ws_acks.is_empty());
    drop(shared);
    assert!(lock(&sink.rows).is_empty());
    assert_eq!(*lock(&sink.cursor_resets), vec![12]);
    assert_eq!(
        fetch_calls.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the contained frontier avoids download but does not confirm the ack"
    );
}

#[tokio::test]
async fn pull_failure_after_a_sibling_reset_still_restores_the_round_snapshot() {
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 41,
        pending: VecDeque::from([PendingPush {
            batch_id: "failed-pull".into(),
            bytes: b"must-replay".to_vec(),
        }]),
        ..Shared::default()
    }));
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedPullFailureTransport {
        release: Mutex::new(Some(release_rx)),
        fetch_after: Mutex::new(Vec::new()),
    });
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let task = tokio::spawn(offline_sync_once(
        transport.clone(),
        shared.clone(),
        sink,
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    ));
    while lock(&transport.fetch_after).is_empty() {
        tokio::task::yield_now().await;
    }
    {
        let mut shared = lock(&shared);
        // A sibling accepted the old ack, then independently observed/reset
        // the room before this HTTP pull failed.
        shared.pending.clear();
        shared.cursor = 0;
        shared.reset_version = 1;
    }
    release_tx.send(()).unwrap();
    task.await.unwrap();

    let shared = lock(&shared);
    assert_eq!(shared.pending.len(), 1);
    assert_eq!(shared.pending[0].batch_id, "failed-pull");
    assert_eq!(shared.pending[0].bytes, b"must-replay");
    assert!(shared.offline_snapshot_ids.is_empty());
}

#[tokio::test]
async fn reset_restoration_does_not_resurrect_a_permanently_rejected_head() {
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 41,
        pending: VecDeque::from([
            PendingPush {
                batch_id: "bad-full".into(),
                bytes: b"oversized".to_vec(),
            },
            PendingPush {
                batch_id: "small".into(),
                bytes: b"small-delta".to_vec(),
            },
        ]),
        ..Shared::default()
    }));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedRowsTransport {
        release: Mutex::new(Some(release_rx)),
        started: Mutex::new(Some(started_tx)),
        body: http_rows_response(1, &[]),
        push_results: Mutex::new(VecDeque::from([
            Err(SyncError::PushRejected("too_large".into())),
            Ok(serde_json::json!({"batchId": "small", "seq": 42, "dup": false}).to_string()),
        ])),
    });
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let task = tokio::spawn(offline_sync_once(
        transport,
        shared.clone(),
        sink,
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    ));

    started_rx.await.unwrap();
    {
        let mut shared = lock(&shared);
        assert!(
            shared.permanent_rejections.contains("bad-full"),
            "HTTP verdict must be visible to reset restoration"
        );
        shared.pending.retain(|push| push.batch_id != "small");
        shared.cursor = 42;
    }
    release_tx.send(()).unwrap();
    task.await.unwrap();

    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    assert_eq!(shared.pending[0].batch_id, "small");
    assert_eq!(shared.pending[0].bytes, b"small-delta");
    assert!(shared.offline_snapshot_ids.is_empty());
    assert!(shared.permanent_rejections.is_empty());
}

#[test]
fn stale_websocket_ack_cannot_retire_work_after_http_reset() {
    let shared = Arc::new(Mutex::new(Shared {
        cursor: 0,
        reset_version: 1,
        pending: VecDeque::from([PendingPush {
            batch_id: "pre-reset".into(),
            bytes: vec![9],
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (nudge, nudge_rx) = mpsc::channel(1);
    let (_probe, probe_rx) = mpsc::channel(1);
    let (_redial, redial_rx) = mpsc::channel(1);
    let (_presence, presence_rx) = mpsc::channel(1);
    let (events, _) = broadcast::channel(1);
    let actor = Actor {
        shared: shared.clone(),
        sink: sink.clone(),
        fetcher: fetch,
        device_id: "dev-a".into(),
        connector: Arc::new(ErrorConnector {
            error: SyncError::Closed,
        }),
        tuning: ChatTuning::default(),
        events,
        shutdown: shutdown_rx,
        nudge,
        nudge_rx,
        probe_rx,
        redial_rx,
        presence_rx,
        flags: Arc::new(Flags::default()),
        cursor_amnesty_done: std::sync::atomic::AtomicBool::new(false),
        transport: None,
        sync_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        resumed: true,
    };
    let ack = decode(&encode(
        frame_type::ACK,
        &serde_json::json!({"batchId": "pre-reset", "seq": 42, "dup": false}),
        &[],
    ))
    .unwrap();

    assert!(!actor.handle_frame(ack, 0), "old session must reconnect");
    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    drop(shared);
    assert!(lock(&sink.cursor_advances).is_empty());
}

#[tokio::test]
async fn malformed_http_pull_never_retires_a_staged_ack() {
    let transport = Arc::new(ScriptedTransport::default());
    lock(&transport.push_results).push_back(Ok(
        serde_json::json!({"batchId": "local", "seq": 1, "dup": false}).to_string(),
    ));
    let mut truncated = http_rows_response(1, &[(1, "local", b"row")]);
    truncated.pop();
    lock(&transport.fetch_results).push_back(Ok(truncated));
    let shared = Arc::new(Mutex::new(Shared {
        pending: VecDeque::from([PendingPush {
            batch_id: "local".into(),
            bytes: b"row".to_vec(),
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, mut nudge_rx) = mpsc::channel(1);

    offline_sync_once(
        transport,
        shared.clone(),
        sink.clone(),
        fetch,
        events,
        Arc::new(Flags::default()),
        nudge,
    )
    .await;

    let shared = lock(&shared);
    assert_eq!(shared.cursor, 0);
    assert_eq!(shared.pending.len(), 1);
    drop(shared);
    assert!(lock(&sink.rows).is_empty());
    assert!(lock(&sink.cursor_advances).is_empty());
    nudge_rx.recv().await.expect("invalid pull must retry");
}

#[tokio::test]
async fn permanent_http_rejection_does_not_wedge_the_following_delta() {
    let transport = Arc::new(ScriptedTransport::default());
    lock(&transport.push_results).push_back(Err(SyncError::PushRejected("too_large".into())));
    lock(&transport.push_results).push_back(Ok(
        serde_json::json!({"batchId": "small", "seq": 1, "dup": false}).to_string(),
    ));
    lock(&transport.fetch_results)
        .push_back(Ok(http_rows_response(1, &[(1, "small", b"small-delta")])));
    let shared = Arc::new(Mutex::new(Shared {
        pending: VecDeque::from([
            PendingPush {
                batch_id: "full".into(),
                bytes: vec![0; 8],
            },
            PendingPush {
                batch_id: "small".into(),
                bytes: b"small-delta".to_vec(),
            },
        ]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (events, _) = broadcast::channel(8);
    let (nudge, _) = mpsc::channel(1);
    let flags = Arc::new(Flags::default());

    offline_sync_once(
        transport.clone(),
        shared.clone(),
        sink,
        fetch,
        events,
        flags.clone(),
        nudge,
    )
    .await;

    assert_eq!(
        lock(&transport.pushes)
            .iter()
            .map(|(batch, _)| batch.as_str())
            .collect::<Vec<_>>(),
        vec!["full", "small"]
    );
    assert!(lock(&shared).pending.is_empty());
    assert_eq!(lock(&shared).cursor, 1);
    assert_eq!(flags.rejected.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn failed_http_pull_self_nudges_a_steady_socket_to_repush() {
    let (pipe, mut end) = pipe_pair();
    let shared = Arc::new(Mutex::new(Shared {
        pending: VecDeque::from([PendingPush {
            batch_id: "retry-me".into(),
            bytes: vec![7],
        }]),
        ..Shared::default()
    }));
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (release_tx, release_rx) = oneshot::channel();
    let transport = Arc::new(GatedPullFailureTransport {
        release: Mutex::new(Some(release_rx)),
        fetch_after: Mutex::new(Vec::new()),
    });
    let (events, _) = broadcast::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (nudge_tx, nudge_rx) = mpsc::channel(1);
    let (_probe_tx, probe_rx) = mpsc::channel(1);
    let (_redial_tx, redial_rx) = mpsc::channel(1);
    let (_presence_tx, presence_rx) = mpsc::channel(1);
    let mut actor = Actor {
        shared: shared.clone(),
        sink,
        fetcher: fetch,
        device_id: "dev-a".into(),
        connector: Arc::new(ErrorConnector {
            error: SyncError::Closed,
        }),
        tuning: ChatTuning::default(),
        events,
        shutdown: shutdown_rx,
        nudge: nudge_tx,
        nudge_rx,
        probe_rx,
        redial_rx,
        presence_rx,
        flags: Arc::new(Flags::default()),
        cursor_amnesty_done: std::sync::atomic::AtomicBool::new(false),
        transport: Some(transport),
        sync_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        resumed: false,
    };
    actor.spawn_offline_sync();
    let actor_task = tokio::spawn(async move {
        let mut ready = None;
        actor.run_session(pipe, &mut ready).await
    });

    let _hello = expect_kind(&mut end, frame_type::HELLO).await;
    send(
        &end,
        frame_type::STATE,
        serde_json::json!({"headSeq": 0, "seqFloor": 0, "checkpointSeq": 0,
            "checkpointSize": 0, "rowCount": 0, "rowBytes": 0}),
        &[],
    )
    .await;
    let _rows = expect_kind(&mut end, frame_type::ROWS_REQ).await;
    let first = expect_kind(&mut end, frame_type::PUSH).await;
    send(
        &end,
        frame_type::ROWS_DONE,
        serde_json::json!({"headSeq": 0}),
        &[],
    )
    .await;
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();

    let replay = expect_kind(&mut end, frame_type::PUSH).await;
    assert_eq!(replay.header["batchId"], first.header["batchId"]);
    assert_eq!(replay.payload, first.payload);
    assert_eq!(
        lock(&shared).pending.len(),
        1,
        "failed pull keeps replay copy"
    );

    shutdown_tx.send(true).unwrap();
    drop(end);
    let _ = actor_task.await.unwrap();
}

#[test]
fn catch_up_plan_covers_the_decision_table() {
    let state = |head: u64, ckpt: u64| wire::StateHeader {
        head_seq: head,
        seq_floor: ckpt,
        checkpoint_seq: ckpt,
        checkpoint_size: if ckpt > 0 { 1000 } else { 0 },
        row_count: 0,
        row_bytes: 0,
    };
    // Seeded-at-zero room (M1): checkpointSeq 0 but a real blob — the
    // presence test is SIZE; a fresh reader must fetch the seed.
    let seeded = wire::StateHeader {
        head_seq: 0,
        seq_floor: 0,
        checkpoint_seq: 0,
        checkpoint_size: 276_000,
        row_count: 0,
        row_bytes: 0,
    };
    assert_eq!(
        plan_catch_up(0, &seeded, false),
        CatchUpPlan::CheckpointThenRows { after: 0 }
    );
    assert_eq!(
        plan_catch_up(0, &seeded, true),
        CatchUpPlan::RowsOnly { after: 0 }
    );
    // Empty room / no checkpoint: rows from the cursor.
    assert_eq!(
        plan_catch_up(0, &state(0, 0), true),
        CatchUpPlan::RowsOnly { after: 0 }
    );
    assert_eq!(
        plan_catch_up(4, &state(9, 0), true),
        CatchUpPlan::RowsOnly { after: 4 }
    );
    // Frontier contained: skip the checkpoint even from an older cursor.
    assert_eq!(
        plan_catch_up(2, &state(9, 5), true),
        CatchUpPlan::RowsOnly { after: 5 }
    );
    assert_eq!(
        plan_catch_up(7, &state(9, 5), true),
        CatchUpPlan::RowsOnly { after: 7 }
    );
    // Frontier missing: checkpoint first, rows after it.
    assert_eq!(
        plan_catch_up(2, &state(9, 5), false),
        CatchUpPlan::CheckpointThenRows { after: 5 }
    );
    // Server lost state (cursor ahead of head): cursor is meaningless.
    assert_eq!(
        plan_catch_up(50, &state(3, 0), true),
        CatchUpPlan::RowsOnly { after: 0 }
    );
}

// ── end-to-end actor behavior ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn fresh_join_backfills_rows_and_advances_cursor() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, fetch_calls) = fetcher(b"");

    let server = tokio::spawn(async move {
        let after = serve_join(
            &mut end,
            serde_json::json!({"headSeq": 2, "seqFloor": 0, "checkpointSeq": 0,
                "checkpointSize": 0, "rowCount": 2, "rowBytes": 64}),
            &[],
            vec![(1, "dev-b", vec![0xaa]), (2, "dev-b", vec![0xbb])],
            false,
        )
        .await;
        assert_eq!(after, 0);
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    server.await.unwrap();

    assert_eq!(
        *lock(&sink.rows),
        vec![(vec![0xaa], 1), (vec![0xbb], 2)],
        "both remote rows imported in seq order"
    );
    assert_eq!(fetch_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    let stats = client.stats();
    assert!(stats.connected);
    assert_eq!(stats.cursor, 2);
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn contained_frontier_skips_the_checkpoint_download() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    sink.frontier_contained
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (fetch, fetch_calls) = fetcher(b"never");

    let server = tokio::spawn(async move {
        let after = serve_join(
            &mut end,
            serde_json::json!({"headSeq": 8, "seqFloor": 5, "checkpointSeq": 5,
                "checkpointSize": 160_000, "rowCount": 3, "rowBytes": 900}),
            &[1, 2, 3],
            vec![
                (6, "dev-b", vec![6]),
                (7, "dev-b", vec![7]),
                (8, "dev-b", vec![8]),
            ],
            false,
        )
        .await;
        // Client-side precision: cursor was 0 but the frontier is local —
        // skip straight past the checkpointed span.
        assert_eq!(after, 5);
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    server.await.unwrap();

    assert_eq!(fetch_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(lock(&sink.checkpoints).is_empty());
    assert_eq!(lock(&sink.rows).len(), 3);
    assert_eq!(client.stats().cursor, 8);
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn missing_frontier_fetches_and_imports_the_checkpoint_first() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, fetch_calls) = fetcher(b"checkpoint-bytes");

    let server = tokio::spawn(async move {
        let after = serve_join(
            &mut end,
            serde_json::json!({"headSeq": 6, "seqFloor": 5, "checkpointSeq": 5,
                "checkpointSize": 16, "rowCount": 1, "rowBytes": 10}),
            &[9, 9, 9],
            vec![(6, "dev-b", vec![6])],
            false,
        )
        .await;
        assert_eq!(after, 5, "rows resume after the checkpoint");
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        2,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    server.await.unwrap();

    assert_eq!(fetch_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        *lock(&sink.checkpoints),
        vec![(b"checkpoint-bytes".to_vec(), 5)]
    );
    assert_eq!(*lock(&sink.rows), vec![(vec![6u8], 6)]);
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unacked_pushes_survive_reconnect_and_acks_retire_them() {
    let (pipe1, mut end1) = pipe_pair();
    let (pipe2, mut end2) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");

    let empty_state = serde_json::json!({"headSeq": 0, "seqFloor": 0,
        "checkpointSeq": 0, "checkpointSize": 0, "rowCount": 0, "rowBytes": 0});

    let s1 = tokio::spawn({
        let state = empty_state.clone();
        async move {
            serve_join(&mut end1, state, &[], vec![], false).await;
            // Receive the push but die before acking — the client must
            // re-push the SAME batch id on the next session.
            let push = expect_kind(&mut end1, frame_type::PUSH).await;
            let batch_id = push.header["batchId"].as_str().unwrap().to_string();
            assert_eq!(push.payload, vec![0xd1u8]);
            drop(end1); // socket dies
            batch_id
        }
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe1, pipe2]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");

    client.enqueue_update(vec![0xd1]);
    let first_batch = s1.await.unwrap();
    assert_eq!(
        client.stats().pending_pushes,
        1,
        "unacked batch stays queued"
    );

    // Second session: same handshake, then the replayed push gets acked.
    let s2 = tokio::spawn({
        let state = empty_state.clone();
        async move {
            serve_join(&mut end2, state, &[], vec![], true).await;
            let push = expect_kind(&mut end2, frame_type::PUSH).await;
            let batch_id = push.header["batchId"].as_str().unwrap().to_string();
            send(
                &end2,
                frame_type::ACK,
                serde_json::json!({"batchId": batch_id, "seq": 1, "dup": false}),
                &[],
            )
            .await;
            (batch_id, end2)
        }
    });
    let (replayed_batch, _keep_alive) = s2.await.unwrap();
    assert_eq!(
        replayed_batch, first_batch,
        "reconnect replays the same batch id"
    );

    // Ack lands asynchronously — wait for the pending queue to drain.
    let mut events = client.events();
    while client.stats().pending_pushes > 0 {
        let _ = events.recv().await;
    }
    assert_eq!(client.stats().cursor, 1, "ack advanced the cursor");
    assert_eq!(*lock(&sink.cursor_advances), vec![1]);
    client.shutdown().await;
}

// ── 2026-08-10 review fixes (F1–F4) ─────────────────────────────────────────

struct PendingFetcher;
impl CheckpointFetcher for PendingFetcher {
    fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        Box::pin(std::future::pending())
    }
}

/// F2: a permanent server verdict (`too_large`) retires the batch from the
/// replay queue; a transient one (`quota`) keeps it and re-pushes on the
/// retry clock without waiting for a new enqueue.
#[tokio::test(start_paused = true)]
async fn permanent_rejection_retires_transient_keeps_and_retries() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let empty_state = serde_json::json!({"headSeq": 0, "seqFloor": 0,
        "checkpointSeq": 0, "checkpointSize": 0, "rowCount": 0, "rowBytes": 0});

    let server = tokio::spawn(async move {
        serve_join(&mut end, empty_state, &[], vec![], false).await;
        // First batch: permanently rejected.
        let doomed = expect_kind(&mut end, frame_type::PUSH).await;
        let doomed_id = doomed.header["batchId"].as_str().unwrap().to_string();
        send(
            &end,
            frame_type::ERROR,
            serde_json::json!({"code": "too_large", "message": "push rejected", "batchId": doomed_id}),
            &[],
        )
        .await;
        // Second batch: quota-limited once, then replayed by the retry clock
        // (no further enqueue nudges) and acked.
        let quotad = expect_kind(&mut end, frame_type::PUSH).await;
        let quotad_id = quotad.header["batchId"].as_str().unwrap().to_string();
        send(
            &end,
            frame_type::ERROR,
            serde_json::json!({"code": "quota", "message": "later", "batchId": quotad_id}),
            &[],
        )
        .await;
        let replay = expect_kind(&mut end, frame_type::PUSH).await;
        assert_eq!(
            replay.header["batchId"].as_str().unwrap(),
            quotad_id,
            "retry clock replays the SAME quota-limited batch"
        );
        send(
            &end,
            frame_type::ACK,
            serde_json::json!({"batchId": quotad_id, "seq": 1, "dup": false}),
            &[],
        )
        .await;
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");

    let mut events = client.events();
    client.enqueue_update(vec![0xd0]); // doomed
    // Retirement lands asynchronously; PushRejected marks it.
    loop {
        if let Ok(ChatEvent::PushRejected) = events.recv().await {
            break;
        }
    }
    assert_eq!(client.stats().pending_pushes, 0, "doomed batch retired");

    client.enqueue_update(vec![0xb0]);
    let _keep = server.await.unwrap();
    while client.stats().pending_pushes > 0 {
        let _ = events.recv().await;
    }
    assert_eq!(client.stats().cursor, 1, "quota batch eventually landed");
    assert!(client.stats().rejected >= 2);
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn permanent_rejection_rearms_an_already_queued_successor() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let (head_seen_tx, head_seen_rx) = oneshot::channel();
    let (reject_tx, reject_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_join(
            &mut end,
            serde_json::json!({"headSeq": 0, "seqFloor": 0, "checkpointSeq": 0,
                "checkpointSize": 0, "rowCount": 0, "rowBytes": 0}),
            &[],
            vec![],
            false,
        )
        .await;
        let head = expect_kind(&mut end, frame_type::PUSH).await;
        let head_id = head.header["batchId"].as_str().unwrap().to_string();
        head_seen_tx.send(head_id.clone()).unwrap();
        reject_rx.await.unwrap();
        send(
            &end,
            frame_type::ERROR,
            serde_json::json!({"code": "too_large", "message": "no", "batchId": head_id}),
            &[],
        )
        .await;
        let successor = expect_kind(&mut end, frame_type::PUSH).await;
        assert_eq!(successor.header["batchId"], "successor");
        assert_eq!(successor.payload, vec![2]);
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink,
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .unwrap();
    client.enqueue_update(vec![1]);
    let _head_id = head_seen_rx.await.unwrap();
    // Queue work without sending a nudge: this pins that handling the permanent
    // verdict itself advances queue liveness.
    lock(&client.shared).pending.push_back(PendingPush {
        batch_id: "successor".into(),
        bytes: vec![2],
    });
    reject_tx.send(()).unwrap();

    let _keep = server.await.unwrap();
    client.shutdown().await;
}

/// F2: batches over the row cap never enter the replay queue.
#[tokio::test(start_paused = true)]
async fn oversized_enqueue_is_refused_at_the_door() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let empty_state = serde_json::json!({"headSeq": 0, "seqFloor": 0,
        "checkpointSeq": 0, "checkpointSize": 0, "rowCount": 0, "rowBytes": 0});
    let server = tokio::spawn(async move {
        serve_join(&mut end, empty_state, &[], vec![], false).await;
        end
    });
    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    let _keep = server.await.unwrap();

    // Exactly the DO row cap (1 MiB): the frame header would push the WS
    // message over the runtime's 1 MiB cap and the socket would close with
    // NO error frame to retire the batch — the gate must refuse it (this is
    // why MAX_PUSH_BYTES carries headroom below the row cap).
    client.enqueue_update(vec![0u8; 1024 * 1024]);
    let stats = client.stats();
    assert_eq!(stats.pending_pushes, 0, "boundary batch not queued");
    assert_eq!(stats.rejected, 1);
    client.shutdown().await;
}

/// F4 (second half): `shutdown()` must complete promptly even while the
/// actor is parked inside a hung checkpoint fetch.
#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_a_hung_checkpoint_fetch() {
    let (pipe1, mut end1) = pipe_pair();
    let (pipe2, mut end2) = pipe_pair();
    let sink = Arc::new(RecordingSink::default()); // frontier NOT contained
    let empty_state = serde_json::json!({"headSeq": 0, "seqFloor": 0,
        "checkpointSeq": 0, "checkpointSize": 0, "rowCount": 0, "rowBytes": 0});

    // Session 1: clean join (no checkpoint), then the socket dies.
    let s1 = tokio::spawn(async move {
        serve_join(&mut end1, empty_state, &[], vec![], false).await;
        drop(end1);
    });
    // Session 2: a checkpoint appeared — the client must fetch, and hangs.
    let s2 = tokio::spawn(async move {
        let _hello = expect_kind(&mut end2, frame_type::HELLO).await;
        send(
            &end2,
            frame_type::STATE,
            serde_json::json!({"headSeq": 9, "seqFloor": 5, "checkpointSeq": 5,
                "checkpointSize": 1000, "rowCount": 4, "rowBytes": 40}),
            &[7, 7, 7],
        )
        .await;
        end2 // keep the pipe alive; only the fetch is stuck
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe1, pipe2]),
        sink,
        Arc::new(PendingFetcher),
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("first join succeeds");
    s1.await.unwrap();
    let _keep = s2.await.unwrap();
    // Let the actor redial and park inside the hung fetch.
    tokio::time::sleep(Duration::from_secs(2)).await;
    tokio::time::timeout(Duration::from_secs(30), client.shutdown())
        .await
        .expect("shutdown must not hang on a stuck fetch");
}

/// F3: a server whose headSeq fell behind our cursor (reset/wiped room) is
/// SURFACED — counted in stats, honest head_seq — not silently absorbed.
#[tokio::test(start_paused = true)]
async fn server_reset_is_counted_and_head_seq_stays_honest() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    let (fetch, _) = fetcher(b"");
    let server = tokio::spawn(async move {
        let after = serve_join(
            &mut end,
            serde_json::json!({"headSeq": 3, "seqFloor": 0, "checkpointSeq": 0,
                "checkpointSize": 0, "rowCount": 3, "rowBytes": 30}),
            &[],
            vec![
                (1, "dev-b", vec![1]),
                (2, "dev-b", vec![2]),
                (3, "dev-b", vec![3]),
            ],
            false,
        )
        .await;
        assert_eq!(after, 0, "meaningless cursor treated as fresh");
        end
    });
    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        50, // persisted cursor from before the room was wiped
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    let _keep = server.await.unwrap();

    let stats = client.stats();
    assert_eq!(stats.server_resets, 1, "reset visible to the host");
    assert_eq!(stats.head_seq, 3, "server view not masked by the cursor");
    assert_eq!(stats.cursor, 3, "cursor re-anchored by the backfill");
    assert_eq!(
        *lock(&sink.cursor_resets),
        vec![0],
        "the backward move must be persisted explicitly before new rows"
    );
    client.shutdown().await;
}

/// F4: a checkpoint fetch that never resolves fails the first join within
/// the deadline instead of hanging the actor (and shutdown) forever.
#[tokio::test(start_paused = true)]
async fn hung_checkpoint_fetch_fails_the_join_within_deadline() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default()); // frontier NOT contained
    let server = tokio::spawn(async move {
        let hello = expect_kind(&mut end, frame_type::HELLO).await;
        assert!(hello.header["device"].is_string());
        send(
            &end,
            frame_type::STATE,
            serde_json::json!({"headSeq": 9, "seqFloor": 5, "checkpointSeq": 5,
                "checkpointSize": 1000, "rowCount": 4, "rowBytes": 40}),
            &[7, 7, 7],
        )
        .await;
        end // keep the pipe alive; the fetch is what must time out
    });
    let joined = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink,
        Arc::new(PendingFetcher),
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await;
    assert!(joined.is_err(), "hung fetch must not hang the join");
    let _keep = server.await.unwrap();
}

/// M1 seed shape: checkpointSeq 0 with a real blob. BOTH presence tests
/// (plan_catch_up AND run_session's frontier short-circuit) must key on
/// SIZE — the 2026-08-10 gauntlet caught seq==0 short-circuits in each,
/// which would have made every adopted reader skip the seed and render an
/// EMPTY transcript.
#[tokio::test(start_paused = true)]
async fn seeded_at_zero_room_fetches_the_checkpoint() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default()); // frontier NOT contained
    let (fetch, fetch_calls) = fetcher(b"seed-checkpoint-bytes");

    let server = tokio::spawn(async move {
        let after = serve_join(
            &mut end,
            serde_json::json!({"headSeq": 0, "seqFloor": 0, "checkpointSeq": 0,
                "checkpointSize": 276_342, "rowCount": 0, "rowBytes": 0}),
            &[7, 7, 7], // non-empty frontier the fresh doc can't contain
            vec![],
            false,
        )
        .await;
        assert_eq!(after, 0);
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds");
    let _keep = server.await.unwrap();

    assert_eq!(
        fetch_calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "seed checkpoint fetched despite checkpointSeq == 0"
    );
    assert_eq!(
        *lock(&sink.checkpoints),
        vec![(b"seed-checkpoint-bytes".to_vec(), 0)]
    );
    client.shutdown().await;
}

// ── 450kbps cold-open: overlap + early push ─────────────────────────────────

/// Fetch that resolves only when the test releases its gate — stands in for
/// a checkpoint blob crawling down a thin link.
struct GatedFetcher {
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    bytes: Vec<u8>,
}

impl CheckpointFetcher for GatedFetcher {
    fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        let gate = lock(&self.gate).take().expect("single fetch");
        let bytes = self.bytes.clone();
        Box::pin(async move {
            let _ = gate.await;
            Ok(bytes)
        })
    }
}

/// The rows request leaves BEFORE the checkpoint download finishes (the
/// server observes it while the fetch is still gated), rows landing
/// mid-download buffer, and the import still applies before any of them —
/// the join must not serialize download → request → backfill.
#[tokio::test]
async fn checkpoint_fetch_overlaps_row_backfill() {
    let (pipe, mut end) = pipe_pair();
    let sink = Arc::new(RecordingSink::default()); // frontier NOT contained
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let fetch = Arc::new(GatedFetcher {
        gate: Mutex::new(Some(gate_rx)),
        bytes: b"parallel-checkpoint".to_vec(),
    });

    let server = tokio::spawn(async move {
        let _hello = expect_kind(&mut end, frame_type::HELLO).await;
        send(
            &end,
            frame_type::STATE,
            serde_json::json!({"headSeq": 7, "seqFloor": 0,
                "checkpointSeq": 5, "checkpointSize": 1000,
                "rowCount": 2, "rowBytes": 64}),
            b"frontier",
        )
        .await;
        // The ordering pin: this arrives while the fetch is still GATED.
        let req = expect_kind(&mut end, frame_type::ROWS_REQ).await;
        assert_eq!(req.header["after"], 5, "rows resume past the checkpoint");
        send(
            &end,
            frame_type::ROW,
            serde_json::json!({"seq": 6, "device": "dev-b", "batchId": "b6"}),
            b"r6",
        )
        .await;
        send(
            &end,
            frame_type::ROW,
            serde_json::json!({"seq": 7, "device": "dev-b", "batchId": "b7"}),
            b"r7",
        )
        .await;
        send(
            &end,
            frame_type::ROWS_DONE,
            serde_json::json!({"headSeq": 7}),
            &[],
        )
        .await;
        // Only now does the "download" complete.
        let _ = gate_tx.send(());
        end
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("join succeeds with rows served before the checkpoint bytes");

    let _end = server.await.unwrap();
    assert_eq!(
        *lock(&sink.ops),
        vec!["ckpt@5", "row@6", "row@7"],
        "checkpoint imports before any row that buffered during the download"
    );
    assert_eq!(client.stats().cursor, 7);
    client.shutdown().await;
}

/// A batch queued while offline flushes right after the reconnect's state
/// answer — NOT after backfill converges. The server script only serves the
/// backfill AFTER seeing (and acking) the push; the old order deadlocks here.
#[tokio::test(start_paused = true)]
async fn pending_push_flushes_before_backfill_completes() {
    let (pipe_a, mut end_a) = pipe_pair();
    let (pipe_b, mut end_b) = pipe_pair();
    let sink = Arc::new(RecordingSink::default());
    sink.frontier_contained
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (fetch, _) = fetcher(b"");
    let empty_state = serde_json::json!({"headSeq": 0, "seqFloor": 0,
        "checkpointSeq": 0, "checkpointSize": 0, "rowCount": 0, "rowBytes": 0});

    let state_b = empty_state.clone();
    let server = tokio::spawn(async move {
        serve_join(&mut end_a, empty_state, &[], vec![], false).await;
        // The push arrives on session A but goes UNACKED; the socket dies.
        let _push = expect_kind(&mut end_a, frame_type::PUSH).await;
        drop(end_a);

        // Session B: the replay must arrive before ANY backfill is served.
        let _hello = expect_kind(&mut end_b, frame_type::HELLO).await;
        send(&end_b, frame_type::STATE, state_b, &[]).await;
        let _req = expect_kind(&mut end_b, frame_type::ROWS_REQ).await;
        let replay = expect_kind(&mut end_b, frame_type::PUSH).await;
        let batch_id = replay.header["batchId"].as_str().unwrap().to_string();
        send(
            &end_b,
            frame_type::ACK,
            serde_json::json!({"batchId": batch_id, "seq": 1, "dup": false}),
            &[],
        )
        .await;
        send(
            &end_b,
            frame_type::ROWS_DONE,
            serde_json::json!({"headSeq": 1}),
            &[],
        )
        .await;
        end_b
    });

    let client = ChatClient::connect_with_tuned(
        connector(vec![pipe_a, pipe_b]),
        sink.clone(),
        fetch,
        "dev-a",
        0,
        ChatTuning::default(),
    )
    .await
    .expect("first join succeeds");

    client.enqueue_update(vec![0xaa]);
    let mut events = client.events();
    // Ride events until the reconnect's early replay is acked.
    while client.stats().pending_pushes > 0 {
        let _ = events.recv().await;
    }
    let _end = server.await.unwrap();
    assert_eq!(
        client.stats().cursor,
        1,
        "early replay acked before ROWS_DONE"
    );
    client.shutdown().await;
}

/// A checkpoint that EXISTS (size > 0) but whose frontier payload is empty
/// must be fetched, not skipped: the empty-frontier-means-contained shortcut
/// made every fresh reader of such a room skip the chat's founding ops and
/// park all dependent rows invisibly ("Add Tweets" incident, 2026-08-18).
/// Fetching is always safe (full-state merge); skipping history never is.
#[test]
fn empty_frontier_with_real_checkpoint_is_not_contained() {
    let state = wire::StateHeader {
        head_seq: 75,
        seq_floor: 5,
        checkpoint_seq: 5,
        checkpoint_size: 2728,
        row_count: 70,
        row_bytes: 17150,
    };
    // The sink cannot vouch for a frontier it cannot read — an empty payload
    // must plan a checkpoint fetch for a cursor-0 reader.
    assert_eq!(
        plan_catch_up(0, &state, false),
        CatchUpPlan::CheckpointThenRows { after: 5 },
        "fresh reader must fetch a present checkpoint when the frontier is unreadable"
    );
}
