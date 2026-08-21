//! ChatClient — WebSocket transport for chat2 rooms (docs/chat2-sync.md C1):
//! hello/state handshake with client-side checkpoint precision, cursor-based
//! row backfill, push/ack with a pending-unacked queue, opaque presence
//! relay, probe/redial liveness, and reconnect with exponential backoff.
//!
//! The client owns no CRDT semantics: update bytes flow through a
//! [`ChatDocSink`] the engine implements over its `ChatDocHandle` (import +
//! persist doc AND cursor in one transaction — the C2 rule). Wire frames are
//! the binary chat2 codec ([`crate::chat_frames`]), byte-compatible with
//! `edge/src/chat-frames.ts`.
//!
//! Liveness discipline is inherited from `registry.rs` and its incidents:
//! transport pings prove nothing about the DO; room health is judged only by
//! protocol frames with probe deadlines.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use crate::chat_frames::{self as wire, frame_type};
use crate::types::{StaticUrl, SyncError, UrlProvider};

const PING_INTERVAL: Duration = Duration::from_secs(15);
const SILENCE_LEASE: Duration = Duration::from_secs(45);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const HELLO_DEADLINE: Duration = Duration::from_secs(15);
/// Backfill after hello must complete (rowsDone) within this deadline —
/// post-strip rooms are KB-scale, so this is generous even at 1.2 Mbps.
const BACKFILL_DEADLINE: Duration = Duration::from_secs(120);
const PROBE_DEADLINE: Duration = Duration::from_secs(10);
const BACKOFF_BASE: Duration = Duration::from_millis(250);
/// Worst-case dark window after the network returns (event wakes usually
/// beat this; the cap only matters when every event path missed).
const BACKOFF_CAP: Duration = Duration::from_secs(16);
/// A joined session must survive this long before a disconnect resets the
/// backoff to base (see registry.rs STABLE_RESET — same connect-and-die
/// hot-loop rationale).
const STABLE_RESET: Duration = Duration::from_secs(30);
/// Safety re-check cadence while parked on "OS says offline" — a stuck or
/// wrong path monitor degrades to slow polling, never to silence.
const OFFLINE_PARK_RECHECK: Duration = Duration::from_secs(30);
/// Quiet-room probe cadence default (matches the registry's fleet math).
const PROBE_QUIET_DEFAULT: Duration = Duration::from_secs(900);
/// A checkpoint fetch that hasn't finished by now is treated as a dead link
/// and the session redials (the fetch itself is Range-resumable, so a retry
/// picks up where the bytes stopped). Sized for MAX_CHECKPOINT_BYTES over
/// the 1.2 Mbps links this design exists for.
const CHECKPOINT_FETCH_DEADLINE: Duration = Duration::from_secs(120);
/// Re-push cadence after a `quota` rejection (server window is 60 s; pending
/// batches must not wait for the next enqueue/probe to retry).
const QUOTA_RETRY: Duration = Duration::from_secs(5);
/// Client-side push cap: the DO's per-row cap (`chat-log.ts MAX_ROW_BYTES`,
/// 1 MiB) minus frame-overhead headroom. The headroom matters: the runtime
/// closes WS messages at 1 MiB BEFORE the DO runs, so a payload within a
/// frame-header's width of the row cap would die with no error frame (and no
/// batchId to retire) — the silent replay-forever wedge, again. Enforced at
/// enqueue: a batch the server can never accept must not enter the replay
/// queue.
pub const MAX_PUSH_BYTES: usize = 1024 * 1024 - 4096;

/// Per-client tuning.
#[derive(Clone, Copy, Debug)]
pub struct ChatTuning {
    pub probe_quiet: Duration,
}

impl Default for ChatTuning {
    fn default() -> Self {
        Self {
            probe_quiet: PROBE_QUIET_DEFAULT,
        }
    }
}

/// Connection/sync lifecycle notifications (best-effort broadcast).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatEvent {
    /// Joined (or re-joined); the hello state has been received.
    Connected,
    /// Backfill finished — the doc is converged with the room at this head.
    CaughtUp { head_seq: u64 },
    /// Remote rows/acks were applied through the sink — republish.
    Applied,
    /// The connection dropped; the client is backing off before redialing.
    Disconnected,
    /// A remote device's presence beat arrived.
    Presence,
    /// The server's headSeq is behind our persisted cursor — the room was
    /// reset/wiped. The catch-up treats the cursor as fresh; the HOST should
    /// react by re-seeding via checkpoint (chat-room.ts `/reset` recovery).
    ServerReset,
    /// A queued batch was permanently rejected (or refused at enqueue) and
    /// dropped from the replay queue. The ops remain in the local doc; the
    /// row-path for them is gone, so they reach peers only when THIS device
    /// next posts a checkpoint — the C3 host should treat this event as a
    /// checkpoint trigger, not a shrug.
    PushRejected,
    /// A room request was authenticated but refused with HTTP 403. This is
    /// also latched on [`ChatClient::access_denied`] because local-first
    /// construction can finish before an event receiver subscribes.
    AccessDenied,
}

// ── engine-facing traits ────────────────────────────────────────────────────

/// Where remote bytes land. The engine implements this over its doc handle;
/// every method persists doc content AND the room cursor in one transaction
/// (`DocsStore::save_snapshot_with_cursor`) so they can never diverge.
pub trait ChatDocSink: Send + Sync + 'static {
    /// Import one remote update row. `cursor` is the caller's HONEST cursor
    /// after the contiguity rule ran — not necessarily the row's own seq: a
    /// row arriving across a gap is applied (loro parks dependents harmlessly)
    /// while the cursor holds back until `maybe_repair_gap` backfills the
    /// missing span. Persisting a parked import is therefore safe.
    fn apply_row(&self, bytes: &[u8], cursor: u64);
    /// Replace/merge from a checkpoint blob; `cursor` is its checkpointSeq.
    fn apply_checkpoint(&self, bytes: &[u8], cursor: u64) -> Result<(), String>;
    /// Client-side precision (replaces the server VV diff): is the server
    /// checkpoint's frontier already contained in the local doc?
    fn contains_frontier(&self, frontier: &[u8]) -> bool;
    /// An own-write ack advanced the cursor with no content change.
    fn advance_cursor(&self, cursor: u64);
    /// The server log was reset/replaced within the same room generation.
    /// Persist `cursor` exactly (including a backwards move) without treating
    /// it as an own-write ack or promoting a generation handoff epoch.
    fn reset_cursor(&self, cursor: u64);
}

/// `GET /chat2/{chatId}/checkpoint` over HTTP. Implementations should resume
/// partial downloads with `Range: bytes=N-` (the DO serves 206) — that
/// resumability is the point of checkpoint-over-HTTP vs export-per-join.
pub trait CheckpointFetcher: Send + Sync + 'static {
    fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, SyncError>>;
}

/// Plain-HTTPS pull/push seam — the airplane-wifi transport. `fetch_rows`
/// GETs `/chat2/{id}/rows?after=` and returns the body: u32-LE
/// length-prefixed frames (state, rows, rowsDone — the WS encoding). `push`
/// POSTs one batch to `/chat2/{id}/rows?batchId=` and returns the JSON ack.
/// Both are safe at-least-once: batchId dedupe and Loro re-import no-ops.
pub trait ChatTransport: Send + Sync + 'static {
    fn fetch_rows(&self, after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>>;
    fn push(
        &self,
        batch_id: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>>;
}

// ── catch-up planning (pure — the client-side precision rule) ───────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpPlan {
    /// Local doc already contains the checkpoint frontier (or there is no
    /// checkpoint): stream rows only.
    RowsOnly { after: u64 },
    /// Fetch + import the checkpoint first, then rows after it.
    CheckpointThenRows { after: u64 },
}

/// Decide the catch-up path from the hello state. `frontier_contained` is the
/// sink's verdict on the checkpoint frontier payload.
pub fn plan_catch_up(
    cursor: u64,
    state: &wire::StateHeader,
    frontier_contained: bool,
) -> CatchUpPlan {
    // A cursor ahead of the server means the server lost state (reset/wipe);
    // our cursor is meaningless there — treat as fresh.
    let cursor = if cursor > state.head_seq { 0 } else { cursor };
    // Presence test is the SIZE, not the seq: a freshly SEEDED room's
    // checkpoint legitimately covers seq 0 (M1 seeds before any rows
    // exist), and seq==0 misread as "no checkpoint" made every adopted
    // reader skip the seed and render an empty transcript (caught by the
    // 2026-08-10 cutover gauntlet).
    if state.checkpoint_size == 0 {
        return CatchUpPlan::RowsOnly { after: cursor };
    }
    if frontier_contained {
        // Rows ≤ checkpointSeq are covered by a checkpoint we already
        // contain — skip straight past them even if our cursor is older.
        CatchUpPlan::RowsOnly {
            after: cursor.max(state.checkpoint_seq),
        }
    } else {
        CatchUpPlan::CheckpointThenRows {
            after: state.checkpoint_seq,
        }
    }
}

/// Fully decoded plain-HTTPS rows response, possibly stitched from several
/// truncated pages (see `fetch_http_rows`). Unlike the WebSocket stream, this
/// body is an atomic proof: staged POST acks may be retired only after every
/// length-prefixed frame and the terminal frontier have been validated.
struct HttpRowsResponse {
    state: wire::StateHeader,
    frontier: Vec<u8>,
    rows: Vec<(wire::RowHeader, Vec<u8>)>,
    head_seq: u64,
}

/// One GET /rows body. `head_seq` is `None` when the server hit its
/// ROWS_BODY_CAP and ended the body WITHOUT ROWS_DONE — deliberate truncation,
/// not an error: the next page resumes after the last row received.
struct HttpRowsPage {
    state: wire::StateHeader,
    frontier: Vec<u8>,
    rows: Vec<(wire::RowHeader, Vec<u8>)>,
    head_seq: Option<u64>,
}

fn decode_http_rows_page(body: &[u8]) -> Result<HttpRowsPage, String> {
    let mut frames = Vec::new();
    let mut off = 0usize;
    while off < body.len() {
        if body.len() - off < 4 {
            return Err("trailing bytes after final HTTP row frame".into());
        }
        let len = u32::from_le_bytes(
            body[off..off + 4]
                .try_into()
                .expect("four-byte prefix was bounds checked"),
        ) as usize;
        off += 4;
        let end = off
            .checked_add(len)
            .ok_or_else(|| "HTTP row frame length overflow".to_string())?;
        if end > body.len() {
            return Err("truncated HTTP row frame".into());
        }
        let frame =
            wire::decode(&body[off..end]).ok_or_else(|| "malformed HTTP row frame".to_string())?;
        frames.push(frame);
        off = end;
    }

    let mut iter = frames.into_iter();
    let state_frame = iter
        .next()
        .ok_or_else(|| "HTTP rows response is empty".to_string())?;
    if state_frame.kind != frame_type::STATE {
        return Err("HTTP rows response does not start with STATE".into());
    }
    let state = serde_json::from_value::<wire::StateHeader>(state_frame.header)
        .map_err(|err| format!("malformed HTTP STATE header: {err}"))?;
    let mut rows = Vec::new();
    let mut done = None;
    for frame in iter {
        if done.is_some() {
            return Err("frame follows HTTP ROWS_DONE".into());
        }
        match frame.kind {
            frame_type::ROW => {
                let row = serde_json::from_value::<wire::RowHeader>(frame.header)
                    .map_err(|err| format!("malformed HTTP ROW header: {err}"))?;
                rows.push((row, frame.payload));
            }
            frame_type::ROWS_DONE => {
                if !frame.payload.is_empty() {
                    return Err("HTTP ROWS_DONE unexpectedly has a payload".into());
                }
                let header = serde_json::from_value::<wire::RowsDoneHeader>(frame.header)
                    .map_err(|err| format!("malformed HTTP ROWS_DONE header: {err}"))?;
                done = Some(header.head_seq);
            }
            _ => return Err("unexpected frame in HTTP rows response".into()),
        }
    }
    Ok(HttpRowsPage {
        state,
        frontier: state_frame.payload,
        rows,
        head_seq: done,
    })
}

enum HttpPullError {
    Transport(SyncError),
    Malformed(String),
}

/// Defensive ceiling on truncated pages per pull: at ~4 MiB a page, 32 pages
/// is a 128 MiB backlog — far past any real log (checkpoints prune rows).
/// Hitting it means a server bug, and erroring beats pulling forever.
const MAX_HTTP_ROWS_PAGES: usize = 32;

/// Pull rows, following truncation pagination. GET /rows bounds its buffered
/// body at ROWS_BODY_CAP (4 MiB); past the cap the server ends the response
/// WITHOUT ROWS_DONE and expects the client to resume after the last row it
/// received. Pages are stitched into one response so the atomic-proof
/// validation downstream is unchanged. The FINAL page's STATE/ROWS_DONE pair
/// is authoritative (the DO builds both in one synchronous read); rows from
/// stale earlier pages surface as validation failures and retry cleanly.
async fn fetch_http_rows(
    transport: &dyn ChatTransport,
    pull_since: u64,
) -> Result<HttpRowsResponse, HttpPullError> {
    let mut rows: Vec<(wire::RowHeader, Vec<u8>)> = Vec::new();
    let mut after = pull_since;
    let mut prev_state: Option<wire::StateHeader> = None;
    for _ in 0..MAX_HTTP_ROWS_PAGES {
        let body = transport
            .fetch_rows(after)
            .await
            .map_err(HttpPullError::Transport)?;
        let page = decode_http_rows_page(&body).map_err(HttpPullError::Malformed)?;
        // Cross-page incarnation fence: a /reset (or checkpoint) between pages
        // could otherwise stitch rows from two room incarnations into one
        // numerically-contiguous response. The epoch is the authoritative
        // fence (/reset bumps it; a checkpointless room's triple is (0,0,0)
        // on both sides of a reset); the checkpoint triple additionally
        // catches a concurrent checkpoint pruning rows mid-pull; head_seq
        // only ever grows. Any drift = restart the pull rather than certify
        // a mixed history.
        if let Some(prev) = prev_state {
            if page.state.epoch != prev.epoch
                || page.state.seq_floor != prev.seq_floor
                || page.state.checkpoint_seq != prev.checkpoint_seq
                || page.state.checkpoint_size != prev.checkpoint_size
                || page.state.head_seq < prev.head_seq
            {
                return Err(HttpPullError::Malformed(
                    "room state changed between HTTP rows pages".into(),
                ));
            }
        }
        prev_state = Some(page.state);
        let page_last = page.rows.last().map(|(row, _)| row.seq);
        rows.extend(page.rows);
        if let Some(head_seq) = page.head_seq {
            return Ok(HttpRowsResponse {
                state: page.state,
                frontier: page.frontier,
                rows,
                head_seq,
            });
        }
        // A truncated page always advances: MAX_ROW_BYTES (1 MiB) is far
        // below the cap, so the server fits at least one row before
        // truncating. No progress = server bug; erroring beats spinning.
        match page_last {
            Some(last) if last > after => after = last,
            _ => {
                return Err(HttpPullError::Malformed(
                    "truncated HTTP rows page made no progress".into(),
                ));
            }
        }
    }
    Err(HttpPullError::Malformed(format!(
        "HTTP rows still truncated after {MAX_HTTP_ROWS_PAGES} pages"
    )))
}

/// Verify that the response proves one contiguous local frontier through its
/// terminal head. Rows at or below `covered_after` may be absent because an
/// imported/contained checkpoint covers them; everything above it is exact.
fn validate_http_rows_frontier(
    response: &HttpRowsResponse,
    requested_after: u64,
    covered_after: u64,
) -> Result<u64, String> {
    if response.head_seq != response.state.head_seq {
        return Err("HTTP STATE and ROWS_DONE disagree on headSeq".into());
    }
    if covered_after > response.head_seq {
        return Err("HTTP covered frontier is ahead of ROWS_DONE".into());
    }
    let mut previous = requested_after;
    let mut covered = covered_after;
    for (row, _) in &response.rows {
        if row.seq <= requested_after || row.seq <= previous {
            return Err("HTTP rows are not strictly ordered after the request cursor".into());
        }
        previous = row.seq;
        if row.seq > response.head_seq {
            return Err("HTTP row is ahead of ROWS_DONE".into());
        }
        if row.seq <= covered_after {
            continue;
        }
        let expected = covered
            .checked_add(1)
            .ok_or_else(|| "HTTP row frontier overflow".to_string())?;
        if row.seq != expected {
            return Err("HTTP rows leave a gap before ROWS_DONE".into());
        }
        covered = row.seq;
    }
    if covered != response.head_seq {
        return Err("HTTP rows do not reach ROWS_DONE".into());
    }
    Ok(covered)
}

// ── transport plumbing (binary sibling of registry.rs's TextPipe) ───────────

pub(crate) struct BinPipe {
    pub(crate) tx: mpsc::Sender<Vec<u8>>,
    pub(crate) rx: mpsc::Receiver<Vec<u8>>,
}

pub(crate) trait BinConnector: Send + Sync + 'static {
    fn connect(&self) -> BoxFuture<'static, Result<BinPipe, SyncError>>;
}

struct WsBinConnector {
    url: Arc<dyn UrlProvider>,
}

fn map_ws_connect_error(err: WsError) -> SyncError {
    if matches!(&err, WsError::Http(response) if response.status().as_u16() == 403) {
        SyncError::AccessDenied("websocket handshake HTTP 403".into())
    } else {
        SyncError::WebSocket(err.to_string())
    }
}

impl BinConnector for WsBinConnector {
    fn connect(&self) -> BoxFuture<'static, Result<BinPipe, SyncError>> {
        let provider = self.url.clone();
        Box::pin(async move {
            let url = provider.url().await?;
            let ws = crate::dial::connect_ws(&url)
                .await
                .map_err(map_ws_connect_error)?;
            let (out_tx, out_rx) = mpsc::channel(64);
            let (in_tx, in_rx) = mpsc::channel(64);
            tokio::spawn(pump(ws, out_rx, in_tx));
            Ok(BinPipe {
                tx: out_tx,
                rx: in_rx,
            })
        })
    }
}

/// Shuttle binary frames between the WebSocket and the actor's channels; the
/// text `"ping"` keepalive rides the same socket (runtime-answered pair).
async fn pump(
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    mut out_rx: mpsc::Receiver<Vec<u8>>,
    in_tx: mpsc::Sender<Vec<u8>>,
) {
    let (mut sink, mut stream) = ws.split();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let mut last_rx = tokio::time::Instant::now();
    loop {
        tokio::select! {
            frame = out_rx.recv() => match frame {
                Some(bytes) => {
                    if sink.send(WsMessage::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                None => {
                    let _ = sink.send(WsMessage::Close(None)).await;
                    break;
                }
            },
            frame = stream.next() => match frame {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    last_rx = tokio::time::Instant::now();
                    if in_tx.send(bytes.to_vec()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {
                    // Text pong / control frames: transport liveness only.
                    last_rx = tokio::time::Instant::now();
                }
                Some(Err(_)) | None => break,
            },
            _ = ping.tick() => {
                if sink.send(WsMessage::Text("ping".into())).await.is_err() {
                    break;
                }
            }
            _ = tokio::time::sleep_until(last_rx + SILENCE_LEASE) => {
                tracing::warn!("chat2 socket silent past lease; treating as dead");
                break;
            }
        }
    }
}

// ── shared client state ─────────────────────────────────────────────────────

struct PendingPush {
    batch_id: String,
    bytes: Vec<u8>,
    /// Pushed at least once on the current socket. The nudge path skips
    /// already-sent batches: re-pushing the whole queue on every enqueue
    /// (8/s during streaming) multiplied the push rate by the queue depth
    /// and livelocked the server's per-device quota (2026-08-20 storm:
    /// 15k rejections/min). Reconnects clear the flags and replay all.
    sent: bool,
}

#[derive(Default)]
struct Shared {
    cursor: u64,
    /// Incremented for every intentional backwards cursor move. HTTP and WS
    /// responses capture this before I/O; a changed value means their sequence
    /// namespace was overtaken even if the numeric cursor later matches again.
    reset_version: u64,
    pending: VecDeque<PendingPush>,
    /// UUIDs captured by the single in-flight HTTPS round. A concurrent WS
    /// permanent verdict only needs a tombstone while its UUID is in this set.
    offline_snapshot_ids: HashSet<String>,
    /// WS acks racing an HTTPS snapshot are staged here instead of retiring
    /// the only replay copy. The HTTP round merges them with its POST acks once
    /// a complete pull proves which room incarnation contains each sequence.
    offline_ws_acks: HashMap<String, u64>,
    /// Batch UUIDs that received a permanent server verdict while an HTTPS
    /// round may still hold a pre-verdict snapshot. The round guard removes
    /// its UUIDs on exit; until then reset recovery must not resurrect them.
    permanent_rejections: HashSet<String>,
    /// Last hello/probe view of the server log (checkpoint-policy inputs).
    server: Option<wire::StateHeader>,
    /// Set by a transient (`quota`) rejection: re-push at this instant
    /// instead of waiting for the next enqueue/probe/reconnect.
    retry_at: Option<tokio::time::Instant>,
    /// True while draining a quota-rejected queue. Retry ticks then probe
    /// with the HEAD batch only (a full-queue replay would itself consume
    /// the server's quota window — N pending × 12 ticks/window livelocks
    /// past N≈25), and each ack immediately re-arms the clock until the
    /// queue empties.
    quota_blocked: bool,
    /// A row/ack arrived with `seq > cursor + 1`: rows exist that this
    /// client never received (live broadcast outran the backfill — the
    /// mid-join race). The cursor must NOT skip the hole (skipped rows'
    /// dependents park invisibly in loro's pending buffer and the doc
    /// reads empty forever — the 2026-08-19 empty-doc/advanced-cursor
    /// wedge); instead this flag asks the session loop for a rowsReq
    /// backfill from the honest cursor.
    gap_repair: bool,
}

/// `zeron sync` surface (plan: cursor / headSeq / floorLag / pendingPushes).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChatStatsSnapshot {
    pub connected: bool,
    /// A state answer (socket hello or HTTPS pull) has been received —
    /// until then every server-side field below is a placeholder zero, and
    /// consumers like the host's bootstrap heal must not act on them.
    pub server_known: bool,
    pub cursor: u64,
    pub head_seq: u64,
    pub seq_floor: u64,
    pub checkpoint_seq: u64,
    /// Byte size of the room's stored checkpoint (0 = none). The host's
    /// bootstrap heal keys off this: a room with rows but NO checkpoint
    /// cannot cover its rows' causal deps for cold readers.
    pub checkpoint_size: u64,
    pub row_count: u64,
    pub row_bytes: u64,
    pub pending_pushes: u64,
    pub rejoins: u64,
    pub disconnects: u64,
    pub rejected: u64,
    /// Times a hello found the server behind our cursor (room reset/wiped).
    /// Nonzero means the host owes the room a re-seed checkpoint.
    pub server_resets: u64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ── the client ──────────────────────────────────────────────────────────────

/// A live chat2-room membership for one chat doc.
pub struct ChatClient {
    shared: Arc<Mutex<Shared>>,
    events: broadcast::Sender<ChatEvent>,
    shutdown: watch::Sender<bool>,
    nudge: mpsc::Sender<()>,
    probe: mpsc::Sender<()>,
    redial: mpsc::Sender<()>,
    presence_out: mpsc::Sender<(i64, Vec<u8>)>,
    flags: Arc<Flags>,
    /// Cancels the detached HTTPS sibling together with this membership. The
    /// actor task alone does not own that spawned round.
    offline_cancel: CancellationToken,
    offline_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct Flags {
    connected: std::sync::atomic::AtomicBool,
    /// Sticky authorization verdict. A broadcast alone is insufficient:
    /// the pull-first actor can receive a 403 before its caller subscribes.
    access_denied: std::sync::atomic::AtomicBool,
    rejoins: std::sync::atomic::AtomicU64,
    disconnects: std::sync::atomic::AtomicU64,
    rejected: std::sync::atomic::AtomicU64,
    server_resets: std::sync::atomic::AtomicU64,
    /// Monotonic dial-attempt counter; each attempt's number is its trace id
    /// in logs, so an incident reads as one numbered sequence.
    dial_seq: std::sync::atomic::AtomicU64,
}

fn signal_access_denied(flags: &Flags, events: &broadcast::Sender<ChatEvent>) {
    use std::sync::atomic::Ordering::AcqRel;
    // One verdict/event per client is enough. The latch is the authoritative
    // state for late subscribers; live subscribers get the transition.
    if !flags.access_denied.swap(true, AcqRel) {
        let _ = events.send(ChatEvent::AccessDenied);
    }
}

fn signal_if_access_denied(err: &SyncError, flags: &Flags, events: &broadcast::Sender<ChatEvent>) {
    if matches!(err, SyncError::AccessDenied(_)) {
        signal_access_denied(flags, events);
    }
}

impl ChatClient {
    /// Connect (fixed URL — dev/tests).
    pub async fn connect(
        url: &str,
        sink: Arc<dyn ChatDocSink>,
        fetcher: Arc<dyn CheckpointFetcher>,
        device_id: &str,
        initial_cursor: u64,
    ) -> Result<Self, SyncError> {
        Self::connect_via(
            Arc::new(StaticUrl(url.to_string())),
            sink,
            fetcher,
            device_id,
            initial_cursor,
        )
        .await
    }

    /// Connect with a per-dial URL provider (fresh `?token=` every attempt).
    /// Resolves once hello/state lands AND the initial catch-up (checkpoint
    /// if needed + row backfill) completes; first-attempt failures are `Err`
    /// (callers own the initial-join retry). After that it reconnects itself.
    pub async fn connect_via(
        provider: Arc<dyn UrlProvider>,
        sink: Arc<dyn ChatDocSink>,
        fetcher: Arc<dyn CheckpointFetcher>,
        device_id: &str,
        initial_cursor: u64,
    ) -> Result<Self, SyncError> {
        let connector = Arc::new(WsBinConnector { url: provider });
        Self::connect_with_tuned(
            connector,
            sink,
            fetcher,
            device_id,
            initial_cursor,
            ChatTuning::default(),
        )
        .await
    }

    /// Connect with a plain-HTTPS pull/push seam alongside the socket: the
    /// construction resolves immediately (local-first — the doc is usable
    /// now and converging it is the actor's ongoing job), an HTTP pull
    /// bootstraps in ~1 RTT while the WS spends its round trips, and every
    /// backoff cycle syncs over HTTPS so a network that never passes the
    /// upgrade still converges and still delivers sends. A later HTTP/WS
    /// 403 is surfaced through [`ChatEvent::AccessDenied`] and the sticky
    /// [`ChatClient::access_denied`] accessor.
    pub async fn connect_via_transport(
        provider: Arc<dyn UrlProvider>,
        sink: Arc<dyn ChatDocSink>,
        fetcher: Arc<dyn CheckpointFetcher>,
        device_id: &str,
        initial_cursor: u64,
        transport: Arc<dyn ChatTransport>,
    ) -> Result<Self, SyncError> {
        let connector = Arc::new(WsBinConnector { url: provider });
        Self::connect_with_transport(
            connector,
            sink,
            fetcher,
            device_id,
            initial_cursor,
            ChatTuning::default(),
            Some(transport),
        )
        .await
    }

    pub(crate) async fn connect_with_tuned(
        connector: Arc<dyn BinConnector>,
        sink: Arc<dyn ChatDocSink>,
        fetcher: Arc<dyn CheckpointFetcher>,
        device_id: &str,
        initial_cursor: u64,
        tuning: ChatTuning,
    ) -> Result<Self, SyncError> {
        Self::connect_with_transport(
            connector,
            sink,
            fetcher,
            device_id,
            initial_cursor,
            tuning,
            None,
        )
        .await
    }

    pub(crate) async fn connect_with_transport(
        connector: Arc<dyn BinConnector>,
        sink: Arc<dyn ChatDocSink>,
        fetcher: Arc<dyn CheckpointFetcher>,
        device_id: &str,
        initial_cursor: u64,
        tuning: ChatTuning,
        transport: Option<Arc<dyn ChatTransport>>,
    ) -> Result<Self, SyncError> {
        let (events, _) = broadcast::channel(256);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (nudge_tx, nudge_rx) = mpsc::channel(1);
        let (probe_tx, probe_rx) = mpsc::channel(1);
        let (redial_tx, redial_rx) = mpsc::channel(1);
        let (presence_tx, presence_rx) = mpsc::channel(4);
        let shared = Arc::new(Mutex::new(Shared {
            cursor: initial_cursor,
            ..Shared::default()
        }));
        let flags = Arc::new(Flags::default());
        let offline_cancel = CancellationToken::new();
        let offline_task = Arc::new(Mutex::new(None));

        let actor = Actor {
            shared: shared.clone(),
            sink,
            fetcher,
            device_id: device_id.to_string(),
            connector,
            tuning,
            events: events.clone(),
            shutdown: shutdown_rx,
            nudge: nudge_tx.clone(),
            nudge_rx,
            probe_rx,
            redial_rx,
            presence_rx,
            flags: flags.clone(),
            cursor_amnesty_done: std::sync::atomic::AtomicBool::new(false),
            transport,
            sync_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            offline_cancel: offline_cancel.clone(),
            offline_task: offline_task.clone(),
        };
        let task = tokio::spawn(actor.run(ready_tx));

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self {
                shared,
                events,
                shutdown: shutdown_tx,
                nudge: nudge_tx,
                probe: probe_tx,
                redial: redial_tx,
                presence_out: presence_tx,
                flags,
                offline_cancel,
                offline_task,
                task: Some(task),
            }),
            Ok(Err(err)) => {
                offline_cancel.cancel();
                if let Some(offline) = lock(&offline_task).as_ref() {
                    offline.abort();
                }
                task.abort();
                Err(err)
            }
            Err(_) => {
                offline_cancel.cancel();
                if let Some(offline) = lock(&offline_task).as_ref() {
                    offline.abort();
                }
                task.abort();
                Err(SyncError::Closed)
            }
        }
    }

    pub fn events(&self) -> broadcast::Receiver<ChatEvent> {
        self.events.subscribe()
    }

    /// Whether any rows request or WebSocket handshake for this client was
    /// refused with HTTP 403. Sticky for the client's lifetime so callers
    /// can subscribe first and then inspect this value without a gap.
    pub fn access_denied(&self) -> bool {
        use std::sync::atomic::Ordering::Acquire;
        self.flags.access_denied.load(Acquire)
    }

    /// Queue one local update batch for push (a fresh batch id is minted; the
    /// batch survives reconnects until acked — the server dedupes replays).
    ///
    /// Batches over [`MAX_PUSH_BYTES`] are refused here: the server can never
    /// accept them (`MAX_ROW_BYTES`), and a queued-forever batch would replay
    /// on every reconnect — the exact wedge class chat2 replaces. The ops
    /// stay in the local doc and reach peers via the next checkpoint.
    pub fn enqueue_update(&self, bytes: Vec<u8>) {
        if let Err(bytes) = self.try_enqueue_update(bytes) {
            lock(&self.shared).pending.push_back(PendingPush {
                batch_id: uuid::Uuid::new_v4().to_string(),
                bytes,
                sent: false,
            });
            let _ = self.nudge.try_send(());
        }
    }

    /// Non-blocking enqueue for a synchronous document callback. A remote
    /// import can enter the engine sink while holding this client's shared
    /// state; blocking on that state from a local callback that already owns
    /// the engine lifecycle fence would invert the two locks. Callers buffer
    /// the returned bytes externally and retry after the document commit.
    pub fn try_enqueue_update(&self, bytes: Vec<u8>) -> Result<(), Vec<u8>> {
        if bytes.len() > MAX_PUSH_BYTES {
            use std::sync::atomic::Ordering::Relaxed;
            tracing::error!(
                bytes = bytes.len(),
                "chat2: update exceeds the row cap; not queued (post-strip \
                 updates are KB-scale — this is an upstream bug)"
            );
            self.flags.rejected.fetch_add(1, Relaxed);
            let _ = self.events.send(ChatEvent::PushRejected);
            return Ok(());
        }
        let mut shared = match self.shared.try_lock() {
            Ok(shared) => shared,
            Err(std::sync::TryLockError::Poisoned(err)) => err.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return Err(bytes),
        };
        shared.pending.push_back(PendingPush {
            batch_id: uuid::Uuid::new_v4().to_string(),
            bytes,
            sent: false,
        });
        drop(shared);
        let _ = self.nudge.try_send(());
        Ok(())
    }

    /// Stop this client and return every still-unacknowledged local update.
    /// Hosts use this when parking an inaccessible legacy room: re-enqueueing
    /// these bytes after a room-generation cutover is safe because both the
    /// row protocol and Loro imports are at-least-once/idempotent.
    pub fn into_pending_updates(mut self) -> Vec<Vec<u8>> {
        self.offline_cancel.cancel();
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Some(task) = lock(&self.offline_task).as_ref() {
            task.abort();
        }
        lock(&self.shared)
            .pending
            .drain(..)
            .map(|push| push.bytes)
            .collect()
    }

    /// Liveness hint: probe the room now (deadline-checked).
    pub fn probe(&self) {
        let _ = self.probe.try_send(());
    }

    /// Escalation: tear the session down and dial a fresh socket.
    pub fn redial(&self) {
        let _ = self.redial.try_send(());
    }

    /// Non-blocking variant for callers that hold an engine chat-slot guard.
    /// Skipping one cache hint is harmless; blocking here can close a
    /// shared-state -> sink-lifecycle -> chat-slot -> shared-state cycle.
    pub fn try_note_checkpoint(&self, seq_covered: u64, size: u64) -> bool {
        let mut shared = match self.shared.try_lock() {
            Ok(shared) => shared,
            Err(std::sync::TryLockError::Poisoned(err)) => err.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };
        Self::note_checkpoint_locked(&mut shared, seq_covered, size);
        true
    }

    fn note_checkpoint_locked(shared: &mut Shared, seq_covered: u64, size: u64) {
        if let Some(server) = &mut shared.server {
            server.checkpoint_seq = seq_covered;
            server.checkpoint_size = size;
            server.seq_floor = seq_covered;
            server.row_count = 0;
            server.row_bytes = 0;
        }
    }

    pub fn stats(&self) -> ChatStatsSnapshot {
        let shared = lock(&self.shared);
        self.stats_locked(&shared)
    }

    /// Best-effort stats for code that already owns an engine chat-slot
    /// guard. `None` means the actor is updating shared state right now; the
    /// caller should skip this observation and retry on its next tick.
    pub fn try_stats(&self) -> Option<ChatStatsSnapshot> {
        let shared = match self.shared.try_lock() {
            Ok(shared) => shared,
            Err(std::sync::TryLockError::Poisoned(err)) => err.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(self.stats_locked(&shared))
    }

    fn stats_locked(&self, shared: &Shared) -> ChatStatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        let server = shared.server.unwrap_or(wire::StateHeader {
            head_seq: 0,
            seq_floor: 0,
            checkpoint_seq: 0,
            checkpoint_size: 0,
            row_count: 0,
            row_bytes: 0,
            epoch: 0,
        });
        ChatStatsSnapshot {
            connected: self.flags.connected.load(Relaxed),
            server_known: shared.server.is_some(),
            cursor: shared.cursor,
            // The server's honest view — deliberately NOT clamped to the
            // cursor: cursor > headSeq is the reset signal and must stay
            // visible to the observability surface, not be masked by it.
            head_seq: server.head_seq,
            seq_floor: server.seq_floor,
            checkpoint_seq: server.checkpoint_seq,
            checkpoint_size: server.checkpoint_size,
            row_count: server.row_count,
            row_bytes: server.row_bytes,
            pending_pushes: shared.pending.len() as u64,
            rejoins: self.flags.rejoins.load(Relaxed),
            disconnects: self.flags.disconnects.load(Relaxed),
            rejected: self.flags.rejected.load(Relaxed),
            server_resets: self.flags.server_resets.load(Relaxed),
        }
    }

    /// Leave cleanly and stop the actor.
    pub async fn shutdown(mut self) {
        self.offline_cancel.cancel();
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        let offline = lock(&self.offline_task).take();
        if let Some(task) = offline {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ChatClient {
    fn drop(&mut self) {
        self.offline_cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(task) = lock(&self.offline_task).as_ref() {
            task.abort();
        }
    }
}

// ── the actor ───────────────────────────────────────────────────────────────

struct Actor {
    shared: Arc<Mutex<Shared>>,
    sink: Arc<dyn ChatDocSink>,
    fetcher: Arc<dyn CheckpointFetcher>,
    device_id: String,
    connector: Arc<dyn BinConnector>,
    tuning: ChatTuning,
    events: broadcast::Sender<ChatEvent>,
    shutdown: watch::Receiver<bool>,
    /// Self-wake path used when the HTTPS sibling re-arms work while a socket
    /// is already parked in steady state.
    nudge: mpsc::Sender<()>,
    nudge_rx: mpsc::Receiver<()>,
    probe_rx: mpsc::Receiver<()>,
    redial_rx: mpsc::Receiver<()>,
    presence_rx: mpsc::Receiver<(i64, Vec<u8>)>,
    flags: Arc<Flags>,
    /// Once-per-actor cursor amnesty (see run_session): a cursor above the
    /// room's checkpoint is re-verified by refetching the rows above it.
    cursor_amnesty_done: std::sync::atomic::AtomicBool,
    /// Plain-HTTPS pull/push (None = socket-only: tests, dev bearers).
    transport: Option<Arc<dyn ChatTransport>>,
    /// One offline sync in flight at a time.
    sync_busy: Arc<std::sync::atomic::AtomicBool>,
    /// Ownership for the detached HTTP round: dropping/parking this client
    /// cancels and aborts it instead of leaving old transport work alive.
    offline_cancel: CancellationToken,
    offline_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

enum SessionEnd {
    Reconnect,
    Stop,
}

/// How a backoff wait ended.
enum Waited {
    Elapsed,
    /// System wake or a sibling dial succeeded: redial NOW on fresh backoff.
    Woke,
    Shutdown,
}

fn retire_confirmed_http_acks(
    shared: &mut Shared,
    acked: &[(String, u64)],
    confirmed_frontier: u64,
) -> (bool, bool) {
    let mut retired = false;
    let mut unconfirmed = false;
    for (batch_id, seq) in acked {
        if !shared.pending.iter().any(|push| push.batch_id == *batch_id) {
            // The WebSocket sibling (or a permanent verdict) already retired
            // this exact UUID; never let an old HTTP result affect new work.
            continue;
        }
        if *seq <= confirmed_frontier {
            shared.pending.retain(|push| push.batch_id != *batch_id);
            retired = true;
        } else {
            unconfirmed = true;
        }
    }
    (retired, unconfirmed)
}

fn merged_round_acks(
    shared: &Shared,
    http_acks: &[(String, u64)],
    batches: &[(String, Vec<u8>)],
) -> Vec<(String, u64)> {
    batches
        .iter()
        .filter_map(|(batch_id, _)| {
            let http_seq = http_acks
                .iter()
                .filter(|(acked_id, _)| acked_id == batch_id)
                .map(|(_, seq)| *seq)
                .max();
            let ws_seq = shared.offline_ws_acks.get(batch_id).copied();
            http_seq
                .into_iter()
                .chain(ws_seq)
                .max()
                .map(|seq| (batch_id.clone(), seq))
        })
        .collect()
}

/// A complete pull can confirm an ack only when the exact `(seq, batchId)` row
/// is present. A sequence above the new head, a missing row (including one
/// omitted by the request cursor or hidden behind a checkpoint), or a
/// different batch at that sequence means the ack came from another log
/// incarnation or the response is otherwise inconsistent. Checkpoint-frontier
/// containment only proves local document state, not that the server's
/// checkpoint contains this pending batch. Conservatively re-anchor and
/// replay; Loro import and batch-id dedupe make that safe.
fn round_acks_require_reset(response: &HttpRowsResponse, acked: &[(String, u64)]) -> bool {
    acked.iter().any(|(batch_id, seq)| {
        if *seq > response.head_seq {
            return true;
        }
        !response
            .rows
            .iter()
            .any(|(row, _)| row.seq == *seq && row.batch_id == *batch_id)
    })
}

struct OfflineRoundGuard {
    shared: Arc<Mutex<Shared>>,
    pull_reset_version: u64,
    reset_observed: bool,
    batches: Vec<(String, Vec<u8>)>,
    finished: bool,
}

impl OfflineRoundGuard {
    /// Finish while the caller still owns the shared-state lock. This closes
    /// the gap in which a WS ack could otherwise be staged after the final
    /// merge but before Drop cleared the round's snapshot IDs.
    fn finish_locked(&mut self, shared: &mut Shared) -> usize {
        if self.finished {
            return 0;
        }
        let restored = if self.reset_observed || shared.reset_version != self.pull_reset_version {
            restore_http_snapshot_after_reset(shared, &self.batches)
        } else {
            0
        };
        // One set for the whole cleanup: a long offline queue makes a nested
        // scan quadratic under the shared lock (blocking enqueue + the WS
        // actor).
        let batch_ids: HashSet<&str> = self.batches.iter().map(|(id, _)| id.as_str()).collect();
        for batch_id in &batch_ids {
            shared.offline_snapshot_ids.remove(*batch_id);
            shared.offline_ws_acks.remove(*batch_id);
            shared.permanent_rejections.remove(*batch_id);
        }
        // Any snapshot batch still pending was not proven delivered by this
        // round — replay responsibility returns to the WS path. Clearing
        // `sent` makes the follow-up nudge actually re-push it (the nudge
        // path skips already-sent batches).
        for push in shared.pending.iter_mut() {
            if batch_ids.contains(push.batch_id.as_str()) {
                push.sent = false;
            }
        }
        self.finished = true;
        restored
    }

    /// Finish by taking the shared lock. Failure paths call this BEFORE their
    /// follow-up nudge: relying on Drop would race the nudged WS actor against
    /// the not-yet-restored replay state.
    fn finish(&mut self) {
        let shared = self.shared.clone();
        let mut shared = lock(&shared);
        self.finish_locked(&mut shared);
    }
}

impl Drop for OfflineRoundGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Restore the replay copies captured before this HTTP round. An ack that
/// arrived before reset evidence is no longer proof that the row survived the
/// reset; permanent verdicts are the sole exception. Snapshot batches retain
/// their original order and stay ahead of work enqueued during the round.
fn restore_http_snapshot_after_reset(shared: &mut Shared, batches: &[(String, Vec<u8>)]) -> usize {
    let mut restored = 0usize;
    let mut replay = VecDeque::new();
    for (batch_id, bytes) in batches {
        if shared.permanent_rejections.contains(batch_id) {
            continue;
        }
        if let Some(index) = shared
            .pending
            .iter()
            .position(|push| push.batch_id == *batch_id)
        {
            replay.push_back(
                shared
                    .pending
                    .remove(index)
                    .expect("position came from the same queue"),
            );
        } else {
            replay.push_back(PendingPush {
                batch_id: batch_id.clone(),
                bytes: bytes.clone(),
                sent: false,
            });
            restored += 1;
        }
    }
    replay.append(&mut shared.pending);
    shared.pending = replay;
    restored
}

async fn offline_sync_once(
    transport: Arc<dyn ChatTransport>,
    shared: Arc<Mutex<Shared>>,
    sink: Arc<dyn ChatDocSink>,
    fetcher: Arc<dyn CheckpointFetcher>,
    events: broadcast::Sender<ChatEvent>,
    flags: Arc<Flags>,
    nudge: mpsc::Sender<()>,
) {
    use std::sync::atomic::Ordering::Relaxed;

    // This is the only cursor the GET may use. A POST ack can sit above unseen
    // foreign rows and therefore is not a pull frontier by itself.
    let (pull_since, pull_reset_version, batches): (u64, u64, Vec<(String, Vec<u8>)>) = {
        let mut shared = lock(&shared);
        let batches: Vec<_> = shared
            .pending
            .iter()
            .map(|push| (push.batch_id.clone(), push.bytes.clone()))
            .collect();
        shared
            .offline_snapshot_ids
            .extend(batches.iter().map(|(batch_id, _)| batch_id.clone()));
        (shared.cursor, shared.reset_version, batches)
    };
    let mut round_guard = OfflineRoundGuard {
        shared: shared.clone(),
        pull_reset_version,
        reset_observed: false,
        batches,
        finished: false,
    };
    let mut acked = Vec::new();
    let mut push_failed = false;
    for (batch_id, bytes) in &round_guard.batches {
        match transport.push(batch_id.clone(), bytes.clone()).await {
            Ok(body) => match serde_json::from_str::<wire::AckHeader>(&body) {
                Ok(ack) if ack.batch_id == *batch_id && ack.seq > 0 => {
                    acked.push((ack.batch_id, ack.seq));
                }
                _ => {
                    tracing::warn!(batch = %batch_id,
                        "chat2: malformed/mismatched http push ack; will retry");
                    push_failed = true;
                    break;
                }
            },
            Err(SyncError::PushRejected(code)) => {
                // A permanent verdict is the one safe pre-pull retirement: no
                // future retry can land this exact payload. Continue so a bad
                // head (notably an oversized handoff full update) cannot wedge
                // later small deltas behind it.
                let dropped = {
                    let mut shared = lock(&shared);
                    shared.permanent_rejections.insert(batch_id.clone());
                    let before = shared.pending.len();
                    shared.pending.retain(|push| push.batch_id != *batch_id);
                    let dropped = before != shared.pending.len();
                    if dropped && !shared.pending.is_empty() {
                        shared.retry_at = Some(tokio::time::Instant::now());
                    }
                    dropped
                };
                if dropped {
                    flags.rejected.fetch_add(1, Relaxed);
                    tracing::error!(batch = %batch_id, code,
                        "chat2: HTTP batch permanently rejected; retired and continuing");
                    let _ = events.send(ChatEvent::PushRejected);
                }
            }
            Err(err) => {
                signal_if_access_denied(&err, &flags, &events);
                tracing::warn!(error = %err, "chat2: http push failed; will retry");
                push_failed = true;
                break;
            }
        }
    }

    let response = match fetch_http_rows(transport.as_ref(), pull_since).await {
        Ok(response) => response,
        Err(HttpPullError::Transport(err)) => {
            signal_if_access_denied(&err, &flags, &events);
            tracing::warn!(error = %err, "chat2: http pull failed; will retry");
            round_guard.finish();
            let _ = nudge.try_send(());
            return;
        }
        Err(HttpPullError::Malformed(err)) => {
            tracing::warn!(error = %err, "chat2: incomplete/malformed http pull; will retry");
            round_guard.finish();
            let _ = nudge.try_send(());
            return;
        }
    };
    if response.head_seq != response.state.head_seq {
        tracing::warn!(
            state_head = response.state.head_seq,
            done_head = response.head_seq,
            "chat2: HTTP STATE/ROWS_DONE frontier mismatch; will retry"
        );
        round_guard.finish();
        let _ = nudge.try_send(());
        return;
    }
    let cursor_reset_observed = response.state.head_seq < pull_since;
    // capturedCursor=0/head=0 cannot express a backwards comparison. Merge
    // HTTP POST acks with WS acks staged against this snapshot, then require
    // the complete GET to prove both their sequence and batch identity.
    let merged_acks = {
        let shared = lock(&shared);
        merged_round_acks(&shared, &acked, &round_guard.batches)
    };
    let mut ack_reset_observed = round_acks_require_reset(&response, &merged_acks);
    let mut reset_observed = cursor_reset_observed || ack_reset_observed;
    round_guard.reset_observed = reset_observed;
    if cursor_reset_observed && !response.rows.is_empty() {
        tracing::warn!(
            pull_since,
            head_seq = response.head_seq,
            "chat2: reset response unexpectedly contains rows beyond the old cursor"
        );
        round_guard.finish();
        let _ = nudge.try_send(());
        return;
    }
    let contained =
        response.state.checkpoint_size == 0 || sink.contains_frontier(&response.frontier);
    let ordinary_plan = plan_catch_up(pull_since, &response.state, contained);
    let reset_plan = plan_catch_up(0, &response.state, contained);
    let initial_plan = if reset_observed {
        reset_plan
    } else {
        ordinary_plan
    };
    let initial_after = match initial_plan {
        CatchUpPlan::RowsOnly { after } | CatchUpPlan::CheckpointThenRows { after } => after,
    };

    // A reset response was fetched with the old, now-meaningless `after`, so
    // its lack of rows cannot prove the new frontier. It is still authoritative
    // evidence for the exact cursor reset; the self-nudge fetches from `after`
    // next. Normal responses must prove every sequence through ROWS_DONE.
    let verified_frontier = if reset_observed {
        None
    } else {
        match validate_http_rows_frontier(&response, pull_since, initial_after) {
            Ok(frontier) => Some(frontier),
            Err(err) => {
                tracing::warn!(error = %err, "chat2: http pull frontier is incomplete; will retry");
                round_guard.finish();
                let _ = nudge.try_send(());
                return;
            }
        }
    };

    let checkpoint = if matches!(initial_plan, CatchUpPlan::CheckpointThenRows { .. }) {
        match tokio::time::timeout(CHECKPOINT_FETCH_DEADLINE, fetcher.fetch()).await {
            Ok(Ok(bytes)) => Some(bytes),
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "chat2: http checkpoint fetch failed; will retry");
                round_guard.finish();
                let _ = nudge.try_send(());
                return;
            }
            Err(_) => {
                tracing::warn!("chat2: http checkpoint fetch timed out; will retry");
                round_guard.finish();
                let _ = nudge.try_send(());
                return;
            }
        }
    } else {
        None
    };

    let mut shared = lock(&shared);
    if shared.reset_version != pull_reset_version {
        // A sibling already crossed a reset boundary. Sequence numbers in this
        // response (and every staged pre-boundary ack) are no longer comparable.
        // Restore snapshot batches that a pre-reset WS ack may have removed.
        let restored = round_guard.finish_locked(&mut shared);
        drop(shared);
        if restored > 0 {
            tracing::warn!(restored, "chat2: restored pre-reset HTTP snapshot batches");
        }
        let _ = nudge.try_send(());
        return;
    }

    // Cover WS acks that arrived while a checkpoint was being fetched. The
    // checkpoint decision itself is independent of the starting cursor, so a
    // newly discovered reset can safely switch from the ordinary plan to the
    // already-fetched reset plan here.
    let merged_acks = merged_round_acks(&shared, &acked, &round_guard.batches);
    let final_ack_reset_observed = round_acks_require_reset(&response, &merged_acks);
    if final_ack_reset_observed {
        ack_reset_observed = true;
        reset_observed = true;
        round_guard.reset_observed = true;
    }
    let plan = if reset_observed {
        reset_plan
    } else {
        ordinary_plan
    };
    let after = match plan {
        CatchUpPlan::RowsOnly { after } | CatchUpPlan::CheckpointThenRows { after } => after,
    };
    if shared.cursor != pull_since && !reset_observed {
        // The WebSocket sibling overtook this GET. Its continuous cursor can
        // confirm staged acks in the same reset generation, but stale HTTP rows
        // must never be allowed to move sink persistence backwards.
        let current = shared.cursor;
        let (retired, unconfirmed) = retire_confirmed_http_acks(&mut shared, &merged_acks, current);
        if retired {
            sink.advance_cursor(current);
        }
        let needs_nudge = push_failed || unconfirmed || response.head_seq > current;
        round_guard.finish_locked(&mut shared);
        drop(shared);
        if retired {
            let _ = events.send(ChatEvent::Applied);
        }
        if needs_nudge {
            let _ = nudge.try_send(());
        }
        return;
    }

    shared.server = Some(response.state);
    if reset_observed {
        if let Some(bytes) = checkpoint {
            if let Err(err) = sink.apply_checkpoint(&bytes, response.state.checkpoint_seq) {
                drop(shared);
                tracing::warn!(error = %err, "chat2: reset checkpoint import failed; will retry");
                // finish() before the nudge: it restores replay eligibility
                // (clears `sent`), so the woken WS actor re-pushes instead of
                // skipping a still-`sent` batch that nothing would wake again.
                round_guard.finish();
                let _ = nudge.try_send(());
                return;
            }
        }
        let restored = round_guard.finish_locked(&mut shared);
        shared.cursor = after;
        shared.reset_version = shared.reset_version.wrapping_add(1);
        sink.reset_cursor(after);
        drop(shared);
        flags.server_resets.fetch_add(1, Relaxed);
        tracing::warn!(
            pull_since,
            head_seq = response.state.head_seq,
            after,
            ack_reset_observed,
            restored,
            "chat2: HTTP pull observed a room reset; pending pushes retained"
        );
        let _ = events.send(ChatEvent::ServerReset);
        let _ = events.send(ChatEvent::Applied);
        let _ = nudge.try_send(());
        return;
    }

    let mut applied = false;
    if let Some(bytes) = checkpoint {
        if let Err(err) = sink.apply_checkpoint(&bytes, response.state.checkpoint_seq) {
            drop(shared);
            tracing::warn!(error = %err, "chat2: http checkpoint import failed; will retry");
            // finish() before the nudge (see the reset-checkpoint path above):
            // restores replay eligibility so the woken WS actor re-pushes.
            round_guard.finish();
            let _ = nudge.try_send(());
            return;
        }
        shared.cursor = shared.cursor.max(after);
        applied = true;
    }
    for (row, payload) in response.rows {
        // CONTIGUITY RULE (same as the WS ROW path): pulls request
        // `after = pull_since` so rows arrive contiguous — but hold the rule
        // anyway. A jump (trimmed log, server surprise) must not stamp the
        // cursor over rows the doc never saw; apply the bytes and let
        // `maybe_repair_gap` walk the missing span.
        if row.seq <= shared.cursor + 1 {
            shared.cursor = shared.cursor.max(row.seq);
        } else {
            shared.gap_repair = true;
            tracing::warn!(
                seq = row.seq,
                cursor = shared.cursor,
                "chat2: http pull row gap; holding cursor and requesting backfill"
            );
        }
        sink.apply_row(&payload, shared.cursor);
        applied = true;
    }
    let verified_frontier = verified_frontier.expect("normal response validated a frontier");
    if shared.cursor < verified_frontier {
        // A contained checkpoint can cover a trimmed prefix without producing
        // a row callback. Persist that proven cursor without promoting epoch.
        shared.cursor = verified_frontier;
        sink.reset_cursor(verified_frontier);
        applied = true;
    }
    let (retired, unconfirmed) =
        retire_confirmed_http_acks(&mut shared, &merged_acks, verified_frontier);
    if retired {
        sink.advance_cursor(shared.cursor);
    }
    let needs_nudge = push_failed || unconfirmed;
    round_guard.finish_locked(&mut shared);
    drop(shared);
    if applied || retired {
        let _ = events.send(ChatEvent::Applied);
    }
    if needs_nudge {
        let _ = nudge.try_send(());
    }
}

impl Actor {
    async fn run(mut self, ready: oneshot::Sender<Result<(), SyncError>>) {
        let mut ready = Some(ready);
        let mut backoff = BACKOFF_BASE;
        // Pull-first bootstrap (see registry.rs run): with an HTTPS
        // transport, construction resolves immediately and an HTTP pull
        // converges the doc in ~1 RTT while the socket spends its 4+.
        if self.transport.is_some() {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Ok(()));
            }
            self.spawn_offline_sync();
        }
        // Suspend/resume and sibling-dial successes are EVENTS that end a
        // backoff wait immediately (see room.rs) — without them a recovered
        // network still waited out the full accumulated delay.
        let mut wake = crate::wake::subscribe();
        let mut online = crate::wake::subscribe_online();
        loop {
            if *self.shutdown.borrow() {
                return;
            }
            let attempt = self
                .flags
                .dial_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            let dial = tokio::time::timeout(CONNECT_TIMEOUT, self.connector.connect()).await;
            let pipe = match dial {
                Ok(Ok(pipe)) => pipe,
                Ok(Err(err)) => {
                    signal_if_access_denied(&err, &self.flags, &self.events);
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(err));
                        return; // first join failed: caller owns the retry
                    }
                    tracing::warn!(error = %err, attempt, "chat2 dial failed; backing off");
                    self.spawn_offline_sync();
                    match self.wait_backoff(&mut wake, &mut online, backoff).await {
                        Waited::Shutdown => return,
                        Waited::Woke => backoff = BACKOFF_BASE,
                        Waited::Elapsed => backoff = (backoff * 2).min(BACKOFF_CAP),
                    }
                    continue;
                }
                Err(_) => {
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(SyncError::WebSocket("connect timeout".into())));
                        return;
                    }
                    tracing::warn!(attempt, "chat2 dial timed out; backing off");
                    self.spawn_offline_sync();
                    match self.wait_backoff(&mut wake, &mut online, backoff).await {
                        Waited::Shutdown => return,
                        Waited::Woke => backoff = BACKOFF_BASE,
                        Waited::Elapsed => backoff = (backoff * 2).min(BACKOFF_CAP),
                    }
                    continue;
                }
            };

            let session_started = tokio::time::Instant::now();
            match self.run_session(pipe, &mut ready).await {
                SessionEnd::Stop => return,
                SessionEnd::Reconnect => {
                    use std::sync::atomic::Ordering::Relaxed;
                    // Only a session that joined AND stayed healthy for a
                    // while earns a fresh backoff. Reset-on-join alone let a
                    // connect-and-die socket hot-loop at 250ms forever;
                    // without any reset, ~7 flaps pinned every future
                    // reconnect at the cap for the life of the client.
                    let joined = self.flags.connected.swap(false, Relaxed);
                    self.flags.disconnects.fetch_add(1, Relaxed);
                    let _ = self.events.send(ChatEvent::Disconnected);
                    if ready.is_some() {
                        if let Some(ready) = ready.take() {
                            let _ = ready
                                .send(Err(SyncError::Protocol("chat2 handshake failed".into())));
                        }
                        return;
                    }
                    if joined && session_started.elapsed() >= STABLE_RESET {
                        backoff = BACKOFF_BASE;
                    }
                    self.spawn_offline_sync();
                    match self.wait_backoff(&mut wake, &mut online, backoff).await {
                        Waited::Shutdown => return,
                        Waited::Woke => backoff = BACKOFF_BASE,
                        Waited::Elapsed => backoff = (backoff * 2).min(BACKOFF_CAP),
                    }
                }
            }
        }
    }

    /// Sleep out one backoff, cut short by system wake, a sibling dial
    /// success, or shutdown. While the OS reports no network path, the wait
    /// parks on the event buses (with a coarse safety timer) instead of
    /// burning dial attempts that cannot succeed.
    async fn wait_backoff(
        &mut self,
        wake: &mut tokio::sync::broadcast::Receiver<()>,
        online: &mut tokio::sync::broadcast::Receiver<()>,
        wait: Duration,
    ) -> Waited {
        // Drain stale events: only wakes/successes DURING this wait count,
        // or our own last dial would cut every wait to zero.
        while wake.try_recv().is_ok() {}
        while online.try_recv().is_ok() {}
        let wait = if crate::wake::path_is_offline() {
            wait.max(OFFLINE_PARK_RECHECK)
        } else {
            wait
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => Waited::Elapsed,
            _ = wake.recv() => Waited::Woke,
            _ = online.recv() => Waited::Woke,
            _ = self.shutdown.changed() => {
                if *self.shutdown.borrow() {
                    Waited::Shutdown
                } else {
                    Waited::Elapsed
                }
            }
        }
    }

    async fn run_session(
        &mut self,
        mut pipe: BinPipe,
        ready: &mut Option<oneshot::Sender<Result<(), SyncError>>>,
    ) -> SessionEnd {
        use std::sync::atomic::Ordering::Relaxed;

        // ── hello / state ───────────────────────────────────────────────────
        let (requested_cursor, requested_reset_version) = {
            let shared = lock(&self.shared);
            (shared.cursor, shared.reset_version)
        };
        let hello = wire::encode(
            frame_type::HELLO,
            &wire::HelloHeader {
                cursor: requested_cursor,
                device: &self.device_id,
            },
            &[],
        );
        if pipe.tx.send(hello).await.is_err() {
            return SessionEnd::Reconnect;
        }
        let state = tokio::time::timeout(HELLO_DEADLINE, async {
            loop {
                let bytes = pipe.rx.recv().await?;
                let Some(frame) = wire::decode(&bytes) else {
                    tracing::warn!("chat2: bad frame during handshake");
                    return None;
                };
                if frame.kind == frame_type::STATE {
                    return Some(frame);
                }
                // Stale broadcast before our state: skip.
            }
        })
        .await;
        let Ok(Some(state_frame)) = state else {
            tracing::warn!("chat2: no state frame within deadline");
            return SessionEnd::Reconnect;
        };
        let Ok(state) = serde_json::from_value::<wire::StateHeader>(state_frame.header.clone())
        else {
            tracing::warn!("chat2: malformed state header");
            return SessionEnd::Reconnect;
        };
        {
            let shared = lock(&self.shared);
            if shared.reset_version != requested_reset_version || shared.cursor != requested_cursor
            {
                tracing::debug!(
                    requested_cursor,
                    current = shared.cursor,
                    "chat2: skipping WS state overtaken by sibling transport"
                );
                return SessionEnd::Reconnect;
            }
        }
        lock(&self.shared).server = Some(state);
        // Server behind our cursor = the room was reset/wiped. Detect on the
        // RAW persisted cursor, BEFORE the amnesty below rewrites it —
        // plan_catch_up treats the cursor as fresh; SURFACE the signal too:
        // the host's re-seed recovery (chat-room.ts /reset) hangs off this
        // event, and masking it was exactly how the s2 wedge class stayed
        // invisible.
        if lock(&self.shared).cursor > state.head_seq {
            self.flags.server_resets.fetch_add(1, Relaxed);
            tracing::warn!(
                cursor = lock(&self.shared).cursor,
                head_seq = state.head_seq,
                "chat2: server lost state (headSeq < cursor) — treating as \
                 fresh; host should re-seed via checkpoint"
            );
            let _ = self.events.send(ChatEvent::ServerReset);
        }
        // Cursor amnesty, once per client: a cursor above the checkpoint seq
        // claims history the doc may have silently parked and dropped —
        // parked imports vanish on export while the cursor advances, and
        // nothing ever re-reads below the cursor ("Add Tweets" wedge:
        // cursor 75 over a checkpoint-only doc, 2026-08-18). Clamp and
        // refetch: re-imports are no-ops and the trim policy bounds the
        // cost to the rows since the last checkpoint.
        if !self.cursor_amnesty_done.swap(true, Relaxed) {
            // Checkpoint-less rooms amnesty to ZERO: the same parked-import
            // wedge (empty doc under an advanced cursor — 2026-08-19: a live
            // broadcast mid-join outran the backfill and the cursor skipped
            // the hole) with no checkpoint to clamp to. Refetching the whole
            // log is bounded by the checkpoint threshold policy (a room
            // past ~200 rows/512KB HAS a checkpoint) and re-imports are
            // no-ops, so this is the cheap universal heal.
            let clamp_to = if state.checkpoint_size > 0 {
                state.checkpoint_seq
            } else {
                0
            };
            let mut shared = lock(&self.shared);
            if shared.cursor > clamp_to {
                tracing::info!(
                    from = shared.cursor,
                    to = clamp_to,
                    "chat2: cursor amnesty — refetching rows the doc may have parked"
                );
                // `clamp_to`, not `checkpoint_seq`: with no checkpoint at all
                // the honest floor is 0 — anchoring at a seq no checkpoint
                // covers leaves the skipped prefix unreachable.
                shared.cursor = clamp_to;
                shared.reset_version = shared.reset_version.wrapping_add(1);
                self.sink.reset_cursor(clamp_to);
            }
        }
        let cursor = lock(&self.shared).cursor;
        self.flags.connected.store(true, Relaxed);
        if ready.is_none() {
            self.flags.rejoins.fetch_add(1, Relaxed);
        }
        let _ = self.events.send(ChatEvent::Connected);

        // ── catch-up: checkpoint precision + row backfill ───────────────────
        // Same presence rule as `plan_catch_up`: SIZE, not seq — a seeded
        // room's checkpoint covers seq 0 (see the decision-table test).
        let contained =
            state.checkpoint_size == 0 || self.sink.contains_frontier(&state_frame.payload);
        let plan = plan_catch_up(cursor, &state, contained);
        let after = match plan {
            CatchUpPlan::RowsOnly { after } => after,
            CatchUpPlan::CheckpointThenRows { after } => after,
        };
        // The plan's `after` IS the cursor now — down (server reset / amnesty
        // already applied) or UP (a contained checkpoint covers the skipped
        // span). Without the raise, the backfill's first row (`after + 1`)
        // reads as a contiguity gap against a stale lower cursor. Either
        // direction is a non-monotonic jump, so it fences the in-flight HTTP
        // sibling via `reset_version`; an unchanged cursor must NOT bump, or
        // every ordinary rejoin would discard a healthy sibling round.
        if after != cursor {
            let mut shared = lock(&self.shared);
            // No await occurred since `cursor` was read, but the HTTP sibling
            // may still have overtaken this handshake on another task.
            if shared.cursor != cursor {
                return SessionEnd::Reconnect;
            }
            shared.cursor = after;
            shared.reset_version = shared.reset_version.wrapping_add(1);
            self.sink.reset_cursor(after);
        }
        let session_reset_version = lock(&self.shared).reset_version;
        // Rows request goes out BEFORE any checkpoint fetch: the row backfill
        // streams over the socket while the checkpoint downloads over HTTP in
        // parallel, instead of serializing download → request → backfill. On
        // a thin link the checkpoint download IS the join time, and every
        // byte of it used to push the row stream (and "ready") back by that
        // much. Rows received mid-fetch are buffered and applied after the
        // checkpoint imports — row seqs are all > checkpointSeq, so ordering
        // is preserved, and the persisted cursor can't advance past state it
        // doesn't contain because nothing applies until the import lands.
        let rows_req = wire::encode(frame_type::ROWS_REQ, &wire::RowsReqHeader { after }, &[]);
        if pipe.tx.send(rows_req).await.is_err() {
            return SessionEnd::Reconnect;
        }
        // Pending pushes ALSO go before catch-up completes: the server's
        // batchId dedupe makes replays no-ops and rows are CRDT-commutative,
        // so a message written on a dead network flushes ~2 RTTs after the
        // socket lands instead of waiting out a whole checkpoint download +
        // backfill ("typing works even when load doesn't").
        if !self.push_pending(&mut pipe, true).await {
            return SessionEnd::Reconnect;
        }
        let mut buffered: Vec<wire::WireFrame> = Vec::new();
        if let CatchUpPlan::CheckpointThenRows { .. } = plan {
            let (checkpoint_base_cursor, checkpoint_reset_version) = {
                let shared = lock(&self.shared);
                (shared.cursor, shared.reset_version)
            };
            tracing::info!(
                checkpoint_seq = state.checkpoint_seq,
                checkpoint_size = state.checkpoint_size,
                "chat2: fetching checkpoint (rows streaming in parallel)"
            );
            // Deadline + shutdown-interruptible: a hung fetch (half-open
            // TCP, stalled link) must neither pin the actor forever nor
            // block `shutdown()`. The fetch is Range-resumable, so the
            // redial retries from wherever the bytes stopped. The socket is
            // drained (into the buffer) for the whole fetch so backpressure
            // can't stall the server's row stream.
            let fetch = self.fetcher.fetch();
            tokio::pin!(fetch);
            let deadline = tokio::time::sleep(CHECKPOINT_FETCH_DEADLINE);
            tokio::pin!(deadline);
            let bytes = loop {
                tokio::select! {
                    fetched = &mut fetch => match fetched {
                        Ok(bytes) => break bytes,
                        Err(err) => {
                            tracing::warn!(error = %err, "chat2: checkpoint fetch failed");
                            return SessionEnd::Reconnect;
                        }
                    },
                    _ = &mut deadline => {
                        tracing::warn!("chat2: checkpoint fetch timed out; redialing");
                        return SessionEnd::Reconnect;
                    }
                    _ = self.shutdown.changed() => return SessionEnd::Stop,
                    inbound = pipe.rx.recv() => match inbound {
                        Some(raw) => match wire::decode(&raw) {
                            Some(frame) => buffered.push(frame),
                            None => {
                                tracing::warn!("chat2: unparseable frame during checkpoint fetch");
                                return SessionEnd::Reconnect;
                            }
                        },
                        None => return SessionEnd::Reconnect,
                    }
                }
            };
            let mut shared = lock(&self.shared);
            if shared.cursor != checkpoint_base_cursor
                || shared.reset_version != checkpoint_reset_version
            {
                tracing::debug!(
                    checkpoint_base_cursor,
                    current = shared.cursor,
                    "chat2: discarding checkpoint response overtaken by sibling transport"
                );
                return SessionEnd::Reconnect;
            }
            if let Err(err) = self.sink.apply_checkpoint(&bytes, state.checkpoint_seq) {
                tracing::warn!(error = %err, "chat2: checkpoint import failed");
                return SessionEnd::Reconnect;
            }
            shared.cursor = shared.cursor.max(state.checkpoint_seq);
            drop(shared);
            let _ = self.events.send(ChatEvent::Applied);
        }
        // Frames buffered during the fetch replay first, then the live socket
        // finishes the backfill — one pass, same ROWS_DONE terminator either
        // way.
        let mut head_seq: Option<u64> = None;
        for frame in buffered.drain(..) {
            if head_seq.is_none() && frame.kind == frame_type::ROWS_DONE {
                let Ok(done) = serde_json::from_value::<wire::RowsDoneHeader>(frame.header) else {
                    return SessionEnd::Reconnect;
                };
                head_seq = Some(done.head_seq);
                continue; // frames after ROWS_DONE are steady-state; keep them
            }
            if !self.handle_frame(frame, session_reset_version) {
                return SessionEnd::Reconnect;
            }
        }
        let head_seq = match head_seq {
            Some(seq) => seq,
            None => {
                let backfill = tokio::time::timeout(BACKFILL_DEADLINE, async {
                    loop {
                        let bytes = pipe.rx.recv().await?;
                        let Some(frame) = wire::decode(&bytes) else {
                            return None;
                        };
                        match frame.kind {
                            frame_type::ROWS_DONE => {
                                let done: wire::RowsDoneHeader =
                                    serde_json::from_value(frame.header).ok()?;
                                return Some(done.head_seq);
                            }
                            _ => {
                                if !self.handle_frame(frame, session_reset_version) {
                                    return None;
                                }
                            }
                        }
                    }
                })
                .await;
                let Ok(Some(seq)) = backfill else {
                    tracing::warn!("chat2: backfill did not complete");
                    return SessionEnd::Reconnect;
                };
                seq
            }
        };
        if let Some(ready) = ready.take() {
            let _ = ready.send(Ok(()));
        }
        let _ = self.events.send(ChatEvent::CaughtUp { head_seq });

        // ── steady state ────────────────────────────────────────────────────
        let mut last_frame = tokio::time::Instant::now();
        let mut probe_deadline: Option<tokio::time::Instant> = None;
        // Row-gap repairs this session (see `Shared::gap_repair`): a live
        // frame during the backfill above may already have flagged one.
        let mut gap_repairs = 0u32;
        if !self.maybe_repair_gap(&mut pipe, &mut gap_repairs).await {
            return SessionEnd::Reconnect;
        }
        loop {
            let quiet_probe_at = last_frame + self.tuning.probe_quiet;
            let deadline_at = probe_deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));
            let retry_at = lock(&self.shared)
                .retry_at
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));
            tokio::select! {
                frame = pipe.rx.recv() => {
                    let Some(bytes) = frame else {
                        return SessionEnd::Reconnect;
                    };
                    last_frame = tokio::time::Instant::now();
                    probe_deadline = None;
                    let Some(frame) = wire::decode(&bytes) else {
                        tracing::warn!("chat2: unparseable frame");
                        return SessionEnd::Reconnect;
                    };
                    if !self.handle_frame(frame, session_reset_version) {
                        return SessionEnd::Reconnect;
                    }
                    if !self.maybe_repair_gap(&mut pipe, &mut gap_repairs).await {
                        return SessionEnd::Reconnect;
                    }
                }
                _ = self.nudge_rx.recv() => {
                    if lock(&self.shared).reset_version != session_reset_version {
                        return SessionEnd::Reconnect;
                    }
                    if !self.push_pending(&mut pipe, false).await {
                        return SessionEnd::Reconnect;
                    }
                }
                beat = self.presence_rx.recv() => {
                    if let Some((at, payload)) = beat {
                        let frame = wire::encode(
                            frame_type::PRESENCE,
                            &wire::PresenceOutHeader { at },
                            &payload,
                        );
                        if pipe.tx.send(frame).await.is_err() {
                            return SessionEnd::Reconnect;
                        }
                    }
                }
                _ = self.probe_rx.recv() => {
                    if !self.send_probe(&mut pipe, &mut probe_deadline).await {
                        return SessionEnd::Reconnect;
                    }
                }
                _ = self.redial_rx.recv() => {
                    tracing::info!("chat2: redial requested");
                    return SessionEnd::Reconnect;
                }
                // Transient (quota) rejection: probe with the HEAD batch on
                // a short clock (see `Shared::quota_blocked`); acks re-arm
                // the clock so the queue drains one-per-grant.
                _ = tokio::time::sleep_until(retry_at) => {
                    lock(&self.shared).retry_at = None;
                    if !self.push_head(&mut pipe).await {
                        return SessionEnd::Reconnect;
                    }
                }
                _ = tokio::time::sleep_until(quiet_probe_at) => {
                    if !self.send_probe(&mut pipe, &mut probe_deadline).await {
                        return SessionEnd::Reconnect;
                    }
                    last_frame = tokio::time::Instant::now();
                }
                _ = tokio::time::sleep_until(deadline_at) => {
                    tracing::warn!("chat2: probe unanswered past deadline; redialing");
                    return SessionEnd::Reconnect;
                }
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() {
                        return SessionEnd::Stop;
                    }
                }
            }
        }
    }

    async fn send_probe(
        &self,
        pipe: &mut BinPipe,
        probe_deadline: &mut Option<tokio::time::Instant>,
    ) -> bool {
        let frame = wire::encode(frame_type::PROBE, &serde_json::json!({}), &[]);
        if pipe.tx.send(frame).await.is_err() {
            return false;
        }
        if probe_deadline.is_none() {
            *probe_deadline = Some(tokio::time::Instant::now() + PROBE_DEADLINE);
        }
        true
    }

    /// Send only the queue's head batch — the quota-probe path.
    async fn push_head(&self, pipe: &mut BinPipe) -> bool {
        let frame = {
            let mut shared = lock(&self.shared);
            shared.pending.front_mut().map(|push| {
                push.sent = true;
                wire::encode(
                    frame_type::PUSH,
                    &wire::PushHeader {
                        batch_id: &push.batch_id,
                    },
                    &push.bytes,
                )
            })
        };
        match frame {
            Some(frame) => pipe.tx.send(frame).await.is_ok(),
            None => true,
        }
    }

    /// One HTTPS sync cycle off the critical path. POST acks are staged until
    /// a strict GET from the pre-push cursor proves the complete resulting
    /// frontier; only then may they retire the sole replay copy of a batch.
    fn spawn_offline_sync(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(transport) = self.transport.clone() else {
            return;
        };
        if self.sync_busy.swap(true, Relaxed) {
            return;
        }
        let shared = self.shared.clone();
        let sink = self.sink.clone();
        let fetcher = self.fetcher.clone();
        let events = self.events.clone();
        let flags = self.flags.clone();
        let nudge = self.nudge.clone();
        let busy = self.sync_busy.clone();
        let cancel = self.offline_cancel.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = offline_sync_once(transport, shared, sink, fetcher, events, flags, nudge) => {}
            }
            busy.store(false, Relaxed);
        });
        *lock(&self.offline_task) = Some(task);
    }

    /// If a row/ack gap was flagged, request a backfill from the honest
    /// cursor. Bounded per session: a gap the server can't fill (should be
    /// impossible below the checkpoint floor) forces a redial, whose full
    /// catch-up is the stronger repair.
    async fn maybe_repair_gap(&self, pipe: &mut BinPipe, repairs: &mut u32) -> bool {
        const MAX_GAP_REPAIRS_PER_SESSION: u32 = 3;
        let (repair, after) = {
            let mut shared = lock(&self.shared);
            (std::mem::take(&mut shared.gap_repair), shared.cursor)
        };
        if !repair {
            return true;
        }
        *repairs += 1;
        if *repairs > MAX_GAP_REPAIRS_PER_SESSION {
            tracing::warn!("chat2: gap repairs exhausted; redialing for a full catch-up");
            return false;
        }
        tracing::info!(
            after,
            attempt = *repairs,
            "chat2: backfilling over a row gap"
        );
        let req = wire::encode(frame_type::ROWS_REQ, &wire::RowsReqHeader { after }, &[]);
        pipe.tx.send(req).await.is_ok()
    }

    /// Push queued batches. The nudge path (`replay_all=false`) sends only
    /// batches never pushed on THIS socket — the storm fix: re-pushing the
    /// whole queue on every enqueue (8/s during streaming × queue depth)
    /// livelocked the server's per-device quota (2026-08-20: 15k rejections/
    /// min). A fresh socket (`replay_all=true`) still resends everything (the
    /// server dedupes by batchId), preserving delivery when a socket died
    /// mid-flight. The quota drain itself is unchanged: a `quota` rejection
    /// arms the head-probe retry clock, which drains one batch per grant.
    async fn push_pending(&self, pipe: &mut BinPipe, replay_all: bool) -> bool {
        let frames: Vec<Vec<u8>> = {
            let mut shared = lock(&self.shared);
            shared
                .pending
                .iter_mut()
                .filter(|push| replay_all || !push.sent)
                .map(|push| {
                    push.sent = true;
                    wire::encode(
                        frame_type::PUSH,
                        &wire::PushHeader {
                            batch_id: &push.batch_id,
                        },
                        &push.bytes,
                    )
                })
                .collect()
        };
        for frame in frames {
            if pipe.tx.send(frame).await.is_err() {
                return false;
            }
        }
        true
    }

    /// Apply one inbound protocol frame. False = protocol breakdown, redial.
    fn handle_frame(&self, frame: wire::WireFrame, session_reset_version: u64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if lock(&self.shared).reset_version != session_reset_version {
            tracing::debug!("chat2: discarding WS frame from an overtaken reset generation");
            return false;
        }
        match frame.kind {
            frame_type::ROW => {
                let Ok(row) = serde_json::from_value::<wire::RowHeader>(frame.header) else {
                    return false;
                };
                // Own-device rows also arrive (the server never filters
                // replay rows) — Loro re-import is a no-op; the cursor
                // advance is what matters.
                // CONTIGUITY RULE: the cursor claims "every row ≤ cursor is
                // reflected in the doc", so it may only walk, never jump. A
                // gap means rows we never received (live broadcast mid-join)
                // — apply the bytes (loro parks dependents harmlessly), keep
                // the honest cursor, and ask for a backfill repair.
                let Some(effective) = ({
                    let mut shared = lock(&self.shared);
                    if shared.reset_version != session_reset_version {
                        None
                    } else {
                        if row.seq > shared.cursor + 1 {
                            shared.gap_repair = true;
                            tracing::warn!(
                                seq = row.seq,
                                cursor = shared.cursor,
                                "chat2: row gap detected; holding cursor and requesting backfill"
                            );
                        } else {
                            shared.cursor = shared.cursor.max(row.seq);
                        }
                        Some(shared.cursor)
                    }
                }) else {
                    return false;
                };
                self.sink.apply_row(&frame.payload, effective);
                let _ = self.events.send(ChatEvent::Applied);
            }
            frame_type::ACK => {
                let Ok(ack) = serde_json::from_value::<wire::AckHeader>(frame.header) else {
                    return false;
                };
                let mut shared = lock(&self.shared);
                if shared.reset_version != session_reset_version {
                    return false;
                }
                if shared.offline_snapshot_ids.contains(&ack.batch_id) {
                    // The HTTPS sibling owns the replay snapshot for this
                    // UUID. Keep the only replay copy and stage the WS verdict
                    // until its complete pull proves the log incarnation and
                    // the exact `(seq, batchId)` row.
                    shared
                        .offline_ws_acks
                        .entry(ack.batch_id)
                        .and_modify(|seq| *seq = (*seq).max(ack.seq))
                        .or_insert(ack.seq);
                    return true;
                }
                let before = shared.pending.len();
                shared.pending.retain(|p| p.batch_id != ack.batch_id);
                let retired = before != shared.pending.len();
                if !retired {
                    // A sibling transport already handled this UUID. In
                    // particular, never let a delayed pre-reset ack advance a
                    // new cursor namespace after its replay copy is gone.
                    return true;
                }
                // Same contiguity rule as ROW: our own batch landing at
                // `seq` proves rows up to seq exist server-side, not that we
                // HAVE the interleaved ones from other devices.
                if ack.seq > shared.cursor + 1 {
                    shared.gap_repair = true;
                    tracing::warn!(
                        seq = ack.seq,
                        cursor = shared.cursor,
                        "chat2: ack gap detected; holding cursor and requesting backfill"
                    );
                } else {
                    shared.cursor = shared.cursor.max(ack.seq);
                }
                let cursor = shared.cursor;
                // Quota drain: each grant immediately probes the next head
                // batch (one-per-grant, never a full-queue burst).
                if shared.quota_blocked {
                    if shared.pending.is_empty() {
                        shared.quota_blocked = false;
                    } else {
                        shared.retry_at = Some(tokio::time::Instant::now());
                    }
                }
                self.sink.advance_cursor(cursor);
                drop(shared);
                let _ = self.events.send(ChatEvent::Applied);
            }
            frame_type::PRESENCE => {
                let _ = self.events.send(ChatEvent::Presence);
            }
            frame_type::PROBE_OK => {
                if let Ok(probe) = serde_json::from_value::<wire::ProbeOkHeader>(frame.header) {
                    if let Some(server) = &mut lock(&self.shared).server {
                        server.head_seq = server.head_seq.max(probe.head_seq);
                    }
                }
            }
            frame_type::STATE => {
                // Late duplicate of a hello answer — refresh the server view.
                if let Ok(state) = serde_json::from_value::<wire::StateHeader>(frame.header) {
                    lock(&self.shared).server = Some(state);
                }
            }
            frame_type::ERROR => {
                self.flags.rejected.fetch_add(1, Relaxed);
                let code = frame.header["code"].as_str().unwrap_or("?").to_string();
                let message = frame.header["message"].as_str().unwrap_or("").to_string();
                let batch_id = frame.header["batchId"].as_str().unwrap_or("");
                match code.as_str() {
                    // Permanent verdicts on a specific batch: retire it, or
                    // it replays on every nudge/reconnect forever — the
                    // wedge class this design exists to kill. The ops stay
                    // in the local doc and travel with the next checkpoint.
                    "too_large" | "empty" | "bad_push" if !batch_id.is_empty() => {
                        let mut shared = lock(&self.shared);
                        if shared.reset_version != session_reset_version {
                            return false;
                        }
                        if shared.offline_snapshot_ids.contains(batch_id) {
                            shared.permanent_rejections.insert(batch_id.to_string());
                        }
                        let before = shared.pending.len();
                        shared.pending.retain(|p| p.batch_id != batch_id);
                        let dropped = before != shared.pending.len();
                        if dropped && !shared.pending.is_empty() {
                            // Do not strand the next batch behind a permanently
                            // rejected head. The steady-state retry arm pushes
                            // it immediately without waiting for a new enqueue.
                            shared.retry_at = Some(tokio::time::Instant::now());
                        }
                        drop(shared);
                        if dropped {
                            tracing::error!(
                                code,
                                batch_id,
                                "chat2: batch permanently rejected — retired \
                                 from the replay queue"
                            );
                            let _ = self.events.send(ChatEvent::PushRejected);
                        }
                    }
                    // Transient: the quota window passes on its own — keep
                    // the batch queued and head-probe on a short clock.
                    "quota" => {
                        let mut shared = lock(&self.shared);
                        if shared.reset_version != session_reset_version {
                            return false;
                        }
                        shared.quota_blocked = true;
                        shared.retry_at = Some(tokio::time::Instant::now() + QUOTA_RETRY);
                    }
                    _ => {}
                }
                tracing::warn!(code, message, "chat2: server rejected a frame");
            }
            other => {
                // Unknown server frame: tolerate (future protocol additions).
                tracing::debug!(kind = other, "chat2: ignoring unknown frame type");
            }
        }
        true
    }
}

#[cfg(test)]
mod tests;
