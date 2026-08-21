//! DocHost — per-chat `SessionDoc` handles: snapshot persistence (debounced), edge room
//! sync (offline-tolerant), and the HOST-ONLY durable command executor.
//!
//! Pragmatic port of zeron's `session-docs.ts` + the `main.ts` executor (spec:
//! feature-inventory §3.3, ARCHITECTURE §2 "command plane"):
//! - the doc IS the outbox: commands and user entries commit locally and sync whenever a
//!   room connection exists; the engine is fully functional with sync disabled;
//! - on every doc change (local commit or remote import) the handle re-emits the joined
//!   transcript to watchers, drains pending commands, and schedules a snapshot save;
//! - command drain: evaluate via `evaluate_command` (with the DocsStore processed
//!   ledger), mark processed BEFORE execute, execute through the sessions engine, then
//!   write the outcome status back into the doc as the sole outcome writer.
//!
//! Chat ownership is gated on the registry doc (`chats[chat_id].deviceId`), with
//! claim-on-first-command for unknown chats. Queueing a command for a chat hosted on
//! another device POSTs a durable nudge to that device's room (§7 cold-chat delivery);
//! the host's relay receives it and warm-opens the doc, which drains the queue.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use base64::Engine as _;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use zeron_doc::{
    COMMAND_DEFAULT_TTL_MS, CommandBasedOn, CommandDisposition, CommandOrigin, DocError,
    EvaluationContext, MessagePart, MessageRole, MessageStatus, SessionCommandEntry,
    SessionCommandPayload, SessionCommandStatus, SessionDoc, SessionMessageEntry, evaluate_command,
    join_continuation_entries,
};
use zeron_proto::{HarnessId, UserInputAnswer, UserInputQuestion};
use zeron_sync::DocsStore;

use crate::registry_host::RegistryHost;
use crate::sessions::{SessionsEngine, SteerOutcome};
use crate::{EngineError, new_id, now_ms};

/// Debounce window for local snapshot saves after a doc change.
const SNAPSHOT_DEBOUNCE_MS: u64 = 1_000;

/// Warm-doc LRU: how many unwatched, run-less docs stay fully open. Everything
/// beyond this (and beyond [`zeron_doc::DOC_LRU_BYTE_BUDGET`]) is evicted
/// oldest-access-first — reopening from the SQLite snapshot measured within
/// ~11ms of a warm doc, so the cap trades no perceptible open latency.
const WARM_DOC_CAP: usize = 12;

/// Resident-memory estimate per compressed snapshot byte. Loro snapshots are
/// columnar+compressed; the in-memory doc plus mirror runs well above the blob
/// size. A rough multiplier is enough here — the budget is a safety ceiling,
/// the count cap does the day-to-day work.
const RESIDENT_BYTES_PER_SNAPSHOT_BYTE: usize = 6;

/// Floor per open doc (room socket buffers, tasks) regardless of content size.
const DOC_RESIDENT_FLOOR_BYTES: usize = 512 * 1024;

/// Docs touched this recently are never evicted. Closes the open→attach race:
/// `open()` returns a handle, and until the caller's `watch_messages` lands
/// the doc is unwatched and unpinned — a concurrent eviction would orphan the
/// watcher on a roomless doc that renders once and never updates again.
const EVICT_MIN_IDLE_MS: i64 = 30_000;

/// Queued-attachment transfer pacing: chunk pushes are bounded per call (a
/// stalled-but-open relay link never fails on its own) and a timeout marks
/// the link suspect; attempts retry on this backoff, cut short by the online
/// bus / system wake.
const TRANSFER_CHUNK_B64: usize = 60_000;
const TRANSFER_CHUNK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const TRANSFER_COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const TRANSFER_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(2);
const TRANSFER_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(30);
/// A command whose attachment bytes are still in transit waits at most this
/// long before the drain rejects it loudly (and the transfer task gives up on
/// the same clock) — a chat must never wedge behind bytes that aren't coming.
const ATTACHMENT_WAIT_MAX: std::time::Duration = std::time::Duration::from_secs(15 * 60);
const ATTACHMENT_WAIT_MAX_MS: i64 = ATTACHMENT_WAIT_MAX.as_millis() as i64;
/// Re-check cadence while a chat's queue is deferred on in-transit bytes
/// (the happy path is event-driven — UploadCommit kicks the drain — this
/// timer only covers the give-up transition and missed kicks).
const ATTACHMENT_WAIT_RECHECK: std::time::Duration = std::time::Duration::from_secs(30);

/// Transfer attempt outcome: transient failures retry (the link may heal),
/// permanent ones stop (the host actively refused, or the bytes are gone).
enum TransferError {
    Transient(String),
    Permanent(String),
}

/// Peer-relay delivery fallback pacing (`spawn_command_delivery`): the grace
/// the normal rows→edge path gets before the relay road opens, the poll while
/// waiting, the relay retry curve, its per-call deadline, and the give-up cap
/// (the command stays durably queued in the doc regardless).
const ROWS_GRACE: std::time::Duration = std::time::Duration::from_secs(10);
const ROWS_POLL: std::time::Duration = std::time::Duration::from_secs(1);
const RELAY_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(5);
const RELAY_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(30);
const RELAY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const RELAY_GIVE_UP: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Edge connection config. The bearer is a **provider**, never a snapshot:
/// every room (re)connect and HTTP request re-reads it, so WorkOS access-token
/// refreshes (~1h expiry) take effect without an engine restart. Dev bearers
/// (which never expire) ride the same seam as a [`zeron_rpc::StaticToken`].
#[derive(Clone)]
pub struct EdgeConfig {
    /// Edge base URL (`http(s)://…`); rewritten to `ws(s)` for the room socket.
    pub url: String,
    /// Fresh-bearer provider (the relay's `TokenSource`), consulted per
    /// connect/request. `None` from the provider = signed out.
    pub token: Arc<dyn zeron_rpc::TokenSource>,
    /// This engine's device id, carried on room dials (`&device=`) so the
    /// edge can attribute sockets in logs. Debugging the 2026-08-04 deaf
    /// socket meant reverse-engineering devices from rotating IPv6 privacy
    /// addresses; never again. Empty = omitted (tests).
    pub device_id: String,
}

impl std::fmt::Debug for EdgeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeConfig")
            .field("url", &self.url)
            .field("token", &"<provider>")
            .finish()
    }
}

impl EdgeConfig {
    pub fn new(url: impl Into<String>, token: Arc<dyn zeron_rpc::TokenSource>) -> Self {
        Self {
            url: url.into(),
            token,
            device_id: String::new(),
        }
    }

    /// Attribute this engine's room sockets in edge logs.
    pub fn with_device(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = device_id.into();
        self
    }

    /// Fixed bearer — dev mode and tests, where tokens never expire.
    pub fn with_static_token(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self::new(url, Arc::new(zeron_rpc::StaticToken(token.into())))
    }

    /// The current bearer, refreshed by the provider if stale. `None` = signed out.
    pub async fn bearer(&self) -> Option<String> {
        self.token.token().await
    }

    pub fn token_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.token.subscribe()
    }

    /// A per-dial room URL provider for `path` (e.g. `/session/{chatId}/ws`):
    /// the bearer is re-fetched before every connect, so reconnects after a
    /// token expiry present a fresh `?token=` instead of the boot-time one.
    pub fn room_url(&self, path: impl Into<String>) -> Arc<dyn zeron_sync::UrlProvider> {
        self.room_url_with(path, "")
    }

    /// [`Self::room_url`] with extra query params appended after the token
    /// and device (e.g. `"&role=host"` for a chat3 host claim).
    pub fn room_url_with(
        &self,
        path: impl Into<String>,
        extra_query: impl Into<String>,
    ) -> Arc<dyn zeron_sync::UrlProvider> {
        let ws_base = self.url.replacen("http", "ws", 1);
        Arc::new(EdgeRoomUrl {
            base: format!("{}{}", ws_base.trim_end_matches('/'), path.into()),
            token: self.token.clone(),
            device_id: self.device_id.clone(),
            extra_query: extra_query.into(),
        })
    }
}

struct EdgeRoomUrl {
    base: String,
    token: Arc<dyn zeron_rpc::TokenSource>,
    device_id: String,
    extra_query: String,
}

impl zeron_sync::UrlProvider for EdgeRoomUrl {
    fn url(&self) -> futures::future::BoxFuture<'static, Result<String, zeron_sync::SyncError>> {
        let token = self.token.clone();
        let base = self.base.clone();
        let device = self.device_id.clone();
        let extra = self.extra_query.clone();
        Box::pin(async move {
            let token = token.token().await.ok_or_else(|| {
                zeron_sync::SyncError::Auth("no access token (signed out)".into())
            })?;
            let mut url = format!("{base}?token={token}");
            if !device.is_empty() {
                url.push_str(&format!("&device={device}"));
            }
            url.push_str(&extra);
            Ok(url)
        })
    }
}

/// Room-path prefix — every chat lives in an organization-shared `chat3` room.
const ROOM_PREFIX: &str = "chat3";

#[derive(Debug, Clone)]
pub struct DocHostConfig {
    pub device_id: String,
    /// The signed-in user — stamped on command entries this engine queues so
    /// shared sessions can render "who sent this". Empty when unknown (dev
    /// setups without auth); empty stamps nothing.
    pub user_id: String,
    /// Harness for doc-command runs on chats without a registry `config` row.
    pub default_harness: HarnessId,
    /// When present, each opened chat joins its edge session room. `None` = fully
    /// offline operation (local snapshots only).
    pub edge: Option<EdgeConfig>,
}

struct DocHostInner {
    store: Arc<DocsStore>,
    config: DocHostConfig,
    /// Set-once (first wins), cleared by `shutdown_workers`: sessions and
    /// doc-host reference each other through Arcs, so a retired runtime's
    /// graph only drops once this back-edge is severed.
    sessions: Mutex<Option<SessionsEngine>>,
    registry: OnceLock<RegistryHost>,
    /// Worktree materialization for Run commands (see `set_repos`).
    repos: OnceLock<crate::repos::Repos>,
    /// Cancels every worker spawned through `spawn_worker` — the loops'
    /// own exit conditions (weak handle death, closed channels) don't cover
    /// runtime replacement, where Edge-capable tasks must stop doing
    /// network work even while something still pins the graph.
    shutdown: CancellationToken,
    /// Tracks every spawned worker so `shutdown_workers` can await them.
    tasks: TaskTracker,
    handles: Mutex<HashMap<String, Arc<ChatDocHandle>>>,
    /// Process-lifetime terminal deletion fences. Chat ids are UUIDs and are
    /// never reused: once DeleteChat/DeleteSpace reaches this host, every
    /// detached recovery/cutover task must fail closed even if its old handle
    /// has already left `handles` and therefore cannot be marked directly.
    purge_fences: Mutex<HashMap<String, Arc<AtomicBool>>>,
    /// Local room updates waiting for a client, keyed outside the handle so
    /// they survive chat2 -> chat3 handle replacement. This is the in-memory
    /// handoff queue, not a second CRDT authority; bytes stay until installed
    /// into a client's ack-backed pending queue.
    chat2_pending_local: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    /// chat2 seeds in flight (one per chat — reopen storms must not race
    /// duplicate rebuild+checkpoint POSTs; benign server-side, wasteful).
    seeding: Mutex<HashSet<String>>,
    /// chat2 quiet-waiters armed (one per chat): the cutover watcher re-arms
    /// on every registry change, and a long run would stack a tick loop per
    /// change without this.
    seed_waiting: Mutex<HashSet<String>>,
    /// Generation cutovers deferred behind a live writer/pending command.
    /// Registry publishes (including presence-only publishes) must not stack
    /// one waiter per chat while the same run is still active.
    cutover_waiting: Mutex<HashSet<String>>,
    /// Deterministic store-failure seam for frozen-generation recovery tests.
    /// Production builds call the real store directly with no extra state.
    #[cfg(test)]
    injected_cutover_store_failures: AtomicUsize,
    /// Attachment-wait re-drain timers armed (one per chat): a command
    /// deferred on in-transit attachment bytes re-checks on a cadence, and
    /// each deferral must not stack another timer.
    drain_waiting: Mutex<HashSet<String>>,
    /// Uploads store (engine assembly) — resolves `pending://` attachment
    /// refs and jails transfer reads to the uploads dir.
    uploads: OnceLock<crate::uploads::Uploads>,
    /// Connectivity watch (`WatchConnectivity`): lazily-started monitor
    /// publishes the edge posture on change (see `watch_connectivity`).
    connectivity: OnceLock<watch::Sender<zeron_proto::Connectivity>>,
    connectivity_started: AtomicBool,
    connectivity_grace: Mutex<DegradeGrace>,
    /// Command ids currently BETWEEN mark-processed and their resolution in a
    /// drain. Distinguishes "executing right now" from "consumed by the
    /// ledger but dead" (a crash between mark and resolve): the drain
    /// terminalizes the latter as Rejected instead of leaving a forever-
    /// Pending entry no retry could ever reach (2026-08-19 swallowed-send).
    executing: Mutex<HashSet<String>>,
    /// Peer links (engine assembly, edge runtimes only) — the transport that
    /// pushes queued attachment bytes to a remote host.
    links: OnceLock<Arc<zeron_rpc::LinkCache>>,
    /// Shared client for sidecar blob PUT/GET (30s timeout, uploads.rs
    /// discipline — diff_sync's untimed client hung on dead links).
    http: reqwest::Client,
    /// chat_id → the live turn's agent-send hop depth (from the executed
    /// command's `origin`; 0 for human sends). `send_to_session` reads it to
    /// stamp `hops + 1` on outgoing sends — the A→B→A ping-pong breaker.
    turn_origins: Mutex<HashMap<String, u32>>,
    /// chat_id → sends spent this turn (reset when a new turn dispatches).
    send_budgets: Mutex<HashMap<String, u32>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The queued-attachment transfers a command's `pending://` refs imply —
/// shared by the retry escort and the retry re-issue.
fn command_transfers(entry: &SessionCommandEntry) -> Vec<crate::uploads::AttachmentTransfer> {
    let refs: Vec<String> = match &entry.payload {
        SessionCommandPayload::Run { request, .. } => request
            .attachments
            .iter()
            .filter(|p| crate::uploads::is_pending_ref(p))
            .cloned()
            .collect(),
        SessionCommandPayload::Steer { prompt, .. } => crate::uploads::pending_refs_in(prompt),
        _ => Vec::new(),
    };
    refs.iter()
        .filter_map(|r| crate::uploads::parse_pending_ref(r))
        .map(
            |(upload_id, file_name)| crate::uploads::AttachmentTransfer {
                upload_id: upload_id.to_string(),
                file_name: file_name.to_string(),
            },
        )
        .collect()
}

/// How long raw degradation must persist before connectivity reports it.
/// Room joins, idle-link wakes, and navigation dials resolve well under a
/// second on healthy networks; real outages outlive this comfortably. Recovery
/// is never delayed.
const DEGRADE_GRACE: std::time::Duration = std::time::Duration::from_secs(4);

/// One tracked degradation source in [`DegradeGrace`].
enum GraceKey<'a> {
    OsPath,
    Registry,
    Chat(&'a str),
}

/// Show-slow / hide-fast hysteresis over raw connectivity signals. Pure over
/// injected `Instant`s so the grace window is unit-testable.
#[derive(Default)]
struct DegradeGrace {
    os_path: Option<std::time::Instant>,
    registry: Option<std::time::Instant>,
    chats: HashMap<String, std::time::Instant>,
}

impl DegradeGrace {
    /// Feed one raw sample; returns whether to REPORT the source as degraded.
    /// Healthy clears the timer instantly; degraded reports only once it has
    /// persisted for [`DEGRADE_GRACE`].
    fn degraded(&mut self, key: GraceKey, raw: bool, now: std::time::Instant) -> bool {
        let slot: &mut Option<std::time::Instant> = match key {
            GraceKey::OsPath => &mut self.os_path,
            GraceKey::Registry => &mut self.registry,
            GraceKey::Chat(id) => {
                if raw && !self.chats.contains_key(id) {
                    self.chats.insert(id.to_string(), now);
                }
                match self.chats.get_mut(id) {
                    Some(_) if !raw => {
                        self.chats.remove(id);
                        return false;
                    }
                    Some(since) => return now.duration_since(*since) >= DEGRADE_GRACE,
                    None => return false,
                }
            }
        };
        if !raw {
            *slot = None;
            return false;
        }
        let since = *slot.get_or_insert(now);
        now.duration_since(since) >= DEGRADE_GRACE
    }

    /// Drop timers for chats that no longer have open docs.
    fn retain_chats(&mut self, keep: impl Fn(&str) -> bool) {
        self.chats.retain(|id, _| keep(id));
    }
}

#[derive(Clone)]
pub struct DocHost {
    inner: Arc<DocHostInner>,
}

/// One open chat doc: the `SessionDoc`, its change plumbing, and the room client.
pub struct ChatDocHandle {
    chat_id: String,
    device_id: String,
    doc: Arc<SessionDoc>,
    messages_tx: watch::Sender<Vec<SessionMessageEntry>>,
    /// True when the doc changed while nobody watched: the mirror rebuild is
    /// deferred to the next `watch_messages` attach instead of paid per commit.
    mirror_dirty: AtomicBool,
    /// Epoch ms of the last open/watch touch — the LRU eviction key.
    last_access: AtomicI64,
    /// Last known snapshot blob size — the eviction budget estimate's input.
    snapshot_bytes: AtomicUsize,
    /// The sync generation this handle was BUILT for (1 = legacy s2,
    /// 2 = chat2). Gen-1 handles no longer join any room (the s2 client is
    /// gone); they serve the local fat doc read-only until the host's seed
    /// flips the chat to chat2. The staleness checks compare this against
    /// the registry — inferring mode from `chat2_local_sub` misread
    /// edge-less chat2 handles (no subscription is ever installed offline)
    /// as stale s2 and retired them on every open, dropping the doc out
    /// from under live runs.
    room_gen: u32,
    /// A threshold checkpoint POST is in flight (review H1: the quiesce
    /// tick must not stack concurrent full-snapshot uploads).
    checkpoint_state: Arc<Mutex<CheckpointState>>,
    /// Highest lifetime `ChatStatsSnapshot::rejected` count covered by a
    /// successful full checkpoint. The stats counter is telemetry and never
    /// resets; only counts above this watermark are outstanding compensation.
    checkpointed_rejections: AtomicU64,
    /// Set when a chat2 seed replaced this handle's lineage on disk: every
    /// further snapshot save from this handle is a stale FAT doc that would
    /// clobber the thin one — retired handles never persist again (unless no
    /// thin lineage exists on disk at all; `save_snapshot` double-checks, so
    /// a doc that was never seeded can't lose its only copy).
    retired: AtomicBool,
    /// Terminal local deletion fence. Unlike a migration freeze, purge must
    /// never write a recovery snapshot: doing so would resurrect the chat
    /// after its row and local bytes were deliberately deleted.
    purged: Arc<AtomicBool>,
    /// A room-generation seed has detached this handle from its source room
    /// and is sealing the final checkpoint. `open()` must not hand the old doc
    /// to a new writer while the replacement checkpoint is in flight.
    generation_frozen: Arc<AtomicBool>,
    /// Serializes every EngineChatSink import/persist and local-update enqueue
    /// with cutover sealing. Lock order is command_drain -> handles -> this
    /// lifecycle gate -> snapshot_save -> chat2 -> host pending.
    sink_lifecycle: Arc<Mutex<()>>,
    /// Invalidates sinks owned by a detached ChatClient. A recovered handle
    /// may unfreeze and attach a new client without re-enabling late HTTP work
    /// from the predecessor.
    sink_generation: Arc<AtomicU64>,
    /// At least one local update lacks a durable room acknowledgement. A
    /// snapshot flush then persists cursor=0 so restart replays the full CRDT
    /// log even if the in-memory pending queue dies with this runtime.
    replay_from_zero: Arc<AtomicBool>,
    /// Closes the local-callback vs replay-clear race. Lock order is
    /// sink_lifecycle (when present) -> replay_fence -> chat2 -> host pending.
    /// A callback marks replay dirty before waiting for the chat slot; a clear
    /// cannot overwrite that mark until the update is observably queued.
    replay_fence: Arc<Mutex<()>>,
    /// Root-diff/local-update handoff tracking. Loro publishes the root diff
    /// before the encoded local-update callback; clear must not bless a
    /// snapshot while a visible local commit is still between those hooks.
    local_callbacks_pending: Arc<AtomicUsize>,
    local_mutation_epoch: Arc<AtomicU64>,
    local_update_tracking: Arc<AtomicBool>,
    /// Serializes ordinary debounced saves with the migration's recovery and
    /// target-epoch saves. Without this, a save that exported before the seal
    /// could land after it and overwrite the final snapshot bytes.
    snapshot_save: Mutex<()>,
    /// chat2 relay client (docs/chat2-sync.md C3) — populated once the
    /// registry names roomGen 2 for this chat and the join resolves.
    chat2: Mutex<Option<zeron_sync::ChatClient>>,
    /// Local-update feed into the chat2 client (drop = unsubscribe).
    chat2_local_sub: Mutex<Option<loro::Subscription>>,
    /// One executor pass per doc. Registry-readiness and doc-change kicks can
    /// race; without serialization both passes can read the same pending entry
    /// before either writes the processed ledger and dispatch it twice.
    command_drain: tokio::sync::Mutex<()>,
    /// Fires before local changes become visible, closing the export-before-
    /// local-update callback window used by replay durability.
    _pre_commit_sub: loro::Subscription,
    /// Doc subscription (drop = unsubscribe) — bumps the change watch on every commit.
    _sub: loro::Subscription,
}

struct LocalUpdateCallbackGuard {
    pending: Arc<AtomicUsize>,
    counted: bool,
}

impl Drop for LocalUpdateCallbackGuard {
    fn drop(&mut self) {
        if self.counted {
            let _ = self
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    pending.checked_sub(1)
                });
        }
    }
}

#[derive(Debug)]
struct CheckpointState {
    running: bool,
    requested: bool,
    reason: &'static str,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            running: false,
            requested: false,
            reason: "unspecified",
        }
    }
}

impl ChatDocHandle {
    fn ensure_mutable(&self) -> Result<(), DocError> {
        if self.generation_frozen.load(Ordering::Acquire)
            || self.retired.load(Ordering::Acquire)
            || self.purged.load(Ordering::Acquire)
        {
            return Err(DocError::Schema(format!(
                "chat {} is frozen for room-generation cutover",
                self.chat_id
            )));
        }
        Ok(())
    }

    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn doc(&self) -> &SessionDoc {
        &self.doc
    }

    /// Acquire a writer lease while serialized with generation freeze. The
    /// extra doc Arc is itself the cutover's live-writer signal, so it must be
    /// minted before a run publishes any durable message/status side effect.
    pub fn writer_doc(&self) -> Result<Arc<SessionDoc>, DocError> {
        let _lifecycle = lock(&self.sink_lifecycle);
        self.ensure_mutable()?;
        Ok(self.doc.clone())
    }

    /// Joined transcript watch — re-sent on every doc change (WatchDocMessages).
    ///
    /// Attach-time refresh: the mirror is only maintained while watched, so a
    /// doc that changed unwatched materializes here, once, instead of on every
    /// commit it sat through in the background.
    pub fn watch_messages(&self) -> watch::Receiver<Vec<SessionMessageEntry>> {
        self.touch();
        // Attach is a user signal: verify a quiet room is actually alive
        // (a doc-wedged DO keeps answering pings while delivering nothing,
        // and the background probe cadence can be hours out). Coalescing
        // no-op on a healthy or recently-active room.
        if let Some(chat2) = lock(&self.chat2).as_ref() {
            chat2.probe();
        }
        // Subscribe BEFORE the dirty check: a commit racing this attach then
        // sees a live receiver and publishes, instead of re-marking dirty
        // after our refresh and leaving the new watcher a cleared mirror.
        let rx = self.messages_tx.subscribe();
        if self.mirror_dirty.load(Ordering::Acquire) {
            self.publish_messages();
        }
        rx
    }

    fn touch(&self) {
        self.last_access.store(now_ms(), Ordering::Relaxed);
    }

    pub fn connected(&self) -> bool {
        lock(&self.chat2).is_some()
    }

    /// Write a complete user message entry, idempotent by id (the client-minted message
    /// id — a re-executed command or optimistic echo never duplicates the entry).
    /// `author` is the issuing user (from the command entry) — `agent:{chatId}`
    /// for agent-to-agent sends; None on entries from pre-attribution writers.
    pub fn write_user_message(
        &self,
        message_id: &str,
        text: &str,
        created_at: i64,
        author: Option<&str>,
    ) -> Result<(), DocError> {
        let _lifecycle = lock(&self.sink_lifecycle);
        self.ensure_mutable()?;
        if self.doc.read_entries()?.iter().any(|e| e.id == message_id) {
            return Ok(());
        }
        self.doc.push_message(&SessionMessageEntry {
            id: message_id.to_string(),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: text.to_string(),
            }],
            created_at,
            device_id: self.device_id.clone(),
            user_id: author.map(str::to_owned),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        })
    }

    /// Recovery sweep: stamp this device's abandoned `streaming` entries `aborted`, appending
    /// `note` as a visible error part so the transcript says WHY the turn
    /// ended (zeron folded "Run interrupted by backend restart" the same
    /// way). Returns the stamped entries' `(id, created_at)` — recovery uses
    /// them for the resume-freshness check.
    pub fn mark_abandoned_streams(&self, note: &str) -> Result<Vec<(String, i64)>, DocError> {
        let _lifecycle = lock(&self.sink_lifecycle);
        self.ensure_mutable()?;
        let mut stamped = Vec::new();
        for entry in self.doc.read_entries()? {
            if entry.role == MessageRole::Assistant
                && entry.status == Some(MessageStatus::Streaming)
                && entry.device_id == self.device_id
                && self
                    .doc
                    .set_message_status(&entry.id, MessageStatus::Aborted)?
            {
                let part_id = format!("{}-recovery", entry.id);
                if let Err(err) = self.doc.append_error_part(&entry.id, &part_id, note) {
                    tracing::warn!(chat = %self.chat_id, error = %err, "recovery note append failed");
                }
                stamped.push((entry.id.clone(), entry.created_at));
            }
        }
        if !stamped.is_empty() {
            self.publish_messages();
        }
        Ok(stamped)
    }

    fn publish_messages(&self) {
        self.mirror_dirty.store(false, Ordering::Release);
        match self.doc.read_entries() {
            Ok(entries) => {
                let joined = join_continuation_entries(entries);
                // send_replace: update the watch even with no subscribers yet, so a
                // late subscriber's first borrow sees the current transcript.
                self.messages_tx.send_replace(joined);
            }
            Err(err) => {
                tracing::warn!(chat = %self.chat_id, error = %err, "transcript read failed");
            }
        }
    }

    /// Per-commit publish path: unwatched docs just mark the mirror dirty —
    /// rebuilding a full transcript nobody reads was a per-tick cost on every
    /// open doc (and kept a second transcript copy hot).
    fn publish_messages_if_watched(&self) {
        if self.messages_tx.receiver_count() == 0 {
            self.mirror_dirty.store(true, Ordering::Release);
            // Shrink the stale mirror: watch_messages rebuilds on attach.
            self.messages_tx.send_replace(Vec::new());
        } else {
            self.publish_messages();
        }
    }

    /// Rough resident cost for the LRU budget.
    fn resident_estimate(&self) -> usize {
        (self.snapshot_bytes.load(Ordering::Relaxed) * RESIDENT_BYTES_PER_SNAPSHOT_BYTE)
            .max(DOC_RESIDENT_FLOOR_BYTES)
    }
}

impl DocHost {
    pub fn new(store: Arc<DocsStore>, config: DocHostConfig) -> Self {
        Self {
            inner: Arc::new(DocHostInner {
                store,
                config,
                sessions: Mutex::new(None),
                registry: OnceLock::new(),
                repos: OnceLock::new(),
                shutdown: CancellationToken::new(),
                tasks: TaskTracker::new(),
                handles: Mutex::new(HashMap::new()),
                purge_fences: Mutex::new(HashMap::new()),
                chat2_pending_local: Mutex::new(HashMap::new()),
                seeding: Mutex::new(HashSet::new()),
                seed_waiting: Mutex::new(HashSet::new()),
                cutover_waiting: Mutex::new(HashSet::new()),
                #[cfg(test)]
                injected_cutover_store_failures: AtomicUsize::new(0),
                drain_waiting: Mutex::new(HashSet::new()),
                uploads: OnceLock::new(),
                connectivity: OnceLock::new(),
                connectivity_started: AtomicBool::new(false),
                connectivity_grace: Mutex::new(DegradeGrace::default()),
                executing: Mutex::new(HashSet::new()),
                links: OnceLock::new(),
                http: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
                turn_origins: Mutex::new(HashMap::new()),
                send_budgets: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn purge_fence(inner: &DocHostInner, chat_id: &str) -> Arc<AtomicBool> {
        lock(&inner.purge_fences)
            .entry(chat_id.to_string())
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone()
    }

    fn chat_was_purged(inner: &DocHostInner, chat_id: &str) -> bool {
        lock(&inner.purge_fences)
            .get(chat_id)
            .is_some_and(|fence| fence.load(Ordering::Acquire))
    }

    fn buffer_chat2_update(inner: &DocHostInner, chat_id: &str, bytes: Vec<u8>) {
        let fence = Self::purge_fence(inner, chat_id);
        if fence.load(Ordering::Acquire) {
            return;
        }
        {
            lock(&inner.chat2_pending_local)
                .entry(chat_id.to_string())
                .or_default()
                .push(bytes);
        }
        // Purge may have cleared the queue between our first check and push.
        // Its own removal wins when it happens second; this removal wins when
        // our append happens second.
        if fence.load(Ordering::Acquire) {
            lock(&inner.chat2_pending_local).remove(chat_id);
        }
    }

    fn prepend_chat2_update(inner: &DocHostInner, chat_id: &str, bytes: Vec<u8>) {
        let fence = Self::purge_fence(inner, chat_id);
        if fence.load(Ordering::Acquire) {
            return;
        }
        {
            lock(&inner.chat2_pending_local)
                .entry(chat_id.to_string())
                .or_default()
                .insert(0, bytes);
        }
        if fence.load(Ordering::Acquire) {
            lock(&inner.chat2_pending_local).remove(chat_id);
        }
    }

    /// Install the local-update feed for one thin-room handle. Recovery uses
    /// the same callback as first construction so an I/O failure cannot leave
    /// a mapped, writable doc without a path into its room replay queue.
    fn install_chat2_local_subscription(&self, handle: &Arc<ChatDocHandle>) {
        if lock(&handle.chat2_local_sub).is_some() {
            return;
        }
        let weak_push = Arc::downgrade(handle);
        let weak_host = Arc::downgrade(&self.inner);
        let pending_chat = handle.chat_id.clone();
        let callback_pending = handle.local_callbacks_pending.clone();
        let sub = handle
            .doc
            .doc()
            .subscribe_local_update(Box::new(move |bytes: &Vec<u8>| {
                let _callback = LocalUpdateCallbackGuard {
                    counted: callback_pending.load(Ordering::Acquire) > 0,
                    pending: callback_pending.clone(),
                };
                if let Some(handle) = weak_push.upgrade() {
                    let _replay = lock(&handle.replay_fence);
                    if handle.generation_frozen.load(Ordering::Acquire)
                        || handle.retired.load(Ordering::Acquire)
                        || handle.purged.load(Ordering::Acquire)
                    {
                        return true;
                    }
                    // Mark before waiting for the chat slot. Snapshot save may
                    // own that slot while exporting this just-committed change;
                    // it must observe cursor-zero durability even though the
                    // enqueue is not visible yet.
                    handle.replay_from_zero.store(true, Ordering::Release);
                    let client_guard = lock(&handle.chat2);
                    if handle.generation_frozen.load(Ordering::Acquire)
                        || handle.retired.load(Ordering::Acquire)
                        || handle.purged.load(Ordering::Acquire)
                    {
                        return true;
                    }
                    match &*client_guard {
                        Some(client) => {
                            if let Err(bytes) = client.try_enqueue_update(bytes.clone())
                                && let Some(inner) = weak_host.upgrade()
                            {
                                DocHost::buffer_chat2_update(&inner, &pending_chat, bytes);
                            }
                        }
                        None => {
                            if let Some(inner) = weak_host.upgrade() {
                                DocHost::buffer_chat2_update(&inner, &pending_chat, bytes.clone());
                            }
                        }
                    }
                }
                true
            }));
        let mut slot = lock(&handle.chat2_local_sub);
        if slot.is_none() {
            *slot = Some(sub);
            handle.local_update_tracking.store(true, Ordering::Release);
        }
    }

    /// Install a room client and atomically hand it every update buffered for
    /// this chat, including updates carried across an older generation's
    /// handle. The local-update callback takes the same client lock before
    /// choosing client vs host-level queue, closing the install/enqueue gap.
    #[cfg(test)]
    fn install_chat2_client(
        &self,
        handle: &ChatDocHandle,
        client: zeron_sync::ChatClient,
    ) -> usize {
        let generation = handle.sink_generation.load(Ordering::Acquire);
        self.install_chat2_client_for_generation(handle, client, generation)
    }

    fn install_chat2_client_for_generation(
        &self,
        handle: &ChatDocHandle,
        client: zeron_sync::ChatClient,
        client_generation: u64,
    ) -> usize {
        let mut count = 0usize;
        loop {
            if handle.purged.load(Ordering::Acquire)
                || Self::chat_was_purged(&self.inner, &handle.chat_id)
            {
                lock(&self.inner.chat2_pending_local).remove(&handle.chat_id);
                return 0;
            }
            // Enqueue only while we own the client and hold no lifecycle or
            // client-slot lock. The sync actor may hold its shared-state mutex
            // while entering the sink; calling enqueue under the lifecycle
            // gate would invert that order and deadlock.
            let pending = lock(&self.inner.chat2_pending_local)
                .remove(&handle.chat_id)
                .unwrap_or_default();
            count += pending.len();
            for update in pending {
                client.enqueue_update(update);
            }

            let _lifecycle = lock(&handle.sink_lifecycle);
            if handle.generation_frozen.load(Ordering::Acquire)
                || handle.retired.load(Ordering::Acquire)
                || handle.purged.load(Ordering::Acquire)
                || handle.sink_generation.load(Ordering::Acquire) != client_generation
            {
                drop(_lifecycle);
                self.carry_parked_updates(handle, Some(client));
                return 0;
            }
            let mut client_slot = lock(&handle.chat2);
            // A callback that observed the still-empty slot must append under
            // this same slot lock. If such a suffix exists, release all gates,
            // enqueue it while owning the client, and retry. An empty suffix
            // lets us publish the client atomically with the decision.
            let late = lock(&self.inner.chat2_pending_local)
                .remove(&handle.chat_id)
                .unwrap_or_default();
            if late.is_empty() {
                *client_slot = Some(client);
                return count;
            }
            drop(client_slot);
            drop(_lifecycle);
            count += late.len();
            for update in late {
                client.enqueue_update(update);
            }
        }
    }

    /// Retry updates that a local Loro callback could not enqueue without
    /// blocking on the sync actor's shared-state lock. Taking temporary
    /// ownership of the client lets the ordinary install handoff drain the
    /// host buffer while holding no lifecycle gate during `enqueue_update`.
    fn flush_buffered_chat2_updates(&self, handle: &ChatDocHandle) -> usize {
        if handle.purged.load(Ordering::Acquire) {
            lock(&self.inner.chat2_pending_local).remove(&handle.chat_id);
            return 0;
        }
        let has_pending = lock(&self.inner.chat2_pending_local)
            .get(&handle.chat_id)
            .is_some_and(|pending| !pending.is_empty());
        if !has_pending {
            return 0;
        }
        let generation = handle.sink_generation.load(Ordering::Acquire);
        let Some(client) = lock(&handle.chat2).take() else {
            return 0;
        };
        self.install_chat2_client_for_generation(handle, client, generation)
    }

    /// Clear the conservative full-replay marker only after the client proves
    /// every local batch is gone and the sink's matching non-zero cursor is
    /// already durable in the same room epoch. Call with `sink_lifecycle`
    /// held; all shared-state observations under the chat slot are try-only.
    fn maybe_clear_replay_from_zero_locked(&self, handle: &ChatDocHandle) {
        let _replay = lock(&handle.replay_fence);
        if !handle.replay_from_zero.load(Ordering::Acquire)
            || handle.generation_frozen.load(Ordering::Acquire)
            || handle.retired.load(Ordering::Acquire)
            || handle.purged.load(Ordering::Acquire)
        {
            return;
        }
        if handle.local_callbacks_pending.load(Ordering::Acquire) != 0 {
            return;
        }
        let mutation_epoch = handle.local_mutation_epoch.load(Ordering::Acquire);
        let client_slot = lock(&handle.chat2);
        let Some(stats) = client_slot
            .as_ref()
            .and_then(zeron_sync::ChatClient::try_stats)
        else {
            return;
        };
        if stats.pending_pushes != 0
            || stats.rejected > handle.checkpointed_rejections.load(Ordering::Acquire)
            || stats.cursor == 0
        {
            return;
        }
        if lock(&self.inner.chat2_pending_local)
            .get(&handle.chat_id)
            .is_some_and(|pending| !pending.is_empty())
        {
            return;
        }
        let Ok(durable_before) = self.inner.store.load_snapshot_with_cursor(&handle.chat_id) else {
            return;
        };
        if durable_before
            .as_ref()
            .is_some_and(|(_, _, epoch)| *epoch > handle.room_gen)
        {
            return; // a newer room lineage owns this id
        }
        // Cursor-zero replay can be retired by either an own-row ack or a full
        // checkpoint that covers every rejected batch. Only the former reaches
        // EngineChatSink::advance_cursor and proves ownership of the target
        // generation. Preserve the sink-owned epoch here; stamping room_gen
        // after checkpoint-only coverage let the still-epoch-2 sink overwrite
        // that row back to epoch 2 on its next remote apply.
        let persist_epoch = durable_before
            .as_ref()
            .map(|(_, _, epoch)| *epoch)
            .unwrap_or(crate::chat2_host::CHAT2_DOC_EPOCH)
            .max(crate::chat2_host::CHAT2_DOC_EPOCH)
            .min(handle.room_gen);
        let Ok(snapshot) = handle.doc.export_snapshot() else {
            return;
        };
        if self
            .inner
            .store
            .save_snapshot_with_cursor(&handle.chat_id, &snapshot, stats.cursor, persist_epoch)
            .is_err()
        {
            return;
        }
        handle
            .snapshot_bytes
            .store(snapshot.len(), Ordering::Relaxed);
        if handle.purged.load(Ordering::Acquire) {
            let _ = self.inner.store.delete_snapshot(&handle.chat_id);
            return;
        }
        let mutation_arrived = || {
            handle.local_callbacks_pending.load(Ordering::Acquire) != 0
                || handle.local_mutation_epoch.load(Ordering::Acquire) != mutation_epoch
        };
        let persist_zero = || {
            handle.replay_from_zero.store(true, Ordering::Release);
            if let Ok(latest) = handle.doc.export_snapshot() {
                let _ = self.inner.store.save_snapshot_with_cursor(
                    &handle.chat_id,
                    &latest,
                    0,
                    persist_epoch,
                );
                handle.snapshot_bytes.store(latest.len(), Ordering::Relaxed);
                if handle.purged.load(Ordering::Acquire) {
                    let _ = self.inner.store.delete_snapshot(&handle.chat_id);
                }
            }
        };
        if mutation_arrived() {
            persist_zero();
            return;
        }
        let durable_exact = self
            .inner
            .store
            .load_snapshot_with_cursor(&handle.chat_id)
            .ok()
            .flatten()
            .is_some_and(|(_, cursor, epoch)| cursor == stats.cursor && epoch == persist_epoch);
        if !durable_exact {
            return;
        }
        handle.replay_from_zero.store(false, Ordering::Release);
        // Close the final store(false) race: pre-commit does not need the
        // replay mutex, so it may publish a new epoch immediately around this
        // instruction. If it did, restore cursor-zero durability before
        // releasing the chat slot/lifecycle gate.
        if mutation_arrived() {
            persist_zero();
        }
    }

    fn maybe_clear_replay_from_zero(&self, handle: &ChatDocHandle) {
        let _lifecycle = lock(&handle.sink_lifecycle);
        self.maybe_clear_replay_from_zero_locked(handle);
    }

    /// Park an access-denied room client without dropping local work. Pending
    /// bytes move to the host-level queue so they survive eviction and the
    /// chat2 -> chat3 replacement; later local updates see `None` under the
    /// same client lock and append to that queue too.
    fn park_chat2_client(&self, handle: &ChatDocHandle) -> usize {
        let client = {
            let _lifecycle = lock(&handle.sink_lifecycle);
            self.detach_chat2_client_locked(handle)
        };
        self.carry_parked_updates(handle, client)
    }

    /// Invalidate and detach a client while `sink_lifecycle` is held. Do not
    /// drain its pending queue here: the sync actor invokes its sink while
    /// holding its own shared-state mutex, so waiting for that mutex while we
    /// hold the sink lifecycle would invert the order and deadlock. Advancing
    /// the generation first makes every late sink call a no-op once the gate
    /// is released.
    fn detach_chat2_client_locked(&self, handle: &ChatDocHandle) -> Option<zeron_sync::ChatClient> {
        // Invalidate the sink before aborting its actor. Detached HTTPS work
        // may outlive the actor task; its generation check then fails under
        // the lifecycle gate before any import or cursor persist.
        handle.sink_generation.fetch_add(1, Ordering::AcqRel);
        lock(&handle.chat2).take()
    }

    /// Drain a detached client's replay queue after releasing the lifecycle
    /// gate. The local-update callback already sees an empty client slot, so
    /// concurrent deltas append to the same host buffer without a handoff gap.
    fn carry_parked_updates(
        &self,
        handle: &ChatDocHandle,
        client: Option<zeron_sync::ChatClient>,
    ) -> usize {
        let pending = client
            .map(zeron_sync::ChatClient::into_pending_updates)
            .unwrap_or_default();
        let count = pending.len();
        if handle.purged.load(Ordering::Acquire)
            || Self::chat_was_purged(&self.inner, &handle.chat_id)
        {
            lock(&self.inner.chat2_pending_local).remove(&handle.chat_id);
        } else if !pending.is_empty() {
            handle.replay_from_zero.store(true, Ordering::Release);
            for update in pending {
                Self::buffer_chat2_update(&self.inner, &handle.chat_id, update);
            }
        }
        count
    }

    /// Every background task rides the tracker, raced against the shutdown
    /// token: the loops' own exits stay authoritative in normal operation;
    /// the token is the retirement override.
    fn spawn_worker(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        let cancel = self.inner.shutdown.clone();
        self.inner.tasks.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = fut => {}
            }
        });
    }

    /// `spawn_worker` for sites that pre-resolve a runtime handle (callers
    /// reachable from bare sync contexts, where `tasks.spawn` would panic).
    fn spawn_worker_on(
        &self,
        runtime: &tokio::runtime::Handle,
        fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let cancel = self.inner.shutdown.clone();
        self.inner.tasks.spawn_on(
            async move {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = fut => {}
                }
            },
            runtime,
        );
    }

    /// The sessions engine, once wired. `None` before assembly or after
    /// `shutdown_workers` — callers treat both as "executor unavailable".
    fn sessions(&self) -> Option<SessionsEngine> {
        lock(&self.inner.sessions).clone()
    }

    /// Wire the sessions engine (engine assembly; see `SessionsEngine::set_doc_host`).
    pub fn set_sessions(&self, sessions: SessionsEngine) {
        {
            // First set wins (the OnceLock contract this slot replaced).
            let mut slot = lock(&self.inner.sessions);
            if slot.is_none() {
                *slot = Some(sessions);
            }
        }
        // Commands may already be pending in warm-opened docs.
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            let host = self.clone();
            self.spawn_worker(async move { host.drain_commands(&handle).await });
        }
    }

    /// Retire this host's workers (runtime replacement, e.g. sign-out): cancel
    /// and await every spawned task, drop every open chat handle (ending the
    /// weak-keyed room/join loops and watcher streams), and sever the sessions
    /// back-edge so the replaced engine graph can actually drop. Idempotent.
    pub async fn shutdown_workers(&self) {
        self.inner.shutdown.cancel();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        // No join task can reinstall after the tracker has drained. Park
        // every client before the final snapshot so unacknowledged local
        // batches become both host-buffered and cursor-zero durable.
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in &handles {
            self.park_chat2_client(handle);
        }
        // Snapshot open docs BEFORE releasing their handles: the handles map
        // holds the only strong doc refs, and an unflushed doc dies with it.
        self.flush_all();
        // Take the map under the lock, drop the handles outside it.
        let handles = std::mem::take(&mut *lock(&self.inner.handles));
        drop(handles);
        lock(&self.inner.seeding).clear();
        lock(&self.inner.seed_waiting).clear();
        lock(&self.inner.cutover_waiting).clear();
        lock(&self.inner.sessions).take();
    }

    /// Test-only retirement sentinel: reports true once the doc-host graph
    /// has actually been freed.
    #[doc(hidden)]
    pub fn retirement_probe(&self) -> Box<dyn Fn() -> bool + Send + Sync> {
        let weak = Arc::downgrade(&self.inner);
        Box::new(move || weak.upgrade().is_none())
    }

    /// Wire the repos engine (engine assembly) — worktree materialization for
    /// Run commands carrying a [`zeron_proto::WorktreeSpec`].
    pub fn set_repos(&self, repos: crate::repos::Repos) {
        let _ = self.inner.repos.set(repos);
    }

    /// Wire the uploads store (engine assembly) — `pending://` ref resolution
    /// and the transfer-read jail.
    pub fn set_uploads(&self, uploads: crate::uploads::Uploads) {
        let _ = self.inner.uploads.set(uploads);
    }

    /// Wire the peer-link cache (engine assembly, edge runtimes only) — the
    /// transport for queued attachment transfers to a remote host.
    pub fn set_links(&self, links: Arc<zeron_rpc::LinkCache>) {
        let _ = self.inner.links.set(links);
    }

    /// Re-evaluate every open chat's command queue NOW. Called after an
    /// upload commit lands bytes on this device: a Run deferred on those
    /// bytes (`pending://` refs not yet on disk) becomes executable the
    /// moment its transfer completes — event-driven, not timer luck.
    pub fn kick_drains(&self) {
        let handles: Vec<Arc<ChatDocHandle>> =
            lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            let host = self.clone();
            self.spawn_worker(async move { host.drain_commands(&handle).await });
        }
    }

    /// Wire the registry host (engine assembly) — the source of chat-ownership rows.
    pub fn set_registry(&self, registry: RegistryHost) {
        let chats = registry.watch_chats();
        if self.inner.registry.set(registry).is_ok() {
            self.spawn_registry_watcher(chats);
        }
    }

    /// Registry-driven lifecycle: purge the local chat lifecycle when a
    /// registry row disappears (a delete on any device propagates here), and
    /// re-drain warm handles once organization ownership becomes readable.
    fn spawn_registry_watcher(&self, mut chats: watch::Receiver<Vec<zeron_proto::Chat>>) {
        // Capture the receiver's baseline before the task is scheduled. A
        // DeleteChat can otherwise publish between `set_registry` and the
        // watcher's first poll; Tokio watch retains only the latest value, so
        // taking `previous_ids` inside the task would forget the deleted row
        // and the first-cycle suppression would silently miss its tombstone.
        let baseline = self
            .registry()
            .and_then(|registry| registry.read_chats().ok())
            .unwrap_or_else(|| chats.borrow().clone());
        let mut previous_ids: HashSet<String> = baseline.into_iter().map(|chat| chat.id).collect();
        let host = self.clone();
        self.spawn_worker(async move {
            let mut first = true;
            let mut ownership_ready_seen = host.inner.config.edge.is_none();
            loop {
                if !first && chats.changed().await.is_err() {
                    return; // registry host gone (shutdown)
                }
                let initial = first;
                first = false;

                // The initial command drain deliberately fails closed while an
                // organization-shared registry has not synced. Registry readiness is a
                // registry event, not a doc mutation, so no chat task would
                // otherwise revisit those durable pending commands. Re-drain
                // every warm handle exactly once when that gate opens.
                if !ownership_ready_seen
                    && host.registry().is_some_and(RegistryHost::ownership_ready)
                {
                    ownership_ready_seen = true;
                    let handles: Vec<_> = lock(&host.inner.handles).values().cloned().collect();
                    for handle in handles {
                        let worker_host = host.clone();
                        host.spawn_worker(async move {
                            worker_host.drain_commands(&handle).await;
                        });
                    }
                }

                // The watch publisher is intentionally asynchronous. On the
                // first cycle its cached value may predate local mutations
                // that are already visible in RegistryDoc, so compare our
                // synchronous baseline with the authoritative overlay. Later
                // cycles are driven by the watch and use its published value.
                let published = if initial {
                    host.registry()
                        .and_then(|registry| registry.read_chats().ok())
                        .unwrap_or_else(|| chats.borrow_and_update().clone())
                } else {
                    chats.borrow_and_update().clone()
                };
                let current_ids: HashSet<String> =
                    published.iter().map(|chat| chat.id.clone()).collect();
                let removed: Vec<String> = previous_ids.difference(&current_ids).cloned().collect();
                for chat_id in removed {
                    tracing::info!(chat = %chat_id,
                        "registry row removed; purging local chat lifecycle");
                    // Tombstone synchronously before asking the run to stop:
                    // no late writer/sink/recovery may persist while the
                    // asynchronous interrupt unwinds.
                    host.purge_chat(&chat_id);
                    if let Some(sessions) = lock(&host.inner.sessions).clone() {
                        let chat = chat_id.clone();
                        host.spawn_worker(async move {
                            if let Err(err) = sessions.interrupt(&chat).await {
                                tracing::debug!(chat = %chat, error = %err,
                                    "remote chat deletion interrupt skipped");
                            }
                        });
                    }
                }
                previous_ids = current_ids;
            }
        });
    }

    fn persist_cutover_snapshot_locked(
        &self,
        handle: &ChatDocHandle,
        target_gen: u32,
    ) -> Result<(), EngineError> {
        if handle.purged.load(Ordering::Acquire) {
            let _ = self.inner.store.delete_snapshot(&handle.chat_id);
            return Err(EngineError::Other(format!(
                "chat {} was deleted during snapshot persistence",
                handle.chat_id
            )));
        }
        let source = handle.doc.export_snapshot()?;
        handle.snapshot_bytes.store(source.len(), Ordering::Relaxed);
        let stored = self
            .inner
            .store
            .load_snapshot_with_cursor(&handle.chat_id)?;

        if handle.room_gen >= crate::chat2_host::CHAT2_DOC_EPOCH && target_gen > handle.room_gen {
            let (bytes, epoch) = match stored {
                Some((target, _, epoch)) if epoch >= target_gen => {
                    let merged = loro::LoroDoc::new();
                    merged.import(&target).map_err(|err| {
                        EngineError::Other(format!("target snapshot import failed: {err}"))
                    })?;
                    merged.import(&source).map_err(|err| {
                        EngineError::Other(format!("source tail merge failed: {err}"))
                    })?;
                    let merged = SessionDoc::from_doc(merged).export_snapshot()?;
                    (merged, epoch)
                }
                _ => (source, target_gen),
            };
            self.inner.store.save_snapshot_with_cursor(
                &handle.chat_id,
                &bytes,
                0,
                epoch.max(target_gen),
            )?;
            if handle.purged.load(Ordering::Acquire) {
                let _ = self.inner.store.delete_snapshot(&handle.chat_id);
                return Err(EngineError::Other(format!(
                    "chat {} was deleted during snapshot persistence",
                    handle.chat_id
                )));
            }
            handle.replay_from_zero.store(true, Ordering::Release);
            return Ok(());
        }

        let newer_lineage = stored
            .as_ref()
            .is_some_and(|(_, _, epoch)| *epoch > handle.room_gen);
        if newer_lineage {
            if handle.room_gen < crate::chat2_host::CHAT2_DOC_EPOCH {
                let rollback_id = format!("{}.pre-chat2", handle.chat_id);
                if !self.inner.store.has_snapshot(&rollback_id)? {
                    self.inner.store.save_snapshot(&rollback_id, &source)?;
                }
            }
            return Ok(());
        }

        if handle.replay_from_zero.load(Ordering::Acquire)
            && handle.room_gen >= crate::chat2_host::CHAT2_DOC_EPOCH
        {
            let epoch = stored
                .as_ref()
                .map(|(_, _, epoch)| *epoch)
                .unwrap_or(crate::chat2_host::CHAT2_DOC_EPOCH)
                .max(crate::chat2_host::CHAT2_DOC_EPOCH)
                .min(handle.room_gen);
            self.inner
                .store
                .save_snapshot_with_cursor(&handle.chat_id, &source, 0, epoch)?;
        } else {
            self.inner.store.save_snapshot(&handle.chat_id, &source)?;
        }
        // Linearizes with purge without adding a global blocking lock: if the
        // save crossed the terminal fence, remove the just-written row. If the
        // fence flips after this load, purge's own delete necessarily runs
        // after our completed save instead.
        if handle.purged.load(Ordering::Acquire) {
            let _ = self.inner.store.delete_snapshot(&handle.chat_id);
            return Err(EngineError::Other(format!(
                "chat {} was deleted during snapshot persistence",
                handle.chat_id
            )));
        }
        Ok(())
    }

    /// The registry host, once wired (tests may assemble a DocHost without one).
    pub fn registry(&self) -> Option<&RegistryHost> {
        self.inner.registry.get()
    }

    pub fn device_id(&self) -> &str {
        &self.inner.config.device_id
    }

    /// Open (or return) the chat's doc handle: load the local snapshot (or init fresh),
    /// start the change-driven task, and join the edge room when configured.
    pub fn open(&self, chat_id: &str) -> Result<Arc<ChatDocHandle>, EngineError> {
        let purge_fence = Self::purge_fence(&self.inner, chat_id);
        if purge_fence.load(Ordering::Acquire) {
            return Err(EngineError::Other(format!(
                "chat {chat_id} has been deleted"
            )));
        }
        let cached = lock(&self.inner.handles).get(chat_id).cloned();
        if let Some(handle) = cached {
            if handle.generation_frozen.load(Ordering::Acquire) {
                return Err(EngineError::Other(format!(
                    "chat {chat_id} is still being constructed; retry shortly"
                )));
            }
            if handle.retired.load(Ordering::Acquire) {
                return Err(EngineError::Other(format!(
                    "chat {chat_id} is retiring its previous room; retry shortly"
                )));
            }
            handle.touch();
            return Ok(handle);
        }
        // Every chat lives on gen 3 (organization-shared chat3 rooms) — the
        // only generation this fork dials. A stored snapshot may still carry
        // the older epoch-2 cursor namespace; the load below resets its
        // cursor and requeues the full local log.
        let room_gen = 3u32;
        let stored = self.inner.store.load_snapshot_with_cursor(chat_id)?;
        let stored_epoch = stored.as_ref().map(|(_, _, e)| *e).unwrap_or(0);
        let mut snapshot_len = 0usize;
        let mut chat2_cursor = 0u64;
        // Until an own batch is acked in a newer room, retain the minimum
        // thin-lineage epoch. Remote catch-up alone must not suppress the
        // crash-time full-local replay.
        let chat2_epoch = stored_epoch
            .max(crate::chat2_host::CHAT2_DOC_EPOCH)
            .min(room_gen);
        let doc = match stored {
            Some((bytes, cursor, epoch)) => {
                snapshot_len = bytes.len();
                // A room generation owns its own seq namespace. Moving a
                // thin doc from an older namespace must not carry its cursor
                // across: start at zero and enqueue the complete local
                // update log.
                chat2_cursor = if room_gen > epoch {
                    tracing::info!(chat = %chat_id, from_gen = epoch, to_gen = room_gen,
                        old_cursor = cursor,
                        "room generation advanced; resetting cursor and requeueing full local doc");
                    0
                } else {
                    cursor
                };
                let raw = loro::LoroDoc::new();
                raw.import(&bytes)
                    .map_err(|e| EngineError::Other(format!("snapshot import failed: {e}")))?;
                SessionDoc::from_doc(raw)
            }
            None => {
                // First open: stamp the cursor namespace NOW. Plain snapshot
                // saves preserve an existing row's epoch but default a NEW
                // row to 0 — without this stamp, the next open would reset
                // the cursor and replay from zero every boot.
                let doc = SessionDoc::init(chat_id)?;
                if !purge_fence.load(Ordering::Acquire)
                    && let Ok(snapshot) = doc.export_snapshot()
                {
                    let _ = self.inner.store.save_snapshot_with_cursor(
                        chat_id,
                        &snapshot,
                        0,
                        crate::chat2_host::CHAT2_DOC_EPOCH,
                    );
                    if purge_fence.load(Ordering::Acquire) {
                        let _ = self.inner.store.delete_snapshot(chat_id);
                    }
                }
                doc
            }
        };
        let doc = Arc::new(doc);

        let replay_from_zero = Arc::new(AtomicBool::new(false));
        let replay_fence = Arc::new(Mutex::new(()));
        let local_callbacks_pending = Arc::new(AtomicUsize::new(0));
        let local_mutation_epoch = Arc::new(AtomicU64::new(0));
        let local_update_tracking = Arc::new(AtomicBool::new(false));

        let (changed_tx, changed_rx) = watch::channel(0u64);
        let root_replay = replay_from_zero.clone();
        let root_pending = local_callbacks_pending.clone();
        let root_epoch = local_mutation_epoch.clone();
        let root_tracking = local_update_tracking.clone();
        let pre_commit_sub = doc.doc().subscribe_pre_commit(Box::new(move |_| {
            if root_tracking.load(Ordering::Acquire) {
                root_replay.store(true, Ordering::Release);
                root_pending.fetch_add(1, Ordering::AcqRel);
                root_epoch.fetch_add(1, Ordering::AcqRel);
            }
            true
        }));
        let sub = doc.doc().subscribe_root(Arc::new(move |_diff| {
            changed_tx.send_modify(|v| *v = v.wrapping_add(1));
        }));
        // The mirror starts dirty and empty: many opens (command queueing,
        // drains, nudges) never watch the transcript, and the first
        // watch_messages attach materializes it on demand.
        let (messages_tx, _) = watch::channel(Vec::new());

        let handle = Arc::new(ChatDocHandle {
            chat_id: chat_id.to_string(),
            device_id: self.inner.config.device_id.clone(),
            doc: doc.clone(),
            messages_tx,
            mirror_dirty: AtomicBool::new(true),
            last_access: AtomicI64::new(now_ms()),
            snapshot_bytes: AtomicUsize::new(snapshot_len),
            room_gen,
            retired: AtomicBool::new(false),
            purged: purge_fence.clone(),
            // Construction reservation. This handle is not published until
            // its room callback/tracking are installed below.
            generation_frozen: Arc::new(AtomicBool::new(true)),
            sink_lifecycle: Arc::new(Mutex::new(())),
            sink_generation: Arc::new(AtomicU64::new(0)),
            replay_from_zero,
            replay_fence,
            local_callbacks_pending,
            local_mutation_epoch,
            local_update_tracking,
            snapshot_save: Mutex::new(()),
            checkpoint_state: Arc::new(Mutex::new(CheckpointState::default())),
            checkpointed_rejections: AtomicU64::new(0),
            chat2: Mutex::new(None),
            chat2_local_sub: Mutex::new(None),
            command_drain: tokio::sync::Mutex::new(()),
            _pre_commit_sub: pre_commit_sub,
            _sub: sub,
        });
        // Edge room join — offline-tolerant AND supervised. `ChatClient` only
        // self-reconnects AFTER a first successful join; a one-shot attempt
        // here (the pre-LRU design) left the doc silently local-only until
        // app restart whenever the dial hit a transient gap — a post-wake
        // network, `Auth::token()` momentarily `None` around a refresh, an
        // edge deploy. The LRU made that dice-roll constant (every reopen),
        // and a watched doc is pinned against eviction, so nothing ever
        // retried: the exact "transcript frozen until restart" report.
        // Retry on the registry host's capped, jittered backoff; a system
        // wake redials immediately; eviction/purge ends the loop via `weak`.
        let mut join_edge = None;
        if let Some(edge) = &self.inner.config.edge {
            {
                // Subscription BEFORE the dial (review B3): every local
                // commit lands in the client when connected, else in the
                // pending buffer the join drains — nothing composed during
                // (or before) the dial is lost to the room.
                self.install_chat2_local_subscription(&handle);
                handle.generation_frozen.store(false, Ordering::Release);
                // First contact with the room (cursor 0): everything
                // committed BEFORE the subscription above — SessionDoc::
                // init's container/meta ops, an adopt's fresh doc — is
                // invisible to the push path, yet every later commit
                // causally DEPENDS on it. Rows built on unpushed deps import
                // into peers' loro pending-buffers and never materialize:
                // born-chat2 cross-device runs sat invisible on every other
                // device (host never saw the command, viewers never saw the
                // transcript). Push the doc's full update log as the join's
                // first batch; once acked the cursor moves and this never
                // re-arms.
                if chat2_cursor == 0 {
                    // Serialize with the local-update callback. The full log
                    // subsumes every delta buffered before this point, but the
                    // individual deltas remain useful if the full batch is
                    // permanently rejected at the row-size boundary. Prepend
                    // the full log and preserve the carry-over suffix: on
                    // success the repeats are idempotent; on rejection the
                    // small deltas can still drain after an owner seed.
                    let _replay = lock(&handle.replay_fence);
                    let _client_guard = lock(&handle.chat2);
                    handle.replay_from_zero.store(true, Ordering::Release);
                    match doc
                        .doc()
                        .export(loro::ExportMode::updates(&loro::VersionVector::default()))
                    {
                        Ok(bytes) if !bytes.is_empty() => {
                            Self::prepend_chat2_update(&self.inner, chat_id, bytes);
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(chat = %chat_id, error = %err,
                                "chat2 first-contact export failed; peers may stall on missing deps");
                        }
                    }
                }
                join_edge = Some(edge.clone());
            }
        } else {
            handle.generation_frozen.store(false, Ordering::Release);
        }
        // Publish only after pre-commit/local-update tracking and the initial
        // cursor-zero replay batch are ready. A racing open can no longer
        // obtain a writable handle in the subscription gap.
        {
            let mut handles = lock(&self.inner.handles);
            if purge_fence.load(Ordering::Acquire) {
                drop(handles);
                let _ = self.inner.store.delete_snapshot(chat_id);
                return Err(EngineError::Other(format!(
                    "chat {chat_id} has been deleted"
                )));
            }
            if let Some(existing) = handles.get(chat_id) {
                return Ok(existing.clone()); // racing open — keep the first
            }
            handles.insert(chat_id.to_string(), handle.clone());
        }
        if let Some(edge) = join_edge {
            self.spawn_chat2_join(edge, &handle, chat2_cursor, chat2_epoch);
        }
        self.spawn_worker(chat_task(self.clone(), Arc::downgrade(&handle), changed_rx));
        self.evict_over_budget();
        Ok(handle)
    }

    /// chat2 relay join (docs/chat2-sync.md C3): deadline on every dial,
    /// capped jittered backoff, wake redial — and the client resolves only
    /// after full catch-up (checkpoint + rows), so "joined" here means
    /// "transcript converged".
    fn spawn_chat2_join(
        &self,
        edge: EdgeConfig,
        handle: &Arc<ChatDocHandle>,
        cursor: u64,
        cursor_epoch: u32,
    ) {
        let chat = handle.chat_id.clone();
        let doc = handle.doc.clone();
        let store = self.inner.store.clone();
        let http = self.inner.http.clone();
        let device = self.inner.config.device_id.clone();
        let room_gen = handle.room_gen;
        let weak = Arc::downgrade(handle);
        let host = self.clone();
        let mut token_changes = edge.token_changes();
        let client_generation = handle.sink_generation.load(Ordering::Acquire);
        let sink_lifecycle = handle.sink_lifecycle.clone();
        let sink_generation = handle.sink_generation.clone();
        let generation_frozen = handle.generation_frozen.clone();
        let replay_from_zero = handle.replay_from_zero.clone();
        let replay_fence = handle.replay_fence.clone();
        self.spawn_worker(async move {
            let sink = Arc::new(crate::chat2_host::EngineChatSink::new_with_lifecycle(
                &doc,
                store,
                chat.clone(),
                room_gen,
                cursor,
                cursor_epoch,
                sink_lifecycle,
                sink_generation,
                generation_frozen,
                replay_from_zero,
                replay_fence,
            ));
            // The sink holds only a Weak doc ref (a strong one made every
            // chat2 handle read as perma-pinned — LRU eviction dead); this
            // task's own strong ref dies when the join resolves.
            drop(doc);
            let prefix = ROOM_PREFIX;
            let fetcher = Arc::new(crate::chat2_host::EdgeCheckpointFetcher::new(
                http,
                edge.clone(),
                chat.clone(),
            ));
            // chat3 host claim rides the join: the DO's host-user slot gates
            // checkpoint/tail/diff/reset to whoever first joins with role=host.
            let host_query = if host.is_host(&chat) { "&role=host" } else { "" };
            let url = edge.room_url_with(format!("/{prefix}/{chat}/ws"), host_query);
            let mut wake = zeron_sync::wake::subscribe();
            // Sibling-dial successes end a backoff wait immediately, exactly
            // like the joined clients' own reconnect loops (chat_client.rs).
            // Without this, a NEW chat whose first joins hit a network blip
            // waited out the full accumulated backoff (→30s) while every
            // established room redialed instantly on recovery — fresh sends
            // to new sessions stalled while other chats hummed (2026-08-19
            // user report, reproduced on two networks).
            let mut online = zeron_sync::wake::subscribe_online();
            let mut backoff = crate::registry_host::JOIN_RETRY_BASE;
            loop {
                if weak.upgrade().is_none() {
                    return; // evicted or purged while dialing
                }
                // Dual transport: WS dial + a plain-HTTPS pull/push seam
                // (rows GET / POST on the same bearer auth as the checkpoint
                // fetch) — bootstraps in ~1 RTT and keeps syncing at backoff
                // cadence on networks that never pass the WS upgrade. With
                // the transport, connect resolves immediately (local-first).
                let transport = Arc::new(crate::chat2_host::EdgeChatTransport::new(
                    host.inner.http.clone(),
                    edge.clone(),
                    chat.clone(),
                    device.clone(),
                ));
                let dial = tokio::time::timeout(
                    std::time::Duration::from_secs(60),
                    zeron_sync::ChatClient::connect_via_transport(
                        url.clone(),
                        sink.clone(),
                        fetcher.clone(),
                        &device,
                        cursor,
                        transport,
                    ),
                )
                .await;
                match dial {
                    Ok(Ok(client)) => {
                        if edge.bearer().await.is_none() {
                            return;
                        }
                        let Some(handle) = weak.upgrade() else {
                            return; // evicted mid-dial: drop leaves the room
                        };
                        let mut events = client.events();
                        let mut lifecycle_events = client.events();
                        host.install_chat2_client_for_generation(
                            &handle,
                            client,
                            client_generation,
                        );
                        tracing::info!(chat = %chat, "chat2 room joined (converged)");
                        // Bootstrap heal: a room with NO checkpoint can't
                        // cover its rows' causal deps for cold readers — a
                        // pre-0.1.34 first contact whose init batch never
                        // went up (every reader parks every row on missing
                        // deps, transcript invisible forever), or a host
                        // whose WS pushes strand. The checkpoint is the
                        // universal patch: full doc over plain HTTP. Checked
                        // once, shortly after join (an idle chat never hits
                        // the quiesce tick, so the tick can't be the only
                        // trigger).
                        if host.is_host(&chat) {
                            let host = host.clone();
                            let weak = weak.clone();
                            host.clone().spawn_worker(async move {
                                // With the pull-first transport the client
                                // constructs before any state answer. Busy and
                                // unknown are not negative verdicts: this is a
                                // one-shot recovery duty, so keep it armed until
                                // a real server view arrives or the handle dies.
                                let no_checkpoint = loop {
                                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                                    let Some(handle) = weak.upgrade() else { return };
                                    if handle.generation_frozen.load(Ordering::Acquire)
                                        || handle.retired.load(Ordering::Acquire)
                                        || handle.purged.load(Ordering::Acquire)
                                    {
                                        return;
                                    }
                                    let stats = lock(&handle.chat2)
                                        .as_ref()
                                        .and_then(zeron_sync::ChatClient::try_stats);
                                    if let Some(stats) = stats
                                        && stats.server_known
                                    {
                                        break stats.checkpoint_size == 0;
                                    }
                                };
                                let Some(handle) = weak.upgrade() else { return };
                                let has_content = handle
                                    .doc
                                    .read_entries()
                                    .map(|e| !e.is_empty())
                                    .unwrap_or(false);
                                if no_checkpoint && has_content {
                                    tracing::info!(chat = %handle.chat_id,
                                        "chat2 room has rows but no checkpoint; posting bootstrap checkpoint");
                                    host.spawn_chat2_checkpoint(&handle, "bootstrap");
                                }
                            });
                        }
                        // Host recovery duties (C3): a wiped room needs a
                        // seed checkpoint or fresh readers see only
                        // post-reset rows; rejected pushes reach peers only
                        // through a checkpoint. Watcher dies with the handle.
                        if host.is_host(&chat) {
                            let host = host.clone();
                            let weak = weak.clone();
                            let chat = chat.clone();
                            host.clone().spawn_worker(async move {
                                use zeron_sync::chat_client::ChatEvent;
                                loop {
                                    match events.recv().await {
                                        Ok(ChatEvent::ServerReset) => {
                                            let Some(handle) = weak.upgrade() else { return };
                                            tracing::warn!(chat = %chat, "chat2 room reset; posting seed checkpoint");
                                            host.spawn_chat2_checkpoint(&handle, "server-reset");
                                        }
                                        Ok(ChatEvent::PushRejected) => {
                                            let Some(handle) = weak.upgrade() else { return };
                                            tracing::warn!(chat = %chat, "chat2 push rejected; compensating via checkpoint");
                                            host.spawn_chat2_checkpoint(&handle, "push-rejected");
                                        }
                                        Ok(_) => {}
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                                    }
                                }
                            });
                        }
                        drop(handle);
                        loop {
                            tokio::select! {
                                event = lifecycle_events.recv() => match event {
                                    Ok(zeron_sync::chat_client::ChatEvent::Applied) => {
                                        if let Some(handle) = weak.upgrade() {
                                            host.maybe_clear_replay_from_zero(&handle);
                                        }
                                    }
                                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                                },
                                _ = crate::registry_host::token_changed(&mut token_changes) => {
                                    if edge.bearer().await.is_none() {
                                        if let Some(handle) = weak.upgrade() {
                                            // Preserve unacknowledged rows and
                                            // keep buffering local shutdown
                                            // mutations after credentials are
                                            // revoked.
                                            host.park_chat2_client(&handle);
                                        }
                                        tracing::info!(chat = %chat,
                                            "chat2 credentials removed; leaving room");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(chat = %chat, error = %err,
                            backoff_ms = backoff.as_millis() as u64,
                            "chat2 join failed; retrying");
                    }
                    Err(_) => {
                        tracing::warn!(chat = %chat,
                            backoff_ms = backoff.as_millis() as u64,
                            "chat2 join timed out; retrying");
                    }
                }
                // Drain stale online events first: only successes DURING this
                // wait count, or our own last dial would cut every wait to
                // zero (same discipline as chat_client's wait_backoff).
                while online.try_recv().is_ok() {}
                tokio::select! {
                    _ = tokio::time::sleep(backoff + crate::registry_host::join_retry_jitter()) => {
                        backoff = (backoff * 2).min(crate::registry_host::JOIN_RETRY_CAP);
                    }
                    _ = wake.recv() => {
                        backoff = crate::registry_host::JOIN_RETRY_BASE;
                    }
                    _ = online.recv() => {
                        backoff = crate::registry_host::JOIN_RETRY_BASE;
                    }
                    _ = crate::registry_host::token_changed(&mut token_changes) => {
                        backoff = crate::registry_host::JOIN_RETRY_BASE;
                    }
                }
            }
        });
    }

    /// chat2 host duties on the doc-quiesce tick (docs/chat2-sync.md C3):
    /// - threshold checkpoint: when the room's row log passes 512KB or 200
    ///   rows, post a full checkpoint so cold readers load one compact blob
    ///   instead of replaying the log (the alert-shaped growth bound);
    /// - tail sidecar: publish the last-64 transcript JSON for thin/instant
    ///   readers (the iOS fallback path).
    async fn chat2_maintenance(&self, handle: &Arc<ChatDocHandle>) {
        if handle.retired.load(Ordering::Relaxed) {
            return;
        }
        // Host duties only: viewers of a doc hosted elsewhere would get a
        // silent 403 on every quiesce tick (tail PUT and checkpoint POST are
        // both host-slot writes on the edge).
        if !self.is_host(&handle.chat_id) {
            return;
        }
        let stats = match &*lock(&handle.chat2) {
            Some(client) => match client.try_stats() {
                Some(stats) => stats,
                None => return,
            },
            None => return,
        };
        let Some(edge) = self.inner.config.edge.clone() else {
            return;
        };
        let chat_id = handle.chat_id.clone();
        // Tail publish: cheap, every quiesce tick.
        if let Ok(tail) =
            zeron_doc::materialize_tail(&handle.doc, now_ms(), zeron_doc::TAIL_MESSAGE_COUNT)
            && let Ok(body) = serde_json::to_vec(&tail)
        {
            let http = self.inner.http.clone();
            let edge_tail = edge.clone();
            let chat = chat_id.clone();
            let prefix = ROOM_PREFIX;
            let host = self.clone();
            self.spawn_worker(async move {
                let Some(bearer) = edge_tail.bearer().await else {
                    return;
                };
                // Recheck ownership after the (async) bearer fetch: a re-home
                // or unshare in that window must not let a former host publish
                // a stale tail sidecar.
                if !host.is_host(&chat) {
                    return;
                }
                let url = format!(
                    "{}/{}/{}/tail",
                    edge_tail.url.trim_end_matches('/'),
                    prefix,
                    chat
                );
                let _ = http
                    .put(&url)
                    .bearer_auth(&bearer)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await;
            });
        }
        // Threshold checkpoint (rowBytes > 512KB || rows > 200), one in
        // flight at a time (review H1).
        if stats.row_bytes <= 512 * 1024 && stats.row_count <= 200 {
            return;
        }
        self.spawn_chat2_checkpoint(handle, "threshold");
    }

    /// POST a full checkpoint for a chat2 room (one in flight per handle).
    /// Callers: the quiesce-tick threshold above, and the client recovery
    /// events (`ServerReset` — a wiped room needs a seed checkpoint or every
    /// fresh reader sees only post-reset rows; `PushRejected` — the rejected
    /// ops reach peers only through a checkpoint).
    fn spawn_chat2_checkpoint(&self, handle: &Arc<ChatDocHandle>, reason: &'static str) {
        use base64::Engine as _;
        let Some(edge) = self.inner.config.edge.clone() else {
            return;
        };
        if handle.generation_frozen.load(Ordering::Acquire)
            || handle.retired.load(Ordering::Acquire)
            || handle.purged.load(Ordering::Acquire)
        {
            return;
        }
        let should_spawn = {
            let mut state = lock(&handle.checkpoint_state);
            state.requested = true;
            state.reason = reason;
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        if !should_spawn {
            return;
        }

        const BUSY_RETRY: std::time::Duration = std::time::Duration::from_millis(25);
        const POST_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(250);
        const POST_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(30);

        let host = self.clone();
        let weak = Arc::downgrade(handle);
        let state = handle.checkpoint_state.clone();
        let http = self.inner.http.clone();
        self.spawn_worker(async move {
            loop {
                // Claim one coalesced request. `running=false` and the empty
                // request decision happen under the same mutex as producers,
                // so a trigger can neither be lost at worker exit nor spawn a
                // second uploader while this one still owns the slot.
                let reason = {
                    let mut checkpoint = lock(&state);
                    if !checkpoint.requested {
                        checkpoint.running = false;
                        return;
                    }
                    checkpoint.requested = false;
                    checkpoint.reason
                };

                let mut post_backoff = POST_RETRY_BASE;
                loop {
                    let Some(handle) = weak.upgrade() else {
                        let mut checkpoint = lock(&state);
                        checkpoint.requested = false;
                        checkpoint.running = false;
                        return;
                    };
                    let current = lock(&host.inner.handles)
                        .get(&handle.chat_id)
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, &handle));
                    if !current
                        || handle.generation_frozen.load(Ordering::Acquire)
                        || handle.retired.load(Ordering::Acquire)
                        || handle.purged.load(Ordering::Acquire)
                    {
                        let mut checkpoint = lock(&state);
                        checkpoint.requested = false;
                        checkpoint.running = false;
                        return;
                    }

                    // Never wait on ChatClient shared state while owning the
                    // engine chat slot. Busy and a temporarily-empty slot are
                    // retryable observations, not reasons to drop a recovery
                    // event.
                    let stats = lock(&handle.chat2)
                        .as_ref()
                        .and_then(zeron_sync::ChatClient::try_stats);
                    let Some(stats) = stats else {
                        drop(handle);
                        tokio::time::sleep(BUSY_RETRY).await;
                        continue;
                    };
                    let Ok(snapshot) = handle.doc.export_snapshot() else {
                        drop(handle);
                        tokio::time::sleep(post_backoff).await;
                        post_backoff = (post_backoff * 2).min(POST_RETRY_CAP);
                        continue;
                    };
                    let frontier = handle.doc.doc().oplog_vv().encode();
                    let seq_covered = stats.cursor;
                    let rejected_covered = stats.rejected;
                    let size = snapshot.len() as u64;
                    let chat_id = handle.chat_id.clone();
                    drop(handle);

                    let Some(bearer) = edge.bearer().await else {
                        tokio::time::sleep(post_backoff).await;
                        post_backoff = (post_backoff * 2).min(POST_RETRY_CAP);
                        continue;
                    };
                    // Recheck ownership immediately before the POST: a live
                    // re-home (or an unshare) between retries must stop a
                    // former host from publishing a stale checkpoint. The
                    // edge's 403 covers a different-user takeover; this covers
                    // same-user device migration, which the edge accepts.
                    let still_current = host.is_host(&chat_id)
                        && weak.upgrade().is_some_and(|handle| {
                            !handle.generation_frozen.load(Ordering::Acquire)
                                && !handle.retired.load(Ordering::Acquire)
                                && !handle.purged.load(Ordering::Acquire)
                                && lock(&host.inner.handles)
                                    .get(&chat_id)
                                    .is_some_and(|candidate| Arc::ptr_eq(candidate, &handle))
                        });
                    if !still_current {
                        let mut checkpoint = lock(&state);
                        checkpoint.requested = false;
                        checkpoint.running = false;
                        return;
                    }

                    // role=host: claims the chat3 host-user slot if the ws
                    // join's claim hasn't landed yet (no-op for the established
                    // host).
                    let url = format!(
                        "{}/{}/{}/checkpoint?seqCovered={}&role=host",
                        edge.url.trim_end_matches('/'),
                        ROOM_PREFIX,
                        chat_id,
                        seq_covered
                    );
                    match http
                        .post(&url)
                        .bearer_auth(&bearer)
                        .header(
                            "x-chat2-frontier",
                            base64::engine::general_purpose::STANDARD.encode(&frontier),
                        )
                        .body(snapshot)
                        .send()
                        .await
                    {
                        Ok(res) if res.status().is_success() => {
                            tracing::info!(chat = %chat_id, seq_covered, reason,
                                "chat2 checkpoint posted");
                            if let Some(handle) = weak.upgrade() {
                                handle
                                    .checkpointed_rejections
                                    .fetch_max(rejected_covered, Ordering::AcqRel);
                                {
                                    let client_slot = lock(&handle.chat2);
                                    if let Some(client) = &*client_slot {
                                        client.try_note_checkpoint(seq_covered, size);
                                    }
                                }
                                host.maybe_clear_replay_from_zero(&handle);
                            }
                            break;
                        }
                        Ok(res) if res.status() == 403 => {
                            // Authorization verdict (chat-room.ts gate → 403):
                            // this device does not hold, and will not be
                            // granted, the room's host slot — e.g. a viewer of
                            // a doc hosted elsewhere. Retrying every 30s per
                            // denied doc lived forever. (401 stays retryable —
                            // it is token expiry/JWKS, which a refresh heals.)
                            tracing::warn!(chat = %chat_id, status = res.status().as_u16(), reason,
                                "chat2 checkpoint refused (not this room's host); giving up");
                            break;
                        }
                        Ok(res) => {
                            tracing::warn!(chat = %chat_id, status = res.status().as_u16(), reason,
                                "chat2 checkpoint rejected; retrying");
                        }
                        Err(err) => {
                            tracing::warn!(chat = %chat_id, error = %err, reason,
                                "chat2 checkpoint POST failed; retrying");
                        }
                    }
                    tokio::time::sleep(post_backoff).await;
                    post_backoff = (post_backoff * 2).min(POST_RETRY_CAP);
                }
            }
        });
    }

    /// LRU eviction: while the warm set exceeds [`WARM_DOC_CAP`] or the
    /// resident estimate exceeds `DOC_LRU_BYTE_BUDGET`, close the
    /// least-recently-touched unpinned docs. Pinned (never evicted):
    /// - watched docs (`messages_tx` has receivers — a UI transcript);
    /// - docs with a live writer (`Arc<SessionDoc>` held outside the handle —
    ///   a run streaming into it);
    /// - host-side docs with pending commands (the executor owes them work).
    ///
    /// Eviction flushes a final snapshot, so reopen loses nothing; missed
    /// remote updates re-arrive through the room join's VV backfill.
    fn evict_over_budget(&self) {
        let mut by_age: Vec<(i64, String)> = {
            let handles = lock(&self.inner.handles);
            handles
                .values()
                .map(|h| (h.last_access.load(Ordering::Relaxed), h.chat_id.clone()))
                .collect()
        };
        by_age.sort_unstable();
        for (last_access, chat_id) in by_age {
            if now_ms() - last_access < EVICT_MIN_IDLE_MS {
                // Sorted oldest-first: everything after this is younger.
                return;
            }
            let (count, estimate) = {
                let handles = lock(&self.inner.handles);
                (
                    handles.len(),
                    handles
                        .values()
                        .map(|h| h.resident_estimate())
                        .sum::<usize>(),
                )
            };
            if count <= WARM_DOC_CAP && estimate <= zeron_doc::DOC_LRU_BYTE_BUDGET {
                return;
            }
            let candidate = lock(&self.inner.handles).get(&chat_id).cloned();
            let Some(candidate) = candidate else { continue };
            // Match the generation coordinator's order. An active executor
            // owns the doc until its outcome has landed and is never evicted.
            let Ok(_drain) = candidate.command_drain.try_lock() else {
                continue;
            };
            let evicted = (|| -> Result<_, EngineError> {
                let mut handles = lock(&self.inner.handles);
                let Some(current) = handles.get(&chat_id).cloned() else {
                    return Ok(None);
                };
                if !Arc::ptr_eq(&current, &candidate) || self.pinned_without_drain(&current) {
                    return Ok(None);
                }
                let _lifecycle = lock(&current.sink_lifecycle);
                if self.pinned_without_drain(&current) {
                    return Ok(None);
                }
                let _save = lock(&current.snapshot_save);
                // The client replay queue is process-local. Persist the full
                // thin lineage at cursor zero before detaching it so restart
                // remains correct even if the host buffer never survives.
                if current.room_gen >= crate::chat2_host::CHAT2_DOC_EPOCH {
                    current.replay_from_zero.store(true, Ordering::Release);
                }
                self.persist_cutover_snapshot_locked(&current, current.room_gen)?;
                current.retired.store(true, Ordering::Release);
                drop(lock(&current.chat2_local_sub).take());
                let parked = self.detach_chat2_client_locked(&current);
                drop(_save);
                drop(_lifecycle);
                handles.remove(&chat_id);
                Ok(Some((current, parked)))
            })();
            match evicted {
                Ok(Some((handle, parked))) => {
                    self.carry_parked_updates(&handle, parked);
                    tracing::debug!(chat = %handle.chat_id, "doc evicted (LRU)");
                }
                Ok(None) => {}
                Err(err) => tracing::warn!(chat = %chat_id, error = %err,
                    "doc eviction snapshot failed; keeping handle resident"),
            }
        }
    }

    fn pinned_without_drain(&self, handle: &Arc<ChatDocHandle>) -> bool {
        if handle.generation_frozen.load(Ordering::Acquire) {
            return true;
        }
        if handle.messages_tx.receiver_count() > 0 {
            return true;
        }
        // The handle itself holds one doc ref; more means a live writer.
        if Arc::strong_count(&handle.doc) > 1 {
            return true;
        }
        if self.is_host(&handle.chat_id) {
            let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
            match handle.doc.read_commands() {
                Ok(commands) => commands
                    .iter()
                    .any(|c| c.status == SessionCommandStatus::Pending && !is_processed(&c.id)),
                // Unreadable ledger: keep the doc, never evict blind.
                Err(_) => true,
            }
        } else {
            false
        }
    }

    /// Probe every open chat's room (window-focus liveness sweep). Each
    /// room ignores the hint unless it has been broadcast-quiet ≥30s.
    pub fn probe_open_chats(&self) {
        let handles: Vec<Arc<ChatDocHandle>> =
            lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            // chat2 rooms verify liveness on user signals — a
            // deaf-but-ponging DO otherwise freezes a watched transcript
            // for the whole background probe quiet window.
            if let Some(chat2) = lock(&handle.chat2).as_ref() {
                chat2.probe();
            }
        }
    }

    /// Window-focus fast path: one cheap HTTP probe of the edge decides
    /// whether to un-park every reconnect backoff NOW (success → online
    /// event → immediate redials with fresh backoff) or to leave them
    /// backing off (failure — a dial can't succeed either, so don't burn
    /// the attempt). Recovery rides the "user looked at the app" event
    /// instead of timer luck.
    pub fn probe_edge_reachability(&self) {
        let Some(edge) = self.inner.config.edge.clone() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let url = format!("{}/health", edge.url.trim_end_matches('/'));
        let http = self.inner.http.clone();
        self.spawn_worker_on(&runtime, async move {
            let res = http
                .get(&url)
                .timeout(std::time::Duration::from_secs(3))
                .send()
                .await;
            if let Ok(res) = res
                && res.status().is_success()
            {
                zeron_sync::wake::notify_online();
            }
        });
    }

    /// The connectivity stream: current posture first, then every change.
    /// Lazily starts a monitor — a 1s recompute over in-memory stats
    /// (atomics + small locks), published only when the value changes. The
    /// retry countdown renders client-side from `retry_at_ms`, so quiet
    /// periods emit nothing at all.
    pub fn watch_connectivity(&self) -> watch::Receiver<zeron_proto::Connectivity> {
        let tx = self
            .inner
            .connectivity
            .get_or_init(|| watch::channel(self.compute_connectivity()).0);
        let rx = tx.subscribe();
        if !self.inner.connectivity_started.swap(true, Ordering::SeqCst)
            && tokio::runtime::Handle::try_current().is_ok()
        {
            let host = self.clone();
            self.spawn_worker(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let next = host.compute_connectivity();
                    if let Some(tx) = host.inner.connectivity.get() {
                        tx.send_if_modified(|cur| {
                            if *cur == next {
                                false
                            } else {
                                *cur = next;
                                true
                            }
                        });
                    }
                }
            });
        }
        rx
    }

    /// One snapshot of the edge posture: OS path status beats registry-room
    /// state beats per-chat rooms. `Disabled` (the default) = local profile.
    ///
    /// Degradation is HYSTERETIC (v0.2.12 feedback): a room mid-join, an
    /// idle link waking for a send, or a navigation-triggered dial all read
    /// "disconnected" for a few hundred ms on a healthy network — surfacing
    /// those flashed amber warnings and "Queued" badges at every chat
    /// switch. Raw degradation must persist [`DEGRADE_GRACE`] before it is
    /// reported; recovery reports instantly.
    fn compute_connectivity(&self) -> zeron_proto::Connectivity {
        use zeron_proto::{ChatConnectivity, Connectivity, ConnectivityState};
        let registry = self.registry();
        let edge_expected = self.inner.config.edge.is_some()
            || registry.as_ref().is_some_and(|w| w.edge_expected());
        if !edge_expected {
            return Connectivity::default();
        }
        let now = std::time::Instant::now();
        let mut grace = lock(&self.inner.connectivity_grace);
        let statuses = self.sync_statuses();
        grace.retain_chats(|id| statuses.iter().any(|(chat_id, _)| chat_id == id));
        let chats = statuses
            .into_iter()
            .map(|(chat_id, stats)| {
                let stats = stats.unwrap_or_default();
                let connected = !grace.degraded(GraceKey::Chat(&chat_id), !stats.connected, now);
                ChatConnectivity {
                    chat_id,
                    connected,
                    pending_pushes: stats.pending_pushes,
                }
            })
            .collect();
        let reconnect = registry.as_ref().and_then(|w| w.reconnect_state());
        let registry_connected = registry
            .as_ref()
            .and_then(|w| w.sync_status())
            .is_some_and(|s| s.connected);
        let path_offline =
            grace.degraded(GraceKey::OsPath, zeron_sync::wake::path_is_offline(), now);
        let registry_down = grace.degraded(GraceKey::Registry, !registry_connected, now);
        let (state, retry_at_ms, last_failure) = if path_offline {
            (
                ConnectivityState::Offline,
                0,
                reconnect.and_then(|r| r.last_failure),
            )
        } else if !registry_down {
            (ConnectivityState::Connected, 0, None)
        } else {
            let reconnect = reconnect.unwrap_or_default();
            (
                ConnectivityState::Reconnecting,
                reconnect.retry_at_ms,
                reconnect.last_failure,
            )
        };
        Connectivity {
            state,
            retry_at_ms,
            last_failure,
            chats,
        }
    }

    /// Per-open-chat room introspection for SyncStatus / `zeron sync`.
    /// `None` room = still dialing (join retry loop) or edge-less.
    pub fn sync_statuses(&self) -> Vec<(String, Option<zeron_sync::ChatStatsSnapshot>)> {
        let handles: Vec<Arc<ChatDocHandle>> =
            lock(&self.inner.handles).values().cloned().collect();
        let mut rows: Vec<(String, Option<zeron_sync::ChatStatsSnapshot>)> = handles
            .iter()
            .map(|h| {
                (
                    h.chat_id.clone(),
                    lock(&h.chat2)
                        .as_ref()
                        .and_then(zeron_sync::ChatClient::try_stats),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    /// Drop a chat's doc unconditionally and delete its local snapshot — the
    /// chat is gone (DeleteChat / DeleteSpace cascade). Watchers see the
    /// stream end; a racing writer keeps its orphaned doc until the run ends.
    pub fn purge_chat(&self, chat_id: &str) {
        // Publish the process-lifetime tombstone before looking in `handles`.
        // A migration may already have removed the source handle and be about
        // to carry pending bytes or reopen; the shared Arc fence still reaches
        // that detached task and makes every continuation fail closed.
        let purge_fence = Self::purge_fence(&self.inner, chat_id);
        purge_fence.store(true, Ordering::Release);
        let detached = {
            let mut handles = lock(&self.inner.handles);
            let current = handles.get(chat_id).cloned();
            match current {
                Some(handle) => {
                    let _lifecycle = lock(&handle.sink_lifecycle);
                    handle.purged.store(true, Ordering::Release);
                    handle.generation_frozen.store(true, Ordering::Release);
                    handle.retired.store(true, Ordering::Release);
                    handle.sink_generation.fetch_add(1, Ordering::AcqRel);
                    drop(lock(&handle.chat2_local_sub).take());
                    // Close the callback's client-vs-host-buffer decision and
                    // clear both destinations before releasing the slot. A
                    // callback that passed its first frozen check performs a
                    // second one after this lock and cannot repopulate either.
                    let client = lock(&handle.chat2).take();
                    lock(&self.inner.chat2_pending_local).remove(chat_id);
                    handles.remove(chat_id);
                    client
                }
                None => {
                    lock(&self.inner.chat2_pending_local).remove(chat_id);
                    None
                }
            }
        };
        // Drop/abort the actor only after releasing engine lifecycle locks.
        drop(detached);
        if let Err(err) = self.inner.store.delete_snapshot(chat_id) {
            tracing::warn!(chat = %chat_id, error = %err, "snapshot delete failed");
        }
        let rollback_id = format!("{chat_id}.pre-chat2");
        if let Err(err) = self.inner.store.delete_snapshot(&rollback_id) {
            tracing::warn!(chat = %chat_id, error = %err, "rollback snapshot delete failed");
        }
        lock(&self.inner.seeding).remove(chat_id);
        lock(&self.inner.seed_waiting).remove(chat_id);
        lock(&self.inner.cutover_waiting).remove(chat_id);
    }

    /// Composer path: append an immutable pending command entry (rule 1). Durable by
    /// construction — the change subscription kicks the drain, so a local host executes
    /// immediately and an offline doc simply holds the entry until it syncs.
    pub fn queue_command(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
    ) -> Result<String, EngineError> {
        self.queue_command_with_transfers(chat_id, payload, Vec::new())
    }

    /// [`Self::queue_command`] with explicit attribution: `user_id` names the
    /// issuing user (`agent:{chatId}` for agent-to-agent sends), `origin`
    /// carries send-chain provenance for hop limiting.
    pub fn queue_command_as(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
        user_id: Option<String>,
        origin: Option<CommandOrigin>,
    ) -> Result<String, EngineError> {
        self.queue_command_inner(chat_id, payload, user_id, origin, Vec::new())
    }

    /// [`Self::queue_command`] plus queued-attachment transfers: the command's
    /// `pending://` refs name bytes already committed to THIS device's uploads
    /// dir; when another device hosts the chat, a background task pushes them
    /// over the peer link (retry until they land) while the command is
    /// already durably queued. The send is a local write — attachment bytes
    /// chase it, never gate it (2026-08-19 incident).
    pub fn queue_command_with_transfers(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
        transfers: Vec<crate::uploads::AttachmentTransfer>,
    ) -> Result<String, EngineError> {
        let user = match self.inner.config.user_id.as_str() {
            "" => None,
            u => Some(u.to_string()),
        };
        self.queue_command_inner(chat_id, payload, user, None, transfers)
    }

    /// The one queue path: attribution (fork's agent-to-agent sends) and
    /// queued-attachment transfers (upstream's durable-send escort) are
    /// independent, so both ride the same entry construction.
    fn queue_command_inner(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
        user_id: Option<String>,
        origin: Option<CommandOrigin>,
        transfers: Vec<crate::uploads::AttachmentTransfer>,
    ) -> Result<String, EngineError> {
        let handle = self.open(chat_id)?;
        let _lifecycle = lock(&handle.sink_lifecycle);
        handle.ensure_mutable()?;
        let id = new_id();
        let now = now_ms();
        let based_on = handle.doc.read_entries()?.last().map(|m| CommandBasedOn {
            turn_id: Some(m.id.clone()),
            frontier: None,
        });
        let is_message = matches!(
            payload,
            SessionCommandPayload::Run { .. } | SessionCommandPayload::Steer { .. }
        );
        let entry = SessionCommandEntry {
            id: id.clone(),
            payload,
            issued_by: self.inner.config.device_id.clone(),
            issued_at: now,
            user_id,
            origin,
            based_on,
            expires_at: Some(now + COMMAND_DEFAULT_TTL_MS),
            status: SessionCommandStatus::Pending,
            resolution: None,
        };
        handle.doc.queue_command(&entry)?;
        // Sending a message revives an archived chat: the user is acting in it
        // again, so the LWW row flips back to active on every device. Best-
        // effort — the command itself is durable regardless.
        if is_message {
            if let Some(registry) = self.registry() {
                match registry.chat(chat_id) {
                    Ok(Some(chat)) if chat.archived => {
                        if let Err(err) = registry.set_chat_archived(chat_id, false) {
                            tracing::warn!(chat = %chat_id, error = %err, "unarchive on send failed");
                        }
                    }
                    _ => {}
                }
            }
        }
        // §7 durable delivery: when another device hosts this chat, nudge its device
        // room so a cold host opens the doc and drains the queue. Fire-and-forget —
        // the command is durable in the doc either way (a host that opens the chat
        // for any other reason still executes it).
        self.nudge_remote_host(chat_id);
        self.spawn_command_delivery(chat_id, entry, transfers);
        Ok(id)
    }

    /// Agent-to-agent send: queue a Steer into `target_chat`'s doc, attributed
    /// `agent:{from_chat}`, delivered by the target's host through the normal
    /// command plane (offline-queued, nudged, deduped). Loop protection:
    /// hop depth (from the live turn's origin) capped at [`MAX_AGENT_HOPS`],
    /// plus a per-turn send budget per source chat.
    pub fn send_to_session(
        &self,
        from_chat: &str,
        target_chat: &str,
        message: &str,
    ) -> Result<String, EngineError> {
        const MAX_AGENT_HOPS: u32 = 4;
        const MAX_SENDS_PER_TURN: u32 = 8;
        if from_chat == target_chat {
            return Err(EngineError::Other("cannot send to the own session".into()));
        }
        if message.trim().is_empty() {
            return Err(EngineError::Other("empty message".into()));
        }
        let hops = lock(&self.inner.turn_origins)
            .get(from_chat)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                EngineError::Other(format!(
                    "agent send chain too deep (>{MAX_AGENT_HOPS} hops) — refusing to ping-pong"
                ))
            })?;
        if hops > MAX_AGENT_HOPS {
            return Err(EngineError::Other(format!(
                "agent send chain too deep (>{MAX_AGENT_HOPS} hops) — refusing to ping-pong"
            )));
        }
        // ponytail: per-turn counter; per-target token bucket if abuse shows up
        {
            let mut budgets = lock(&self.inner.send_budgets);
            let spent = budgets.entry(from_chat.to_string()).or_insert(0);
            if *spent >= MAX_SENDS_PER_TURN {
                return Err(EngineError::Other(format!(
                    "send budget exhausted ({MAX_SENDS_PER_TURN} per turn)"
                )));
            }
            *spent += 1;
        }
        self.queue_command_as(
            target_chat,
            SessionCommandPayload::Steer {
                prompt: message.to_string(),
                message_id: Some(new_id()),
            },
            Some(format!("agent:{from_chat}")),
            Some(CommandOrigin {
                from_chat_id: from_chat.to_string(),
                hops,
            }),
        )
    }

    /// Test seam: pretend `chat_id`'s live turn originated from an agent send
    /// chain `hops` deep (what `execute` records for real turns).
    #[cfg(test)]
    pub(crate) fn set_turn_origin_for_test(&self, chat_id: &str, hops: u32) {
        lock(&self.inner.turn_origins).insert(chat_id.to_string(), hops);
    }

    /// POST `{edge}/device/{host}/nudge {chatId}` when the chat's registry row names
    /// another device as host. Best-effort: offline/edge-less engines skip silently.
    fn nudge_remote_host(&self, chat_id: &str) {
        let Some(edge) = self.inner.config.edge.clone() else {
            return;
        };
        let Some(registry) = self.registry() else {
            return;
        };
        let host_device = match registry.chat(chat_id) {
            Ok(Some(chat)) => chat.device_id,
            // Unclaimed chat: whoever drains first claims it — nobody to nudge.
            _ => return,
        };
        if host_device == self.inner.config.device_id {
            return;
        }
        // Only meaningful inside a runtime (RPC handlers, executors); bare sync
        // callers (unit tests) skip rather than panic.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let url = format!(
            "{}/device/{}/nudge",
            edge.url.trim_end_matches('/'),
            host_device
        );
        let chat = chat_id.to_string();
        self.spawn_worker_on(&runtime, async move {
            // Fresh bearer per request — never the boot-time snapshot.
            let Some(bearer) = edge.bearer().await else {
                tracing::warn!(chat = %chat, "nudge skipped: signed out");
                return;
            };
            let send = reqwest::Client::new()
                .post(&url)
                .bearer_auth(&bearer)
                .json(&serde_json::json!({ "chatId": chat }))
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;
            match send {
                Ok(res) if res.status().is_success() => {
                    tracing::info!(chat = %chat, device = %host_device, "host nudged");
                }
                Ok(res) => tracing::warn!(chat = %chat, device = %host_device,
                    status = res.status().as_u16(), "nudge rejected"),
                Err(err) => {
                    tracing::warn!(chat = %chat, error = %err, "nudge failed (best-effort)")
                }
            }
        });
    }

    /// The chat's host device when it is NOT this engine (mirrors
    /// `nudge_remote_host`'s ownership read).
    fn remote_host_for(&self, chat_id: &str) -> Option<String> {
        let registry = self.registry()?;
        let host_device = match registry.chat(chat_id) {
            Ok(Some(chat)) => chat.device_id,
            _ => return None,
        };
        (host_device != self.inner.config.device_id).then_some(host_device)
    }

    /// Durable-delivery escort for one queued command aimed at a REMOTE host:
    ///
    /// 1. push any queued attachment bytes over the peer link (retry until
    ///    they land — the relayed command must never outrun its bytes);
    /// 2. give the normal path (chat2 rows → edge → host's room) a short
    ///    grace to ack;
    /// 3. rows still not at the edge but the peer link alive → relay-forward
    ///    the entry itself ([`zeron_rpc::methods::RELAY_COMMAND`]). The
    ///    host's processed ledger claims the client-minted id, so the doc
    ///    row arriving later dedupes to a no-op — exactly-once by
    ///    construction (the 2026-08-18 03:45 incident shape: nudges flowed,
    ///    rows didn't; there was no second road for the command).
    ///
    /// Stops the moment any path lands. No-op for locally-hosted chats.
    fn spawn_command_delivery(
        &self,
        chat_id: &str,
        entry: SessionCommandEntry,
        transfers: Vec<crate::uploads::AttachmentTransfer>,
    ) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return; // bare sync callers (unit tests) skip rather than panic
        };
        let host = self.clone();
        let chat = chat_id.to_string();
        self.spawn_worker_on(&runtime, async move {
            let Some(target) = host.remote_host_for(&chat) else {
                return; // local host (or no row yet claimed remotely)
            };
            if !transfers.is_empty() && !host.deliver_attachments(&chat, &transfers).await {
                return; // gave up; the drain's wait cap surfaces the failure
            }
            let mut wake = zeron_sync::wake::subscribe();
            let mut online = zeron_sync::wake::subscribe_online();
            let give_up = tokio::time::Instant::now() + RELAY_GIVE_UP;
            let grace_end = tokio::time::Instant::now() + ROWS_GRACE;
            while tokio::time::Instant::now() < grace_end {
                if host.rows_flushed(&chat) {
                    return; // rows on the edge — the normal path has it
                }
                tokio::time::sleep(ROWS_POLL).await;
            }
            let mut backoff = RELAY_BACKOFF_BASE;
            loop {
                if host.rows_flushed(&chat) {
                    return; // the normal path won while we were retrying
                }
                match host.relay_command(&target, &chat, &entry).await {
                    Ok(outcome) => {
                        tracing::info!(chat = %chat, device = %target, command = %entry.id,
                            outcome, "command delivered via peer relay");
                        return;
                    }
                    Err(err) => {
                        tracing::warn!(chat = %chat, device = %target, error = %err,
                            backoff_ms = backoff.as_millis() as u64,
                            "peer-relay delivery retrying");
                    }
                }
                if tokio::time::Instant::now() >= give_up {
                    tracing::warn!(chat = %chat, command = %entry.id,
                        "peer-relay delivery gave up; command remains queued in the doc");
                    return;
                }
                while wake.try_recv().is_ok() {}
                while online.try_recv().is_ok() {}
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = wake.recv() => {}
                    _ = online.recv() => {}
                }
                backoff = (backoff * 2).min(RELAY_BACKOFF_CAP);
            }
        });
    }

    /// Step 1 of the escort: push staged bytes until they land (event-driven
    /// backoff), `true` on success. Re-resolves the host each attempt (a
    /// claim can move the chat).
    async fn deliver_attachments(
        &self,
        chat: &str,
        transfers: &[crate::uploads::AttachmentTransfer],
    ) -> bool {
        let mut wake = zeron_sync::wake::subscribe();
        let mut online = zeron_sync::wake::subscribe_online();
        let mut backoff = TRANSFER_BACKOFF_BASE;
        let deadline = tokio::time::Instant::now() + ATTACHMENT_WAIT_MAX;
        loop {
            let Some(target) = self.remote_host_for(chat) else {
                return true; // became locally hosted: bytes already here
            };
            match self.push_attachments(&target, transfers).await {
                Ok(()) => {
                    tracing::info!(chat = %chat, device = %target,
                        count = transfers.len(), "queued attachments delivered");
                    // The bytes beat the drain's next look — kick it via the
                    // durable nudge (the host's UploadCommit already kicked
                    // its local drains too).
                    self.nudge_remote_host(chat);
                    return true;
                }
                Err(TransferError::Permanent(err)) => {
                    tracing::warn!(chat = %chat, device = %target, error = %err,
                        "queued attachment transfer failed permanently");
                    return false;
                }
                Err(TransferError::Transient(err)) => {
                    tracing::warn!(chat = %chat, device = %target, error = %err,
                        backoff_ms = backoff.as_millis() as u64,
                        "queued attachment transfer retrying");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(chat = %chat, "queued attachment transfer gave up (wait cap)");
                return false;
            }
            while wake.try_recv().is_ok() {}
            while online.try_recv().is_ok() {}
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = wake.recv() => {}
                _ = online.recv() => {}
            }
            backoff = (backoff * 2).min(TRANSFER_BACKOFF_CAP);
        }
    }

    /// Every local chat2 batch acked while connected — our rows are ON the
    /// edge, and the host's own room connection will deliver them. (A room
    /// that hasn't joined keeps pre-join updates in a local buffer, so
    /// `connected` is load-bearing here, not just the empty queue.)
    fn rows_flushed(&self, chat_id: &str) -> bool {
        let handle = lock(&self.inner.handles).get(chat_id).cloned();
        handle
            .and_then(|h| lock(&h.chat2).as_ref().map(|c| c.stats()))
            .is_some_and(|s| s.connected && s.pending_pushes == 0)
    }

    /// One relay attempt: version-gate the host, then forward the entry over
    /// the peer link. Timeouts mark the link suspect (drop + redial next
    /// attempt); a host-side refusal is permanent for THIS attempt but the
    /// escort keeps retrying until the give-up cap (the refusal may be
    /// "attachments not landed yet").
    async fn relay_command(
        &self,
        target: &str,
        chat_id: &str,
        entry: &SessionCommandEntry,
    ) -> Result<&'static str, String> {
        let links = self
            .inner
            .links
            .get()
            .ok_or_else(|| "peer links not wired".to_string())?;
        let client = links
            .client(target)
            .await
            .map_err(|e| format!("peer link: {e}"))?;
        let params = serde_json::json!({ "chatId": chat_id, "entry": entry });
        let call = client.call(zeron_rpc::methods::RELAY_COMMAND, params);
        match tokio::time::timeout(RELAY_CALL_TIMEOUT, call).await {
            Err(_) => {
                links.invalidate(target);
                Err("relay call timed out; peer link suspect".into())
            }
            Ok(Err(zeron_rpc::RpcError::Failed(err))) => Err(format!("host refused: {err}")),
            Ok(Err(err)) => {
                links.invalidate(target);
                Err(format!("relay call failed: {err}"))
            }
            Ok(Ok(reply)) => Ok(match reply.get("outcome").and_then(|v| v.as_str()) {
                Some("executed") => "executed",
                Some("duplicate") => "duplicate",
                Some("expired") => "expired",
                Some("superseded") => "superseded",
                _ => "accepted",
            }),
        }
    }

    /// User-driven retry (the failed-send affordance): re-kick every
    /// delivery road for a chat whose queued sends haven't been adopted —
    /// fresh chat2 socket (a zombie room is the usual suspect), host nudge,
    /// a local drain pass, and a fresh delivery escort per still-pending
    /// command with its attachment transfers re-derived from the entries'
    /// `pending://` refs (idempotent: re-pushing landed bytes re-commits the
    /// same file; the processed ledger keeps execution exactly-once).
    pub fn retry_delivery(&self, chat_id: &str) -> Result<(), EngineError> {
        let handle = self.open(chat_id)?;
        if let Some(chat2) = lock(&handle.chat2).as_ref() {
            chat2.redial();
        }
        self.nudge_remote_host(chat_id);
        let commands = handle.doc.read_commands()?;
        let pending: Vec<SessionCommandEntry> = commands
            .iter()
            .filter(|c| {
                c.status == SessionCommandStatus::Pending
                    && !self.inner.store.is_processed(&c.id).unwrap_or(false)
            })
            .cloned()
            .collect();
        for entry in pending {
            let transfers = command_transfers(&entry);
            self.spawn_command_delivery(chat_id, entry, transfers);
        }
        // Dead attempts: a Run/Steer whose user message never landed and
        // whose command can never execute again — Rejected (execute failed,
        // or the dead-command sweep terminalized it), or consumed by the
        // ledger with no outcome and not currently executing (crash between
        // mark and resolve). Exactly-once is per command ID, so a
        // user-driven retry mints a FRESH attempt: new id, same payload and
        // message id (the executor's user-entry pre-write dedupes by message
        // id). One re-issue per message — the LATEST attempt speaks for it.
        let messages = handle.doc.read_entries().unwrap_or_default();
        let message_landed = |mid: &str| messages.iter().any(|m| m.id == mid);
        let mut latest_dead: HashMap<String, &SessionCommandEntry> = HashMap::new();
        for c in &commands {
            let Some(mid) = (match &c.payload {
                SessionCommandPayload::Run { message_id, .. } => Some(message_id.as_str()),
                SessionCommandPayload::Steer { message_id, .. } => message_id.as_deref(),
                _ => None,
            }) else {
                continue;
            };
            if message_landed(mid) {
                continue;
            }
            let dead = match c.status {
                // Rejected: execute failed or the sweep terminalized a crash
                // window. Expired: the entry outlived its TTL undelivered —
                // an explicit user retry is exactly the consent to re-send.
                SessionCommandStatus::Rejected | SessionCommandStatus::Expired => true,
                SessionCommandStatus::Pending => {
                    self.inner.store.is_processed(&c.id).unwrap_or(false)
                        && !lock(&self.inner.executing).contains(&c.id)
                }
                _ => false,
            };
            if !dead {
                continue;
            }
            // A LIVE pending attempt for the same message (queued or being
            // escorted above) makes a re-issue a duplicate — skip.
            let live_attempt = commands.iter().any(|o| {
                o.id != c.id
                    && o.status == SessionCommandStatus::Pending
                    && !self.inner.store.is_processed(&o.id).unwrap_or(false)
                    && match (&o.payload, &c.payload) {
                        (
                            SessionCommandPayload::Run { message_id: a, .. },
                            SessionCommandPayload::Run { message_id: b, .. },
                        ) => a == b,
                        (
                            SessionCommandPayload::Steer { message_id: a, .. },
                            SessionCommandPayload::Steer { message_id: b, .. },
                        ) => a == b,
                        _ => false,
                    }
            });
            if live_attempt {
                continue;
            }
            match latest_dead.entry(mid.to_string()) {
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if c.issued_at > slot.get().issued_at {
                        slot.insert(c);
                    }
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(c);
                }
            }
        }
        for old in latest_dead.values() {
            if old.status == SessionCommandStatus::Pending {
                // Terminalize the consumed-but-dead original so the doc tells
                // the truth and the next retry pass doesn't see it again.
                self.resolve_command(
                    &handle,
                    &old.id,
                    SessionCommandStatus::Rejected,
                    Some("interrupted before completion — superseded by retry"),
                );
            }
            let now = now_ms();
            let reissue = SessionCommandEntry {
                id: new_id(),
                payload: old.payload.clone(),
                issued_by: self.inner.config.device_id.clone(),
                issued_at: now,
                // Carry the original attribution: a retry is the SAME send,
                // so an agent-to-agent message must not re-enter the doc as
                // an unattributed human one (and its hop provenance has to
                // survive, or the ping-pong breaker loses its count).
                user_id: old.user_id.clone(),
                origin: old.origin.clone(),
                based_on: messages.last().map(|m| CommandBasedOn {
                    turn_id: Some(m.id.clone()),
                    frontier: None,
                }),
                expires_at: Some(now + COMMAND_DEFAULT_TTL_MS),
                status: SessionCommandStatus::Pending,
                resolution: None,
            };
            tracing::info!(chat = %chat_id, old = %old.id, new = %reissue.id,
                "retry re-issues a dead send attempt");
            handle.doc.queue_command(&reissue)?;
            let transfers = command_transfers(&reissue);
            self.spawn_command_delivery(chat_id, reissue, transfers);
        }
        // Locally-hosted (or already-synced) commands: a drain pass is the
        // whole retry.
        if tokio::runtime::Handle::try_current().is_ok() {
            let host = self.clone();
            let handle = handle.clone();
            self.spawn_worker(async move { host.drain_commands(&handle).await });
        }
        Ok(())
    }

    /// Host side of [`zeron_rpc::methods::RELAY_COMMAND`]: evaluate the
    /// forwarded entry against OUR doc (dedupe/TTL/supersede rules apply
    /// unchanged), claim its client-minted id in the processed ledger, then
    /// execute. The claim is what makes the doc row arriving later — over
    /// chat2 sync — a no-op in the drain: exactly-once across both roads.
    pub async fn ingest_relayed_command(
        &self,
        chat_id: &str,
        entry: SessionCommandEntry,
    ) -> Result<&'static str, EngineError> {
        let handle = self.open(chat_id)?;
        // The sender sequences attachment transfers BEFORE the relay; refuse
        // (retryably) rather than run without the images.
        if !self.missing_attachments(&entry).is_empty() {
            return Err(EngineError::Other("attachments not landed yet".into()));
        }
        let sessions = self
            .sessions()
            .ok_or_else(|| EngineError::Other("executor unavailable".into()))?;
        let commands = handle.doc.read_commands()?;
        let messages = handle.doc.read_entries().unwrap_or_default();
        let current_turn_id = messages.last().map(|m| m.id.clone());
        let turn_is_past = |turn_id: &str| messages.iter().any(|m| m.id == turn_id);
        let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
        let disposition = evaluate_command(
            &entry,
            &EvaluationContext {
                is_processed: &is_processed,
                now_ms: now_ms(),
                entries: &commands,
                current_turn_id: current_turn_id.as_deref(),
                turn_is_past: &turn_is_past,
            },
        );
        if matches!(disposition, CommandDisposition::Skip) {
            return Ok("duplicate");
        }
        // In-flight claim first (the drain's dead-command sweep must see this
        // id as alive, not crashed, while the execute below runs).
        if !lock(&self.inner.executing).insert(entry.id.clone()) {
            return Ok("duplicate");
        }
        // Claim BEFORE executing (the drain's own mark-before-execute rule).
        let marked = self.inner.store.mark_processed(&entry.id);
        let result = match marked {
            Err(err) => Err(err.into()),
            Ok(false) => Ok("duplicate"),
            Ok(true) => match disposition {
                CommandDisposition::Skip => Ok("duplicate"),
                CommandDisposition::Expired => Ok("expired"),
                CommandDisposition::Superseded => Ok("superseded"),
                CommandDisposition::Execute => match self.execute(&sessions, &handle, &entry).await
                {
                    Ok(_) => Ok("executed"),
                    Err(err) => Err(err),
                },
            },
        };
        lock(&self.inner.executing).remove(&entry.id);
        result
    }

    /// One transfer attempt: chunked `UploadChunk` + `UploadCommit` straight
    /// over the peer link (same wire the UI's legacy path used, so old and
    /// new engines interoperate). Timeouts mark the link suspect —
    /// `invalidate` drops the cached socket so the retry dials fresh instead
    /// of feeding a zombie pipe forever (2026-08-19 incident).
    async fn push_attachments(
        &self,
        target: &str,
        transfers: &[crate::uploads::AttachmentTransfer],
    ) -> Result<(), TransferError> {
        use TransferError::{Permanent, Transient};
        let Some(links) = self.inner.links.get() else {
            return Err(Permanent("peer links not wired".into()));
        };
        let Some(uploads) = self.inner.uploads.get() else {
            return Err(Permanent("uploads not wired".into()));
        };
        let client = links
            .client(target)
            .await
            .map_err(|e| Transient(format!("peer link: {e}")))?;
        for transfer in transfers {
            // Bytes come from the uploads jail only — a transfer names an
            // upload identity, never an arbitrary path.
            let source = uploads.pending_target(&transfer.upload_id, &transfer.file_name);
            let bytes = tokio::fs::read(&source)
                .await
                .map_err(|e| Permanent(format!("staged attachment missing: {e}")))?;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let mut start = 0usize;
            let mut seq = 0u64;
            loop {
                let end = (start + TRANSFER_CHUNK_B64).min(b64.len());
                let params = serde_json::json!({
                    "uploadId": transfer.upload_id, "seq": seq, "data": &b64[start..end],
                });
                let call = client.call(zeron_rpc::methods::UPLOAD_CHUNK, params);
                match tokio::time::timeout(TRANSFER_CHUNK_TIMEOUT, call).await {
                    Err(_) => {
                        links.invalidate(target);
                        return Err(Transient("chunk push timed out; peer link suspect".into()));
                    }
                    Ok(Err(zeron_rpc::RpcError::Failed(err))) => {
                        return Err(Permanent(format!("host refused chunk: {err}")));
                    }
                    Ok(Err(err)) => {
                        links.invalidate(target);
                        return Err(Transient(format!("chunk push failed: {err}")));
                    }
                    Ok(Ok(_)) => {}
                }
                start = end;
                seq += 1;
                if start >= b64.len() {
                    break;
                }
            }
            let params = serde_json::json!({
                "uploadId": transfer.upload_id, "fileName": transfer.file_name,
            });
            let call = client.call(zeron_rpc::methods::UPLOAD_COMMIT, params);
            match tokio::time::timeout(TRANSFER_COMMIT_TIMEOUT, call).await {
                Err(_) => {
                    links.invalidate(target);
                    return Err(Transient("commit timed out; peer link suspect".into()));
                }
                Ok(Err(zeron_rpc::RpcError::Failed(err))) => {
                    return Err(Permanent(format!("host refused commit: {err}")));
                }
                Ok(Err(err)) => {
                    links.invalidate(target);
                    return Err(Transient(format!("commit failed: {err}")));
                }
                Ok(Ok(_)) => {}
            }
        }
        Ok(())
    }

    /// Upload a tool result's full output/diff to the R2 sidecar
    /// (`PUT {edge}/blob/{chatId}/{partId}[.diff]`, docs/chat2-sync.md A2).
    /// Fire-and-forget: the doc already carries the summary, so a lost upload
    /// degrades to "full output unavailable" — it must never block or fail
    /// the run. Offline/edge-less engines skip silently.
    pub fn upload_tool_sidecar(&self, chat_id: &str, payload: zeron_doc::SidecarPayload) {
        let Some(edge) = self.inner.config.edge.clone() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return; // bare sync callers (unit tests) skip rather than panic
        };
        let http = self.inner.http.clone();
        let base = format!(
            "{}/blob/{}/{}",
            edge.url.trim_end_matches('/'),
            chat_id,
            encode_part_segment(&payload.part_id)
        );
        self.spawn_worker_on(&runtime, async move {
            let Some(bearer) = edge.bearer().await else {
                return; // signed out; summary-only until the next session
            };
            let mut puts: Vec<(String, &'static str, Vec<u8>)> = Vec::new();
            if let Some(output) = &payload.output {
                puts.push((
                    base.clone(),
                    "text/plain; charset=utf-8",
                    output.clone().into_bytes(),
                ));
            }
            if let Some(diff) = &payload.diff
                && let Ok(json) = serde_json::to_vec(diff)
            {
                puts.push((format!("{base}.diff"), "application/json", json));
            }
            for (url, content_type, body) in puts {
                let sent = http
                    .put(&url)
                    .bearer_auth(&bearer)
                    .header("content-type", content_type)
                    .body(body)
                    .send()
                    .await;
                match sent {
                    Ok(res) if res.status().is_success() => {}
                    Ok(res) => tracing::warn!(url, status = res.status().as_u16(),
                        "tool sidecar upload rejected"),
                    Err(err) => {
                        tracing::warn!(url, error = %err, "tool sidecar upload failed (best-effort)")
                    }
                }
            }
        });
    }

    /// Fetch a sidecar blob by its doc-resident ref (`{chatId}/{partId}` or
    /// `…​.diff`) — the UI's lazy "Show full output" path, served over RPC
    /// because the UI crate has no HTTP client or edge bearer.
    pub async fn fetch_tool_blob(&self, blob_ref: &str) -> Result<String, EngineError> {
        // Same shape `apply_sidecar_refs` writes; anything else is a forged ref.
        let valid = blob_ref.split_once('/').is_some_and(|(chat, part)| {
            !chat.is_empty()
                && chat.len() <= 128
                && chat
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                && !part.is_empty()
                && part.len() <= 200
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:#~-".contains(&b))
        });
        if !valid {
            return Err(EngineError::Other(format!("bad blob ref: {blob_ref}")));
        }
        let Some(edge) = self.inner.config.edge.clone() else {
            return Err(EngineError::Other("offline: no edge configured".into()));
        };
        let Some(bearer) = edge.bearer().await else {
            return Err(EngineError::Other("signed out".into()));
        };
        // `valid` above guarantees the split; re-split to encode the part
        // segment for transport (PART_RE allows `#`, which a raw URL would
        // truncate as a fragment — the 2026-08-10 silent-collision bug).
        let (chat, part) = blob_ref.split_once('/').expect("validated above");
        let url = format!(
            "{}/blob/{}/{}",
            edge.url.trim_end_matches('/'),
            chat,
            encode_part_segment(part)
        );
        let res = self
            .inner
            .http
            .get(&url)
            .bearer_auth(&bearer)
            .send()
            .await
            .map_err(|e| EngineError::Other(format!("sidecar fetch failed: {e}")))?;
        if !res.status().is_success() {
            return Err(EngineError::Other(format!(
                "sidecar fetch: HTTP {}",
                res.status().as_u16()
            )));
        }
        res.text()
            .await
            .map_err(|e| EngineError::Other(format!("sidecar body read failed: {e}")))
    }

    /// §2.2 writer discipline: we host a chat iff its registry row's `deviceId` is
    /// ours; a chat with no row is claimable (claim-on-first-command). Without a
    /// wired registry host (bare-DocHost tests) every open chat is ours — M2's
    /// behavior, now the degenerate case.
    fn is_host(&self, chat_id: &str) -> bool {
        self.registry().is_none_or(|ws| ws.is_host(chat_id))
    }

    /// Chat-config harness when the registry row carries one, else the default.
    pub(crate) fn harness_for(&self, chat_id: &str) -> HarnessId {
        self.registry()
            .and_then(|ws| ws.chat_config(chat_id))
            .map(|config| config.harness)
            .unwrap_or(self.inner.config.default_harness)
    }

    /// The harness a request dispatches on: the request's own pick when it
    /// carries one (rides the command plane, immune to registry-row races),
    /// else [`Self::harness_for`].
    pub(crate) fn harness_for_request(
        &self,
        chat_id: &str,
        request: &zeron_proto::RunRequest,
    ) -> HarnessId {
        request.harness.unwrap_or_else(|| self.harness_for(chat_id))
    }

    /// Drain pending commands (host-only): evaluate → mark processed BEFORE execute →
    /// execute → write the outcome as the sole outcome writer.
    pub async fn drain_commands(&self, handle: &Arc<ChatDocHandle>) {
        let _drain = handle.command_drain.lock().await;
        // A queued worker can lose the race to a room-generation cutover
        // before it obtains the drain lock. Never execute or resolve commands
        // against a detached source lineage.
        let current = lock(&self.inner.handles)
            .get(&handle.chat_id)
            .is_some_and(|current| Arc::ptr_eq(current, handle));
        if !current
            || handle.generation_frozen.load(Ordering::Acquire)
            || handle.retired.load(Ordering::Acquire)
        {
            return;
        }
        let Some(sessions) = self.sessions() else {
            return; // executor not wired yet (or retired); the set_sessions kick re-drains
        };
        if !self.is_host(&handle.chat_id) {
            return;
        }
        // Entries this pass decided to leave alone (processed dedupe hits).
        let mut skipped: HashSet<String> = HashSet::new();
        loop {
            let commands = match handle.doc.read_commands() {
                Ok(commands) => commands,
                Err(err) => {
                    tracing::warn!(chat = %handle.chat_id, error = %err, "command read failed");
                    return;
                }
            };
            let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
            // Dead-command sweep: Pending in the doc, consumed by the ledger,
            // and NOT mid-execution in this process — the crash window
            // between mark-processed and the outcome write. Left alone it is
            // a send no drain or retry can ever reach ("Sending…" forever,
            // 2026-08-19); terminalize it so the truth lands in the doc and
            // a user retry can mint a fresh attempt.
            for c in &commands {
                if c.status == SessionCommandStatus::Pending
                    && !skipped.contains(&c.id)
                    && is_processed(&c.id)
                    && !lock(&self.inner.executing).contains(&c.id)
                {
                    tracing::warn!(chat = %handle.chat_id, command = %c.id,
                        "command consumed but never resolved (crash mid-execute?); rejecting");
                    self.resolve_command(
                        handle,
                        &c.id,
                        SessionCommandStatus::Rejected,
                        Some("interrupted before completion — retry to send again"),
                    );
                    skipped.insert(c.id.clone());
                }
            }
            let Some(entry) = commands
                .iter()
                .find(|c| {
                    c.status == SessionCommandStatus::Pending
                        && !skipped.contains(&c.id)
                        && !is_processed(&c.id)
                })
                .cloned()
            else {
                return;
            };
            let messages = handle.doc.read_entries().unwrap_or_default();
            let current_turn_id = messages.last().map(|m| m.id.clone());
            let turn_is_past = |turn_id: &str| messages.iter().any(|m| m.id == turn_id);
            let disposition = evaluate_command(
                &entry,
                &EvaluationContext {
                    is_processed: &is_processed,
                    now_ms: now_ms(),
                    entries: &commands,
                    current_turn_id: current_turn_id.as_deref(),
                    turn_is_past: &turn_is_past,
                },
            );
            // Queued-attachment gate (BEFORE the processed mark — a deferred
            // command must stay eligible): a Run/Steer naming `pending://`
            // refs whose bytes haven't landed on this device yet waits for
            // the transfer instead of running without its images. The wait is
            // bounded; past it the command fails loudly.
            if matches!(disposition, CommandDisposition::Execute) {
                let missing = self.missing_attachments(&entry);
                if !missing.is_empty() {
                    if now_ms().saturating_sub(entry.issued_at) < ATTACHMENT_WAIT_MAX_MS {
                        tracing::info!(chat = %handle.chat_id, command = %entry.id,
                            missing = missing.len(), "command deferred: attachment bytes in transit");
                        self.arm_attachment_wait(handle);
                        return; // preserve order; UploadCommit / the wait timer re-kick
                    }
                    if let Err(err) = self.inner.store.mark_processed(&entry.id) {
                        tracing::error!(chat = %handle.chat_id, error = %err,
                            "processed-ledger write failed; halting drain");
                        return;
                    }
                    tracing::warn!(chat = %handle.chat_id, command = %entry.id,
                        "command rejected: attachments never arrived");
                    self.resolve_command(
                        handle,
                        &entry.id,
                        SessionCommandStatus::Rejected,
                        Some("attachments never arrived"),
                    );
                    continue;
                }
            }
            // In-flight claim: guards the dead-command sweep (an id in
            // `executing` is alive, not crashed) and serializes racing
            // drains on the same entry.
            if !lock(&self.inner.executing).insert(entry.id.clone()) {
                skipped.insert(entry.id.clone());
                continue;
            }
            // Mark BEFORE executing: a crash mid-execution must never double-run a
            // command whose side effect may already have happened.
            if let Err(err) = self.inner.store.mark_processed(&entry.id) {
                tracing::error!(chat = %handle.chat_id, error = %err, "processed-ledger write failed; halting drain");
                lock(&self.inner.executing).remove(&entry.id);
                return;
            }
            match disposition {
                CommandDisposition::Skip => {
                    skipped.insert(entry.id.clone());
                }
                CommandDisposition::Expired => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Expired, None);
                }
                CommandDisposition::Superseded => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Superseded, None);
                }
                CommandDisposition::Execute => {
                    let (status, resolution) = match self.execute(&sessions, handle, &entry).await {
                        Ok(outcome) => outcome,
                        Err(err) => (SessionCommandStatus::Rejected, Some(err.to_string())),
                    };
                    self.resolve_command(handle, &entry.id, status, resolution.as_deref());
                }
            }
            lock(&self.inner.executing).remove(&entry.id);
        }
    }

    /// The command's `pending://` attachment refs whose bytes are NOT on this
    /// device's disk yet. Empty when the command names none, when everything
    /// has landed, or when no uploads store is wired (tests) — absence of the
    /// subsystem must never wedge a queue.
    fn missing_attachments(&self, entry: &SessionCommandEntry) -> Vec<String> {
        let refs: Vec<String> = match &entry.payload {
            SessionCommandPayload::Run { request, .. } => request
                .attachments
                .iter()
                .filter(|p| crate::uploads::is_pending_ref(p))
                .cloned()
                .collect(),
            SessionCommandPayload::Steer { prompt, .. } => crate::uploads::pending_refs_in(prompt),
            _ => Vec::new(),
        };
        if refs.is_empty() {
            return refs;
        }
        let Some(uploads) = self.inner.uploads.get() else {
            return Vec::new();
        };
        refs.into_iter()
            .filter(|r| uploads.resolve_pending(r).is_none())
            .collect()
    }

    /// Arm (once per chat) the deferred-command re-check loop: while a
    /// pending unprocessed command still waits on attachment bytes, re-drain
    /// on a cadence so the bounded wait actually expires even if every
    /// event-driven kick was missed.
    fn arm_attachment_wait(&self, handle: &Arc<ChatDocHandle>) {
        let chat = handle.chat_id.clone();
        if !lock(&self.inner.drain_waiting).insert(chat.clone()) {
            return;
        }
        let weak = Arc::downgrade(handle);
        let host = self.clone();
        self.spawn_worker(async move {
            loop {
                tokio::time::sleep(ATTACHMENT_WAIT_RECHECK).await;
                let Some(handle) = weak.upgrade() else { break };
                if !host.awaiting_attachments(&handle) {
                    break;
                }
                host.drain_commands(&handle).await;
                let Some(handle) = weak.upgrade() else { break };
                if !host.awaiting_attachments(&handle) {
                    break;
                }
            }
            lock(&host.inner.drain_waiting).remove(&chat);
        });
    }

    /// True while some pending, unprocessed command still waits on bytes.
    fn awaiting_attachments(&self, handle: &Arc<ChatDocHandle>) -> bool {
        let commands = handle.doc.read_commands().unwrap_or_default();
        commands.iter().any(|c| {
            c.status == SessionCommandStatus::Pending
                && !self.inner.store.is_processed(&c.id).unwrap_or(false)
                && !self.missing_attachments(c).is_empty()
        })
    }

    /// Rewrite a request's landed `pending://` refs to this device's absolute
    /// paths — in the attachments list AND the prompt text — so the harness
    /// (and the persisted user entry) see ordinary local files, exactly like
    /// the legacy pre-upload flow produced.
    fn resolve_request_attachments(&self, request: &mut zeron_proto::RunRequest) {
        let Some(uploads) = self.inner.uploads.get() else {
            return;
        };
        for path in request.attachments.iter_mut() {
            if let Some(abs) = uploads.resolve_pending(path) {
                request.prompt = request.prompt.replace(path.as_str(), &abs);
                *path = abs;
            }
        }
    }

    /// [`Self::resolve_request_attachments`] for a bare prompt (Steer).
    fn resolve_prompt_attachments(&self, prompt: &str) -> String {
        let Some(uploads) = self.inner.uploads.get() else {
            return prompt.to_string();
        };
        let mut out = prompt.to_string();
        for r in crate::uploads::pending_refs_in(prompt) {
            if let Some(abs) = uploads.resolve_pending(&r) {
                out = out.replace(&r, &abs);
            }
        }
        out
    }

    /// Host-only outcome write (ledger rule 2).
    fn resolve_command(
        &self,
        handle: &ChatDocHandle,
        command_id: &str,
        status: SessionCommandStatus,
        resolution: Option<&str>,
    ) {
        let _lifecycle = lock(&handle.sink_lifecycle);
        if let Err(err) = handle.ensure_mutable() {
            tracing::warn!(
                chat = %handle.chat_id,
                command = %command_id,
                error = %err,
                "command outcome skipped on retired room generation"
            );
            return;
        }
        if let Err(err) = handle
            .doc
            .set_command_status(command_id, status, resolution)
        {
            tracing::warn!(
                chat = %handle.chat_id,
                command = %command_id,
                error = %err,
                "command outcome write failed"
            );
        }
    }

    async fn execute(
        &self,
        sessions: &SessionsEngine,
        handle: &Arc<ChatDocHandle>,
        entry: &SessionCommandEntry,
    ) -> Result<(SessionCommandStatus, Option<String>), EngineError> {
        let chat_id = &handle.chat_id;
        // Attribution: pre-write the user entry with the command's issuing
        // user (write_user_message is idempotent by id, so the dispatch/steer
        // path's own write becomes a no-op). Keeps the sessions dispatch
        // plumbing author-free.
        if entry.user_id.is_some() {
            let prewrite = match &entry.payload {
                SessionCommandPayload::Run {
                    request,
                    message_id,
                } => Some((message_id.as_str(), request.prompt.as_str())),
                SessionCommandPayload::Steer {
                    prompt,
                    message_id: Some(message_id),
                } => Some((message_id.as_str(), prompt.as_str())),
                _ => None,
            };
            if let Some((message_id, prompt)) = prewrite {
                // This write is by-id idempotent and lands FIRST, so it must
                // already match what the execute arms below would persist —
                // whatever it writes, theirs dedupes to a no-op. That means
                // resolving `pending://` refs here too (the drain gated on the
                // bytes, so they resolve) and using the send-time timestamp,
                // or a queued send would keep the sender's unresolved refs and
                // be stamped at drain time instead of send time.
                handle.write_user_message(
                    message_id,
                    &self.resolve_prompt_attachments(prompt),
                    entry.issued_at.min(now_ms()),
                    entry.user_id.as_deref(),
                )?;
            }
        }
        // The live turn's agent-send depth: what send_to_session increments.
        // A new turn also resets the chat's per-turn send budget.
        if matches!(
            entry.payload,
            SessionCommandPayload::Run { .. } | SessionCommandPayload::Steer { .. }
        ) {
            lock(&self.inner.turn_origins).insert(
                chat_id.clone(),
                entry.origin.as_ref().map(|o| o.hops).unwrap_or(0),
            );
            lock(&self.inner.send_budgets).remove(chat_id);
        }
        match &entry.payload {
            SessionCommandPayload::Run {
                request,
                message_id,
            } => {
                let mut request = request.clone();
                // Queued-attachment refs (`pending://`) resolve to this
                // host's absolute paths before anything persists or
                // dispatches — the drain already gated on the bytes being
                // present, so every ref resolves here.
                self.resolve_request_attachments(&mut request);
                // Worktree directive (WorktreeSpec): materialize on THIS host at
                // drain time — the durable command plane replaces the sender's
                // old blocking CreateWorktree relay RPC, whose lost reply wedged
                // the composer on "Sending…" while the run proceeded anyway.
                // `take()` resolves the request before dispatch, so the journal
                // and steer→new-turn fallbacks reuse the created path instead of
                // minting another checkout.
                let fresh_worktree = match request.worktree.take() {
                    Some(spec) => {
                        let (cwd, fresh) = self.materialize_worktree(chat_id, &spec).await?;
                        request.cwd = cwd;
                        fresh
                    }
                    None => None,
                };
                // Claim-on-first-command: a run for a chat with no registry row
                // creates the row under our device id (we are about to host it).
                if let Some(ws) = self.registry() {
                    ws.claim_chat(chat_id, Some(&request.cwd))?;
                    // A pre-existing row (the client's createChat raced ahead)
                    // still carries the repo folder — repoint it at the fresh
                    // worktree, and stamp the actual `zeron/<name>` branch so
                    // the footer and the title-rename flow see it.
                    if let Some(wt) = &fresh_worktree {
                        if let Err(err) = ws.set_chat_cwd(chat_id, &wt.path) {
                            tracing::warn!(chat = %chat_id, error = %err, "worktree cwd stamp failed");
                        }
                        if let Err(err) = ws.set_chat_branch(chat_id, &wt.branch) {
                            tracing::warn!(chat = %chat_id, error = %err, "worktree branch stamp failed");
                        }
                    }
                }
                let harness = self.harness_for_request(chat_id, &request);
                // A row with no config renders no harness glyph (and every
                // later dispatch falls back to the engine default), so stamp
                // what this run actually executes with. Claimed rows and
                // catalog-not-loaded createChats both land here; the racing
                // real createChat carries the same picked values.
                if let Some(ws) = self.registry()
                    && ws.chat_config(chat_id).is_none()
                {
                    let config = zeron_proto::ChatConfig {
                        harness,
                        model: request.model.clone(),
                        reasoning: request.reasoning,
                        model_options: request.model_options.clone(),
                        sandbox: request.sandbox,
                    };
                    if let Err(err) = ws.set_chat_config(chat_id, &config) {
                        tracing::warn!(chat = %chat_id, error = %err, "run-config backfill failed");
                    }
                }
                // Timestamp canonicalization: the user message lands in
                // history at the moment the user SENT it (the entry's
                // issued_at, clamped against clock skew) — not whenever this
                // host got around to draining a queued command. Idempotent by
                // id, so the dispatch path's own execution-time write dedupes
                // to a no-op.
                if let Err(err) = handle.write_user_message(
                    message_id,
                    &request.prompt,
                    entry.issued_at.min(now_ms()),
                    entry.user_id.as_deref(),
                ) {
                    tracing::warn!(chat = %chat_id, error = %err, "canonical user-message write failed");
                }
                sessions
                    .dispatch(chat_id, harness, request, Some(message_id.clone()))
                    .await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::Steer { prompt, message_id } => {
                // Same `pending://` → absolute rewrite as the Run arm.
                let prompt = &self.resolve_prompt_attachments(prompt);
                // Same send-time canonicalization as the Run arm.
                if let Some(message_id) = message_id
                    && let Err(err) = handle.write_user_message(
                        message_id,
                        prompt,
                        entry.issued_at.min(now_ms()),
                        entry.user_id.as_deref(),
                    )
                {
                    tracing::warn!(chat = %chat_id, error = %err, "canonical user-message write failed");
                }
                match sessions.steer(chat_id, prompt, message_id.clone()).await? {
                    SteerOutcome::Accepted => Ok((SessionCommandStatus::Applied, None)),
                    SteerOutcome::NotSteerable => {
                        // No live steerable run: the durable command still delivers —
                        // run it as the next turn (zeron's fallback, executor-side).
                        // After an engine restart `last_request` is empty too, so
                        // rebuild the run config from the chat's registry row
                        // (zeron derived dispatch config from the chat row the
                        // same way — sessions.ts:601-620); dispatch's engine-owned
                        // resume then reattaches the prior harness conversation.
                        let request = sessions
                            .last_request(chat_id)
                            .or_else(|| self.request_from_chat_row(chat_id, prompt));
                        let Some(mut request) = request else {
                            return Ok((
                                SessionCommandStatus::Rejected,
                                Some("no live run and no prior run config".into()),
                            ));
                        };
                        request.prompt = prompt.clone();
                        request.resume = None; // dispatch re-derives the harness session
                        // A reused config must not re-inline the PREVIOUS
                        // turn's images; this steer's own refs (if any) already
                        // ride the prompt text.
                        request.attachments = Vec::new();
                        let harness = self.harness_for_request(chat_id, &request);
                        sessions
                            .dispatch(chat_id, harness, request, message_id.clone())
                            .await?;
                        Ok((
                            SessionCommandStatus::Applied,
                            Some("queued as new turn".into()),
                        ))
                    }
                }
            }
            SessionCommandPayload::Interrupt {} => {
                sessions.interrupt(chat_id).await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::RespondInput {
                request_id,
                answers,
            } => {
                if sessions.respond_input(chat_id, request_id, answers.clone())? {
                    return Ok((SessionCommandStatus::Applied, None));
                }
                // No live resolver. Only a request id the doc shows as an
                // OPEN question on a SETTLED entry gets the orphan fallback:
                // a mismatched or already-resolved id is a stale/buggy answer
                // and must still reject, and a still-streaming entry's
                // question belongs to the live run (a just-consumed resolver
                // racing a second answer must not spawn a duplicate turn).
                let questions = handle.doc.read_entries().ok().and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .filter(|e| e.status != Some(MessageStatus::Streaming))
                        .find_map(|e| {
                            e.parts.iter().find_map(|p| match p {
                                MessagePart::Input {
                                    request_id: rid,
                                    questions,
                                    resolved: false,
                                    ..
                                } if rid == request_id => Some(questions.clone()),
                                _ => None,
                            })
                        })
                });
                let Some(questions) = questions else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request".into()),
                    ));
                };
                // The run died under the question (engine restart, crash).
                // The question is still open in the doc and the command is
                // durable, so honor it anyway — stamp the part resolved and
                // deliver the answers as the next (resumed) turn, the same
                // fallback a dead-run steer takes. The question UI stays up
                // until the user answers (user requirement); this is what
                // makes that answer still WORK.
                let request = sessions
                    .last_request(chat_id)
                    .or_else(|| self.request_from_chat_row(chat_id, ""));
                let Some(mut request) = request else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request and no prior run config".into()),
                    ));
                };
                request.prompt = respond_input_prompt(&questions, answers);
                request.resume = None; // dispatch re-derives the harness session
                request.attachments = Vec::new();
                if let Err(err) = handle.doc.resolve_input(request_id) {
                    tracing::warn!(chat = %chat_id, request = %request_id, error = %err,
                        "orphaned input resolve failed");
                }
                let harness = self.harness_for_request(chat_id, &request);
                sessions.dispatch(chat_id, harness, request, None).await?;
                Ok((
                    SessionCommandStatus::Applied,
                    Some("answered as new turn".into()),
                ))
            }
        }
    }

    /// Create (or reuse) the isolated worktree a Run's [`zeron_proto::WorktreeSpec`]
    /// asks for, returning the resolved cwd plus the fresh worktree when one was
    /// actually created. Reuse guard: a chat whose row already points inside a
    /// linked worktree of the same repo keeps it — a duplicate Run (client retry
    /// after a lost ack, ledger reset) must not mint a second checkout.
    async fn materialize_worktree(
        &self,
        chat_id: &str,
        spec: &zeron_proto::WorktreeSpec,
    ) -> Result<(String, Option<zeron_proto::Worktree>), EngineError> {
        if let Some(ws) = self.registry()
            && let Ok(Some(chat)) = ws.chat(chat_id)
            && let Some(cwd) = chat.cwd
            && cwd != spec.repo_path
            && crate::registry_host::linked_worktree_root(std::path::Path::new(&cwd)).as_deref()
                == Some(spec.repo_path.as_str())
        {
            tracing::info!(chat = %chat_id, cwd = %cwd, "worktree spec: reusing the chat's existing worktree");
            return Ok((cwd, None));
        }
        let repos = self
            .inner
            .repos
            .get()
            .ok_or_else(|| EngineError::Other("repos engine not wired".into()))?;
        let worktree = repos
            .create_worktree(std::path::Path::new(&spec.repo_path), &spec.base)
            .await?;
        tracing::info!(
            chat = %chat_id,
            path = %worktree.path,
            branch = %worktree.branch,
            "worktree materialized for run"
        );
        Ok((worktree.path.clone(), Some(worktree)))
    }

    /// A steer-turned-run with no in-process `last_request` (engine restarted
    /// since the last turn): rebuild the run config from the chat's registry
    /// row — cwd from the row, model/reasoning/options/sandbox from its config
    /// (composer defaults otherwise). `None` without a registry host or row.
    // (Also the RespondInput dead-run fallback's config source.)
    pub(crate) fn request_from_chat_row(
        &self,
        chat_id: &str,
        prompt: &str,
    ) -> Option<zeron_proto::RunRequest> {
        let registry = self.registry()?;
        let chat = match registry.chat(chat_id) {
            Ok(chat) => chat?,
            Err(err) => {
                tracing::warn!(chat = %chat_id, error = %err, "registry chat read failed");
                return None;
            }
        };
        let config = chat.config;
        Some(zeron_proto::RunRequest {
            prompt: prompt.to_string(),
            harness: config.as_ref().map(|c| c.harness),
            model: config.as_ref().and_then(|c| c.model.clone()),
            reasoning: config.as_ref().and_then(|c| c.reasoning),
            model_options: config
                .as_ref()
                .map(|c| c.model_options.clone())
                .unwrap_or_default(),
            cwd: chat.cwd.unwrap_or_default(),
            sandbox: config
                .as_ref()
                .map(|c| c.sandbox)
                .unwrap_or(zeron_proto::SandboxLevel::WorkspaceWrite),
            auto_approve: false,
            attachments: Vec::new(),
            mcp_servers: Vec::new(),
            resume: None,
            worktree: None,
        })
    }

    fn save_snapshot(&self, handle: &ChatDocHandle) {
        let _lifecycle = lock(&handle.sink_lifecycle);
        if handle.purged.load(Ordering::Acquire) {
            return;
        }
        self.maybe_clear_replay_from_zero_locked(handle);
        let _save = lock(&handle.snapshot_save);
        // Close the callback's client-vs-host-buffer decision while deciding
        // replay durability. We deliberately do not inspect ChatClient's
        // internal stats here: its actor enters the sink while holding that
        // state mutex, which would invert the lifecycle order.
        let _client_slot = lock(&handle.chat2);
        if lock(&self.inner.chat2_pending_local)
            .get(&handle.chat_id)
            .is_some_and(|pending| !pending.is_empty())
        {
            handle.replay_from_zero.store(true, Ordering::Release);
        }
        if let Err(err) = self.persist_cutover_snapshot_locked(handle, handle.room_gen) {
            tracing::warn!(chat = %handle.chat_id, error = %err, "snapshot save failed");
        }
    }

    /// Persist every open doc now (shutdown path; bypasses the debounce).
    pub fn flush_all(&self) {
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            self.save_snapshot(&handle);
        }
    }

    /// Close all account-scoped room memberships before graceful engine
    /// draining. Auth-aware join supervisors will not install a late client.
    pub fn disconnect_edge(&self) {
        let handles: Vec<_> = lock(&self.inner.handles).values().cloned().collect();
        for handle in handles {
            // Keep the local subscription alive while SessionsEngine settles
            // its active turns. New deltas then enter the host replay buffer
            // even though account-scoped transport has already been revoked.
            self.park_chat2_client(&handle);
        }
    }
}

/// The resumed-turn prompt for answers to a question whose run died: each
/// answer paired with its question text so the reattached conversation reads
/// naturally. Pure.
pub fn respond_input_prompt(
    questions: &[UserInputQuestion],
    answers: &[UserInputAnswer],
) -> String {
    let mut lines = vec!["Answering your earlier question:".to_string()];
    for answer in answers {
        let picked = answer.labels.join(", ");
        let question = questions
            .iter()
            .find(|q| q.id == answer.question_id)
            .map(|q| q.question.trim())
            .filter(|q| !q.is_empty());
        match question {
            Some(question) => lines.push(format!("{question} — {picked}")),
            None => lines.push(picked),
        }
    }
    lines.join("\n")
}

/// Percent-encode one URL path segment of a sidecar part id. PART_RE's
/// alphabet includes `#` and `:` — legal in R2 keys and doc refs, but a raw
/// `#` in a URL is a fragment delimiter (the request would silently hit the
/// truncated key, colliding parts). The Worker decodes before validating.
fn encode_part_segment(part_id: &str) -> String {
    let mut out = String::with_capacity(part_id.len());
    for byte in part_id.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod degrade_grace_tests {
    use super::{DEGRADE_GRACE, DegradeGrace, GraceKey};
    use std::time::{Duration, Instant};

    #[test]
    fn blips_shorter_than_the_grace_never_report() {
        let mut g = DegradeGrace::default();
        let t0 = Instant::now();
        // A 300ms room join: degraded at t0, healthy again shortly after.
        assert!(!g.degraded(GraceKey::Chat("c1"), true, t0));
        assert!(!g.degraded(GraceKey::Chat("c1"), true, t0 + Duration::from_millis(300)));
        assert!(!g.degraded(GraceKey::Chat("c1"), false, t0 + Duration::from_millis(600)));
        // The recovery cleared the timer — a fresh blip starts from zero.
        assert!(!g.degraded(GraceKey::Chat("c1"), true, t0 + Duration::from_secs(10)));
    }

    #[test]
    fn persistent_degradation_reports_after_the_grace_and_clears_instantly() {
        let mut g = DegradeGrace::default();
        let t0 = Instant::now();
        assert!(!g.degraded(GraceKey::Registry, true, t0));
        assert!(!g.degraded(GraceKey::Registry, true, t0 + DEGRADE_GRACE / 2));
        assert!(g.degraded(GraceKey::Registry, true, t0 + DEGRADE_GRACE));
        assert!(g.degraded(GraceKey::Registry, true, t0 + DEGRADE_GRACE * 3));
        // Hide-fast: one healthy sample reports Connected immediately.
        assert!(!g.degraded(GraceKey::Registry, false, t0 + DEGRADE_GRACE * 4));
    }

    #[test]
    fn sources_are_independent_and_closed_chats_are_dropped() {
        let mut g = DegradeGrace::default();
        let t0 = Instant::now();
        assert!(!g.degraded(GraceKey::Chat("gone"), true, t0));
        assert!(!g.degraded(GraceKey::OsPath, true, t0));
        // The chat's doc closes; its timer must not leak.
        g.retain_chats(|id| id != "gone");
        assert!(g.chats.is_empty());
        // OsPath kept its own timer through the retain.
        assert!(g.degraded(GraceKey::OsPath, true, t0 + DEGRADE_GRACE));
    }
}

#[cfg(test)]
mod part_segment_tests {
    use super::encode_part_segment;

    #[test]
    fn hash_and_colon_are_escaped_unreserved_pass_through() {
        assert_eq!(encode_part_segment("m1#c1"), "m1%23c1");
        assert_eq!(encode_part_segment("tool:call_9"), "tool%3Acall_9");
        assert_eq!(encode_part_segment("plain-id_0.diff~"), "plain-id_0.diff~");
    }
}

/// Per-chat background task: reacts to doc changes (local commits and remote imports)
/// by re-publishing the transcript watch, draining commands, and debouncing snapshots.
/// Holds only a weak handle so a dropped host tears the task down.
async fn chat_task(host: DocHost, weak: Weak<ChatDocHandle>, mut changed_rx: watch::Receiver<u64>) {
    // Initial pass: the snapshot may already carry pending commands. The
    // mirror stays lazy — it materializes on the first watch attach.
    {
        let Some(handle) = weak.upgrade() else { return };
        host.flush_buffered_chat2_updates(&handle);
        host.drain_commands(&handle).await;
    }
    let mut save_deadline: Option<tokio::time::Instant> = None;
    loop {
        let sleep_until = save_deadline.unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {
            changed = changed_rx.changed() => {
                if changed.is_err() {
                    break; // doc handle (and its change sender) is gone
                }
                let Some(handle) = weak.upgrade() else { break };
                handle.publish_messages_if_watched();
                host.flush_buffered_chat2_updates(&handle);
                host.drain_commands(&handle).await;
                if save_deadline.is_none() {
                    save_deadline = Some(
                        tokio::time::Instant::now()
                            + std::time::Duration::from_millis(SNAPSHOT_DEBOUNCE_MS),
                    );
                }
            }
            _ = tokio::time::sleep_until(sleep_until), if save_deadline.is_some() => {
                save_deadline = None;
                let Some(handle) = weak.upgrade() else { break };
                host.flush_buffered_chat2_updates(&handle);
                host.save_snapshot(&handle);
                // chat2 host duties ride the same quiesce tick (C3):
                // threshold checkpoints + the tail sidecar publish.
                host.chat2_maintenance(&handle).await;
                // Post-quiesce eviction pass: sizes just refreshed.
                host.evict_over_budget();
            }
        }
    }
}

#[cfg(test)]
mod agent_send_tests {
    use super::*;
    use futures::future::BoxFuture;
    use zeron_proto::HarnessId;
    use zeron_sync::chat_client::{ChatDocSink, ChatTransport, CheckpointFetcher};

    use crate::registry_host::RegistryHostConfig;

    struct NoopChatSink;

    impl ChatDocSink for NoopChatSink {
        fn apply_row(&self, _bytes: &[u8], _cursor: u64) {}
        fn apply_checkpoint(&self, _bytes: &[u8], _cursor: u64) -> Result<(), String> {
            Ok(())
        }
        fn contains_frontier(&self, _frontier: &[u8]) -> bool {
            true
        }
        fn advance_cursor(&self, _cursor: u64) {}
        fn reset_cursor(&self, _cursor: u64) {}
    }

    struct ClosedFetcher;

    impl CheckpointFetcher for ClosedFetcher {
        fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, zeron_sync::SyncError>> {
            Box::pin(async { Err(zeron_sync::SyncError::Closed) })
        }
    }

    struct ClosedTransport;

    impl ChatTransport for ClosedTransport {
        fn fetch_rows(
            &self,
            _after: u64,
        ) -> BoxFuture<'static, Result<Vec<u8>, zeron_sync::SyncError>> {
            Box::pin(async { Err(zeron_sync::SyncError::Closed) })
        }

        fn push(
            &self,
            _batch_id: String,
            _bytes: Vec<u8>,
        ) -> BoxFuture<'static, Result<String, zeron_sync::SyncError>> {
            Box::pin(async { Err(zeron_sync::SyncError::Closed) })
        }
    }

    struct ChatMigrationEdge {
        url: String,
        checkpoint: Arc<Mutex<Option<Vec<u8>>>>,
        checkpoints: Arc<Mutex<Vec<Vec<u8>>>>,
        checkpoint_requests: Arc<AtomicUsize>,
        first_checkpoint_started: Arc<tokio::sync::Notify>,
        release_first_checkpoint: Arc<tokio::sync::Notify>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ChatMigrationEdge {
        async fn start() -> Self {
            Self::start_with_first_checkpoint_gate(false).await
        }

        async fn start_gated() -> Self {
            Self::start_with_first_checkpoint_gate(true).await
        }

        async fn start_with_first_checkpoint_gate(gate_first: bool) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let checkpoint = Arc::new(Mutex::new(None));
            let checkpoints = Arc::new(Mutex::new(Vec::new()));
            let checkpoint_requests = Arc::new(AtomicUsize::new(0));
            let first_checkpoint_started = Arc::new(tokio::sync::Notify::new());
            let release_first_checkpoint = Arc::new(tokio::sync::Notify::new());
            let checkpoint_server = checkpoint.clone();
            let checkpoints_server = checkpoints.clone();
            let requests_server = checkpoint_requests.clone();
            let started_server = first_checkpoint_started.clone();
            let release_server = release_first_checkpoint.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let checkpoint = checkpoint_server.clone();
                    let checkpoints = checkpoints_server.clone();
                    let requests = requests_server.clone();
                    let started = started_server.clone();
                    let release = release_server.clone();
                    tokio::spawn(async move {
                        serve_chat_migration_request(
                            stream,
                            checkpoint,
                            checkpoints,
                            requests,
                            started,
                            release,
                            gate_first,
                        )
                        .await;
                    });
                }
            });
            Self {
                url,
                checkpoint,
                checkpoints,
                checkpoint_requests,
                first_checkpoint_started,
                release_first_checkpoint,
                task,
            }
        }

        async fn wait_for_first_checkpoint(&self) {
            loop {
                let notified = self.first_checkpoint_started.notified();
                if self.checkpoint_requests.load(Ordering::Acquire) > 0 {
                    return;
                }
                notified.await;
            }
        }

        fn release_first_checkpoint(&self) {
            self.release_first_checkpoint.notify_one();
        }
    }

    impl Drop for ChatMigrationEdge {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn read_http_request(
        stream: &mut tokio::net::TcpStream,
    ) -> Option<(String, String, Vec<u8>)> {
        use tokio::io::AsyncReadExt as _;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
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
        let mut request = lines.next()?.split_whitespace();
        let method = request.next()?.to_string();
        let target = request.next()?.to_string();
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
        Some((method, target, body))
    }

    async fn write_http_response(
        stream: &mut tokio::net::TcpStream,
        status: &str,
        content_type: &str,
        body: &[u8],
    ) {
        use tokio::io::AsyncWriteExt as _;

        let head = format!(
            "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.write_all(body).await;
    }

    fn length_prefix(frame: Vec<u8>, output: &mut Vec<u8>) {
        output.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        output.extend_from_slice(&frame);
    }

    async fn serve_chat_migration_request(
        mut stream: tokio::net::TcpStream,
        checkpoint: Arc<Mutex<Option<Vec<u8>>>>,
        checkpoints: Arc<Mutex<Vec<Vec<u8>>>>,
        checkpoint_requests: Arc<AtomicUsize>,
        first_checkpoint_started: Arc<tokio::sync::Notify>,
        release_first_checkpoint: Arc<tokio::sync::Notify>,
        gate_first: bool,
    ) {
        let Some((method, target, body)) = read_http_request(&mut stream).await else {
            return;
        };
        if method == "GET" && target.contains("/rows?") {
            let mut response = Vec::new();
            length_prefix(
                zeron_sync::chat_frames::encode(
                    zeron_sync::chat_frames::frame_type::STATE,
                    &serde_json::json!({
                        "headSeq": 1,
                        "seqFloor": 0,
                        "checkpointSeq": 0,
                        "checkpointSize": 0,
                        "rowCount": 1,
                        "rowBytes": 1,
                    }),
                    &[],
                ),
                &mut response,
            );
            length_prefix(
                zeron_sync::chat_frames::encode(
                    zeron_sync::chat_frames::frame_type::ROWS_DONE,
                    &serde_json::json!({ "headSeq": 1 }),
                    &[],
                ),
                &mut response,
            );
            write_http_response(&mut stream, "200 OK", "application/octet-stream", &response).await;
        } else if method == "POST" && target.contains("/checkpoint?") {
            let request = checkpoint_requests.fetch_add(1, Ordering::AcqRel);
            *lock(&checkpoint) = Some(body.clone());
            lock(&checkpoints).push(body);
            if request == 0 {
                first_checkpoint_started.notify_waiters();
                if gate_first {
                    release_first_checkpoint.notified().await;
                }
            }
            write_http_response(&mut stream, "200 OK", "application/json", b"{}").await;
        } else {
            // The WebSocket transport is intentionally unavailable; the test
            // exercises the production HTTPS pull plus checkpoint path.
            write_http_response(
                &mut stream,
                "503 Service Unavailable",
                "application/json",
                br#"{"error":"unavailable"}"#,
            )
            .await;
        }
    }

    fn host() -> (tempfile::TempDir, DocHost) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let host = DocHost::new(
            store,
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: None,
            },
        );
        (dir, host)
    }

    fn queued_commands(host: &DocHost, chat: &str) -> Vec<SessionCommandEntry> {
        host.open(chat).unwrap().doc.read_commands().unwrap()
    }

    async fn local_first_chat_client() -> zeron_sync::ChatClient {
        local_first_chat_client_at(0).await
    }

    async fn local_first_chat_client_at(cursor: u64) -> zeron_sync::ChatClient {
        zeron_sync::ChatClient::connect_via_transport(
            Arc::new(zeron_sync::StaticUrl("ws://127.0.0.1:9".into())),
            Arc::new(NoopChatSink),
            Arc::new(ClosedFetcher),
            "dev-a",
            cursor,
            Arc::new(ClosedTransport),
        )
        .await
        .expect("local-first client constructs before I/O")
    }

    #[test]
    fn first_contact_full_update_prepends_without_dropping_carry_over_deltas() {
        let (_dir, host) = host();
        DocHost::buffer_chat2_update(&host.inner, "handoff-chat", vec![1, 2]);
        DocHost::buffer_chat2_update(&host.inner, "handoff-chat", vec![3, 4]);

        DocHost::prepend_chat2_update(&host.inner, "handoff-chat", vec![9, 9]);

        assert_eq!(
            lock(&host.inner.chat2_pending_local)
                .get("handoff-chat")
                .cloned()
                .unwrap(),
            vec![vec![9, 9], vec![1, 2], vec![3, 4]],
            "full state is first, while individually sendable deltas survive as a suffix"
        );
    }

    #[test]
    fn detached_old_sink_cannot_overwrite_newer_epoch_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let old_doc = Arc::new(SessionDoc::init("late-sink").unwrap());
        let lifecycle = Arc::new(Mutex::new(()));
        let generation = Arc::new(AtomicU64::new(0));
        let frozen = Arc::new(AtomicBool::new(false));
        let replay = Arc::new(AtomicBool::new(false));
        let replay_fence = Arc::new(Mutex::new(()));
        let sink = crate::chat2_host::EngineChatSink::new_with_lifecycle(
            &old_doc,
            store.clone(),
            "late-sink",
            2,
            7,
            2,
            lifecycle.clone(),
            generation.clone(),
            frozen.clone(),
            replay,
            replay_fence,
        );
        sink.advance_cursor(8);

        let target = SessionDoc::init("late-sink").unwrap();
        target
            .doc()
            .get_map("meta")
            .insert("target-epoch", "three")
            .unwrap();
        target.doc().commit();
        {
            let _gate = lock(&lifecycle);
            frozen.store(true, Ordering::Release);
            generation.fetch_add(1, Ordering::AcqRel);
            store
                .save_snapshot_with_cursor("late-sink", &target.export_snapshot().unwrap(), 0, 3)
                .unwrap();
            frozen.store(false, Ordering::Release);
        }

        // A detached HTTP task may finish after the replacement is live. Its
        // captured generation must make both cursor and bytes writes inert.
        sink.advance_cursor(999);
        let (persisted, cursor, epoch) = store
            .load_snapshot_with_cursor("late-sink")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (0, 3));
        let raw = loro::LoroDoc::new();
        raw.import(&persisted).unwrap();
        assert!(matches!(
            raw.get_map("meta").get("target-epoch"),
            Some(loro::ValueOrContainer::Value(loro::LoroValue::String(value)))
                if value.as_ref() == "three"
        ));
    }

    #[tokio::test]
    async fn cursor_nonzero_disconnect_persists_full_replay_for_same_generation_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let registry = RegistryHost::open(
            store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("disconnect-replay", None, Some("dev-a"), None, None)
            .unwrap();
        let initial = SessionDoc::init("disconnect-replay").unwrap();
        store
            .save_snapshot_with_cursor(
                "disconnect-replay",
                &initial.export_snapshot().unwrap(),
                41,
                2,
            )
            .unwrap();
        let edge = EdgeConfig::with_static_token("http://127.0.0.1:9", "token");
        let first_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: Some(edge.clone()),
            },
        );
        first_host.set_registry(registry.clone());
        let first = first_host.open("disconnect-replay").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while lock(&first.chat2).is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("first client did not install");
        first
            .doc
            .doc()
            .get_map("meta")
            .insert("pending-before-disconnect", "replay-me")
            .unwrap();
        first.doc.doc().commit();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let client_pending = lock(&first.chat2)
                    .as_ref()
                    .is_some_and(|client| client.stats().pending_pushes > 0);
                let host_pending = lock(&first_host.inner.chat2_pending_local)
                    .get("disconnect-replay")
                    .is_some_and(|pending| !pending.is_empty());
                if client_pending || host_pending {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("local update never entered a replay queue");
        first_host.disconnect_edge();
        first_host.flush_all();
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("disconnect-replay")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (0, 2));
        first_host.shutdown_workers().await;
        drop(first);
        drop(first_host);

        let second_host = DocHost::new(
            store,
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: Some(edge),
            },
        );
        second_host.set_registry(registry);
        let reopened = second_host.open("disconnect-replay").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let ready = lock(&reopened.chat2)
                    .as_ref()
                    .is_some_and(|client| client.stats().pending_pushes > 0);
                if ready {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cursor-zero restart did not enqueue the complete doc");
        let client = lock(&reopened.chat2).take().unwrap();
        let updates = client.into_pending_updates();
        let replay = loro::LoroDoc::new();
        for update in updates {
            replay.import(&update).unwrap();
        }
        assert!(matches!(
            replay.get_map("meta").get("pending-before-disconnect"),
            Some(loro::ValueOrContainer::Value(loro::LoroValue::String(value)))
                if value.as_ref() == "replay-me"
        ));
        second_host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn replay_clear_persists_the_exact_lower_cursor_before_clearing() {
        let (_dir, host) = host();
        let handle = host.open("reset-clear").unwrap();
        let client = local_first_chat_client_at(5).await;
        host.install_chat2_client(&handle, client);
        handle.replay_from_zero.store(true, Ordering::Release);
        host.inner
            .store
            .save_snapshot_with_cursor(
                "reset-clear",
                &handle.doc.export_snapshot().unwrap(),
                41,
                handle.room_gen,
            )
            .unwrap();

        host.maybe_clear_replay_from_zero(&handle);

        assert!(!handle.replay_from_zero.load(Ordering::Acquire));
        let (_, cursor, epoch) = host
            .inner
            .store
            .load_snapshot_with_cursor("reset-clear")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (5, handle.room_gen));
        host.save_snapshot(&handle);
        let (_, cursor, epoch) = host
            .inner
            .store
            .load_snapshot_with_cursor("reset-clear")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (5, handle.room_gen));
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn checkpoint_only_replay_clear_preserves_the_sink_owned_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let registry = RegistryHost::open(
            store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("checkpoint-epoch", None, Some("dev-a"), None, None)
            .unwrap();
        registry.set_chat_room_gen("checkpoint-epoch", 3).unwrap();
        let source = SessionDoc::init("checkpoint-epoch").unwrap();
        store
            .save_snapshot_with_cursor("checkpoint-epoch", &source.export_snapshot().unwrap(), 0, 2)
            .unwrap();
        let host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: None,
            },
        );
        host.set_registry(registry);
        let handle = host.open("checkpoint-epoch").unwrap();
        assert_eq!(handle.room_gen, 3);
        host.install_chat2_client(&handle, local_first_chat_client_at(5).await);
        handle.replay_from_zero.store(true, Ordering::Release);
        // A successful checkpoint may cover a rejected replay without an own
        // row ack. Clearing cursor-zero is then safe, but promoting epoch 2 ->
        // 3 is not: the live sink still owns epoch 2 until advance_cursor.
        handle
            .checkpointed_rejections
            .store(u64::MAX, Ordering::Release);

        host.maybe_clear_replay_from_zero(&handle);

        assert!(!handle.replay_from_zero.load(Ordering::Acquire));
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("checkpoint-epoch")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (5, 2));
        host.save_snapshot(&handle);
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("checkpoint-epoch")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (5, 2));
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn replay_clear_never_overwrites_a_newer_snapshot_epoch() {
        let (_dir, host) = host();
        let handle = host.open("newer-clear").unwrap();
        let client = local_first_chat_client_at(5).await;
        host.install_chat2_client(&handle, client);
        handle.replay_from_zero.store(true, Ordering::Release);
        host.inner
            .store
            .save_snapshot_with_cursor(
                "newer-clear",
                &handle.doc.export_snapshot().unwrap(),
                9,
                handle.room_gen + 1,
            )
            .unwrap();

        host.maybe_clear_replay_from_zero(&handle);

        assert!(handle.replay_from_zero.load(Ordering::Acquire));
        let (_, cursor, epoch) = host
            .inner
            .store
            .load_snapshot_with_cursor("newer-clear")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (9, handle.room_gen + 1));
        host.shutdown_workers().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_callback_dirty_mark_cannot_be_overwritten_while_waiting_for_chat_slot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let registry = RegistryHost::open(
            store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("replay-fence", None, Some("dev-a"), None, None)
            .unwrap();
        let initial = SessionDoc::init("replay-fence").unwrap();
        store
            .save_snapshot_with_cursor("replay-fence", &initial.export_snapshot().unwrap(), 5, 2)
            .unwrap();
        let host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: Some(EdgeConfig::with_static_token("http://127.0.0.1:9", "token")),
            },
        );
        host.set_registry(registry);
        let handle = host.open("replay-fence").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while lock(&handle.chat2).is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("local-first client did not install");

        let client_slot = lock(&handle.chat2);
        let writer = handle.doc.clone();
        let writer_task = std::thread::spawn(move || {
            writer
                .doc()
                .get_map("meta")
                .insert("callback-race", "durable")
                .unwrap();
            writer.doc().commit();
        });
        for _ in 0..200 {
            if handle.replay_from_zero.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(handle.replay_from_zero.load(Ordering::Acquire));

        let save_host = host.clone();
        let save_handle = handle.clone();
        let save_task = std::thread::spawn(move || save_host.save_snapshot(&save_handle));
        std::thread::sleep(std::time::Duration::from_millis(10));
        drop(client_slot);
        writer_task.join().unwrap();
        save_task.join().unwrap();

        assert!(handle.replay_from_zero.load(Ordering::Acquire));
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("replay-fence")
            .unwrap()
            .unwrap();
        // The cursor resets for the newer room generation, but the epoch
        // stays at the stored thin epoch until an own batch is acked there.
        assert_eq!((cursor, epoch), (0, 2));
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn frozen_handle_rejects_new_writer_leases() {
        let (_dir, host) = host();
        let handle = host.open("writer-freeze").unwrap();
        {
            let _lifecycle = lock(&handle.sink_lifecycle);
            handle.generation_frozen.store(true, Ordering::Release);
        }
        assert!(handle.writer_doc().is_err());
        host.purge_chat("writer-freeze");
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn checkpoint_request_survives_a_temporarily_empty_client_slot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let registry = RegistryHost::open(
            store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("checkpoint-retry", None, Some("dev-a"), None, None)
            .unwrap();
        let edge_server = ChatMigrationEdge::start().await;
        let host = DocHost::new(
            store,
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: Some(EdgeConfig::with_static_token(&edge_server.url, "token")),
            },
        );
        host.set_registry(registry);
        let handle = host.open("checkpoint-retry").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while lock(&handle.chat2).is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("client never installed");
        let client = lock(&handle.chat2).take().unwrap();

        host.spawn_chat2_checkpoint(&handle, "busy-regression");
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        assert_eq!(edge_server.checkpoint_requests.load(Ordering::Acquire), 0);
        host.install_chat2_client(&handle, client);

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while edge_server.checkpoint_requests.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("checkpoint request was dropped while the client slot was unavailable");
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn registry_row_removal_purges_warm_handle_pending_and_snapshot() {
        let (_dir, host) = host();
        let registry = RegistryHost::open(
            host.inner.store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("remote-delete", None, Some("dev-a"), None, None)
            .unwrap();
        host.set_registry(registry.clone());
        let handle = host.open("remote-delete").unwrap();
        host.save_snapshot(&handle);
        DocHost::buffer_chat2_update(&host.inner, "remote-delete", vec![1, 2, 3]);

        registry.delete_chat("remote-delete").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if DocHost::chat_was_purged(&host.inner, "remote-delete")
                    && !lock(&host.inner.handles).contains_key("remote-delete")
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("registry row removal did not purge the local lifecycle");
        assert!(!lock(&host.inner.chat2_pending_local).contains_key("remote-delete"));
        assert!(
            host.inner
                .store
                .load_snapshot_with_cursor("remote-delete")
                .unwrap()
                .is_none()
        );
        assert!(host.open("remote-delete").is_err());
        host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn restart_after_foreign_chat2_park_requeues_full_doc_into_chat3() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let registry = RegistryHost::open(
            store.clone(),
            RegistryHostConfig {
                device_id: "dev-a".into(),
                device_name: "Device A".into(),
                platform: "test".into(),
                organization_id: "org-a".into(),
                user_id: "alice".into(),
                edge: None,
            },
        )
        .unwrap();
        registry
            .create_chat("restart-carry", None, Some("dev-b"), None, None)
            .unwrap();

        // Process one: a foreign chat2 snapshot already has a non-zero chat2
        // cursor, then accumulates local work while its single-owner room is
        // parked. Plain snapshot saves retain cursor=41, epoch=2.
        let first_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: None,
            },
        );
        first_host.set_registry(registry.clone());
        let first = first_host.open("restart-carry").unwrap();
        first
            .doc
            .doc()
            .get_map("meta")
            .insert("parked-local", "survives-restart")
            .unwrap();
        first.doc.doc().commit();
        let snapshot = first.doc.export_snapshot().unwrap();
        store
            .save_snapshot_with_cursor("restart-carry", &snapshot, 41, 2)
            .unwrap();
        first_host.shutdown_workers().await;
        drop(first);
        drop(first_host);

        // The stored snapshot still carries the old epoch-2 cursor namespace;
        // process two has no in-memory carry-over map.
        let second_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "dev-a".into(),
                user_id: "alice".into(),
                default_harness: HarnessId::Mock,
                edge: Some(EdgeConfig::with_static_token("http://127.0.0.1:9", "token")),
            },
        );
        second_host.set_registry(registry);
        let replacement = second_host.open("restart-carry").unwrap();
        assert_eq!(replacement.room_gen, 3);

        // Local-first join installs quickly even though the test endpoint is
        // closed. The old cursor must be treated as zero, which activates the
        // complete-update enqueue before any network success is possible.
        for _ in 0..100 {
            let ready = lock(&replacement.chat2)
                .as_ref()
                .is_some_and(|client| client.stats().pending_pushes > 0);
            if ready {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let client = lock(&replacement.chat2)
            .take()
            .expect("replacement client installed");
        assert_eq!(
            client.stats().cursor,
            0,
            "chat2 cursor must not cross into chat3"
        );
        let updates = client.into_pending_updates();
        assert!(!updates.is_empty(), "full local update must be requeued");

        let replay = loro::LoroDoc::new();
        for update in updates {
            replay.import(&update).unwrap();
        }
        assert!(matches!(
            replay.get_map("meta").get("parked-local"),
            Some(loro::ValueOrContainer::Value(loro::LoroValue::String(value)))
                if value.as_ref() == "survives-restart"
        ));
        second_host.shutdown_workers().await;
    }

    #[tokio::test]
    async fn send_lands_a_steer_with_agent_attribution_and_hops() {
        let (_dir, host) = host();
        host.send_to_session("chat-a", "chat-b", "hello from a")
            .unwrap();
        let commands = queued_commands(&host, "chat-b");
        assert_eq!(commands.len(), 1);
        let entry = &commands[0];
        assert_eq!(entry.user_id.as_deref(), Some("agent:chat-a"));
        let origin = entry.origin.as_ref().expect("origin");
        assert_eq!(origin.from_chat_id, "chat-a");
        assert_eq!(origin.hops, 1);
        assert!(matches!(
            entry.payload,
            SessionCommandPayload::Steer { ref prompt, .. } if prompt == "hello from a"
        ));
    }

    #[tokio::test]
    async fn self_send_and_empty_message_are_rejected() {
        let (_dir, host) = host();
        assert!(host.send_to_session("chat-a", "chat-a", "loop").is_err());
        assert!(host.send_to_session("chat-a", "chat-b", "   ").is_err());
        assert!(queued_commands(&host, "chat-b").is_empty());
    }

    #[tokio::test]
    async fn hop_limit_breaks_ping_pong() {
        let (_dir, host) = host();
        host.set_turn_origin_for_test("chat-a", 3);
        assert!(host.send_to_session("chat-a", "chat-b", "hop 4").is_ok());
        host.set_turn_origin_for_test("chat-a", 4);
        let err = host
            .send_to_session("chat-a", "chat-c", "hop 5")
            .unwrap_err();
        assert!(err.to_string().contains("too deep"), "{err}");

        host.set_turn_origin_for_test("chat-a", u32::MAX);
        let err = host
            .send_to_session("chat-a", "chat-d", "overflow must reject")
            .unwrap_err();
        assert!(err.to_string().contains("too deep"), "{err}");
        assert!(queued_commands(&host, "chat-d").is_empty());
    }

    #[tokio::test]
    async fn per_turn_send_budget_is_enforced_and_reset_on_new_turn() {
        let (_dir, host) = host();
        for i in 0..8 {
            host.send_to_session("chat-a", "chat-b", &format!("m{i}"))
                .unwrap();
        }
        let err = host
            .send_to_session("chat-a", "chat-b", "one too many")
            .unwrap_err();
        assert!(err.to_string().contains("budget"), "{err}");
        // A fresh turn (execute records the origin and clears the budget).
        lock(&host.inner.send_budgets).remove("chat-a");
        assert!(
            host.send_to_session("chat-a", "chat-b", "next turn")
                .is_ok()
        );
    }
}
