//! chat2 host wiring (docs/chat2-sync.md C3): the engine-side implementations
//! of [`zeron_sync::chat_client::ChatDocSink`] and
//! [`zeron_sync::chat_client::CheckpointFetcher`], binding a
//! [`crate::doc_host::ChatDocHandle`]'s live doc to a chat2 room.
//!
//! The C2 rule is enforced HERE: every sink method persists doc content AND
//! the room cursor in one `save_snapshot_with_cursor` transaction, so a
//! restored backup can never disagree with its own cursor — the root cause
//! of the redownload-forever class the old s2 clients suffered.

use std::sync::Arc;

use futures::future::BoxFuture;
use zeron_doc::SessionDoc;
use zeron_sync::chat_client::{ChatDocSink, CheckpointFetcher};
use zeron_sync::{DocsStore, SyncError};

use crate::doc_host::EdgeConfig;

/// Minimum synchronized-room epoch. Values below 2 are legacy fat/s2 docs;
/// values at or above 2 identify the room generation whose cursor is stored
/// beside the thin snapshot (2 = chat2, 3 = org-shared chat3).
pub const CHAT2_DOC_EPOCH: u32 = 2;

/// [`ChatDocSink`] over a live [`SessionDoc`] + the cursor-bearing store.
///
/// Loro import of a remote row/checkpoint fires the doc's root subscription,
/// so the transcript watch, command drain, and debounced UI publish all ride
/// the existing change plumbing — this type only owns import + same-tx
/// persistence.
pub struct EngineChatSink {
    /// WEAK: the sink lives inside the handle's `ChatClient` for the
    /// client's whole life — a strong ref here kept
    /// `Arc::strong_count(&handle.doc) > 1` permanently, which reads as
    /// "live writer" to `pinned()` and made every chat2 handle immune to
    /// LRU eviction (unbounded warm-doc growth). Callbacks upgrade per
    /// call; a dead doc (evicted handle) is a no-op.
    doc: std::sync::Weak<SessionDoc>,
    store: Arc<DocsStore>,
    chat_id: String,
    /// Room generation this client is converging onto.
    target_room_gen: u32,
    /// Serialized cursor/epoch persistence. During a generation handoff,
    /// remote rows may arrive before the full-local-update batch is acked;
    /// those rows keep the old epoch so a crash still resets/requeues. The
    /// first own-write ack promotes the epoch to `target_room_gen`.
    persist: std::sync::Mutex<PersistState>,
}

struct PersistState {
    cursor: u64,
    epoch: u32,
}

impl EngineChatSink {
    pub fn new(
        doc: &Arc<SessionDoc>,
        store: Arc<DocsStore>,
        chat_id: impl Into<String>,
        room_gen: u32,
        initial_cursor: u64,
        initial_epoch: u32,
    ) -> Self {
        let target_room_gen = room_gen.max(CHAT2_DOC_EPOCH);
        Self {
            doc: Arc::downgrade(doc),
            store,
            chat_id: chat_id.into(),
            target_room_gen,
            persist: std::sync::Mutex::new(PersistState {
                cursor: initial_cursor,
                epoch: initial_epoch.max(CHAT2_DOC_EPOCH).min(target_room_gen),
            }),
        }
    }

    /// Export the CURRENT doc and persist it with its cursor in one tx.
    /// `promote` means an own batch was acked in the target room. `exact`
    /// crosses an explicitly observed server-reset boundary and is the sole
    /// path allowed to move a persisted cursor backwards.
    fn persist_with_cursor(&self, cursor: u64, promote: bool, exact: bool) {
        let Some(doc) = self.doc.upgrade() else {
            return;
        };
        let mut state = self
            .persist
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.cursor = if exact {
            cursor
        } else {
            state.cursor.max(cursor)
        };
        if promote {
            state.epoch = self.target_room_gen;
        }
        match doc.export_snapshot() {
            Ok(bytes) => {
                if let Err(err) = self.store.save_snapshot_with_cursor(
                    &self.chat_id,
                    &bytes,
                    state.cursor,
                    state.epoch,
                ) {
                    tracing::warn!(chat = %self.chat_id, error = %err,
                        "chat2 sink: snapshot persist failed (will retry on next change)");
                }
            }
            Err(err) => {
                tracing::warn!(chat = %self.chat_id, error = %err,
                    "chat2 sink: snapshot export failed");
            }
        }
    }
}

impl ChatDocSink for EngineChatSink {
    fn apply_row(&self, bytes: &[u8], cursor: u64) {
        let Some(doc) = self.doc.upgrade() else {
            return;
        };
        if let Err(err) = doc.doc().import(bytes) {
            // Malformed remote bytes cost the row, never the doc (the same
            // skip-not-fail rule as transcript reads). The cursor still
            // advances: replaying a poison row forever is the wedge class.
            tracing::warn!(chat = %self.chat_id, error = %err,
                "chat2 sink: row import failed; skipping row");
        }
        self.persist_with_cursor(cursor, false, false);
    }

    fn apply_checkpoint(&self, bytes: &[u8], cursor: u64) -> Result<(), String> {
        let doc = self.doc.upgrade().ok_or("doc evicted")?;
        doc.doc()
            .import(bytes)
            .map_err(|e| format!("checkpoint import: {e}"))?;
        self.persist_with_cursor(cursor, false, false);
        Ok(())
    }

    fn contains_frontier(&self, frontier: &[u8]) -> bool {
        let Some(doc) = self.doc.upgrade() else {
            return true; // evicted: claim contained so the client idles, not refetches
        };
        // NOTE deliberately no empty-frontier shortcut: an empty payload on
        // a PRESENT checkpoint is unreadable provenance, not proof there is
        // nothing to fetch — that shortcut made every fresh reader of such a
        // room skip the chat's founding ops and park all dependent rows
        // invisibly ("Add Tweets" incident, 2026-08-18). Empty falls through
        // to the decode failure below: NOT contained, fetch the checkpoint —
        // always safe (full-state merge; an empty-doc seed applies as a
        // no-op), never silently skips history. Callers already short-circuit
        // the checkpointSize == 0 (no checkpoint at all) case.
        let Ok(vv) = loro::VersionVector::decode(frontier) else {
            // Unreadable frontier → claim NOT contained: the client then
            // fetches the checkpoint, which is always safe (full-state
            // merge), never silently skips history.
            tracing::info!(chat = %self.chat_id, bytes = frontier.len(),
                "chat2 frontier unreadable; fetching checkpoint");
            return false;
        };
        // A decoded-but-EMPTY version vector is the vacuous claim: every doc
        // "includes" empty, so the check would pass for readers that hold
        // NOTHING and they'd skip the chat's founding ops (the actual "Add
        // Tweets" poison, one representation deeper than zero-length bytes).
        // A checkpoint the callers care about (size > 0) claiming empty
        // state is a contradiction — fetch it.
        if vv.is_empty() {
            tracing::info!(chat = %self.chat_id,
                "chat2 frontier decodes empty (vacuous); fetching checkpoint");
            return false;
        }
        doc.doc().oplog_vv().includes_vv(&vv)
    }

    fn advance_cursor(&self, cursor: u64) {
        self.persist_with_cursor(cursor, true, false);
    }

    fn reset_cursor(&self, cursor: u64) {
        self.persist_with_cursor(cursor, false, true);
    }
}

/// `GET /chat2/{chatId}/checkpoint` with Range resume — the fetcher half of
/// the C1 client contract. Partial downloads resume at the byte offset where
/// the previous attempt died (the DO serves 206), which is the entire point
/// of checkpoint-over-HTTP on the 1.2 Mbps links this design targets.
pub struct EdgeCheckpointFetcher {
    http: reqwest::Client,
    edge: EdgeConfig,
    chat_id: String,
    /// 2 = `/chat2/...`, 3 = org-shared `/chat3/...`.
    room_gen: u32,
}

impl EdgeCheckpointFetcher {
    pub fn new(
        http: reqwest::Client,
        edge: EdgeConfig,
        chat_id: impl Into<String>,
        room_gen: u32,
    ) -> Self {
        Self {
            http,
            edge,
            chat_id: chat_id.into(),
            room_gen,
        }
    }
}

impl CheckpointFetcher for EdgeCheckpointFetcher {
    fn fetch(&self) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        let http = self.http.clone();
        let edge = self.edge.clone();
        let url = format!(
            "{}/{}/{}/checkpoint",
            edge.url.trim_end_matches('/'),
            if self.room_gen >= 3 { "chat3" } else { "chat2" },
            self.chat_id
        );
        Box::pin(async move {
            let mut got: Vec<u8> = Vec::new();
            let mut seen_seq: Option<String> = None;
            // Range-resume loop: each attempt continues at the byte where
            // the last one stopped. Attempt count bounds a flapping link;
            // the ChatClient's own deadline bounds wall clock.
            for _attempt in 0..4 {
                let bearer = edge
                    .bearer()
                    .await
                    .ok_or_else(|| SyncError::Auth("signed out".into()))?;
                let mut req = http.get(&url).bearer_auth(&bearer);
                if !got.is_empty() {
                    req = req.header("range", format!("bytes={}-", got.len()));
                }
                let res = match req.send().await {
                    Ok(res) => res,
                    Err(err) => {
                        tracing::warn!(error = %err, "chat2 checkpoint fetch attempt failed");
                        continue;
                    }
                };
                // Resume validator: a NEW checkpoint can commit between
                // attempts, and a Range against it would splice two different
                // blobs (the import fails and burns a whole redial cycle).
                // The DO stamps every response with the checkpoint's seq —
                // on change, restart the download from byte 0.
                let seq = res
                    .headers()
                    .get("x-chat2-checkpoint-seq")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                if seq.is_some() && seen_seq.is_some() && seq != seen_seq {
                    tracing::info!(
                        resumed_at = got.len(),
                        "chat2 checkpoint replaced mid-download; restarting from 0"
                    );
                    got.clear();
                    seen_seq = seq;
                    continue;
                }
                if seq.is_some() {
                    seen_seq = seq;
                }
                match res.status().as_u16() {
                    200 => got.clear(),
                    206 => {}
                    416 => return Err(SyncError::Protocol("checkpoint range beyond end".into())),
                    404 => return Err(SyncError::Protocol("no checkpoint".into())),
                    code => return Err(SyncError::Protocol(format!("checkpoint HTTP {code}"))),
                }
                let mut stream = res;
                loop {
                    match stream.chunk().await {
                        Ok(Some(chunk)) => got.extend_from_slice(&chunk),
                        Ok(None) => return Ok(got),
                        Err(err) => {
                            // Mid-body drop: keep the bytes, resume via Range.
                            tracing::warn!(error = %err, resumed_at = got.len(),
                                "chat2 checkpoint stream dropped; resuming");
                            break;
                        }
                    }
                }
            }
            Err(SyncError::Protocol(
                "checkpoint fetch exhausted resume attempts".into(),
            ))
        })
    }
}

/// Plain-HTTPS chat pull/push (the airplane-wifi transport): GET/POST
/// `/chat2/{id}/rows` for legacy rooms or `/chat3/{id}/rows` for org-shared
/// rooms, with the same bearer auth the checkpoint fetcher uses.
pub struct EdgeChatTransport {
    http: reqwest::Client,
    edge: EdgeConfig,
    chat_id: String,
    device_id: String,
    /// 2 = `/chat2/...`, 3 = org-shared `/chat3/...`.
    room_gen: u32,
}

impl EdgeChatTransport {
    pub fn new(
        http: reqwest::Client,
        edge: EdgeConfig,
        chat_id: impl Into<String>,
        device_id: impl Into<String>,
        room_gen: u32,
    ) -> Self {
        Self {
            http,
            edge,
            chat_id: chat_id.into(),
            device_id: device_id.into(),
            room_gen,
        }
    }

    fn rows_url(&self) -> String {
        format!(
            "{}/{}/{}/rows",
            self.edge.url.trim_end_matches('/'),
            if self.room_gen >= 3 { "chat3" } else { "chat2" },
            self.chat_id
        )
    }
}

fn chat_rows_status_error(operation: &str, status: reqwest::StatusCode) -> SyncError {
    let message = format!("chat {operation} http {status}");
    if status == reqwest::StatusCode::FORBIDDEN {
        SyncError::AccessDenied(message)
    } else {
        SyncError::Protocol(message)
    }
}

fn chat_push_status_error(status: reqwest::StatusCode, body: &str) -> SyncError {
    if status == reqwest::StatusCode::FORBIDDEN {
        return SyncError::AccessDenied(format!("chat push http {status}"));
    }
    let code = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value["error"].as_str().map(str::to_owned));
    if matches!(code.as_deref(), Some("too_large" | "empty" | "bad_push")) {
        return SyncError::PushRejected(code.expect("matched a present code"));
    }
    SyncError::Protocol(match code {
        Some(code) => format!("chat push http {status}: {code}"),
        None => format!("chat push http {status}"),
    })
}

impl zeron_sync::chat_client::ChatTransport for EdgeChatTransport {
    fn fetch_rows(&self, after: u64) -> BoxFuture<'static, Result<Vec<u8>, SyncError>> {
        let http = self.http.clone();
        let edge = self.edge.clone();
        let url = self.rows_url();
        let device = self.device_id.clone();
        Box::pin(async move {
            let bearer = edge
                .bearer()
                .await
                .ok_or_else(|| SyncError::Auth("signed out".into()))?;
            let res = http
                .get(&url)
                .query(&[("after", after.to_string()), ("device", device)])
                .bearer_auth(&bearer)
                .send()
                .await
                .map_err(|e| SyncError::WebSocket(e.to_string()))?;
            if !res.status().is_success() {
                return Err(chat_rows_status_error("pull", res.status()));
            }
            let bytes = res
                .bytes()
                .await
                .map_err(|e| SyncError::WebSocket(e.to_string()))?;
            Ok(bytes.to_vec())
        })
    }

    fn push(
        &self,
        batch_id: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String, SyncError>> {
        let http = self.http.clone();
        let edge = self.edge.clone();
        let url = self.rows_url();
        let device = self.device_id.clone();
        Box::pin(async move {
            let bearer = edge
                .bearer()
                .await
                .ok_or_else(|| SyncError::Auth("signed out".into()))?;
            let res = http
                .post(&url)
                .query(&[("batchId", batch_id), ("device", device)])
                .bearer_auth(&bearer)
                .body(bytes)
                .send()
                .await
                .map_err(|e| SyncError::WebSocket(e.to_string()))?;
            let status = res.status();
            let body = res
                .text()
                .await
                .map_err(|e| SyncError::WebSocket(e.to_string()))?;
            if !status.is_success() {
                return Err(chat_push_status_error(status, &body));
            }
            Ok(body)
        })
    }
}

#[cfg(test)]
mod frontier_tests {
    use super::*;
    use std::sync::Arc;

    /// The empty-frontier-means-contained shortcut skipped the chat's
    /// founding ops for every fresh reader of a room whose checkpoint
    /// carries an empty frontier label, parking all dependent rows
    /// invisibly ("Add Tweets" incident, 2026-08-18). An empty frontier on
    /// a present checkpoint must read as NOT contained — the fetch is
    /// always safe; the skip never is.
    /// The 2026-08-18 room's actual poison, one level deeper: a frontier
    /// that is a VALID ENCODING of an EMPTY version vector. Any doc
    /// vacuously "includes" empty, so the containment check said yes and
    /// fresh readers skipped the checkpoint anyway. A vacuous claim is not
    /// containment.
    #[test]
    fn encoded_empty_frontier_is_not_contained() {
        let dir = std::env::temp_dir().join(format!("zeron-frontier2-{}", std::process::id()));
        let store = Arc::new(DocsStore::open(&dir).expect("store opens"));
        let doc = Arc::new(SessionDoc::from_doc(loro::LoroDoc::new()));
        let sink = EngineChatSink::new(&doc, store, "frontier-test-2", 2, 0, 2);
        let encoded_empty = loro::VersionVector::default().encode();
        assert!(
            !sink.contains_frontier(&encoded_empty),
            "an encoded-empty frontier must trigger the fetch, not vacuous containment"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_frontier_is_not_contained() {
        let dir = std::env::temp_dir().join(format!("zeron-frontier-test-{}", std::process::id()));
        let store = Arc::new(DocsStore::open(&dir).expect("store opens"));
        let doc = Arc::new(SessionDoc::from_doc(loro::LoroDoc::new()));
        let sink = EngineChatSink::new(&doc, store, "frontier-test", 2, 0, 2);
        assert!(
            !sink.contains_frontier(&[]),
            "empty frontier on a present checkpoint must trigger the fetch"
        );
        // A real, contained frontier still short-circuits the fetch — the
        // doc needs actual ops, or its own frontier is the vacuous-empty one.
        doc.doc().get_map("meta").insert("k", "v").expect("insert");
        doc.doc().commit();
        let vv = doc.doc().oplog_vv().encode();
        assert!(sink.contains_frontier(&vv));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn edge_transport_routes_org_rooms_to_chat3_rows() {
        let edge = EdgeConfig::with_static_token("https://edge.example/", "token");
        let legacy = EdgeChatTransport::new(
            reqwest::Client::new(),
            edge.clone(),
            "chat-id",
            "device-id",
            2,
        );
        let org_shared =
            EdgeChatTransport::new(reqwest::Client::new(), edge, "chat-id", "device-id", 3);

        assert_eq!(legacy.rows_url(), "https://edge.example/chat2/chat-id/rows");
        assert_eq!(
            org_shared.rows_url(),
            "https://edge.example/chat3/chat-id/rows"
        );
        assert!(!org_shared.rows_url().contains("/chat2/"));
    }

    #[test]
    fn rows_http_403_is_typed_without_reclassifying_other_statuses() {
        for operation in ["pull", "push"] {
            assert!(matches!(
                chat_rows_status_error(operation, reqwest::StatusCode::FORBIDDEN),
                SyncError::AccessDenied(_)
            ));
            assert!(matches!(
                chat_rows_status_error(operation, reqwest::StatusCode::UNAUTHORIZED),
                SyncError::Protocol(_)
            ));
            assert!(matches!(
                chat_rows_status_error(operation, reqwest::StatusCode::INTERNAL_SERVER_ERROR),
                SyncError::Protocol(_)
            ));
        }
        assert!(matches!(
            chat_push_status_error(
                reqwest::StatusCode::PAYLOAD_TOO_LARGE,
                r#"{"error":"too_large"}"#,
            ),
            SyncError::PushRejected(code) if code == "too_large"
        ));
        assert!(matches!(
            chat_push_status_error(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                r#"{"error":"quota"}"#,
            ),
            SyncError::Protocol(_)
        ));
    }

    #[test]
    fn gen3_sink_persists_the_gen3_cursor_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let doc = Arc::new(SessionDoc::from_doc(loro::LoroDoc::new()));
        doc.doc().get_map("meta").insert("k", "v").unwrap();
        doc.doc().commit();
        let sink = EngineChatSink::new(&doc, store.clone(), "gen3-chat", 3, 0, 2);

        sink.apply_row(&doc.export_snapshot().unwrap(), 5);
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("gen3-chat")
            .unwrap()
            .unwrap();
        assert_eq!(
            (cursor, epoch),
            (5, 2),
            "remote rows must not finish the handoff"
        );

        sink.advance_cursor(17);

        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("gen3-chat")
            .unwrap()
            .unwrap();
        assert_eq!(cursor, 17);
        assert_eq!(epoch, 3);
    }

    #[test]
    fn same_generation_reset_persists_an_exact_lower_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let doc = Arc::new(SessionDoc::from_doc(loro::LoroDoc::new()));
        doc.doc().get_map("meta").insert("k", "v").unwrap();
        doc.doc().commit();
        let sink = EngineChatSink::new(&doc, store.clone(), "reset-chat", 3, 41, 3);

        sink.reset_cursor(0);
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("reset-chat")
            .unwrap()
            .unwrap();
        assert_eq!((cursor, epoch), (0, 3), "reset preserves room generation");

        let update = doc
            .doc()
            .export(loro::ExportMode::updates(&loro::VersionVector::default()))
            .unwrap();
        sink.apply_row(&update, 1);
        sink.advance_cursor(1);
        let (_, cursor, epoch) = store
            .load_snapshot_with_cursor("reset-chat")
            .unwrap()
            .unwrap();
        assert_eq!(
            (cursor, epoch),
            (1, 3),
            "post-reset row/ack must not be masked by the old cursor 41"
        );
    }
}
