// chat2/chat3 room client — a Swift port of crates/sync/src/chat_client.rs,
// the binary-frame sibling of RegistryClient. One WebSocket per chat to
// /{chat2|chat3}/{chatId}/ws: hello/state handshake with client-side checkpoint
// precision (the frontier payload decides fetch-vs-skip), cursor-based row
// backfill, push/ack with a pending-unacked batch queue, probe/redial
// liveness, reconnect with exponential backoff.
//
// The client owns no CRDT semantics: update bytes flow through main-actor
// delegate closures into SessionStore's doc, which persists doc content AND
// the room cursor in one file write (the C2 rule — they can never diverge).
//
// Liveness discipline is inherited from the registry client and its
// incidents: the text "ping" elicits a runtime auto-pong that proves NOTHING
// about the DO, so room health is judged only by protocol frames against
// hard deadlines.

import Foundation
import os

/// Sync must never fail silently (2026-07-31: a send that never left the
/// device was indistinguishable from a working one — `try?` all the way
/// down). Visible in Console.app / `log stream` under this subsystem.
let roomLog = Logger(subsystem: "sh.zeron.ios", category: "sync")

enum ChatRoomEvent: Sendable {
    /// Joined (or re-joined) and the initial catch-up (checkpoint if needed +
    /// row backfill) has been delivered through the apply closures.
    case connected
    /// The connection dropped; the client is backing off before redialing.
    case disconnected
}

actor ChatRoomClient {
    // Constants mirrored from crates/sync/src/chat_client.rs.
    static let pingIntervalNs: UInt64 = 15_000_000_000
    static let silenceLeaseNs: UInt64 = 45_000_000_000
    static let helloDeadlineNs: UInt64 = 15_000_000_000
    /// Checkpoint fetch + row backfill must complete within this deadline —
    /// post-strip docs are KB-scale, so this is generous even at 1.2 Mbps.
    static let backfillDeadlineNs: UInt64 = 120_000_000_000
    static let probeDeadlineNs: UInt64 = 10_000_000_000
    static let probeQuietNs: UInt64 = 900_000_000_000  // 15min quiet-room probe
    static let livenessTickNs: UInt64 = 1_000_000_000
    static let backoffBaseMs = 250
    static let backoffCapMs = 16_000
    /// Stability-gated backoff reset (chat_client.rs STABLE_RESET): only a
    /// session that joined AND survived this long resets backoff to base —
    /// reset-on-join let connect-and-die sockets hot-loop at 250ms forever.
    static let stableResetNs: UInt64 = 30_000_000_000
    /// Re-push cadence after a `quota` rejection (server window is 60s).
    static let quotaRetryNs: UInt64 = 5_000_000_000
    /// Client-side push cap: the DO's per-row cap (1 MiB) minus frame-header
    /// headroom — the runtime closes WS messages at 1 MiB BEFORE the DO runs,
    /// so an over-cap payload would die with no error frame to retire it.
    static let maxPushBytes = 1024 * 1024 - 4096

    /// MainActor-isolated bridge to SessionStore's doc. Every apply persists
    /// doc + cursor together. An apply closure returns false when Loro accepted
    /// the bytes but left causal dependencies pending; the client re-anchors
    /// and redials rather than advancing a cursor past unmaterialized ops.
    struct Delegate: Sendable {
        var cursor: @MainActor @Sendable () -> UInt64
        var containsFrontier: @MainActor @Sendable (Data) -> Bool
        var applyCheckpoint: @MainActor @Sendable (Data, UInt64) -> Bool
        var applyRow: @MainActor @Sendable (Data, UInt64) -> Bool
        var advanceCursor: @MainActor @Sendable (UInt64) -> Void
        /// Cursor amnesty: lower the cursor to the checkpoint seq (no-op if
        /// already at or below). See the once-per-session clamp in
        /// handleState/pullSync.
        var clampCursor: @MainActor @Sendable (UInt64) -> Void
        /// Assign the cursor outright — the catch-up plan's `after` IS the
        /// cursor, down (server reset / amnesty) or UP (a contained
        /// checkpoint covers the skipped span; without the raise the
        /// backfill's first row reads as a contiguity gap).
        var setCursor: @MainActor @Sendable (UInt64) -> Void
        var event: @MainActor @Sendable (ChatRoomEvent) -> Void
    }

    private struct PendingPush {
        var batchId: String
        var bytes: Data
        /// In flight on the CURRENT connection (cleared on disconnect so
        /// reconnects re-push; the server dedupes replays by batchId).
        var inFlight = false
        /// Reset version of the socket that sent this batch. An ACK from an
        /// older room incarnation must not retire it.
        var sentVersion: UInt64? = nil
    }

    /// ACK retirement is reversible until a later authoritative state/pull
    /// confirms the same room incarnation. This preserves the update bytes if
    /// a reset is discovered immediately after an ACK advanced the cursor.
    private struct AcknowledgedPush {
        var push: PendingPush
        var seq: UInt64
        var version: UInt64
    }

    private let chatId: String
    private let device: String
    private let urlProvider: @Sendable () async -> URL?
    private let checkpointRequest: @Sendable () async -> URLRequest?
    /// GET /{chat2|chat3}/{id}/rows?after= — the HTTPS pull twin of the backfill.
    private let rowsRequest: @Sendable (UInt64) async -> URLRequest?
    /// POST /{chat2|chat3}/{id}/rows?batchId= — the HTTPS push twin.
    private let pushRequest: @Sendable (String) async -> URLRequest?
    private let delegate: Delegate

    private var socket: URLSessionWebSocketTask?
    private var pullTask: Task<Void, Never>?
    private var receiveTask: Task<Void, Never>?
    private var pingTask: Task<Void, Never>?
    private var livenessTask: Task<Void, Never>?
    private var quotaTask: Task<Void, Never>?
    private var pending: [PendingPush] = []
    private var acknowledged: [AcknowledgedPush] = []
    /// Permanent payload verdicts must survive reset reconciliation; restoring
    /// one would create a replay-forever wedge.
    private var permanentlyRejected = Set<String>()
    /// Monotonic client-side room-incarnation fence. Unlike socket generation,
    /// this also invalidates HTTP work racing a server reset.
    private var resetVersion: UInt64 = 0
    /// State frame received on the CURRENT socket — pushes are legal from
    /// here (the server accepts them any time post-hello; batchId dedupe).
    private var stateReceived = false
    /// Non-nil while a checkpoint downloads in parallel with the row
    /// backfill: row/rowsDone frames land here and replay after the import.
    private var checkpointBuffer: [ChatWireFrame]?
    /// One checkpoint download at a time, and it OUTLIVES socket redials: on
    /// a saturated link the WS silence lease can trip mid-download (pongs
    /// queue behind the blob), and discarding the bytes on every redial
    /// looped a fresh-chat open forever (NLC Edge, 2026-08-17).
    private var fetchInFlight = false
    /// Once-per-client cursor amnesty (see handleState): a cursor above the
    /// room's checkpoint is re-verified by refetching the rows above it.
    private var cursorAmnestyDone = false
    /// A row/ack arrived with `seq > cursor + 1`: rows exist that this client
    /// never received (live broadcast outran the backfill — the mid-join
    /// race). The cursor must NOT skip the hole (skipped rows' dependents
    /// park invisibly in loro's pending buffer and the doc reads empty
    /// forever — the 2026-08-19 empty-doc/advanced-cursor wedge); the flag
    /// asks for a rowsReq backfill from the honest cursor.
    private var gapRepair = false
    /// Repairs this SESSION — exhaustion forces a redial (full catch-up).
    private var gapRepairs = 0
    /// Partial download preserved across fetch attempts (Range-resumed).
    private var partialCheckpoint = Data()
    private var partialCheckpointSeq: String?
    /// Bytes moved on the checkpoint stream recently — download progress IS
    /// liveness while it runs; the silence lease defers to it.
    private var checkpointProgressAt: DispatchTime?
    /// Safe replay anchor advertised by the STATE for the current socket.
    /// It is version-bound and cleared before every dial; a missing-dependency
    /// row must never recover from a stale room incarnation's checkpoint.
    private var socketRecoveryAnchor: (version: UInt64, cursor: UInt64)?
    private var joined = false
    /// When the current session joined — feeds the stability-gated reset.
    private var joinedAt: DispatchTime?
    private var closed = false
    private var generation = 0
    private var backoffMs = ChatRoomClient.backoffBaseMs
    /// First backfill of THIS client instance must NOT exclude own rows: the
    /// pending queue doesn't survive restarts, and a restored device's doc
    /// may be missing its own post-backup writes — they exist only on the
    /// server. Loro re-import is a no-op, so redownloading own bytes once is
    /// pure safety; same-process reconnects skip them.
    private var resumed = false
    /// Transport clock — pongs count, so a healthy socket never trips it.
    private var lastInbound = DispatchTime.now()
    /// Protocol clock — only real frames count (pongs prove nothing).
    private var lastProtocolRx = DispatchTime.now()
    private var helloSentAt: DispatchTime?
    private var backfillStartedAt: DispatchTime?
    private var probeSentAt: DispatchTime?

    init(chatId: String,
         device: String,
         urlProvider: @escaping @Sendable () async -> URL?,
         checkpointRequest: @escaping @Sendable () async -> URLRequest?,
         rowsRequest: @escaping @Sendable (UInt64) async -> URLRequest?,
         pushRequest: @escaping @Sendable (String) async -> URLRequest?,
         delegate: Delegate) {
        self.chatId = chatId
        self.device = device
        self.urlProvider = urlProvider
        self.checkpointRequest = checkpointRequest
        self.rowsRequest = rowsRequest
        self.pushRequest = pushRequest
        self.delegate = delegate
    }

    // MARK: Lifecycle

    func start() {
        closed = false
        connect()
        // Pull-first bootstrap + poll-while-unjoined: one HTTPS GET catches
        // the doc up in ~1 RTT while the socket spends 4+ on TLS + upgrade +
        // hello — and on networks that strip WS upgrades (airplane wifi) the
        // 20s poll is the transport, delivering reads AND queued sends.
        pullTask?.cancel()
        pullTask = Task { [weak self] in
            await self?.pullSync()
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 20_000_000_000)
                guard let self, !Task.isCancelled else { return }
                if await self.shouldPoll() { await self.pullSync() }
            }
        }
    }

    /// One HTTPS sync cycle: flush pending batches (POST — batchId dedupe
    /// makes replays no-ops), then pull rows (GET) and apply them through the
    /// exact frame path the socket uses. The airplane-wifi transport.
    func pullSync() async {
        guard !closed else { return }
        let cycleVersion = resetVersion
        // This is the remote frontier known BEFORE any POST in this cycle.
        // Numeric reset detection keeps this value, while the later GET may
        // deliberately pull farther back to prove old/deduped ACK identities.
        let prePushCursor = await delegate.cursor()
        guard isCurrent(version: cycleVersion) else { return }
        let pushes = pending
        var stagedAcks: [ChatHTTPPushAck] = []
        // Push first, so a message typed on dead wifi leaves the device on
        // this cycle rather than the next.
        for push in pushes {
            guard var request = await pushRequest(push.batchId) else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http push skipped — no URL (token unavailable)")
                break
            }
            guard isCurrent(version: cycleVersion) else { return }
            request.httpBody = push.bytes
            guard let (data, response) = try? await URLSession.shared.data(for: request),
                  let http = response as? HTTPURLResponse else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http push transport error; will retry")
                break
            }
            guard isCurrent(version: cycleVersion) else { return }
            if http.statusCode == 200 {
                // No cursor movement here — a staged ack is committed only
                // after the GET below proves its (seq, batchId) identity and
                // the pulled rows (contiguity-checked) carry the cursor. The
                // socket ACK path's contiguity rule is intrinsic to this
                // proof machinery.
                guard let ack = chatDecodeHTTPPushAck(data, expectedBatchId: push.batchId) else {
                    roomLog.warning("chat2 \(self.chatId, privacy: .public): malformed/mismatched http push ack; will retry")
                    break
                }
                stagedAcks.append(ack)
            } else if let err = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let code = err["error"] as? String,
                      ["bad_push", "too_large", "empty"].contains(code) {
                // Permanent verdict — and provably OUR edge's (a parsed error
                // code, not a middlebox's arbitrary 4xx: captive portals
                // answer POSTs with junk statuses, and retiring a real
                // message batch on one would silently drop the send on
                // exactly the networks this path exists for). Retire, same
                // as the WS error-frame path; the ops stay in the local doc.
                roomLog.error("chat2 \(self.chatId, privacy: .public): http push rejected (\(code, privacy: .public)); retiring batch")
                permanentlyRetire(batchId: push.batchId)
            } else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http push http=\(http.statusCode); will retry")
                break  // quota/transient/middlebox: retry next cycle
            }
        }
        // Freeze the proof set immediately before dispatch. Pull from one row
        // before its earliest ACK so even an ACK already behind the pre-push
        // cursor has an exact `(seq, batchId)` identity witness. Journal ACKs
        // arriving after this snapshot deliberately wait for the next cycle.
        let proofJournalAcks = acknowledged
            .filter { $0.version == cycleVersion }
            .map { ChatHTTPPushAck(batchId: $0.push.batchId, seq: $0.seq) }
        let proofAcks = stagedAcks + proofJournalAcks
        let pullSince = chatHTTPPullSince(prePushCursor: prePushCursor,
                                          acknowledgements: proofAcks)
        // The edge caps GET /rows bodies (ROWS_BODY_CAP) and omits rowsDone on
        // a truncated page. Stitch pages here — advancing `after` to the last
        // row of each truncated page — so everything below (reset detection,
        // the frontier proof) still sees one atomic response whose STATE and
        // headSeq come from the FINAL page. A truncated page that fails to
        // advance the cursor would refetch itself forever, so progress is
        // mandatory, and the page count is bounded.
        var stitched: [ChatHTTPPullRow] = []
        var complete: ChatHTTPPullBatch?
        var after = pullSince
        for _ in 0..<32 {
            guard let request = await rowsRequest(after) else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http pull skipped — no URL (token unavailable)")
                return
            }
            guard isCurrent(version: cycleVersion) else { return }
            let fetched = try? await URLSession.shared.data(for: request)
            guard let (body, response) = fetched, let http = response as? HTTPURLResponse else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http pull transport error; will retry")
                return
            }
            guard isCurrent(version: cycleVersion) else { return }
            guard http.statusCode == 200 else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http pull http=\(http.statusCode); will retry")
                return
            }
            guard let (page, truncated) = chatDecodeHTTPPullPage(body) else {
                roomLog.error("chat2 \(self.chatId, privacy: .public): http pull body malformed (\(body.count)B)")
                return
            }
            if truncated {
                guard let lastSeq = page.rows.last?.seq, lastSeq > after else {
                    roomLog.error("chat2 \(self.chatId, privacy: .public): http pull truncated page made no progress (after=\(after)); will retry")
                    return
                }
                stitched.append(contentsOf: page.rows)
                after = lastSeq
                continue
            }
            complete = ChatHTTPPullBatch(stateFrame: page.stateFrame, state: page.state,
                                         rows: stitched + page.rows, headSeq: page.headSeq)
            break
        }
        guard let pull = complete else {
            roomLog.error("chat2 \(self.chatId, privacy: .public): http pull still truncated after 32 pages; will retry")
            return
        }
        let stateFrame = pull.stateFrame
        let state = pull.state

        // A reset between POST and GET invalidates every ack from this round:
        // its row may have belonged to the lost incarnation. Include WS ACKs
        // that raced this HTTP cycle: a failed POST does not make a concurrent
        // socket retirement safe. Lower the cursor, re-arm socket-local
        // in-flight flags, and leave all batches pending.
        if let resetCursor = chatHTTPResetCursor(capturedCursor: prePushCursor,
                                                 state: state,
                                                 stagedAcks: proofAcks) {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): server reset during http sync (headSeq=\(state.headSeq), captured=\(prePushCursor)); keeping pushes and resetting cursor to \(resetCursor)")
            await confirmReset(cursor: resetCursor, snapshot: pushes,
                               observedVersion: cycleVersion)
            return
        }
        // This response's STATE is the only recovery authority for its rows.
        // Do not borrow the concurrently dialing socket's checkpoint anchor.
        let recoveryCursor = chatResetAnchor(state: state)
        guard chatHTTPPullCoversFrontier(capturedCursor: pullSince, pull: pull) else {
            roomLog.error("chat2 \(self.chatId, privacy: .public): http pull has a row gap or inconsistent frontier; keeping pushes")
            return
        }
        if !cursorAmnestyDone {
            cursorAmnestyDone = true
            // Checkpoint-LESS rooms amnesty to ZERO (PR #172) — see the twin
            // clamp in handleState.
            let amnestyFloor = state.checkpointSize > 0 ? state.checkpointSeq : 0
            await delegate.clampCursor(amnestyFloor)
            guard isCurrent(version: cycleVersion) else { return }
            // This response was requested above the newly lowered cursor, so
            // it cannot contain the amnesty gap. Keep acks and let the next
            // cycle fetch from the checkpoint (or row zero) instead of
            // certifying a hole.
            if pullSince > amnestyFloor {
                roomLog.info("chat2 \(self.chatId, privacy: .public): http pull cursor amnesty requires a refetch; keeping pushes")
                return
            }
        }
        let planAfter = await delegate.cursor()
        guard isCurrent(version: cycleVersion) else { return }
        var contained = state.checkpointSize == 0
        if !contained {
            contained = await delegate.containsFrontier(stateFrame.payload)
            guard isCurrent(version: cycleVersion) else { return }
        }
        if case .checkpointThenRows = chatPlanCatchUp(cursor: planAfter, state: state,
                                                      frontierContained: contained) {
            // The local doc lacks the checkpoint's frontier. Route through
            // completeCheckpointFetch — NOT an inline fetch: the socket
            // handshake may race this pull, see fetchInFlight set, skip its
            // own fetch, and arm checkpointBuffer expecting the completion
            // path to drain it. An inline fetch satisfied the guard but
            // never drained, stranding every socket row in the buffer until
            // the backfill deadline ("chat frozen", 2026-08-18).
            guard !fetchInFlight else { return }  // socket's fetch owns it
            fetchInFlight = true
            guard await completeCheckpointFetch(
                seq: state.checkpointSeq, expectedVersion: cycleVersion) else {
                return
            }
            guard isCurrent(version: cycleVersion) else { return }
            let fetchedFrontierContained = await delegate.containsFrontier(stateFrame.payload)
            guard fetchedFrontierContained else {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): http pull — frontier still missing after checkpoint; will retry")
                return
            }
            guard isCurrent(version: cycleVersion) else { return }
        }
        if chatAcknowledgementsRequireReset(
            pullSince: pullSince, pull: pull,
            acknowledgements: proofAcks) {
            let resetCursor = chatResetAnchor(state: state)
            roomLog.warning("chat2 \(self.chatId, privacy: .public): http pull cannot prove ACK batch identity; keeping pushes and resetting cursor to \(resetCursor)")
            await confirmReset(cursor: resetCursor, snapshot: pushes,
                               observedVersion: cycleVersion)
            return
        }
        guard isCurrent(version: cycleVersion) else { return }
        for row in pull.rows {
            guard isCurrent(version: cycleVersion) else { return }
            guard await applyRowFrame(row.frame, recoveryCursor: recoveryCursor,
                                      observedVersion: cycleVersion),
                  isCurrent(version: cycleVersion) else { return }
        }
        let appliedCursor = await delegate.cursor()
        guard isCurrent(version: cycleVersion) else { return }
        let pullApplied = appliedCursor >= pull.headSeq
        guard pullApplied else {
            roomLog.error("chat2 \(self.chatId, privacy: .public): http pull apply stopped below headSeq=\(pull.headSeq); keeping pushes")
            return
        }

        // Commit exactly the fixed pre-dispatch proof set. This block has no
        // awaits, so retiring staged pushes and pruning the matching journal
        // tuples is one actor-isolated transition. A post-dispatch ACK with a
        // new tuple remains for the next poll; a late WS copy of a staged tuple
        // is cleared because that exact tuple was proven by this pull.
        let provenAcks = chatHTTPProvenAcknowledgements(
            pullSince: pullSince, pull: pull, acknowledgements: proofAcks)
        for ack in stagedAcks {
            guard chatHTTPPushDisposition(prePushCursor: prePushCursor,
                                          pullSince: pullSince, pull: pull,
                                          pullApplied: true,
                                          ack: ack) == .retire else {
                for ix in pending.indices where pending[ix].batchId == ack.batchId {
                    pending[ix].inFlight = false
                    pending[ix].sentVersion = nil
                }
                continue
            }
            pending.removeAll { $0.batchId == ack.batchId }
        }
        let currentJournalAcks = acknowledged
            .filter { $0.version == cycleVersion }
            .map { ChatHTTPPushAck(batchId: $0.push.batchId, seq: $0.seq) }
        let retainedJournalAcks = Set(chatHTTPRetainedAcknowledgements(
            current: currentJournalAcks, proven: provenAcks))
        acknowledged.removeAll { item in
            item.version == cycleVersion
                && !retainedJournalAcks.contains(
                    ChatHTTPPushAck(batchId: item.push.batchId, seq: item.seq))
        }
    }

    private func isCurrent(version: UInt64) -> Bool {
        !closed && chatSessionVersionIsCurrent(version, currentVersion: resetVersion)
    }

    private func rememberAcknowledged(_ push: PendingPush, seq: UInt64,
                                      version: UInt64) {
        acknowledged.removeAll { $0.push.batchId == push.batchId }
        var retired = push
        retired.inFlight = false
        retired.sentVersion = nil
        acknowledged.append(AcknowledgedPush(push: retired, seq: seq, version: version))
    }

    private func permanentlyRetire(batchId: String) {
        guard !batchId.isEmpty else { return }
        permanentlyRejected.insert(batchId)
        pending.removeAll { $0.batchId == batchId }
        acknowledged.removeAll { $0.push.batchId == batchId }
    }

    /// Cut over to a new client-side room incarnation before the cursor await.
    /// That ordering makes every old HTTP response and WS frame fail its token
    /// guard while reset reconciliation is in progress.
    private func confirmReset(cursor: UInt64, snapshot: [PendingPush],
                              observedVersion: UInt64) async {
        guard isCurrent(version: observedVersion) else { return }
        resetVersion &+= 1
        let newVersion = resetVersion

        let ackedPushes = acknowledged
            .filter { $0.version <= observedVersion }
            .map(\.push)
        let order = chatResetReplayOrder(
            acknowledgedBatchIds: ackedPushes.map(\.batchId),
            snapshotBatchIds: snapshot.map(\.batchId),
            pendingBatchIds: pending.map(\.batchId),
            permanentlyRejected: permanentlyRejected)
        var byId: [String: PendingPush] = [:]
        for push in ackedPushes + snapshot + pending
        where !permanentlyRejected.contains(push.batchId) && byId[push.batchId] == nil {
            var replay = push
            replay.inFlight = false
            replay.sentVersion = nil
            byId[push.batchId] = replay
        }
        pending = order.compactMap { byId[$0] }
        acknowledged.removeAll()
        cursorAmnestyDone = false
        stateReceived = false
        joined = false
        socketRecoveryAnchor = nil
        checkpointBuffer = nil
        partialCheckpoint.removeAll()
        partialCheckpointSeq = nil

        await delegate.clampCursor(cursor)
        guard isCurrent(version: newVersion) else { return }
        restartSocketAfterReset()
    }

    private func restartSocketAfterReset() {
        guard !closed else { return }
        // Invalidate the receive loop synchronously; connect() assigns the
        // replacement generation after the old task/socket are cancelled.
        generation += 1
        cancelTasks()
        socket?.cancel(with: .goingAway, reason: nil)
        socket = nil
        connect()
    }

    private func shouldPoll() -> Bool {
        !closed && (!joined || !acknowledged.isEmpty)
    }

    func stop() {
        closed = true
        generation += 1
        cancelTasks()
        pullTask?.cancel()
        pullTask = nil
        socket?.cancel(with: .goingAway, reason: nil)
        socket = nil
        joined = false
        socketRecoveryAnchor = nil
    }

    /// Queue one local update batch for push. The batch survives reconnects
    /// until acked — the server dedupes replays by batchId.
    ///
    /// Batches over `maxPushBytes` are refused here: the server can never
    /// accept them, and a queued-forever batch would replay on every
    /// reconnect — the exact wedge class chat2 replaces. The ops stay in the
    /// local doc; on a viewer device there is no checkpoint fallback, so
    /// this is loud (post-strip updates are KB-scale — an over-cap update is
    /// an upstream bug).
    func enqueue(update: Data) {
        guard update.count <= ChatRoomClient.maxPushBytes else {
            roomLog.error("chat2 \(self.chatId, privacy: .public): update \(update.count)B exceeds the row cap; not queued")
            return
        }
        pending.append(PendingPush(batchId: UUID().uuidString.lowercased(), bytes: update))
        if stateReceived {
            Task { await self.pushPending() }
        }
    }

    /// Foreground hook (the chat2 twin of RegistryClient.kick): suspension
    /// kills the socket without running any failure path. A dead or unjoined
    /// session redials NOW on fresh backoff; a joined one gets an immediate
    /// deadline-checked probe (post-suspend sockets are half-open more often
    /// than not).
    func kick() async {
        guard !closed else { return }
        backoffMs = ChatRoomClient.backoffBaseMs
        if socket == nil {
            connect()
            return
        }
        if !joined {
            // A handshake/catch-up is already in flight on this socket — its
            // own deadlines police it (hello 15s, backfill 120s). Redialing
            // here abandoned a healthy catch-up and refetched the checkpoint
            // (observed on launch: foregrounded() fires during the first
            // join). A zombie socket with NO handshake pending is redialed.
            if helloSentAt == nil, backfillStartedAt == nil {
                connect()
            }
            return
        }
        guard probeSentAt == nil else { return }
        await sendProbe()
    }

    private func cancelTasks() {
        receiveTask?.cancel()
        pingTask?.cancel()
        livenessTask?.cancel()
        quotaTask?.cancel()
    }

    private func connect() {
        guard !closed else { return }
        generation += 1
        let gen = generation
        let sessionVersion = resetVersion
        joined = false
        stateReceived = false
        socketRecoveryAnchor = nil
        checkpointBuffer = nil
        helloSentAt = nil
        backfillStartedAt = nil
        probeSentAt = nil
        gapRepair = false
        gapRepairs = 0
        lastProtocolRx = .now()
        for ix in pending.indices {
            pending[ix].inFlight = false
            pending[ix].sentVersion = nil
        }

        Task {
            guard let url = await urlProvider() else {
                // No URL = no token (refresh failed or signed out) — the most
                // confusing silent failure: everything cached renders,
                // nothing syncs. Say so and back off.
                roomLog.error("chat2 \(self.chatId, privacy: .public): no socket URL (token unavailable); backing off")
                self.scheduleReconnect(gen: gen)
                return
            }
            guard await self.isCurrentSocket(gen: gen, version: sessionVersion) else { return }
            await self.openSocket(url: url, gen: gen, sessionVersion: sessionVersion)
        }
    }

    private func openSocket(url: URL, gen: Int, sessionVersion: UInt64) async {
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        let task = URLSession.shared.webSocketTask(with: url)
        socket = task
        task.resume()
        lastInbound = .now()
        lastProtocolRx = .now()

        receiveTask = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                guard let sock = await self.currentSocket(gen: gen,
                                                          version: sessionVersion) else { return }
                do {
                    let message = try await sock.receive()
                    await self.handleInbound(message, gen: gen,
                                             sessionVersion: sessionVersion)
                } catch {
                    await self.onSocketError(gen: gen)
                    return
                }
            }
        }

        pingTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: ChatRoomClient.pingIntervalNs)
                guard let self else { return }
                await self.pingTick(gen: gen)
            }
        }

        livenessTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: ChatRoomClient.livenessTickNs)
                guard let self else { return }
                await self.livenessTick(gen: gen)
            }
        }

        // Hello with the persisted cursor. The deadline is armed BEFORE the
        // send — an unanswered hello must never hang the session.
        helloSentAt = .now()
        let cursor = await delegate.cursor()
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        await send(ChatWire.encode(ChatFrameType.hello,
                                   header: ["cursor": cursor, "device": device]))
    }

    private func isCurrentSocket(gen: Int, version: UInt64) -> Bool {
        gen == generation && isCurrent(version: version)
    }

    private func currentSocket(gen: Int, version: UInt64) -> URLSessionWebSocketTask? {
        isCurrentSocket(gen: gen, version: version) ? socket : nil
    }

    private func onSocketError(gen: Int) async {
        guard gen == generation, !closed else { return }
        roomLog.warning("chat2 \(self.chatId, privacy: .public): session ended (joined=\(self.joined)); redialing in \(self.backoffMs)ms")
        joined = false
        await delegate.event(.disconnected)
        scheduleReconnect(gen: gen)
    }

    private func scheduleReconnect(gen: Int) {
        guard gen == generation, !closed else { return }
        socket?.cancel(with: .abnormalClosure, reason: nil)
        socket = nil
        cancelTasks()
        // Stability-gated reset: only a session that survived ≥30s earns a
        // fresh 250ms; a connect-and-die session keeps growing the backoff.
        if let joinedAt,
           DispatchTime.now().uptimeNanoseconds - joinedAt.uptimeNanoseconds
               >= ChatRoomClient.stableResetNs {
            backoffMs = ChatRoomClient.backoffBaseMs
        }
        joinedAt = nil
        let delay = backoffMs
        backoffMs = min(backoffMs * 2, ChatRoomClient.backoffCapMs)
        Task {
            // Parked while the OS path is offline; cut short by any online
            // event (a sibling dial success, path recovery, focus probe).
            await OnlineBus.shared.waitBackoff(ms: delay)
            self.connect()
        }
    }

    // MARK: Timers

    private func pingTick(gen: Int) async {
        guard gen == generation, let socket else { return }
        let silence = DispatchTime.now().uptimeNanoseconds - lastInbound.uptimeNanoseconds
        // A checkpoint download that is still moving bytes owns the pipe —
        // pongs queueing behind it are not a dead socket. The 120s backfill
        // deadline stays as the true backstop.
        let fetchMoving = checkpointProgressAt.map {
            DispatchTime.now().uptimeNanoseconds - $0.uptimeNanoseconds
                < ChatRoomClient.silenceLeaseNs
        } ?? false
        if silence > ChatRoomClient.silenceLeaseNs, !fetchMoving {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): socket silent past lease; treating as dead")
            await onSocketError(gen: gen)
            return
        }
        try? await socket.send(.string("ping"))
    }

    /// Hello answers, the catch-up, and probes run against hard deadlines; a
    /// long-quiet joined room gets a probe. Any protocol frame clears the
    /// probe deadline; catch-up progress (rows landing) feeds the protocol
    /// clock, so a draining backfill is never killed mid-stream.
    private func livenessTick(gen: Int) async {
        guard gen == generation, socket != nil, !closed else { return }
        let now = DispatchTime.now().uptimeNanoseconds
        if let sent = helloSentAt, now - sent.uptimeNanoseconds > ChatRoomClient.helloDeadlineNs {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): no state frame within deadline; room presumed wedged, redialing")
            await onSocketError(gen: gen)
            return
        }
        if let started = backfillStartedAt,
           now - started.uptimeNanoseconds > ChatRoomClient.backfillDeadlineNs {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): backfill did not complete within deadline; redialing")
            await onSocketError(gen: gen)
            return
        }
        if let sent = probeSentAt, now - sent.uptimeNanoseconds > ChatRoomClient.probeDeadlineNs {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): probe unanswered past deadline; redialing")
            await onSocketError(gen: gen)
            return
        }
        if joined, probeSentAt == nil,
           now - lastProtocolRx.uptimeNanoseconds > ChatRoomClient.probeQuietNs {
            await sendProbe()
            // Don't re-arm the quiet timer against the same silence.
            lastProtocolRx = .now()
        }
    }

    private func sendProbe() async {
        // Armed BEFORE the send suspends — the actor is reentrant across the
        // await, and the answer must find the deadline already set.
        probeSentAt = .now()
        await send(ChatWire.encode(ChatFrameType.probe, header: [:]))
    }

    // MARK: Inbound

    private func handleInbound(_ message: URLSessionWebSocketTask.Message, gen: Int,
                               sessionVersion: UInt64) async {
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        lastInbound = .now()
        guard case .data(let data) = message else { return }  // "pong" text
        guard let frame = ChatWire.decode(data) else {
            // Protocol breakdown — redial rather than run blind against a
            // server we can't parse.
            roomLog.error("chat2 \(self.chatId, privacy: .public): unparseable frame; redialing")
            await onSocketError(gen: gen)
            return
        }
        lastProtocolRx = .now()
        probeSentAt = nil

        switch frame.kind {
        case ChatFrameType.state:
            await handleState(frame, gen: gen, sessionVersion: sessionVersion)

        case ChatFrameType.row, ChatFrameType.rowsDone:
            // Rows landing while a checkpoint downloads in parallel wait for
            // the import (their seqs are all past the checkpoint; applying
            // them first would advance the cursor past state the doc doesn't
            // hold if the fetch then dies).
            if checkpointBuffer != nil {
                checkpointBuffer?.append(frame)
                return
            }
            let recoveryCursor = socketRecoveryAnchor.flatMap {
                $0.version == sessionVersion ? $0.cursor : nil
            } ?? 0
            _ = await applyRowFrame(frame, recoveryCursor: recoveryCursor,
                                    observedVersion: sessionVersion)
            // Self-guarded on generation/state — a confirmReset inside the
            // apply bumped the generation, so a stale repair is a no-op.
            await maybeRepairGap(gen: gen)

        case ChatFrameType.ack:
            guard let batchId = frame.header["batchId"] as? String,
                  let seq = (frame.header["seq"] as? NSNumber)?.uint64Value,
                  seq > 0,
                  let ix = pending.firstIndex(where: { $0.batchId == batchId }),
                  pending[ix].inFlight,
                  pending[ix].sentVersion == sessionVersion,
                  isCurrent(version: sessionVersion) else { return }
            let push = pending[ix]
            rememberAcknowledged(push, seq: seq, version: sessionVersion)
            pending.removeAll { $0.batchId == batchId }
            // Contiguity rule, ack flavor: our own batch landing at `seq`
            // proves rows up to seq exist SERVER-side, not that we have the
            // interleaved ones from other devices. Hold and backfill; the
            // acknowledged journal keeps the ack reversible either way.
            let cursor = await delegate.cursor()
            guard gen == generation, isCurrent(version: sessionVersion) else { return }
            if seq > cursor + 1 {
                gapRepair = true
                roomLog.warning("chat2 \(self.chatId, privacy: .public): ack gap (seq=\(seq), cursor=\(cursor)); holding cursor and requesting backfill")
            } else {
                await delegate.advanceCursor(seq)
                guard gen == generation, isCurrent(version: sessionVersion) else { return }
            }
            // A grant after a quota rejection: drain whatever the error
            // handler un-flagged (sparse mobile queues — no livelock risk).
            await pushPending()
            await maybeRepairGap(gen: gen)

        case ChatFrameType.probeOk:
            break  // liveness proven; clocks already advanced above

        case ChatFrameType.presence:
            break  // no consumer on mobile today

        case ChatFrameType.error:
            await handleErrorFrame(frame.header, gen: gen,
                                   sessionVersion: sessionVersion)

        default:
            // Unknown server frame: tolerate (future protocol additions).
            break
        }
    }

    /// The hello answer: plan the catch-up (chat_client.rs run_session).
    private func handleState(_ frame: ChatWireFrame, gen: Int,
                             sessionVersion: UInt64) async {
        guard let state = ChatStateHeader(frame.header) else {
            roomLog.error("chat2 \(self.chatId, privacy: .public): malformed state header; redialing")
            await onSocketError(gen: gen)
            return
        }
        guard helloSentAt != nil else { return }  // late duplicate — ignore
        helloSentAt = nil
        backfillStartedAt = .now()

        let cursor = await delegate.cursor()
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        if !acknowledged.isEmpty {
            // A reconnect state proves only a numeric head, not that an ACK's
            // batch still occupies its sequence after an ABA reset. Replay the
            // reversible journal once from the exact safe anchor; confirmReset
            // clears it before opening the replacement socket.
            let resetCursor = chatResetAnchor(state: state)
            roomLog.warning("chat2 \(self.chatId, privacy: .public): reconnect cannot prove ACK batch identity; replaying from \(resetCursor)")
            await confirmReset(cursor: resetCursor, snapshot: [],
                               observedVersion: sessionVersion)
            return
        }
        if let resetCursor = chatHTTPResetCursor(capturedCursor: cursor, state: state) {
            // Exact cursor reset is required: chatPlanCatchUp's local `0`
            // alone cannot lower SessionStore's persisted max cursor.
            roomLog.warning("chat2 \(self.chatId, privacy: .public): server lost state (headSeq=\(state.headSeq), cursor=\(cursor)); resetting cursor to \(resetCursor)")
            await confirmReset(cursor: resetCursor, snapshot: [],
                               observedVersion: sessionVersion)
            return
        }
        // Bind missing-dependency recovery to this exact STATE generation.
        // A room with no checkpoint must always replay from zero.
        socketRecoveryAnchor = (version: sessionVersion,
                                cursor: chatResetAnchor(state: state))
        // Same presence rule as chatPlanCatchUp: SIZE, not seq — a seeded
        // room's checkpoint covers seq 0.
        var contained = state.checkpointSize == 0
        if !contained {
            contained = await delegate.containsFrontier(frame.payload)
            guard gen == generation, isCurrent(version: sessionVersion) else { return }
        }
        // Cursor amnesty, once per client: a cursor above the checkpoint seq
        // claims history the doc may have silently parked and dropped (the
        // "Add Tweets" wedge). Clamp and refetch — no-op re-imports, KB cost,
        // converts a lying cursor into a true one. Checkpoint-LESS rooms
        // amnesty to ZERO (PR #172): the same parked-import wedge with no
        // checkpoint to clamp to; refetching the whole log is bounded by the
        // checkpoint threshold policy (a room past ~200 rows/512KB HAS a
        // checkpoint), so this is the cheap universal heal for every
        // already-wedged chat on disk.
        if !cursorAmnestyDone {
            cursorAmnestyDone = true
            await delegate.clampCursor(state.checkpointSize > 0 ? state.checkpointSeq : 0)
            guard gen == generation, isCurrent(version: sessionVersion) else { return }
        }
        let planCursor = await delegate.cursor()
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        let plan = chatPlanCatchUp(cursor: planCursor, state: state, frontierContained: contained)
        stateReceived = true
        let after: UInt64
        switch plan {
        case .rowsOnly(let a):
            after = a
        case .checkpointThenRows(let a):
            // Fetch in PARALLEL with the row backfill: the rows request goes
            // out below immediately, rows landing mid-download buffer (see
            // handleInbound), and the import + replay happen when the bytes
            // arrive. On a thin link the checkpoint download IS the join
            // time; serializing download → rows pushed "joined" back by the
            // whole blob.
            roomLog.info("chat2 \(self.chatId, privacy: .public): fetching checkpoint (seq=\(state.checkpointSeq), \(state.checkpointSize)B, rows in parallel)")
            checkpointBuffer = []
            // One download per chat, and it survives redials: a new session
            // that still needs the checkpoint just re-arms the buffer and
            // waits for the in-flight fetch to land.
            if !fetchInFlight {
                fetchInFlight = true
                let seq = state.checkpointSeq
                Task {
                    _ = await self.completeCheckpointFetch(
                        seq: seq, expectedVersion: sessionVersion)
                }
            }
            after = a
        }
        // The plan's `after` IS the cursor now — down (server reset /
        // amnesty already applied) or UP (a contained checkpoint covers the
        // skipped span). Without the raise, the backfill's first row
        // (`after + 1`) reads as a contiguity gap against a stale cursor.
        await delegate.setCursor(after)
        await send(ChatWire.encode(ChatFrameType.rowsReq,
                                   header: ["after": after, "excludeOwn": resumed]))
        // Pending pushes go before catch-up completes: batchId dedupe makes
        // replays no-ops and rows are CRDT-commutative, so a message written
        // offline flushes ~2 RTTs after the socket lands instead of waiting
        // out the whole checkpoint + backfill.
        await pushPending()
    }

    /// Second half of the parallel checkpoint fetch: import the blob, then
    /// replay the rows that buffered while it downloaded. Deliberately NOT
    /// socket-generation-guarded on the apply: reconnects within one room
    /// incarnation may reuse it, but a reset-version change must discard it.
    private func completeCheckpointFetch(seq: UInt64,
                                         expectedVersion: UInt64) async -> Bool {
        let bytes = await fetchCheckpoint()
        fetchInFlight = false
        checkpointProgressAt = nil
        guard isCurrent(version: expectedVersion) else {
            // fetchCheckpoint may have saved pre-reset partial bytes after the
            // reset handler cleared state; never splice them into the new room.
            partialCheckpoint.removeAll()
            partialCheckpointSeq = nil
            if checkpointBuffer != nil {
                checkpointBuffer = nil
                restartSocketAfterReset()
            }
            return false
        }
        guard !closed else { return false }
        guard let bytes else {
            // Redial only if a session is actually waiting on this blob
            // (buffer armed) — a stale failure must not kill a healthy
            // RowsOnly session that no longer needs it.
            if checkpointBuffer != nil {
                roomLog.warning("chat2 \(self.chatId, privacy: .public): checkpoint fetch failed (partial kept); redialing")
                checkpointBuffer = nil
                await onSocketError(gen: generation)
            }
            return false
        }
        guard await delegate.applyCheckpoint(bytes, seq), !closed else {
            if !closed {
                roomLog.error("chat2 \(self.chatId, privacy: .public): checkpoint import failed; redialing")
                checkpointBuffer = nil
                await onSocketError(gen: generation)
            }
            return false
        }
        guard isCurrent(version: expectedVersion) else { return false }
        let recoveryCursor = socketRecoveryAnchor.flatMap {
            $0.version == expectedVersion ? $0.cursor : nil
        } ?? 0
        while let frame = checkpointBuffer?.first {
            checkpointBuffer?.removeFirst()
            guard await applyRowFrame(frame, recoveryCursor: recoveryCursor,
                                      observedVersion: expectedVersion),
                  isCurrent(version: expectedVersion) else { return false }
        }
        checkpointBuffer = nil
        await maybeRepairGap(gen: generation)
        return true
    }

    /// One backfill/broadcast frame: a row, or the ROWS_DONE terminator.
    /// Factored out so frames buffered during a parallel checkpoint fetch
    /// replay through exactly the live path.
    @discardableResult
    private func applyRowFrame(_ frame: ChatWireFrame, recoveryCursor: UInt64,
                               observedVersion: UInt64) async -> Bool {
        if frame.kind == ChatFrameType.row {
            guard let seq = (frame.header["seq"] as? NSNumber)?.uint64Value else { return true }
            // Own-device rows can still arrive (first-backfill redownload, a
            // racing second socket) — Loro re-import is a no-op; the cursor
            // advance is what matters.
            //
            // CONTIGUITY RULE (PR #172): the cursor claims "every row ≤
            // cursor is reflected in the doc", so it may only walk, never
            // jump. A gap means rows we never received (live broadcast
            // mid-join) — apply the bytes (loro parks dependents
            // harmlessly), hold the honest cursor, and ask for a bounded
            // backfill repair (maybeRepairGap; redial when exhausted) —
            // the cheap first-line fix. Skipping the hole was the random
            // new-session forever-hang: an empty doc under an advanced
            // cursor.
            let cursor = await delegate.cursor()
            guard isCurrent(version: observedVersion) else { return false }
            if seq > cursor + 1 {
                gapRepair = true
                roomLog.warning("chat2 \(self.chatId, privacy: .public): row gap (seq=\(seq), cursor=\(cursor)); holding cursor and requesting backfill")
                // A parked import is EXPECTED across a known gap — the
                // repair refetches the missing span; no reset yet.
                _ = await delegate.applyRow(frame.payload, cursor)
                return isCurrent(version: observedVersion)
            }
            guard await delegate.applyRow(frame.payload, seq) else {
                // A CONTIGUOUS row can still park: a Loro import can succeed
                // while leaving the update causally pending, and its missing
                // dependency is at or below the cursor — no forward backfill
                // can supply it. Invalidate this session and recover from
                // the checkpoint anchor before ROWS_DONE/HTTP ACK logic can
                // claim convergence.
                guard isCurrent(version: observedVersion) else { return false }
                roomLog.warning("chat2 \(self.chatId, privacy: .public): row \(seq) is causally pending; recovering from cursor \(recoveryCursor)")
                await confirmReset(cursor: recoveryCursor, snapshot: [],
                                   observedVersion: observedVersion)
                return false
            }
            return isCurrent(version: observedVersion)
        }
        guard isCurrent(version: observedVersion), backfillStartedAt != nil else {
            return false
        }
        backfillStartedAt = nil
        let wasResumed = resumed
        resumed = true
        joined = true
        joinedAt = .now()
        roomLog.info("chat2 \(self.chatId, privacy: .public): joined (converged, resumed=\(wasResumed))")
        // One recovered socket un-parks the whole fleet's backoffs.
        OnlineBus.shared.notifyOnline()
        await delegate.event(.connected)
        // Anything not yet in flight goes now — the server's batchId dedupe
        // makes replays exact no-ops.
        await pushPending()
        return isCurrent(version: observedVersion)
    }

    /// If a row/ack gap was flagged, request a backfill from the honest
    /// cursor. Bounded per session: a gap the server can't fill (should be
    /// impossible below the checkpoint floor) forces a redial, whose full
    /// catch-up is the stronger repair. The HTTP pull path needs no send —
    /// its next cycle re-requests from the held cursor.
    private func maybeRepairGap(gen: Int) async {
        guard gapRepair, gen == generation, socket != nil, stateReceived else { return }
        gapRepair = false
        gapRepairs += 1
        guard gapRepairs <= 3 else {
            roomLog.warning("chat2 \(self.chatId, privacy: .public): gap repairs exhausted; redialing for a full catch-up")
            await onSocketError(gen: gen)
            return
        }
        let after = await delegate.cursor()
        roomLog.info("chat2 \(self.chatId, privacy: .public): backfilling over a row gap (after=\(after), attempt \(self.gapRepairs))")
        await send(ChatWire.encode(ChatFrameType.rowsReq,
                                   header: ["after": after, "excludeOwn": false]))
    }

    private func handleErrorFrame(_ header: [String: Any], gen: Int,
                                  sessionVersion: UInt64) async {
        guard gen == generation, isCurrent(version: sessionVersion) else { return }
        let code = header["code"] as? String ?? "?"
        let message = header["message"] as? String ?? ""
        let batchId = header["batchId"] as? String ?? ""
        roomLog.error("chat2 \(self.chatId, privacy: .public): server rejected a frame: \(code, privacy: .public): \(message, privacy: .public)")
        switch code {
        case "too_large", "empty", "bad_push":
            // Permanent verdicts on a specific batch: retire it, or it
            // replays on every reconnect forever — the wedge class this
            // design exists to kill. The ops stay in the local doc.
            if !batchId.isEmpty {
                permanentlyRetire(batchId: batchId)
            }
        case "quota":
            // Transient: the window passes on its own — keep the batch
            // queued and probe with the HEAD batch on a short clock
            // (one-per-grant; a full-queue replay would itself consume the
            // server's quota window).
            for ix in pending.indices {
                pending[ix].inFlight = false
                pending[ix].sentVersion = nil
            }
            quotaTask?.cancel()
            quotaTask = Task { [weak self] in
                try? await Task.sleep(nanoseconds: ChatRoomClient.quotaRetryNs)
                guard !Task.isCancelled, let self else { return }
                await self.pushHead(gen: gen)
            }
        case "hello_first":
            // Session state desynced from the server — start over.
            await onSocketError(gen: gen)
        default:
            break
        }
    }

    // MARK: Outbound

    private func pushPending() async {
        guard stateReceived else { return }
        let version = resetVersion
        let gen = generation
        let batchIds = pending.filter { !$0.inFlight }.map(\.batchId)
        for batchId in batchIds {
            guard gen == generation, isCurrent(version: version), stateReceived,
                  let ix = pending.firstIndex(where: { $0.batchId == batchId }),
                  !pending[ix].inFlight else { return }
            pending[ix].inFlight = true
            pending[ix].sentVersion = version
            let push = pending[ix]
            await send(ChatWire.encode(ChatFrameType.push,
                                       header: ["batchId": push.batchId],
                                       payload: push.bytes))
            guard gen == generation, isCurrent(version: version) else { return }
        }
    }

    /// Send only the queue's head batch — the quota-probe path.
    private func pushHead(gen: Int) async {
        guard gen == generation, joined, let head = pending.first else { return }
        pending[0].inFlight = true
        pending[0].sentVersion = resetVersion
        await send(ChatWire.encode(ChatFrameType.push,
                                   header: ["batchId": head.batchId],
                                   payload: head.bytes))
    }

    private func send(_ frame: Data) async {
        guard let socket else { return }
        try? await socket.send(.data(frame))
    }

    // MARK: Checkpoint fetch (GET /{chat2|chat3}/{chatId}/checkpoint, Range resume)

    /// Range-resume loop (chat2_host.rs EdgeCheckpointFetcher): each attempt
    /// continues at the byte where the last one stopped; the DO stamps every
    /// response with the checkpoint's seq, and a mid-download replacement
    /// restarts from 0 (a Range against a new blob would splice two blobs).
    private func fetchCheckpoint() async -> Data? {
        // Resume from any partial a previous attempt (even a previous
        // SOCKET generation) left behind — starting a slow blob over from
        // zero on every hiccup is how a thin link never converges.
        var got = partialCheckpoint
        var seenSeq: String? = partialCheckpointSeq
        func keepPartial() {
            partialCheckpoint = got
            partialCheckpointSeq = seenSeq
        }
        for _ in 0..<4 {
            guard var request = await checkpointRequest() else {
                keepPartial()
                return nil
            }
            if !got.isEmpty {
                request.setValue("bytes=\(got.count)-", forHTTPHeaderField: "Range")
            }
            guard let (stream, response) = try? await URLSession.shared.bytes(for: request),
                  let http = response as? HTTPURLResponse else { continue }
            let seq = http.value(forHTTPHeaderField: "x-chat2-checkpoint-seq")
            if let seq {
                if let prev = seenSeq, seq != prev {
                    roomLog.info("chat2 \(self.chatId, privacy: .public): checkpoint replaced mid-download; restarting")
                    got.removeAll()
                    seenSeq = seq
                    continue
                }
                seenSeq = seq
            }
            switch http.statusCode {
            case 200: got.removeAll()
            case 206: break
            default:
                roomLog.warning("chat2 \(self.chatId, privacy: .public): checkpoint HTTP \(http.statusCode)")
                keepPartial()
                return nil
            }
            do {
                for try await byte in stream {
                    got.append(byte)
                    // Download progress is liveness (see pingTick) — stamp
                    // it every 8KB, not every byte.
                    if got.count & 0x1FFF == 0 {
                        checkpointProgressAt = .now()
                    }
                }
                partialCheckpoint = Data()
                partialCheckpointSeq = nil
                return got
            } catch {
                // Mid-body drop: keep the bytes, resume via Range.
                roomLog.warning("chat2 \(self.chatId, privacy: .public): checkpoint stream dropped at \(got.count)B; resuming")
            }
        }
        keepPartial()
        return nil
    }
}
