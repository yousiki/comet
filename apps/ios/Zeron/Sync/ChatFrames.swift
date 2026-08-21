// chat2 wire frames — the Swift twin of crates/sync/src/chat_frames.rs and
// edge/src/chat-frames.ts. Binary WS frames:
// `[type u8][headerLen u32 LE][header JSON][payload]`. Headers are tiny JSON;
// payloads are opaque bytes (Loro updates, the checkpoint frontier). The
// layout tests in ZeronTests pin the same vectors as the Rust/TS suites —
// change all three together.

import Foundation

/// Frame type bytes (shared client/server space; mirror `FRAME` in TS).
enum ChatFrameType {
    static let hello: UInt8 = 0x01
    static let state: UInt8 = 0x02
    static let rowsReq: UInt8 = 0x03
    static let row: UInt8 = 0x04
    static let rowsDone: UInt8 = 0x05
    static let push: UInt8 = 0x06
    static let ack: UInt8 = 0x07
    static let presence: UInt8 = 0x08
    static let probe: UInt8 = 0x09
    static let probeOk: UInt8 = 0x0a
    static let error: UInt8 = 0x0b
}

/// Headers are ids + a few integers; anything bigger is a peer bug.
let chatFrameMaxHeaderBytes = 4096

struct ChatWireFrame {
    var kind: UInt8
    var header: [String: Any]
    var payload: Data
}

enum ChatWire {
    static func encode(_ kind: UInt8, header: [String: Any], payload: Data = Data()) -> Data {
        // JSONSerialization never fails on the [String: JSON-primitive] maps
        // this file builds; sortedKeys keeps output stable for the tests.
        let headerData = (try? JSONSerialization.data(withJSONObject: header,
                                                      options: [.sortedKeys])) ?? Data("{}".utf8)
        var out = Data(capacity: 5 + headerData.count + payload.count)
        out.append(kind)
        var len = UInt32(headerData.count).littleEndian
        withUnsafeBytes(of: &len) { out.append(contentsOf: $0) }
        out.append(headerData)
        out.append(payload)
        return out
    }

    /// `nil` = malformed. Unknown type bytes are NOT rejected here (unlike
    /// the DO): the client tolerates future server frame types by skipping.
    static func decode(_ data: Data) -> ChatWireFrame? {
        guard data.count >= 5 else { return nil }
        let bytes = [UInt8](data.prefix(5))
        let kind = bytes[0]
        let headerLen = Int(UInt32(bytes[1]) | UInt32(bytes[2]) << 8
                            | UInt32(bytes[3]) << 16 | UInt32(bytes[4]) << 24)
        guard headerLen <= chatFrameMaxHeaderBytes, 5 + headerLen <= data.count else { return nil }
        let headerData = data.subdata(in: 5..<(5 + headerLen))
        guard let obj = try? JSONSerialization.jsonObject(with: headerData),
              let header = obj as? [String: Any] else { return nil }
        return ChatWireFrame(kind: kind, header: header,
                             payload: data.subdata(in: (5 + headerLen)..<data.count))
    }
}

// ── typed server headers (loose-dictionary reads; absent = 0) ───────────────

/// The hello answer: server log metadata only — the CLIENT plans catch-up.
struct ChatStateHeader: Equatable {
    var headSeq: UInt64
    var seqFloor: UInt64
    var checkpointSeq: UInt64
    var checkpointSize: UInt64
    var rowCount: UInt64
    var rowBytes: UInt64

    init?(_ header: [String: Any]) {
        func u64(_ key: String) -> UInt64 { (header[key] as? NSNumber)?.uint64Value ?? 0 }
        guard header["headSeq"] is NSNumber else { return nil }
        headSeq = u64("headSeq")
        seqFloor = u64("seqFloor")
        checkpointSeq = u64("checkpointSeq")
        checkpointSize = u64("checkpointSize")
        rowCount = u64("rowCount")
        rowBytes = u64("rowBytes")
    }
}

// ── catch-up planning (pure — the client-side precision rule) ───────────────

// ── HTTPS push/pull validation ──

/// A POST `/rows` acknowledgement. It stays staged until the following GET
/// proves that the remote frontier containing this sequence was applied.
struct ChatHTTPPushAck: Hashable {
    var batchId: String
    var seq: UInt64
}

private struct ChatHTTPPushAckBody: Decodable {
    var batchId: String
    var seq: UInt64
    var dup: Bool?
}

/// Decode an ack without NSNumber's lossy/coercing `uint64Value` behavior.
/// In particular, booleans, negative/fractional values and an ack for another
/// batch are protocol failures, not permission to retire local data.
func chatDecodeHTTPPushAck(_ data: Data, expectedBatchId: String) -> ChatHTTPPushAck? {
    guard !expectedBatchId.isEmpty,
          let body = try? JSONDecoder().decode(ChatHTTPPushAckBody.self, from: data),
          body.batchId == expectedBatchId, body.seq > 0 else { return nil }
    return ChatHTTPPushAck(batchId: body.batchId, seq: body.seq)
}

struct ChatHTTPPullRow {
    var frame: ChatWireFrame
    var seq: UInt64
    var batchId: String
}

/// A completely decoded GET `/rows` body. Construction is intentionally
/// strict: a truncated/unknown/skipped frame must never become evidence used
/// to retire a pending push.
struct ChatHTTPPullBatch {
    var stateFrame: ChatWireFrame
    var state: ChatStateHeader
    var rows: [ChatHTTPPullRow]
    var headSeq: UInt64
}

private struct ChatHTTPStateBody: Decodable {
    var headSeq: UInt64
    var seqFloor: UInt64
    var checkpointSeq: UInt64
    var checkpointSize: UInt64
    var rowCount: UInt64?
    var rowBytes: UInt64?
}

private struct ChatHTTPRowBody: Decodable {
    var seq: UInt64
    var device: String
    var batchId: String
}

private struct ChatHTTPRowsDoneBody: Decodable {
    var headSeq: UInt64
}

private func chatDecodeHeader<T: Decodable>(_ header: [String: Any], as: T.Type) -> T? {
    guard JSONSerialization.isValidJSONObject(header),
          let data = try? JSONSerialization.data(withJSONObject: header),
          let decoded = try? JSONDecoder().decode(T.self, from: data) else { return nil }
    return decoded
}

/// Decode the u32-LE length-prefixed HTTP response. Grammar: `state, row*,
/// rowsDone` for a complete page, or `state, row+` for one truncated by the
/// edge's ROWS_BODY_CAP (the cap breaks before the frame that would cross it
/// and MAX_ROW_BYTES < cap, so a truncated page always carries at least one
/// row — a bare `state` with no done is malformed). Rows must be strictly
/// increasing; completeness relative to the request cursor is checked by
/// `chatHTTPPullCoversFrontier` after truncated pages are stitched.
func chatDecodeHTTPPullPage(_ body: Data) -> (batch: ChatHTTPPullBatch, truncated: Bool)? {
    var frames: [ChatWireFrame] = []
    var offset = 0
    while offset < body.count {
        guard body.count - offset >= 4 else { return nil }
        let length = body.subdata(in: offset..<(offset + 4)).withUnsafeBytes {
            Int($0.loadUnaligned(as: UInt32.self).littleEndian)
        }
        offset += 4
        guard length >= 5, length <= body.count - offset,
              let frame = ChatWire.decode(body.subdata(in: offset..<(offset + length))) else {
            return nil
        }
        frames.append(frame)
        offset += length
    }

    guard frames.count >= 2,
          frames[0].kind == ChatFrameType.state,
          let strictState = chatDecodeHeader(frames[0].header, as: ChatHTTPStateBody.self),
          let state = ChatStateHeader(frames[0].header),
          strictState.seqFloor <= strictState.checkpointSeq,
          strictState.checkpointSeq <= strictState.headSeq,
          state.headSeq == strictState.headSeq else { return nil }

    let truncated = frames[frames.count - 1].kind != ChatFrameType.rowsDone
    var rowFrames = frames.dropFirst()
    if !truncated {
        guard let done = chatDecodeHeader(frames[frames.count - 1].header,
                                          as: ChatHTTPRowsDoneBody.self),
              done.headSeq == strictState.headSeq else { return nil }
        rowFrames = rowFrames.dropLast()
    }

    var rows: [ChatHTTPPullRow] = []
    var previousSeq: UInt64 = 0
    for frame in rowFrames {
        guard frame.kind == ChatFrameType.row,
              let header = chatDecodeHeader(frame.header, as: ChatHTTPRowBody.self),
              header.seq > previousSeq, header.seq <= strictState.headSeq,
              !header.batchId.isEmpty, !frame.payload.isEmpty else { return nil }
        previousSeq = header.seq
        rows.append(ChatHTTPPullRow(frame: frame, seq: header.seq,
                                    batchId: header.batchId))
    }
    return (ChatHTTPPullBatch(stateFrame: frames[0], state: state,
                              rows: rows, headSeq: strictState.headSeq),
            truncated)
}

/// A complete (non-truncated) pull body — the single-page fast path and the
/// shape every existing test exercises.
func chatDecodeHTTPPull(_ body: Data) -> ChatHTTPPullBatch? {
    guard let (batch, truncated) = chatDecodeHTTPPullPage(body), !truncated else { return nil }
    return batch
}

/// The cursor represented by a checkpoint-confirmed local doc before the
/// response's rows are applied. Rows at/below it may have been pruned.
func chatHTTPConfirmedBase(capturedCursor: UInt64, state: ChatStateHeader) -> UInt64 {
    state.checkpointSize > 0 ? max(capturedCursor, state.checkpointSeq) : capturedCursor
}

/// A decoded body is a frontier proof only when it contains every unpruned
/// sequence after the pre-push cursor (or checkpoint) through rowsDone.
func chatHTTPPullCoversFrontier(capturedCursor: UInt64,
                                pull: ChatHTTPPullBatch) -> Bool {
    guard pull.state.headSeq >= capturedCursor else { return false }
    let base = chatHTTPConfirmedBase(capturedCursor: capturedCursor, state: pull.state)
    guard base <= pull.headSeq else { return false }
    var expected = base
    for row in pull.rows {
        guard expected < UInt64.max else { return false }
        expected += 1
        guard row.seq == expected else { return false }
    }
    return expected == pull.headSeq
}

/// A state behind the request cursor, or behind an ack returned just before
/// it, is authoritative reset evidence. With a checkpoint, its sequence is
/// the only safe restart point; otherwise replay the room from zero.
func chatHTTPResetCursor(capturedCursor: UInt64,
                         state: ChatStateHeader,
                         stagedAcks: [ChatHTTPPushAck] = []) -> UInt64? {
    // `captured=0, head=0` is still a reset when this cycle just received an
    // ack at seq 1+: the POST and GET observed different room incarnations.
    guard state.headSeq < capturedCursor
            || stagedAcks.contains(where: { $0.seq > state.headSeq }) else { return nil }
    return chatResetAnchor(state: state)
}

/// Exact safe cursor for a confirmed reset. A malformed/ahead checkpoint is
/// not an anchor and falls back to a full replay.
func chatResetAnchor(state: ChatStateHeader) -> UInt64 {
    state.checkpointSize > 0 && state.checkpointSeq <= state.headSeq
        ? state.checkpointSeq : 0
}

/// Pull far enough back to include every acknowledgement whose batch identity
/// this cycle intends to prove. `seq == 0` is malformed but still maps safely
/// to zero so the later identity check forces reset instead of underflowing.
func chatHTTPPullSince(prePushCursor: UInt64,
                       acknowledgements: [ChatHTTPPushAck]) -> UInt64 {
    acknowledgements.reduce(prePushCursor) { cursor, ack in
        min(cursor, ack.seq > 0 ? ack.seq - 1 : 0)
    }
}

func chatHTTPAcknowledgementIsProven(pullSince: UInt64,
                                     pull: ChatHTTPPullBatch,
                                     ack: ChatHTTPPushAck) -> Bool {
    guard ack.seq > pullSince, ack.seq <= pull.headSeq,
          let row = pull.rows.first(where: { $0.seq == ack.seq }) else { return false }
    return row.batchId == ack.batchId
}

/// Exact proof tuples from the pre-dispatch snapshot. The caller uses this
/// same set to retire pending pushes and journal entries atomically after all
/// rows apply; acknowledgements arriving after dispatch are not swept up.
func chatHTTPProvenAcknowledgements(pullSince: UInt64,
                                    pull: ChatHTTPPullBatch,
                                    acknowledgements: [ChatHTTPPushAck]) -> [ChatHTTPPushAck] {
    var seen = Set<ChatHTTPPushAck>()
    return acknowledgements.filter { ack in
        chatHTTPAcknowledgementIsProven(pullSince: pullSince, pull: pull, ack: ack)
            && seen.insert(ack).inserted
    }
}

/// Remove only exact tuples proven by this cycle. A late ACK with a different
/// tuple remains journaled for the next pull, while a late WS copy of a staged
/// HTTP ACK is cleared by the staged tuple's proof.
func chatHTTPRetainedAcknowledgements(current: [ChatHTTPPushAck],
                                      proven: [ChatHTTPPushAck]) -> [ChatHTTPPushAck] {
    let provenSet = Set(proven)
    return current.filter { !provenSet.contains($0) }
}

/// Detect an ABA room reset even when the new incarnation's numeric head has
/// already caught up with the old cursor. Every acknowledgement in the fixed
/// pre-dispatch proof snapshot must name its exact row in this response.
func chatAcknowledgementsRequireReset(pullSince: UInt64,
                                      pull: ChatHTTPPullBatch,
                                      acknowledgements: [ChatHTTPPushAck]) -> Bool {
    acknowledgements.contains {
        !chatHTTPAcknowledgementIsProven(pullSince: pullSince, pull: pull, ack: $0)
    }
}

/// Stable replay order after reset. Acked entries come first because they may
/// have been removed from `pending` by a racing WS ACK; the HTTP snapshot and
/// current queue fill the rest. Permanent server verdicts are never restored.
func chatResetReplayOrder(acknowledgedBatchIds: [String],
                          snapshotBatchIds: [String],
                          pendingBatchIds: [String],
                          permanentlyRejected: Set<String>) -> [String] {
    var seen = Set<String>()
    return (acknowledgedBatchIds + snapshotBatchIds + pendingBatchIds).filter { batchId in
        !batchId.isEmpty && !permanentlyRejected.contains(batchId)
            && seen.insert(batchId).inserted
    }
}

/// Every async transport response is stamped with the room-reset version at
/// dispatch. A reset changes the version before any await, invalidating old
/// HTTP responses and frames from the socket incarnation being replaced.
func chatSessionVersionIsCurrent(_ responseVersion: UInt64,
                                 currentVersion: UInt64) -> Bool {
    responseVersion == currentVersion
}

enum ChatHTTPPushDisposition: Equatable {
    case retire
    case retry
    case reset(cursor: UInt64)
}

/// Decide whether a staged POST ack can retire its local batch. `pullApplied`
/// means checkpoint/frontier validation and every parsed row application have
/// completed, and the delegate cursor covers `pull.headSeq`.
func chatHTTPPushDisposition(prePushCursor: UInt64,
                             pullSince: UInt64,
                             pull: ChatHTTPPullBatch,
                             pullApplied: Bool,
                             ack: ChatHTTPPushAck) -> ChatHTTPPushDisposition {
    if let cursor = chatHTTPResetCursor(capturedCursor: prePushCursor,
                                        state: pull.state,
                                        stagedAcks: [ack]) {
        return .reset(cursor: cursor)
    }
    if chatAcknowledgementsRequireReset(
        pullSince: pullSince, pull: pull, acknowledgements: [ack]) {
        return .reset(cursor: chatResetAnchor(state: pull.state))
    }
    guard pullApplied,
          chatHTTPPullCoversFrontier(capturedCursor: pullSince, pull: pull),
          ack.seq <= pull.headSeq else { return .retry }

    if pull.rows.contains(where: { $0.seq == ack.seq && $0.batchId == ack.batchId }) {
        return .retire
    }
    return .retry
}

enum ChatCatchUpPlan: Equatable {
    /// Local doc already contains the checkpoint frontier (or there is no
    /// checkpoint): stream rows only.
    case rowsOnly(after: UInt64)
    /// Fetch + import the checkpoint first, then rows after it.
    case checkpointThenRows(after: UInt64)
}

/// Decide the catch-up path from the hello state (chat_client.rs
/// plan_catch_up, decision table pinned by its tests):
/// - cursor > headSeq means the server lost state — treat the cursor as fresh;
/// - checkpoint presence is the SIZE, not the seq (a freshly seeded room's
///   checkpoint legitimately covers seq 0);
/// - a contained frontier skips rows the checkpoint already covers.
func chatPlanCatchUp(cursor: UInt64, state: ChatStateHeader,
                     frontierContained: Bool) -> ChatCatchUpPlan {
    let cursor = cursor > state.headSeq ? 0 : cursor
    if state.checkpointSize == 0 {
        return .rowsOnly(after: cursor)
    }
    if frontierContained {
        return .rowsOnly(after: max(cursor, state.checkpointSeq))
    }
    return .checkpointThenRows(after: state.checkpointSeq)
}
