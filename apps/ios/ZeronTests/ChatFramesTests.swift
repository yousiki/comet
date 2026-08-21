// chat2 wire-frame conformance — pins the same layout vectors as
// crates/sync/src/chat_frames.rs and edge/src/chat-frames.test.ts. The three
// codecs must stay byte-compatible; change all suites together.

import XCTest
@testable import Zeron

final class ChatFramesTests: XCTestCase {
    private func httpBody(_ frames: [Data]) -> Data {
        var body = Data()
        for frame in frames {
            var length = UInt32(frame.count).littleEndian
            withUnsafeBytes(of: &length) { body.append(contentsOf: $0) }
            body.append(frame)
        }
        return body
    }

    private func state(_ head: UInt64, checkpointSeq: UInt64 = 0,
                       checkpointSize: UInt64 = 0) -> Data {
        ChatWire.encode(ChatFrameType.state,
                        header: ["headSeq": head, "seqFloor": checkpointSeq,
                                 "checkpointSeq": checkpointSeq,
                                 "checkpointSize": checkpointSize,
                                 "rowCount": head, "rowBytes": 100])
    }

    private func row(_ seq: UInt64, batchId: String) -> Data {
        ChatWire.encode(ChatFrameType.row,
                        header: ["seq": seq, "device": "dev-b", "batchId": batchId],
                        payload: Data([UInt8(truncatingIfNeeded: seq)]))
    }

    private func done(_ head: UInt64) -> Data {
        ChatWire.encode(ChatFrameType.rowsDone, header: ["headSeq": head])
    }

    func testPinsTheWireLayout() {
        // Must match the Rust/TS vector: [type][headerLen u32 LE][header][payload].
        let frame = ChatWire.encode(ChatFrameType.push,
                                    header: ["batchId": "b1"],
                                    payload: Data([9, 8, 7]))
        XCTAssertEqual(frame[0], ChatFrameType.push)
        let header = Data(#"{"batchId":"b1"}"#.utf8)
        XCTAssertEqual([UInt8](frame[1..<5]),
                       [UInt8(header.count), 0, 0, 0])
        XCTAssertEqual(frame.subdata(in: 5..<(5 + header.count)), header)
        XCTAssertEqual(frame.subdata(in: (5 + header.count)..<frame.count), Data([9, 8, 7]))
    }

    func testRoundTripsAndRejectsMalformed() {
        let payload = Data(repeating: 1, count: 1000)
        let frame = ChatWire.encode(ChatFrameType.row, header: ["seq": 7], payload: payload)
        let decoded = ChatWire.decode(frame)
        XCTAssertEqual(decoded?.kind, ChatFrameType.row)
        XCTAssertEqual((decoded?.header["seq"] as? NSNumber)?.uint64Value, 7)
        XCTAssertEqual(decoded?.payload, payload)

        XCTAssertNil(ChatWire.decode(Data()))
        XCTAssertNil(ChatWire.decode(Data([ChatFrameType.hello])))
        // Header length past the buffer.
        var truncated = ChatWire.encode(ChatFrameType.hello, header: [:])
        truncated.replaceSubrange(1..<5, with: withUnsafeBytes(of: UInt32(9999).littleEndian) { Data($0) })
        XCTAssertNil(ChatWire.decode(truncated))
        // Non-object header (raw array JSON in the header slot).
        var arr = Data([ChatFrameType.hello])
        let arrJSON = Data("[1]".utf8)
        arr.append(withUnsafeBytes(of: UInt32(arrJSON.count).littleEndian) { Data($0) })
        arr.append(arrJSON)
        XCTAssertNil(ChatWire.decode(arr))
        // Oversized header.
        let fat = ChatWire.encode(ChatFrameType.hello,
                                  header: ["pad": String(repeating: "x", count: chatFrameMaxHeaderBytes)])
        XCTAssertNil(ChatWire.decode(fat))
    }

    func testStateHeaderParsesServerShape() {
        let state = ChatStateHeader([
            "headSeq": 10, "seqFloor": 3, "checkpointSeq": 3,
            "checkpointSize": 160_000, "rowCount": 7, "rowBytes": 14_000,
        ])
        XCTAssertEqual(state?.headSeq, 10)
        XCTAssertEqual(state?.checkpointSeq, 3)
        XCTAssertEqual(state?.checkpointSize, 160_000)
        XCTAssertNil(ChatStateHeader(["nope": 1]))
    }

    /// The catch-up decision table from chat_client.rs plan_catch_up.
    func testPlanCatchUp() {
        func state(_ head: UInt64, _ ckSeq: UInt64, _ ckSize: UInt64) -> ChatStateHeader {
            ChatStateHeader(["headSeq": head, "seqFloor": 0,
                             "checkpointSeq": ckSeq, "checkpointSize": ckSize])!
        }
        // No checkpoint: rows from the cursor.
        XCTAssertEqual(chatPlanCatchUp(cursor: 4, state: state(10, 0, 0), frontierContained: false),
                       .rowsOnly(after: 4))
        // Contained frontier skips rows the checkpoint covers.
        XCTAssertEqual(chatPlanCatchUp(cursor: 2, state: state(10, 6, 100), frontierContained: true),
                       .rowsOnly(after: 6))
        XCTAssertEqual(chatPlanCatchUp(cursor: 8, state: state(10, 6, 100), frontierContained: true),
                       .rowsOnly(after: 8))
        // Missing frontier: fetch the checkpoint, then rows after it.
        XCTAssertEqual(chatPlanCatchUp(cursor: 2, state: state(10, 6, 100), frontierContained: false),
                       .checkpointThenRows(after: 6))
        // Server behind the cursor (reset/wipe): the cursor is meaningless.
        XCTAssertEqual(chatPlanCatchUp(cursor: 20, state: state(10, 0, 0), frontierContained: false),
                       .rowsOnly(after: 0))
        // A freshly SEEDED room: checkpoint covers seq 0 but has SIZE — it
        // must not be misread as "no checkpoint" (2026-08-10 cutover gauntlet).
        XCTAssertEqual(chatPlanCatchUp(cursor: 0, state: state(0, 0, 5_000), frontierContained: false),
                       .checkpointThenRows(after: 0))
    }

    func testHTTPPushAckIsStrictAndBatchBound() {
        XCTAssertEqual(chatDecodeHTTPPushAck(Data(#"{"batchId":"b1","seq":7,"dup":false}"#.utf8),
                                             expectedBatchId: "b1"),
                       ChatHTTPPushAck(batchId: "b1", seq: 7))
        XCTAssertNil(chatDecodeHTTPPushAck(Data(#"{"batchId":"other","seq":7}"#.utf8),
                                           expectedBatchId: "b1"))
        XCTAssertNil(chatDecodeHTTPPushAck(Data(#"{"batchId":"b1","seq":true}"#.utf8),
                                           expectedBatchId: "b1"))
        XCTAssertNil(chatDecodeHTTPPushAck(Data(#"{"batchId":"b1","seq":-1}"#.utf8),
                                           expectedBatchId: "b1"))
        XCTAssertNil(chatDecodeHTTPPushAck(Data(#"{"batchId":"b1","seq":1.5}"#.utf8),
                                           expectedBatchId: "b1"))
        XCTAssertNil(chatDecodeHTTPPushAck(Data(#"{"batchId":"b1","seq":0}"#.utf8),
                                           expectedBatchId: "b1"))
    }

    func testHTTPPullRequiresCompleteOrderedFrameStream() {
        let valid = httpBody([state(3), row(1, batchId: "foreign"),
                              row(2, batchId: "own"), row(3, batchId: "later"), done(3)])
        let pull = chatDecodeHTTPPull(valid)
        XCTAssertEqual(pull?.headSeq, 3)
        XCTAssertEqual(pull?.rows.map(\.seq), [1, 2, 3])
        XCTAssertTrue(chatHTTPPullCoversFrontier(capturedCursor: 0, pull: pull!))

        var trailing = valid
        trailing.append(0xff)
        XCTAssertNil(chatDecodeHTTPPull(trailing))
        XCTAssertNil(chatDecodeHTTPPull(Data(valid.dropLast())))
        let gap = chatDecodeHTTPPull(httpBody([state(3), row(1, batchId: "b1"),
                                               row(3, batchId: "b3"), done(3)]))!
        XCTAssertFalse(chatHTTPPullCoversFrontier(capturedCursor: 0, pull: gap))
        XCTAssertNil(chatDecodeHTTPPull(httpBody([state(1), done(2)])))
        XCTAssertNil(chatDecodeHTTPPull(httpBody([state(1),
                                                  ChatWire.encode(ChatFrameType.probeOk,
                                                                  header: [:]), done(1)])))
    }

    func testHTTPPullPageAcceptsTruncationButDemandsProgress() {
        // A page cut by the edge's ROWS_BODY_CAP: state + rows, no done frame.
        let (page, truncated) = chatDecodeHTTPPullPage(
            httpBody([state(5), row(1, batchId: "b1"), row(2, batchId: "b2")]))!
        XCTAssertTrue(truncated)
        XCTAssertEqual(page.rows.map(\.seq), [1, 2])
        XCTAssertEqual(page.headSeq, 5)
        // The strict decoder refuses a truncated page outright.
        XCTAssertNil(chatDecodeHTTPPull(
            httpBody([state(5), row(1, batchId: "b1"), row(2, batchId: "b2")])))
        // A bare state with neither rows nor done is malformed, not truncated —
        // the cap always leaves at least one row, so this shape can't advance.
        XCTAssertNil(chatDecodeHTTPPullPage(httpBody([state(5)])))
        // A complete page reports truncated=false through the page decoder too.
        let (full, fullTruncated) = chatDecodeHTTPPullPage(
            httpBody([state(2), row(1, batchId: "b1"), row(2, batchId: "b2"), done(2)]))!
        XCTAssertFalse(fullTruncated)
        XCTAssertEqual(full.rows.map(\.seq), [1, 2])
    }

    func testHTTPFrontierCoverageAccountsForCheckpoint() {
        let body = httpBody([state(8, checkpointSeq: 5, checkpointSize: 500),
                             row(6, batchId: "b6"), row(7, batchId: "b7"),
                             row(8, batchId: "b8"), done(8)])
        let pull = chatDecodeHTTPPull(body)!
        XCTAssertTrue(chatHTTPPullCoversFrontier(capturedCursor: 2, pull: pull))

        let gap = chatDecodeHTTPPull(httpBody([
            state(8, checkpointSeq: 5, checkpointSize: 500),
            row(7, batchId: "b7"), row(8, batchId: "b8"), done(8),
        ]))!
        XCTAssertFalse(chatHTTPPullCoversFrontier(capturedCursor: 2, pull: gap))
    }

    func testHTTPAckDecisionTableDefersUntilAppliedAndChecksRowIdentity() {
        let pull = chatDecodeHTTPPull(httpBody([
            state(7), row(6, batchId: "foreign"), row(7, batchId: "ours"), done(7),
        ]))!
        let ours = ChatHTTPPushAck(batchId: "ours", seq: 7)
        XCTAssertEqual(chatHTTPPushDisposition(prePushCursor: 5, pullSince: 5,
                                               pull: pull, pullApplied: false,
                                               ack: ours), .retry)
        XCTAssertEqual(chatHTTPPushDisposition(prePushCursor: 5, pullSince: 5,
                                               pull: pull, pullApplied: true,
                                               ack: ours), .retire)
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 5, pullSince: 5, pull: pull, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "ours", seq: 6)), .reset(cursor: 0))
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 5, pullSince: 5, pull: pull, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "ours", seq: 8)), .reset(cursor: 0))

        // A dedupe ack already covered by the request cursor has no row in
        // this response, so its identity is not proven without a checkpoint.
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 5, pullSince: 5, pull: pull, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "old", seq: 4)), .reset(cursor: 0))

        // Even a confirmed checkpoint frontier cannot prove that this numeric
        // sequence still belongs to our batch after an ABA room reset.
        let checkpointed = chatDecodeHTTPPull(httpBody([
            state(7, checkpointSeq: 5, checkpointSize: 500),
            row(6, batchId: "b6"), row(7, batchId: "b7"), done(7),
        ]))!
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 5, pullSince: 3, pull: checkpointed, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "old", seq: 4)), .reset(cursor: 5))
    }

    func testHTTPAckIdentityDetectsABARoomResetBeforeRowsApply() {
        let pull = chatDecodeHTTPPull(httpBody([
            state(14), row(11, batchId: "new-11"), row(12, batchId: "new-12"),
            row(13, batchId: "foreign"), row(14, batchId: "new-14"), done(14),
        ]))!
        let oldIncarnationAck = ChatHTTPPushAck(batchId: "ours", seq: 13)

        XCTAssertTrue(chatHTTPPullCoversFrontier(capturedCursor: 10, pull: pull))
        XCTAssertTrue(chatAcknowledgementsRequireReset(
            pullSince: 10, pull: pull,
            acknowledgements: [oldIncarnationAck]))
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 10, pullSince: 10, pull: pull, pullApplied: false,
            ack: oldIncarnationAck), .reset(cursor: 0))
    }

    func testRememberedWSAckRequiresIdentityProofWhenHTTPStagedNone() {
        let pull = chatDecodeHTTPPull(httpBody([
            state(4), row(3, batchId: "foreign"), row(4, batchId: "new-4"), done(4),
        ]))!
        let rememberedOnly = [ChatHTTPPushAck(batchId: "ws-acked", seq: 3)]

        XCTAssertTrue(chatAcknowledgementsRequireReset(
            pullSince: 2, pull: pull,
            acknowledgements: rememberedOnly))
    }

    func testHTTPPullSinceBacktracksBehindEveryProofAckWithoutUnderflow() {
        XCTAssertEqual(chatHTTPPullSince(
            prePushCursor: 10,
            acknowledgements: [ChatHTTPPushAck(batchId: "old", seq: 4),
                               ChatHTTPPushAck(batchId: "new", seq: 12)]), 3)
        XCTAssertEqual(chatHTTPPullSince(
            prePushCursor: 10,
            acknowledgements: [ChatHTTPPushAck(batchId: "malformed", seq: 0)]), 0)
        XCTAssertEqual(chatHTTPPullSince(prePushCursor: 10, acknowledgements: []), 10)
    }

    func testHTTPProofCleanupClearsOnlySnapshotTuplesAndAvoidsResetLoop() {
        let pull = chatDecodeHTTPPull(httpBody([
            state(7), row(4, batchId: "http"), row(5, batchId: "foreign-5"),
            row(6, batchId: "remembered"), row(7, batchId: "late"), done(7),
        ]))!
        let staged = ChatHTTPPushAck(batchId: "http", seq: 4)
        let remembered = ChatHTTPPushAck(batchId: "remembered", seq: 6)
        let proofSnapshot = [staged, remembered]
        let pullSince = chatHTTPPullSince(prePushCursor: 7,
                                          acknowledgements: proofSnapshot)

        XCTAssertEqual(pullSince, 3)
        XCTAssertTrue(chatHTTPPullCoversFrontier(capturedCursor: pullSince, pull: pull))
        XCTAssertFalse(chatAcknowledgementsRequireReset(
            pullSince: pullSince, pull: pull, acknowledgements: proofSnapshot))
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 7, pullSince: pullSince, pull: pull,
            pullApplied: true, ack: staged), .retire)
        let proven = chatHTTPProvenAcknowledgements(
            pullSince: pullSince, pull: pull, acknowledgements: proofSnapshot)
        XCTAssertEqual(Set(proven), Set(proofSnapshot))

        // The staged tuple may arrive through WS after dispatch; its exact
        // proof clears that late copy too. An unrelated post-dispatch ACK stays.
        let late = ChatHTTPPushAck(batchId: "late", seq: 7)
        XCTAssertEqual(chatHTTPRetainedAcknowledgements(
            current: [remembered, staged, late], proven: proven), [late])

        // A pure-HTTP proof creates no journal entry, so the next cycle keeps
        // its real cursor and has no acknowledgement that can force a reset.
        let noJournal = chatHTTPRetainedAcknowledgements(current: [], proven: [staged])
        XCTAssertTrue(noJournal.isEmpty)
        XCTAssertEqual(chatHTTPPullSince(prePushCursor: 7,
                                         acknowledgements: noJournal), 7)
        XCTAssertFalse(chatAcknowledgementsRequireReset(
            pullSince: 7,
            pull: chatDecodeHTTPPull(httpBody([state(7), done(7)]))!,
            acknowledgements: noJournal))
    }

    func testHTTPResetKeepsPreResetAckAndChoosesSafeCursor() {
        let emptyReset = chatDecodeHTTPPull(httpBody([state(0), done(0)]))!
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 10, pullSince: 10, pull: emptyReset, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "ours", seq: 11)), .reset(cursor: 0))

        let checkpointReset = chatDecodeHTTPPull(httpBody([
            state(5, checkpointSeq: 3, checkpointSize: 100),
            row(4, batchId: "b4"), row(5, batchId: "b5"), done(5),
        ]))!
        XCTAssertEqual(chatHTTPPushDisposition(
            prePushCursor: 10, pullSince: 10,
            pull: checkpointReset, pullApplied: true,
            ack: ChatHTTPPushAck(batchId: "ours", seq: 11)), .reset(cursor: 3))
    }

    func testAckPastHeadConfirmsResetEvenAtZeroCursor() {
        let empty = ChatStateHeader([
            "headSeq": 0, "seqFloor": 0, "checkpointSeq": 0, "checkpointSize": 0,
        ])!
        XCTAssertEqual(chatHTTPResetCursor(
            capturedCursor: 0, state: empty,
            stagedAcks: [ChatHTTPPushAck(batchId: "old-incarnation", seq: 1)]), 0)
        XCTAssertNil(chatHTTPResetCursor(capturedCursor: 0, state: empty))
    }

    func testResetReplayRestoresAckedSnapshotButNeverPermanentReject() {
        XCTAssertEqual(
            chatResetReplayOrder(
                acknowledgedBatchIds: ["acked-before-reset"],
                snapshotBatchIds: ["acked-before-reset", "permanent", "still-pending"],
                pendingBatchIds: ["still-pending", "new-during-await"],
                permanentlyRejected: ["permanent"]),
            ["acked-before-reset", "still-pending", "new-during-await"])
    }

    func testWSResetUsesExactSafeCursorAndInvalidatesOldSession() {
        let bare = ChatStateHeader([
            "headSeq": 4, "seqFloor": 0, "checkpointSeq": 0, "checkpointSize": 0,
        ])!
        XCTAssertEqual(chatHTTPResetCursor(capturedCursor: 9, state: bare), 0)

        let checkpointed = ChatStateHeader([
            "headSeq": 4, "seqFloor": 2, "checkpointSeq": 2, "checkpointSize": 100,
        ])!
        XCTAssertEqual(chatHTTPResetCursor(capturedCursor: 9, state: checkpointed), 2)
        XCTAssertTrue(chatSessionVersionIsCurrent(7, currentVersion: 7))
        XCTAssertFalse(chatSessionVersionIsCurrent(7, currentVersion: 8))
    }
}
