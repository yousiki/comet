import Loro
import XCTest
@testable import Zeron

final class SessionStoreSyncTests: XCTestCase {
    /// A syntactically valid Loro update can still be unapplied when its
    /// causal predecessor is absent. This is the exact status that must stop
    /// chat2/chat3 cursor advancement and force a checkpoint/backfill retry.
    func testRemoteImportReportsMissingDependenciesUntilPredecessorArrives() throws {
        let source = LoroDoc()
        try source.setPeerId(peer: 1)
        let text = source.getText(id: "transcript")

        try text.insert(pos: 0, s: "A")
        source.commit()
        let afterFirst = source.oplogVv()
        let predecessor = try source.export(mode: .updates(from: VersionVector()))

        try text.insert(pos: 1, s: "B")
        source.commit()
        let dependent = try source.export(mode: .updates(from: afterFirst))

        let replica = LoroDoc()
        XCTAssertEqual(try chatImportRemote(into: replica, bytes: dependent),
                       .missingDependencies)
        // Replaying the already parked row must still expose that payload's
        // gap; otherwise a reconnect would accept it and recreate a lying
        // cursor without ever seeing the founding update.
        XCTAssertEqual(try chatImportRemote(into: replica, bytes: dependent),
                       .missingDependencies)

        XCTAssertEqual(try chatImportRemote(into: replica, bytes: predecessor),
                       .applied)
        XCTAssertEqual(replica.getText(id: "transcript").toString(), "AB")
    }

    func testCompleteSnapshotIsAppliedWithoutPendingDependencies() throws {
        let source = LoroDoc()
        try source.getText(id: "transcript").insert(pos: 0, s: "complete")
        source.commit()

        let replica = LoroDoc()
        let snapshot = try source.export(mode: .snapshot)
        XCTAssertEqual(try chatImportRemote(into: replica, bytes: snapshot), .applied)
        XCTAssertEqual(replica.getText(id: "transcript").toString(), "complete")
    }

    /// A later parked row must not block the earlier rows needed to repair it.
    /// This distinguishes per-blob materialization from a global "any pending"
    /// gate, which would redial forever on the first repair row.
    func testEarlierRowsApplyWhileUnrelatedFutureRowRemainsPending() throws {
        let chain = LoroDoc()
        try chain.setPeerId(peer: 11)
        let chainText = chain.getText(id: "chain")
        try chainText.insert(pos: 0, s: "A")
        chain.commit()
        let afterFirst = chain.oplogVv()
        let first = try chain.export(mode: .updates(from: VersionVector()))

        try chainText.insert(pos: 1, s: "B")
        chain.commit()
        let afterSecond = chain.oplogVv()
        let second = try chain.export(mode: .updates(from: afterFirst))

        let futureSource = LoroDoc()
        try futureSource.setPeerId(peer: 22)
        _ = try futureSource.importBatch(bytes: [chain.export(mode: .snapshot)])
        try futureSource.getText(id: "future").insert(pos: 0, s: "C")
        futureSource.commit()
        let future = try futureSource.export(mode: .updates(from: afterSecond))

        let replica = LoroDoc()
        XCTAssertEqual(try chatImportRemote(into: replica, bytes: future),
                       .missingDependencies)
        XCTAssertEqual(try chatImportRemote(into: replica, bytes: first), .applied)
        XCTAssertEqual(replica.getText(id: "chain").toString(), "A")
        XCTAssertEqual(replica.getText(id: "future").toString(), "")

        XCTAssertEqual(try chatImportRemote(into: replica, bytes: second), .applied)
        XCTAssertEqual(replica.getText(id: "chain").toString(), "AB")
        XCTAssertEqual(replica.getText(id: "future").toString(), "C")
    }
}
