import XCTest
@testable import Zeron

final class AppConfigTests: XCTestCase {
    private func config() -> AppConfig {
        AppConfig(
            edgeURL: URL(string: "https://edge.example/base")!,
            mode: .dev,
            userId: "user-1",
            organizationId: "org-1",
            deviceId: "device-1",
            deviceName: "Phone",
            devBearer: "dev-token")
    }

    func testGenerationTwoUsesChat2ForEveryTransport() async throws {
        try await assertRoomPaths(roomGen: 2, prefix: "/base/chat2/chat-1")
    }

    func testGenerationThreeUsesChat3ForEveryTransport() async throws {
        try await assertRoomPaths(roomGen: 3, prefix: "/base/chat3/chat-1")
    }

    private func assertRoomPaths(roomGen: Int, prefix: String) async throws {
        let config = config()
        let socketValue = await config.chat2SocketURL(chatId: "chat-1", roomGen: roomGen)
        let checkpointValue = await config.chat2CheckpointRequest(
            chatId: "chat-1", roomGen: roomGen)
        let rowsValue = await config.chat2RowsRequest(
            chatId: "chat-1", roomGen: roomGen, after: 42)
        let pushValue = await config.chat2PushRequest(
            chatId: "chat-1", roomGen: roomGen, batchId: "batch-1")

        let socket = try XCTUnwrap(socketValue)
        let checkpoint = try XCTUnwrap(checkpointValue)
        let rows = try XCTUnwrap(rowsValue)
        let push = try XCTUnwrap(pushValue)

        XCTAssertEqual(socket.path, "\(prefix)/ws")
        XCTAssertEqual(checkpoint.url?.path, "\(prefix)/checkpoint")
        XCTAssertEqual(rows.url?.path, "\(prefix)/rows")
        XCTAssertEqual(push.url?.path, "\(prefix)/rows")
        XCTAssertEqual(push.httpMethod, "POST")
    }
}
