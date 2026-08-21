// Edge auth client — /auth/exchange, /auth/refresh, /auth/organizations
// (edge/src/auth-routes.ts). Two modes, mirroring the engine:
// - WorkOS: paste-code exchange → access/refresh tokens; refresh scoped to an
//   Organization adds the WorkOS `org_id` wire claim required by legacy rooms.
// - Dev (AUTH_MODE=dev edge): the bearer string IS the user id; the legacy
//   `user@organization` test-token shape supplies a fake Organization claim.

import Foundation

struct AuthUser: Codable, Equatable {
    var id: String
    var email: String?
    var firstName: String?
    var lastName: String?
}

struct AuthOrganization: Codable, Identifiable, Equatable {
    var id: String
    var organizationId: String
    var name: String
}

struct AuthTokens: Codable, Equatable {
    var accessToken: String
    var refreshToken: String
}

enum AuthError: LocalizedError {
    case http(Int, String)
    case invalidResponse

    var errorDescription: String? {
        switch self {
        case .http(let code, let body): return "Auth failed (\(code)): \(body)"
        case .invalidResponse: return "Unexpected auth response"
        }
    }
}

struct AuthClient {
    var baseURL: URL

    func exchange(code: String) async throws -> (AuthUser, AuthTokens) {
        struct Response: Codable {
            var user: AuthUser
            var accessToken: String
            var refreshToken: String
        }
        let r: Response = try await post("auth/exchange", body: ["code": code])
        return (r.user, AuthTokens(accessToken: r.accessToken, refreshToken: r.refreshToken))
    }

    func refresh(refreshToken: String, organizationId: String? = nil) async throws -> AuthTokens {
        var body: [String: String] = ["refreshToken": refreshToken]
        if let organizationId { body["organizationId"] = organizationId }
        return try await post("auth/refresh", body: body)
    }

    func organizations(accessToken: String) async throws -> [AuthOrganization] {
        struct Response: Codable { var organizations: [AuthOrganization] }
        var request = URLRequest(url: baseURL.appending(path: "auth/organizations"))
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await URLSession.shared.data(for: request)
        try Self.check(data: data, response: response)
        return try JSONDecoder().decode(Response.self, from: data).organizations
    }

    private func post<T: Decodable>(_ path: String, body: [String: String]) async throws -> T {
        var request = URLRequest(url: baseURL.appending(path: path))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(body)
        let (data, response) = try await URLSession.shared.data(for: request)
        try Self.check(data: data, response: response)
        return try JSONDecoder().decode(T.self, from: data)
    }

    private static func check(data: Data, response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else { throw AuthError.invalidResponse }
        guard (200..<300).contains(http.statusCode) else {
            throw AuthError.http(http.statusCode, String(data: data, encoding: .utf8) ?? "")
        }
    }
}

// MARK: - Keychain storage

enum Keychain {
    private static let service = "sh.zeron.ios"

    static func save(_ value: String, key: String) {
        let data = Data(value.utf8)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
        ]
        SecItemDelete(query as CFDictionary)
        var add = query
        add[kSecValueData as String] = data
        add[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
        SecItemAdd(add as CFDictionary, nil)
    }

    static func load(key: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: AnyObject?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }

    static func delete(key: String) {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
        ]
        SecItemDelete(query as CFDictionary)
    }
}
