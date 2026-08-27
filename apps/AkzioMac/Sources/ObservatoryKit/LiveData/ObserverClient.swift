import Foundation

public enum ObservatoryDataMode: String, Sendable, CaseIterable, Identifiable {
    case mock
    case live

    public var id: String { rawValue }
    public var displayName: String { rawValue.capitalized }
}

public enum ObserverTransportPolicy {
    /// Rust bounds individual Observer broker groups at twenty seconds. The
    /// client keeps margin so it receives the server's availability downgrade.
    public static let standardRequestTimeout: TimeInterval = 25
    public static let snapshotRequestTimeout: TimeInterval = 45
}

public enum ObserverConnectionState: Sendable, Equatable {
    case mock
    case connecting
    case connected(Date)
    case stale(String)
    case offline(String)

    public var label: String {
        switch self {
        case .mock: "Mock"
        case .connecting: "Connecting"
        case .connected: "Connected"
        case .stale: "Stale"
        case .offline: "Offline"
        }
    }

    public var detail: String? {
        switch self {
        case .stale(let message), .offline(let message): message
        default: nil
        }
    }
}


enum ObserverStreamEvent: Sendable {
    case invalidate(Int64)
    case reasoning(ObserverReasoningEventPayload, receivedAt: Date)
}

enum ObserverClientError: LocalizedError {
    case invalidEndpoint
    case invalidResponse
    case httpStatus(Int)
    case unsupportedSchema(Int)

    var errorDescription: String? {
        switch self {
        case .invalidEndpoint: "Invalid loopback observer endpoint"
        case .invalidResponse: "Observer returned an invalid response"
        case .httpStatus(let status): "Observer returned HTTP \(status)"
        case .unsupportedSchema(let version): "Unsupported observer schema \(version)"
        }
    }
}

struct ObserverClient: Sendable {
    let endpoint: URL
    let token: String

    init(endpoint: URL, token: String) throws {
        guard endpoint.scheme == "http",
              let host = endpoint.host,
              host == "127.0.0.1" || host == "localhost" || host == "::1"
        else { throw ObserverClientError.invalidEndpoint }
        self.endpoint = endpoint
        self.token = token
    }

    func fetchSnapshot() async throws -> ObserverSnapshotPayload {
        let data = try await data(
            path: "v1/observer/snapshot",
            timeout: ObserverTransportPolicy.snapshotRequestTimeout
        )
        let payload = try Self.decoder().decode(ObserverSnapshotPayload.self, from: data)
        guard payload.schemaVersion == 2 else {
            throw ObserverClientError.unsupportedSchema(payload.schemaVersion)
        }
        return payload
    }

    func fetchRun(_ runID: String) async throws -> ObserverRunDetailPayload {
        let data = try await data(path: "v1/observer/runs/\(runID)")
        return try Self.decoder().decode(ObserverRunDetailPayload.self, from: data)
    }

    func fetchPortfolioHistory(
        range: EquityRange
    ) async throws -> ObserverSectionPayload<ObserverPortfolioHistoryPayload> {
        let value: String
        switch range {
        case .oneDay: value = "1d"
        case .fiveDay: value = "1w"
        case .oneMonth: value = "1m"
        case .threeMonth: value = "3m"
        case .ytd, .oneYear, .all:
            throw ObserverClientError.invalidEndpoint
        }
        var components = URLComponents(
            url: endpoint.appending(path: "v1/observer/portfolio/history"),
            resolvingAgainstBaseURL: false
        )
        components?.queryItems = [
            URLQueryItem(name: "range", value: value)
        ]
        guard let url = components?.url else { throw ObserverClientError.invalidEndpoint }
        let data = try await data(url: url)
        return try Self.decoder().decode(
            ObserverSectionPayload<ObserverPortfolioHistoryPayload>.self,
            from: data
        )
    }

    func events(after cursor: Int64) -> AsyncThrowingStream<ObserverStreamEvent, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    var components = URLComponents(
                        url: endpoint.appending(path: "v1/observer/events"),
                        resolvingAgainstBaseURL: false
                    )
                    components?.queryItems = [URLQueryItem(name: "after", value: String(cursor))]
                    guard let url = components?.url else {
                        throw ObserverClientError.invalidEndpoint
                    }
                    var request = authorizedRequest(url: url)
                    request.timeoutInterval = 60
                    let (bytes, response) = try await URLSession.shared.bytes(for: request)
                    try Self.validate(response)
                    var eventName = "message"
                    for try await line in bytes.lines {
                        try Task.checkCancellation()
                        if line.hasPrefix("event:") {
                            eventName = line.dropFirst(6).trimmingCharacters(in: .whitespaces)
                            continue
                        }
                        if line.isEmpty {
                            eventName = "message"
                            continue
                        }
                        guard line.hasPrefix("data:") else { continue }
                        let value = line.dropFirst(5).trimmingCharacters(in: .whitespaces)
                        guard let data = value.data(using: .utf8) else { continue }
                        switch eventName {
                        case "invalidate":
                            let payload = try JSONDecoder().decode(
                                ObserverInvalidationPayload.self,
                                from: data
                            )
                            continuation.yield(.invalidate(payload.cursor))
                        case "reasoning-start", "reasoning-delta", "reasoning-end":
                            let payload = try Self.decoder().decode(
                                ObserverReasoningEventPayload.self,
                                from: data
                            )
                            continuation.yield(.reasoning(payload, receivedAt: Date()))
                        default:
                            continue
                        }
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    private func data(
        path: String,
        timeout: TimeInterval = ObserverTransportPolicy.standardRequestTimeout
    ) async throws -> Data {
        try await data(url: endpoint.appending(path: path), timeout: timeout)
    }

    private func data(
        url: URL,
        timeout: TimeInterval = ObserverTransportPolicy.standardRequestTimeout
    ) async throws -> Data {
        var request = authorizedRequest(url: url)
        request.timeoutInterval = timeout
        let (data, response) = try await URLSession.shared.data(for: request)
        try Self.validate(response)
        return data
    }

    private func authorizedRequest(url: URL) -> URLRequest {
        var request = URLRequest(url: url)
        request.setValue(token, forHTTPHeaderField: "x-akzio-observer-token")
        return request
    }

    private static func validate(_ response: URLResponse) throws {
        guard let response = response as? HTTPURLResponse else {
            throw ObserverClientError.invalidResponse
        }
        guard response.statusCode == 200 else {
            throw ObserverClientError.httpStatus(response.statusCode)
        }
    }

    static func decoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let value = try decoder.singleValueContainer().decode(String.self)
            let fractional = ISO8601DateFormatter()
            fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            if let date = fractional.date(from: value) { return date }
            let regular = ISO8601DateFormatter()
            regular.formatOptions = [.withInternetDateTime]
            if let date = regular.date(from: value) { return date }
            throw DecodingError.dataCorruptedError(
                in: try decoder.singleValueContainer(),
                debugDescription: "Invalid RFC 3339 date"
            )
        }
        return decoder
    }
}
