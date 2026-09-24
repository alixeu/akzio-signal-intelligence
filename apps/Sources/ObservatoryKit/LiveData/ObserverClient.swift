import Foundation

// 文件职责：定义 ObservatoryKit 到本机 Rust Core Observer API 的只读客户端。
// 它负责回环地址校验、认证 header、HTTP 状态/Schema 检查、JSON 解码和 SSE 事件转换；
// 它不决定业务是否完成，也不绕过 Core 的 Rust 权威状态。
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

    // Every authenticated Core request shares this policy, including SSE and
    // Debug POSTs. Validating the initial loopback URL does not authorize a
    // redirect to another service carrying x-akzio-token or a control body.
    static let session = URLSession(
        configuration: .ephemeral,
        delegate: ObserverRedirectPolicy(),
        delegateQueue: nil
    )
}

private final class ObserverRedirectPolicy: NSObject, URLSessionTaskDelegate {
    // 禁止携带 x-akzio-token 的请求跟随 HTTP 重定向离开已验证的回环端点。
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
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
        // 客户端只接受本机 HTTP Observer；这里的检查发生在任何网络请求之前。
        guard endpoint.scheme == "http",
              let host = endpoint.host,
              host == "127.0.0.1" || host == "localhost" || host == "::1"
        else { throw ObserverClientError.invalidEndpoint }
        self.endpoint = endpoint
        self.token = token
    }

    func fetchSnapshot() async throws -> ObserverSnapshotPayload {
        // Snapshot 是页面的初始投影；Schema 不匹配时拒绝解码，避免 UI 猜测新字段语义。
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
        // Run detail 只读取指定 Run 的持久化投影；runID 本身不授予额外控制权限。
        let data = try await data(path: "v1/observer/runs/\(runID)")
        return try Self.decoder().decode(ObserverRunDetailPayload.self, from: data)
    }

    func fetchBlueprint(purpose: String) async throws -> WorkflowBlueprintPayload {
        // Blueprint 是当前 Core 已安装图的只读预览；purpose 由服务端再次校验。
        var components = URLComponents(url: endpoint.appending(path: "v1/workflows/blueprint"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "purpose", value: purpose)]
        let payload = try await data(url: components.url!, timeout: ObserverTransportPolicy.standardRequestTimeout)
        return try Self.decoder().decode(WorkflowBlueprintPayload.self, from: payload)
    }

    func fetchJournal(runID: String, after: Int64) async throws -> RuntimeJournalPayload {
        // Journal 使用游标分页，after 不会修改 Core 状态；limit 在客户端固定为有限值。
        var components = URLComponents(url: endpoint.appending(path: "v1/observer/runs/\(runID)/journal"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "after", value: String(after)), URLQueryItem(name: "limit", value: "50")]
        let payload = try await data(url: components.url!, timeout: ObserverTransportPolicy.standardRequestTimeout)
        return try Self.decoder().decode(RuntimeJournalPayload.self, from: payload)
    }

    func fetchPortfolioHistory(
        range: EquityRange
    ) async throws -> ObserverSectionPayload<ObserverPortfolioHistoryPayload> {
        // 只有 Core 支持的短窗口可从该 endpoint 读取；不支持的范围在 HTTP 前本地拒绝。
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
        // AsyncThrowingStream 把 SSE 的逐行解析暴露为异步序列；取消消费者时会取消底层 Task。
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
                    let (bytes, response) = try await ObserverTransportPolicy.session.bytes(for: request)
                    try Self.validate(response)
                    var eventName = "message"
                    for try await line in bytes.lines {
                        // 每次循环只处理一行；取消检查让关闭页面尽快停止网络读取。
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
                            // 未知事件不被推断成业务状态，保持向前兼容的忽略行为。
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
        // 统一请求路径负责认证、超时和 HTTP 响应检查；调用方只接收已验证的正文。
        var request = authorizedRequest(url: url)
        request.timeoutInterval = timeout
        let (data, response) = try await ObserverTransportPolicy.session.data(for: request)
        try Self.validate(response)
        return data
    }

    private func authorizedRequest(url: URL) -> URLRequest {
        // token 只放进发往已验证回环端点的请求 header，不写入 URL 或返回模型。
        var request = URLRequest(url: url)
        request.setValue(token, forHTTPHeaderField: "x-akzio-token")
        return request
    }

    private static func validate(_ response: URLResponse) throws {
        // URLSession 成功只代表传输完成；这里还要确认响应类型和精确的 HTTP 200。
        guard let response = response as? HTTPURLResponse else {
            throw ObserverClientError.invalidResponse
        }
        guard response.statusCode == 200 else {
            throw ObserverClientError.httpStatus(response.statusCode)
        }
    }

    static func decoder() -> JSONDecoder {
        // Core 的日期既可能带小数秒也可能不带；两种格式都解析，其他格式明确失败。
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
