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

enum JSONValue: Decodable, Sendable {
    case object([String: JSONValue])
    case array([JSONValue])
    case string(String)
    case number(Double)
    case bool(Bool)
    case null

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() { self = .null }
        else if let value = try? container.decode(Bool.self) { self = .bool(value) }
        else if let value = try? container.decode(Double.self) { self = .number(value) }
        else if let value = try? container.decode(String.self) { self = .string(value) }
        else if let value = try? container.decode([JSONValue].self) { self = .array(value) }
        else { self = .object(try container.decode([String: JSONValue].self)) }
    }

    subscript(_ key: String) -> JSONValue? {
        guard case .object(let values) = self else { return nil }
        return values[key]
    }

    var string: String? {
        guard case .string(let value) = self else { return nil }
        return value
    }

    var int: Int? {
        guard case .number(let value) = self else { return nil }
        return Int(exactly: value)
    }

    var int64: Int64? {
        guard case .number(let value) = self else { return nil }
        return Int64(exactly: value)
    }

    var array: [JSONValue]? {
        guard case .array(let value) = self else { return nil }
        return value
    }

    var object: [String: JSONValue]? {
        guard case .object(let value) = self else { return nil }
        return value
    }

    var bool: Bool? {
        guard case .bool(let value) = self else { return nil }
        return value
    }

    var prettyPrinted: String {
        let object = foundationObject
        guard JSONSerialization.isValidJSONObject(object),
              let data = try? JSONSerialization.data(
                  withJSONObject: object,
                  options: [.prettyPrinted, .sortedKeys]
              ),
              let value = String(data: data, encoding: .utf8)
        else {
            return String(describing: object)
        }
        return value
    }

    private var foundationObject: Any {
        switch self {
        case .object(let values):
            values.mapValues(\.foundationObject)
        case .array(let values):
            values.map(\.foundationObject)
        case .string(let value):
            value
        case .number(let value):
            value
        case .bool(let value):
            value
        case .null:
            NSNull()
        }
    }
}

struct ObserverSnapshotPayload: Decodable, Sendable {
    let schemaVersion: Int
    let generatedAt: Date
    let eventCursor: Int64
    let core: ObserverCorePayload
    let currentRun: ObserverRunDetailPayload?
    let recentRuns: [ObserverWorkflowPayload]
    let runSummaries: [ObserverRunSummaryPayload]?
    let portfolio: ObserverSectionPayload<ObserverPortfolioPayload>
    let outcome: ObserverSectionPayload<ObserverOutcomePayload>?
    let learning: ObserverSectionPayload<ObserverLearningPayload>

    var health: ObserverHealthPayload { core.health }
    var runs: [ObserverWorkflowPayload] { recentRuns }

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedAt = "generated_at"
        case eventCursor = "event_cursor"
        case core
        case currentRun = "current_run"
        case recentRuns = "recent_runs"
        case runSummaries = "run_summaries"
        case portfolio
        case outcome
        case learning
    }
}

struct ObserverCorePayload: Decodable, Sendable {
    let ready: Bool
    let readinessPpm: UInt32?
    let autoPaper: Bool
    let health: ObserverHealthPayload
    let approval: ObserverApprovalPayload

    enum CodingKeys: String, CodingKey {
        case ready
        case readinessPpm = "readiness_ppm"
        case autoPaper = "auto_paper"
        case health
        case approval
    }
}

struct ObserverApprovalPayload: Decodable, Sendable {
    let status: String
    let operatorIdentity: String?
    let reason: String?
    let expiresAt: Date?

    enum CodingKeys: String, CodingKey {
        case status
        case operatorIdentity = "operator_identity"
        case reason
        case expiresAt = "expires_at"
    }
}

struct ObserverHealthPayload: Decodable, Sendable {
    let status: String
    let frozen: Bool
    let schedulerOwner: String?
    let schedulerEpoch: UInt64?
    let alerts: [ObserverAlertPayload]

    enum CodingKeys: String, CodingKey {
        case status
        case frozen
        case schedulerOwner = "scheduler_owner"
        case schedulerEpoch = "scheduler_epoch"
        case alerts
    }
}

struct ObserverAlertPayload: Decodable, Sendable {
    let code: String
    let severity: String
    let count: UInt64
}

struct ObserverSectionPayload<T: Decodable & Sendable>: Decodable, Sendable {
    let status: String
    let observedAt: Date?
    let reason: String?
    let data: T?

    enum CodingKeys: String, CodingKey {
        case status
        case observedAt = "observed_at"
        case reason
        case data
    }
}

struct ObserverRunDetailPayload: Decodable, Sendable {
    let workflow: ObserverWorkflowPayload
    let events: [ObserverEventPayload]
    let trajectory: [ObserverTrajectoryPayload]
    let artifacts: [ObserverArtifactPayload]
    let telemetry: ObserverRunTelemetryPayload?
}

struct ObserverRunTelemetryPayload: Decodable, Sendable {
    let modelID: String?
    let latencyMillis: UInt64?
    let inputTokens: UInt64?
    let outputTokens: UInt64?
    let toolCalls: Int
    let turns: Int

    enum CodingKeys: String, CodingKey {
        case modelID = "model_id"
        case latencyMillis = "latency_millis"
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case toolCalls = "tool_calls"
        case turns
    }
}

struct ObserverRunSummaryPayload: Decodable, Sendable {
    let runID: String
    let modelID: String?
    let latencyMillis: UInt64?
    let brokerSession: String?
    let resultUtilityPpm: Int64?

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
        case modelID = "model_id"
        case latencyMillis = "latency_millis"
        case brokerSession = "broker_session"
        case resultUtilityPpm = "result_utility_ppm"
    }
}

struct ObserverEventPayload: Decodable, Sendable {
    let cursor: Int64
    let eventType: String
    let taskID: String?
    let createdAt: Date

    enum CodingKeys: String, CodingKey {
        case cursor
        case eventType = "event_type"
        case taskID = "task_id"
        case createdAt = "created_at"
    }
}

struct ObserverTrajectoryPayload: Decodable, Sendable {
    let cursor: Int64
    let taskID: String?
    let turn: UInt32?
    let phase: String?
    let assistantText: String?
    let eventType: String
    let artifactID: String?
    let artifactKind: String?
    let model: ObserverModelMetadataPayload?
    let latencyMillis: UInt64?
    let inputTokens: UInt64?
    let outputTokens: UInt64?
    let tool: ObserverToolLifecyclePayload?
    let deliberation: ObserverDeliberationPayload?
    let outputRefs: [ObserverArtifactReferencePayload]?

    enum CodingKeys: String, CodingKey {
        case cursor
        case taskID = "task_id"
        case turn
        case phase
        case assistantText = "assistant_text"
        case eventType = "event_type"
        case artifactID = "artifact_id"
        case artifactKind = "artifact_kind"
        case model
        case latencyMillis = "latency_millis"
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case tool
        case deliberation
        case outputRefs = "output_refs"
    }
}

struct ObserverToolLifecyclePayload: Decodable, Sendable {
    let callID: String?
    let name: String?
    let lifecycle: String

    enum CodingKeys: String, CodingKey {
        case callID = "call_id"
        case name
        case lifecycle
    }
}

struct ObserverArtifactReferencePayload: Decodable, Sendable {
    let artifactID: String
    let kind: String

    enum CodingKeys: String, CodingKey {
        case artifactID = "artifact_id"
        case kind
    }
}

struct ObserverModelMetadataPayload: Decodable, Sendable {
    let providerID: String?
    let modelID: String?
    let reasoningEffort: String?

    enum CodingKeys: String, CodingKey {
        case providerID = "provider_id"
        case modelID = "model_id"
        case reasoningEffort = "reasoning_effort"
    }
}

struct ObserverDeliberationPayload: Decodable, Sendable {
    let selectedPath: String
    let alternatives: [String]
    let alternativeMatchPpm: [UInt32]?
    let uncertainties: [String]
    let uncertaintyWeightPpm: [UInt32]?
    let assessmentSource: String?
    let basisArtifactIDs: [String]
    let confidencePpm: UInt32

    enum CodingKeys: String, CodingKey {
        case selectedPath = "selected_path"
        case alternatives
        case alternativeMatchPpm = "alternative_match_ppm"
        case uncertainties
        case uncertaintyWeightPpm = "uncertainty_weight_ppm"
        case assessmentSource = "assessment_source"
        case basisArtifactIDs = "basis_artifact_ids"
        case confidencePpm = "confidence_ppm"
    }
}

struct ObserverArtifactPayload: Decodable, Sendable {
    let artifactID: String
    let kind: String
    let createdAt: Date
    let payload: JSONValue

    enum CodingKeys: String, CodingKey {
        case artifactID = "artifact_id"
        case kind
        case createdAt = "created_at"
        case payload
    }
}

struct ObserverLearningPayload: Decodable, Sendable {
    let artifacts: [ObserverArtifactPayload]
    let policyTransitions: [JSONValue]
    let summary: ObserverLearningSummaryPayload?
    let policyMetrics: [ObserverPolicyMetricPayload]?

    enum CodingKeys: String, CodingKey {
        case artifacts
        case policyTransitions = "policy_transitions"
        case summary
        case policyMetrics = "policy_metrics"
    }
}

struct ObserverLearningSummaryPayload: Decodable, Sendable {
    let rangeDays: UInt32
    let attributedUtilityMicros: Int64?
    let attributedUtilityPpm: Int64?
    let lessonCandidates: Int
    let lessonCandidatesDelta: Int64
    let policiesEvolved: Int
    let policiesEvolvedDelta: Int64
    let impactAreas: [ObserverImpactAreaPayload]

    enum CodingKeys: String, CodingKey {
        case rangeDays = "range_days"
        case attributedUtilityMicros = "attributed_utility_micros"
        case attributedUtilityPpm = "attributed_utility_ppm"
        case lessonCandidates = "lesson_candidates"
        case lessonCandidatesDelta = "lesson_candidates_delta"
        case policiesEvolved = "policies_evolved"
        case policiesEvolvedDelta = "policies_evolved_delta"
        case impactAreas = "impact_areas"
    }
}

struct ObserverImpactAreaPayload: Decodable, Sendable {
    let category: String
    let impactPpm: Int64

    enum CodingKeys: String, CodingKey {
        case category
        case impactPpm = "impact_ppm"
    }
}

struct ObserverPolicyMetricPayload: Decodable, Sendable {
    let subject: JSONValue
    let state: JSONValue
    let sampleCount: Int
    let winRatePpm: Int64?
    let netImpactPpm: Int64?
    let stabilityPpm: Int64?
    let exposurePpm: UInt32?

    enum CodingKeys: String, CodingKey {
        case subject
        case state
        case sampleCount = "sample_count"
        case winRatePpm = "win_rate_ppm"
        case netImpactPpm = "net_impact_ppm"
        case stabilityPpm = "stability_ppm"
        case exposurePpm = "exposure_ppm"
    }
}

struct ObserverPortfolioPayload: Decodable, Sendable {
    let brokerSession: String
    let marketOpen: Bool
    let status: String
    let equityMicros: Int64
    let lastEquityMicros: Int64?
    let buyingPowerMicros: Int64
    let dayPnlMicros: Int64?
    let dayPnlPpm: Int64?
    let realizedPnlMicros: Int64?
    let realizedPnlPpm: Int64?
    let fills: ObserverSectionPayload<[ObserverBrokerFillPayload]>?
    let analytics: ObserverSectionPayload<ObserverPortfolioAnalyticsPayload>?
    let positions: [ObserverPositionPayload]

    enum CodingKeys: String, CodingKey {
        case brokerSession = "broker_session"
        case marketOpen = "market_open"
        case status
        case equityMicros = "equity_micros"
        case lastEquityMicros = "last_equity_micros"
        case buyingPowerMicros = "buying_power_micros"
        case dayPnlMicros = "day_pnl_micros"
        case dayPnlPpm = "day_pnl_ppm"
        case realizedPnlMicros = "realized_pnl_micros"
        case realizedPnlPpm = "realized_pnl_ppm"
        case fills
        case analytics
        case positions
    }
}

struct ObserverPositionPayload: Decodable, Sendable {
    let symbol: String
    let quantityMicros: Int64
    let marketValueMicros: Int64
    let averageEntryPriceMicros: Int64?
    let unrealizedPnlMicros: Int64?
    let unrealizedPnlPpm: Int64?
    let sparklinePpm: [Int64]?

    enum CodingKeys: String, CodingKey {
        case symbol
        case quantityMicros = "quantity_micros"
        case marketValueMicros = "market_value_micros"
        case averageEntryPriceMicros = "average_entry_price_micros"
        case unrealizedPnlMicros = "unrealized_pnl_micros"
        case unrealizedPnlPpm = "unrealized_pnl_ppm"
        case sparklinePpm = "sparkline_ppm"
    }
}

struct ObserverBrokerFillPayload: Decodable, Sendable {
    let activityID: String
    let brokerOrderID: String
    let symbol: String
    let side: String
    let quantityMicros: Int64
    let priceMicros: Int64
    let transactionAt: Date
    let venue: String?
    let source: String

    enum CodingKeys: String, CodingKey {
        case activityID = "activity_id"
        case brokerOrderID = "broker_order_id"
        case symbol
        case side
        case quantityMicros = "quantity_micros"
        case priceMicros = "price_micros"
        case transactionAt = "transaction_at"
        case venue
        case source
    }
}

struct ObserverPortfolioAnalyticsPayload: Decodable, Sendable {
    let benchmarkSymbol: String
    let lookback: String
    let sampleCount: Int
    let betaPpm: Int64?
    let volatilityPpm: Int64
    let maxDrawdownPpm: Int64
    let var95Micros: Int64

    enum CodingKeys: String, CodingKey {
        case benchmarkSymbol = "benchmark_symbol"
        case lookback
        case sampleCount = "sample_count"
        case betaPpm = "beta_ppm"
        case volatilityPpm = "volatility_ppm"
        case maxDrawdownPpm = "max_drawdown_ppm"
        case var95Micros = "var_95_micros"
    }
}

struct ObserverOutcomePayload: Decodable, Sendable {
    let outcomeID: String
    let completedTradingSessions: UInt8
    let horizons: [ObserverOutcomeHorizonPayload]

    enum CodingKeys: String, CodingKey {
        case outcomeID = "outcome_id"
        case completedTradingSessions = "completed_trading_sessions"
        case horizons
    }
}

struct ObserverOutcomeHorizonPayload: Decodable, Sendable {
    let horizon: String
    let progressPpm: UInt32
    let window: ObserverOutcomeWindowPayload?
    let sampleCount: Int
    let winRatePpm: Int64?
    let profitFactorPpm: Int64?
    let sharpePpm: Int64?
    let maxDrawdownPpm: Int64?
    let comparison: [ObserverOutcomeComparisonPointPayload]

    enum CodingKeys: String, CodingKey {
        case horizon
        case progressPpm = "progress_ppm"
        case window
        case sampleCount = "sample_count"
        case winRatePpm = "win_rate_ppm"
        case profitFactorPpm = "profit_factor_ppm"
        case sharpePpm = "sharpe_ppm"
        case maxDrawdownPpm = "max_drawdown_ppm"
        case comparison
    }
}

struct ObserverOutcomeWindowPayload: Decodable, Sendable {
    let horizon: String
    let observedTradingDay: String
    let portfolioReturnPpm: Int64
    let benchmarkReturnPpm: Int64
    let transactionCostPpm: UInt32
    let slippagePpm: UInt32
    let utilityPpm: Int64
    let calibrationPpm: UInt32?
    let evidenceCompletenessPpm: UInt32
    let riskRecallPpm: UInt32?

    enum CodingKeys: String, CodingKey {
        case horizon
        case observedTradingDay = "observed_trading_day"
        case portfolioReturnPpm = "portfolio_return_ppm"
        case benchmarkReturnPpm = "benchmark_return_ppm"
        case transactionCostPpm = "transaction_cost_ppm"
        case slippagePpm = "slippage_ppm"
        case utilityPpm = "utility_ppm"
        case calibrationPpm = "calibration_ppm"
        case evidenceCompletenessPpm = "evidence_completeness_ppm"
        case riskRecallPpm = "risk_recall_ppm"
    }
}

struct ObserverOutcomeComparisonPointPayload: Decodable, Sendable {
    let tradingDay: String
    let portfolioPpm: Int64
    let benchmarkPpm: Int64

    enum CodingKeys: String, CodingKey {
        case tradingDay = "trading_day"
        case portfolioPpm = "portfolio_ppm"
        case benchmarkPpm = "benchmark_ppm"
    }
}

struct ObserverPortfolioHistoryPayload: Decodable, Sendable {
    let range: String
    let benchmarkSymbol: String?
    let points: [ObserverPortfolioHistoryPointPayload]

    enum CodingKeys: String, CodingKey {
        case range
        case benchmarkSymbol = "benchmark_symbol"
        case points
    }
}

struct ObserverPortfolioHistoryPointPayload: Decodable, Sendable {
    let timestamp: Date
    let equityMicros: Int64
    let profitLossMicros: Int64?
    let profitLossPpm: Int64?
    let benchmarkEquityMicros: Int64?

    enum CodingKeys: String, CodingKey {
        case timestamp
        case equityMicros = "equity_micros"
        case profitLossMicros = "profit_loss_micros"
        case profitLossPpm = "profit_loss_ppm"
        case benchmarkEquityMicros = "benchmark_equity_micros"
    }
}

struct ObserverWorkflowPayload: Decodable, Sendable {
    let run: ObserverRunPayload
    let status: String
    let finishedAt: Date?
    let tasks: [ObserverTaskPayload]
    let eventCursor: Int64

    enum CodingKeys: String, CodingKey {
        case run
        case status
        case finishedAt = "finished_at"
        case tasks
        case eventCursor = "event_cursor"
    }
}

struct ObserverRunPayload: Decodable, Sendable {
    let runID: String
    let purpose: String
    let topologyID: String
    let createdAt: Date

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
        case purpose
        case topologyID = "topology_id"
        case createdAt = "created_at"
    }
}

struct ObserverTaskPayload: Decodable, Sendable {
    let node: ObserverNodePayload
    let taskStatus: String
    let readyAt: Date
    let attemptCount: UInt64
    let finishedAt: Date?

    enum CodingKeys: String, CodingKey {
        case node
        case taskStatus = "status"
        case readyAt = "ready_at"
        case attemptCount = "attempt_count"
        case finishedAt = "finished_at"
    }
}

struct ObserverNodePayload: Decodable, Sendable {
    let taskID: String
    let recipeID: String
    let objective: String
    let dependencies: [String]

    enum CodingKeys: String, CodingKey {
        case taskID = "task_id"
        case recipeID = "recipe_id"
        case objective
        case dependencies
    }
}

private struct ObserverInvalidationPayload: Decodable {
    let cursor: Int64
}

struct ObserverReasoningEventPayload: Decodable, Sendable {
    let type: String
    let runID: String
    let taskID: String
    let attemptID: String
    let purpose: String
    let turn: UInt16
    let delta: String?

    enum CodingKeys: String, CodingKey {
        case type
        case runID = "run_id"
        case taskID = "task_id"
        case attemptID = "attempt_id"
        case purpose
        case turn
        case delta
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

    fileprivate static func decoder() -> JSONDecoder {
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

public struct ObserverContractSummary: Sendable, Equatable {
    public let schemaVersion: Int
    public let eventCursor: Int64
    public let runCount: Int
    public let firstRunID: String?
    public let readinessPpm: Int?
    public let outcomeStatus: String?
}

public struct ObserverTraceContractSummary: Sendable, Equatable {
    public let artifactID: String?
    public let toolCallID: String?
    public let toolName: String?
    public let toolLifecycle: String?
    public let outputReferenceIDs: [String]
    public let structuredArtifactKinds: [String]
    public let firstStructuredPayload: String?
}

public struct ObserverReasoningContractSummary: Sendable, Equatable {
    public let type: String
    public let runID: String
    public let taskID: String
    public let purpose: String
    public let turn: UInt16
    public let delta: String?
}

public enum ObserverContractProbe {
    public static func decode(_ data: Data) throws -> ObserverContractSummary {
        let payload = try ObserverClient.decoder().decode(ObserverSnapshotPayload.self, from: data)
        return ObserverContractSummary(
            schemaVersion: payload.schemaVersion,
            eventCursor: payload.eventCursor,
            runCount: payload.recentRuns.count,
            firstRunID: payload.recentRuns.first?.run.runID,
            readinessPpm: payload.core.readinessPpm.map(Int.init),
            outcomeStatus: payload.outcome?.status
        )
    }

    public static func decodeTrace(_ data: Data) throws -> ObserverTraceContractSummary {
        let payload = try ObserverClient.decoder().decode(ObserverTraceEnvelope.self, from: data)
        let first = payload.trajectory.first
        return ObserverTraceContractSummary(
            artifactID: first?.artifactID,
            toolCallID: first?.tool?.callID,
            toolName: first?.tool?.name,
            toolLifecycle: first?.tool?.lifecycle,
            outputReferenceIDs: first?.outputRefs?.map(\.artifactID) ?? [],
            structuredArtifactKinds: payload.artifacts.map(\.kind),
            firstStructuredPayload: payload.artifacts.first?.payload.prettyPrinted
        )
    }

    public static func decodeReasoning(_ data: Data) throws -> ObserverReasoningContractSummary {
        let payload = try ObserverClient.decoder().decode(
            ObserverReasoningEventPayload.self,
            from: data
        )
        return ObserverReasoningContractSummary(
            type: payload.type,
            runID: payload.runID,
            taskID: payload.taskID,
            purpose: payload.purpose,
            turn: payload.turn,
            delta: payload.delta
        )
    }
}

private struct ObserverTraceEnvelope: Decodable {
    let trajectory: [ObserverTrajectoryPayload]
    let artifacts: [ObserverArtifactPayload]
}
