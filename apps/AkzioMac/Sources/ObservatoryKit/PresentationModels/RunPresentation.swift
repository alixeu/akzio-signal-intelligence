import Foundation

// MARK: - Run header
//
// Everything the Run Status Bar needs. Elapsed time is a plain Int so the snapshot
// stays deterministic — no `Date()` reads anywhere in the presentation layer.
public struct RunPresentation: Sendable, Equatable {
    public let runId: String
    public let purpose: RunPurpose
    public let status: WorkflowStatus
    public let topology: String
    public let model: String
    public let market: String
    public let startedAt: Date
    public let elapsedSeconds: Int
    public let systemHealthPpm: Int
    public let marketOpen: Bool
    public let dataLive: Bool
    public let dataStale: Bool
    public let latencyMillis: Int?
    public let brokerSession: String

    public init(
        runId: String,
        purpose: RunPurpose,
        status: WorkflowStatus,
        topology: String,
        model: String,
        market: String,
        startedAt: Date,
        elapsedSeconds: Int,
        systemHealthPpm: Int,
        marketOpen: Bool,
        dataLive: Bool,
        dataStale: Bool = false,
        latencyMillis: Int?,
        brokerSession: String
    ) {
        self.runId = runId
        self.purpose = purpose
        self.status = status
        self.topology = topology
        self.model = model
        self.market = market
        self.startedAt = startedAt
        self.elapsedSeconds = elapsedSeconds
        self.systemHealthPpm = systemHealthPpm
        self.marketOpen = marketOpen
        self.dataLive = dataLive
        self.dataStale = dataStale
        self.latencyMillis = latencyMillis
        self.brokerSession = brokerSession
    }

    /// `R-20260817-0930`: readable handle shown next to the full UUID.
    public var shortId: String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyyMMdd-HHmm"
        return "R-" + formatter.string(from: startedAt)
    }

    public var idPrefix: String { String(runId.prefix(8)) }

    /// Paper Commit only applies to canonical Paper runs.
    public var submitsPaperOrders: Bool { purpose.submitsPaperOrders }

    public var dataStatus: AkzioStatus {
        if dataStale { return .stale }
        return dataLive ? .running : .queued
    }
}

// MARK: - Latest event

public struct EventPresentation: Sendable, Equatable, Identifiable {
    public enum Severity: String, Sendable, Equatable {
        case info, notable, critical

        public var tone: AkzioTone {
            switch self {
            case .info: .neutral
            case .notable: .gold
            case .critical: .coral
            }
        }

        public var label: String {
            switch self {
            case .info: "Low"
            case .notable: "Medium"
            case .critical: "High"
            }
        }
    }

    public let id: String
    public let title: String
    public let detail: String
    public let severity: Severity
    public let symbol: String
    public let timestamp: Date
    public let relativeLabel: String

    public init(
        id: String,
        title: String,
        detail: String,
        severity: Severity,
        symbol: String,
        timestamp: Date,
        relativeLabel: String
    ) {
        self.id = id
        self.title = title
        self.detail = detail
        self.severity = severity
        self.symbol = symbol
        self.timestamp = timestamp
        self.relativeLabel = relativeLabel
    }
}

// MARK: - Active agents rail

public struct AgentRailItem: Sendable, Equatable, Identifiable {
    public let id: String
    public let name: String
    public let role: AgentRole
    public let model: String
    public let status: AkzioStatus
    public let activityLabel: String
    public let progressPpm: Int

    public init(
        id: String,
        name: String,
        role: AgentRole,
        model: String,
        status: AkzioStatus,
        activityLabel: String,
        progressPpm: Int
    ) {
        self.id = id
        self.name = name
        self.role = role
        self.model = model
        self.status = status
        self.activityLabel = activityLabel
        self.progressPpm = progressPpm
    }
}

// MARK: - Health snapshot

public struct HealthMetric: Sendable, Equatable, Identifiable {
    public let id: String
    public let label: String
    public let value: String
    /// `nil` keeps the gauge empty instead of drawing a fake zero.
    public let fraction: Double?
    public let isElevatedRisk: Bool

    public init(id: String, label: String, value: String, fraction: Double?, isElevatedRisk: Bool) {
        self.id = id
        self.label = label
        self.value = value
        self.fraction = fraction
        self.isElevatedRisk = isElevatedRisk
    }

    public var tone: AkzioTone { isElevatedRisk ? .coral : .gold }
}
