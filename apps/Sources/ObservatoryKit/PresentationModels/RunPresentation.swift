import Foundation

// MARK: - Run header
//
// Everything the Run Status Bar needs. Elapsed time is a plain Int so the snapshot
// stays deterministic — no `Date()` reads anywhere in the presentation layer.
public struct RunPresentation: Sendable, Equatable {
    // RunPresentation 是 Observer/Mock 的运行头部投影；字段不可变，页面只能读取快照。
    public let runId: String
    public let purpose: RunPurpose
    public let status: WorkflowStatus
    public let topology: String
    public let model: String
    public let market: String
    public let startedAt: Date
    public let elapsedSeconds: Int
    public let systemHealthPpm: Int?
    public let marketOpen: Bool
    public let marketStatusKnown: Bool
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
        systemHealthPpm: Int?,
        marketOpen: Bool,
        marketStatusKnown: Bool = true,
        dataLive: Bool,
        dataStale: Bool = false,
        latencyMillis: Int?,
        brokerSession: String
    ) {
        // 初始化保留 Rust/Observer 已给出的 Optional 和状态，不在展示层补造实时值。
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
        self.marketStatusKnown = marketStatusKnown
        self.dataLive = dataLive
        self.dataStale = dataStale
        self.latencyMillis = latencyMillis
        self.brokerSession = brokerSession
    }

    /// `R-20260817-0930`: readable handle shown next to the full UUID.
    public var shortId: String {
        // DateFormatter 只把冻结 startedAt 格式化为短句柄，不读取当前墙上时间。
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyyMMdd-HHmm"
        return "R-" + formatter.string(from: startedAt)
    }

    // idPrefix 是完整 runId 的 UI 缩略，不是新的运行标识。
    public var idPrefix: String { String(runId.prefix(8)) }

    /// Paper Commit only applies to canonical Paper runs.
    // 订单资格沿用 RunPurpose 的投影，RunPresentation 不自行判断执行权限。
    public var submitsPaperOrders: Bool { purpose.submitsPaperOrders }

    // MissingValue.unavailable 只表示没有当前 run；它不等于某个失败状态。
    public var hasRun: Bool { runId.caseInsensitiveCompare(MissingValue.unavailable.rawValue) != .orderedSame }

    // 无 run 时使用明确占位文案，保留底层 WorkflowStatus 供其他页面读取。
    public var displayStatus: String { hasRun ? status.displayName : "No active run" }

    public var dataStatus: AkzioStatus {
        // stale 优先于 live，避免过期数据在 UI 上被误读为正在更新。
        if dataStale { return .stale }
        return dataLive ? .running : .queued
    }
}

// MARK: - Latest event

public struct EventPresentation: Sendable, Equatable, Identifiable {
    // 事件是 Observer 已排序的只读记录；Severity 的颜色和标签由 enum 映射。
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
        // 初始化不重新计算 relativeLabel，保留上游观察时刻的展示投影。
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
    // AgentRailItem 是 Overview 右栏的轻量投影，不持有 Agent 执行对象或异步生命周期。
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
        // progress/status 由上游快照共同提供，页面不在这里推导任务进度。
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
    // 健康指标保留 Optional fraction，nil 明确表示没有可绘制比例，而不是零。
    public let id: String
    public let label: String
    public let value: String
    /// `nil` keeps the gauge empty instead of drawing a fake zero.
    public let fraction: Double?
    public let isElevatedRisk: Bool

    public init(id: String, label: String, value: String, fraction: Double?, isElevatedRisk: Bool) {
        // 值语义初始化只复制观察投影；风险色调通过 tone 派生。
        self.id = id
        self.label = label
        self.value = value
        self.fraction = fraction
        self.isElevatedRisk = isElevatedRisk
    }

    // tone 是风险标记的展示映射，不能反向修改 isElevatedRisk。
    public var tone: AkzioTone { isElevatedRisk ? .coral : .gold }
}
