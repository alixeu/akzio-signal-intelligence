import Foundation

// MARK: - Run vocabulary
//
// Mirrors `akzio-domain` exactly. `rawValue` is the serde wire name so the UI and
// the Rust store speak the same language even though this build reads mock data.

/// `RunPurpose` — crates/akzio-domain/src/core.rs:307
public enum RunPurpose: String, CaseIterable, Codable, Hashable, Sendable {
    // Codable 的 rawValue 是 Rust serde wire name；Swift 展示文本由后续 computed property 提供。
    case paper
    case debug
    case positionPlan = "position_plan"
    case paperDryRun = "paper_dry_run"
    case replay
    case shadow

    public var displayName: String {
        // displayName 只负责本地 UI 的稳定英文标签，不参与编码或权限判断。
        switch self {
        case .paper: "Paper"
        case .debug: "Debug"
        case .positionPlan: "Position Plan"
        case .paperDryRun: "Paper Dry Run"
        case .replay: "Replay"
        case .shadow: "Shadow"
        }
    }

    /// Only canonical Paper runs submit broker orders; everything else must render
    /// Paper Commit as `Not Applicable`.
    // submitsPaperOrders/isCanonical 是展示边界上的只读投影，不能被页面状态覆盖。
    public var submitsPaperOrders: Bool { self == .paper }

    /// Only scheduler-owned Paper runs may feed canonical learning.
    public var isCanonical: Bool { self == .paper }

    public var tone: AkzioTone {
        // 色调是页面语义映射；同一 purpose 的 wire 值仍保持不变。
        switch self {
        case .paper: .gold
        case .shadow: .gold
        case .debug, .positionPlan, .paperDryRun, .replay: .neutral
        }
    }

    public static let userLaunchModes: [RunPurpose] = [.positionPlan, .paper]

    public var launchModeName: String {
        // 启动模式文案由 purpose 派生，列表顺序由 userLaunchModes 固定。
        switch self {
        case .paper: "完整模式 · Paper"
        case .positionPlan: "仅生成仓位计划"
        default: displayName
        }
    }

    public var launchModeSummary: String {
        switch self {
        case .paper: "投研 → 决策 → 执行检查 → Paper → 对账 → 后续评估"
        case .positionPlan: "Position plan · No execution"
        default: displayName
        }
    }

    public var launchModeDescription: String {
        switch self {
        case .paper:
            "运行完整 Paper 流程；实际下单仍需有效审批和全部 Gate 通过。"
        case .positionPlan:
            "Generate target positions and stop before execution."
        default:
            displayName
        }
    }
}

/// `WorkflowStatus` — core.rs:344
public enum WorkflowStatus: String, CaseIterable, Sendable {
    // status rawValue 直接对应 Core 状态；Swift 额外提供 AkzioStatus 和显示标签映射。
    case queued
    case leased
    case running
    case decisionCompleted = "decision_completed"
    case completed
    case completedWithExecutionRejection = "completed_with_execution_rejection"
    case failed
    case cancelled

    public var status: AkzioStatus {
        // status 把 Rust workflow 状态转换为设计系统状态，不丢失原始 WorkflowStatus。
        switch self {
        case .queued: .queued
        case .leased: .leased
        case .running: .running
        case .decisionCompleted: .accepted
        case .completed: .completed
        case .completedWithExecutionRejection: .completedWithRejection
        case .failed: .failed
        case .cancelled: .cancelled
        }
    }

    public var displayName: String {
        // 特殊状态使用更明确的页面文案，其余复用 status 的设计系统标签。
        switch self {
        case .decisionCompleted: "Decision Completed"
        case .completedWithExecutionRejection: "Completed · Rejected"
        default: status.style.label
        }
    }
}

/// `TaskStatus` — core.rs:323
public enum TaskStatus: String, CaseIterable, Sendable {
    // TaskStatus 保留任务生命周期 wire 值；是否终态与页面颜色分别由 computed property 映射。
    case pending
    case leased
    case running
    case succeeded
    case failed
    case cancelled
    case skipped

    public var isTerminal: Bool {
        // 终态判断只基于任务生命周期，不把业务 Outcome 或成交状态混入。
        switch self {
        case .succeeded, .failed, .cancelled, .skipped: true
        case .pending, .leased, .running: false
        }
    }

    /// A skipped *optional* step is "Not Triggered", not a success and not a failure.
    public func status(optional: Bool, applicable: Bool = true) -> AkzioStatus {
        // optional/applicable 是展示层的两项上下文，负责把 skipped 区分为未触发或不适用。
        if !applicable { return .notApplicable }
        switch self {
        case .pending: return .queued
        case .leased: return .leased
        case .running: return .running
        case .succeeded: return .succeeded
        case .failed: return .failed
        case .cancelled: return .cancelled
        case .skipped: return optional ? .notTriggered : .skipped
        }
    }
}

/// `Asset` — core.rs:59 (serde SCREAMING_SNAKE_CASE)
public enum TradableAsset: String, CaseIterable, Sendable, Identifiable {
    // rawValue 保留 Rust 资产 wire name；Cash 是派生余额标签，不扩展资产枚举。
    case tqqq = "TQQQ"
    case qqq = "QQQ"
    case soxx = "SOXX"
    case soxl = "SOXL"

    public var id: String { rawValue }

    public var longName: String {
        // 长名称只供页面辅助文本，资产 ID 仍由 rawValue 提供。
        switch self {
        case .tqqq: "ProShares UltraPro QQQ"
        case .qqq: "Invesco QQQ Trust"
        case .soxx: "iShares Semiconductor ETF"
        case .soxl: "Direxion Daily SOXL Bull 3X"
        }
    }

    /// Cash is a derived balance, not an `Asset` variant — kept separate on purpose.
    // cashLabel 明确表示组合汇总中的派生余额，不可作为可交易资产传入交易模型。
    public static let cashLabel = "Cash"
}
