import Foundation

// MARK: - Run vocabulary
//
// Mirrors `akzio-domain` exactly. `rawValue` is the serde wire name so the UI and
// the Rust store speak the same language even though this build reads mock data.

/// `RunPurpose` — crates/akzio-domain/src/core.rs:307
public enum RunPurpose: String, CaseIterable, Codable, Hashable, Sendable {
    case paper
    case debug
    case positionPlan = "position_plan"
    case paperDryRun = "paper_dry_run"
    case replay
    case shadow

    public var displayName: String {
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
    public var submitsPaperOrders: Bool { self == .paper }

    /// Only scheduler-owned Paper runs may feed canonical learning.
    public var isCanonical: Bool { self == .paper }

    public var tone: AkzioTone {
        switch self {
        case .paper: .gold
        case .shadow: .gold
        case .debug, .positionPlan, .paperDryRun, .replay: .neutral
        }
    }

    public static let userLaunchModes: [RunPurpose] = [.debug, .positionPlan]

    public var launchModeName: String {
        switch self {
        case .debug: "Full research"
        case .positionPlan: "Position plan only"
        default: displayName
        }
    }

    public var launchModeSummary: String {
        switch self {
        case .debug: "Full research · Debug safety mode"
        case .positionPlan: "Position plan · No execution"
        default: displayName
        }
    }

    public var launchModeDescription: String {
        switch self {
        case .debug:
            "Run a full real-model research workflow. Paper submission remains scheduler-owned."
        case .positionPlan:
            "Generate target positions and stop before execution."
        default:
            displayName
        }
    }
}

/// `WorkflowStatus` — core.rs:344
public enum WorkflowStatus: String, CaseIterable, Sendable {
    case queued
    case leased
    case running
    case decisionCompleted = "decision_completed"
    case completed
    case completedWithExecutionRejection = "completed_with_execution_rejection"
    case failed
    case cancelled

    public var status: AkzioStatus {
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
        switch self {
        case .decisionCompleted: "Decision Completed"
        case .completedWithExecutionRejection: "Completed · Rejected"
        default: status.style.label
        }
    }
}

/// `TaskStatus` — core.rs:323
public enum TaskStatus: String, CaseIterable, Sendable {
    case pending
    case leased
    case running
    case succeeded
    case failed
    case cancelled
    case skipped

    public var isTerminal: Bool {
        switch self {
        case .succeeded, .failed, .cancelled, .skipped: true
        case .pending, .leased, .running: false
        }
    }

    /// A skipped *optional* step is "Not Triggered", not a success and not a failure.
    public func status(optional: Bool, applicable: Bool = true) -> AkzioStatus {
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
    case tqqq = "TQQQ"
    case qqq = "QQQ"
    case soxx = "SOXX"
    case soxl = "SOXL"

    public var id: String { rawValue }

    public var longName: String {
        switch self {
        case .tqqq: "ProShares UltraPro QQQ"
        case .qqq: "Invesco QQQ Trust"
        case .soxx: "iShares Semiconductor ETF"
        case .soxl: "Direxion Daily SOXL Bull 3X"
        }
    }

    /// Cash is a derived balance, not an `Asset` variant — kept separate on purpose.
    public static let cashLabel = "Cash"
}
