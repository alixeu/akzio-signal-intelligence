import Foundation

// MARK: - Decision & execution vocabulary

/// `HardBlocker` — crates/akzio-domain/src/decision.rs:13 (all 22 variants)
public enum HardBlocker: String, CaseIterable, Sendable, Identifiable {
    // 每个 rawValue 对应 Rust Decision/Execution 的阻断 wire 值，页面只读取不重分类。
    case unsupportedUniverse = "unsupported_universe"
    case noExecutableOrder = "no_executable_order"
    case frozen
    case missingEvidence = "missing_evidence"
    case invalidProvenance = "invalid_provenance"
    case materialConflict = "material_conflict"
    case staleQuote = "stale_quote"
    case invalidQuote = "invalid_quote"
    case missingQuote = "missing_quote"
    case staleAccount = "stale_account"
    case missingAccount = "missing_account"
    case marketClosed = "market_closed"
    case factorLimit = "factor_limit"
    case pairExposureLimit = "pair_exposure_limit"
    case turnoverLimit = "turnover_limit"
    case planHashMismatch = "plan_hash_mismatch"
    case duplicateCommitment = "duplicate_commitment"
    case nonPaperEndpoint = "non_paper_endpoint"
    case nonCanonicalRun = "non_canonical_run"
    case recoveryIncomplete = "recovery_incomplete"
    case externalPosition = "external_position"
    case unmanagedOpenOrder = "unmanaged_open_order"

    public var id: String { rawValue }

    public var displayName: String {
        // split/map/join 闭包把 snake_case wire 值转换为可读标签，原始值保持可追踪。
        rawValue
            .split(separator: "_")
            .map { $0.capitalized }
            .joined(separator: " ")
    }

    /// Which gate surfaces the blocker, so the DAG can attach it to the right node.
    public var gate: GateKind {
        // gate 只决定阻断在 DAG 哪个 Gate 节点展示，不改变 blocker 本身。
        switch self {
        case .missingEvidence, .invalidProvenance, .staleQuote, .missingQuote,
             .staleAccount, .missingAccount, .unsupportedUniverse:
            .evidence
        case .materialConflict, .noExecutableOrder, .frozen, .nonCanonicalRun:
            .decision
        default:
            .execution
        }
    }
}

public enum GateKind: String, Sendable {
    // GateKind 是 HardBlocker 的展示归属枚举，不承载 Gate 执行逻辑。
    case evidence
    case decision
    case execution

    public var displayName: String {
        switch self {
        case .evidence: "Evidence Gate"
        case .decision: "Decision Gate"
        case .execution: "Execution Gate"
        }
    }
}

/// `SoftWarning` — decision.rs:39
public enum SoftWarning: String, CaseIterable, Sendable, Identifiable {
    // warning 是非阻断提示；rawValue 与 Rust wire 名称保持一致。
    case lowConfidence = "low_confidence"
    case incompleteEvidence = "incomplete_evidence"
    case elevatedTurnover = "elevated_turnover"
    case slowModelResponse = "slow_model_response"
    case staleNoncriticalEvidence = "stale_noncritical_evidence"

    public var id: String { rawValue }

    public var displayName: String {
        // 闭包只格式化标签，不改变 warning 的严重级别或触发条件。
        rawValue.split(separator: "_").map { $0.capitalized }.joined(separator: " ")
    }
}

/// `OrderReceiptState` — execution.rs:509. The reference image shows
/// "Pending / Working"; those are AI-generated labels and are not used.
public enum OrderReceiptState: String, CaseIterable, Sendable {
    // receipt state 只表示 Broker 回执生命周期；页面通过 status 映射为统一 AkzioStatus。
    case accepted
    case partiallyFilled = "partially_filled"
    case filled
    case canceled
    case rejected
    case failed

    public var status: AkzioStatus {
        // 这是单向展示映射，不能由状态颜色反推订单回执细节。
        switch self {
        case .accepted: .accepted
        case .partiallyFilled: .partial
        case .filled: .completed
        case .canceled: .cancelled
        case .rejected: .rejected
        case .failed: .failed
        }
    }

    public var displayName: String { status.style.label }
}

/// `ReconciliationState` — execution.rs:632
public enum ReconciliationState: String, CaseIterable, Sendable {
    // 对账状态独立于订单状态，保留 pending/partial/complete/failed 的原始语义。
    case pending
    case partial
    case complete
    case failed

    public var status: AkzioStatus {
        switch self {
        case .pending: .queued
        case .partial: .partial
        case .complete: .completed
        case .failed: .failed
        }
    }
}

/// `ExecutionVerdict` — execution.rs:436. `NoOrder` is a first-class outcome,
/// never a failure.
public enum ExecutionVerdictKind: String, Sendable {
    // NoOrder 是正式业务结果；status 映射为 notApplicable 而不是 failed。
    case accepted
    case noOrder = "no_order"

    public var status: AkzioStatus {
        switch self {
        case .accepted: .accepted
        case .noOrder: .notApplicable
        }
    }

    public var displayName: String {
        switch self {
        case .accepted: "Accepted"
        case .noOrder: "No Order"
        }
    }
}

public enum OrderSide: String, CaseIterable, Sendable {
    // side 的 rawValue 供模型和展示共享；tone 仅提供买卖的视觉区分。
    case buy
    case sell

    public var displayName: String { self == .buy ? "BUY" : "SELL" }
    public var tone: AkzioTone { self == .buy ? .gold : .coral }
}
