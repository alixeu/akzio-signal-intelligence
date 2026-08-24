import Foundation

// MARK: - Decision & execution vocabulary

/// `HardBlocker` — crates/akzio-domain/src/decision.rs:13 (all 21 variants)
public enum HardBlocker: String, CaseIterable, Sendable, Identifiable {
    case unsupportedUniverse = "unsupported_universe"
    case noExecutableOrder = "no_executable_order"
    case frozen
    case missingEvidence = "missing_evidence"
    case invalidProvenance = "invalid_provenance"
    case materialConflict = "material_conflict"
    case staleQuote = "stale_quote"
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
        rawValue
            .split(separator: "_")
            .map { $0.capitalized }
            .joined(separator: " ")
    }

    /// Which gate surfaces the blocker, so the DAG can attach it to the right node.
    public var gate: GateKind {
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
    case lowConfidence = "low_confidence"
    case incompleteEvidence = "incomplete_evidence"
    case elevatedTurnover = "elevated_turnover"
    case slowModelResponse = "slow_model_response"
    case staleNoncriticalEvidence = "stale_noncritical_evidence"

    public var id: String { rawValue }

    public var displayName: String {
        rawValue.split(separator: "_").map { $0.capitalized }.joined(separator: " ")
    }
}

/// `OrderReceiptState` — execution.rs:509. The reference image shows
/// "Pending / Working"; those are AI-generated labels and are not used.
public enum OrderReceiptState: String, CaseIterable, Sendable {
    case accepted
    case partiallyFilled = "partially_filled"
    case filled
    case canceled
    case rejected
    case failed

    public var status: AkzioStatus {
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
    case buy
    case sell

    public var displayName: String { self == .buy ? "BUY" : "SELL" }
    public var tone: AkzioTone { self == .buy ? .gold : .coral }
}
