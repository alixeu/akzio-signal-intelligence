import Foundation

// MARK: - Outcome & learning vocabulary

/// `OutcomeHorizon` — crates/akzio-domain/src/evaluation.rs:14
public enum OutcomeHorizonKind: String, CaseIterable, Sendable, Identifiable {
    case t1
    case t3
    case t5

    public var id: String { rawValue }
    public var tradingDays: Int {
        switch self {
        case .t1: 1
        case .t3: 3
        case .t5: 5
        }
    }

    public var displayName: String { "T+\(tradingDays)" }
    /// Trading sessions, never calendar days.
    public var windowLabel: String { tradingDays == 1 ? "1 Trading Session" : "\(tradingDays) Trading Sessions" }
}

/// `MemoryLifecycle` — evaluation.rs:460
public enum MemoryLifecycle: String, CaseIterable, Sendable, Identifiable {
    case candidate
    case active
    case proven
    case contested
    case retired

    public var id: String { rawValue }
    public var displayName: String { rawValue.capitalized }

    public var tone: AkzioTone {
        switch self {
        case .candidate: .neutral
        case .active, .proven: .gold
        case .contested: .coral
        case .retired: .muted
        }
    }

    public var symbol: String {
        switch self {
        case .candidate: "circle.dashed"
        case .active: "flame"
        case .proven: "checkmark.seal"
        case .contested: "questionmark.diamond"
        case .retired: "archivebox"
        }
    }
}

/// `CandidatePolicyState` — evaluation.rs:470. The canary ladder exists in code.
public enum CandidatePolicyState: String, CaseIterable, Sendable, Identifiable {
    case candidate
    case canary10 = "canary10"
    case canary25 = "canary25"
    case canary50 = "canary50"
    case active

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .candidate: "Candidate"
        case .canary10: "Canary 10%"
        case .canary25: "Canary 25%"
        case .canary50: "Canary 50%"
        case .active: "Active"
        }
    }

    public var tone: AkzioTone { self == .active ? .gold : .neutral }
}

/// `PolicySubject` — evaluation.rs:481
public enum PolicySubjectKind: String, CaseIterable, Sendable {
    case memory
    case contract
    case topology

    public var displayName: String { rawValue.capitalized }
    public var symbol: String {
        switch self {
        case .memory: "brain.head.profile"
        case .contract: "doc.text"
        case .topology: "point.3.connected.trianglepath.dotted"
        }
    }
}

/// `RetrospectiveCategory` — evaluation.rs:174
public enum RetrospectiveCategory: String, CaseIterable, Sendable, Identifiable {
    case research
    case evidence
    case risk
    case decision
    case execution
    case topology
    case contract

    public var id: String { rawValue }
    public var displayName: String { rawValue.capitalized }
}

/// `RetrospectiveConclusion` — evaluation.rs:186
public enum RetrospectiveConclusion: String, CaseIterable, Sendable {
    case worked
    case failed
    case mixed
    case unresolved

    public var displayName: String {
        switch self {
        case .worked: "What Worked"
        case .failed: "What Failed"
        case .mixed: "Mixed"
        case .unresolved: "Unresolved"
        }
    }

    public var tone: AkzioTone {
        switch self {
        case .worked: .gold
        case .failed: .coral
        case .mixed: .neutral
        case .unresolved: .muted
        }
    }

    public var symbol: String {
        switch self {
        case .worked: "checkmark.circle"
        case .failed: "xmark.circle"
        case .mixed: "circle.lefthalf.filled"
        case .unresolved: "questionmark.circle"
        }
    }
}

/// `RetrospectiveStatus` — evaluation.rs:195
public enum RetrospectiveStatus: String, CaseIterable, Sendable {
    case complete
    case modelUnavailable = "model_unavailable"

    public var displayName: String {
        self == .complete ? "Complete" : "Model Unavailable"
    }
}
