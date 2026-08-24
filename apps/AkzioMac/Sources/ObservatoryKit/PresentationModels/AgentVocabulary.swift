import Foundation

// MARK: - Agents & workflow stages

/// Research roles as they appear in `akzio-research` contracts.
public enum AgentRole: String, CaseIterable, Sendable, Identifiable {
    case planner
    case analyst
    case critic
    case synthesizer
    case outcomeWorker = "outcome_worker"

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .planner: "Planner"
        case .analyst: "Analyst"
        case .critic: "Critic"
        case .synthesizer: "Synthesizer"
        case .outcomeWorker: "Outcome Worker"
        }
    }

    public var responsibility: String {
        switch self {
        case .planner: "Strategy & Planning"
        case .analyst: "Data & Analysis"
        case .critic: "Validation & Risk"
        case .synthesizer: "Synthesis & Integration"
        case .outcomeWorker: "Outcome & Evaluation"
        }
    }

    public var symbol: String {
        switch self {
        case .planner: "map"
        case .analyst: "chart.xyaxis.line"
        case .critic: "exclamationmark.shield"
        case .synthesizer: "square.stack.3d.up"
        case .outcomeWorker: "clock.badge.checkmark"
        }
    }

    /// The Critic is the only optional role in the workflow.
    public var isOptional: Bool { self == .critic }
}

/// Reasoning effort reported by the Rust model adapter.
public enum ReasoningIntensity: String, CaseIterable, Sendable, Identifiable {
    case none
    case minimal
    case low
    case medium
    case high
    case xhigh
    case max

    public var id: String { rawValue }
    public var displayName: String { self == .xhigh ? "XHigh" : rawValue.capitalized }

    /// Orbit rings drawn by `IntensityOrbitCanvas`.
    public var orbitCount: Int {
        switch self {
        case .none: 0
        case .minimal, .low: 1
        case .medium: 2
        case .high: 3
        case .xhigh, .max: 4
        }
    }

    public var coreBrightness: Double {
        switch self {
        case .none: 0.18
        case .minimal: 0.26
        case .low: 0.35
        case .medium: 0.55
        case .high: 0.78
        case .xhigh: 0.9
        case .max: 1.0
        }
    }

    /// Max leans on stronger gold plus a hint of coral — never purple.
    public var usesCoralAccent: Bool { self == .xhigh || self == .max }
}

/// Canonical pipeline stages, in execution order.
public enum WorkflowStageKind: Hashable, Sendable, Identifiable {
    case planner
    case evidenceGate
    case analyst(Int)
    case critic
    case synthesizer
    case decisionGate
    case executionGate
    case paperCommit
    case reconcile
    case evaluate
    case learning
    case horizon(OutcomeHorizonKind)

    public var id: String {
        switch self {
        case .planner: "planner"
        case .evidenceGate: "evidence_gate"
        case .analyst(let index): "analyst.\(index)"
        case .critic: "critic"
        case .synthesizer: "synthesizer"
        case .decisionGate: "decision_gate"
        case .executionGate: "execution_gate"
        case .paperCommit: "paper_commit"
        case .reconcile: "reconcile"
        case .evaluate: "evaluate"
        case .learning: "learning"
        case .horizon(let horizon): "horizon.\(horizon.rawValue)"
        }
    }

    public var displayName: String {
        switch self {
        case .planner: "Planner"
        case .evidenceGate: "Evidence Gate"
        case .analyst(let index): "Analyst \(index)"
        case .critic: "Critic"
        case .synthesizer: "Synthesizer"
        case .decisionGate: "Decision Gate"
        case .executionGate: "Execution Gate"
        case .paperCommit: "Paper Commit"
        case .reconcile: "Reconcile"
        case .evaluate: "Evaluate"
        case .learning: "Learning & Experience"
        case .horizon(let horizon): horizon.displayName
        }
    }

    public var symbol: String {
        switch self {
        case .planner: "map"
        case .evidenceGate: "shield.lefthalf.filled"
        case .analyst: "person.3"
        case .critic: "exclamationmark.shield"
        case .synthesizer: "square.stack.3d.up"
        case .decisionGate: "scale.3d"
        case .executionGate: "bolt.horizontal.circle"
        case .paperCommit: "doc.badge.plus"
        case .reconcile: "arrow.triangle.2.circlepath"
        case .evaluate: "chart.bar.doc.horizontal"
        case .learning: "sparkles.rectangle.stack"
        case .horizon: "target"
        }
    }

    /// Only the Critic is optional; only Paper Commit can be not-applicable.
    public var isOptional: Bool { self == .critic }
    public var requiresPaperRun: Bool { self == .paperCommit }

    public var role: AgentRole? {
        switch self {
        case .planner: .planner
        case .analyst: .analyst
        case .critic: .critic
        case .synthesizer: .synthesizer
        case .evaluate, .learning, .horizon: .outcomeWorker
        default: nil
        }
    }
}
