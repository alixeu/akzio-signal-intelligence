import Foundation

// MARK: - Agents & workflow stages

/// Research roles as they appear in `akzio-research` contracts.
public enum AgentRole: String, CaseIterable, Sendable, Identifiable {
    // rawValue 是跨 Rust 合同和 Swift 展示层的稳定角色标识，displayName 等属性只提供 UI 语义。
    case planner
    case analyst
    case critic
    case synthesizer
    case outcomeWorker = "outcome_worker"

    public var id: String { rawValue }

    public var displayName: String {
        // 名称、职责和图标都是由枚举值派生的只读展示映射，不回写运行时角色。
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
    // isOptional 只供 workflow 展示判断，不改变 Rust 侧任务是否创建。
    public var isOptional: Bool { self == .critic }
}

/// Reasoning effort reported by the Rust model adapter.
public enum ReasoningIntensity: String, CaseIterable, Sendable, Identifiable {
    // 该 enum 对应 Rust adapter 报告的推理强度；数值视觉参数由 computed property 派生。
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
        // 轨道数量是显示密度映射，不是模型预算或实际调用次数。
        switch self {
        case .none: 0
        case .minimal, .low: 1
        case .medium: 2
        case .high: 3
        case .xhigh, .max: 4
        }
    }

    public var coreBrightness: Double {
        // 核心亮度只决定画布颜色强度，保留与强度枚举一一对应的稳定值。
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
    // 阶段 enum 是 workflow 图的 Swift 投影；关联值保留 Analyst 序号和 horizon 语义。
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
        // id 供节点、边和 accessibility overlay 对齐，不能替代 Rust taskID。
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
        // displayName 只面向页面文本；关联值由同一 stage 映射为可读标签。
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
        // SF Symbol 是视觉映射，和阶段 wire 名称保持解耦。
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
    // 这两个判断供 presentation status 映射使用，不在展示层新增阶段。
    public var isOptional: Bool { self == .critic }
    public var requiresPaperRun: Bool { self == .paperCommit }

    public var role: AgentRole? {
        // 只有能对应 Agent 的阶段返回 role；门控和结构节点保持 nil。
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
