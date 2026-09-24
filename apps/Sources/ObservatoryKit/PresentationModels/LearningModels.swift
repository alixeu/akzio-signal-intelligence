import Foundation

// MARK: - Learning

public struct RetrospectiveCardPresentation: Sendable, Hashable, Identifiable {
    // retrospective card 是封存 Outcome 到 Learning 页的值语义投影，结果和 lesson 可分别缺失。
    public let id: String
    public let title: String
    public let dateLabel: String
    public let conclusion: RetrospectiveConclusion
    public let status: RetrospectiveStatus
    public let categories: [RetrospectiveCategory]
    public let pnlMicros: Int64?
    public let impactPpm: Int?
    public let spark: [Double]
    public let counterfactual: String
    public let lessonCandidate: String
    public let diagnosticGaps: [String]
    public let tags: [String]
    public let impact: EventPresentation.Severity

    public init(
        id: String,
        title: String,
        dateLabel: String,
        conclusion: RetrospectiveConclusion,
        status: RetrospectiveStatus,
        categories: [RetrospectiveCategory],
        pnlMicros: Int64?,
        impactPpm: Int?,
        spark: [Double],
        counterfactual: String,
        lessonCandidate: String,
        diagnosticGaps: [String],
        tags: [String],
        impact: EventPresentation.Severity
    ) {
        // 初始化保留 diagnosticGaps 和 Optional 数值，页面不从文本猜测模型是否成功。
        self.id = id
        self.title = title
        self.dateLabel = dateLabel
        self.conclusion = conclusion
        self.status = status
        self.categories = categories
        self.pnlMicros = pnlMicros
        self.impactPpm = impactPpm
        self.spark = spark
        self.counterfactual = counterfactual
        self.lessonCandidate = lessonCandidate
        self.diagnosticGaps = diagnosticGaps
        self.tags = tags
        self.impact = impact
    }

    /// A retrospective the model could not produce keeps its outcome numbers but
    /// must not present invented conclusions.
    // isDegraded 是 status 的只读映射，供 lessonCandidates 等过滤边界复用。
    public var isDegraded: Bool { status == .modelUnavailable }
}

public struct TimelineNodePresentation: Sendable, Hashable, Identifiable {
    // timeline node 是事件→Decision→Outcome→Lesson 的固定页面坐标投影。
    public enum Kind: String, Sendable, CaseIterable {
        case event, decision, outcome, lesson

        public var displayName: String { rawValue.capitalized }
        public var tone: AkzioTone {
            switch self {
            case .event: .neutral
            case .decision: .gold
            case .outcome: .coral
            case .lesson: .gold
            }
        }
    }

    public let id: String
    public let kind: Kind
    public let label: String
    public let dateLabel: String
    public let detail: String
    public let position: Double
    public let isCurrent: Bool

    public init(
        id: String,
        kind: Kind,
        label: String,
        dateLabel: String,
        detail: String,
        position: Double,
        isCurrent: Bool
    ) {
        // position/isCurrent 只描述时间线绘制状态，不改变事件或学习生命周期。
        self.id = id
        self.kind = kind
        self.label = label
        self.dateLabel = dateLabel
        self.detail = detail
        self.position = position
        self.isCurrent = isCurrent
    }
}

/// Policy tracks come in two shapes: memory lifecycle (5 states) and the canary
/// ladder used by contracts and topologies.
public struct PolicyTrackPresentation: Sendable, Hashable, Identifiable {
    // policy track 同时容纳 memoryState/candidateState，但两者都可为 nil 以表达未提供。
    public let subject: PolicySubjectKind
    public let name: String
    public let memoryState: MemoryLifecycle?
    public let candidateState: CandidatePolicyState?
    public let activeSinceLabel: String
    public let sampleCount: Int
    public let winRatePpm: Int?
    public let netImpactPpm: Int?
    public let stabilityPpm: Int?
    public let exposurePpm: Int?

    public var id: String { "\(subject.rawValue).\(name)" }

    public init(
        subject: PolicySubjectKind,
        name: String,
        memoryState: MemoryLifecycle?,
        candidateState: CandidatePolicyState?,
        activeSinceLabel: String,
        sampleCount: Int = 0,
        winRatePpm: Int?,
        netImpactPpm: Int?,
        stabilityPpm: Int?,
        exposurePpm: Int?
    ) {
        // 初始化不把 candidate ladder 误转成 active memory，两个 Optional 保持独立。
        self.subject = subject
        self.name = name
        self.memoryState = memoryState
        self.candidateState = candidateState
        self.activeSinceLabel = activeSinceLabel
        self.sampleCount = sampleCount
        self.winRatePpm = winRatePpm
        self.netImpactPpm = netImpactPpm
        self.stabilityPpm = stabilityPpm
        self.exposurePpm = exposurePpm
    }

    public var tone: AkzioTone {
        // memory 优先于 candidate；若两者都缺失使用 neutral，不伪造生命周期。
        memoryState?.tone ?? candidateState?.tone ?? .neutral
    }

    public var stateLabel: String {
        // 标签只在读取时由可用状态派生，完全缺失时返回统一 unavailable 占位。
        memoryState?.displayName ?? candidateState?.displayName ?? MissingValue.unavailable.rawValue
    }
}
