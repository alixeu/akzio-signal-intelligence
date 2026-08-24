import Foundation

// MARK: - Learning

public struct RetrospectiveCardPresentation: Sendable, Hashable, Identifiable {
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
    public var isDegraded: Bool { status == .modelUnavailable }
}

public struct TimelineNodePresentation: Sendable, Hashable, Identifiable {
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
        memoryState?.tone ?? candidateState?.tone ?? .neutral
    }

    public var stateLabel: String {
        memoryState?.displayName ?? candidateState?.displayName ?? MissingValue.unavailable.rawValue
    }
}
