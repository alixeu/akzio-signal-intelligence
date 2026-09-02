import Foundation

// MARK: - Learning aggregate

public struct ImpactAreaPresentation: Sendable, Hashable, Identifiable {
    public let label: String
    public let impactPpm: Int

    public var id: String { label }

    public init(label: String, impactPpm: Int) {
        self.label = label
        self.impactPpm = impactPpm
    }
}

public struct ImpactSummaryPresentation: Sendable, Hashable {
    public let totalImpactMicros: Int64?
    public let totalImpactPpm: Int
    public let lessonsCreated: Int
    public let lessonsDelta: Int
    public let policiesEvolved: Int
    public let policiesDelta: Int
    public let areas: [ImpactAreaPresentation]

    public init(
        totalImpactMicros: Int64?,
        totalImpactPpm: Int,
        lessonsCreated: Int,
        lessonsDelta: Int,
        policiesEvolved: Int,
        policiesDelta: Int,
        areas: [ImpactAreaPresentation]
    ) {
        self.totalImpactMicros = totalImpactMicros
        self.totalImpactPpm = totalImpactPpm
        self.lessonsCreated = lessonsCreated
        self.lessonsDelta = lessonsDelta
        self.policiesEvolved = policiesEvolved
        self.policiesDelta = policiesDelta
        self.areas = areas
    }
}

public struct LearningPresentation: Sendable, Hashable {
    public enum Tab: String, CaseIterable, Sendable, Identifiable {
        case retrospective
        case timeline
        case policy
        case lessons
        case impact

        public var id: String { rawValue }
        public var displayName: String {
            switch self {
            case .retrospective: "Retrospective"
            case .timeline: "Experience Timeline"
            case .policy: "Policy Transitions"
            case .lessons: "Lessons"
            case .impact: "Impact"
            }
        }
    }

    public let cards: [RetrospectiveCardPresentation]
    public let timeline: [TimelineNodePresentation]
    public let policyTracks: [PolicyTrackPresentation]
    public let impact: ImpactSummaryPresentation
    public let activePolicyName: String
    public let timeRangeLabel: String
    public let availabilityStatus: AkzioStatus
    public let availabilityReason: String?

    public init(
        cards: [RetrospectiveCardPresentation],
        timeline: [TimelineNodePresentation],
        policyTracks: [PolicyTrackPresentation],
        impact: ImpactSummaryPresentation,
        activePolicyName: String,
        timeRangeLabel: String,
        availabilityStatus: AkzioStatus = .completed,
        availabilityReason: String? = nil
    ) {
        self.cards = cards
        self.timeline = timeline
        self.policyTracks = policyTracks
        self.impact = impact
        self.activePolicyName = activePolicyName
        self.timeRangeLabel = timeRangeLabel
        self.availabilityStatus = availabilityStatus
        self.availabilityReason = availabilityReason
    }

    public var lessonCandidates: [RetrospectiveCardPresentation] {
        cards.filter { !$0.lessonCandidate.isEmpty && !$0.isDegraded }
    }
}

// MARK: - Run archive

public struct ArchiveRowPresentation: Sendable, Hashable, Identifiable {
    public let id: String
    public let runID: String
    public let purposeLabel: String
    public let purpose: RunPurpose
    public let topology: String
    public let status: WorkflowStatus
    public let durationSeconds: Int?
    public let currentStage: String
    public let model: String
    public let resultPpm: Int?
    public let startedAt: Date
    public let startedAtLabel: String
    public let stageProgress: [ArchiveStageProgress]

    public init(
        id: String,
        runID: String,
        purposeLabel: String,
        purpose: RunPurpose,
        topology: String,
        status: WorkflowStatus,
        durationSeconds: Int?,
        currentStage: String,
        model: String,
        resultPpm: Int?,
        startedAt: Date,
        startedAtLabel: String,
        stageProgress: [ArchiveStageProgress]
    ) {
        self.id = id
        self.runID = runID
        self.purposeLabel = purposeLabel
        self.purpose = purpose
        self.topology = topology
        self.status = status
        self.durationSeconds = durationSeconds
        self.currentStage = currentStage
        self.model = model
        self.resultPpm = resultPpm
        self.startedAt = startedAt
        self.startedAtLabel = startedAtLabel
        self.stageProgress = stageProgress
    }
}

public struct ArchiveStageProgress: Sendable, Hashable, Identifiable {
    public let label: String
    public let status: AkzioStatus
    public let timeLabel: String

    public var id: String { label }

    public init(label: String, status: AkzioStatus, timeLabel: String) {
        self.label = label
        self.status = status
        self.timeLabel = timeLabel
    }
}

public struct ArchivePresentation: Sendable, Hashable {
    public let rows: [ArchiveRowPresentation]
    public let totalRuns: Int
    public let successRatePpm: Int
    public let page: Int
    public let pageSize: Int
    public let selectedRowID: String?
    public let activeFilters: [String]

    public init(
        rows: [ArchiveRowPresentation],
        totalRuns: Int,
        successRatePpm: Int,
        page: Int,
        pageSize: Int,
        selectedRowID: String?,
        activeFilters: [String]
    ) {
        self.rows = rows
        self.totalRuns = totalRuns
        self.successRatePpm = successRatePpm
        self.page = page
        self.pageSize = pageSize
        self.selectedRowID = selectedRowID
        self.activeFilters = activeFilters
    }

    public var pageLabel: String {
        let start = (page - 1) * pageSize + 1
        let end = min(page * pageSize, totalRuns)
        return "\(start)–\(end) of \(PpmFormatter.count(totalRuns)) runs"
    }
}
