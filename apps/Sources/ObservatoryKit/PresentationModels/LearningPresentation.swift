import Foundation

// MARK: - Learning aggregate

public struct ImpactAreaPresentation: Sendable, Hashable, Identifiable {
    // impact area 是汇总图表的不可变条目，label 同时作为稳定 ID。
    public let label: String
    public let impactPpm: Int

    public var id: String { label }

    public init(label: String, impactPpm: Int) {
        self.label = label
        self.impactPpm = impactPpm
    }
}

public struct ImpactSummaryPresentation: Sendable, Hashable {
    // impact summary 汇总 lesson/policy 变化与可用影响值，不负责计算来源证据。
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
        // 初始化保留 totalImpactMicros Optional，缺失金额不能由 ppm 结果自动补造。
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
    // LearningPresentation 是页面读取的聚合投影，卡片、时间线、政策和影响各自保持独立数组。
    public enum Tab: String, CaseIterable, Sendable, Identifiable {
        // Tab rawValue 只用于页面选择和持久化 UI 选择，不代表学习数据状态。
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
    public let researchAudit: [ResearchAuditRowPresentation]

    public init(
        cards: [RetrospectiveCardPresentation],
        timeline: [TimelineNodePresentation],
        policyTracks: [PolicyTrackPresentation],
        impact: ImpactSummaryPresentation,
        activePolicyName: String,
        timeRangeLabel: String,
        availabilityStatus: AkzioStatus = .completed,
        availabilityReason: String? = nil,
        researchAudit: [ResearchAuditRowPresentation] = []
    ) {
        // 默认空数组表示当前投影没有对应条目；availabilityReason 单独表达不可用原因。
        self.cards = cards
        self.timeline = timeline
        self.policyTracks = policyTracks
        self.impact = impact
        self.activePolicyName = activePolicyName
        self.timeRangeLabel = timeRangeLabel
        self.availabilityStatus = availabilityStatus
        self.availabilityReason = availabilityReason
        self.researchAudit = researchAudit
    }

    public var lessonCandidates: [RetrospectiveCardPresentation] {
        // filter 闭包只保留有 lesson 文本且非 degraded 的卡片，避免把模型缺失提升为 lesson。
        cards.filter { !$0.lessonCandidate.isEmpty && !$0.isDegraded }
    }
}

// MARK: - Run archive

public struct ArchiveRowPresentation: Sendable, Hashable, Identifiable {
    // ArchiveRow 是 Observer archive projection 的单行值模型，result/duration 可缺失。
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
        // 初始化保持运行状态、结果和阶段进度独立，页面再决定如何筛选或排序。
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
    // 阶段进度保留 horizon Optional；displayLabel 只在页面读取时组合标签。
    public let id: String
    public let label: String
    public let horizon: String?
    public let status: AkzioStatus
    public let timeLabel: String

    public var displayLabel: String {
        // horizon map 闭包把有窗口的阶段加上大写后缀，没有窗口时保留原 label。
        horizon.map { "\(label) · \($0.uppercased())" } ?? label
    }

    public init(id: String, label: String, horizon: String? = nil, status: AkzioStatus, timeLabel: String) {
        self.id = id
        self.label = label
        self.horizon = horizon
        self.status = status
        self.timeLabel = timeLabel
    }
}

public struct ArchivePresentation: Sendable, Hashable {
    // ArchivePresentation 聚合行、分页和当前筛选摘要；筛选/排序本身属于页面 State。
    public let rows: [ArchiveRowPresentation]
    public let totalRuns: Int
    public let successRatePpm: Int?
    public let page: Int
    public let pageSize: Int
    public let selectedRowID: String?
    public let activeFilters: [String]

    public init(
        rows: [ArchiveRowPresentation],
        totalRuns: Int,
        successRatePpm: Int?,
        page: Int,
        pageSize: Int,
        selectedRowID: String?,
        activeFilters: [String]
    ) {
        // 初始化只复制 Observer/fixture 快照，不在模型内执行查询或变更 rows。
        self.rows = rows
        self.totalRuns = totalRuns
        self.successRatePpm = successRatePpm
        self.page = page
        self.pageSize = pageSize
        self.selectedRowID = selectedRowID
        self.activeFilters = activeFilters
    }

    public var pageLabel: String {
        // pageLabel 根据已提供 page/pageSize/totalRuns 生成文案，边界校验由页面查询函数负责。
        let start = (page - 1) * pageSize + 1
        let end = min(page * pageSize, totalRuns)
        return "\(start)–\(end) of \(PpmFormatter.count(totalRuns)) runs"
    }
}
