import Foundation

// MARK: - Snapshot
//
// One immutable value that every page reads. Built by `ScenarioLibrary` from a
// deterministic mock scenario; nothing in the UI mutates it.
//
// `anchor` is a frozen instant rather than `Date()`, so screenshots, count-ups and
// elapsed clocks reproduce exactly between runs.
public struct ObservatorySnapshot: Sendable, Equatable {
    // Snapshot 是所有页面共享的不可变根投影；各子模型按领域拆分但共同来自同一 anchor。
    /// 2026-08-19 09:30 America/New_York (13:30 UTC) — a market open, frozen.
    public static let anchor = Date(timeIntervalSince1970: 1_787_146_200)

    public let scenarioID: String
    public let scenarioTitle: String
    public let anchor: Date

    public let run: RunPresentation
    public let workflow: WorkflowPresentation
    public let council: CouncilPresentation
    public let portfolio: PortfolioPresentation
    public let outcome: OutcomePresentation
    public let learning: LearningPresentation
    public let archive: ArchivePresentation

    public let events: [EventPresentation]
    public let agents: [AgentRailItem]
    public let health: [HealthMetric]

    public init(
        scenarioID: String,
        scenarioTitle: String,
        anchor: Date = ObservatorySnapshot.anchor,
        run: RunPresentation,
        workflow: WorkflowPresentation,
        council: CouncilPresentation,
        portfolio: PortfolioPresentation,
        outcome: OutcomePresentation,
        learning: LearningPresentation,
        archive: ArchivePresentation,
        events: [EventPresentation],
        agents: [AgentRailItem],
        health: [HealthMetric]
    ) {
        // 初始化只组装已生成的展示模型，不在快照层重新查询 Observer 或修改任何子值。
        self.scenarioID = scenarioID
        self.scenarioTitle = scenarioTitle
        self.anchor = anchor
        self.run = run
        self.workflow = workflow
        self.council = council
        self.portfolio = portfolio
        self.outcome = outcome
        self.learning = learning
        self.archive = archive
        self.events = events
        self.agents = agents
        self.health = health
    }

    /// The frozen anchor rendered in the broker's time zone, for Settings to show.
    public static var anchorLabel: String {
        // anchorLabel 只格式化冻结静态 anchor，避免设置页读取墙上时钟造成漂移。
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyy-MM-dd HH:mm 'ET'"
        return formatter.string(from: anchor)
    }

    /// Elapsed seconds are stored, never derived from the wall clock.
    public var elapsedLabel: String {
        // elapsedLabel 由 RunPresentation 的存储秒数派生，页面不会自行计算当前 elapsed。
        PpmFormatter.elapsed(seconds: run.elapsedSeconds)
    }
}
