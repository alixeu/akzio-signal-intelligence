import Foundation

// MARK: - Snapshot
//
// One immutable value that every page reads. Built by `ScenarioLibrary` from a
// deterministic mock scenario; nothing in the UI mutates it.
//
// `anchor` is a frozen instant rather than `Date()`, so screenshots, count-ups and
// elapsed clocks reproduce exactly between runs.
public struct ObservatorySnapshot: Sendable, Equatable {
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
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyy-MM-dd HH:mm 'ET'"
        return formatter.string(from: anchor)
    }

    /// Structural fingerprint proving two builds of the same scenario are identical.
    /// Value-type reflection is deterministic here because every field is a value.
    public var determinismFingerprint: String {
        var parts: [String] = [scenarioID, String(anchor.timeIntervalSince1970)]
        parts.append(String(describing: run))
        parts.append(String(describing: workflow))
        parts.append(String(describing: council))
        parts.append(String(describing: portfolio))
        parts.append(String(describing: outcome))
        parts.append(String(describing: learning))
        parts.append(String(describing: archive))
        parts.append(String(describing: events))
        parts.append(String(describing: agents))
        parts.append(String(describing: health))
        return parts.joined(separator: "|")
    }

    /// Elapsed seconds are stored, never derived from the wall clock.
    public var elapsedLabel: String {
        PpmFormatter.elapsed(seconds: run.elapsedSeconds)
    }
}
