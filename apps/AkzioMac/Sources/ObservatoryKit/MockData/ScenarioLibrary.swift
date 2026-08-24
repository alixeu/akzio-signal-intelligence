import Foundation

// MARK: - Scenario library
//
// The single entry point the app uses to obtain data. Every snapshot is built from
// the scenario seed and the frozen anchor, so two builds are byte-identical.
public enum ScenarioLibrary {
    public static func snapshot(_ scenario: MockScenario) -> ObservatorySnapshot {
        cache[scenario] ?? build(scenario)
    }

    public static var `default`: ObservatorySnapshot { snapshot(.paperRunningSynthesizerActive) }

    /// Prebuilt once; immutable and safe to share.
    private static let cache: [MockScenario: ObservatorySnapshot] = Dictionary(
        uniqueKeysWithValues: MockScenario.allCases.map { ($0, build($0)) }
    )

    public static func build(_ scenario: MockScenario) -> ObservatorySnapshot {
        let nodes = WorkflowFixtures.nodes(scenario: scenario)
        let run = run(scenario: scenario)
        let workflow = WorkflowPresentation(
            nodes: nodes,
            edges: WorkflowFixtures.edges(scenario: scenario),
            activeStageID: nodes.first(where: \.isActive)?.id,
            inspector: CouncilFixtures.inspector(scenario: scenario, nodes: nodes),
            observedTradingDays: scenario.sealedHorizons.map(\.tradingDays).max() ?? 0,
            totalTradingDays: OutcomeHorizonKind.t5.tradingDays
        )

        return ObservatorySnapshot(
            scenarioID: scenario.code,
            scenarioTitle: scenario.title,
            run: run,
            workflow: workflow,
            council: CouncilFixtures.council(scenario: scenario),
            portfolio: PortfolioFixtures.portfolio(scenario: scenario),
            outcome: OutcomeFixtures.outcome(scenario: scenario),
            learning: LearningFixtures.learning(scenario: scenario),
            archive: ArchiveFixtures.archive(scenario: scenario, currentRun: run),
            events: EventFixtures.events(scenario: scenario),
            agents: EventFixtures.agents(scenario: scenario),
            health: EventFixtures.health(scenario: scenario)
        )
    }

    // MARK: Run header

    static func run(scenario: MockScenario) -> RunPresentation {
        var generator = SeededGenerator(seed: scenario.seed &+ 1_009)
        let elapsed = elapsedSeconds(scenario: scenario)
        return RunPresentation(
            runId: runID(&generator),
            purpose: scenario.purpose,
            status: scenario.workflowStatus,
            topology: scenario.purpose == .debug ? "fixture-debug" : "three-analyst-fanout",
            model: ModelCatalog.primary,
            market: "US Equities",
            startedAt: ObservatorySnapshot.anchor.addingTimeInterval(-Double(elapsed)),
            elapsedSeconds: elapsed,
            // System load is the process's own metric, so it stays known even when
            // market metrics are unavailable.
            systemHealthPpm: generator.int(in: scenario.dataStale ? 720_000...840_000 : 940_000...998_000),
            marketOpen: true,
            dataLive: !scenario.dataStale && !scenario.dataUnavailable,
            dataStale: scenario.dataStale,
            latencyMillis: generator.int(in: scenario.dataStale ? 1_400...3_800 : 90...420),
            brokerSession: brokerSession
        )
    }

    private static func elapsedSeconds(scenario: MockScenario) -> Int {
        switch scenario.workflowStatus {
        case .running: 1_247 + scenario.rawValue * 13
        case .queued, .leased: 0
        default: 3_180 + scenario.rawValue * 41
        }
    }

    private static func runID(_ generator: inout SeededGenerator) -> String {
        let hex = "0123456789abcdef".map { String($0) }
        func block(_ length: Int) -> String {
            (0..<length).map { _ in generator.pick(hex) }.joined()
        }
        return [block(8), block(4), block(4), block(4), block(12)].joined(separator: "-")
    }

    /// Alpaca Paper broker session date for the frozen anchor.
    static let brokerSession: String = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: ObservatorySnapshot.anchor)
    }()

    // MARK: Settings

    /// Settings are display-only, but scenario 17 ships with reduce-motion on.
    public static func settings(_ scenario: MockScenario) -> SettingsPresentation {
        SettingsPresentation(reduceMotionOverride: scenario.reduceMotionPreferred)
    }
}
