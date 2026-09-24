import Foundation

// 文件导读：MockData 的统一装配入口；ObservatoryStore 初始化/切换场景时调用 snapshot，
// 它从固定场景构造共享的 ObservatorySnapshot，再把每个领域 fixture 汇入页面投影。
// 静态 cache 首次读取时为全部 MockScenario 建值快照；快照是 Swift 值类型，页面读取不共享可变生成器状态。
// 先读 build 与 run：build 串起 workflow→Run→页面模型，run 的随机字段来自专属 seed，日期来自冻结 anchor。
// MARK: - Scenario library
//
// The single entry point the app uses to obtain data. Every snapshot is built from
// the scenario seed and the frozen anchor, so two builds are byte-identical.
public enum ScenarioLibrary {
    public static func snapshot(_ scenario: MockScenario) -> ObservatorySnapshot {
        // snapshot 优先读取预构建缓存，保证同一场景在多个页面共享同一份不可变快照。
        cache[scenario] ?? build(scenario)
    }

    public static var `default`: ObservatorySnapshot { snapshot(.paperRunningSynthesizerActive) }

    /// Prebuilt once; immutable and safe to share.
    private static let cache: [MockScenario: ObservatorySnapshot] = Dictionary(
        // map 闭包在初始化阶段为全部场景建立快照；后续读取不会重复生成数据。
        uniqueKeysWithValues: MockScenario.allCases.map { ($0, build($0)) }
    )

    public static func build(_ scenario: MockScenario) -> ObservatorySnapshot {
        // build 是 Mock 数据流的汇聚点：先生成 workflow/run，再组装所有页面展示投影。
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
        // run 使用独立 salt 生成运行头部；dataLive/dataStale 只表达样例快照边界。
        var generator = SeededGenerator(seed: scenario.seed &+ 1_009)
        let elapsed = elapsedSeconds(scenario: scenario)
        return RunPresentation(
            runId: runID(&generator),
            purpose: scenario.purpose,
            status: scenario.workflowStatus,
            topology: "three-horizon-research",
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
        // elapsed 由终态/运行态和 rawValue 固定推导，不使用当前时间。
        switch scenario.workflowStatus {
        case .running: 1_247 + scenario.rawValue * 13
        case .queued, .leased: 0
        default: 3_180 + scenario.rawValue * 41
        }
    }

    private static func runID(_ generator: inout SeededGenerator) -> String {
        // inout 暂借本函数持有的 generator；同步嵌套函数连续推进同一个值后返回，
        // 因而 ID 稳定依赖调用顺序，但它只是 fixture 字符串，不是 Store 创建的 Run。
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
        // 设置 fixture 只覆盖场景 17 的减少动效偏好，其余设置使用默认值。
        SettingsPresentation(reduceMotionOverride: scenario.reduceMotionPreferred)
    }
}
