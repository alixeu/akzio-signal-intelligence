import Foundation

// MARK: - Event, agent and health fixtures

enum EventFixtures {
    static func events(scenario: MockScenario) -> [EventPresentation] {
        var items: [EventPresentation] = []

        if scenario.criticTriggered {
            items.append(
                EventPresentation(
                    id: "critic",
                    title: "Critic Triggered",
                    detail: "Material conflict detected in sector exposure.",
                    severity: .critical,
                    symbol: "exclamationmark.shield",
                    timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-92),
                    relativeLabel: "Just now"
                )
            )
        }

        if scenario.isDecisionBlocked {
            items.append(
                EventPresentation(
                    id: "decision-blocked",
                    title: "Decision Gate Blocked",
                    detail: "Turnover limit exceeded; execution withheld.",
                    severity: .critical,
                    symbol: "hand.raised",
                    timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-180),
                    relativeLabel: "3m ago"
                )
            )
        } else if scenario.hasOrders {
            items.append(
                EventPresentation(
                    id: "execution-gate",
                    title: "Execution Gate Passed",
                    detail: "Risk within limits. Proceeding to execution.",
                    severity: .notable,
                    symbol: "bolt.horizontal.circle",
                    timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-140),
                    relativeLabel: "2m ago"
                )
            )
        } else {
            items.append(
                EventPresentation(
                    id: "no-order",
                    title: "No Executable Order",
                    detail: "Targets already within tolerance; nothing submitted.",
                    severity: .info,
                    symbol: "slash.circle",
                    timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-160),
                    relativeLabel: "2m ago"
                )
            )
        }

        if scenario.dataStale {
            items.append(
                EventPresentation(
                    id: "stale",
                    title: "Quote Snapshot Stale",
                    detail: "Market data older than the freshness budget.",
                    severity: .critical,
                    symbol: "exclamationmark.arrow.circlepath",
                    timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-240),
                    relativeLabel: "4m ago"
                )
            )
        }

        items.append(
            EventPresentation(
                id: "evidence",
                title: "Evidence Gate Passed",
                detail: "12 normalized documents with valid provenance.",
                severity: .info,
                symbol: "shield.lefthalf.filled",
                timestamp: ObservatorySnapshot.anchor.addingTimeInterval(-420),
                relativeLabel: "7m ago"
            )
        )

        return items
    }

    static func agents(scenario: MockScenario) -> [AgentRailItem] {
        var generator = SeededGenerator(seed: scenario.seed &+ 501)
        let synthesizerActive = scenario == .paperRunningSynthesizerActive

        var rows: [AgentRailItem] = [
            AgentRailItem(
                id: "planner",
                name: "Planner",
                role: .planner,
                model: ModelCatalog.primary,
                status: .succeeded,
                activityLabel: "Plan compiled",
                progressPpm: 1_000_000
            ),
            AgentRailItem(
                id: "analyst-1",
                name: "Analyst-1",
                role: .analyst,
                model: ModelCatalog.primary,
                status: .succeeded,
                activityLabel: "Market structure",
                progressPpm: generator.int(in: 900_000...990_000)
            ),
            AgentRailItem(
                id: "analyst-2",
                name: "Analyst-2",
                role: .analyst,
                model: ModelCatalog.alternates[0],
                status: scenario.workflowStatus == .running ? .running : .succeeded,
                activityLabel: "Capital flow",
                progressPpm: generator.int(in: 700_000...920_000)
            ),
            AgentRailItem(
                id: "analyst-3",
                name: "Analyst-3",
                role: .analyst,
                model: ModelCatalog.alternates[1],
                status: scenario.workflowStatus == .running ? .running : .succeeded,
                activityLabel: "Sentiment & positioning",
                progressPpm: generator.int(in: 640_000...880_000)
            ),
            AgentRailItem(
                id: "synthesizer",
                name: "Synthesizer",
                role: .synthesizer,
                model: ModelCatalog.primary,
                status: synthesizerActive ? .running : (scenario.workflowStatus == .running ? .queued : .succeeded),
                activityLabel: synthesizerActive ? "Converging claims" : "Synthesis",
                progressPpm: synthesizerActive ? generator.int(in: 620_000...760_000) : 1_000_000
            ),
        ]

        rows.append(
            AgentRailItem(
                id: "critic",
                name: "Critic",
                role: .critic,
                model: ModelCatalog.alternates[2],
                status: scenario.criticTriggered ? .running : .notTriggered,
                activityLabel: scenario.criticTriggered ? "Reviewing conflict" : "No material conflict detected",
                progressPpm: scenario.criticTriggered ? generator.int(in: 400_000...700_000) : 0
            )
        )

        return rows
    }

    static func health(scenario: MockScenario) -> [HealthMetric] {
        let unavailable = scenario.dataUnavailable
        var generator = SeededGenerator(seed: scenario.seed &+ 977)

        func metric(_ id: String, _ label: String, _ value: String, _ fraction: Double?, risky: Bool = false) -> HealthMetric {
            HealthMetric(
                id: id,
                label: label,
                value: unavailable ? MissingValue.unavailable.rawValue : value,
                fraction: unavailable ? nil : fraction,
                isElevatedRisk: !unavailable && risky
            )
        }

        let varPpm = generator.int(in: 11_000...14_000)
        let drawdownPpm = generator.int(in: 28_000...36_000)
        let leveragePpm = generator.int(in: 200_000...260_000)
        let quality = generator.int(in: 985_000...999_000)
        let load = generator.int(in: 300_000...460_000)

        return [
            metric("var", "Portfolio VaR (95%)", PpmFormatter.share(ppm: varPpm, fractionDigits: 2), Double(varPpm) / 40_000, risky: scenario.dataStale),
            metric("drawdown", "Max Drawdown", PpmFormatter.share(ppm: drawdownPpm, fractionDigits: 2), Double(drawdownPpm) / 80_000),
            metric("leverage", "Leverage Used", PpmFormatter.share(ppm: leveragePpm, fractionDigits: 0), Double(leveragePpm) / 1_000_000),
            metric("quality", "Data Quality", PpmFormatter.share(ppm: quality, fractionDigits: 1), Double(quality) / PpmFormatter.ppmPerUnit, risky: scenario.dataStale),
            metric("load", "System Load", PpmFormatter.share(ppm: load, fractionDigits: 0), Double(load) / PpmFormatter.ppmPerUnit),
        ]
    }
}

// MARK: - Model catalog

/// The real configured model plus mock alternates for the gallery.
enum ModelCatalog {
    static let primary = "gpt-5.6-luna"
    static let alternates = ["claude-4.2-sonnet", "gemini-3.1-pro", "gpt-5.6-luna-mini", "qwen-3.5-max"]

    static let gallery: [ModelOption] = {
        var options = [ModelOption(name: primary, tier: "Primary Path", isSelected: true)]
        options += alternates.enumerated().map { index, name in
            ModelOption(name: name, tier: index == 0 ? "Alternate" : "Candidate", isSelected: false)
        }
        return options
    }()
}
