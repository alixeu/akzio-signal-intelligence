import Foundation

// MARK: - Council & inspector fixtures

enum CouncilFixtures {
    static func uncertainties(scenario: MockScenario) -> [UncertaintyPresentation] {
        guard !scenario.dataUnavailable else { return [] }
        var generator = SeededGenerator(seed: scenario.seed &+ 401)
        let labels = [
            "Rate path repricing",
            "Semiconductor demand",
            "Liquidity concentration",
            "Earnings dispersion",
        ]
        return labels.map { label in
            UncertaintyPresentation(label: label, weightPpm: generator.int(in: 120_000...480_000))
        }
    }

    static func alternatives(scenario: MockScenario) -> [AlternativePresentation] {
        var generator = SeededGenerator(seed: scenario.seed &+ 419)
        return [
            AlternativePresentation(
                tag: "A",
                label: "Hold current weights",
                matchPpm: generator.int(in: 300_000...520_000)
            ),
            AlternativePresentation(
                tag: "B",
                label: "Rotate into QQQ",
                matchPpm: generator.int(in: 200_000...420_000)
            ),
            AlternativePresentation(
                tag: "C",
                label: "Trim leveraged exposure",
                matchPpm: scenario.dataUnavailable ? nil : generator.int(in: 120_000...320_000)
            ),
        ]
    }

    static func basisArtifacts(scenario: MockScenario) -> [BasisArtifact] {
        var items = [
            BasisArtifact(label: "12 normalized documents", symbol: "doc.text.magnifyingglass"),
            BasisArtifact(label: "Quote snapshot", symbol: "chart.bar"),
            BasisArtifact(label: "Account snapshot", symbol: "building.columns"),
        ]
        if scenario.dataStale {
            items.append(BasisArtifact(label: "Freshness budget exceeded", symbol: "exclamationmark.arrow.circlepath"))
        }
        if scenario.criticTriggered {
            items.append(BasisArtifact(label: "Conflict report", symbol: "exclamationmark.shield"))
        }
        return items
    }

    /// config/akzio.toml sets `low`; the gallery scenarios exercise the other steps.
    static func intensity(scenario: MockScenario) -> ReasoningIntensity {
        switch scenario {
        case .criticTriggeredMaterialConflict, .decisionBlocked: .high
        case .policyProven, .policyContested: .medium
        default: .low
        }
    }

    static func roles(scenario: MockScenario) -> [RoleCardPresentation] {
        var generator = SeededGenerator(seed: scenario.seed &+ 433)
        let running = scenario.workflowStatus == .running

        func card(
            _ role: AgentRole,
            model: String,
            status: AkzioStatus,
            hasMetrics: Bool = true
        ) -> RoleCardPresentation {
            RoleCardPresentation(
                role: role,
                model: model,
                status: status,
                tokensIn: hasMetrics ? generator.int(in: 8_000...42_000) : nil,
                tokensOut: hasMetrics ? generator.int(in: 1_200...9_800) : nil,
                toolCalls: hasMetrics ? generator.int(in: 2...18) : nil,
                latencyMillis: hasMetrics ? generator.int(in: 420...5_400) : nil,
                confidencePpm: hasMetrics && !scenario.dataUnavailable ? generator.int(in: 640_000...940_000) : nil,
                intensity: intensity(scenario: scenario)
            )
        }

        return [
            card(.planner, model: ModelCatalog.primary, status: .succeeded),
            card(.analyst, model: ModelCatalog.primary, status: running ? .running : .succeeded),
            card(
                .critic,
                model: ModelCatalog.alternates[2],
                status: scenario.criticTriggered ? .running : .notTriggered,
                hasMetrics: scenario.criticTriggered
            ),
            card(.synthesizer, model: ModelCatalog.primary, status: running ? .running : .succeeded),
            card(
                .outcomeWorker,
                model: ModelCatalog.alternates[0],
                status: scenario.sealedHorizons.isEmpty ? .queued : .observing,
                hasMetrics: !scenario.sealedHorizons.isEmpty
            ),
        ]
    }

    static func council(scenario: MockScenario) -> CouncilPresentation {
        let cards = roles(scenario: scenario)
        let selected: AgentRole = scenario.criticTriggered ? .critic : .synthesizer
        var generator = SeededGenerator(seed: scenario.seed &+ 461)
        return CouncilPresentation(
            roles: cards,
            selectedRole: selected,
            selectedModelName: cards.first { $0.role == selected }?.model ?? ModelCatalog.primary,
            selectedModelSummary: scenario.criticTriggered
                ? "Reviewing a material conflict between momentum and liquidity claims."
                : "Converging analyst claims into a single allocation proposal.",
            selectedPath: ["Evidence", "Claims", "Conflict Check", "Synthesis"],
            intensity: intensity(scenario: scenario),
            gallery: ModelCatalog.gallery,
            alternatives: alternatives(scenario: scenario),
            uncertainties: uncertainties(scenario: scenario),
            basisArtifacts: basisArtifacts(scenario: scenario),
            overallUncertaintyPpm: scenario.dataUnavailable ? nil : generator.int(in: 180_000...340_000)
        )
    }

    static func inspector(scenario: MockScenario, nodes: [WorkflowNodePresentation]) -> StageInspectorPresentation {
        let node = nodes.first { $0.isActive } ?? nodes.first { $0.taskStatus == .failed } ?? nodes[0]
        var generator = SeededGenerator(seed: scenario.seed &+ 487)
        return StageInspectorPresentation(
            stageTitle: node.stage.displayName,
            status: node.status,
            model: ModelCatalog.primary,
            reasoningMode: intensity(scenario: scenario).displayName,
            turn: generator.int(in: 2...6),
            totalTurns: 8,
            toolCalls: generator.int(in: 3...14),
            latencyMillis: scenario.dataUnavailable ? nil : generator.int(in: 640...4_200),
            confidencePpm: node.confidencePpm ?? (scenario.dataUnavailable ? nil : generator.int(in: 600_000...880_000)),
            summary: node.isBlocked
                ? "Blocked before execution: a hard blocker is present and no order may be submitted."
                : "Claims are consistent with the evidence set; proceeding along the critical path.",
            alternatives: alternatives(scenario: scenario).map(\.label),
            uncertainties: uncertainties(scenario: scenario),
            blockers: node.blockers,
            warnings: node.warnings
        )
    }
}
