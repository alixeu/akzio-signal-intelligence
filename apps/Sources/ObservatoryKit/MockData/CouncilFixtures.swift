import Foundation

// MARK: - Council & inspector fixtures

enum CouncilFixtures {
    static func uncertainties(scenario: MockScenario) -> [UncertaintyPresentation] {
        // 数据不可用时不返回不确定性权重；正常场景的 label 顺序固定。
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
        // alternatives 是固定的研究分支展示，第三项在数据不可用时保持 nil 匹配度。
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
        // 基础材料列表随 stale/critic 场景追加提示，反映上下文来源而不伪造新证据。
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
        // reasoning intensity 由 scenario 选择展示级别；不改变实际模型配置。
        switch scenario {
        case .criticTriggeredMaterialConflict, .decisionBlocked: .high
        case .policyProven, .policyContested: .medium
        default: .low
        }
    }

    static func roles(scenario: MockScenario) -> [RoleCardPresentation] {
        // roles 用同一 generator 生成 token/latency 等指标，状态与场景的 workflow 对齐。
        var generator = SeededGenerator(seed: scenario.seed &+ 433)
        let running = scenario.workflowStatus == .running

        func card(
            _ role: AgentRole,
            model: String,
            status: AkzioStatus,
            hasMetrics: Bool = true
        ) -> RoleCardPresentation {
            // card 闭包统一处理有指标与无指标角色，缺失值继续保持 nil。
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
        // council 汇总角色、模型候选、替代方案、不确定性和依据材料，供 Intelligence 页面读取。
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
        // inspector 优先选择当前节点，其次选择失败节点，最后回退到节点列表首项。
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
