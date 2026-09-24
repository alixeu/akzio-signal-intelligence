import Foundation

struct LiveProjection: Sendable {
    // Projection 是跨 Task 传递的不可变值快照；View 读取它，不直接共享 Observer 的可变解码对象。
    let generatedAt: Date
    let run: RunPresentation
    let workflow: WorkflowPresentation
    let archive: ArchivePresentation
    let events: [EventPresentation]
    let agents: [AgentRailItem]
    let health: [HealthMetric]
    let council: CouncilPresentation
    let portfolio: PortfolioPresentation
    let outcome: OutcomePresentation
    let learning: LearningPresentation
    let hasArchiveSuccessRate: Bool

    init(
        payload: ObserverSnapshotPayload,
        reasoningRecords: [LiveReasoningRecord] = [],
        now: Date = Date()
    ) {
        // 一个 payload 只生成一套 run/workflow/archive/页面投影；now 可注入以保持截图和测试确定性。
        generatedAt = payload.generatedAt
        let workflows = payload.runs
        let current = workflows.first
        let summaries = Dictionary(
            uniqueKeysWithValues: (payload.runSummaries ?? []).map { ($0.runID, $0) }
        )
        run = Self.run(
            current,
            summary: current.flatMap { summaries[$0.run.runID] },
            payload: payload,
            now: now
        )
        workflow = Self.workflow(
            current,
            detail: payload.currentRun,
            outcome: payload.outcome?.data,
            reasoningRecords: reasoningRecords
        )
        archive = Self.archive(workflows, summaries: summaries)
        let terminal = workflows.filter { Self.workflowStatus($0.status).isTerminal }
        hasArchiveSuccessRate = !terminal.isEmpty
        events = Self.events(payload)
        agents = Self.agents(current)
        council = Self.council(payload, reasoningRecords: reasoningRecords)
        portfolio = Self.portfolio(payload)
        outcome = Self.outcome(payload)
        learning = Self.learning(payload)
        let readinessPpm = payload.core.readinessPpm.map(Int.init)
        // health 只汇总 Core 已提供的 readiness/health/policy/news/alerts/cursor，不把缺失值补成成功。
        health = [
            HealthMetric(
                id: "readiness",
                label: "Core Readiness",
                value: PpmFormatter.share(ppm: readinessPpm, fractionDigits: 0),
                fraction: PpmFormatter.fraction(ppm: readinessPpm),
                isElevatedRisk: readinessPpm != 1_000_000
            ),
            HealthMetric(
                id: "daemon",
                label: "Daemon",
                value: payload.health.frozen
                    ? "Frozen"
                    : (payload.health.status == "ok" ? "OK" : payload.health.status),
                fraction: nil,
                isElevatedRisk: payload.health.frozen || payload.health.status != "ok"
            ),
            HealthMetric(
                id: "decision-policy",
                label: "Decision Policy",
                value: payload.health.decisionPolicyStatus ?? "Unknown",
                fraction: nil,
                isElevatedRisk: payload.health.decisionCapable != true
            ),
            HealthMetric(
                id: "news-web",
                label: "News Evidence",
                value: payload.health.newsWebStatus.map { status in
                    if let route = payload.health.newsWebRoute, !route.isEmpty {
                        return "\(status) · \(route)"
                    }
                    return status
                } ?? "Unknown",
                fraction: nil,
                isElevatedRisk: payload.health.newsWebStatus != "verified"
            ),
            HealthMetric(
                id: "alerts",
                label: "Store Alerts",
                value: String(payload.health.alerts.count),
                fraction: nil,
                isElevatedRisk: !payload.health.alerts.isEmpty
            ),
            HealthMetric(
                id: "cursor",
                label: "Event Cursor",
                value: String(payload.eventCursor),
                fraction: nil,
                isElevatedRisk: false
            ),
        ]
    }

    private static func run(
        _ workflow: ObserverWorkflowPayload?,
        summary: ObserverRunSummaryPayload?,
        payload: ObserverSnapshotPayload,
        now: Date
    ) -> RunPresentation {
        // 没有 current workflow 时返回明确 unavailable/debug 占位；有 workflow 时 elapsed 由快照时间和注入 now 得到。
        guard let workflow else {
            return RunPresentation(
                runId: "unavailable",
                purpose: .debug,
                status: .queued,
                topology: "Unavailable",
                model: "Unavailable",
                market: "Unavailable",
                startedAt: payload.generatedAt,
                elapsedSeconds: 0,
                systemHealthPpm: payload.core.readinessPpm.map(Int.init),
                marketOpen: payload.portfolio.data?.marketOpen ?? false,
            marketStatusKnown: payload.portfolio.data != nil,
                dataLive: true,
                latencyMillis: summary?.latencyMillis.map(Int.init),
                brokerSession: "Unavailable"
            )
        }
        let end = workflow.finishedAt ?? now
        return RunPresentation(
            runId: workflow.run.runID,
            purpose: runPurpose(workflow.run.purpose),
            status: workflowStatus(workflow.status),
            topology: workflow.run.topologyID,
            model: summary?.modelID ?? payload.currentRun?.telemetry?.modelID
                ?? MissingValue.unavailable.rawValue,
            market: "US Equities",
            startedAt: workflow.run.createdAt,
            elapsedSeconds: max(0, Int(end.timeIntervalSince(workflow.run.createdAt))),
            systemHealthPpm: payload.core.readinessPpm.map(Int.init),
            marketOpen: payload.portfolio.data?.marketOpen ?? false,
            marketStatusKnown: payload.portfolio.data != nil,
            dataLive: true,
            latencyMillis: summary?.latencyMillis.map(Int.init)
                ?? payload.currentRun?.telemetry?.latencyMillis.map(Int.init),
            brokerSession: summary?.brokerSession
                ?? payload.portfolio.data?.brokerSession
                ?? MissingValue.unavailable.rawValue
        )
    }

    private static func workflow(
        _ workflow: ObserverWorkflowPayload?,
        detail: ObserverRunDetailPayload?,
        outcome: ObserverOutcomePayload?,
        reasoningRecords: [LiveReasoningRecord]
    ) -> WorkflowPresentation {
        // workflow 投影只把 Core task/dependency/trajectory 映射到布局；stage/status 的解释仍是显示层映射。
        guard let workflow else { return unavailableWorkflow }
        let trajectory = detail?.trajectory ?? []
        var analystIndex = 0
        var taskStages: [String: WorkflowStageKind] = [:]
        var stageOccurrences: [String: Int] = [:]
        let nodes = WorkflowDisplay.orderedTasks(workflow.tasks).map { task -> WorkflowNodePresentation in
            let stage = stage(for: task.node.recipeID, analystIndex: &analystIndex)
            taskStages[task.node.taskID] = stage
            let position = WorkflowLayout.position(stage)
            let occurrence = stageOccurrences[stage.id, default: 0]
            stageOccurrences[stage.id] = occurrence + 1
            let confidence = trajectory
                .filter { $0.taskID == task.node.taskID }
                .compactMap(\.deliberation)
                .last
                .map { Int($0.confidencePpm) }
            return WorkflowNodePresentation(
                taskID: task.node.taskID,
                stage: stage,
                taskStatus: taskStatus(task.taskStatus),
                isApplicable: true,
                confidencePpm: confidence,
                column: position.column,
                row: position.row + occurrence
            )
        }
        let edges = workflow.tasks.flatMap { task -> [WorkflowEdgePresentation] in
            guard taskStages[task.node.taskID] != nil else { return [] }
            return task.node.dependencies.compactMap { dependency in
                taskStages[dependency].map {
                    _ in WorkflowEdgePresentation(fromTask: dependency, toTask: task.node.taskID, kind: .sequential)
                }
            }
        }
        let activeTask = workflow.tasks.first { taskStatus($0.taskStatus) == .running }
            ?? workflow.tasks.first { taskStatus($0.taskStatus) == .leased }
        let activeStageID = activeTask?.node.taskID
        let artifactsByID = Dictionary(
            uniqueKeysWithValues: (detail?.artifacts ?? []).map { ($0.artifactID, $0) }
        )
        let inspectorPairs: [(String, StageInspectorPresentation)] = workflow.tasks.compactMap {
            // 每个 task 只生成自己的 inspector；找不到 stage 时返回 nil，不为未知 recipe 猜测角色。
            task -> (String, StageInspectorPresentation)? in
            guard let stage = taskStages[task.node.taskID] else { return nil }
            return (
                task.node.taskID,
                stageInspector(
                task: task,
                stage: stage,
                trajectory: trajectory,
                artifactsByID: artifactsByID,
                reasoningRecords: reasoningRecords.filter { $0.taskID == task.node.taskID },
                researchAudit: detail?.researchAudit?.records.filter { $0.taskID == task.node.taskID }.flatMap(\.rows) ?? []
                )
            )
        }
        let stageInspectors = Dictionary(uniqueKeysWithValues: inspectorPairs)
        let inspector = activeStageID.flatMap { stageInspectors[$0] }
            ?? StageInspectorPresentation(
                stageTitle: workflowStatus(workflow.status).displayName,
                status: workflowStatus(workflow.status).status,
                model: MissingValue.unavailable.rawValue,
                reasoningMode: MissingValue.unavailable.rawValue,
                turn: 0,
                totalTurns: 0,
                toolCalls: 0,
                latencyMillis: nil,
                confidencePpm: nil,
                summary: "No active task",
                alternatives: [],
                uncertainties: []
            )
        return WorkflowPresentation(
            nodes: nodes,
            edges: edges,
            activeStageID: activeStageID,
            inspector: inspector,
            observedTradingDays: Int(outcome?.completedTradingSessions ?? 0),
            totalTradingDays: 5,
            stageInspectors: stageInspectors
        )
    }

    private static func stageInspector(
        task: ObserverTaskPayload,
        stage: WorkflowStageKind,
        trajectory: [ObserverTrajectoryPayload],
        artifactsByID: [String: ObserverArtifactPayload],
        reasoningRecords: [LiveReasoningRecord],
        researchAudit: [ResearchAuditRowPresentation] = []
    ) -> StageInspectorPresentation {
        // inspector 的 token/tool/model 字段来自 trajectory 的最后/累计记录；Optional 保持“未观测”与 0 区分。
        let entries = trajectory.filter { $0.taskID == task.node.taskID }
        let deliberation = entries.compactMap(\.deliberation).last
        let uncertaintyWeights = deliberation?.uncertaintyWeightPpm ?? []
        let inputTokens = entries.compactMap(\.inputTokens)
        let outputTokens = entries.compactMap(\.outputTokens)
        let modelTurns = entries.filter { $0.turn != nil && $0.model != nil }.count
        let toolEvents = entries.compactMap { entry -> StageToolEventPresentation? in
            guard let tool = entry.tool else { return nil }
            return StageToolEventPresentation(
                cursor: entry.cursor,
                callID: tool.callID,
                name: tool.name ?? MissingValue.unavailable.rawValue,
                lifecycle: tool.lifecycle,
                turn: entry.turn.map(Int.init)
            )
        }
        let outputArtifacts = modelArtifacts(from: entries, artifactsByID: artifactsByID)
        let memoRecords = entries.compactMap { entry -> AnalysisRecordPresentation? in
            guard entry.phase == "draft",
                  let memo = entry.assistantText?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !memo.isEmpty
            else { return nil }
            return AnalysisRecordPresentation(
                id: "memo-\(entry.cursor)",
                sequence: entry.cursor,
                kind: .researchMemo,
                actor: stage.displayName,
                title: "Research memo",
                body: memo,
                model: entry.model?.modelID,
                reasoningMode: entry.model?.reasoningEffort,
                latencyMillis: entry.latencyMillis.map(Int.init),
                inputTokens: entry.inputTokens.map(Int.init),
                outputTokens: entry.outputTokens.map(Int.init)
            )
        }
        // CoreModelStage 命中时才显示模型/provider 缺失；纯 Rust task 明确显示 Rust/N/A，避免混淆执行器与 LLM。
        let modelMediated = CoreModelStage(rawValue: task.node.recipeID) != nil
        return StageInspectorPresentation(
            stageTitle: stage.displayName,
            status: taskStatus(task.taskStatus).status(optional: stage.isOptional),
            model: entries.compactMap(\.model?.modelID).last
                ?? (modelMediated ? MissingValue.unavailable.rawValue : "Rust"),
            provider: entries.compactMap(\.model?.providerID).last
                ?? (modelMediated ? MissingValue.unavailable.rawValue : "Rust"),
            reasoningMode: entries.compactMap(\.model?.reasoningEffort).last
                ?? (modelMediated ? MissingValue.unavailable.rawValue : "N/A"),
            turn: modelTurns,
            totalTurns: modelTurns,
            toolCalls: entries.filter {
                $0.tool != nil && $0.eventType.contains("called")
            }.count,
            latencyMillis: entries.compactMap(\.latencyMillis).last.map(Int.init),
            inputTokens: inputTokens.isEmpty ? nil : Int(inputTokens.reduce(0, +)),
            outputTokens: outputTokens.isEmpty ? nil : Int(outputTokens.reduce(0, +)),
            confidencePpm: deliberation.map { Int($0.confidencePpm) },
            summary: deliberation?.selectedPath ?? (modelMediated ? "" : task.node.objective),
            conclusion: outputArtifacts.reversed().compactMap(modelConclusion).first,
            alternatives: deliberation?.alternatives ?? [],
            uncertainties: (deliberation?.uncertainties ?? []).enumerated().map { index, label in
                UncertaintyPresentation(
                    label: label,
                    weightPpm: uncertaintyWeights.indices.contains(index)
                        ? Int(uncertaintyWeights[index])
                        : nil
                )
            },
            toolEvents: toolEvents,
            llmOutputs: outputArtifacts.map { artifact in
                StageLLMOutputPresentation(
                    id: artifact.artifactID,
                    kind: artifact.kind,
                    createdAt: artifact.createdAt,
                    body: boundedJSON(artifact.payload),
                    sequence: entries.first { entry in
                        entry.artifactID == artifact.artifactID
                            || entry.outputRefs?.contains(where: { $0.artifactID == artifact.artifactID }) == true
                    }?.cursor ?? 0
                )
            },
            transientAnalysisRecords: reasoningRecords.map(\.presentation) + memoRecords,
            taskID: task.node.taskID,
            horizon: task.node.horizon,
            researchAudit: researchAudit
        )
    }

    private static func modelArtifacts(
        from entries: [ObserverTrajectoryPayload],
        artifactsByID: [String: ObserverArtifactPayload]
    ) -> [ObserverArtifactPayload] {
        // 通过 artifactID 和 outputRefs 去重，再按 trajectory 首次出现顺序返回可展示的模型产物。
        var seen = Set<String>()
        var artifactIDs: [String] = []
        for entry in entries {
            if let artifactID = entry.artifactID,
               let kind = entry.artifactKind,
               modelOutputKinds.contains(kind),
               seen.insert(artifactID).inserted {
                artifactIDs.append(artifactID)
            }
            for reference in entry.outputRefs ?? []
            where modelOutputKinds.contains(reference.kind)
                && seen.insert(reference.artifactID).inserted {
                artifactIDs.append(reference.artifactID)
            }
        }
        return artifactIDs.compactMap { artifactsByID[$0] }
    }

    private static func modelConclusion(from artifact: ObserverArtifactPayload) -> String? {
        // 不同 artifact kind 使用各自受控字段读取结论；未知 kind 返回 nil，不把任意 JSON 文本当成结论。
        let keys: [String]
        switch artifact.kind {
        case "claim":
            keys = ["statement"]
        case "critique", "decision_proposal", "retrospective_draft":
            keys = ["summary", "conclusion"]
        default:
            return nil
        }
        return keys.compactMap { artifact.payload[$0]?.string }
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .first { !$0.isEmpty }
    }

    private static func boundedJSON(_ payload: JSONValue) -> String {
        // 大 payload 只在 UI 展示层截断到行数/字符数上限；原始 Codable/Store Artifact 不被修改。
        let rendered = payload.prettyPrinted
        let lines = rendered.split(separator: "\n", omittingEmptySubsequences: false)
        let lineLimited = lines.prefix(80).joined(separator: "\n")
        let value = String(lineLimited.prefix(6_000))
        return lines.count > 80 || value.count < lineLimited.count ? value + "\n…" : value
    }

    private static let modelOutputKinds: Set<String> = [
        "workflow_proposal_draft",
        "claim",
        "critique",
        "decision_proposal",
        "retrospective_draft",
    ]

    static var unavailableWorkflow: WorkflowPresentation {
        // 没有 durable workflow 时保留空 nodes/edges 和 unavailable inspector，UI 不模拟任务进度。
        WorkflowPresentation(
            nodes: [],
            edges: [],
            activeStageID: nil,
            inspector: StageInspectorPresentation(
                stageTitle: "Unavailable",
                status: .unavailable,
                model: "Unavailable",
                reasoningMode: "Unavailable",
                turn: 0,
                totalTurns: 0,
                toolCalls: 0,
                latencyMillis: nil,
                confidencePpm: nil,
                summary: "No durable workflow is available.",
                alternatives: [],
                uncertainties: []
            ),
            observedTradingDays: 0,
            totalTradingDays: 0
        )
    }

    private static func archive(
        _ workflows: [ObserverWorkflowPayload],
        summaries: [String: ObserverRunSummaryPayload]
    ) -> ArchivePresentation {
        // archive 每行保留 Run 的原始 purpose/status；successRate 只按 terminal runs 计算，未结束 Run 不进入分母。
        let rows = workflows.map { workflow -> ArchiveRowPresentation in
            let status = workflowStatus(workflow.status)
            let summary = summaries[workflow.run.runID]
            let end = workflow.finishedAt
            let active = workflow.tasks.first { taskStatus($0.taskStatus) == .running }
                ?? workflow.tasks.first { taskStatus($0.taskStatus) == .leased }
            let progress = WorkflowDisplay.orderedTasks(workflow.tasks).map { task in
                ArchiveStageProgress(
                    id: task.node.taskID,
                    label: task.node.recipeID,
                    horizon: task.node.horizon,
                    status: taskStatus(task.taskStatus).status(optional: false),
                    timeLabel: task.finishedAt.map(timeLabel) ?? MissingValue.pending.rawValue
                )
            }
            return ArchiveRowPresentation(
                id: workflow.run.runID,
                runID: workflow.run.runID,
                purposeLabel: runPurpose(workflow.run.purpose).displayName,
                purpose: runPurpose(workflow.run.purpose),
                topology: workflow.run.topologyID,
                status: status,
                durationSeconds: end.map {
                    max(0, Int($0.timeIntervalSince(workflow.run.createdAt)))
                },
                currentStage: active?.node.recipeID ?? status.displayName,
                model: summary?.modelID ?? MissingValue.unavailable.rawValue,
                resultPpm: summary?.resultUtilityPpm.map(Int.init),
                startedAt: workflow.run.createdAt,
                startedAtLabel: dateLabel(workflow.run.createdAt),
                stageProgress: progress
            )
        }
        let terminal = workflows.filter { workflowStatus($0.status).isTerminal }
        let completed = terminal.filter { [.completed, .completedWithExecutionRejection, .decisionCompleted].contains(workflowStatus($0.status)) }.count
        let successRate: Int? = terminal.isEmpty ? nil : completed * 1_000_000 / terminal.count
        return ArchivePresentation(
            rows: rows,
            totalRuns: rows.count,
            successRatePpm: successRate,
            page: 1,
            pageSize: max(1, rows.count),
            selectedRowID: rows.first?.id,
            activeFilters: []
        )
    }

    private static func agents(_ workflow: ObserverWorkflowPayload?) -> [AgentRailItem] {
        // agent rail 是 task 的显示列表；Rust task 没有模型轨迹时显示 Rust task，不冒充 LLM 调用。
        guard let workflow else { return [] }
        var analystIndex = 0
        return workflow.tasks.compactMap { task in
            let stage = stage(for: task.node.recipeID, analystIndex: &analystIndex)
            guard let role = stage.role else { return nil }
            let status = taskStatus(task.taskStatus)
            return AgentRailItem(
                id: task.node.taskID,
                name: stage.displayName,
                role: role,
                model: "Rust task",
                status: status.status(optional: stage.isOptional),
                activityLabel: task.node.objective,
                progressPpm: status == .succeeded ? 1_000_000 : (status == .running ? 500_000 : 0)
            )
        }
    }

    private static func stage(
        for recipeID: String,
        analystIndex: inout Int
    ) -> WorkflowStageKind {
        // recipeID 到 UI stage 是兼容映射；未命名节点按出现顺序编号 analyst，不能反向改变 Core graph。
        let value = recipeID.lowercased()
        if value.contains("planner") { return .planner }
        if value.contains("evidence") { return .evidenceGate }
        if value.contains("critic") { return .critic }
        if value.contains("synth") { return .synthesizer }
        if value.contains("decision") { return .decisionGate }
        if value.contains("reconcile") { return .reconcile }
        if value.contains("paper") || value.contains("commit") { return .paperCommit }
        if value.contains("execution") { return .executionGate }
        if value.contains("outcome") || value.contains("evaluate") { return .evaluate }
        if value.contains("learning") { return .learning }
        analystIndex += 1
        return .analyst(analystIndex)
    }

    private static func runPurpose(_ value: String) -> RunPurpose {
        // 未知 purpose 只回退 debug 展示；这不会修改 Rust payload 或授予新的执行权限。
        RunPurpose(rawValue: value) ?? .debug
    }

    private static func workflowStatus(_ value: String) -> WorkflowStatus {
        // 未知 workflow status 回退 queued，保持状态机的保守显示边界。
        WorkflowStatus(rawValue: value) ?? .queued
    }

    private static func taskStatus(_ value: String) -> TaskStatus {
        // 未知 task status 回退 pending，不把未识别状态当成 succeeded。
        TaskStatus(rawValue: value) ?? .pending
    }

    private static func dateLabel(_ date: Date) -> String {
        // DateFormatter 仅负责稳定的 POSIX 展示格式，Date 本身和 Core 时间戳不被修改。
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "yyyy-MM-dd HH:mm"
        return formatter.string(from: date)
    }

    private static func timeLabel(_ date: Date) -> String {
        // 时间标签同样是纯 projection helper，用于 archive/event 的显示，不参与排序。
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "HH:mm:ss"
        return formatter.string(from: date)
    }

}

private extension WorkflowStatus {
    var isTerminal: Bool {
        // 终态集合只影响 archive success-rate 分母；queued/leased/running 保持未完成。
        switch self {
        case .queued, .leased, .running: false
        case .decisionCompleted, .completed, .completedWithExecutionRejection, .failed, .cancelled:
            true
        }
    }
}
