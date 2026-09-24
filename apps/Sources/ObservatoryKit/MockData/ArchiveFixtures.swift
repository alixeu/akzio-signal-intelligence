import Foundation

// 文件导读：为 Run Archive 页面组装固定 canary、当前 Run 和场景化历史行；入口由
// ScenarioLibrary.build 调用，时间与指标来自冻结 anchor/场景 seed，而非 Store 查询。
// 这些行只用于 UI 演示与截图；即使固定 canary ID 对应历史真实 Run，也不提供实时状态或已核验收益。
// 先读 rows、stageProgress 与 archive：它们展示集合生成、状态映射，以及 Swift 闭包对局部随机生成器的同步捕获。
// MARK: - Run archive fixtures

public enum ArchiveFixtures {
    // pageSize/totalRuns 是归档页面的演示尺度；rows 仍只生成当前页可展示的 fixture 行。
    static let pageSize = 12
    static let totalRuns = 1_284

    // 这里仅固定一个可识别的历史 ID；不读取 Store，也不由 ID 推导成交、收益或校准资格。
    /// The one real Paper canary in the Store (crates checkpoint, 2026-08-17).
    /// It is pinned rather than generated so the archive shows a run the operator
    /// can recognise.
    public static let canaryRunID = "77395cfd-8d03-405d-9b47-ca99b19525f1"

    static func rows(
        scenario: MockScenario,
        currentRun: RunPresentation
    ) -> [ArchiveRowPresentation] {
        // 当前 run 和固定 canary 先放入列表，其余行由场景 seed 派生，保证筛选结果稳定。
        var generator = SeededGenerator(seed: scenario.seed &+ 907)
        let filtered = scenario == .archiveFilteredResults
        let purposes: [RunPurpose] = filtered ? [.paper] : [.paper, .paper, .debug, .paperDryRun, .replay]
        let statuses: [WorkflowStatus] = filtered
            ? [.completed]
            : [.completed, .completed, .running, .completedWithExecutionRejection, .decisionCompleted, .failed]

        var rows: [ArchiveRowPresentation] = [
            canaryRow(scenario: scenario),
            currentRow(scenario: scenario, run: currentRun),
        ]
        rows += (2..<pageSize).map { index in
            // Array.map 会立即按 index 顺序执行；闭包捕获并推进同一个局部 generator，
            // 因而行顺序属于 fixture 的可复现输入。没有封存结果的行保持 nil，避免伪造收益。
            let purpose = generator.pick(purposes)
            let status = generator.pick(statuses)
            let isTerminal = status != .running && status != .queued && status != .leased
            let sealedRun = isTerminal && purpose.isCanonical
            let startedAt = LearningFixtures.sessionDate(sessionsAgo: index / 2 + 1)
            return ArchiveRowPresentation(
                id: "archive-\(scenario.code)-\(index + 1)",
                runID: runID(&generator),
                purposeLabel: purpose.displayName,
                purpose: purpose,
                topology: purpose == .debug ? "fixture-debug" : "three-analyst-fanout",
                status: status,
                durationSeconds: isTerminal ? generator.int(in: 240...5_400) : nil,
                currentStage: stageLabel(status: status),
                model: generator.bool(probability: 0.7)
                    ? ModelCatalog.primary
                    : generator.pick(ModelCatalog.alternates),
                // Only sealed canonical runs have an outcome number; the rest stay Unavailable.
                resultPpm: sealedRun ? generator.int(in: -14_000...38_000) : nil,
                startedAt: startedAt,
                startedAtLabel: LearningFixtures.dateLabel(daysAgo: index / 2 + 1),
                stageProgress: stageProgress(status: status)
            )
        }
        return rows
    }

    private static func canaryRow(scenario: MockScenario) -> ArchiveRowPresentation {
        // canary 使用固定可识别 ID，但结果仍保持未知，ID 本身不等于已核验收益。
        let startedAt = LearningFixtures.sessionDate(sessionsAgo: 2)
        return ArchiveRowPresentation(
            id: "archive-\(scenario.code)-canary",
            runID: canaryRunID,
            purposeLabel: RunPurpose.paper.displayName,
            purpose: .paper,
            topology: "three-analyst-fanout",
            status: .completed,
            durationSeconds: 4_182,
            currentStage: WorkflowStageKind.learning.displayName,
            model: ModelCatalog.primary,
            // The id is real; a verified sealed return is not baked into fixtures.
            resultPpm: nil,
            startedAt: startedAt,
            startedAtLabel: LearningFixtures.dateLabel(daysAgo: 2),
            stageProgress: stageProgress(status: .completed)
        )
    }

    private static func currentRow(
        scenario: MockScenario,
        run: RunPresentation
    ) -> ArchiveRowPresentation {
        // 当前行从 RunPresentation 投影而来，运行中状态不填充 duration 或 result。
        let terminal = ![WorkflowStatus.queued, .leased, .running].contains(run.status)
        return ArchiveRowPresentation(
            id: "archive-\(scenario.code)-current",
            runID: run.runId,
            purposeLabel: run.purpose.displayName,
            purpose: run.purpose,
            topology: run.topology,
            status: run.status,
            durationSeconds: terminal ? run.elapsedSeconds : nil,
            currentStage: stageLabel(status: run.status),
            model: run.model,
            resultPpm: nil,
            startedAt: run.startedAt,
            startedAtLabel: LearningFixtures.dateLabel(daysAgo: 0),
            stageProgress: stageProgress(status: run.status)
        )
    }

    private static func runID(_ generator: inout SeededGenerator) -> String {
        // inout 将调用方持有的可变值临时借给本函数；block 同步捕获它并连续取样，
        // 返回时修改后的 generator 仍归调用方，期间不会另行复制出一条随机序列。
        let hex = "0123456789abcdef".map { String($0) }
        func block(_ length: Int) -> String {
            (0..<length).map { _ in generator.pick(hex) }.joined()
        }
        return [block(8), block(4), block(4), block(4), block(12)].joined(separator: "-")
    }

    private static func stageLabel(status: WorkflowStatus) -> String {
        // 将工作流总状态压成 Archive 行的当前阶段标签；这是 fixture 展示规则，不是 Core task 查询。
        switch status {
        case .running: WorkflowStageKind.synthesizer.displayName
        case .queued, .leased: WorkflowStageKind.planner.displayName
        case .decisionCompleted: WorkflowStageKind.decisionGate.displayName
        case .completedWithExecutionRejection: WorkflowStageKind.executionGate.displayName
        case .failed: WorkflowStageKind.evidenceGate.displayName
        case .completed, .cancelled: WorkflowStageKind.learning.displayName
        }
    }

    /// One row per pipeline milestone for the selected archive run.
    static func stageProgress(status: WorkflowStatus) -> [ArchiveStageProgress] {
        // 阶段进度把 WorkflowStatus 映射为一条可读时间线，未到达阶段统一保持 queued。
        let labels = [
            "Planner", "Evidence Gate", "Analyst", "Critic", "Synthesizer",
            "Decision Gate", "Execution Gate", "Paper Commit", "Reconcile",
            "Evaluate", "T+1", "Learning & Experience",
        ]
        let settled: Int
        let active: Int?
        let terminal: AkzioStatus?
        switch status {
        case .queued, .leased:
            settled = 0; active = nil; terminal = nil
        case .running:
            settled = 4; active = 4; terminal = nil
        case .decisionCompleted:
            settled = 6; active = nil; terminal = .notApplicable
        case .completed:
            settled = labels.count; active = nil; terminal = nil
        case .completedWithExecutionRejection:
            settled = 6; active = nil; terminal = .rejected
        case .failed:
            settled = 1; active = nil; terminal = .failed
        case .cancelled:
            settled = 1; active = nil; terminal = .cancelled
        }
        return labels.enumerated().map { index, label in
            // map 闭包按 settled/active/terminal 三个边界为每个阶段选择展示状态。
            let stageStatus: AkzioStatus
            if index < settled {
                stageStatus = .succeeded
            } else if index == active {
                stageStatus = .running
            } else if index == settled, let terminal {
                stageStatus = terminal
            } else {
                stageStatus = .queued
            }
            return ArchiveStageProgress(
                id: "fixture-stage-\(index)",
                label: label,
                status: stageStatus,
                timeLabel: stageStatus == .succeeded
                    ? String(format: "09:%02d", 31 + index)
                    : MissingValue.pending.rawValue
            )
        }
    }

    static func archive(
        scenario: MockScenario,
        currentRun: RunPresentation
    ) -> ArchivePresentation {
        // archive 汇总行、统计值、选中行和场景筛选标签，供 RunArchivePage 一次读取。
        var generator = SeededGenerator(seed: scenario.seed &+ 929)
        let items = rows(scenario: scenario, currentRun: currentRun)
        let filtered = scenario == .archiveFilteredResults
        return ArchivePresentation(
            rows: items,
            totalRuns: items.count,
            successRatePpm: generator.int(in: 780_000...960_000),
            page: 1,
            pageSize: pageSize,
            selectedRowID: items.first?.id,
            activeFilters: filtered ? ["Paper", "Completed", "Last 30 Sessions"] : []
        )
    }
}
