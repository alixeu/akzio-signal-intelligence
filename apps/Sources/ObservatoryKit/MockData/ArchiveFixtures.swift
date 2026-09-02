import Foundation

// MARK: - Run archive fixtures

public enum ArchiveFixtures {
    static let pageSize = 12
    static let totalRuns = 1_284

    /// The one real Paper canary in the Store (crates checkpoint, 2026-08-17).
    /// It is pinned rather than generated so the archive shows a run the operator
    /// can recognise.
    public static let canaryRunID = "77395cfd-8d03-405d-9b47-ca99b19525f1"

    static func rows(
        scenario: MockScenario,
        currentRun: RunPresentation
    ) -> [ArchiveRowPresentation] {
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
        let hex = "0123456789abcdef".map { String($0) }
        func block(_ length: Int) -> String {
            (0..<length).map { _ in generator.pick(hex) }.joined()
        }
        return [block(8), block(4), block(4), block(4), block(12)].joined(separator: "-")
    }

    private static func stageLabel(status: WorkflowStatus) -> String {
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
