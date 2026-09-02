import Foundation

// MARK: - Workflow fixtures
//
// The DAG shape is fixed; only task statuses move per scenario. Column/row are
// assigned here so the canvas, the inspector and the accessibility list agree.
enum WorkflowFixtures {
    /// Execution order. The index in this array is the progress ruler.
    static let order: [WorkflowStageKind] = [
        .planner,
        .evidenceGate,
        .analyst(1), .analyst(2), .analyst(3),
        .critic,
        .synthesizer,
        .decisionGate,
        .executionGate,
        .paperCommit,
        .reconcile,
        .evaluate,
        .horizon(.t1), .horizon(.t3), .horizon(.t5),
        .learning,
    ]

    /// The stage currently running, or nil when the run reached a terminal state.
    static func activeStage(_ scenario: MockScenario) -> WorkflowStageKind? {
        switch scenario {
        case .criticTriggeredMaterialConflict: .critic
        case .allOrdersFilled, .partialFillAndReprice: .reconcile
        case .horizonsMixed: .horizon(.t3)
        case .debugCompleted, .t5Completed, .retrospectiveMixed, .decisionBlocked,
             .executionNoOrder, .policyCandidate, .policyActive, .policyProven,
             .policyContested, .archiveFilteredResults:
            nil
        default: .synthesizer
        }
    }

    static func nodes(scenario: MockScenario) -> [WorkflowNodePresentation] {
        var generator = SeededGenerator(seed: scenario.seed &+ 307)
        let active = activeStage(scenario)
        let frontier = active.flatMap { stage in order.firstIndex(of: stage) } ?? order.count

        return order.enumerated().map { index, stage in
            let position = WorkflowLayout.position(stage)
            let applicable = stage.requiresPaperRun ? scenario.purpose.submitsPaperOrders : true
            var status: TaskStatus = index < frontier ? .succeeded : (index == frontier ? .running : .pending)
            var blockers: [HardBlocker] = []
            var warnings: [SoftWarning] = []

            if stage == .critic, !scenario.criticTriggered {
                status = .skipped
            }
            if !applicable {
                status = .skipped
            }
            if case .horizon(let horizon) = stage {
                status = horizonStatus(scenario: scenario, horizon: horizon, activeStage: active)
            }
            if stage == .learning, active != nil {
                status = .pending
            }
            if scenario.isDecisionBlocked {
                switch stage {
                case .decisionGate:
                    status = .failed
                    blockers = [.materialConflict]
                case .executionGate, .paperCommit, .reconcile:
                    status = .skipped
                default:
                    break
                }
            }
            if scenario == .executionNoOrder, stage == .paperCommit || stage == .reconcile {
                status = .skipped
            }
            if scenario.dataStale, stage == .evidenceGate {
                warnings = [.staleNoncriticalEvidence]
            }
            if scenario.hasPartialFill, stage == .reconcile {
                warnings = [.elevatedTurnover]
            }

            let confidence: Int? = status == .succeeded && stage.role != nil
                ? generator.int(in: 720_000...940_000)
                : nil

            return WorkflowNodePresentation(
                stage: stage,
                taskStatus: status,
                isApplicable: applicable,
                confidencePpm: confidence,
                blockers: blockers,
                warnings: warnings,
                column: position.column,
                row: position.row
            )
        }
    }

    private static func horizonStatus(
        scenario: MockScenario,
        horizon: OutcomeHorizonKind,
        activeStage: WorkflowStageKind?
    ) -> TaskStatus {
        if scenario.sealedHorizons.contains(horizon) { return .succeeded }
        return activeStage == .horizon(horizon) ? .running : .pending
    }

    static func edges(scenario: MockScenario) -> [WorkflowEdgePresentation] {
        var edges: [WorkflowEdgePresentation] = [
            .init(from: .planner, to: .evidenceGate, kind: .sequential)
        ]
        for index in 1...3 {
            edges.append(.init(from: .evidenceGate, to: .analyst(index), kind: .parallel))
            edges.append(.init(from: .analyst(index), to: .critic, kind: .optional))
            edges.append(.init(from: .analyst(index), to: .synthesizer, kind: .sequential))
        }
        edges.append(.init(from: .critic, to: .synthesizer, kind: .optional))
        if scenario.criticTriggered {
            edges.append(.init(from: .critic, to: .analyst(2), kind: .loopBack))
            edges.append(.init(from: .analyst(1), to: .analyst(3), kind: .conflict))
        }
        edges += [
            .init(from: .synthesizer, to: .decisionGate, kind: .criticalPath),
            .init(from: .decisionGate, to: .executionGate, kind: .criticalPath),
            .init(from: .executionGate, to: .paperCommit, kind: .sequential),
            .init(from: .paperCommit, to: .reconcile, kind: .sequential),
            .init(from: .reconcile, to: .evaluate, kind: .sequential),
        ]
        for horizon in OutcomeHorizonKind.allCases {
            edges.append(.init(from: .evaluate, to: .horizon(horizon), kind: .parallel))
            edges.append(.init(from: .horizon(horizon), to: .learning, kind: .sequential))
        }
        return edges
    }
}
