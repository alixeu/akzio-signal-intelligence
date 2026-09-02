import Foundation

enum WorkflowLayout {
    static func position(_ stage: WorkflowStageKind) -> (column: Int, row: Int) {
        switch stage {
        case .planner: (0, 1)
        case .evidenceGate: (1, 1)
        case .analyst(let index): (2, index - 1)
        case .critic: (3, 1)
        case .synthesizer: (4, 1)
        case .decisionGate: (5, 1)
        case .executionGate: (6, 1)
        case .paperCommit: (7, 1)
        case .reconcile: (8, 1)
        case .evaluate: (9, 1)
        case .horizon(let horizon):
            (10, horizon.tradingDays == 1 ? 0 : (horizon.tradingDays == 3 ? 1 : 2))
        case .learning: (11, 1)
        }
    }
}
