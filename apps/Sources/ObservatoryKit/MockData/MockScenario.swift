import Foundation

// MARK: - Scenarios
//
// Twenty fixed scenarios from the spec. Each one pins a seed and a set of expected
// states so screenshots and visual regressions are reproducible.
public enum MockScenario: Int, CaseIterable, Sendable, Identifiable {
    case paperRunningSynthesizerActive = 1
    case debugCompleted = 2
    case criticNotTriggered = 3
    case criticTriggeredMaterialConflict = 4
    case decisionBlocked = 5
    case executionNoOrder = 6
    case allOrdersFilled = 7
    case partialFillAndReprice = 8
    case horizonsMixed = 9
    case t5Completed = 10
    case retrospectiveMixed = 11
    case policyCandidate = 12
    case policyActive = 13
    case policyProven = 14
    case policyContested = 15
    case archiveFilteredResults = 16
    case settingsReduceMotion = 17
    case dataUnavailable = 18
    case staleData = 19
    case nonPaperPaperCommitNotApplicable = 20

    public var id: Int { rawValue }

    /// `01`…`20`, used in file names and the capture CLI.
    public var code: String { String(format: "%02d", rawValue) }

    /// Resolve a scenario from a CLI token: either its two-digit code or its title.
    public static func named(_ token: String) -> MockScenario? {
        if let number = Int(token), let scenario = MockScenario(rawValue: number) {
            return scenario
        }
        let needle = token.lowercased()
        return allCases.first { $0.title.lowercased() == needle }
    }

    public var seed: UInt64 { UInt64(rawValue) &* 7_919 }

    public var title: String {
        return switch self {
        case .paperRunningSynthesizerActive: "Paper Running — Synthesizer Active"
        case .debugCompleted: "Debug Completed"
        case .criticNotTriggered: "Critic Not Triggered"
        case .criticTriggeredMaterialConflict: "Critic Triggered — Material Conflict"
        case .decisionBlocked: "Decision Blocked"
        case .executionNoOrder: "Execution — No Order"
        case .allOrdersFilled: "All Orders Filled"
        case .partialFillAndReprice: "Partial Fill and Reprice"
        case .horizonsMixed: "T+1 Completed / T+3 Observing / T+5 Waiting"
        case .t5Completed: "T+5 Completed"
        case .retrospectiveMixed: "Retrospective Mixed"
        case .policyCandidate: "Policy Candidate"
        case .policyActive: "Policy Active"
        case .policyProven: "Policy Proven"
        case .policyContested: "Policy Contested"
        case .archiveFilteredResults: "Archive Filtered Results"
        case .settingsReduceMotion: "Settings — Reduce Motion Enabled"
        case .dataUnavailable: "Data Unavailable"
        case .staleData: "Stale Data"
        case .nonPaperPaperCommitNotApplicable: "Non-Paper — Paper Commit Not Applicable"
        }
    }

    /// Pages this scenario is meant to exercise, for the capture script.
    public var routes: [AppRoute] {
        return switch self {
        case .paperRunningSynthesizerActive: AppRoute.primary
        case .debugCompleted: [.overview, .workflow, .runArchive]
        case .criticNotTriggered, .criticTriggeredMaterialConflict, .decisionBlocked:
            [.workflow, .intelligence]
        case .executionNoOrder, .allOrdersFilled, .partialFillAndReprice:
            [.portfolio, .workflow]
        case .horizonsMixed, .t5Completed: [.outcome, .overview]
        case .retrospectiveMixed, .policyCandidate, .policyActive, .policyProven, .policyContested:
            [.learning]
        case .archiveFilteredResults: [.runArchive]
        case .settingsReduceMotion: [.overview, .workflow]
        case .dataUnavailable, .staleData: [.overview, .portfolio, .outcome]
        case .nonPaperPaperCommitNotApplicable: [.workflow, .overview]
        }
    }

    // MARK: Scenario switches

    public var purpose: RunPurpose {
        switch self {
        case .debugCompleted: .debug
        case .nonPaperPaperCommitNotApplicable: .replay
        case .settingsReduceMotion: .paperDryRun
        default: .paper
        }
    }

    public var workflowStatus: WorkflowStatus {
        switch self {
        case .debugCompleted, .t5Completed, .retrospectiveMixed,
             .policyCandidate, .policyActive, .policyProven, .policyContested,
             .archiveFilteredResults:
            .completed
        case .decisionBlocked: .completedWithExecutionRejection
        case .executionNoOrder: .decisionCompleted
        default: .running
        }
    }

    public var criticTriggered: Bool {
        switch self {
        case .criticTriggeredMaterialConflict, .decisionBlocked: true
        default: false
        }
    }

    public var isDecisionBlocked: Bool { self == .decisionBlocked }
    public var hasOrders: Bool {
        guard purpose.submitsPaperOrders, !isDecisionBlocked, self != .executionNoOrder else {
            return false
        }
        return switch self {
        case .allOrdersFilled, .partialFillAndReprice, .horizonsMixed, .t5Completed,
             .retrospectiveMixed, .policyCandidate, .policyActive, .policyProven,
             .policyContested, .archiveFilteredResults:
            true
        default:
            false
        }
    }
    public var hasPartialFill: Bool { self == .partialFillAndReprice }
    public var allFilled: Bool { self == .allOrdersFilled || self == .horizonsMixed || self == .t5Completed }

    /// Scenario 18 hides metrics; 19 marks them stale. Neither may print `0`.
    public var dataUnavailable: Bool { self == .dataUnavailable }
    public var dataStale: Bool { self == .staleData }
    public var reduceMotionPreferred: Bool { self == .settingsReduceMotion }

    public var sealedHorizons: Set<OutcomeHorizonKind> {
        guard purpose.isCanonical else { return [] }
        return switch self {
        case .t5Completed: [.t1, .t3, .t5]
        case .horizonsMixed, .retrospectiveMixed, .policyCandidate,
             .policyActive, .policyProven, .policyContested:
            [.t1]
        default: []
        }
    }

    public var observingHorizon: OutcomeHorizonKind? {
        self == .horizonsMixed ? .t3 : nil
    }

    public var memoryLifecycle: MemoryLifecycle {
        switch self {
        case .policyCandidate: .candidate
        case .policyActive: .active
        case .policyProven: .proven
        case .policyContested: .contested
        default: .active
        }
    }

    public var candidateState: CandidatePolicyState {
        switch self {
        case .policyCandidate: .candidate
        case .policyActive: .canary25
        case .policyProven: .active
        case .policyContested: .canary10
        default: .canary25
        }
    }
}
