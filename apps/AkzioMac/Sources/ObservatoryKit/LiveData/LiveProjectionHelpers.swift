import Foundation

func liveObserverSectionStatus(_ status: String?) -> AkzioStatus {
    switch status {
    case "available": .completed
    case "pending": .waiting
    case "stale": .stale
    default: .unavailable
    }
}

func liveOutcomeWindow(
    _ value: ObserverOutcomeWindowPayload,
    metrics: ObserverOutcomeHorizonPayload?
) -> OutcomeWindowPresentation {
    OutcomeWindowPresentation(
        horizon: OutcomeHorizonKind(rawValue: value.horizon) ?? .t1,
        portfolioReturnPpm: Int(value.portfolioReturnPpm),
        benchmarkReturnPpm: Int(value.benchmarkReturnPpm),
        transactionCostPpm: Int(value.transactionCostPpm),
        slippagePpm: Int(value.slippagePpm),
        utilityPpm: Int(value.utilityPpm),
        calibrationPpm: value.calibrationPpm.map(Int.init),
        evidenceCompletenessPpm: Int(value.evidenceCompletenessPpm),
        riskRecallPpm: value.riskRecallPpm.map(Int.init),
        winRatePpm: metrics?.winRatePpm.map(Int.init),
        profitFactorPpm: metrics?.profitFactorPpm.map(Int.init),
        sharpePpm: metrics?.sharpePpm.map(Int.init),
        maxDrawdownPpm: metrics?.maxDrawdownPpm.map(Int.init),
        comparison: liveOutcomeComparison(metrics)
    )
}

func liveOutcomeWindow(
    _ value: JSONValue,
    metrics: ObserverOutcomeHorizonPayload?
) -> OutcomeWindowPresentation? {
    guard let horizon = value["horizon"]?.string.flatMap(OutcomeHorizonKind.init(rawValue:)),
          let portfolioReturn = value["portfolio_return_ppm"]?.int,
          let benchmarkReturn = value["benchmark_return_ppm"]?.int,
          let transactionCost = value["transaction_cost_ppm"]?.int,
          let slippage = value["slippage_ppm"]?.int,
          let utility = value["utility_ppm"]?.int,
          let completeness = value["evidence_completeness_ppm"]?.int
    else { return nil }
    return OutcomeWindowPresentation(
        horizon: horizon,
        portfolioReturnPpm: portfolioReturn,
        benchmarkReturnPpm: benchmarkReturn,
        transactionCostPpm: transactionCost,
        slippagePpm: slippage,
        utilityPpm: utility,
        calibrationPpm: value["calibration_ppm"]?.int,
        evidenceCompletenessPpm: completeness,
        riskRecallPpm: value["risk_recall_ppm"]?.int,
        winRatePpm: metrics?.winRatePpm.map(Int.init),
        profitFactorPpm: metrics?.profitFactorPpm.map(Int.init),
        sharpePpm: metrics?.sharpePpm.map(Int.init),
        maxDrawdownPpm: metrics?.maxDrawdownPpm.map(Int.init),
        comparison: liveOutcomeComparison(metrics)
    )
}

func liveOutcomeComparison(
    _ metrics: ObserverOutcomeHorizonPayload?
) -> [EquityPoint] {
    (metrics?.comparison ?? []).enumerated().map { index, point in
        EquityPoint(
            index: index,
            minutesFromOpen: index * 390,
            portfolio: Double(point.portfolioPpm) / 10_000,
            benchmark: Double(point.benchmarkPpm) / 10_000
        )
    }
}

func livePolicyTrack(_ value: JSONValue) -> PolicyTrackPresentation? {
    guard let subjectValue = value["transition"]?["subject"],
          let subject = subjectValue["kind"]?.string.flatMap(PolicySubjectKind.init(rawValue:)),
          let name = subjectValue["id"]?.string
    else { return nil }
    let to = value["transition"]?["to"]
    let stateValue = to?["state"]?.string
    return PolicyTrackPresentation(
        subject: subject,
        name: String(name.prefix(24)),
        memoryState: subject == .memory ? stateValue.flatMap(MemoryLifecycle.init(rawValue:)) : nil,
        candidateState: subject == .memory
            ? nil
            : stateValue.flatMap(CandidatePolicyState.init(rawValue:)),
        activeSinceLabel: value["transition"]?["created_at"]?.string ?? MissingValue.unavailable.rawValue,
        sampleCount: 0,
        winRatePpm: nil,
        netImpactPpm: nil,
        stabilityPpm: nil,
        exposurePpm: nil
    )
}

func livePolicyTrack(_ metric: ObserverPolicyMetricPayload) -> PolicyTrackPresentation? {
    guard let subject = metric.subject["kind"]?.string.flatMap(PolicySubjectKind.init(rawValue:)),
          let name = metric.subject["id"]?.string
    else { return nil }
    let stateValue = metric.state["state"]?.string
    return PolicyTrackPresentation(
        subject: subject,
        name: String(name.prefix(24)),
        memoryState: subject == .memory ? stateValue.flatMap(MemoryLifecycle.init(rawValue:)) : nil,
        candidateState: subject == .memory
            ? nil
            : stateValue.flatMap(CandidatePolicyState.init(rawValue:)),
        activeSinceLabel: "Latest durable evaluation",
        sampleCount: metric.sampleCount,
        winRatePpm: metric.winRatePpm.map(Int.init),
        netImpactPpm: metric.netImpactPpm.map(Int.init),
        stabilityPpm: metric.stabilityPpm.map(Int.init),
        exposurePpm: metric.exposurePpm.map(Int.init)
    )
}

func liveTimelineKind(_ artifactKind: String) -> TimelineNodePresentation.Kind {
    switch artifactKind {
    case "outcome", "outcome_schedule": .outcome
    case "retrospective", "experience": .lesson
    case "evaluation": .decision
    default: .event
    }
}

func liveAnalysisText(_ artifact: ObserverArtifactPayload) -> String? {
    let values: [String?]
    switch artifact.kind {
    case "workflow_proposal_draft":
        values = [artifact.payload["summary"]?.string, artifact.payload["objective"]?.string]
    case "claim":
        values = [artifact.payload["statement"]?.string]
    case "critique", "decision_proposal", "retrospective_draft":
        values = [artifact.payload["summary"]?.string, artifact.payload["conclusion"]?.string]
    default:
        return nil
    }
    return values.compactMap { $0 }
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        .first { !$0.isEmpty }
}

func liveTopicKind(_ artifactKind: String) -> IntelligenceTopicKind {
    switch artifactKind {
    case "critique": .issue
    case "decision_proposal", "retrospective_draft": .conclusion
    default: .topic
    }
}

func liveRole(_ recipeID: String) -> AgentRole {
    let value = recipeID.lowercased()
    if value.contains("planner") { return .planner }
    if value.contains("critic") { return .critic }
    if value.contains("synth") || value.contains("decision") { return .synthesizer }
    if value.contains("outcome") || value.contains("learning") { return .outcomeWorker }
    return .analyst
}

func liveTaskStatus(_ rawValue: String) -> AkzioStatus {
    (TaskStatus(rawValue: rawValue) ?? .pending).status(optional: false)
}

func liveRatio(_ value: Int64, _ total: Int64) -> Int {
    guard total != 0 else { return 0 }
    return Int((Double(value) * 1_000_000 / Double(total)).rounded())
}

func liveTimeLabel(_ date: Date) -> String {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "HH:mm:ss"
    return formatter.string(from: date)
}

func liveDateLabel(_ date: Date) -> String {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd"
    return formatter.string(from: date)
}
