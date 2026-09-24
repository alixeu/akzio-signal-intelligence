import Foundation

// 这些函数只做 payload → Presentation 的无副作用转换；Optional/默认值表达“Core 没有给出该事实”。

func liveObserverSectionStatus(_ status: String?) -> AkzioStatus {
    // 未知状态统一为 unavailable；客户端不扩展服务端状态机。
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
    // 结构化窗口已通过 Codable 类型约束；只有补充 analytics 指标仍可为空。
    OutcomeWindowPresentation(
        horizon: OutcomeHorizonKind(rawValue: value.horizon) ?? .t1,
        portfolioReturnPpm: Int(value.portfolioReturnPpm),
        implementationEffectPpm: value.implementationEffectPpm.map(Int.init),
        initialValuationEffectPpm: value.initialValuationEffectPpm.map(Int.init),
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
    // 旧 JSON 窗口要求核心收益字段全部存在；任一必需字段缺失就跳过该窗口而非伪造 0。
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
        implementationEffectPpm: value["implementation_effect_ppm"]?.int,
        initialValuationEffectPpm: value["initial_valuation_effect_ppm"]?.int,
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
    // comparison 的 ppm 只转换为绘图 Double；index*390 是展示坐标，不是交易日历或收益计算。
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
    // transition JSON 缺少 subject kind/id 时无法建立稳定轨道；其余统计字段保持“未提供”。
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
    // 结构化 metric 才能填充 sample/impact/stability/exposure；状态仍由 rawValue 解析并可缺失。
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
    // artifact kind 到 timeline kind 是白名单映射；未知种类只显示普通 event，不提升为 Outcome/Lesson。
    switch artifactKind {
    case "outcome", "outcome_schedule": .outcome
    case "retrospective", "experience": .lesson
    case "evaluation": .decision
    default: .event
    }
}

func liveAnalysisText(_ artifact: ObserverArtifactPayload) -> String? {
    // 只从各类 artifact 的约定字段抽取短文本，并 trim/过滤空值；原始 payload 不在 helper 中修改。
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
    // topic kind 只用于 UI 分类；未知 artifact 保守落到 topic。
    switch artifactKind {
    case "critique": .issue
    case "decision_proposal", "retrospective_draft": .conclusion
    default: .topic
    }
}

func liveRole(_ recipeID: String) -> AgentRole {
    // recipeID 是 Core 拓扑标识，按固定关键词映射角色；未命中时回退 analyst，不改变任务本身。
    let value = recipeID.lowercased()
    if value.contains("planner") { return .planner }
    if value.contains("critic") { return .critic }
    if value.contains("synth") || value.contains("decision") { return .synthesizer }
    if value.contains("outcome") || value.contains("learning") { return .outcomeWorker }
    return .analyst
}

func liveTaskStatus(_ rawValue: String) -> AkzioStatus {
    // 未知 TaskStatus 回退 pending，再由状态 helper 转成 UI status。
    (TaskStatus(rawValue: rawValue) ?? .pending).status(optional: false)
}

func liveRatio(_ value: Int64, _ total: Int64) -> Int {
    // total 为零时返回 0 只是防除零的显示边界，不表示比例已测量或账户无价值。
    guard total != 0 else { return 0 }
    return Int((Double(value) * 1_000_000 / Double(total)).rounded())
}

func liveTimeLabel(_ date: Date) -> String {
    // 日期格式化只生成本地 projection 文本，使用 POSIX locale 避免环境差异影响快照/测试。
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "HH:mm:ss"
    return formatter.string(from: date)
}

func liveDateLabel(_ date: Date) -> String {
    // 日级标签与 time label 一样不改变 Date，只决定列表/时间线的展示粒度。
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd"
    return formatter.string(from: date)
}
