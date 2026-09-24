import Foundation

// MARK: - Outcome

/// One T+N ring. `progress == nil` means the window has not opened yet, which the
/// ring draws as a dashed track rather than 0%.
public struct HorizonPresentation: Sendable, Hashable, Identifiable {
    // HorizonPresentation 把 Outcome 窗口状态投影为环所需字段；nil progress 表示尚未开始。
    public let horizon: OutcomeHorizonKind
    public let status: AkzioStatus
    public let progress: Double?
    public let evidenceCompletenessPpm: Int?
    public let isSealed: Bool
    public let note: String

    public var id: String { horizon.rawValue }

    public init(
        horizon: OutcomeHorizonKind,
        status: AkzioStatus,
        progress: Double?,
        evidenceCompletenessPpm: Int?,
        isSealed: Bool,
        note: String
    ) {
        // 初始化保留 sealed 和 progress 的独立语义，页面不能从 status 反推封存。
        self.horizon = horizon
        self.status = status
        self.progress = progress
        self.evidenceCompletenessPpm = evidenceCompletenessPpm
        self.isSealed = isSealed
        self.note = note
    }

}

/// Mirrors `OutcomeWindow` (evaluation.rs:130). `calibrationPpm` and `riskRecallPpm`
/// are optional in Rust and must stay optional here.
public struct OutcomeWindowPresentation: Sendable, Hashable, Identifiable {
    // OutcomeWindow 镜像 Rust evaluation 窗口；校准、风险召回和效果字段保持 Optional。
    public let horizon: OutcomeHorizonKind
    public let portfolioReturnPpm: Int
    public let implementationEffectPpm: Int?
    public let initialValuationEffectPpm: Int?
    public let benchmarkReturnPpm: Int
    public let transactionCostPpm: Int
    public let slippagePpm: Int
    public let utilityPpm: Int
    public let calibrationPpm: Int?
    public let evidenceCompletenessPpm: Int
    public let riskRecallPpm: Int?
    public let winRatePpm: Int?
    public let profitFactorPpm: Int?
    public let sharpePpm: Int?
    public let maxDrawdownPpm: Int?
    public let comparison: [EquityPoint]

    public var id: String { horizon.rawValue }

    public init(
        horizon: OutcomeHorizonKind,
        portfolioReturnPpm: Int,
        implementationEffectPpm: Int? = nil,
        initialValuationEffectPpm: Int? = nil,
        benchmarkReturnPpm: Int,
        transactionCostPpm: Int,
        slippagePpm: Int,
        utilityPpm: Int,
        calibrationPpm: Int?,
        evidenceCompletenessPpm: Int,
        riskRecallPpm: Int?,
        winRatePpm: Int?,
        profitFactorPpm: Int?,
        sharpePpm: Int?,
        maxDrawdownPpm: Int?,
        comparison: [EquityPoint]
    ) {
        // 初始化逐项复制结果与对比曲线，缺失项不在这里补零。
        self.horizon = horizon
        self.portfolioReturnPpm = portfolioReturnPpm
        self.implementationEffectPpm = implementationEffectPpm
        self.initialValuationEffectPpm = initialValuationEffectPpm
        self.benchmarkReturnPpm = benchmarkReturnPpm
        self.transactionCostPpm = transactionCostPpm
        self.slippagePpm = slippagePpm
        self.utilityPpm = utilityPpm
        self.calibrationPpm = calibrationPpm
        self.evidenceCompletenessPpm = evidenceCompletenessPpm
        self.riskRecallPpm = riskRecallPpm
        self.winRatePpm = winRatePpm
        self.profitFactorPpm = profitFactorPpm
        self.sharpePpm = sharpePpm
        self.maxDrawdownPpm = maxDrawdownPpm
        self.comparison = comparison
    }

    // alpha/netReturn 是页面读取时的确定性派生值，Optional effect 缺失按领域约定作为零贡献。
    public var alphaPpm: Int { portfolioReturnPpm - benchmarkReturnPpm }
    public var netReturnPpm: Int { portfolioReturnPpm + (implementationEffectPpm ?? 0) + (initialValuationEffectPpm ?? 0) - transactionCostPpm - slippagePpm }
}

public struct OutcomePresentation: Sendable, Hashable {
    // OutcomePresentation 聚合所有 horizon ring/window，并显式保留 availability 边界。
    public let horizons: [HorizonPresentation]
    public let windows: [OutcomeWindowPresentation]
    public let selected: OutcomeHorizonKind
    public let observedTradingDays: Int?
    public let totalTradingDays: Int
    public let outcomeID: String
    public let availabilityStatus: AkzioStatus
    public let availabilityReason: String?

    public init(
        horizons: [HorizonPresentation],
        windows: [OutcomeWindowPresentation],
        selected: OutcomeHorizonKind,
        observedTradingDays: Int?,
        totalTradingDays: Int,
        outcomeID: String,
        availabilityStatus: AkzioStatus = .completed,
        availabilityReason: String? = nil
    ) {
        // 初始化不把 unavailable/partial 状态折叠为已完成，原因由上游 Optional 提供。
        self.horizons = horizons
        self.windows = windows
        self.selected = selected
        self.observedTradingDays = observedTradingDays
        self.totalTradingDays = totalTradingDays
        self.outcomeID = outcomeID
        self.availabilityStatus = availabilityStatus
        self.availabilityReason = availabilityReason
    }

    public func horizon(_ kind: OutcomeHorizonKind) -> HorizonPresentation? {
        // first 闭包按 horizon 查找环；缺失时由页面决定是否显示占位。
        horizons.first { $0.horizon == kind }
    }

    public func window(_ kind: OutcomeHorizonKind) -> OutcomeWindowPresentation? {
        // window 查询与 ring 查询分开，避免把窗口存在误认为环已经封存。
        windows.first { $0.horizon == kind }
    }

}
