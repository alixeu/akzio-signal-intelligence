import Foundation

// MARK: - Outcome

/// One T+N ring. `progress == nil` means the window has not opened yet, which the
/// ring draws as a dashed track rather than 0%.
public struct HorizonPresentation: Sendable, Hashable, Identifiable {
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
        self.horizon = horizon
        self.status = status
        self.progress = progress
        self.evidenceCompletenessPpm = evidenceCompletenessPpm
        self.isSealed = isSealed
        self.note = note
    }

    /// Guard rail from the domain: only a sealed outcome may read as Completed.
    public var isConsistent: Bool {
        status == .completed ? isSealed : true
    }
}

/// Mirrors `OutcomeWindow` (evaluation.rs:130). `calibrationPpm` and `riskRecallPpm`
/// are optional in Rust and must stay optional here.
public struct OutcomeWindowPresentation: Sendable, Hashable, Identifiable {
    public let horizon: OutcomeHorizonKind
    public let portfolioReturnPpm: Int
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
        self.horizon = horizon
        self.portfolioReturnPpm = portfolioReturnPpm
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

    public var alphaPpm: Int { portfolioReturnPpm - benchmarkReturnPpm }
    public var netReturnPpm: Int { portfolioReturnPpm - transactionCostPpm - slippagePpm }
}

public struct OutcomePresentation: Sendable, Hashable {
    public let horizons: [HorizonPresentation]
    public let windows: [OutcomeWindowPresentation]
    public let selected: OutcomeHorizonKind
    public let observedTradingDays: Int
    public let totalTradingDays: Int
    public let outcomeID: String
    public let availabilityStatus: AkzioStatus
    public let availabilityReason: String?

    public init(
        horizons: [HorizonPresentation],
        windows: [OutcomeWindowPresentation],
        selected: OutcomeHorizonKind,
        observedTradingDays: Int,
        totalTradingDays: Int,
        outcomeID: String,
        availabilityStatus: AkzioStatus = .completed,
        availabilityReason: String? = nil
    ) {
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
        horizons.first { $0.horizon == kind }
    }

    public func window(_ kind: OutcomeHorizonKind) -> OutcomeWindowPresentation? {
        windows.first { $0.horizon == kind }
    }

    public var selectedWindow: OutcomeWindowPresentation? { window(selected) }

    /// At most one ring may breathe at a time.
    public var observingCount: Int { horizons.filter { $0.status == .observing }.count }
}
