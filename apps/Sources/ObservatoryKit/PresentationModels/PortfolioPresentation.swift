import Foundation

// MARK: - Allocation flow & risk

public struct AllocationFlowStage: Sendable, Hashable, Identifiable {
    public let title: String
    public let symbol: String
    public let isActive: Bool

    public var id: String { title }

    public init(title: String, symbol: String, isActive: Bool) {
        self.title = title
        self.symbol = symbol
        self.isActive = isActive
    }
}

public struct RiskPresentation: Sendable, Hashable {
    public let betaPpm: Int?
    public let volatilityPpm: Int?
    public let maxDrawdownPpm: Int?
    public let varMicros: Int64?
    public let leveragePpm: Int?
    public let isElevated: Bool

    public init(
        betaPpm: Int?,
        volatilityPpm: Int?,
        maxDrawdownPpm: Int?,
        varMicros: Int64?,
        leveragePpm: Int?,
        isElevated: Bool
    ) {
        self.betaPpm = betaPpm
        self.volatilityPpm = volatilityPpm
        self.maxDrawdownPpm = maxDrawdownPpm
        self.varMicros = varMicros
        self.leveragePpm = leveragePpm
        self.isElevated = isElevated
    }

    public var status: AkzioStatus { isElevated ? .stale : .running }
    public var betaValue: Double? { betaPpm.map { Double($0) / PpmFormatter.ppmPerUnit } }
}

// MARK: - Portfolio aggregate

public struct PortfolioPresentation: Sendable, Hashable {
    public let equityMicros: Int64
    public let todayPnlMicros: Int64
    public let todayPnlPpm: Int
    public let unrealizedPnlMicros: Int64?
    public let realizedPnlMicros: Int64?
    public let unrealizedPnlPpm: Int?
    public let realizedPnlPpm: Int?

    public let curve: [EquityPoint]
    public let range: EquityRange
    public let benchmarkLabel: String

    public let allocations: [AllocationRow]
    public let positions: [PositionPresentation]
    public let orders: [OrderPresentation]
    public let fills: [FillPresentation]
    public let flow: [AllocationFlowStage]
    public let risk: RiskPresentation
    public let verdict: ExecutionVerdictKind
    public let reconciliation: ReconciliationState

    public init(
        equityMicros: Int64,
        todayPnlMicros: Int64,
        todayPnlPpm: Int,
        unrealizedPnlMicros: Int64?,
        realizedPnlMicros: Int64?,
        unrealizedPnlPpm: Int?,
        realizedPnlPpm: Int?,
        curve: [EquityPoint],
        range: EquityRange,
        benchmarkLabel: String,
        allocations: [AllocationRow],
        positions: [PositionPresentation],
        orders: [OrderPresentation],
        fills: [FillPresentation],
        flow: [AllocationFlowStage],
        risk: RiskPresentation,
        verdict: ExecutionVerdictKind,
        reconciliation: ReconciliationState
    ) {
        self.equityMicros = equityMicros
        self.todayPnlMicros = todayPnlMicros
        self.todayPnlPpm = todayPnlPpm
        self.unrealizedPnlMicros = unrealizedPnlMicros
        self.realizedPnlMicros = realizedPnlMicros
        self.unrealizedPnlPpm = unrealizedPnlPpm
        self.realizedPnlPpm = realizedPnlPpm
        self.curve = curve
        self.range = range
        self.benchmarkLabel = benchmarkLabel
        self.allocations = allocations
        self.positions = positions
        self.orders = orders
        self.fills = fills
        self.flow = flow
        self.risk = risk
        self.verdict = verdict
        self.reconciliation = reconciliation
    }

    public var equityValue: Double { Double(equityMicros) / PpmFormatter.ppmPerUnit }
    public var todayPnlValue: Double { Double(todayPnlMicros) / PpmFormatter.ppmPerUnit }
    public var isGain: Bool { todayPnlMicros >= 0 }

    /// Sparkline series shared with Overview; the same numbers drive both so the
    /// shared-element handoff lands on identical geometry.
    public var sparkSeries: [Double] { curve.map(\.portfolio) }

    /// `No Order` is a legitimate execution outcome, not a failure.
    public var hasOrders: Bool { !orders.isEmpty }
}
