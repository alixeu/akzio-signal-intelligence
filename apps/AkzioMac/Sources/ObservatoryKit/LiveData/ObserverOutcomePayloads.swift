import Foundation

struct ObserverPolicyMetricPayload: Decodable, Sendable {
    let subject: JSONValue
    let state: JSONValue
    let sampleCount: Int
    let winRatePpm: Int64?
    let netImpactPpm: Int64?
    let stabilityPpm: Int64?
    let exposurePpm: UInt32?

    enum CodingKeys: String, CodingKey {
        case subject
        case state
        case sampleCount = "sample_count"
        case winRatePpm = "win_rate_ppm"
        case netImpactPpm = "net_impact_ppm"
        case stabilityPpm = "stability_ppm"
        case exposurePpm = "exposure_ppm"
    }
}

struct ObserverPortfolioPayload: Decodable, Sendable {
    let brokerSession: String
    let marketOpen: Bool
    let status: String
    let equityMicros: Int64
    let lastEquityMicros: Int64?
    let buyingPowerMicros: Int64
    let dayPnlMicros: Int64?
    let dayPnlPpm: Int64?
    let realizedPnlMicros: Int64?
    let realizedPnlPpm: Int64?
    let fills: ObserverSectionPayload<[ObserverBrokerFillPayload]>?
    let analytics: ObserverSectionPayload<ObserverPortfolioAnalyticsPayload>?
    let positions: [ObserverPositionPayload]

    enum CodingKeys: String, CodingKey {
        case brokerSession = "broker_session"
        case marketOpen = "market_open"
        case status
        case equityMicros = "equity_micros"
        case lastEquityMicros = "last_equity_micros"
        case buyingPowerMicros = "buying_power_micros"
        case dayPnlMicros = "day_pnl_micros"
        case dayPnlPpm = "day_pnl_ppm"
        case realizedPnlMicros = "realized_pnl_micros"
        case realizedPnlPpm = "realized_pnl_ppm"
        case fills
        case analytics
        case positions
    }
}

struct ObserverPositionPayload: Decodable, Sendable {
    let symbol: String
    let quantityMicros: Int64
    let marketValueMicros: Int64
    let averageEntryPriceMicros: Int64?
    let unrealizedPnlMicros: Int64?
    let unrealizedPnlPpm: Int64?
    let sparklinePpm: [Int64]?

    enum CodingKeys: String, CodingKey {
        case symbol
        case quantityMicros = "quantity_micros"
        case marketValueMicros = "market_value_micros"
        case averageEntryPriceMicros = "average_entry_price_micros"
        case unrealizedPnlMicros = "unrealized_pnl_micros"
        case unrealizedPnlPpm = "unrealized_pnl_ppm"
        case sparklinePpm = "sparkline_ppm"
    }
}

struct ObserverBrokerFillPayload: Decodable, Sendable {
    let activityID: String
    let brokerOrderID: String
    let symbol: String
    let side: String
    let quantityMicros: Int64
    let priceMicros: Int64
    let transactionAt: Date
    let venue: String?
    let source: String

    enum CodingKeys: String, CodingKey {
        case activityID = "activity_id"
        case brokerOrderID = "broker_order_id"
        case symbol
        case side
        case quantityMicros = "quantity_micros"
        case priceMicros = "price_micros"
        case transactionAt = "transaction_at"
        case venue
        case source
    }
}

struct ObserverPortfolioAnalyticsPayload: Decodable, Sendable {
    let benchmarkSymbol: String
    let lookback: String
    let sampleCount: Int
    let betaPpm: Int64?
    let volatilityPpm: Int64
    let maxDrawdownPpm: Int64
    let var95Micros: Int64

    enum CodingKeys: String, CodingKey {
        case benchmarkSymbol = "benchmark_symbol"
        case lookback
        case sampleCount = "sample_count"
        case betaPpm = "beta_ppm"
        case volatilityPpm = "volatility_ppm"
        case maxDrawdownPpm = "max_drawdown_ppm"
        case var95Micros = "var_95_micros"
    }
}

struct ObserverOutcomePayload: Decodable, Sendable {
    let outcomeID: String
    let completedTradingSessions: UInt8
    let horizons: [ObserverOutcomeHorizonPayload]

    enum CodingKeys: String, CodingKey {
        case outcomeID = "outcome_id"
        case completedTradingSessions = "completed_trading_sessions"
        case horizons
    }
}

struct ObserverOutcomeHorizonPayload: Decodable, Sendable {
    let horizon: String
    let progressPpm: UInt32
    let window: ObserverOutcomeWindowPayload?
    let sampleCount: Int
    let winRatePpm: Int64?
    let profitFactorPpm: Int64?
    let sharpePpm: Int64?
    let maxDrawdownPpm: Int64?
    let comparison: [ObserverOutcomeComparisonPointPayload]

    enum CodingKeys: String, CodingKey {
        case horizon
        case progressPpm = "progress_ppm"
        case window
        case sampleCount = "sample_count"
        case winRatePpm = "win_rate_ppm"
        case profitFactorPpm = "profit_factor_ppm"
        case sharpePpm = "sharpe_ppm"
        case maxDrawdownPpm = "max_drawdown_ppm"
        case comparison
    }
}

struct ObserverOutcomeWindowPayload: Decodable, Sendable {
    let horizon: String
    let observedTradingDay: String
    let portfolioReturnPpm: Int64
    let benchmarkReturnPpm: Int64
    let transactionCostPpm: UInt32
    let slippagePpm: UInt32
    let utilityPpm: Int64
    let calibrationPpm: UInt32?
    let evidenceCompletenessPpm: UInt32
    let riskRecallPpm: UInt32?

    enum CodingKeys: String, CodingKey {
        case horizon
        case observedTradingDay = "observed_trading_day"
        case portfolioReturnPpm = "portfolio_return_ppm"
        case benchmarkReturnPpm = "benchmark_return_ppm"
        case transactionCostPpm = "transaction_cost_ppm"
        case slippagePpm = "slippage_ppm"
        case utilityPpm = "utility_ppm"
        case calibrationPpm = "calibration_ppm"
        case evidenceCompletenessPpm = "evidence_completeness_ppm"
        case riskRecallPpm = "risk_recall_ppm"
    }
}

struct ObserverOutcomeComparisonPointPayload: Decodable, Sendable {
    let tradingDay: String
    let portfolioPpm: Int64
    let benchmarkPpm: Int64

    enum CodingKeys: String, CodingKey {
        case tradingDay = "trading_day"
        case portfolioPpm = "portfolio_ppm"
        case benchmarkPpm = "benchmark_ppm"
    }
}

struct ObserverPortfolioHistoryPayload: Decodable, Sendable {
    let range: String
    let benchmarkSymbol: String?
    let points: [ObserverPortfolioHistoryPointPayload]

    enum CodingKeys: String, CodingKey {
        case range
        case benchmarkSymbol = "benchmark_symbol"
        case points
    }
}

struct ObserverPortfolioHistoryPointPayload: Decodable, Sendable {
    let timestamp: Date
    let equityMicros: Int64
    let profitLossMicros: Int64?
    let profitLossPpm: Int64?
    let benchmarkEquityMicros: Int64?

    enum CodingKeys: String, CodingKey {
        case timestamp
        case equityMicros = "equity_micros"
        case profitLossMicros = "profit_loss_micros"
        case profitLossPpm = "profit_loss_ppm"
        case benchmarkEquityMicros = "benchmark_equity_micros"
    }
}

struct ObserverWorkflowPayload: Decodable, Sendable {
    let run: ObserverRunPayload
    let status: String
    let finishedAt: Date?
    let tasks: [ObserverTaskPayload]
    let eventCursor: Int64

    enum CodingKeys: String, CodingKey {
        case run
        case status
        case finishedAt = "finished_at"
        case tasks
        case eventCursor = "event_cursor"
    }
}

struct ObserverRunPayload: Decodable, Sendable {
    let runID: String
    let purpose: String
    let topologyID: String
    let createdAt: Date

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
        case purpose
        case topologyID = "topology_id"
        case createdAt = "created_at"
    }
}

struct ObserverTaskPayload: Decodable, Sendable {
    let node: ObserverNodePayload
    let taskStatus: String
    let readyAt: Date
    let attemptCount: UInt64
    let finishedAt: Date?

    enum CodingKeys: String, CodingKey {
        case node
        case taskStatus = "status"
        case readyAt = "ready_at"
        case attemptCount = "attempt_count"
        case finishedAt = "finished_at"
    }
}

struct ObserverNodePayload: Decodable, Sendable {
    let taskID: String
    let recipeID: String
    let objective: String
    let dependencies: [String]

    enum CodingKeys: String, CodingKey {
        case taskID = "task_id"
        case recipeID = "recipe_id"
        case objective
        case dependencies
    }
}

struct ObserverInvalidationPayload: Decodable {
    let cursor: Int64
}

struct ObserverReasoningEventPayload: Decodable, Sendable {
    let type: String
    let runID: String
    let taskID: String
    let attemptID: String
    let purpose: String
    let turn: UInt16
    let delta: String?

    enum CodingKeys: String, CodingKey {
        case type
        case runID = "run_id"
        case taskID = "task_id"
        case attemptID = "attempt_id"
        case purpose
        case turn
        case delta
    }
}
