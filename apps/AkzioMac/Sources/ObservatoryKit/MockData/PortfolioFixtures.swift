import Foundation

// MARK: - Portfolio, order and execution fixtures

enum PortfolioFixtures {
    private static let venue = "ALPACA-PAPER"

    static func orders(scenario: MockScenario) -> [OrderPresentation] {
        guard scenario.hasOrders, !scenario.isDecisionBlocked else { return [] }
        var generator = SeededGenerator(seed: scenario.seed &+ 601)
        let plan: [(TradableAsset, OrderSide)] = [
            (.tqqq, .buy), (.qqq, .sell), (.soxx, .buy), (.soxl, .buy),
        ]
        return plan.enumerated().map { index, entry in
            let quantity = Int64(generator.int(in: 40...260)) * 1_000_000
            let limit = Int64(generator.int(in: 42_000_000...98_000_000))
            return OrderPresentation(
                id: "ord-\(scenario.code)-\(index + 1)",
                timeLabel: String(format: "09:%02d:%02d", 31 + index, generator.int(in: 10...58)),
                asset: entry.0,
                side: entry.1,
                type: "Limit",
                quantityMicros: quantity,
                limitPriceMicros: limit,
                state: state(scenario: scenario, index: index)
            )
        }
    }

    private static func state(scenario: MockScenario, index: Int) -> OrderReceiptState {
        if scenario.allFilled { return .filled }
        if scenario.hasPartialFill {
            switch index {
            case 0: return .partiallyFilled
            case 1: return .accepted
            case 2: return .filled
            default: return .canceled
            }
        }
        return index < 2 ? .filled : .accepted
    }

    static func fills(scenario: MockScenario) -> [FillPresentation] {
        var generator = SeededGenerator(seed: scenario.seed &+ 619)
        return orders(scenario: scenario)
            .filter { $0.state == .filled || $0.state == .partiallyFilled }
            .enumerated()
            .map { index, order in
                let ratio = order.state == .partiallyFilled ? 0.42 : 1.0
                return FillPresentation(
                    id: "fill-\(scenario.code)-\(index + 1)",
                    timeLabel: order.timeLabel,
                    asset: order.asset,
                    side: order.side,
                    quantityMicros: Int64(Double(order.quantityMicros) * ratio),
                    priceMicros: (order.limitPriceMicros ?? 0) - Int64(generator.int(in: 0...240_000)),
                    venue: venue
                )
            }
    }

    static func flow(scenario: MockScenario) -> [AllocationFlowStage] {
        let reached: Int
        if scenario.isDecisionBlocked {
            reached = 3
        } else if !scenario.hasOrders {
            reached = 4
        } else if scenario.allFilled {
            reached = 6
        } else {
            reached = 5
        }
        let stages: [(String, String)] = [
            ("Target Weights", "target"),
            ("Delta", "arrow.left.arrow.right"),
            ("Order Intent", "doc.badge.plus"),
            ("Gate", "shield.lefthalf.filled"),
            ("Broker", "building.columns"),
            ("Reconcile", "arrow.triangle.2.circlepath"),
        ]
        return stages.enumerated().map { index, stage in
            AllocationFlowStage(title: stage.0, symbol: stage.1, isActive: index < reached)
        }
    }

    static func risk(scenario: MockScenario) -> RiskPresentation {
        guard !scenario.dataUnavailable else {
            return RiskPresentation(
                betaPpm: nil,
                volatilityPpm: nil,
                maxDrawdownPpm: nil,
                varMicros: nil,
                leveragePpm: nil,
                isElevated: false
            )
        }
        var generator = SeededGenerator(seed: scenario.seed &+ 641)
        return RiskPresentation(
            betaPpm: generator.int(in: 1_180_000...1_640_000),
            volatilityPpm: generator.int(in: 180_000...320_000),
            maxDrawdownPpm: generator.int(in: 28_000...52_000),
            varMicros: Int64(generator.int(in: 11_000...19_000)) * 1_000_000,
            leveragePpm: generator.int(in: 1_020_000...1_280_000),
            isElevated: scenario.dataStale || scenario.hasPartialFill
        )
    }

    static func reconciliation(scenario: MockScenario) -> ReconciliationState {
        if scenario.isDecisionBlocked || !scenario.hasOrders { return .pending }
        if scenario.hasPartialFill { return .partial }
        return scenario.allFilled ? .complete : .pending
    }

    static func portfolio(scenario: MockScenario, range: EquityRange = .oneDay) -> PortfolioPresentation {
        var generator = SeededGenerator(seed: scenario.seed &+ 659)
        let curve = CurveFixtures.equityCurve(scenario: scenario, range: range)
        let equity = curve.last?.portfolio ?? CurveFixtures.baseEquity
        let opening = curve.first?.portfolio ?? CurveFixtures.baseEquity
        let todayPnl = equity - opening
        let todayPpm = Int(todayPnl / opening * PpmFormatter.ppmPerUnit)
        let unavailable = scenario.dataUnavailable
        let unrealizedPpm = unavailable ? nil : generator.int(in: -6_000...21_000)
        let realizedPpm = unavailable ? nil : generator.int(in: -2_400...9_600)

        func money(_ ppm: Int?) -> Int64? {
            ppm.map { Int64(Double($0) / PpmFormatter.ppmPerUnit * equity * PpmFormatter.ppmPerUnit) }
        }

        return PortfolioPresentation(
            equityMicros: Int64(equity * PpmFormatter.ppmPerUnit),
            todayPnlMicros: Int64(todayPnl * PpmFormatter.ppmPerUnit),
            todayPnlPpm: todayPpm,
            unrealizedPnlMicros: money(unrealizedPpm),
            realizedPnlMicros: money(realizedPpm),
            unrealizedPnlPpm: unrealizedPpm,
            realizedPnlPpm: realizedPpm,
            curve: curve,
            range: range,
            benchmarkLabel: TradableAsset.qqq.rawValue,
            allocations: CurveFixtures.allocations(scenario: scenario),
            positions: CurveFixtures.positions(scenario: scenario),
            orders: orders(scenario: scenario),
            fills: fills(scenario: scenario),
            flow: flow(scenario: scenario),
            risk: risk(scenario: scenario),
            verdict: scenario.hasOrders && !scenario.isDecisionBlocked ? .accepted : .noOrder,
            reconciliation: reconciliation(scenario: scenario)
        )
    }
}
