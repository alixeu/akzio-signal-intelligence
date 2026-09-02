import Foundation

// MARK: - Curve fixtures
//
// All series are generated from the scenario seed, so the equity curve in Overview,
// the full curve in Portfolio and the comparison chart in Outcome are the same
// numbers — which is what lets the shared-element handoff land pixel-perfectly.
public enum CurveFixtures {
    public static let baseEquity: Double = 1_028_645.72

    /// Intraday equity plus benchmark, one point per five minutes of the session.
    public static func equityCurve(
        scenario: MockScenario,
        range: EquityRange = .oneDay
    ) -> [EquityPoint] {
        var generator = SeededGenerator(seed: scenario.seed &+ 11)
        let count = range.pointCount
        let drift = scenario.dataStale ? 0.004 : 0.0148
        let portfolio = generator.walk(
            count: count,
            start: baseEquity * 0.986,
            drift: drift,
            volatility: 0.0016
        )
        var benchmarkGenerator = SeededGenerator(seed: scenario.seed &+ 29)
        let benchmark = benchmarkGenerator.walk(
            count: count,
            start: baseEquity * 0.990,
            drift: drift * 0.42,
            volatility: 0.0011
        )
        let stride = max(1, 390 / max(count - 1, 1))
        return (0..<count).map { index in
            EquityPoint(
                index: index,
                minutesFromOpen: index * stride,
                portfolio: portfolio[index],
                benchmark: benchmark[index]
            )
        }
    }

    /// Compact series for KPI cards and position cards.
    public static func spark(scenario: MockScenario, salt: UInt64, trend: Double) -> [Double] {
        var generator = SeededGenerator(seed: scenario.seed &+ salt)
        return generator.walk(count: 34, start: 100, drift: trend, volatility: 0.006)
    }

    /// Outcome comparison chart: portfolio vs benchmark across the observed horizon.
    public static func comparison(scenario: MockScenario, horizon: OutcomeHorizonKind) -> [EquityPoint] {
        var generator = SeededGenerator(seed: scenario.seed &+ 71 &+ UInt64(horizon.tradingDays))
        let count = 12 * horizon.tradingDays
        let portfolio = generator.walk(count: count, start: 100, drift: 0.0146, volatility: 0.0042)
        var benchmarkGenerator = SeededGenerator(seed: scenario.seed &+ 97 &+ UInt64(horizon.tradingDays))
        let benchmark = benchmarkGenerator.walk(count: count, start: 100, drift: 0.0058, volatility: 0.0031)
        return (0..<count).map { index in
            EquityPoint(
                index: index,
                minutesFromOpen: index * 30,
                portfolio: portfolio[index],
                benchmark: benchmark[index]
            )
        }
    }

    /// Retrospective card thumbnails; sign of `trend` decides the tone.
    public static func retrospectiveSpark(scenario: MockScenario, index: Int, trend: Double) -> [Double] {
        var generator = SeededGenerator(seed: scenario.seed &+ 131 &+ UInt64(index))
        return generator.walk(count: 28, start: 100, drift: trend, volatility: 0.010)
    }

    // MARK: Allocation

    /// Actual vs target weights. Targets are the policy; actuals drift off them.
    public static func allocations(scenario: MockScenario) -> [AllocationRow] {
        let targets: [(String, Int)] = [
            (TradableAsset.tqqq.rawValue, 300_000),
            (TradableAsset.qqq.rawValue, 250_000),
            (TradableAsset.soxx.rawValue, 225_000),
            (TradableAsset.soxl.rawValue, 150_000),
            (TradableAsset.cashLabel, 75_000),
        ]
        var generator = SeededGenerator(seed: scenario.seed &+ 53)
        var rows: [AllocationRow] = []
        var drifted = 0
        for (label, target) in targets.dropLast() {
            let delta = generator.int(in: -13_000...11_000)
            drifted += delta
            rows.append(AllocationRow(label: label, actualPpm: target + delta, targetPpm: target))
        }
        let cash = targets[targets.count - 1]
        rows.append(AllocationRow(label: cash.0, actualPpm: cash.1 - drifted, targetPpm: cash.1))
        return rows
    }

    public static func positions(scenario: MockScenario) -> [PositionPresentation] {
        let allocations = allocations(scenario: scenario)
        var generator = SeededGenerator(seed: scenario.seed &+ 67)
        return TradableAsset.allCases.enumerated().map { index, asset in
            let row = allocations.first { $0.label == asset.rawValue }
            let weight = row?.actualPpm ?? 0
            let marketValue = Int64(Double(weight) / PpmFormatter.ppmPerUnit * baseEquity * 1_000_000)
            let pnlPpm = generator.int(in: -9_000...24_000)
            let pnl = Int64(Double(pnlPpm) / PpmFormatter.ppmPerUnit * Double(marketValue))
            return PositionPresentation(
                asset: asset,
                weightPpm: weight,
                marketValueMicros: marketValue,
                pnlMicros: pnl,
                pnlPpm: pnlPpm,
                spark: spark(scenario: scenario, salt: 200 &+ UInt64(index), trend: Double(pnlPpm) / 400_000),
                actualPpm: weight,
                targetPpm: row?.targetPpm ?? 0
            )
        }
    }
}
