import Foundation

// MARK: - Outcome fixtures
//
// Only a sealed horizon produces an OutcomeWindow. Unsealed horizons keep a nil
// progress so the ring draws a dashed track instead of a fake 0%.
enum OutcomeFixtures {
    static func horizons(scenario: MockScenario) -> [HorizonPresentation] {
        // horizon map 闭包区分 sealed、observing 和 waiting；未到达窗口保留 nil progress。
        var generator = SeededGenerator(seed: scenario.seed &+ 701)
        let sealedSet = scenario.sealedHorizons
        let observing = scenario.dataUnavailable ? nil : scenario.observingHorizon

        return OutcomeHorizonKind.allCases.map { horizon in
            if sealedSet.contains(horizon) {
                return HorizonPresentation(
                    horizon: horizon,
                    status: .completed,
                    progress: 1,
                    evidenceCompletenessPpm: generator.int(in: 960_000...999_000),
                    isSealed: true,
                    note: "Sealed after \(horizon.windowLabel.lowercased())"
                )
            }
            if horizon == observing {
                return HorizonPresentation(
                    horizon: horizon,
                    status: .observing,
                    progress: Double(generator.int(in: 240_000...780_000)) / PpmFormatter.ppmPerUnit,
                    evidenceCompletenessPpm: generator.int(in: 420_000...780_000),
                    isSealed: false,
                    note: "Observing · \(horizon.windowLabel)"
                )
            }
            return HorizonPresentation(
                horizon: horizon,
                status: .waiting,
                progress: nil,
                evidenceCompletenessPpm: nil,
                isSealed: false,
                note: "Window not reached"
            )
        }
    }

    static func windows(scenario: MockScenario) -> [OutcomeWindowPresentation] {
        // 只有 sealed horizon 才生成可评估窗口，filter/map 不会为未封存日期造结果。
        var generator = SeededGenerator(seed: scenario.seed &+ 719)
        return OutcomeHorizonKind.allCases
            .filter { scenario.sealedHorizons.contains($0) }
            .map { horizon in
                // 每个窗口闭包消费同一个场景生成器，保持 portfolio/benchmark 成对可比较。
                let portfolioReturn = generator.int(in: -8_000...34_000)
                let benchmarkReturn = generator.int(in: -6_000...18_000)
                return OutcomeWindowPresentation(
                    horizon: horizon,
                    portfolioReturnPpm: portfolioReturn,
                    benchmarkReturnPpm: benchmarkReturn,
                    transactionCostPpm: generator.int(in: 200...900),
                    slippagePpm: generator.int(in: 60...480),
                    utilityPpm: portfolioReturn - benchmarkReturn,
                    // Calibration needs paired predictions; a window may lack them.
                    calibrationPpm: scenario.dataUnavailable ? nil : generator.int(in: 520_000...880_000),
                    evidenceCompletenessPpm: generator.int(in: 940_000...999_000),
                    riskRecallPpm: scenario.dataStale ? nil : generator.int(in: 600_000...940_000),
                    winRatePpm: generator.int(in: 420_000...720_000),
                    profitFactorPpm: generator.int(in: 1_040_000...2_400_000),
                    sharpePpm: generator.int(in: 640_000...2_100_000),
                    maxDrawdownPpm: generator.int(in: 12_000...64_000),
                    comparison: CurveFixtures.comparison(scenario: scenario, horizon: horizon)
                )
            }
    }

    static func outcome(scenario: MockScenario) -> OutcomePresentation {
        // outcome 把环、窗口、当前选择和交易日进度组合成页面的单次展示投影。
        let rings = horizons(scenario: scenario)
        let sealedDays = scenario.sealedHorizons.map(\.tradingDays).max() ?? 0
        let selected = OutcomeHorizonKind.allCases.last { scenario.sealedHorizons.contains($0) }
            ?? OutcomeHorizonKind.t1
        return OutcomePresentation(
            horizons: rings,
            windows: windows(scenario: scenario),
            selected: selected,
            observedTradingDays: sealedDays,
            totalTradingDays: OutcomeHorizonKind.t5.tradingDays,
            outcomeID: "outcome-\(scenario.code)"
        )
    }
}
