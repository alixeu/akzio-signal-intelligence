import Foundation

// MARK: - Deterministic randomness
//
// A linear congruential generator seeded from the scenario number. No system time,
// no `Double.random`, so every scenario rebuilds byte-identically.
public struct SeededGenerator: RandomNumberGenerator {
    private var state: UInt64

    public init(seed: UInt64) {
        // Splitmix-style mixing so small seeds still diverge quickly.
        var s = seed &* 0x9E37_79B9_7F4A_7C15 &+ 0x1234_5678_9ABC_DEF0
        s ^= s >> 30
        s = s &* 0xBF58_476D_1CE4_E5B9
        s ^= s >> 27
        state = s == 0 ? 0x4D59_5DF4_D0F3_3173 : s
    }

    public mutating func next() -> UInt64 {
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    /// Uniform double in 0..<1.
    public mutating func unit() -> Double {
        Double(next() >> 11) * (1.0 / 9_007_199_254_740_992.0)
    }

    /// Uniform double in the closed range.
    public mutating func double(in range: ClosedRange<Double>) -> Double {
        range.lowerBound + unit() * (range.upperBound - range.lowerBound)
    }

    public mutating func int(in range: ClosedRange<Int>) -> Int {
        let span = range.upperBound - range.lowerBound + 1
        return range.lowerBound + Int(next() % UInt64(max(span, 1)))
    }

    /// Approximately normal via the mean of four uniforms — cheap and stable.
    public mutating func gaussian(mean: Double = 0, deviation: Double = 1) -> Double {
        let sum = unit() + unit() + unit() + unit()
        return mean + (sum / 2 - 1) * deviation * 2
    }

    public mutating func bool(probability: Double) -> Bool {
        unit() < probability
    }

    public mutating func pick<T>(_ options: [T]) -> T {
        options[int(in: 0...(options.count - 1))]
    }
}

// MARK: - Random walk helper

extension SeededGenerator {
    /// Deterministic price-like series: drifting random walk with mild mean reversion.
    public mutating func walk(
        count: Int,
        start: Double,
        drift: Double,
        volatility: Double,
        meanReversion: Double = 0.04
    ) -> [Double] {
        var values: [Double] = []
        values.reserveCapacity(count)
        var value = start
        for index in 0..<count {
            let progress = Double(index) / Double(max(count - 1, 1))
            let target = start * (1 + drift * progress)
            let shock = gaussian(deviation: volatility) * start
            value += shock + (target - value) * meanReversion
            values.append(value)
        }
        return values
    }
}
