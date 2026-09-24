import Foundation

// 文件导读：实现 Swift RandomNumberGenerator 所需的确定性伪随机状态，并为 UI fixture 提供抽样/随机游走帮助。
// CurveFixtures、ArchiveFixtures 等按场景编号与 salt 初始化它；相同 seed 和调用顺序才得到相同结果。
// SeededGenerator 是 struct：局部 var 持有自己的 state，mutating 方法原位推进；传入 inout 时只在调用期间暂借，
// 数组/闭包都在本进程内同步生成。它产生的“价格样”序列是合成图形，不是行情、Broker 回执或统计校准样本。
// MARK: - Deterministic randomness
//
// A linear congruential generator seeded from the scenario number. No system time,
// no `Double.random`, so every scenario rebuilds byte-identically.
public struct SeededGenerator: RandomNumberGenerator {
    // state 是唯一可变值；实例按值传递，调用方可为不同 fixture 各自保存独立序列。
    private var state: UInt64

    public init(seed: UInt64) {
        // 初始化混合 seed，避免相邻 scenario 直接产生相似的首项。
        // Splitmix-style mixing so small seeds still diverge quickly.
        var s = seed &* 0x9E37_79B9_7F4A_7C15 &+ 0x1234_5678_9ABC_DEF0
        s ^= s >> 30
        s = s &* 0xBF58_476D_1CE4_E5B9
        s ^= s >> 27
        state = s == 0 ? 0x4D59_5DF4_D0F3_3173 : s
    }

    public mutating func next() -> UInt64 {
        // next 按固定 LCG 更新 state，并返回经过混合的伪随机 UInt64。
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    /// Uniform double in 0..<1.
    public mutating func unit() -> Double {
        // 每次调用先通过 mutating next() 推进 state，再取高 53 位并缩放到 [0, 1) 的 Double 精度范围。
        Double(next() >> 11) * (1.0 / 9_007_199_254_740_992.0)
    }

    /// Uniform double in the closed range.
    public mutating func double(in range: ClosedRange<Double>) -> Double {
        // 实际公式是 lower + unit() × 宽度；因 unit() 不到 1，通常返回 [lowerBound, upperBound)，
        // 所以即使参数类型是 ClosedRange，这段实现也不会取到非退化区间的上端点。
        range.lowerBound + unit() * (range.upperBound - range.lowerBound)
    }

    public mutating func int(in range: ClosedRange<Int>) -> Int {
        // 整数范围两端都包含；next() 推进 state 后按跨度取模，再平移到下界，所有 fixture 都传有限小范围。
        let span = range.upperBound - range.lowerBound + 1
        return range.lowerBound + Int(next() % UInt64(max(span, 1)))
    }

    /// Approximately normal via the mean of four uniforms — cheap and stable.
    public mutating func gaussian(mean: Double = 0, deviation: Double = 1) -> Double {
        // 四次 unit() 共享并连续推进同一 state，再作线性变换得到中心化的平滑扰动；
        // 这是简单的 fixture 造型方法，不是经统计拟合的收益分布。
        let sum = unit() + unit() + unit() + unit()
        return mean + (sum / 2 - 1) * deviation * 2
    }

    public mutating func bool(probability: Double) -> Bool {
        // 与 probability 作一次阈值比较并推进随机状态；调用方需自行保证概率落在 0...1，函数不会裁剪输入。
        unit() < probability
    }

    public mutating func pick<T>(_ options: [T]) -> T {
        // 泛型 T 不要求额外 trait；前提是调用方传入非空数组，否则构造 0...(count-1) 会触发运行时失败。
        // 下标由当前 generator 推进得到，数组顺序不变，返回的是所选元素的值。
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
        // walk 预留 count 个槽位并按索引推进本地 Double；count 需为非负值，否则 0..<count 无法形成有效序列。
        // generator 由当前实例独占，每轮调用 gaussian 后再向 start→target 方向回归，结果只用于曲线造型而非历史价格。
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
