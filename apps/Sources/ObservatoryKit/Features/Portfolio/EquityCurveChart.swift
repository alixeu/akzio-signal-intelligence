import Charts
import SwiftUI

// MARK: - Equity curve
//
// Portfolio line plus a dashed benchmark. Hovering snaps to the nearest sample and
// shows a glass tooltip; the latest point diffuses once when the series changes and
// then stops.
struct EquityCurveChart: View {
    // curve、range 和 benchmarkLabel 都来自当前 PortfolioPresentation；该 View 只做图表/hover 投影。
    let curve: [EquityPoint]
    let range: EquityRange
    let benchmarkLabel: String
    let isGain: Bool
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    // hovered 与 bloomTick 是本地交互/动画状态，不会回写曲线或账户数据。
    @State private var hovered: EquityPoint?
    @State private var bloomTick = 0

    private var tone: AkzioTone { isGain ? .gold : .coral }

    var body: some View {
        // Chart 的 result builder 组合 portfolio、可选 benchmark、latest marker 和 hover RuleMark。
        Chart {
            ForEach(curve) { point in
                LineMark(
                    x: .value("Time", point.chartX),
                    y: .value("Equity", point.portfolio),
                    series: .value("Series", "Portfolio")
                )
                .foregroundStyle(tone.color)
                .lineStyle(StrokeStyle(lineWidth: 1.8, lineCap: .round))
                .interpolationMethod(.monotone)

                AreaMark(
                    x: .value("Time", point.chartX),
                    y: .value("Equity", point.portfolio)
                )
                .foregroundStyle(
                    LinearGradient(
                        colors: [tone.color.opacity(0.20), tone.color.opacity(0.01)],
                        startPoint: .top,
                        endPoint: .bottom
                    )
                )
                .interpolationMethod(.monotone)

                    if let benchmark = point.benchmark {
                        // benchmark 是逐点 Optional；缺失点跳过 LineMark，避免用 0 伪造比较路径。
                        LineMark(
                        x: .value("Time", point.chartX),
                            y: .value("Benchmark", benchmark),
                            series: .value("Series", benchmarkLabel)
                        )
                        .foregroundStyle(AkzioColor.secondaryText.opacity(0.6))
                        .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 4]))
                        .interpolationMethod(.monotone)
                    }
            }

            if let latest = curve.last {
                // latest marker 只在曲线非空时存在，并通过 shared element 与 Outcome 视觉连接。
                PointMark(
                    x: .value("Time", latest.chartX),
                    y: .value("Equity", latest.portfolio)
                )
                .foregroundStyle(tone.color)
                .symbolSize(46)
                .annotation(position: .trailing, spacing: 4) {
                    Text(PpmFormatter.price(micros: Int64(latest.portfolio * PpmFormatter.ppmPerUnit)))
                        .akzioMono(10, color: AkzioColor.primaryText)
                        // The point the Outcome ring grows out of, and shrinks back to.
                        .sharedElement(.equityLatestPoint, in: namespace)
                }
            }

            if let hovered {
                // hover 状态只增加当前样本的竖向 RuleMark，不改变原始 curve。
                RuleMark(x: .value("Time", hovered.chartX))
                    .foregroundStyle(AkzioColor.gold(0.35))
                    .lineStyle(StrokeStyle(lineWidth: 1, dash: [2, 3]))
            }
        }
        .chartXAxis {
            AxisMarks(values: axisPoints.map(\.chartX)) { value in
                AxisGridLine().foregroundStyle(.white.opacity(0.04))
                AxisValueLabel {
                    if let x = value.as(Double.self),
                       let point = curve.min(by: { abs($0.chartX - x) < abs($1.chartX - x) }) {
                        Text(point.axisLabel(for: range, locale: language.locale))
                            .akzioMono(9, color: AkzioColor.mutedText)
                    }
                }
            }
        }
        .chartYAxis {
            AxisMarks(position: .trailing, values: .automatic(desiredCount: 5)) { value in
                AxisGridLine().foregroundStyle(.white.opacity(0.04))
                AxisValueLabel {
                    if let amount = value.as(Double.self) {
                        Text(PpmFormatter.count(Int(amount / 1000)) + "k")
                            .akzioMono(9, color: AkzioColor.mutedText)
                    }
                }
            }
        }
        .chartOverlay { proxy in
            // overlay 闭包捕获 ChartProxy/GeometryProxy；连续 hover 时按像素位置解析最近样本，结束时清空 State。
            GeometryReader { geometry in
                Rectangle()
                    .fill(.clear)
                    .contentShape(Rectangle())
                    .onContinuousHover { phase in
                        switch phase {
                        case .active(let location):
                            hovered = sample(at: location, proxy: proxy, geometry: geometry)
                        case .ended:
                            hovered = nil
                        }
                    }
            }
        }
        .chartLegend(.hidden)
        .frame(minHeight: 260)
        .animation(ChartAnimation.rangeMorph(policy), value: range)
        .animation(ChartAnimation.tailAppend(policy), value: curve.count)
        .completionBloom(trigger: bloomTick, tone: tone)
        .onChange(of: curve.last?.portfolio) { _, _ in bloomTick += 1 }
        .overlay(alignment: .topLeading) { tooltip }
        .sharedElement(.sparkline, in: namespace)
        .accessibilityLabel("\(L10n.text("Equity curve", language: language)) \(range.rawValue)")
    }

    @ViewBuilder
    private var tooltip: some View {
        // tooltip 是 hovered 的可选 ViewBuilder 分支；它只展示当前样本和可选 benchmark。
        if let hovered {
            VStack(alignment: .leading, spacing: 3) {
                Text(hovered.axisLabel(for: range, locale: language.locale)).akzioText(.caption)
                Text(PpmFormatter.currency(micros: Int64(hovered.portfolio * PpmFormatter.ppmPerUnit)))
                    .akzioMono(11, color: AkzioColor.primaryText)
                if let benchmark = hovered.benchmark {
                    Text("\(benchmarkLabel) \(PpmFormatter.price(micros: Int64(benchmark * PpmFormatter.ppmPerUnit)))")
                        .akzioMono(10, color: AkzioColor.mutedText)
                }
            }
            .padding(AkzioLayout.s2)
            .akzioGlass(.elevated, radius: AkzioLayout.chipRadius)
            .padding(AkzioLayout.s2)
            .transition(.opacity)
        }
    }

    private func sample(at location: CGPoint, proxy: ChartProxy, geometry: GeometryProxy) -> EquityPoint? {
        // plotFrame 或 value(atX:) 任一不可用都返回 nil；成功后在同一 curve 中选择 chartX 最近点。
        guard let frame = proxy.plotFrame else { return nil }
        let origin = geometry[frame].origin
        guard let x: Double = proxy.value(atX: location.x - origin.x) else { return nil }
        return curve.min { abs($0.chartX - x) < abs($1.chartX - x) }
    }

    private var axisPoints: [EquityPoint] {
        // 短曲线保留全部点，长曲线等距挑六个索引作为轴标签，避免重采样业务数据。
        guard curve.count > 6 else { return curve }
        return (0..<6).map { tick in
            curve[Int((Double(tick) * Double(curve.count - 1) / 5).rounded())]
        }
    }
}
