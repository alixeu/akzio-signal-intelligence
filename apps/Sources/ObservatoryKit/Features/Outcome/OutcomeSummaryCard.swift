import Charts
import SwiftUI

// MARK: - Outcome summary
//
// Portfolio vs benchmark for the selected horizon, plus the four headline ratios.
// The card title is a shared element, so switching horizons keeps it in place while
// the numbers and the chart change underneath.
struct OutcomeSummaryCard: View {
    // window 可能尚未 sealed；horizon 只标识当前选中的 T+1/T+3/T+5，namespace 只服务共享元素。
    let window: OutcomeWindowPresentation?
    let horizon: OutcomeHorizonKind
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // ViewBuilder 的 if let 是 outcome 完成边界：有窗口才渲染 headline/chart/ratios，否则显示 waiting。
        SectionCard(
            title: "Outcome Summary",
            subtitle: "\(horizon.displayName) · \(L10n.text(horizon.windowLabel, language: language))"
        ) {
            if let window {
                VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                    Text(L10n.text("Frozen decision exposure; subsequent account trades are excluded. Actual account NAV is unavailable.", language: language)).akzioText(.caption)
                    headline(window)
                    chart(window)
                    ratios(window)
                }
                .animation(policy.resolve(.smooth(duration: 0.48)), value: horizon)
            } else {
                StatusExplanation(.waiting, detail: "Sealing has not been confirmed for this horizon")
                    .frame(height: 200)
            }
        } accessory: {
            Text(horizon.displayName)
                .akzioMono(12, color: AkzioColor.primaryGold)
                .sharedElement(.outcomeSummary, in: namespace)
        }
    }

    private func headline(_ window: OutcomeWindowPresentation) -> some View {
        // 三个 headline 都从同一个 sealed window 读取，避免把账户 NAV 或后续交易混入冻结 exposure。
        HStack(spacing: AkzioLayout.s5) {
            value("Frozen Exposure", PpmFormatter.percent(ppm: window.portfolioReturnPpm), tone: window.portfolioReturnPpm >= 0 ? .gold : .coral)
                .sharedElement(.portfolioReturn, in: namespace)
            value("Benchmark", PpmFormatter.percent(ppm: window.benchmarkReturnPpm), tone: .neutral)
            value("Alpha", PpmFormatter.percent(ppm: window.alphaPpm), tone: window.alphaPpm >= 0 ? .gold : .coral)
        }
    }

    private func value(_ label: String, _ text: String, tone: AkzioTone) -> some View {
        // numeric 动效接收已经格式化的 text；tone 只决定正负/neutral 的视觉颜色。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(text)
                .akzioMetric(22, color: tone == .neutral ? AkzioColor.primaryText : tone.color)
                .akzioNumeric(text, policy: policy)
        }
    }

    private func chart(_ window: OutcomeWindowPresentation) -> some View {
        // Chart 的 ForEach 将 comparison 投影成 portfolio 与可选 benchmark 两条线；benchmark 缺失时不补零。
        Chart {
            ForEach(window.comparison) { point in
                LineMark(
                    x: .value("Step", point.index),
                    y: .value("Frozen Exposure", point.portfolio),
                    series: .value("Series", "Portfolio")
                )
                .foregroundStyle(AkzioColor.primaryGold)
                .lineStyle(StrokeStyle(lineWidth: 1.6, lineCap: .round))
                .interpolationMethod(.monotone)

                    if let benchmark = point.benchmark {
                        // benchmark 是逐点 Optional，只有 provider/Outcome 投影实际提供时才绘制虚线。
                        LineMark(
                            x: .value("Step", point.index),
                            y: .value("Benchmark", benchmark),
                            series: .value("Series", "Benchmark")
                        )
                        .foregroundStyle(AkzioColor.secondaryText.opacity(0.6))
                        .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 4]))
                        .interpolationMethod(.monotone)
                    }
            }
        }
        .chartXAxis(.hidden)
        .chartYAxis {
            AxisMarks(position: .trailing, values: .automatic(desiredCount: 4)) { _ in
                AxisGridLine().foregroundStyle(.white.opacity(0.04))
            }
        }
        .chartLegend(.hidden)
        .frame(height: 168)
        .animation(ChartAnimation.rangeMorph(policy), value: horizon)
    }

    private func ratios(_ window: OutcomeWindowPresentation) -> some View {
        // ratios 直接显示 Outcome 计算出的四个指标，UI 不重新计算收益或风险。
        HStack(spacing: AkzioLayout.s4) {
            ratio("Win Rate", PpmFormatter.share(ppm: window.winRatePpm))
            ratio("Profit Factor", PpmFormatter.multiple(ppm: window.profitFactorPpm))
            ratio("Sharpe", PpmFormatter.ratio(ppm: window.sharpePpm))
            ratio("Max Drawdown", PpmFormatter.share(ppm: window.maxDrawdownPpm, fractionDigits: 2))
        }
    }

    private func ratio(_ label: String, _ value: String) -> some View {
        // ratio helper 统一布局和 numeric transition，value 的含义由调用方对应字段保证。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value)
                .akzioMono(12, color: AkzioColor.primaryText)
                .akzioNumeric(value, policy: policy)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
