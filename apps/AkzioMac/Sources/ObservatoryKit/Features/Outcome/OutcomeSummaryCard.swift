import Charts
import SwiftUI

// MARK: - Outcome summary
//
// Portfolio vs benchmark for the selected horizon, plus the four headline ratios.
// The card title is a shared element, so switching horizons keeps it in place while
// the numbers and the chart change underneath.
struct OutcomeSummaryCard: View {
    let window: OutcomeWindowPresentation?
    let horizon: OutcomeHorizonKind
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        SectionCard(
            title: "Outcome Summary",
            subtitle: "\(horizon.displayName) · \(L10n.text(horizon.windowLabel, language: language))"
        ) {
            if let window {
                VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                    headline(window)
                    chart(window)
                    ratios(window)
                }
                .animation(policy.resolve(.smooth(duration: 0.48)), value: horizon)
            } else {
                StatusExplanation(.waiting, detail: "This horizon has not sealed yet")
                    .frame(height: 200)
            }
        } accessory: {
            Text(horizon.displayName)
                .akzioMono(12, color: AkzioColor.primaryGold)
                .sharedElement(.outcomeSummary, in: namespace)
        }
    }

    private func headline(_ window: OutcomeWindowPresentation) -> some View {
        HStack(spacing: AkzioLayout.s5) {
            value("Portfolio", PpmFormatter.percent(ppm: window.portfolioReturnPpm), tone: window.portfolioReturnPpm >= 0 ? .gold : .coral)
                .sharedElement(.portfolioReturn, in: namespace)
            value("Benchmark", PpmFormatter.percent(ppm: window.benchmarkReturnPpm), tone: .neutral)
            value("Alpha", PpmFormatter.percent(ppm: window.alphaPpm), tone: window.alphaPpm >= 0 ? .gold : .coral)
        }
    }

    private func value(_ label: String, _ text: String, tone: AkzioTone) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(text)
                .akzioMetric(22, color: tone == .neutral ? AkzioColor.primaryText : tone.color)
                .akzioNumeric(text, policy: policy)
        }
    }

    private func chart(_ window: OutcomeWindowPresentation) -> some View {
        Chart {
            ForEach(window.comparison) { point in
                LineMark(
                    x: .value("Step", point.index),
                    y: .value("Portfolio", point.portfolio),
                    series: .value("Series", "Portfolio")
                )
                .foregroundStyle(AkzioColor.primaryGold)
                .lineStyle(StrokeStyle(lineWidth: 1.6, lineCap: .round))
                .interpolationMethod(.monotone)

                    if let benchmark = point.benchmark {
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
        HStack(spacing: AkzioLayout.s4) {
            ratio("Win Rate", PpmFormatter.share(ppm: window.winRatePpm))
            ratio("Profit Factor", PpmFormatter.multiple(ppm: window.profitFactorPpm))
            ratio("Sharpe", PpmFormatter.ratio(ppm: window.sharpePpm))
            ratio("Max Drawdown", PpmFormatter.share(ppm: window.maxDrawdownPpm, fractionDigits: 2))
        }
    }

    private func ratio(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value)
                .akzioMono(12, color: AkzioColor.primaryText)
                .akzioNumeric(value, policy: policy)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
