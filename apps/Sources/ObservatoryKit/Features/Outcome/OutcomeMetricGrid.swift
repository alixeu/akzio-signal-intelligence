import SwiftUI

// MARK: - Outcome metrics
//
// Digits roll into place and the comparison arrow updates with them. `calibration`
// and `riskRecall` are optional in the domain, so a missing one reads `Unavailable`
// and hides its bar entirely.
struct OutcomeMetricGrid: View {
    // window 是可选的 sealed Outcome 投影；nil 时只显示 waiting，不把缺失指标填成零。
    let window: OutcomeWindowPresentation?

    // policy 控制数字过渡，language 只影响 label；指标值和排序由 Rust 投影定义。
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // ViewBuilder 的 if let 决定是否生成完整四列网格；可选 effect/risk 字段在网格内部继续保持边界。
        SectionCard(title: "Window Metrics", subtitle: window?.horizon.windowLabel) {
            if let window {
                LazyVGrid(
                    columns: Array(repeating: GridItem(.flexible(), alignment: .leading), count: 4),
                    alignment: .leading,
                    spacing: AkzioLayout.s3
                ) {
                    metric("Transaction Cost", PpmFormatter.share(ppm: window.transactionCostPpm, fractionDigits: 3), index: 0)
                    metric("Slippage", PpmFormatter.share(ppm: window.slippagePpm, fractionDigits: 3), index: 1)
                    metric(
                        "Frozen Net Return",
                        PpmFormatter.percent(ppm: window.netReturnPpm),
                        index: 2,
                        tone: window.netReturnPpm >= 0 ? .gold : .coral,
                        arrow: window.netReturnPpm >= 0 ? "arrow.up.right" : "arrow.down.right"
                    )
                    metric(
                        "Utility",
                        PpmFormatter.percent(ppm: window.utilityPpm),
                        index: 3,
                        tone: window.utilityPpm >= 0 ? .gold : .coral,
                        arrow: window.utilityPpm >= 0 ? "arrow.up.right" : "arrow.down.right"
                    )
                    if let effect = window.implementationEffectPpm {
                        metric("Implementation Effect", PpmFormatter.percent(ppm: effect), index: 8)
                    }
                    if let effect = window.initialValuationEffectPpm {
                        metric("Initial Valuation Effect", PpmFormatter.percent(ppm: effect), index: 9)
                    }
                    optionalMetric("Calibration", ppm: window.calibrationPpm, index: 4)
                    metric("Market Window Coverage", PpmFormatter.share(ppm: window.evidenceCompletenessPpm), index: 5, bar: PpmFormatter.fraction(ppm: window.evidenceCompletenessPpm))
                    optionalMetric("Risk Recall", ppm: window.riskRecallPpm, index: 6)
                    metric("Max Drawdown", PpmFormatter.share(ppm: window.maxDrawdownPpm, fractionDigits: 2), index: 7)
                }
            } else {
                StatusExplanation(.waiting, detail: "Metrics appear once the window seals")
            }
        }
    }

    private func metric(
        _ label: String,
        _ value: String,
        index: Int,
        tone: AkzioTone = .neutral,
        arrow: String? = nil,
        bar: Double? = nil
    ) -> some View {
        // metric 将已格式化的 value、可选箭头和可选比例条组合；nil bar 不渲染任何虚构的进度。
        VStack(alignment: .leading, spacing: 3) {
            Text(L10n.text(label, language: language)).akzioText(.caption).lineLimit(1)
            HStack(spacing: 4) {
                Text(value)
                    .akzioMono(12, color: tone == .neutral ? AkzioColor.primaryText : tone.color)
                    .akzioNumeric(value, policy: policy)
                if let arrow {
                    Image(systemName: arrow)
                        .font(.system(size: 8, weight: .semibold))
                        .foregroundStyle(tone.color)
                }
            }
            if let bar {
                RatioBar(fraction: bar, height: 4)
            }
        }
        .staggeredReveal(index: index)
    }

    /// Optional in Rust, optional here: no bar and no invented zero.
    private func optionalMetric(_ label: String, ppm: Int?, index: Int) -> some View {
        // optionalMetric 对应 Rust 的 Optional：有值时显示数值和 bar，nil 时只显示 Unavailable。
        VStack(alignment: .leading, spacing: 3) {
            Text(L10n.text(label, language: language)).akzioText(.caption).lineLimit(1)
            if let ppm {
                Text(PpmFormatter.share(ppm: ppm))
                    .akzioMono(12, color: AkzioColor.primaryText)
                    .akzioNumeric(ppm, policy: policy)
                RatioBar(fraction: PpmFormatter.fraction(ppm: ppm), height: 4)
            } else {
                UnavailableValue(.unavailable, size: 12)
            }
        }
        .staggeredReveal(index: index)
    }
}
