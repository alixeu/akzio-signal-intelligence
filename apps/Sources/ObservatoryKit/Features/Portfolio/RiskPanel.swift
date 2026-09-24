import SwiftUI

// MARK: - Risk panel
//
// Beta / Volatility / Max Drawdown / VaR / Leverage. Needles move in 350–500ms and
// only turn coral once the value is genuinely inside its risk band. A nil metric
// shows `Unavailable` and draws no needle.
struct RiskPanel: View {
    // risk 是 Rust 计算后的风险投影；View 只展示阈值状态，不进行风险裁剪或仓位决策。
    @Environment(\.appLanguage) private var language
    let risk: RiskPresentation

    // policy 只控制数字过渡，language 只影响标签和说明文字。
    @Environment(\.motionPolicy) private var policy

    var body: some View {
        // ViewBuilder 对 beta 单独处理 Optional，其余指标通过 metric/money 保留各自缺失边界。
        SectionCard(title: "Risk", subtitle: risk.isElevated ? "Elevated" : "Within limits") {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                if let beta = risk.betaValue {
                    // beta 有值时才绘制 RiskGauge；nil 进入 unavailable，不把未知风险当成 0。
                    RiskGauge(
                        value: beta,
                        bounds: 0...2.5,
                        riskThreshold: 1.8,
                        label: "Beta",
                        caption: "vs \(TradableAsset.qqq.rawValue)"
                    )
                } else {
                    unavailable("Beta")
                }
                metric("Volatility", ppm: risk.volatilityPpm, threshold: 280_000)
                metric("Max Drawdown", ppm: risk.maxDrawdownPpm, threshold: 45_000)
                money("VaR (95%)", micros: risk.varMicros)
                metric("Leverage", ppm: risk.leveragePpm, threshold: 1_200_000, scale: 2_000_000)
                HStack(spacing: AkzioLayout.s2) {
                    StatusBadge(risk.status, size: .compact)
                    if risk.isElevated {
                    // isElevated 是上游状态，不由 UI 根据单个指标重新判断。
                Text(L10n.text("Position sizing constrained", language: language))
                    .akzioText(.caption, color: AkzioColor.actionCoral)
                    }
                }
            }
        }
    }

    private func metric(_ label: String, ppm: Int?, threshold: Int, scale: Int = 1_000_000) -> some View {
        // metric 保留 ppm Optional；有值时同时显示格式化数字、阈值颜色和比例条，nil 只显示 Unavailable。
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: AkzioLayout.s2) {
                Text(L10n.text(label, language: language)).akzioText(.label)
                Spacer(minLength: 4)
                if let ppm {
                    Text(PpmFormatter.share(ppm: ppm, fractionDigits: 2))
                        .akzioMono(11, color: ppm >= threshold ? AkzioColor.actionCoral : AkzioColor.primaryText)
                        .akzioNumeric(ppm, policy: policy)
                } else {
                    UnavailableValue(.unavailable, size: 11)
                }
            }
            RatioBar(
                // map 只在 ppm 存在时生成 fraction，scale 负责把不同量纲映射到 0...1 的视觉范围。
                fraction: ppm.map { min(Double($0) / Double(scale), 1) },
                tone: (ppm ?? 0) >= threshold ? .coral : .gold,
                height: 5
            )
        }
    }

    private func money(_ label: String, micros: Int64?) -> some View {
        // VaR 金额是可选的；缺失时不输出货币零值，避免把未观测解释为无风险。
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(label, language: language)).akzioText(.label)
            Spacer(minLength: 4)
            if micros == nil {
                UnavailableValue(.unavailable, size: 11)
            } else {
                Text(PpmFormatter.currency(micros: micros))
                    .akzioMono(11, color: AkzioColor.primaryText)
            }
        }
    }

    private func unavailable(_ label: String) -> some View {
        // unavailable 统一渲染没有可用风险字段的 label/value 行，不引入默认数值。
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(label, language: language)).akzioText(.label)
            Spacer(minLength: 4)
            UnavailableValue(.unavailable, size: 11)
        }
    }
}
