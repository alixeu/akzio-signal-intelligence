import SwiftUI

// MARK: - Risk panel
//
// Beta / Volatility / Max Drawdown / VaR / Leverage. Needles move in 350–500ms and
// only turn coral once the value is genuinely inside its risk band. A nil metric
// shows `Unavailable` and draws no needle.
struct RiskPanel: View {
    @Environment(\.appLanguage) private var language
    let risk: RiskPresentation

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        SectionCard(title: "Risk", subtitle: risk.isElevated ? "Elevated" : "Within limits") {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                if let beta = risk.betaValue {
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
                Text(L10n.text("Position sizing constrained", language: language))
                    .akzioText(.caption, color: AkzioColor.actionCoral)
                    }
                }
            }
        }
    }

    private func metric(_ label: String, ppm: Int?, threshold: Int, scale: Int = 1_000_000) -> some View {
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
                fraction: ppm.map { min(Double($0) / Double(scale), 1) },
                tone: (ppm ?? 0) >= threshold ? .coral : .gold,
                height: 5
            )
        }
    }

    private func money(_ label: String, micros: Int64?) -> some View {
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
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(label, language: language)).akzioText(.label)
            Spacer(minLength: 4)
            UnavailableValue(.unavailable, size: 11)
        }
    }
}
