import SwiftUI

// MARK: - Position card

struct PositionCardView: View {
    // position 是单资产的 live/mock 统一展示投影；isSelected、namespace 和 onSelect 由父级协调选择与过渡。
    let position: PositionPresentation
    let isSelected: Bool
    let namespace: Namespace.ID?
    let onSelect: () -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // Button action 只调用父级 onSelect；卡片内的金额、权重、P&L 和 spark 都是只读数据流。
        Button(action: onSelect) {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                HStack(spacing: AkzioLayout.s2) {
                    VStack(alignment: .leading, spacing: 1) {
                        Text(position.asset.rawValue).akzioMono(12, color: AkzioColor.primaryText)
                        Text(position.asset.longName)
                            .akzioText(.caption)
                            .lineLimit(1)
                    }
                    Spacer(minLength: AkzioLayout.s2)
                    Text(PpmFormatter.share(ppm: position.weightPpm, fractionDigits: 1))
                        .akzioMono(11, color: AkzioColor.secondaryText)
                }
                Text(PpmFormatter.currency(micros: position.marketValueMicros))
                    .akzioMetric(18)
                    .akzioCountUp(Double(position.marketValueMicros) / PpmFormatter.ppmPerUnit, policy: policy)
                HStack(spacing: 6) {
                    Text(PpmFormatter.currency(micros: position.pnlMicros, signed: true))
                        .akzioMono(11, color: position.isGain ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                    Text(PpmFormatter.percent(ppm: position.pnlPpm))
                        .akzioMono(11, color: position.isGain ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                }
                MiniSparkline(values: position.spark, tone: position.isGain ? .gold : .coral)
                    .frame(height: 24)
                HStack(spacing: 4) {
                    // Target 行同时展示目标权重和 actual-target drift，差值不在 View 层重新归一化。
            Text(L10n.text("Target", language: language)).akzioText(.caption)
                    Text(PpmFormatter.share(ppm: position.targetPpm, fractionDigits: 1))
                        .akzioMono(10, color: AkzioColor.mutedText)
                    Spacer(minLength: 4)
                    Text(PpmFormatter.percent(ppm: position.actualPpm - position.targetPpm))
                        .akzioMono(10, color: AkzioColor.mutedText)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .akzioCard(border: isSelected ? AkzioColor.goldHairline : AkzioColor.hairline)
            .contentShape(Rectangle())
        }
        .buttonStyle(PressableButtonStyle(scale: 0.985))
        .hoverLift(lift: 4, tilt: 1.8)
        .sharedElement(.positionCard(position.asset), in: namespace)
        .accessibilityLabel("\(position.asset.rawValue) position")
    }
}
