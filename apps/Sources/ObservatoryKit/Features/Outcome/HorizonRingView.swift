import SwiftUI

// MARK: - Horizon ring
//
// T+1 / T+3 / T+5 always coexist. A sealed ring closes with one check and a single
// bloom; the observing ring breathes very lightly and is the only one that moves; a
// waiting ring is a dashed low-brightness track, never 0%.
struct HorizonRingView: View {
    // horizon 是单个 T+1/T+3/T+5 的 Rust 展示投影；onSelect 闭包只把选择权交回父级 store。
    let horizon: HorizonPresentation
    let isSelected: Bool
    let namespace: Namespace.ID?
    let onSelect: () -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    // bloomTick 是本地动画触发器，不代表新的 Outcome 状态或一次新的 sealing。
    @State private var bloomTick = 0

    var body: some View {
        // Button 的 action 由父级提供；环形进度、Optional progress 和 status badge 都只读取 horizon。
        Button(action: onSelect) {
            VStack(spacing: AkzioLayout.s2) {
                ProgressRing(
                    progress: horizon.progress,
                    tone: horizon.status.style.tone,
                    lineWidth: 7,
                    diameter: 108,
                    isBreathing: horizon.status == .observing
                ) {
                    VStack(spacing: 1) {
                        Text(horizon.horizon.displayName)
                            .akzioMetric(17)
                        if horizon.isSealed {
                            Image(systemName: "checkmark")
                                .font(.system(size: 9, weight: .bold))
                                .foregroundStyle(AkzioColor.primaryGold)
                        } else if let progress = horizon.progress {
                            // observing 的百分比只有在投影提供 progress 时显示，nil 保持 waiting 而不显示 0%。
                            Text(PpmFormatter.share(
                                ppm: Int(progress * PpmFormatter.ppmPerUnit),
                                fractionDigits: 0
                            ))
                            .akzioMono(9, color: AkzioColor.secondaryText)
                        } else {
                            Text(L10n.text(MissingValue.waiting.rawValue, language: language))
                                .akzioMono(9, color: AkzioColor.mutedText)
                        }
                    }
                }
                .frame(width: 108, height: 108)
                .completionBloom(trigger: bloomTick)
                .sharedElement(sharedID, in: namespace)

                statusBadge
                Text(L10n.text(horizon.horizon.windowLabel, language: language)).akzioText(.caption)
                Text(L10n.text(horizon.note, language: language))
                    .akzioText(.caption, color: AkzioColor.mutedText)
                    .multilineTextAlignment(.center)
                    .lineLimit(2)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, AkzioLayout.s2)
            .background {
                if isSelected {
                    RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                        .fill(AkzioColor.gold(0.06))
                }
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(PressableButtonStyle(scale: 0.99))
        .onChange(of: horizon.isSealed) { _, sealed in
            // SwiftUI 的变化闭包只在 sealed 边沿递增 bloomTick，让 completionBloom 播放一次。
            if sealed { bloomTick += 1 }
        }
        .accessibilityLabel("\(horizon.horizon.displayName) \(L10n.text("Outcome", language: language))")
        .accessibilityValue("\(L10n.text(horizon.status.style.label, language: language)). \(L10n.text(horizon.note, language: language))")
        .accessibilityAddTraits(isSelected ? [.isSelected] : [])
    }

    /// Sealed rings feed the Learning handoff; every ring keeps its own anchor.
    private var sharedID: SharedElementID {
        horizon.isSealed ? .completedRing : .horizonRing(horizon.horizon)
    }

    /// Badge morphs between states instead of swapping the string outright.
    private var statusBadge: some View {
        // badge 用 status 作为 identity，让状态过渡有明确的插入/替换动画而不混淆 horizon 数据。
        StatusBadge(horizon.status, size: .compact)
            .id(horizon.status)
            .transition(.scale(scale: 0.94).combined(with: .opacity))
            .animation(policy.resolve(Motion.badge), value: horizon.status)
    }
}
