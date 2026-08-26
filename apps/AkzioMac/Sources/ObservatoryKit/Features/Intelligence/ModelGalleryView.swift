import SwiftUI

// MARK: - Model gallery
//
// Cover flow: the centre card is crisp, neighbours scale to 0.88–0.94 and dim.
// Deliberately shallow — no dramatic 3D, because this is a picker, not a carousel ad.
struct ModelGalleryView: View {
    let options: [ModelOption]
    @Binding var centerIndex: Int

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Text("Model Gallery").akzioText(.sectionTitle)
                Spacer(minLength: AkzioLayout.s2)
                stepper
            }
            HStack(spacing: AkzioLayout.s2) {
                ForEach(Array(options.enumerated()), id: \.element.id) { index, option in
                    card(option, distance: abs(index - centerIndex))
                        .onTapGesture {
                            withAnimation(policy.resolve(.spring(response: 0.52, dampingFraction: 0.95))) {
                                centerIndex = index
                            }
                        }
                }
            }
            .animation(policy.resolve(.spring(response: 0.52, dampingFraction: 0.95)), value: centerIndex)
        }
        .akzioCard()
    }

    private var stepper: some View {
        HStack(spacing: 2) {
            Button { step(-1) } label: {
                Image(systemName: "chevron.left").font(.system(size: 10, weight: .semibold))
            }
            .buttonStyle(PressableButtonStyle())
            Button { step(1) } label: {
                Image(systemName: "chevron.right").font(.system(size: 10, weight: .semibold))
            }
            .buttonStyle(PressableButtonStyle())
        }
        .foregroundStyle(AkzioColor.secondaryText)
    }

    private func step(_ delta: Int) {
        withAnimation(policy.resolve(.spring(response: 0.52, dampingFraction: 0.95))) {
            centerIndex = min(max(centerIndex + delta, 0), options.count - 1)
        }
    }

    private func card(_ option: ModelOption, distance: Int) -> some View {
        let scale: CGFloat = distance == 0 ? 1 : (distance == 1 ? 0.94 : 0.88)
        return VStack(alignment: .leading, spacing: 4) {
            Text(option.name)
                .akzioMono(11, color: distance == 0 ? AkzioColor.primaryText : AkzioColor.secondaryText)
                .lineLimit(1)
            Text(option.tier).akzioText(.caption)
            if option.isSelected {
                PillTag("Configured", tone: .gold)
            }
        }
        .padding(AkzioLayout.s2)
        .frame(maxWidth: .infinity, alignment: .leading)
            .akzioGlassBackdrop(
                distance == 0 ? AkzioColor.elevatedSurface : AkzioColor.deepBackground,
                radius: AkzioLayout.chipRadius
            )
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(distance == 0 ? AkzioColor.goldHairline : AkzioColor.hairline, lineWidth: 1)
        )
        .scaleEffect(scale)
        .opacity(distance == 0 ? 1 : (distance == 1 ? 0.8 : 0.6))
        .accessibilityLabel("\(option.name), \(option.tier)")
    }
}
