import SwiftUI

// MARK: - Canvas toolbar
//
// Ten controls. Zoom / pan / visibility are real view state; the rest are visual
// affordances that acknowledge the click and change nothing in the system — this
// build never writes to the Store.
struct CanvasToolbar: View {
    @Binding var scale: CGFloat
    @Binding var offset: CGSize
    @Binding var showsLabels: Bool
    @Binding var showsParticles: Bool
    @Binding var showsGrid: Bool
    @Binding var highlightsCriticalPath: Bool
    @Binding var collapsesOptional: Bool

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var acknowledged: String?

    var body: some View {
        HStack(spacing: AkzioLayout.s1) {
            button("minus.magnifyingglass", "Zoom Out") { zoom(by: 0.9) }
            button("plus.magnifyingglass", "Zoom In") { zoom(by: 1.1) }
            button("arrow.up.left.and.arrow.down.right", "Fit") { reset() }
            button("arrow.counterclockwise", "Reset View") { reset() }
            HairlineDivider(.vertical).frame(height: 16)
            toggle("textformat.size", "Labels", $showsLabels)
            toggle("sparkles", "Particles", $showsParticles)
            toggle("grid", "Grid", $showsGrid)
            toggle("bolt.horizontal", "Critical Path", $highlightsCriticalPath)
            toggle("arrow.down.right.and.arrow.up.left", "Collapse Optional", $collapsesOptional)
        }
        .padding(.horizontal, AkzioLayout.s2)
        .padding(.vertical, 5)
        .akzioGlass(.elevated, radius: AkzioLayout.chipRadius)
    }

    private func zoom(by factor: CGFloat) {
        withAnimation(policy.resolve(Motion.control)) {
            scale = min(max(scale * factor, 0.5), 2.0)
        }
    }

    private func reset() {
        withAnimation(policy.resolve(Motion.panel)) {
            scale = 1
            offset = .zero
        }
    }

    private func button(_ symbol: String, _ title: String, action: @escaping () -> Void) -> some View {
        Button {
            acknowledged = title
            action()
        } label: {
            Image(systemName: symbol)
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(AkzioColor.secondaryText)
                .frame(width: 24, height: 22)
                .background {
                    if acknowledged == title {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(AkzioColor.gold(0.16))
                    }
                }
        }
        .buttonStyle(PressableButtonStyle())
        .help(L10n.text(title, language: language))
        .animation(policy.resolve(Motion.hover), value: acknowledged)
    }

    private func toggle(_ symbol: String, _ title: String, _ binding: Binding<Bool>) -> some View {
        Button {
            withAnimation(policy.resolve(Motion.control)) { binding.wrappedValue.toggle() }
        } label: {
            Image(systemName: symbol)
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(binding.wrappedValue ? AkzioColor.primaryGold : AkzioColor.mutedText)
                .frame(width: 24, height: 22)
                .background {
                    if binding.wrappedValue {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(AkzioColor.gold(0.14))
                    }
                }
        }
        .buttonStyle(PressableButtonStyle())
        .help(L10n.text(title, language: language))
    }
}
