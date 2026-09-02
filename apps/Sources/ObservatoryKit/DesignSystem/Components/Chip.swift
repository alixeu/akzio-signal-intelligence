import SwiftUI

// MARK: - Chip
//
// Filter and tag chips. Selection turns the border gold and expands the chip
// slightly (180–240ms) — no colour-only signalling.
public struct Chip: View {
    public enum Kind { case filter, tag, artifact }

    private let title: String
    private let symbol: String?
    private let kind: Kind
    private let isSelected: Bool
    private let action: (() -> Void)?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var isHovering = false

    public init(
        _ title: String,
        symbol: String? = nil,
        kind: Kind = .filter,
        isSelected: Bool = false,
        action: (() -> Void)? = nil
    ) {
        self.title = title
        self.symbol = symbol
        self.kind = kind
        self.isSelected = isSelected
        self.action = action
    }

    public var body: some View {
        let content = HStack(spacing: 5) {
            if let symbol {
                Image(systemName: symbol)
                    .font(.system(size: 9, weight: .medium))
            }
            Text(L10n.text(title, language: language))
                .font(AkzioFont.label)
                .tracking(AkzioFont.labelTracking)
                .lineLimit(1)
        }
        .foregroundStyle(isSelected ? AkzioColor.primaryGold : AkzioColor.secondaryText)
        .padding(.horizontal, kind == .tag ? 7 : 9)
        .padding(.vertical, kind == .tag ? 3 : 5)
        .background(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .fill(isSelected ? AkzioColor.primaryGold.opacity(0.12) : AkzioColor.elevatedSurface.opacity(isHovering ? 0.9 : 0.55))
        )
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(
                    isSelected ? AkzioColor.goldHairline.opacity(2.2) : AkzioColor.hairline,
                    lineWidth: AkzioLayout.hairlineWidth
                )
        )
        .scaleEffect(isSelected ? 1.03 : 1.0)
        .animation(policy.resolve(Motion.hover), value: isSelected)
        .animation(policy.resolve(Motion.hover), value: isHovering)
        .onHover { hovering in
            // Hover only exists for precise pointers; trackpad taps must not latch it.
            isHovering = hovering
        }

        if let action {
            Button(action: action) { content }
                .buttonStyle(PressableButtonStyle())
                .accessibilityAddTraits(isSelected ? [.isSelected] : [])
        } else {
            content
        }
    }
}

// MARK: - Segmented control

/// Selection background slides between options instead of flashing.
public struct AkzioSegmentedControl<Value: Hashable>: View {
    private let options: [(value: Value, label: String)]
    @Binding private var selection: Value

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Namespace private var indicator

    public init(selection: Binding<Value>, options: [(value: Value, label: String)]) {
        self._selection = selection
        self.options = options
    }

    public var body: some View {
        HStack(spacing: 2) {
            ForEach(options, id: \.value) { option in
                let isSelected = option.value == selection
                Button {
                    withAnimation(policy.resolve(Motion.selection)) { selection = option.value }
                } label: {
                    Text(L10n.text(option.label, language: language))
                        .font(AkzioFont.label)
                        .tracking(AkzioFont.labelTracking)
                        .foregroundStyle(isSelected ? AkzioColor.primaryGold : AkzioColor.secondaryText)
                        .padding(.horizontal, 10)
                        .padding(.vertical, 5)
                        .background {
                            if isSelected {
                                RoundedRectangle(cornerRadius: 6, style: .continuous)
                                    .fill(AkzioColor.primaryGold.opacity(0.14))
                                    .matchedGeometryEffect(id: "segment", in: indicator)
                            }
                        }
                }
                .buttonStyle(.plain)
                .accessibilityAddTraits(isSelected ? [.isSelected] : [])
            }
        }
        .padding(2)
    .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 8)
        .overlay(
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: AkzioLayout.hairlineWidth)
        )
    }
}
