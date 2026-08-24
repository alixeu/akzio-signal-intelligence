import SwiftUI

// MARK: - Press feedback
//
// Feedback lands on press-down, not on release: 0.97 scale, ~160ms.
public struct PressableButtonStyle: ButtonStyle {
    @Environment(\.motionPolicy) private var policy
    private let scale: CGFloat

    public init(scale: CGFloat = 0.97) {
        self.scale = scale
    }

    public func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? scale : 1)
            .animation(policy.resolve(Motion.control), value: configuration.isPressed)
            .contentShape(Rectangle())
    }
}

// MARK: - Hover lift

/// Card hover: 3–6pt lift, ≤2° tilt, warm edge light sweeping top-left → bottom-right.
public struct HoverLift: ViewModifier {
    let lift: CGFloat
    let tilt: Double
    let radius: CGFloat

    @Environment(\.motionPolicy) private var policy
    @State private var isHovering = false

    public func body(content: Content) -> some View {
        content
            .overlay {
                if isHovering && !policy.isReduced {
                    RoundedRectangle(cornerRadius: radius, style: .continuous)
                        .strokeBorder(
                            LinearGradient(
                                colors: [
                                    AkzioColor.primaryGold.opacity(0.55),
                                    AkzioColor.primaryGold.opacity(0.10),
                                    .clear,
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            ),
                            lineWidth: 1
                        )
                        .allowsHitTesting(false)
                }
            }
            .offset(y: isHovering ? -policy.travel(lift) : 0)
            .rotation3DEffect(
                .degrees(isHovering && !policy.isReduced ? tilt : 0),
                axis: (x: 1, y: -0.35, z: 0),
                perspective: 0.6
            )
            .shadow(
                color: .black.opacity(isHovering ? 0.34 : 0),
                radius: isHovering ? 14 : 0,
                y: isHovering ? 6 : 0
            )
            .animation(policy.resolve(Motion.hover), value: isHovering)
            .onHover { isHovering = $0 }
    }
}

extension View {
    public func hoverLift(
        lift: CGFloat = 4,
        tilt: Double = 1.6,
        radius: CGFloat = AkzioLayout.cardRadius
    ) -> some View {
        modifier(HoverLift(lift: lift, tilt: tilt, radius: radius))
    }

    /// Row hover: background warms, row height never changes.
    public func rowHoverHighlight(isSelected: Bool = false) -> some View {
        modifier(RowHoverHighlight(isSelected: isSelected))
    }
}

public struct RowHoverHighlight: ViewModifier {
    let isSelected: Bool

    @Environment(\.motionPolicy) private var policy
    @State private var isHovering = false

    public func body(content: Content) -> some View {
        content
            .background {
                if isSelected {
                    LinearGradient(
                        colors: [AkzioColor.primaryGold.opacity(0.14), AkzioColor.primaryGold.opacity(0.05)],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                } else if isHovering {
                    LinearGradient(
                        colors: [AkzioColor.primaryGold.opacity(0.07), .clear],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                }
            }
            .overlay(alignment: .leading) {
                if isSelected {
                    Rectangle()
                        .fill(AkzioColor.primaryGold)
                        .frame(width: 2)
                }
            }
            .offset(y: isSelected ? -1 : 0)
            .animation(policy.resolve(Motion.hover), value: isHovering)
            .animation(policy.resolve(Motion.selection), value: isSelected)
            .onHover { isHovering = $0 }
    }
}
