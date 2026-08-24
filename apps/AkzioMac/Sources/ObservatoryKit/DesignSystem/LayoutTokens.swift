import SwiftUI

// MARK: - Layout
//
// 8pt baseline. Page gutters, card radii, borders and the two shadow levels
// are all named so no view invents its own spacing.
public enum AkzioLayout {
    public static let s1: CGFloat = 4
    public static let s2: CGFloat = 8
    public static let s3: CGFloat = 12
    public static let s4: CGFloat = 16
    public static let s5: CGFloat = 20
    public static let s6: CGFloat = 24
    public static let s8: CGFloat = 32

    public static let pageMargin: CGFloat = 20
    public static let statusBarHeight: CGFloat = 44
    public static let sidebarWidth: CGFloat = 248
    public static let inspectorWidth: CGFloat = 300
    public static let workflowInspectorWidth: CGFloat = 520
    public static let inspectorOverlayMaxHeight: CGFloat = 560
    /// Wide enough for "Analyst 1 · gemini-3.1-pro · 83%" on one line without
    /// truncating the role name, which is the row's primary identifier.
    public static let rightRailWidth: CGFloat = 312

    public static let cardRadius: CGFloat = 14
    public static let chipRadius: CGFloat = 8
    public static let sheetRadius: CGFloat = 20

    public static let hairlineWidth: CGFloat = 1
    public static let focusRingWidth: CGFloat = 2

    /// Below this width the shell collapses the sidebar and folds the right rail.
    public static let compactWidthThreshold: CGFloat = 1380

    /// Keeps an in-page inspector inside its owning content region at every
    /// window size. The outer `s3` padding consumes `s6` across both edges.
    public static func inspectorOverlaySize(in available: CGSize) -> CGSize {
        CGSize(
            width: min(inspectorWidth, max(1, available.width - s6)),
            height: min(inspectorOverlayMaxHeight, max(1, available.height - s6))
        )
    }
}

public enum AkzioShadow {
    case card
    case float

    public var radius: CGFloat {
        switch self {
        case .card: 2
        case .float: 24
        }
    }

    public var y: CGFloat {
        switch self {
        case .card: 1
        case .float: 8
        }
    }

    public var color: Color {
        switch self {
        case .card: .black.opacity(0.28)
        case .float: .black.opacity(0.40)
        }
    }
}

extension View {
    public func akzioShadow(_ level: AkzioShadow) -> some View {
        shadow(color: level.color, radius: level.radius, x: 0, y: level.y)
    }

    /// Standard liquid-glass content card. Accessibility and nested-glass
    /// fallbacks are owned by `akzioGlass`, so every module degrades alike.
    public func akzioCard(
        radius: CGFloat = AkzioLayout.cardRadius,
        padding: CGFloat = AkzioLayout.s4,
        border: Color = AkzioColor.hairline
    ) -> some View {
        self
            .padding(padding)
            .akzioGlass(.base, radius: radius)
            .overlay(
                RoundedRectangle(cornerRadius: radius, style: .continuous)
                    .strokeBorder(border, lineWidth: AkzioLayout.hairlineWidth)
            )
    }

    /// Hairline separator that fades at both ends instead of a hard 1px rule.
    public func akzioTopEdgeFade() -> some View {
        overlay(alignment: .top) {
            LinearGradient(
                colors: [AkzioColor.hairline, .clear],
                startPoint: .top,
                endPoint: .bottom
            )
            .frame(height: 8)
            .allowsHitTesting(false)
        }
    }
}

public struct HairlineDivider: View {
    private let axis: Axis
    private let color: Color

    public init(_ axis: Axis = .horizontal, color: Color = AkzioColor.hairline) {
        self.axis = axis
        self.color = color
    }

    public var body: some View {
        Rectangle()
            .fill(color)
            .frame(
                width: axis == .vertical ? AkzioLayout.hairlineWidth : nil,
                height: axis == .horizontal ? AkzioLayout.hairlineWidth : nil
            )
    }
}
