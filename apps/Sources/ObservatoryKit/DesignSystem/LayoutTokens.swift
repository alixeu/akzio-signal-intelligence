import SwiftUI

// 文件职责：提供 8pt 基线下的间距、尺寸、圆角、边框和阴影 token，并把重复布局规则封装成值函数/Modifier。
// Token 只返回 CGFloat/Color 等值；View 通过名称消费它们，从而避免局部尺寸漂移。
// MARK: - Layout
//
// 8pt baseline. Page gutters, card radii, borders and the two shadow levels
// are all named so no view invents its own spacing.
public enum AkzioLayout {
    // s1...s8 是离散间距阶梯；页面和组件应组合这些值，而不是在调用点另造任意间距。
    public static let s1: CGFloat = 4
    public static let s2: CGFloat = 8
    public static let s3: CGFloat = 12
    public static let s4: CGFloat = 16
    public static let s5: CGFloat = 20
    public static let s6: CGFloat = 24
    public static let s8: CGFloat = 32

    public static let pageMargin: CGFloat = 20
    public static let statusBarHeight: CGFloat = 48
    /// Minimum fixed width that fits the longest English navigation label
    /// ("Intelligence") with the icon, row padding and sidebar breathing room.
    public static let sidebarWidth: CGFloat = 224
    public static let sidebarHorizontalPadding: CGFloat = 16
    /// The compact sidebar control sits to the right of the native traffic lights,
    /// with a visible left breathing room before the control.
    public static let collapsedSidebarToggleLeading: CGFloat = 72
    public static let collapsedSidebarToggleSize: CGFloat = 36
    /// Breathing room between the compact control and the first status value.
    public static let collapsedSidebarToggleGap: CGFloat = 8
    public static let collapsedSidebarContentLeading: CGFloat =
        collapsedSidebarToggleLeading + collapsedSidebarToggleSize + collapsedSidebarToggleGap
    public static var collapsedSidebarToggleTop: CGFloat {
        // computed property 从固定状态栏与按钮尺寸推导顶部偏移，并以 0 截断小窗口负值。
        max(0, (statusBarHeight - collapsedSidebarToggleSize) / 2)
    }
    public static let inspectorWidth: CGFloat = 300
    public static let workflowInspectorWidth: CGFloat = 360
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
        // 输入可用容器尺寸，输出受 rail 宽度/最大高度和 1pt 下限约束的 CGSize；不修改父布局状态。
        // 宽高都至少保留 1pt；同时受固定 rail 宽度和最大高度限制，避免小窗口出现负尺寸。
        CGSize(
            width: min(inspectorWidth, max(1, available.width - s6)),
            height: min(inspectorOverlayMaxHeight, max(1, available.height - s6))
        )
    }
}

public enum AkzioShadow {
    case card
    case float

    // shadow 的三个 computed property 从同一个层级值派生，保证半径、偏移和颜色成套变化。
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
    // 输入 View 和阴影层级，输出应用 token 阴影的 some View；Modifier 不拥有或改变原 View 的数据。
    public func akzioShadow(_ level: AkzioShadow) -> some View {
        // 阴影参数只由 token 决定，View 不在调用点自行组合半径和偏移。
        shadow(color: level.color, radius: level.radius, x: 0, y: level.y)
    }

    /// Standard liquid-glass content card. Accessibility and nested-glass
    /// fallbacks are owned by `akzioGlass`, so every module degrades alike.
    public func akzioCard(
        radius: CGFloat = AkzioLayout.cardRadius,
        padding: CGFloat = AkzioLayout.s4,
        border: Color = AkzioColor.hairline
    ) -> some View {
        // Card 统一执行 padding → glass → hairline overlay；参数只是允许调用方选择边界，不改变 token 默认值。
        self
            .padding(padding)
            .akzioGlass(.base, radius: radius)
            .overlay(
                RoundedRectangle(cornerRadius: radius, style: .continuous)
                    .strokeBorder(border, lineWidth: AkzioLayout.hairlineWidth)
            )
    }

}

public struct HairlineDivider: View {
    private let axis: Axis
    private let color: Color

    // 初始化时复制 axis/color 值；作为 View 的不可变存储，后续 body 只根据它们计算尺寸。
    public init(_ axis: Axis = .horizontal, color: Color = AkzioColor.hairline) {
        self.axis = axis
        self.color = color
    }

    public var body: some View {
        // body 输出 Rectangle；选定轴使用 hairlineWidth，另一轴留 nil 让父布局决定长度。
        // Divider 只改变对应轴的尺寸，另一轴保持由父布局决定。
        Rectangle()
            .fill(color)
            .frame(
                width: axis == .vertical ? AkzioLayout.hairlineWidth : nil,
                height: axis == .horizontal ? AkzioLayout.hairlineWidth : nil
            )
    }
}
