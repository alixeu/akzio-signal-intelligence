import SwiftUI

// 文件职责：集中定义深灰、金色、珊瑚色及语义色调，供 View 和 Modifier 通过名称复用。
// 这些 token 是值语义的静态 Color/Gradient；调用方只消费结果，不在组件中重新发明颜色或状态含义。
// MARK: - Palette
//
// Hard constraint from the spec: deep grey + warm gold + coral only.
// No purple, no blue-violet, no neon. Low-saturation green is allowed *only*
// for tiny success dots and positive-return micro copy.
public enum AkzioColor {
    // 基础表面、文字和高光 token 共同约束整套液态玻璃层级；成功绿只保留给极小状态提示。
    public static let appBackground = Color(hex: 0x1A1A1A)
    public static let deepBackground = Color(hex: 0x1D1D1D)
    public static let raisedSurface = Color(hex: 0x232323)
    public static let elevatedSurface = Color(hex: 0x292929)

    public static let primaryGold = Color(hex: 0xD4A15E)
    public static let actionCoral = Color(hex: 0xFF6B4A)

    public static let primaryText = Color(hex: 0xF3EFE9)
    public static let secondaryText = Color(hex: 0xC7C0B7)
    public static let mutedText = Color(hex: 0xACA69E)

    public static let hairline = Color.white.opacity(0.08)
    public static let goldHairline = Color(hex: 0xD4A15E).opacity(0.18)
    public static let goldGlow = Color(hex: 0xD4A15E).opacity(0.22)
    public static let coralGlow = Color(hex: 0xFF6B4A).opacity(0.20)

    /// Only for <=8pt status dots and <=10pt positive-return copy. Never a surface.
    public static let successDot = Color(hex: 0x5E8F6B)

    // MARK: Sidebar reference surface
    // 侧栏引用同一套 surface/text/hairline token；theme 变体由函数返回，不复制第二套调色板。
    //
    // The sidebar follows the same theme surface as the page; the glass material,
    // edge and depth provide separation without introducing a second solid palette.
    public static let sidebarSurface = raisedSurface
    public static func sidebarSurface(for theme: SettingsPresentation.Theme) -> Color {
        surface(for: theme)
    }
    public static let sidebarSelection = primaryGold.opacity(0.18)
    public static let sidebarAccent = primaryGold
    public static let sidebarPrimaryText = primaryText
    public static let sidebarSecondaryText = secondaryText
    public static let sidebarHairline = hairline

    // MARK: Theme variants
    //
    // All three themes stay inside the deep-grey band; the only difference is a few
    // degrees of warmth. Anything wider would break the gold/coral relationship.
    public static func background(for theme: SettingsPresentation.Theme) -> Color {
        switch theme {
        case .dark: appBackground
        case .dusk: Color(hex: 0x211F1D)
        case .midnight: Color(hex: 0x181818)
        }
    }

    public static func surface(for theme: SettingsPresentation.Theme) -> Color {
        switch theme {
        case .dark: raisedSurface
        case .dusk: Color(hex: 0x272421)
        case .midnight: Color(hex: 0x202020)
        }
    }

    // MARK: Derived
    // 输入 opacity，输出带统一主题色的 Color；透明度只改变显示强度，不改变语义类别。
    public static func gold(_ opacity: Double) -> Color { primaryGold.opacity(opacity) }
    public static func coral(_ opacity: Double) -> Color { actionCoral.opacity(opacity) }

    /// Warm radial bloom used behind the active node / gauge glow.
    public static let goldBloom = RadialGradient(
        colors: [primaryGold.opacity(0.34), primaryGold.opacity(0.06), .clear],
        center: .center,
        startRadius: 0,
        endRadius: 90
    )

    /// Selected sidebar item background.
    public static let goldFill = LinearGradient(
        colors: [primaryGold.opacity(0.22), primaryGold.opacity(0.10)],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
    )

    /// Hairline that catches light on the top edge of a glass surface.
    public static let glassTopEdge = LinearGradient(
        colors: [Color.white.opacity(0.10), Color.white.opacity(0.02)],
        startPoint: .top,
        endPoint: .bottom
    )
}

// MARK: - Semantic tone

/// Every status maps onto one of four tones. Tone alone never carries meaning —
/// `StatusSemantics` always pairs it with a symbol and a label.
public enum AkzioTone: Sendable, Hashable {
    case gold
    case coral
    case neutral
    case muted

    // computed property 将值语义的 tone 映射成展示色；它不保存 View 状态，也不依赖 Environment。
    public var color: Color {
        switch self {
        case .gold: AkzioColor.primaryGold
        case .coral: AkzioColor.actionCoral
        case .neutral: AkzioColor.secondaryText
        case .muted: AkzioColor.mutedText
        }
    }

    // glow 是同一 tone 的装饰性光晕；缺省/静默语义使用低强度或透明色，而不是另造状态。
    public var glow: Color {
        switch self {
        case .gold: AkzioColor.goldGlow
        case .coral: AkzioColor.coralGlow
        case .neutral: Color.white.opacity(0.06)
        case .muted: .clear
        }
    }
}

// MARK: - Hex support

extension Color {
    // 输入 0xRRGGBB 的无符号整数，输出固定 sRGB、alpha=1 的 Color；位移只拆分通道，不做运行时状态转换。
    public init(hex: UInt32) {
        self.init(
            .sRGB,
            red: Double((hex >> 16) & 0xFF) / 255,
            green: Double((hex >> 8) & 0xFF) / 255,
            blue: Double(hex & 0xFF) / 255,
            opacity: 1
        )
    }
}
