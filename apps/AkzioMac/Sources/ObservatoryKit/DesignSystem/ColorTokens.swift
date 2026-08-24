import SwiftUI

// MARK: - Palette
//
// Hard constraint from the spec: deep grey + warm gold + coral only.
// No purple, no blue-violet, no neon. Low-saturation green is allowed *only*
// for tiny success dots and positive-return micro copy.
public enum AkzioColor {
    public static let appBackground = Color(hex: 0x1A1A1A)
    public static let deepBackground = Color(hex: 0x1D1D1D)
    public static let raisedSurface = Color(hex: 0x232323)
    public static let elevatedSurface = Color(hex: 0x292929)

    public static let primaryGold = Color(hex: 0xD4A15E)
    public static let actionCoral = Color(hex: 0xFF6B4A)

    public static let primaryText = Color(hex: 0xF3EFE9)
    public static let secondaryText = Color(hex: 0xB0A9A0)
    public static let mutedText = Color(hex: 0x948D84)

    public static let hairline = Color.white.opacity(0.08)
    public static let goldHairline = Color(hex: 0xD4A15E).opacity(0.18)
    public static let goldGlow = Color(hex: 0xD4A15E).opacity(0.22)
    public static let coralGlow = Color(hex: 0xFF6B4A).opacity(0.20)

    /// Only for <=8pt status dots and <=10pt positive-return copy. Never a surface.
    public static let successDot = Color(hex: 0x5E8F6B)

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

    public var color: Color {
        switch self {
        case .gold: AkzioColor.primaryGold
        case .coral: AkzioColor.actionCoral
        case .neutral: AkzioColor.secondaryText
        case .muted: AkzioColor.mutedText
        }
    }

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

/// Hue/saturation probe used by `ColorTokenTests` to keep the palette honest.
public struct ColorProbe: Sendable {
    public let hue: Double
    public let saturation: Double
    public let brightness: Double

    public init(hex: UInt32) {
        let r = Double((hex >> 16) & 0xFF) / 255
        let g = Double((hex >> 8) & 0xFF) / 255
        let b = Double(hex & 0xFF) / 255
        let maxV = max(r, g, b)
        let minV = min(r, g, b)
        let delta = maxV - minV
        brightness = maxV
        saturation = maxV == 0 ? 0 : delta / maxV
        if delta == 0 {
            hue = 0
        } else if maxV == r {
            hue = (60 * ((g - b) / delta)).truncatingRemainder(dividingBy: 360)
        } else if maxV == g {
            hue = 60 * ((b - r) / delta) + 120
        } else {
            hue = 60 * ((r - g) / delta) + 240
        }
    }

    /// Purple / violet / blue band the spec bans outright.
    public var isBannedHue: Bool { saturation > 0.15 && hue >= 200 && hue <= 330 }
}
