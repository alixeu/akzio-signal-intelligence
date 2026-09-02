import SwiftUI

// MARK: - Typography
//
// System fonts only (SF Pro Display / SF Pro Text / SF Mono).
// Tracking is size-specific: large text tightens, small labels open up.
// Every numeric readout is monospaced-digit so count-ups never reflow.
public enum AkzioFont {
    /// Base point sizes. Everything else is derived by multiplying by the accessibility
    /// text scale, so one setting moves the whole type system together.
    public enum Size {
        public static let display: CGFloat = 28
        public static let title: CGFloat = 18
        public static let sectionTitle: CGFloat = 14
        public static let body: CGFloat = 13
        public static let bodySmall: CGFloat = 12
        public static let label: CGFloat = 11
        public static let caption: CGFloat = 10
    }

    public static let display = Font.system(size: Size.display, weight: .semibold)
    public static let displayTracking: CGFloat = -0.4

    public static let title = Font.system(size: Size.title, weight: .semibold)
    public static let titleTracking: CGFloat = -0.2

    public static let sectionTitle = Font.system(size: Size.sectionTitle, weight: .semibold)
    public static let sectionTracking: CGFloat = 0.1

    public static let body = Font.system(size: Size.body, weight: .regular)
    public static let bodySmall = Font.system(size: Size.bodySmall, weight: .regular)

    public static let label = Font.system(size: Size.label, weight: .medium)
    public static let labelTracking: CGFloat = 0.2

    public static let caption = Font.system(size: Size.caption, weight: .medium)
    public static let captionTracking: CGFloat = 0.3

    public static func metric(_ size: CGFloat) -> Font {
        .system(size: size, weight: .semibold).monospacedDigit()
    }

    public static func mono(_ size: CGFloat = 12, weight: Font.Weight = .medium) -> Font {
        .system(size: size, weight: weight, design: .monospaced)
    }

    /// Text Size from Settings, clamped so layout never breaks.
    public static func scaled(_ size: CGFloat, _ scale: Double) -> CGFloat {
        (size * CGFloat(min(max(scale, 0.9), 1.3))).rounded()
    }
}

// MARK: - Text scale

private struct AkzioTextScaleKey: EnvironmentKey {
    static let defaultValue: Double = 1.0
}

extension EnvironmentValues {
    /// 0.9–1.3 from Settings. Read by every text modifier in the design system.
    public var akzioTextScale: Double {
        get { self[AkzioTextScaleKey.self] }
        set { self[AkzioTextScaleKey.self] = newValue }
    }
}

// MARK: - Text styles

public enum AkzioTextRole {
    case display, title, sectionTitle, body, bodySmall, label, caption
}

extension View {
    /// Applies font + tracking + default colour for a role in one call.
    public func akzioText(_ role: AkzioTextRole, color: Color? = nil) -> some View {
        modifier(AkzioTextModifier(role: role, color: color))
    }

    /// Monospaced technical value (Run ID, hash, ppm, latency).
    public func akzioMono(_ size: CGFloat = 12, color: Color = AkzioColor.secondaryText) -> some View {
        modifier(AkzioMonoModifier(size: size, color: color))
    }

    /// Large numeric readout that must not jitter while animating.
    public func akzioMetric(_ size: CGFloat, color: Color = AkzioColor.primaryText) -> some View {
        modifier(AkzioMetricModifier(size: size, color: color))
    }
}

struct AkzioTextModifier: ViewModifier {
    let role: AkzioTextRole
    let color: Color?

    @Environment(\.akzioTextScale) private var scale

    private func font(_ size: CGFloat, weight: Font.Weight) -> Font {
        .system(size: AkzioFont.scaled(size, scale), weight: weight)
    }

    func body(content: Content) -> some View {
        switch role {
        case .display:
            content.font(font(AkzioFont.Size.display, weight: .semibold))
                .tracking(AkzioFont.displayTracking)
                .foregroundStyle(color ?? AkzioColor.primaryText)
        case .title:
            content.font(font(AkzioFont.Size.title, weight: .semibold))
                .tracking(AkzioFont.titleTracking)
                .foregroundStyle(color ?? AkzioColor.primaryText)
        case .sectionTitle:
            content.font(font(AkzioFont.Size.sectionTitle, weight: .semibold))
                .tracking(AkzioFont.sectionTracking)
                .foregroundStyle(color ?? AkzioColor.primaryText)
        case .body:
            content.font(font(AkzioFont.Size.body, weight: .regular))
                .lineSpacing(4)
                .foregroundStyle(color ?? AkzioColor.secondaryText)
        case .bodySmall:
            content.font(font(AkzioFont.Size.bodySmall, weight: .regular))
                .lineSpacing(3)
                .foregroundStyle(color ?? AkzioColor.secondaryText)
        case .label:
            content.font(font(AkzioFont.Size.label, weight: .medium))
                .tracking(AkzioFont.labelTracking)
                .foregroundStyle(color ?? AkzioColor.secondaryText)
        case .caption:
            content.font(font(AkzioFont.Size.caption, weight: .medium))
                .tracking(AkzioFont.captionTracking)
                .textCase(.uppercase)
                .foregroundStyle(color ?? AkzioColor.mutedText)
        }
    }
}

struct AkzioMonoModifier: ViewModifier {
    let size: CGFloat
    let color: Color

    @Environment(\.akzioTextScale) private var scale

    func body(content: Content) -> some View {
        content
            .font(AkzioFont.mono(AkzioFont.scaled(size, scale)))
            .foregroundStyle(color)
    }
}

struct AkzioMetricModifier: ViewModifier {
    let size: CGFloat
    let color: Color

    @Environment(\.akzioTextScale) private var scale

    func body(content: Content) -> some View {
        content
            .font(AkzioFont.metric(AkzioFont.scaled(size, scale)))
            .tracking(-0.3)
            .foregroundStyle(color)
    }
}
