import SwiftUI

// 文件职责：定义液态玻璃层级、透明度/无障碍 Environment、ViewModifier 及统一 View 入口。
// Modifier 读取当前 Environment 后返回新的 View；GlassLevel/GlassIntensity 是 Sendable 值配置，不携带引用型渲染状态。
// MARK: - Glass
//
// One glass implementation serves chrome and content cards. Nesting glass
// inside glass is blocked at the API level to preserve contrast and depth.
public enum GlassLevel: Sendable {
    case base      // status bar / content card
    case elevated  // inspector / popover / tooltip
    case modal     // settings layer

    // 输入层级，输出与层级匹配的阴影 token；层级本身保持轻量、可跨并发传递的值语义。
    var shadow: AkzioShadow {
        switch self {
        case .base: .card
        case .elevated, .modal: .float
        }
    }

    /// Bigger surfaces read as thicker glass.
}

public enum GlassIntensity: String, CaseIterable, Sendable {
    case low, medium, high

    // computed property 把用户可选强度转换为内部 blur boost；它不直接决定透明度或状态语义。
    var blurBoost: Double {
        switch self {
        case .low: 0.0
        case .medium: 0.5
        case .high: 1.0
        }
    }
}

private struct InsideGlassKey: EnvironmentKey { static let defaultValue = false }
private struct HighContrastKey: EnvironmentKey { static let defaultValue = false }
private struct CompactLayoutKey: EnvironmentKey { static let defaultValue = false }
private struct ColorIndependentStatusKey: EnvironmentKey { static let defaultValue = true }
private struct GlassIntensityKey: EnvironmentKey { static let defaultValue = GlassIntensity.medium }
private struct GlassTransparencyKey: EnvironmentKey { static let defaultValue = 0.65 }
private struct ReduceTransparencyOverrideKey: EnvironmentKey { static let defaultValue = false }

extension EnvironmentValues {
    // 这些 get/set 访问器把窗口/Store 注入的显示策略沿 View 树传递；默认值保证孤立预览也能渲染。
    public var insideGlass: Bool {
        get { self[InsideGlassKey.self] }
        set { self[InsideGlassKey.self] = newValue }
    }

    public var glassIntensity: GlassIntensity {
        get { self[GlassIntensityKey.self] }
        set { self[GlassIntensityKey.self] = newValue }
    }

    /// 0.00–1.00 in Settings; higher means more see-through.
    public var glassTransparency: Double {
        get { self[GlassTransparencyKey.self] }
        set { self[GlassTransparencyKey.self] = newValue }
    }

    /// Near-solid surfaces and explicit borders; status never relies on colour alone.
    public var akzioHighContrast: Bool {
        get { self[HighContrastKey.self] }
        set { self[HighContrastKey.self] = newValue }
    }

    /// True below ~1400pt wide: sidebar collapses and inspectors become popovers.
    public var akzioCompactLayout: Bool {
        get { self[CompactLayoutKey.self] }
        set { self[CompactLayoutKey.self] = newValue }
    }

    /// Status must be readable without perceiving colour: glyph + word always.
    public var akzioColorIndependentStatus: Bool {
        get { self[ColorIndependentStatusKey.self] }
        set { self[ColorIndependentStatusKey.self] = newValue }
    }

    public var akzioReduceTransparencyOverride: Bool {
        get { self[ReduceTransparencyOverrideKey.self] }
        set { self[ReduceTransparencyOverrideKey.self] = newValue }
    }
}

struct GlassSurfaceModifier: ViewModifier {
    let level: GlassLevel
    let radius: CGFloat

    @Environment(\.insideGlass) private var insideGlass
    @Environment(\.glassIntensity) private var intensity
    @Environment(\.glassTransparency) private var transparency
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.akzioReduceTransparencyOverride) private var reduceTransparencyOverride
    @Environment(\.akzioHighContrast) private var highContrast
    @Environment(\.akzioRendersOffscreen) private var rendersOffscreen

    // computed property 只根据 level、intensity 和 transparency 选择原生 Glass；它不改变 content 或环境授权。
    private var liquidGlass: Glass {
        // 透明度只决定基础 material；elevated/modal 固定 regular，避免检查器因透明度过高失去边界。
        let variant: Glass
        switch level {
        case .base:
            // Keep low-transparency cards light, but add a frosted backdrop when
            // the user turns transparency up so underlying text does not remain
            // perfectly sharp through the surface.
            variant = transparency > 0.35 ? .regular : .clear
        case .elevated, .modal:
            variant = .regular
        }

        // A glass surface should remain lightly tinted even at the most
        // transparent setting; the native material supplies the Gaussian blur.
        let tintOpacity = max(0.28, min(1, 1 - transparency))
        return variant.tint(AkzioColor.raisedSurface.opacity(tintOpacity))
    }

    private var specularOpacity: Double {
        // 高光强度随层级和 blur boost 变化，但不参与语义状态判断。
        switch level {
        case .base: 0.025 + 0.01 * intensity.blurBoost
        case .elevated: 0.08
        case .modal: 0.12
        }
    }

    // body 输入 SwiftUI Content，输出 opaque fallback 或原生 glass surface；嵌套/无障碍条件只改变材质路径。
    func body(content: Content) -> some View {
        let shape = RoundedRectangle(cornerRadius: radius, style: .continuous)
        // Reduce Transparency, High Contrast, or glass-in-glass: fall back to an
        // opaque raised surface. Blur is never a carrier of meaning.
        let opaque = transparency <= 0.001
            || reduceTransparency
            || reduceTransparencyOverride
            || highContrast
            || rendersOffscreen
            || insideGlass
        // 任一无障碍/离屏/嵌套条件都会降级为不透明表面；这条分支不触碰 content 的数据。
        if opaque {
            #if DEBUG
            if insideGlass && !reduceTransparency {
                GlassAudit.warnNesting(level: level)
            }
            #endif
            // Spec: the opaque replacement is the #1C1C1C Raised Surface, with a
            // stronger border under High Contrast so the chrome edge stays legible.
            return AnyView(
                content
                .background(
                    shape.fill(
                        transparency <= 0.001
                            ? AkzioColor.elevatedSurface
                            : AkzioColor.raisedSurface
                    )
                )
                    .overlay(
                        shape.strokeBorder(
                            highContrast ? AkzioColor.primaryText.opacity(0.34) : AkzioColor.hairline,
                            lineWidth: highContrast ? 1 : AkzioLayout.hairlineWidth
                        )
                    )
                    .akzioShadow(level.shadow)
                    .environment(\.insideGlass, true)
            )
        }
        return AnyView(
            content
        // macOS 26's native Liquid Glass supplies the refraction, specular
                // edge and adaptive backdrop. Do not cover it with an opaque tint.
                .glassEffect(liquidGlass, in: shape)
                .overlay {
                    shape
                        .fill(
                            LinearGradient(
                                colors: [.white.opacity(specularOpacity), .clear],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .allowsHitTesting(false)
                }
                .overlay(alignment: .top) {
                    // Bright top edge: light catching the material.
                    shape
                        .strokeBorder(AkzioColor.glassTopEdge, lineWidth: AkzioLayout.hairlineWidth)
                }
                .akzioShadow(level.shadow)
                .environment(\.insideGlass, true)
        )
    }
}

private struct GlassBackdropModifier: ViewModifier {
    let tint: Color
    let radius: CGFloat

    @Environment(\.insideGlass) private var insideGlass
    @Environment(\.glassTransparency) private var transparency
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.akzioReduceTransparencyOverride) private var reduceTransparencyOverride
    @Environment(\.akzioHighContrast) private var highContrast
    @Environment(\.akzioRendersOffscreen) private var rendersOffscreen

    // surfaceOpacity 是 transparency 的反向显示值；它只服务于背景强度，不把用户设置写回 Store。
    private var surfaceOpacity: Double {
        max(0, min(1, 1 - transparency))
    }

    private var frostedSurfaceOpacity: Double {
        // Keep a small amount of tint behind the material so transparent cards
        // separate from dense content without becoming opaque panels.
        max(0.28, surfaceOpacity)
    }

    private var surfaceTint: Color {
        transparency <= 0.001 ? AkzioColor.elevatedSurface : tint
    }

    // body 只包裹背景：满足降级条件时填充实色，否则让 tint 与 regular glass 叠加形成背板。
    func body(content: Content) -> some View {
        let shape = RoundedRectangle(cornerRadius: radius, style: .continuous)

        // Backdrop 与完整 glass 不同：只包背景；嵌套或无障碍条件下不再叠加 material。
        content.background {
            if transparency <= 0.001
                || reduceTransparency
                || reduceTransparencyOverride
                || highContrast
                || rendersOffscreen
                || insideGlass
            {
                shape.fill(surfaceTint)
            } else {
                shape
                    .fill(surfaceTint.opacity(frostedSurfaceOpacity))
                    .glassEffect(.regular, in: shape)
            }
        }
    }
}

/// The SwiftUI window tint sits above the full-window AppKit desktop blur. The
/// actual Gaussian blur is installed by `WindowChromeConfigurator`, so this
/// layer only controls how strongly the blurred desktop is darkened.
private struct WindowBackdropModifier: ViewModifier {
    let tint: Color

    @Environment(\.glassTransparency) private var transparency
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.akzioReduceTransparencyOverride) private var reduceTransparencyOverride
    @Environment(\.akzioHighContrast) private var highContrast
    @Environment(\.akzioRendersOffscreen) private var rendersOffscreen

    // 窗口级 computed property 将透明度映射到 tint；系统 Reduce Transparency/High Contrast 仍由 body 强制覆盖。
    private var surfaceOpacity: Double {
        max(0, min(1, 1 - transparency))
    }

    private var surfaceTint: Color {
        transparency <= 0.001 ? AkzioColor.elevatedSurface : tint
    }

    private var desktopTintOpacity: Double {
        // The real desktop blur is supplied by NSVisualEffectView behind the
        // window. Let the material reveal soft shapes and colour from the
        // desktop, while keeping the tint strong enough that text does not
        // remain readable through the window.
        min(0.40, max(0.28, 0.18 + surfaceOpacity * 0.62))
    }

    // body 输出仅负责 SwiftUI tint；真正的桌面模糊由 AppKit 层提供，避免 View 重复实现渲染资源。
    func body(content: Content) -> some View {
        // 窗口背板只调节桌面 tint；真实模糊由 AppKit NSVisualEffectView 提供。
        content.background {
            if reduceTransparency || reduceTransparencyOverride || highContrast || rendersOffscreen {
                surfaceTint
            } else {
                surfaceTint.opacity(desktopTintOpacity)
            }
        }
    }
}

enum GlassAudit {
    nonisolated(unsafe) private static var reported = Set<String>()

    // 输入玻璃层级，输出无值；按层级去重 debug 警告，非isolated 状态只用于诊断而不影响 View 结果。
    static func warnNesting(level: GlassLevel) {
        let key = "\(level)"
        // reported 按层级去重，调试日志只提醒第一次降级，不改变渲染结果。
        guard !reported.contains(key) else { return }
        reported.insert(key)
        print("[Akzio] glass-in-glass blocked for \(key); degraded to opaque elevated surface.")
    }
}

extension View {
    // 三个入口把层级/颜色参数转成对应 ViewModifier；调用方只获得新的 some View，不接触 modifier 内部状态。
    public func akzioGlass(_ level: GlassLevel, radius: CGFloat = AkzioLayout.cardRadius) -> some View {
        modifier(GlassSurfaceModifier(level: level, radius: radius))
    }

    public func akzioGlassBackdrop(_ tint: Color, radius: CGFloat = 0) -> some View {
        modifier(GlassBackdropModifier(tint: tint, radius: radius))
    }

    public func akzioWindowBackdrop(_ tint: Color) -> some View {
        modifier(WindowBackdropModifier(tint: tint))
    }
}
