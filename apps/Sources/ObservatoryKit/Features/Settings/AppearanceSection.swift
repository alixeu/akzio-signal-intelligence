import SwiftUI

// 文件职责：把 language/theme/density/glass 的 SettingsPresentation Binding 映射为控件和值预览。
// 金色/珊瑚色 token 在这里仅展示设计约束；Glass/animation 由 Environment 和 token 统一提供。
// MARK: - Appearance
//
// Theme, density and glass. The accent pair is deliberately not editable: gold is
// the system's primary and coral is reserved for irreversible actions, so exposing
// an accent picker would let the user break the one rule the palette depends on.
struct AppearanceSection: View {
    // settings 是父级拥有的值语义设置；policy/language 由环境注入，分别控制过渡和文案。
    @Binding var settings: SettingsPresentation

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // body 中的 map 闭包把 CaseIterable 枚举转换成 SettingsSegmented 的 value/label 对，不创建独立状态。
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection("Language") {
                SettingsSegmented(
                    "Language",
                    selection: $settings.language,
                    options: AppLanguage.allCases.map { (value: $0, label: $0.displayName) }
                )
            }
            HairlineDivider()
            SettingsSection("Theme") {
                SettingsSegmented(
                    "Theme",
                    detail: "Three deep-grey variants; warmth only.",
                    selection: $settings.theme,
                    options: SettingsPresentation.Theme.allCases.map { (value: $0, label: $0.displayName) }
                )
                ThemePreviewTile(theme: settings.theme, glassTransparency: settings.glassTransparency)
            }
            HairlineDivider()
            SettingsSection("Accent", footnote: "Fixed by design: gold carries state, coral is reserved for actions that cannot be undone.") {
                HStack(spacing: AkzioLayout.s3) {
                    swatch("Primary", AkzioColor.primaryGold, "D4A15E")
                    swatch("Action", AkzioColor.actionCoral, "FF6B4A")
                    Spacer(minLength: 0)
                    PillTag("Locked", tone: .neutral)
                }
            }
            HairlineDivider()
            SettingsSection("Layout") {
                SettingsSegmented(
                    "Density",
                    detail: "Scales card padding and table row height.",
                    selection: $settings.density,
                    options: SettingsPresentation.Density.allCases.map { (value: $0, label: $0.displayName) }
                )
            }
            HairlineDivider()
            SettingsSection("Glass", footnote: "Liquid glass is used across cards, the status bar, inspectors and this panel.") {
                SettingsSegmented(
                    "Glass Highlight",
                    selection: $settings.glassIntensity,
                    options: GlassIntensity.allCases.map { (value: $0, label: $0.rawValue.capitalized) }
                )
                SettingsSlider(
                    "Transparency",
                    detail: "Higher reveals more of the desktop behind the window.",
                    value: $settings.glassTransparency,
                    range: 0.00...1.00
                )
            }
        }
        .animation(policy.resolve(Motion.glassDepth), value: settings.glassIntensity)
    }

    private func swatch(_ label: String, _ color: Color, _ hex: String) -> some View {
        // 输入文案、Color token 和十六进制说明，输出只读色板 View；不允许用户编辑强调色。
        HStack(spacing: 6) {
            RoundedRectangle(cornerRadius: 4, style: .continuous)
                .fill(color)
                .frame(width: 20, height: 20)
            VStack(alignment: .leading, spacing: 1) {
                Text(L10n.text(label, language: language)).akzioText(.label)
                Text("#\(hex)").akzioMono(9, color: AkzioColor.mutedText)
            }
        }
    }
}

// MARK: - Live theme preview

/// A miniature of the Overview centrepiece. It crossfades between themes rather
/// than re-laying-out, so the comparison reads as one surface changing colour.
struct ThemePreviewTile: View {
    // theme/transparency 是当前设置的只读投影；Canvas 预览不把任何绘制结果写回设置。
    let theme: SettingsPresentation.Theme
    let glassTransparency: Double

    @Environment(\.motionPolicy) private var policy

    private static let dotCount = 16

    var body: some View {
        // Canvas 闭包接收 GraphicsContext/尺寸并调用 draw；chrome 仍用同一 transparency 值展示材质反馈。
        ZStack {
            Canvas(rendersAsynchronously: false) { context, size in
                draw(&context, size: size)
            }
            // The chrome strip is what glass actually looks like at this setting.
            VStack {
                Spacer(minLength: 0)
                HStack(spacing: 4) {
                    ForEach(0..<5, id: \.self) { index in
                        Circle()
                            .fill(index == 2 ? AkzioColor.primaryGold : AkzioColor.mutedText)
                            .frame(width: 4, height: 4)
                    }
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 5)
                .background(
                    Capsule(style: .continuous)
                        .fill(.white.opacity(glassTransparency * 0.12))
                )
                .overlay(
                    Capsule(style: .continuous).strokeBorder(AkzioColor.hairline, lineWidth: 1)
                )
                .padding(.bottom, 8)
            }
        }
        .frame(height: 116)
        .frame(maxWidth: .infinity)
        .akzioGlassBackdrop(AkzioColor.background(for: theme), radius: AkzioLayout.cardRadius)
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: 1)
        )
        .clipShape(RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous))
        .animation(policy.resolve(Motion.themeCrossfade), value: theme)
        .accessibilityLabel("Theme preview: \(theme.displayName)")
        .accessibilityHint("Miniature of the Signal Universe using the selected theme.")
    }

    private func draw(_ context: inout GraphicsContext, size: CGSize) {
        // 输入可变绘制上下文和尺寸，输出无值；所有圆环/节点均由固定 token 和 index 计算，保持预览确定性。
        let centre = CGPoint(x: size.width / 2, y: size.height / 2 - 6)
        let radii: [CGFloat] = [22, 34, 46]
        for radius in radii {
            let rect = CGRect(
                x: centre.x - radius,
                y: centre.y - radius * 0.42,
                width: radius * 2,
                height: radius * 0.84
            )
            context.stroke(
                Path(ellipseIn: rect),
                with: .color(AkzioColor.primaryGold.opacity(0.10)),
                lineWidth: 0.5
            )
        }
        // Fixed angles: the preview is a still frame, never an animation loop.
        for index in 0..<Self.dotCount {
            // 循环按固定索引推导角度、半径和激活节点，不读取时间，也不创建 ambient Task。
            let fraction = Double(index) / Double(Self.dotCount)
            let angle = fraction * 2 * .pi
            let radius = radii[index % radii.count]
            let point = CGPoint(
                x: centre.x + cos(angle) * radius,
                y: centre.y + sin(angle) * radius * 0.42
            )
            let isActive = index == 5
            let diameter: CGFloat = isActive ? 7 : 4
            let rect = CGRect(
                x: point.x - diameter / 2,
                y: point.y - diameter / 2,
                width: diameter,
                height: diameter
            )
            let color = isActive
                ? AkzioColor.primaryGold
                : (index % 5 == 0 ? AkzioColor.secondaryText : AkzioColor.mutedText)
            context.fill(Path(ellipseIn: rect), with: .color(color))
            if isActive {
                context.stroke(
                    Path(ellipseIn: rect.insetBy(dx: -4, dy: -4)),
                    with: .color(AkzioColor.primaryGold.opacity(0.35)),
                    lineWidth: 1
                )
            }
        }
    }
}
