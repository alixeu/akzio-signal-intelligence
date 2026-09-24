import SwiftUI

// 文件职责：把 AkzioStatus 的 tone、symbol、label 渲染成 badge、dot 或只读 pill。
// 状态语义来自 StatusSemantics；组件只读取 Environment 的语言/动效/无障碍策略，不从颜色反推状态。
// MARK: - Status badge
//
// Tone + symbol + label, always all three. Changing state morphs the badge
// (220–320ms) instead of swapping text.
public struct StatusBadge: View {
    // Size 和显示开关是值语义配置；overrideLabel 只替换文案，不改变底层 status。
    public enum Size { case compact, regular }

    private let status: AkzioStatus
    private let size: Size
    private let showsLabel: Bool
    private let overrideLabel: String?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.akzioColorIndependentStatus) private var colorIndependent
    @Environment(\.appLanguage) private var language

    // 初始化复制 status 和展示偏好；调用方传入的 label 仍是 Optional，不会伪造新的状态枚举。
    public init(
        _ status: AkzioStatus,
        size: Size = .regular,
        showsLabel: Bool = true,
        label: String? = nil
    ) {
        self.status = status
        self.size = size
        self.showsLabel = showsLabel
        self.overrideLabel = label
    }

    public var body: some View {
        // body 从 status.style 得到成套语义，再按 colorIndependent policy 决定文字是否必须出现。
        let style = status.style
        // style 同时提供图标、颜色和默认文案；无障碍策略开启时即使 showsLabel=false 仍显示文字。
        HStack(spacing: size == .compact ? 4 : 5) {
            Image(systemName: style.symbol)
                .font(.system(size: size == .compact ? 9 : 10, weight: .semibold))
                .symbolRenderingMode(.hierarchical)
            // Colour is never the only carrier: with the accessibility rule on, the
            // word is shown even where a caller asked for the glyph alone.
            if showsLabel || colorIndependent {
                Text(L10n.text(overrideLabel ?? style.label, language: language))
                    .font(.system(size: size == .compact ? 10 : 11, weight: .medium))
                    .tracking(0.2)
                    .lineLimit(1)
            }
        }
        .foregroundStyle(style.color)
        .padding(.horizontal, size == .compact ? 6 : 8)
        .padding(.vertical, size == .compact ? 2.5 : 4)
        .background(
            Capsule(style: .continuous)
                .fill(style.color.opacity(0.10))
        )
        .overlay(
            Capsule(style: .continuous)
                .strokeBorder(style.color.opacity(0.28), lineWidth: AkzioLayout.hairlineWidth)
        )
        .contentTransition(.symbolEffect(.replace))
        .animation(policy.resolve(Motion.badge), value: status)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(L10n.text(overrideLabel ?? style.label, language: language))
    }
}

// MARK: - Status dot

/// The smallest status affordance. A live status breathes slowly; terminal states
/// stay put. Never a fast blink.
public struct StatusDot: View {
    // dot 是 status 的最小图形投影；diameter 仅影响几何尺寸，不改变 tone 或 live 判断。
    private let status: AkzioStatus
    private let diameter: CGFloat

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.appLanguage) private var language

    // 初始化保存状态和直径；ambient 是否运行由 canvas policy 在 body 中决定。
    public init(_ status: AkzioStatus, diameter: CGFloat = 7) {
        self.status = status
        self.diameter = diameter
    }

    public var body: some View {
        // body 用 style.tone 选择填充；AmbientCanvas 闭包只依据 Timeline 时间绘制呼吸环。
        let tone = status.style.tone
        // 只有 live 状态且全局允许 ambient 时绘制呼吸环；终态圆点保持静止。
        Circle()
            .fill(tone == .gold && status == .succeeded ? AkzioColor.successDot : tone.color)
            .frame(width: diameter, height: diameter)
            .overlay {
                if status.isLive, canvas.runsAmbient {
                    AmbientCanvas { time in
                        // 时间闭包不写回 status，只把周期时间映射到透明度和 scale。
                        let phase = (time.truncatingRemainder(dividingBy: Motion.pulsePeriod)) / Motion.pulsePeriod
                        let eased = 0.5 - 0.5 * cos(phase * 2 * .pi)
                        Circle()
                            .stroke(tone.color.opacity(0.5 - 0.35 * eased), lineWidth: 1)
                            .scaleEffect(1 + 0.7 * eased)
                    }
                }
            }
            .animation(policy.resolve(Motion.badge), value: status)
        .accessibilityLabel(L10n.text(status.style.label, language: language))
    }
}

// MARK: - Pill tag

public struct PillTag: View {
    // PillTag 是不可变、无交互的语义标签；tone 只负责展示颜色，文案仍由 language Environment 翻译。
    private let text: String
    private let tone: AkzioTone
    @Environment(\.appLanguage) private var language

    // 初始化复制文本和 tone；没有 Binding/Action，因此不会向设置或 Store 回写。
    public init(_ text: String, tone: AkzioTone = .neutral) {
        self.text = text
        self.tone = tone
    }

    public var body: some View {
        // body 输出单一 Text + Capsule 背景，不创建 Button 或 hover 状态。
        // PillTag 是只读语义标签，不引入 Button 或 hover 状态。
        Text(L10n.text(text, language: language))
            .font(AkzioFont.caption)
            .tracking(AkzioFont.captionTracking)
            .foregroundStyle(tone.color)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(Capsule().fill(tone.color.opacity(0.12)))
    }
}
