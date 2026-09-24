import SwiftUI

// 文件职责：提供 Settings 面板共用的 row、toggle、slider、segmented 和 section 容器。
// 控件通过 Binding 回写父级值；ViewBuilder/formatter 闭包只负责构造或格式化，不越过 Store 直接持久化。
// MARK: - Settings controls
//
// One row shape for the whole panel: label + optional explanation on the left,
// control on the right. Controls animate on their own value, never on layout, so
// nothing in the panel shifts while a slider is being dragged.
struct SettingsRow<Control: View>: View {
    // Control 是调用方提供的值语义 View；title/detail 是本地化前的稳定 key。
    private let title: String
    private let detail: String?
    private let control: Control
    @Environment(\.appLanguage) private var language

    // @ViewBuilder 闭包在 init 时物化 control，body 只负责统一左右布局和 Optional detail。
    init(_ title: String, detail: String? = nil, @ViewBuilder control: () -> Control) {
        self.title = title
        self.detail = detail
        self.control = control()
    }

    var body: some View {
        // body 读取 language Environment 翻译文案；detail 为 nil 时不创建第二行，也不影响 control 尺寸。
        HStack(alignment: .firstTextBaseline, spacing: AkzioLayout.s3) {
            VStack(alignment: .leading, spacing: 2) {
                Text(L10n.text(title, language: language)).akzioText(.body)
                if let detail {
                    Text(L10n.text(detail, language: language))
                        .akzioText(.caption)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
            Spacer(minLength: AkzioLayout.s3)
            control
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

/// A switch whose knob springs (180–240ms) and whose track colour crossfades.
struct SettingsToggle: View {
    // isOn 是父级 Bool 的 Binding；本 View 只发出 toggle 写入，不复制一份独立开关状态。
    let title: String
    let detail: String?
    @Binding var isOn: Bool

    @Environment(\.motionPolicy) private var policy

    // 初始化保留 Binding 连接；property wrapper 的 _isOn 让 body 的 Button 直接写回调用方。
    init(_ title: String, detail: String? = nil, isOn: Binding<Bool>) {
        self.title = title
        self.detail = detail
        _isOn = isOn
    }

    var body: some View {
        // Button action 闭包调用 Binding.toggle；animation 只观察值变化，accessibility 同步表达 On/Off。
        SettingsRow(title, detail: detail) {
            Button {
                // 该闭包只更新绑定值，持久化或副作用由外层 Store/调用方处理。
                isOn.toggle()
            } label: {
                ZStack(alignment: isOn ? .trailing : .leading) {
                    Capsule(style: .continuous)
                        .fill(isOn ? AkzioColor.primaryGold.opacity(0.85) : AkzioColor.hairline)
                        .frame(width: 34, height: 18)
                    Circle()
                        .fill(isOn ? AkzioColor.deepBackground : AkzioColor.secondaryText)
                        .frame(width: 14, height: 14)
                        .padding(2)
                }
                .contentShape(Capsule(style: .continuous))
            }
            .buttonStyle(.plain)
            .animation(policy.resolve(Motion.toggle), value: isOn)
            .accessibilityAddTraits(.isToggle)
            .accessibilityValue(isOn ? "On" : "Off")
        }
        .accessibilityElement(children: .combine)
    }
}

/// Value tracks the drag continuously; the readout retargets over 120–180ms so it
/// never lags the thumb, and the debounced value drives expensive previews only.
struct SettingsSlider: View {
    // value 是 Double Binding，range 是值域，format 是调用方提供的展示闭包；拖动值与昂贵预览分层处理。
    let title: String
    let detail: String?
    @Binding var value: Double
    let range: ClosedRange<Double>
    let format: (Double) -> String

    @Environment(\.motionPolicy) private var policy

    // 默认 format 把 0...1 转为百分比 share；自定义闭包可保留同一 Double 输入而选择不同文案。
    init(
        _ title: String,
        detail: String? = nil,
        value: Binding<Double>,
        range: ClosedRange<Double>,
        // 默认 formatter 闭包只负责把 UI fraction 转成展示百分比，不参与 Binding 写入。
        format: @escaping (Double) -> String = { PpmFormatter.share(ppm: Int($0 * PpmFormatter.ppmPerUnit), fractionDigits: 0) }
    ) {
        self.title = title
        self.detail = detail
        _value = value
        self.range = range
        self.format = format
    }

    var body: some View {
        // body 用 format(value) 生成回显，Slider 直接写 $value；动画只平滑文本变化，不延迟真实 Binding。
        VStack(alignment: .leading, spacing: 5) {
            SettingsRow(title, detail: detail) {
                Text(format(value))
                    .akzioMono(11, color: AkzioColor.primaryText)
                    .animation(policy.resolve(.smooth(duration: 0.15)), value: value)
                    .frame(width: 62, alignment: .trailing)
            }
            Slider(value: $value, in: range)
                .controlSize(.small)
                .tint(AkzioColor.primaryGold)
                .accessibilityValue(format(value))
        }
    }
}

/// Segmented picker with a sliding gold background (220–320ms) rather than a
/// hard-swapped selection fill.
struct SettingsSegmented<Value: Hashable>: View {
    // 泛型 Value 只需 Hashable 以支撑 ForEach identity；selection Binding 仍由父级拥有真实值。
    let title: String
    let detail: String?
    @Binding var selection: Value
    let options: [(value: Value, label: String)]

    @Namespace private var indicator
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    // 初始化保存标题/选项并接入 Binding；options 的 label 是本地化 key，Value 保持原始类型。
    init(
        _ title: String,
        detail: String? = nil,
        selection: Binding<Value>,
        options: [(value: Value, label: String)]
    ) {
        self.title = title
        self.detail = detail
        _selection = selection
        self.options = options
    }

    var body: some View {
        // body 闭包比较 option.value 与 selection；点击动画闭包只写回 Binding，matched geometry 负责视觉移动。
        SettingsRow(title, detail: detail) {
            HStack(spacing: 2) {
                ForEach(options, id: \.value) { option in
                    // ForEach 闭包借用单个 option，选中判断不创建本地副本状态。
                    Button {
                        // withAnimation 闭包提交新的 Value；父级 Binding 是选择状态的唯一所有者。
                        withAnimation(policy.resolve(Motion.highlight)) { selection = option.value }
                    } label: {
                        Text(L10n.text(option.label, language: language))
                            .akzioText(
                                .label,
                                color: option.value == selection ? AkzioColor.deepBackground : AkzioColor.secondaryText
                            )
                            .padding(.horizontal, AkzioLayout.s2)
                            .frame(height: 22)
                            .background {
                                if option.value == selection {
                                    Capsule(style: .continuous)
                                        .fill(AkzioColor.primaryGold)
                                        .matchedGeometryEffect(id: "segment", in: indicator)
                                }
                            }
                            .contentShape(Capsule(style: .continuous))
                    }
                    .buttonStyle(.plain)
                    .accessibilityAddTraits(option.value == selection ? [.isSelected] : [])
                }
            }
            .padding(2)
        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 99)
        }
    }
}

/// Section wrapper: title, hairline, staggered rows.
struct SettingsSection<Content: View>: View {
    // Content 是调用方 ViewBuilder 生成的值；footnote Optional 控制说明行是否存在。
    private let title: String
    private let footnote: String?
    private let content: Content
    @Environment(\.appLanguage) private var language

    // init 物化 content 值；section 不保存独立设置状态，所有可变数据由嵌套控件的 Binding 管理。
    init(_ title: String, footnote: String? = nil, @ViewBuilder content: () -> Content) {
        self.title = title
        self.footnote = footnote
        self.content = content()
    }

    var body: some View {
        // body 统一标题/内容/可选 footnote 的垂直布局，并通过 language Environment 翻译两个文案字段。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            Text(L10n.text(title, language: language)).akzioText(.caption)
            content
            if let footnote {
                Text(L10n.text(footnote, language: language))
                    .akzioText(.caption, color: AkzioColor.mutedText)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
