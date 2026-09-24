import SwiftUI

// 文件职责：提供带标题、主数值、变化值和可选副标题的 KPI 卡片，并把数值格式与 count-up 动效分开。
// 所有输入均按值传入；numericValue 只作为动画驱动，value/delta 是已经准备好的展示文本。
// MARK: - Section card

/// Standard opaque content container with an optional header and trailing accessory.
public struct SectionCard<Content: View, Accessory: View>: View {
    // 泛型 Content/Accessory 保留调用方 View 的静态类型；Optional 标题只控制 header 是否存在。
    private let title: String?
    private let subtitle: String?
    private let content: Content
    private let accessory: Accessory
    private let padding: CGFloat
    @Environment(\.appLanguage) private var language

    // @ViewBuilder 闭包在初始化时物化为值，body 后续只组合保存的 content/accessory，不重复调用 builder。
    public init(
        title: String? = nil,
        subtitle: String? = nil,
        padding: CGFloat = AkzioLayout.s4,
        @ViewBuilder content: () -> Content,
        @ViewBuilder accessory: () -> Accessory = { EmptyView() }
    ) {
        self.title = title
        self.subtitle = subtitle
        self.padding = padding
        self.content = content()
        self.accessory = accessory()
    }

    public var body: some View {
        // body 以语言 Environment 翻译标题，再按 Optional 选择 header；正文和 accessory 不被卡片重新解释。
        // SectionCard 只在有标题/副标题时占用 header 行；正文和 accessory 保持调用方传入的 View 类型。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            if title != nil || subtitle != nil {
                HStack(alignment: .firstTextBaseline, spacing: AkzioLayout.s2) {
                    VStack(alignment: .leading, spacing: 2) {
                        if let title {
                        Text(L10n.text(title, language: language)).akzioText(.sectionTitle)
                        }
                        if let subtitle {
                        Text(L10n.text(subtitle, language: language)).akzioText(.caption)
                        }
                    }
                    Spacer(minLength: AkzioLayout.s2)
                    accessory
                }
            }
            content
        }
        .akzioCard(padding: padding)
    }
}

// MARK: - Metric card

/// KPI tile: label, big monospaced-digit value, optional delta and sparkline.
public struct MetricCard: View {
    // value/delta/secondary 是只读文本，numericValue 是独立的 Double 动画输入；二者可以有不同精度而不互相覆盖。
    private let label: String
    private let value: String
    private let numericValue: Double
    private let delta: String?
    private let deltaTone: AkzioTone
    private let secondary: String?
    private let valueSize: CGFloat
    private let symbol: String?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    // 初始化复制所有展示参数；View 本身不从 numericValue 重新格式化 value。
    public init(
        label: String,
        value: String,
        numericValue: Double = 0,
        delta: String? = nil,
        deltaTone: AkzioTone = .gold,
        secondary: String? = nil,
        valueSize: CGFloat = 26,
        symbol: String? = nil
    ) {
        self.label = label
        self.value = value
        self.numericValue = numericValue
        self.delta = delta
        self.deltaTone = deltaTone
        self.secondary = secondary
        self.valueSize = valueSize
        self.symbol = symbol
    }

    public var body: some View {
        // body 从 language/motion Environment 读取策略；闭包式 delta.map 只拼接 accessibility 文案，不改变可见文本。
        // 数值显示、delta 和 secondary 都是只读投影；numericValue 只供动效使用，不改变展示字符串。
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 5) {
                if let symbol {
                    Image(systemName: symbol)
                        .font(.system(size: 9, weight: .medium))
                        .foregroundStyle(AkzioColor.mutedText)
                }
                Text(L10n.text(label, language: language)).akzioText(.caption)
            }
            Text(value)
                .akzioMetric(valueSize)
                .akzioCountUp(numericValue, policy: policy)
                .lineLimit(1)
                .minimumScaleFactor(0.7)
            HStack(spacing: 6) {
                if let delta {
                    Text(delta)
                        .font(AkzioFont.mono(11))
                        .foregroundStyle(deltaTone.color)
                        .akzioNumeric(delta, policy: policy)
                }
                if let secondary {
                    Text(L10n.text(secondary, language: language))
                        .font(AkzioFont.mono(11))
                        .foregroundStyle(AkzioColor.mutedText)
                }
            }
            .frame(height: 14)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(L10n.text(label, language: language)): \(value)\(delta.map { ", change \($0)" } ?? "")")
    }
}
