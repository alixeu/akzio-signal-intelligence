import SwiftUI

// MARK: - Section card

/// Standard opaque content container with an optional header and trailing accessory.
public struct SectionCard<Content: View, Accessory: View>: View {
    private let title: String?
    private let subtitle: String?
    private let content: Content
    private let accessory: Accessory
    private let padding: CGFloat
    @Environment(\.appLanguage) private var language

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

// MARK: - Count-up text

/// Digit-rolling readout with a fixed sign / currency slot.
public struct CountUpText: View {
    private let text: String
    private let value: Double
    private let size: CGFloat
    private let color: Color

    @Environment(\.motionPolicy) private var policy

    public init(_ text: String, value: Double, size: CGFloat = 20, color: Color = AkzioColor.primaryText) {
        self.text = text
        self.value = value
        self.size = size
        self.color = color
    }

    public var body: some View {
        Text(text)
            .akzioMetric(size, color: color)
            .akzioCountUp(value, policy: policy)
            .lineLimit(1)
    }
}
