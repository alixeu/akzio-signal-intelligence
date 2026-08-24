import SwiftUI

// MARK: - Settings controls
//
// One row shape for the whole panel: label + optional explanation on the left,
// control on the right. Controls animate on their own value, never on layout, so
// nothing in the panel shifts while a slider is being dragged.
struct SettingsRow<Control: View>: View {
    private let title: String
    private let detail: String?
    private let control: Control
    @Environment(\.appLanguage) private var language

    init(_ title: String, detail: String? = nil, @ViewBuilder control: () -> Control) {
        self.title = title
        self.detail = detail
        self.control = control()
    }

    var body: some View {
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
    let title: String
    let detail: String?
    @Binding var isOn: Bool

    @Environment(\.motionPolicy) private var policy

    init(_ title: String, detail: String? = nil, isOn: Binding<Bool>) {
        self.title = title
        self.detail = detail
        _isOn = isOn
    }

    var body: some View {
        SettingsRow(title, detail: detail) {
            Button {
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
    let title: String
    let detail: String?
    @Binding var value: Double
    let range: ClosedRange<Double>
    let format: (Double) -> String

    @Environment(\.motionPolicy) private var policy

    init(
        _ title: String,
        detail: String? = nil,
        value: Binding<Double>,
        range: ClosedRange<Double>,
        format: @escaping (Double) -> String = { PpmFormatter.share(ppm: Int($0 * PpmFormatter.ppmPerUnit), fractionDigits: 0) }
    ) {
        self.title = title
        self.detail = detail
        _value = value
        self.range = range
        self.format = format
    }

    var body: some View {
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
    let title: String
    let detail: String?
    @Binding var selection: Value
    let options: [(value: Value, label: String)]

    @Namespace private var indicator
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

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
        SettingsRow(title, detail: detail) {
            HStack(spacing: 2) {
                ForEach(options, id: \.value) { option in
                    Button {
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
            .background(
                Capsule(style: .continuous).fill(AkzioColor.deepBackground)
            )
        }
    }
}

/// Section wrapper: title, hairline, staggered rows.
struct SettingsSection<Content: View>: View {
    private let title: String
    private let footnote: String?
    private let content: Content
    @Environment(\.appLanguage) private var language

    init(_ title: String, footnote: String? = nil, @ViewBuilder content: () -> Content) {
        self.title = title
        self.footnote = footnote
        self.content = content()
    }

    var body: some View {
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
