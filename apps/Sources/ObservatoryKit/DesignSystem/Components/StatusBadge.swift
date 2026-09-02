import SwiftUI

// MARK: - Status badge
//
// Tone + symbol + label, always all three. Changing state morphs the badge
// (220–320ms) instead of swapping text.
public struct StatusBadge: View {
    public enum Size { case compact, regular }

    private let status: AkzioStatus
    private let size: Size
    private let showsLabel: Bool
    private let overrideLabel: String?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.akzioColorIndependentStatus) private var colorIndependent
    @Environment(\.appLanguage) private var language

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
        let style = status.style
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
    private let status: AkzioStatus
    private let diameter: CGFloat

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.appLanguage) private var language

    public init(_ status: AkzioStatus, diameter: CGFloat = 7) {
        self.status = status
        self.diameter = diameter
    }

    public var body: some View {
        let tone = status.style.tone
        Circle()
            .fill(tone == .gold && status == .succeeded ? AkzioColor.successDot : tone.color)
            .frame(width: diameter, height: diameter)
            .overlay {
                if status.isLive, canvas.runsAmbient {
                    AmbientCanvas { time in
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
    private let text: String
    private let tone: AkzioTone
    @Environment(\.appLanguage) private var language

    public init(_ text: String, tone: AkzioTone = .neutral) {
        self.text = text
        self.tone = tone
    }

    public var body: some View {
        Text(L10n.text(text, language: language))
            .font(AkzioFont.caption)
            .tracking(AkzioFont.captionTracking)
            .foregroundStyle(tone.color)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(Capsule().fill(tone.color.opacity(0.12)))
    }
}
