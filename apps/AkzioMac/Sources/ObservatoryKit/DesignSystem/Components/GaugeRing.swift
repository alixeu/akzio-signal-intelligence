import SwiftUI

// MARK: - Progress ring
//
// Stroke-length growth (700–1000ms). A `nil` progress means "no data yet" and
// renders as a dashed low-brightness ring — never a 0% ring.
public struct ProgressRing<Center: View>: View {
    private let progress: Double?
    private let tone: AkzioTone
    private let lineWidth: CGFloat
    private let diameter: CGFloat
    private let isBreathing: Bool
    private let center: Center

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @State private var completionTick = 0

    public init(
        progress: Double?,
        tone: AkzioTone = .gold,
        lineWidth: CGFloat = 6,
        diameter: CGFloat = 120,
        isBreathing: Bool = false,
        @ViewBuilder center: () -> Center = { EmptyView() }
    ) {
        self.progress = progress
        self.tone = tone
        self.lineWidth = lineWidth
        self.diameter = diameter
        self.isBreathing = isBreathing
        self.center = center()
    }

    public var body: some View {
        ZStack {
            // Track
            Circle()
                .strokeBorder(
                    style: StrokeStyle(
                        lineWidth: lineWidth,
                        dash: progress == nil ? [3, 5] : []
                    )
                )
                .foregroundStyle(
                    progress == nil ? AkzioColor.mutedText.opacity(0.45) : Color.white.opacity(0.05)
                )

            if let progress {
                Circle()
                    .trim(from: 0, to: max(0.001, progress))
                    .stroke(
                        tone.color,
                        style: StrokeStyle(lineWidth: lineWidth, lineCap: .round)
                    )
                    .rotationEffect(.degrees(-90))
                    .shadow(color: tone.glow, radius: progress >= 1 ? 8 : 4)
                    .animation(ChartAnimation.ringProgress(policy), value: progress)
            }

            // Observing state breathes slowly; at most one per screen by construction.
            if isBreathing, canvas.runsAmbient, progress != nil {
                AmbientCanvas { time in
                    let phase = (time.truncatingRemainder(dividingBy: Motion.observingPeriod)) / Motion.observingPeriod
                    let eased = 0.5 - 0.5 * cos(phase * 2 * .pi)
                    Circle()
                        .strokeBorder(tone.color.opacity(0.10 + 0.12 * eased), lineWidth: lineWidth * 1.6)
                        .scaleEffect(1.02 + 0.02 * eased)
                }
            }

            center
        }
        .frame(width: diameter, height: diameter)
        .completionBloom(trigger: completionTick, tone: tone)
        .onChange(of: progress) { _, new in
            if let new, new >= 1 { completionTick += 1 }
        }
        .accessibilityElement(children: .combine)
    }
}

// MARK: - Risk gauge

/// Half-arc gauge with a needle. The rim turns coral once the value enters the
/// risk band, so the state is never colour-only — the needle position says it too.
public struct RiskGauge: View {
    private let value: Double
    private let bounds: ClosedRange<Double>
    private let riskThreshold: Double
    private let label: String
    private let caption: String?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    public init(
        value: Double,
        bounds: ClosedRange<Double> = 0...2,
        riskThreshold: Double = 1.5,
        label: String,
        caption: String? = nil
    ) {
        self.value = value
        self.bounds = bounds
        self.riskThreshold = riskThreshold
        self.label = label
        self.caption = caption
    }

    private var normalized: Double {
        let span = bounds.upperBound - bounds.lowerBound
        guard span > 0 else { return 0 }
        return min(max((value - bounds.lowerBound) / span, 0), 1)
    }

    private var isRisky: Bool { value >= riskThreshold }

    public var body: some View {
        VStack(spacing: 4) {
            ZStack {
                Circle()
                    .trim(from: 0, to: 0.5)
                    .stroke(Color.white.opacity(0.06), style: StrokeStyle(lineWidth: 6, lineCap: .round))
                Circle()
                    .trim(from: 0, to: 0.5 * normalized)
                    .stroke(
                        isRisky ? AkzioColor.actionCoral : AkzioColor.primaryGold,
                        style: StrokeStyle(lineWidth: 6, lineCap: .round)
                    )
                    .animation(policy.resolve(Motion.gauge), value: normalized)
                Rectangle()
                    .fill(isRisky ? AkzioColor.actionCoral : AkzioColor.primaryGold)
                    .frame(width: 1.5, height: 26)
                    .offset(y: -13)
                    .rotationEffect(.degrees(-90 + 180 * normalized))
                    .animation(policy.resolve(Motion.gauge), value: normalized)
                Text(PpmFormatter.signPrefix(0) == "±" ? String(format: "%.2f", value) : "")
                    .akzioMono(11, color: AkzioColor.primaryText)
                    .offset(y: 14)
            }
            .rotationEffect(.degrees(180))
            .frame(width: 74, height: 42)
            Text(L10n.text(label, language: language)).akzioText(.caption)
        if let caption {
            Text(L10n.text(caption, language: language)).akzioMono(10, color: AkzioColor.mutedText)
        }
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(
            "\(L10n.text(label, language: language)) \(String(format: "%.2f", value))"
        )
    }
}
