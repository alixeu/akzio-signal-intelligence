import SwiftUI

// MARK: - Allocation flow
//
// Signal → Suggestion → Risk Check → Order → Fill. One light travels the active
// segment at a time (1.4–2.0s); the whole path never pulses at once.
struct AllocationFlowCanvas: View {
    let stages: [AllocationFlowStage]

    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    private var activeIndex: Int {
        max(0, (stages.lastIndex { $0.isActive } ?? 0))
    }

    var body: some View {
        SectionCard(title: "Allocation Flow", subtitle: "Signal to fill") {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                AmbientCanvas { time in
                    Canvas(rendersAsynchronously: true) { context, size in
                        draw(&context, size: size, time: time)
                    }
                }
                .frame(height: 54)
                // Pure decoration: the labelled stage row below is the accessible
                // equivalent, so the drawing itself must not be announced.
                .accessibilityHidden(true)
                HStack(spacing: 0) {
                    ForEach(Array(stages.enumerated()), id: \.element.id) { index, stage in
                        VStack(spacing: 3) {
                            Image(systemName: stage.symbol)
                                .font(.system(size: 10, weight: .medium))
                                .foregroundStyle(stage.isActive ? AkzioColor.primaryGold : AkzioColor.mutedText)
                            Text(L10n.text(stage.title, language: language))
                                .akzioText(.caption, color: stage.isActive ? AkzioColor.secondaryText : AkzioColor.mutedText)
                                .lineLimit(1)
                                .minimumScaleFactor(0.8)
                        }
                        .frame(maxWidth: .infinity)
                        .accessibilityLabel(
                            "\(L10n.text(stage.title, language: language)): \(L10n.text(stage.isActive ? "Reached" : "Not Reached", language: language))"
                        )
                        .opacity(index <= activeIndex ? 1 : 0.55)
                    }
                }
            }
        }
    }

    private func draw(_ context: inout GraphicsContext, size: CGSize, time: Double) {
        guard stages.count > 1 else { return }
        let y = size.height / 2
        let step = size.width / CGFloat(stages.count)
        let centers = (0..<stages.count).map { step * (CGFloat($0) + 0.5) }

        for index in 0..<(stages.count - 1) {
            var path = Path()
            path.move(to: CGPoint(x: centers[index], y: y))
            path.addLine(to: CGPoint(x: centers[index + 1], y: y))
            let reached = stages[index + 1].isActive
            context.stroke(
                path,
                with: .color(reached ? AkzioColor.gold(0.42) : AkzioColor.mutedText.opacity(0.22)),
                style: StrokeStyle(lineWidth: 1.4, dash: reached ? [] : [3, 4])
            )
        }

        for (index, center) in centers.enumerated() {
            let reached = stages[index].isActive
            let radius: CGFloat = index == activeIndex ? 7 : 5
            context.fill(
                Path(ellipseIn: CGRect(x: center - radius, y: y - radius, width: radius * 2, height: radius * 2)),
                with: .color(reached ? AkzioColor.primaryGold : AkzioColor.mutedText.opacity(0.35))
            )
        }

        // Single travelling light on the segment that is currently completing.
        guard canvas.runsAmbient, activeIndex > 0 else { return }
        let from = centers[activeIndex - 1]
        let to = centers[activeIndex]
        let progress = (time / 1.7).truncatingRemainder(dividingBy: 1)
        let x = from + (to - from) * CGFloat(progress)
        let fade = sin(progress * .pi)
        context.fill(
            Path(ellipseIn: CGRect(x: x - 3, y: y - 3, width: 6, height: 6)),
            with: .color(AkzioColor.gold(0.85 * fade))
        )
    }
}
