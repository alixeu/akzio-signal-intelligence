import SwiftUI

// MARK: - Experience timeline
//
// Event → Decision → Outcome → Lesson, drawn left to right once on entry. The current
// node keeps an outer ring; hovering a node shows what caused it and where it came
// from. Canvas draws, overlay handles pointer and VoiceOver.
struct ExperienceTimelineCanvas: View {
    let nodes: [TimelineNodePresentation]
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.appLanguage) private var language
    @State private var appearTime: Double?
    @State private var hovered: String?

    var body: some View {
        SectionCard(title: "Experience Timeline", subtitle: "\(nodes.count) \(L10n.text("events", language: language))") {
            if nodes.isEmpty {
                StatusExplanation(.waiting, detail: "No experience recorded for this run yet")
            } else {
                GeometryReader { proxy in
                    AmbientCanvas { time in
                        let progress = growth(now: time)
                        ZStack(alignment: .topLeading) {
                            Canvas(rendersAsynchronously: true) { context, size in
                                draw(&context, size: size, progress: progress)
                            }
                            // The node overlay carries the labels; the drawing is decoration.
                            .accessibilityHidden(true)
                            overlay(in: proxy.size)
                        }
                        .onAppear { if appearTime == nil { appearTime = time } }
                    }
                }
                .frame(height: 128)
            }
        }
    }

    private func growth(now: Double) -> Double {
        guard canvas.runsAmbient, let appearTime else { return 1 }
        return min(1, max(0, (now - appearTime) / 0.8))
    }

    private func point(_ node: TimelineNodePresentation, in size: CGSize) -> CGPoint {
        CGPoint(x: 34 + (size.width - 68) * node.position, y: size.height * 0.44)
    }

    private func draw(_ context: inout GraphicsContext, size: CGSize, progress: Double) {
        let y = size.height * 0.44
        var track = Path()
        track.move(to: CGPoint(x: 22, y: y))
        track.addLine(to: CGPoint(x: 22 + (size.width - 44) * progress, y: y))
        context.stroke(track, with: .color(AkzioColor.gold(0.28)), style: StrokeStyle(lineWidth: 1.4, lineCap: .round))

        for node in nodes {
            let center = point(node, in: size)
            guard center.x <= 22 + (size.width - 44) * progress + 6 else { continue }
            let radius: CGFloat = node.isCurrent ? 8 : 6
            if node.isCurrent {
                let outer = radius + 5
                context.stroke(
                    Path(ellipseIn: CGRect(x: center.x - outer, y: center.y - outer, width: outer * 2, height: outer * 2)),
                    with: .color(AkzioColor.gold(0.55)),
                    lineWidth: 1.2
                )
            }
            context.fill(
                Path(ellipseIn: CGRect(x: center.x - radius, y: center.y - radius, width: radius * 2, height: radius * 2)),
                with: .color(node.kind.tone.color.opacity(node.isCurrent ? 0.95 : 0.6))
            )
            var label = context.resolve(
                Text(node.label)
                    .font(AkzioFont.caption)
                    .foregroundColor(node.isCurrent ? AkzioColor.primaryText : AkzioColor.secondaryText)
            )
            label.shading = .color(node.isCurrent ? AkzioColor.primaryText : AkzioColor.secondaryText)
            context.draw(label, at: CGPoint(x: center.x, y: center.y + radius + 11))

            var date = context.resolve(
                Text(node.dateLabel).font(AkzioFont.caption).foregroundColor(AkzioColor.mutedText)
            )
            date.shading = .color(AkzioColor.mutedText)
            context.draw(date, at: CGPoint(x: center.x, y: center.y - radius - 10))
        }
    }

    private func overlay(in size: CGSize) -> some View {
        ZStack(alignment: .topLeading) {
            ForEach(nodes) { node in
                let center = point(node, in: size)
                TooltipPopover {
                    Circle()
                        .fill(Color.white.opacity(0.001))
                        .frame(width: 26, height: 26)
                } content: {
                    VStack(alignment: .leading, spacing: 3) {
                        Text(L10n.text(node.kind.displayName, language: language)).akzioText(.caption)
                        Text(node.label).akzioText(.bodySmall, color: AkzioColor.primaryText)
                        Text(node.detail).akzioText(.bodySmall)
                        Text("\(L10n.text("Source", language: language)): \(node.dateLabel) \(L10n.text("session", language: language))").akzioMono(10, color: AkzioColor.mutedText)
                    }
                    .frame(maxWidth: 220, alignment: .leading)
                }
                .position(center)
                .accessibilityLabel("\(L10n.text(node.kind.displayName, language: language)): \(node.label)")
                .accessibilityValue(node.detail)
                .sharedElement(
            node.kind == .lesson ? .learningNode : .noSharedElement,
                    in: node.kind == .lesson ? namespace : nil
                )
            }
        }
    }
}
