import SwiftUI

// MARK: - Experience timeline
//
// Event → Decision → Outcome → Lesson, drawn left to right once on entry. The current
// node keeps an outer ring; hovering a node shows what caused it and where it came
// from. Canvas draws, overlay handles pointer and VoiceOver.
struct ExperienceTimelineCanvas: View {
    // nodes 是 Rust 产生的事件/Decision/Outcome/Lesson 投影；namespace 仅用于可选的共享元素动画。
    let nodes: [TimelineNodePresentation]
    let namespace: Namespace.ID?

    // appearTime 记录首次出现的动画起点，hovered 由 overlay 的交互闭包管理，不进入持久化数据。
    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.appLanguage) private var language
    @State private var appearTime: Double?
    @State private var hovered: String?

    var body: some View {
        // GeometryReader 把布局尺寸传给装饰 Canvas；AmbientCanvas 的时间闭包驱动一次进入动画和悬浮命中层。
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
                            // onAppear 捕获当前 time 作为 State 起点，只写入一次，避免重绘反复重置动画。
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
        // 关闭 ambient 或尚未记录 appearTime 时直接完成；否则将时间差限制在 0...1 的绘制进度。
        guard canvas.runsAmbient, let appearTime else { return 1 }
        return min(1, max(0, (now - appearTime) / 0.8))
    }

    private func point(_ node: TimelineNodePresentation, in size: CGSize) -> CGPoint {
        // node.position 是数据投影中的归一化横坐标，这里只根据画布尺寸转换为像素点。
        CGPoint(x: 34 + (size.width - 68) * node.position, y: size.height * 0.44)
    }

    private func draw(_ context: inout GraphicsContext, size: CGSize, progress: Double) {
        // draw 只负责 track、节点和文字的像素绘制；可访问名称和 tooltip 由 overlay 提供。
        let y = size.height * 0.44
        var track = Path()
        track.move(to: CGPoint(x: 22, y: y))
        track.addLine(to: CGPoint(x: 22 + (size.width - 44) * progress, y: y))
        context.stroke(track, with: .color(AkzioColor.gold(0.28)), style: StrokeStyle(lineWidth: 1.4, lineCap: .round))

        for node in nodes {
            let center = point(node, in: size)
            // 尚未到达 progress 的节点暂不绘制，保持动画轨道与节点出现顺序一致。
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
        // overlay 是真正的交互层：每个透明命中圆绑定一个 TooltipPopover、VoiceOver 文案和可选 shared element。
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
