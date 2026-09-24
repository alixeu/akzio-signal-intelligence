import SwiftUI

// MARK: - Allocation flow
//
// Signal → Suggestion → Risk Check → Order → Fill. One light travels the active
// segment at a time (1.4–2.0s); the whole path never pulses at once.
struct AllocationFlowCanvas: View {
    // stages 是 Rust/Portfolio 投影的执行链；Canvas 只表达已到达状态，不代表新的订单或成交。
    let stages: [AllocationFlowStage]

    // canvas 决定环境动画是否运行，policy 保留统一的 motion 环境入口，language 用于阶段标签。
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    private var activeIndex: Int {
        // lastIndex 选择最近的 active stage；没有 active 时回退到 0，避免数组索引越界。
        max(0, (stages.lastIndex { $0.isActive } ?? 0))
    }

    var body: some View {
        // 上方 Canvas 是装饰，下方 stage row 是可访问的 ViewBuilder 等价投影。
        SectionCard(title: "Allocation Flow", subtitle: "Signal to fill") {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                AmbientCanvas { time in
                    // time 只传给绘制闭包驱动单段 traveling light，不改变 stages 数据。
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
                        // index 仅控制已到达/未到达的 opacity；stage 的业务状态仍由 isActive 提供。
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
        // draw 将阶段投影为连接线、节点和单个动画光点；所有可读信息由下方 row 负责。
        guard stages.count > 1 else { return }
        let y = size.height / 2
        let step = size.width / CGFloat(stages.count)
        let centers = (0..<stages.count).map { step * (CGFloat($0) + 0.5) }

        for index in 0..<(stages.count - 1) {
            // 每段根据下一个 stage 是否 active 决定实线或虚线，不对未到达阶段做推断。
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
        // activeIndex <= 0 或关闭 ambient 时保持静态节点，不启动异步光点动画。
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
