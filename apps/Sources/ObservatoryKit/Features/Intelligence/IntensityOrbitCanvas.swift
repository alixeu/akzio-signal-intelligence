import SwiftUI

// MARK: - Reasoning intensity
//
// Intensity is expressed as orbit layers + core brightness + particle count.
struct IntensityOrbitCanvas: View {
    // intensity 是已解析的展示模型；画布只把它转换成亮度、轨道层数和粒子数量。
    @Environment(\.appLanguage) private var language
    let intensity: ReasoningIntensity

    // motionPolicy 与 canvasRenderPolicy 由 Environment 注入，分别控制过渡动画和环境粒子是否运行。
    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas

    var body: some View {
        VStack(spacing: AkzioLayout.s2) {
            // AmbientCanvas 的 time 闭包提供动画时钟；内部异步 Canvas 只绘制装饰，不承载可访问文本。
            AmbientCanvas { time in
                Canvas(rendersAsynchronously: true) { context, size in
                    draw(&context, size: size, time: time)
                }
            }
            .frame(height: 132)
            .accessibilityHidden(true)
            .animation(policy.resolve(.smooth(duration: 0.62)), value: intensity)

        HStack(spacing: 6) {
            Text(intensity.displayName).akzioText(.sectionTitle)
        }
        Text("\(intensity.orbitCount) orbit layers · configured per stage in Core Settings")
                .akzioText(.caption)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(L10n.text("Reasoning intensity", language: language)) \(intensity.displayName)")
    }

    private func draw(_ context: inout GraphicsContext, size: CGSize, time: Double) {
        // draw 接收 Canvas 的 inout context 和当前尺寸，把同一 intensity 投影为中心核心及椭圆轨道。
        let center = CGPoint(x: size.width / 2, y: size.height / 2)
        let span = min(size.width, size.height)
        let core = 8 + 5 * intensity.coreBrightness

        context.fill(
            Path(ellipseIn: CGRect(
                x: center.x - core * 2.6,
                y: center.y - core * 2.6,
                width: core * 5.2,
                height: core * 5.2
            )),
            with: .radialGradient(
                Gradient(colors: [
                    AkzioColor.gold(0.42 * intensity.coreBrightness),
                    .clear,
                ]),
                center: center,
                startRadius: 0,
                endRadius: core * 2.6
            )
        )
        context.fill(
            Path(ellipseIn: CGRect(x: center.x - core / 2, y: center.y - core / 2, width: core, height: core)),
            with: .color(AkzioColor.primaryGold.opacity(0.55 + 0.45 * intensity.coreBrightness))
        )

        for layer in 0..<intensity.orbitCount {
            // 每层按相对进度计算 rx/ry；最后一层可按模型要求使用 coral accent，其余保持 gold。
            let progress = Double(layer + 1) / Double(intensity.orbitCount + 1)
            let rx = span * 0.16 + span * 0.30 * progress
            let ry = rx * 0.62
            let rect = CGRect(x: center.x - rx, y: center.y - ry, width: rx * 2, height: ry * 2)
            let isCoralLayer = intensity.usesCoralAccent && layer == intensity.orbitCount - 1
            context.stroke(
                Path(ellipseIn: rect),
                with: .color(isCoralLayer ? AkzioColor.coral(0.34) : AkzioColor.gold(0.22)),
                lineWidth: 1
            )
            drawParticles(&context, center: center, rx: rx, ry: ry, layer: layer, time: time, coral: isCoralLayer)
        }
    }

    private func drawParticles(
        _ context: inout GraphicsContext,
        center: CGPoint,
        rx: CGFloat,
        ry: CGFloat,
        layer: Int,
        time: Double,
        coral: Bool
    ) {
        // canvas.scaled 负责按渲染策略缩放粒子预算；关闭 ambient 时 drift 为零，仍保留静态投影。
        let perLayer = canvas.scaled(2 + layer)
        guard perLayer > 0 else { return }
        for index in 0..<perLayer {
            let base = Double(index) / Double(perLayer) * 2 * .pi
            let drift = canvas.runsAmbient ? time / (Motion.ambientPeriod + Double(layer) * 2) * 2 * .pi : 0
            let angle = base + drift
            let x = center.x + CGFloat(cos(angle)) * rx
            let y = center.y + CGFloat(sin(angle)) * ry
            context.fill(
                Path(ellipseIn: CGRect(x: x - 1.6, y: y - 1.6, width: 3.2, height: 3.2)),
                with: .color(coral ? AkzioColor.coral(0.6) : AkzioColor.gold(0.62))
            )
        }
    }
}
