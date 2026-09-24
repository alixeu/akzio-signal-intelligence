import SwiftUI

// 文件职责：把可选进度和风险数值绘制成环形/半弧形 View，并把缺失值与动效策略显式化。
// 输入是值语义的数值、范围和内容 builder；Environment 只提供 motion/canvas policy，不成为业务状态源。
// MARK: - Progress ring
//
// Stroke-length growth (700–1000ms). A `nil` progress means "no data yet" and
// renders as a dashed low-brightness ring — never a 0% ring.
public struct ProgressRing<Center: View>: View {
    // Center 是调用方提供的 View 值；progress 使用 Optional 区分“无数据”与真正的 0%。
    private let progress: Double?
    private let tone: AkzioTone
    private let lineWidth: CGFloat
    private let diameter: CGFloat
    private let isBreathing: Bool
    private let center: Center

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @State private var completionTick = 0

    // 初始化复制绘制参数并立即执行 center builder；闭包只负责构造中心 View，不在此处读取外部状态。
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
        // body 读取 canvas/motion Environment；completionTick 是本地一次性动画触发器，绘制结果仍由 progress 决定。
        // nil progress 只显示虚线轨道；有值时才消费 progress 画 trim，完成阈值另行触发一次 bloom。
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
                    // AmbientCanvas 闭包只用时间和不可变 tone/lineWidth 计算呼吸环，不写回业务值。
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
            // onChange 闭包只在新值进入完成区间时递增本地 tick；Optional nil 不会被解释成完成或 0%。
            // 只有跨入 100% 才递增 completionTick，避免同一完成值重复播放。
            if let new, new >= 1 { completionTick += 1 }
        }
        .accessibilityElement(children: .combine)
    }
}

// MARK: - Risk gauge

/// Half-arc gauge with a needle. The rim turns coral once the value enters the
/// risk band, so the state is never colour-only — the needle position says it too.
public struct RiskGauge: View {
    // value/bounds/riskThreshold 是不可变输入；caption 为 Optional，仅在调用方提供时渲染。
    private let value: Double
    private let bounds: ClosedRange<Double>
    private let riskThreshold: Double
    private let label: String
    private let caption: String?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    // 初始化保存风险范围和文案；View 重算仍由这些值和 Environment 决定，不持有可变业务模型。
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
        // computed property 将 value 映射到 0...1；退化 bounds 直接返回 0，避免除零和越界指针。
        let span = bounds.upperBound - bounds.lowerBound
        // 退化区间不参与除法；正常区间把 value 夹到 0...1，needle 与弧线使用同一比例。
        guard span > 0 else { return 0 }
        return min(max((value - bounds.lowerBound) / span, 0), 1)
    }

    // computed property 只比较原始 value 与阈值；它与 normalized 分别驱动颜色和几何位置。
    private var isRisky: Bool { value >= riskThreshold }

    public var body: some View {
        // body 同时渲染轨道、填充、指针和本地化文案；风险语义由位置与 tone 共同表达。
        // 风险颜色和指针位置同时反映阈值，避免仅靠颜色传递风险语义。
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
