import SwiftUI

// 文件职责：用单一 Path 绘制轻量 sparkline、分段进度条和连续比例条，供卡片内嵌使用。
// 数组和数值按值语义传入；View 只在 GeometryReader 闭包内计算局部点，不修改源数据。
// MARK: - Mini sparkline
//
// Drawn as a single `Path`, not a chart, so it is cheap enough to sit inside cards
// and to act as the shared element that expands into the full equity curve.
public struct MiniSparkline: View {
    // values 保留原始顺序，tone/显示开关是不可变绘制配置；空/短数组由 helper 显式降级为空 Path。
    private let values: [Double]
    private let tone: AkzioTone
    private let showsFill: Bool
    private let showsLatestPoint: Bool
    private let lineWidth: CGFloat

    public init(
        values: [Double],
        tone: AkzioTone = .gold,
        showsFill: Bool = true,
        showsLatestPoint: Bool = false,
        lineWidth: CGFloat = 1.4
    ) {
        // 初始化复制数组和绘制开关；后续 body 不向调用方回写 values。
        self.values = values
        self.tone = tone
        self.showsFill = showsFill
        self.showsLatestPoint = showsLatestPoint
        self.lineWidth = lineWidth
    }

    public var body: some View {
        // GeometryReader 闭包接收实际尺寸，复用同一组 normalized points 生成 fill、line 和 latest marker。
        // GeometryReader 给出当前卡片尺寸；所有点先归一化，再按同一组 points 生成填充、折线和最新点。
        GeometryReader { proxy in
            let points = normalizedPoints(in: proxy.size)
            ZStack {
                if showsFill, points.count > 1 {
                    fillPath(points, in: proxy.size)
                        .fill(
                            LinearGradient(
                                colors: [tone.color.opacity(0.22), tone.color.opacity(0.02)],
                                startPoint: .top,
                                endPoint: .bottom
                            )
                        )
                }
                linePath(points)
                    .stroke(tone.color, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round, lineJoin: .round))
                if showsLatestPoint, let last = points.last {
                    Circle()
                        .fill(tone.color)
                        .frame(width: 4, height: 4)
                        .position(last)
                        .shadow(color: tone.glow, radius: 4)
                }
            }
        }
        .drawingGroup(opaque: false)
        .accessibilityHidden(true)
    }

    private func normalizedPoints(in size: CGSize) -> [CGPoint] {
        // 输入容器尺寸，输出按最小/最大值归一化的点数组；不足两点时返回空数组而不是伪造线段。
        // 少于两个值无法形成线段；span 加最小值避免全相等数据除零，并保持点的原始顺序。
        guard values.count > 1 else { return [] }
        let minValue = values.min() ?? 0
        let maxValue = values.max() ?? 1
        let span = max(maxValue - minValue, 0.000_001)
        let stepX = size.width / CGFloat(values.count - 1)
        return values.enumerated().map { index, value in
            // map 闭包只把每个原始值转换为 CGPoint，index 保持横轴顺序，value 不被舍入或改写。
            let ratio = (value - minValue) / span
            return CGPoint(
                x: CGFloat(index) * stepX,
                y: size.height - CGFloat(ratio) * size.height
            )
        }
    }

    private func linePath(_ points: [CGPoint]) -> Path {
        // 输入归一化点借用，输出拥有独立 Path；Path builder 闭包不改变 points。
        Path { path in
            // Path 闭包内部只是消费已计算点，不修改源数组；空数组直接得到空路径。
            guard let first = points.first else { return }
            path.move(to: first)
            for point in points.dropFirst() {
                path.addLine(to: point)
            }
        }
    }

    private func fillPath(_ points: [CGPoint], in size: CGSize) -> Path {
        // 输入点和容器尺寸，输出从底边闭合的填充 Path；空点集合通过 guard 保持空路径。
        Path { path in
            // 填充路径从底边起步并在最后闭合，视觉上不会改变折线的上边界。
            guard let first = points.first, let last = points.last else { return }
            path.move(to: CGPoint(x: first.x, y: size.height))
            path.addLine(to: first)
            for point in points.dropFirst() {
                path.addLine(to: point)
            }
            path.addLine(to: CGPoint(x: last.x, y: size.height))
            path.closeSubpath()
        }
    }
}

// MARK: - Progress bar

/// Segmented progress used by the workflow strip and evidence completeness bars.
public struct SegmentedProgressBar: View {
    // completed/total 是整数值语义；total=0 仍由 body 保留一个占位段，避免布局消失。
    private let completed: Int
    private let total: Int
    private let tone: AkzioTone

    @Environment(\.motionPolicy) private var policy

    public init(completed: Int, total: Int, tone: AkzioTone = .gold) {
        // 初始化保存完成数量和 tone；动画策略由 Environment 在 body 中解析。
        self.completed = completed
        self.total = total
        self.tone = tone
    }

    public var body: some View {
        // ForEach 闭包按索引比较 completed，只决定每段填充色，不改变 total 或完成计数。
        // total 为 0 时仍保留一个空段，避免 Range(0..<0) 让进度条从布局中消失；completed 只控制已完成段。
        HStack(spacing: 3) {
            ForEach(0..<max(total, 1), id: \.self) { index in
                RoundedRectangle(cornerRadius: 2, style: .continuous)
                    .fill(index < completed ? tone.color : Color.white.opacity(0.08))
                    .frame(height: 6)
            }
        }
        .animation(policy.resolve(Motion.selection), value: completed)
        .accessibilityLabel("\(completed) of \(total) complete")
    }
}

/// Continuous bar used for actual-vs-target and completeness metrics.
public struct RatioBar: View {
    // fraction 为 Optional：nil 表示未知/不提供填充，非 nil 才会被夹紧到 0...1。
    private let fraction: Double?
    private let tone: AkzioTone
    private let height: CGFloat

    @Environment(\.motionPolicy) private var policy

    public init(fraction: Double?, tone: AkzioTone = .gold, height: CGFloat = 5) {
        // 初始化复制比例、色调和高度；不把缺失 fraction 转换为 0。
        self.fraction = fraction
        self.tone = tone
        self.height = height
    }

    public var body: some View {
        // GeometryReader 闭包按当前宽度计算填充；motion Environment 只控制 fraction 变化的动画。
        // fraction 缺失表示未知而非 0%，因此只画空轨道；有值时先夹紧再计算填充宽度。
        GeometryReader { proxy in
            ZStack(alignment: .leading) {
                Capsule().fill(Color.white.opacity(0.07))
                if let fraction {
                    Capsule()
                        .fill(tone.color)
                        .frame(width: max(2, proxy.size.width * min(max(fraction, 0), 1)))
                        .animation(ChartAnimation.barShift(policy), value: fraction)
                }
                // No fraction: the bar stays empty rather than implying 0%.
            }
        }
        .frame(height: height)
    }
}
