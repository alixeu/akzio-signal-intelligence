import SwiftUI

// MARK: - Mini sparkline
//
// Drawn as a single `Path`, not a chart, so it is cheap enough to sit inside cards
// and to act as the shared element that expands into the full equity curve.
public struct MiniSparkline: View {
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
        self.values = values
        self.tone = tone
        self.showsFill = showsFill
        self.showsLatestPoint = showsLatestPoint
        self.lineWidth = lineWidth
    }

    public var body: some View {
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
        guard values.count > 1 else { return [] }
        let minValue = values.min() ?? 0
        let maxValue = values.max() ?? 1
        let span = max(maxValue - minValue, 0.000_001)
        let stepX = size.width / CGFloat(values.count - 1)
        return values.enumerated().map { index, value in
            let ratio = (value - minValue) / span
            return CGPoint(
                x: CGFloat(index) * stepX,
                y: size.height - CGFloat(ratio) * size.height
            )
        }
    }

    private func linePath(_ points: [CGPoint]) -> Path {
        Path { path in
            guard let first = points.first else { return }
            path.move(to: first)
            for point in points.dropFirst() {
                path.addLine(to: point)
            }
        }
    }

    private func fillPath(_ points: [CGPoint], in size: CGSize) -> Path {
        Path { path in
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
    private let completed: Int
    private let total: Int
    private let tone: AkzioTone

    @Environment(\.motionPolicy) private var policy

    public init(completed: Int, total: Int, tone: AkzioTone = .gold) {
        self.completed = completed
        self.total = total
        self.tone = tone
    }

    public var body: some View {
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
    private let fraction: Double?
    private let tone: AkzioTone
    private let height: CGFloat

    @Environment(\.motionPolicy) private var policy

    public init(fraction: Double?, tone: AkzioTone = .gold, height: CGFloat = 5) {
        self.fraction = fraction
        self.tone = tone
        self.height = height
    }

    public var body: some View {
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
