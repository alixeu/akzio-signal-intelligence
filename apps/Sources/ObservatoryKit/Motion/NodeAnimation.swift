import SwiftUI

// MARK: - Node motion
//
extension View {
    /// Soft one-shot bloom used for "completed" moments. Never loops.
    public func completionBloom(trigger: Int, tone: AkzioTone = .gold) -> some View {
        // trigger 由上层状态递增，tone 只决定光晕颜色；是否 reduced 交给 modifier 内的策略。
        modifier(CompletionBloom(trigger: trigger, tone: tone))
    }
}

private struct CompletionBloom: ViewModifier {
    // modifier 保持一次性 overlay，不保存额外触发状态，phaseAnimator 由 trigger 重播。
    let trigger: Int
    let tone: AkzioTone

    @Environment(\.motionPolicy) private var policy

    func body(content: Content) -> some View {
        // overlay 闭包捕获 content、tone 和 policy；命中测试始终穿透到原视图。
        content.overlay {
            Circle()
                .fill(tone.glow)
                .blur(radius: policy.isReduced ? 0 : 12)
                .phaseAnimator([0, 1], trigger: trigger) { view, step in
                    // 每一帧只改变尺度和透明度，reduced 下保留轻微完成提示而不放大移动。
                    view
                        .scaleEffect(policy.isReduced ? 1 : (step == 0 ? 0.85 : 1.35))
                        .opacity(step == 0 ? 0 : (policy.isReduced ? 0.25 : 0.55))
                } animation: { _ in policy.resolve(.easeOut(duration: 0.7)) }
                .allowsHitTesting(false)
        }
    }
}

// MARK: - Path growth

/// Draws a connection from source to target: line first, then the target lights up.
/// Parallel edges are offset by 40–80ms so they read as a fan, not a flash.
public struct PathGrowth: Sendable {
    // progress 和 isLit 是连接边的纯展示值，由外部时钟或 workflow 状态提供。
    public let progress: Double
    public let isLit: Bool

    public init(progress: Double, isLit: Bool) {
        // 初始化不做额外归一化，调用方保留路径动画阶段的原始语义。
        self.progress = progress
        self.isLit = isLit
    }

    public static func staggered(index: Int, elapsed: Double, policy: MotionPolicy) -> PathGrowth {
        // reduced 直接完成路径；完整模式按 index 延迟，每条边独立从 elapsed 计算进度。
        guard !policy.isReduced else { return PathGrowth(progress: 1, isLit: true) }
        let delay = Double(index) * 0.06
        let local = max(0, min((elapsed - delay) / 0.8, 1))
        return PathGrowth(progress: local, isLit: local >= 1)
    }
}
