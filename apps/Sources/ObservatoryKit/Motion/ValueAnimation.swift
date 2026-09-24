import SwiftUI

// MARK: - Numeric motion
//
// Digits roll; the sign slot, currency symbol and decimal places never move.
// A whole-string cross-fade is treated as a defect.
extension View {
    /// Digit-rolling transition for any monospaced-digit readout.
    public func akzioNumeric<V: Equatable>(_ value: V, policy: MotionPolicy) -> some View {
        // 泛型 value 只作为 contentTransition 的变化标识，动画本身统一经过 policy.resolve。
        self
            .contentTransition(.numericText())
            .animation(policy.resolve(Motion.numeric), value: value)
    }

    /// Interpolating variant for continuous counters (equity, P&L).
    public func akzioCountUp(_ value: Double, policy: MotionPolicy) -> some View {
        // 连续数值使用传入 Double 插值，适合 equity/P&L 等会随快照变化的计数。
        self
            .contentTransition(.numericText(value: value))
            .animation(policy.resolve(Motion.numeric), value: value)
    }
}

// MARK: - Chart motion

public enum ChartAnimation {
    /// Range switches morph the path; in-place refreshes only extend the tail.
    public static func rangeMorph(_ policy: MotionPolicy) -> Animation {
        // 范围切换和尾部追加使用不同 token，调用方不直接依赖 duration 数值。
        policy.resolve(.smooth(duration: 0.6))
    }

    public static func tailAppend(_ policy: MotionPolicy) -> Animation {
        // 原地刷新只延展曲线尾部，仍保留 reduced policy 的统一降级入口。
        policy.resolve(.smooth(duration: 0.35))
    }

    /// Bars/ratios slide from their old value instead of snapping.
    public static func barShift(_ policy: MotionPolicy) -> Animation {
        // 比例条从旧值移动到新值，而不是瞬间替换；数值由调用方提供。
        policy.resolve(.smooth(duration: 0.42))
    }

    /// Ring stroke-length growth for T+N progress.
    public static func ringProgress(_ policy: MotionPolicy) -> Animation {
        // Outcome ring 的 stroke 长度沿同一策略变化，缺失进度不在这里补值。
        policy.resolve(Motion.ring)
    }
}

// MARK: - Staggered reveal

/// Short travel + fade, offset per row. Used by lists (alternatives, retrospectives,
/// metric grids). Reduced motion collapses the stagger to zero.
public struct StaggeredReveal: ViewModifier {
    // modifier 只接收可见性、序号和位移；真实阶段由调用方通过 isVisible 传入。
    let index: Int
    let isVisible: Bool
    let travel: CGFloat

    @Environment(\.motionPolicy) private var policy

    public func body(content: Content) -> some View {
        // opacity/offset 是理解线索，delay 由 policy.stagger 决定，reduced 会清零错峰。
        content
            .opacity(isVisible ? 1 : 0)
            .offset(y: isVisible ? 0 : policy.travel(travel))
            .animation(
                policy.resolve(.spring(response: 0.34, dampingFraction: 1.0))
                    .delay(isVisible ? policy.stagger(index) : 0),
                value: isVisible
            )
    }
}

extension View {
    public func staggeredReveal(index: Int, isVisible: Bool = true, travel: CGFloat = 8) -> some View {
        // 便捷入口把列表行的 index 和可选位移封装成 StaggeredReveal。
        modifier(StaggeredReveal(index: index, isVisible: isVisible, travel: travel))
    }

    /// Entrances start at 0.96 scale, never 0 — nothing in the real world appears
    /// out of nothing.
    public func materialize(isVisible: Bool, policy: MotionPolicy, scale: CGFloat = 0.96) -> some View {
        // materialize 同时控制缩放、透明度和模糊，isVisible 是唯一动画触发值。
        self
            .scaleEffect(isVisible ? 1 : scale)
            .opacity(isVisible ? 1 : 0)
            .blur(radius: isVisible ? 0 : 2)
            .animation(policy.resolve(Motion.panel), value: isVisible)
    }
}
