import SwiftUI

// MARK: - Numeric motion
//
// Digits roll; the sign slot, currency symbol and decimal places never move.
// A whole-string cross-fade is treated as a defect.
extension View {
    /// Digit-rolling transition for any monospaced-digit readout.
    public func akzioNumeric<V: Equatable>(_ value: V, policy: MotionPolicy) -> some View {
        self
            .contentTransition(.numericText())
            .animation(policy.resolve(Motion.numeric), value: value)
    }

    /// Interpolating variant for continuous counters (equity, P&L).
    public func akzioCountUp(_ value: Double, policy: MotionPolicy) -> some View {
        self
            .contentTransition(.numericText(value: value))
            .animation(policy.resolve(Motion.numeric), value: value)
    }
}

// MARK: - Chart motion

public enum ChartAnimation {
    /// Range switches morph the path; in-place refreshes only extend the tail.
    public static func rangeMorph(_ policy: MotionPolicy) -> Animation {
        policy.resolve(.smooth(duration: 0.6))
    }

    public static func tailAppend(_ policy: MotionPolicy) -> Animation {
        policy.resolve(.smooth(duration: 0.35))
    }

    /// Latest point highlight: one soft diffusion, then it stops.
    public static func latestPointBloom(_ policy: MotionPolicy) -> Animation {
        policy.resolve(.easeOut(duration: 0.55))
    }

    /// Bars/ratios slide from their old value instead of snapping.
    public static func barShift(_ policy: MotionPolicy) -> Animation {
        policy.resolve(.smooth(duration: 0.42))
    }

    /// Ring stroke-length growth for T+N progress.
    public static func ringProgress(_ policy: MotionPolicy) -> Animation {
        policy.resolve(Motion.ring)
    }
}

// MARK: - Staggered reveal

/// Short travel + fade, offset per row. Used by lists (alternatives, retrospectives,
/// metric grids). Reduced motion collapses the stagger to zero.
public struct StaggeredReveal: ViewModifier {
    let index: Int
    let isVisible: Bool
    let travel: CGFloat

    @Environment(\.motionPolicy) private var policy

    public func body(content: Content) -> some View {
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
        modifier(StaggeredReveal(index: index, isVisible: isVisible, travel: travel))
    }

    /// Entrances start at 0.96 scale, never 0 — nothing in the real world appears
    /// out of nothing.
    public func materialize(isVisible: Bool, policy: MotionPolicy, scale: CGFloat = 0.96) -> some View {
        self
            .scaleEffect(isVisible ? 1 : scale)
            .opacity(isVisible ? 1 : 0)
            .blur(radius: isVisible ? 0 : 2)
            .animation(policy.resolve(Motion.panel), value: isVisible)
    }
}
