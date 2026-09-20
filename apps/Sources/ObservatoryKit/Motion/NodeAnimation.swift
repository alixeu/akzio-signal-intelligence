import SwiftUI

// MARK: - Node motion
//
extension View {
    /// Soft one-shot bloom used for "completed" moments. Never loops.
    public func completionBloom(trigger: Int, tone: AkzioTone = .gold) -> some View {
        modifier(CompletionBloom(trigger: trigger, tone: tone))
    }
}

private struct CompletionBloom: ViewModifier {
    let trigger: Int
    let tone: AkzioTone

    @Environment(\.motionPolicy) private var policy

    func body(content: Content) -> some View {
        content.overlay {
            Circle()
                .fill(tone.glow)
                .blur(radius: policy.isReduced ? 0 : 12)
                .phaseAnimator([0, 1], trigger: trigger) { view, step in
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
    public let progress: Double
    public let isLit: Bool

    public init(progress: Double, isLit: Bool) {
        self.progress = progress
        self.isLit = isLit
    }

    public static func staggered(index: Int, elapsed: Double, policy: MotionPolicy) -> PathGrowth {
        guard !policy.isReduced else { return PathGrowth(progress: 1, isLit: true) }
        let delay = Double(index) * 0.06
        let local = max(0, min((elapsed - delay) / 0.8, 1))
        return PathGrowth(progress: local, isLit: local >= 1)
    }
}
