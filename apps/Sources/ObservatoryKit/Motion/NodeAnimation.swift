import SwiftUI

// MARK: - Node motion
//
// Activation: centre brightness rises first, then the outer ring expands, then the
// pulse decays into a steady gold border. Only one node breathes per screen.
public struct NodeActivationPulse: ViewModifier {
    let isActive: Bool
    let tone: AkzioTone

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @State private var activationTick = 0

    public func body(content: Content) -> some View {
        content
            .overlay {
                if isActive {
                    if canvas.runsAmbient {
                        AmbientCanvas { time in
                            let phase = (time.truncatingRemainder(dividingBy: Motion.pulsePeriod)) / Motion.pulsePeriod
                            let eased = 0.5 - 0.5 * cos(phase * 2 * .pi)
                            ring(scale: 1.0 + 0.12 * eased, opacity: 0.45 - 0.28 * eased)
                        }
                    } else {
                        ring(scale: 1.04, opacity: 0.28)
                    }
                }
            }
            .overlay {
                // One-shot activation flash, retriggered when the node becomes active.
                if isActive {
                    ring(scale: 1.0, opacity: 0)
                        .phaseAnimator([0, 1, 2], trigger: activationTick) { view, step in
                            view
                    .scaleEffect(
                        policy.isReduced ? 1 : (step == 0 ? 0.94 : (step == 1 ? 1.16 : 1.0))
                    )
                                .opacity(step == 1 ? 0.9 : 0)
                        } animation: { step in
                            policy.resolve(step == 1 ? .easeOut(duration: 0.25) : .easeOut(duration: 0.35))
                        }
                }
            }
            .onChange(of: isActive) { _, active in
                if active { activationTick += 1 }
            }
    }

    private func ring(scale: CGFloat, opacity: Double) -> some View {
        Circle()
            .strokeBorder(tone.color.opacity(opacity), lineWidth: 1.5)
            .scaleEffect(scale)
            .allowsHitTesting(false)
    }
}

extension View {
    public func nodeActivationPulse(isActive: Bool, tone: AkzioTone = .gold) -> some View {
        modifier(NodeActivationPulse(isActive: isActive, tone: tone))
    }

    /// Gate emphasis: hairline door frame brightens; a single check or blocker follows.
    public func gateHighlight(isOpen: Bool, isRejected: Bool) -> some View {
        overlay {
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(
                    isRejected ? AkzioColor.actionCoral : AkzioColor.primaryGold,
                    lineWidth: isRejected || isOpen ? 1.6 : 1
                )
                .opacity(isRejected ? 1 : (isOpen ? 0.9 : 0.35))
                .allowsHitTesting(false)
        }
    }

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
