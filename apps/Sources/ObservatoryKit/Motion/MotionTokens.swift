import SwiftUI

// MARK: - Motion tokens
//
// Thought of as damping + response, not duration. Default is critically damped
// (no overshoot); bounce is reserved for motion that carried momentum.
// `easeIn` is never used: it stalls exactly when the user is watching hardest.
public enum Motion {
    // Interaction
    public static let hover = Animation.spring(response: 0.20, dampingFraction: 1.0)
    public static let selection = Animation.spring(response: 0.28, dampingFraction: 1.0)
    public static let control = Animation.spring(response: 0.24, dampingFraction: 0.90)

    // Surfaces
    public static let panel = Animation.spring(response: 0.40, dampingFraction: 1.0)
    public static let cardDetail = Animation.spring(response: 0.55, dampingFraction: 0.92)
    public static let route = Animation.spring(response: 0.62, dampingFraction: 0.95)

    // Data
    public static let numeric = Animation.smooth(duration: 0.45)
    public static let ring = Animation.smooth(duration: 0.85)
    public static let path = Animation.smooth(duration: 0.80)
    public static let gauge = Animation.smooth(duration: 0.42)
    public static let badge = Animation.spring(response: 0.26, dampingFraction: 0.88)

    // Controls
    /// Toggles carry a small bounce (180–240ms) so the state change is felt.
    public static let toggle = Animation.spring(response: 0.21, dampingFraction: 0.72)
    /// Sliding selection highlights (220–320ms) glide instead of jumping.
    public static let highlight = Animation.spring(response: 0.27, dampingFraction: 0.95)

    // Settings layer
    /// Gear → settings expansion (450–600ms).
    public static let settingsLayer = Animation.spring(response: 0.52, dampingFraction: 0.94)
    /// Live theme preview crossfade (300–450ms).
    public static let themeCrossfade = Animation.smooth(duration: 0.38)
    /// Glass depth changes (280–420ms).
    public static let glassDepth = Animation.smooth(duration: 0.34)

    // Momentum-flavoured (the only place overshoot is allowed)
    public static let flick = Animation.spring(response: 0.42, dampingFraction: 0.78)

    /// Ambient orbit period in seconds (6–12s slow loop).
    public static let ambientPeriod: Double = 9
    /// Active-node breathing period (1.6–2.4s).
    public static let pulsePeriod: Double = 2.0
    /// Observing ring breathing period (2.0–3.0s).
    public static let observingPeriod: Double = 2.6

    /// Stagger step for list reveals (35–80ms depending on row count).
    public static func stagger(_ index: Int, step: Double = 0.045, cap: Double = 0.36) -> Double {
        min(Double(index) * step, cap)
    }
}

// MARK: - Policy

/// Full vs reduced motion. Everything animated in the app resolves its animation
/// through the policy so Reduce Motion has exactly one switch to flip.
public struct MotionPolicy: Sendable, Equatable {
    public enum Level: String, Sendable, CaseIterable {
        case full
        case reduced
    }

    public var level: Level
    /// 0…1 user-facing intensity from Settings; scales travel distance and particle count.
    public var intensity: Double
    /// Route transitions can be softened without disabling internal motion.
    public var routeStrength: Double

    public init(level: Level = .full, intensity: Double = 1.0, routeStrength: Double = 1.0) {
        self.level = level
        self.intensity = intensity
        self.routeStrength = routeStrength
    }

    public static let full = MotionPolicy()
    public static let reduced = MotionPolicy(level: .reduced, intensity: 0, routeStrength: 0)

    public var isReduced: Bool { level == .reduced }

    /// Reduced motion keeps comprehension cues (opacity, colour) and drops travel.
    public func resolve(_ animation: Animation) -> Animation {
        isReduced ? .easeOut(duration: 0.22) : animation
    }

    /// Continuous ambient loops (orbits, particles, breathing) stop entirely.
    public var allowsAmbient: Bool { !isReduced && intensity > 0.01 }

    /// Travel distance for entrances; reduced motion clamps to a 6pt hint.
    public func travel(_ points: CGFloat) -> CGFloat {
        isReduced ? min(points, 6) : points * CGFloat(max(intensity, 0.2))
    }

    /// Shared-element choreography strength; 0 means a plain crossfade handoff.
    public var sharedElementStrength: Double {
        isReduced ? 0 : max(0, min(routeStrength, 1))
    }

    public func stagger(_ index: Int) -> Double {
        isReduced ? 0 : Motion.stagger(index)
    }
}

private struct MotionPolicyKey: EnvironmentKey {
    static let defaultValue = MotionPolicy.full
}

extension EnvironmentValues {
    public var motionPolicy: MotionPolicy {
        get { self[MotionPolicyKey.self] }
        set { self[MotionPolicyKey.self] = newValue }
    }
}

extension View {
    /// Animate with a token, already filtered through the active policy.
    public func akzioAnimation<V: Equatable>(
        _ animation: Animation,
        value: V,
        policy: MotionPolicy
    ) -> some View {
        self.animation(policy.resolve(animation), value: value)
    }
}
