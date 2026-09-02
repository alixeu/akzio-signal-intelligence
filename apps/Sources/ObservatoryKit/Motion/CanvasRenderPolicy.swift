import SwiftUI

// MARK: - Canvas render policy
//
// Every ambient effect is drawn inside a single `Canvas`; there is never one View
// per particle. Budgets scale with the user's render-quality choice and drop to
// zero under Reduce Motion.
public struct CanvasRenderPolicy: Sendable, Equatable {
    public enum Quality: String, CaseIterable, Sendable {
        case low, medium, high

        public var displayName: String { rawValue.capitalized }
    }

    public var quality: Quality
    public var allowsAmbient: Bool
    public var density: Double
    /// Paused while the window is inactive or occluded.
    public var isPaused: Bool

    public init(
        quality: Quality = .high,
        allowsAmbient: Bool = true,
        density: Double = 1,
        isPaused: Bool = false
    ) {
        self.quality = quality
        self.allowsAmbient = allowsAmbient
        self.density = max(0, min(density, 1))
        self.isPaused = isPaused
    }

    /// Total orbital particles in the Signal Universe.
    public var particleBudget: Int {
        guard allowsAmbient, !isPaused else { return 0 }
        let base = switch quality {
        case .low: 60
        case .medium: 140
        case .high: 220
        }
        return Int((Double(base) * density).rounded())
    }

    /// Particles travelling along a converging DAG path.
    public var pathParticleBudget: Int {
        guard allowsAmbient, !isPaused else { return 0 }
        let base = switch quality {
        case .low: 4
        case .medium: 8
        case .high: 12
        }
        return Int((Double(base) * density).rounded())
    }

    /// Timeline refresh interval. 30fps is plenty for slow orbital drift and keeps
    /// long-running CPU/energy cost low.
    public var frameInterval: Double {
        switch quality {
        case .low: 1.0 / 20
        case .medium: 1.0 / 24
        case .high: 1.0 / 30
        }
    }

    /// Ambient loops must not run when paused — this is the single gate views check.
    public var runsAmbient: Bool { allowsAmbient && !isPaused }

    public func scaled(_ count: Int) -> Int {
        guard runsAmbient else { return 0 }
        switch quality {
        case .low: return max(1, count / 3)
        case .medium: return max(1, count * 2 / 3)
        case .high: return count
        }
    }
}

private struct CanvasRenderPolicyKey: EnvironmentKey {
    static let defaultValue = CanvasRenderPolicy()
}

private struct LabelDensityKey: EnvironmentKey {
    static let defaultValue = SettingsPresentation.LabelDensity.auto
}

extension EnvironmentValues {
    public var canvasRenderPolicy: CanvasRenderPolicy {
        get { self[CanvasRenderPolicyKey.self] }
        set { self[CanvasRenderPolicyKey.self] = newValue }
    }

    public var akzioLabelDensity: SettingsPresentation.LabelDensity {
        get { self[LabelDensityKey.self] }
        set { self[LabelDensityKey.self] = newValue }
    }
}

// MARK: - Ambient clock

public enum AmbientClock {
    public static func elapsed(sample: Date, origin: Date) -> Double {
        max(0, sample.timeIntervalSince(origin))
    }
}

/// Wraps `TimelineView` so ambient motion has exactly one pause switch.
/// When paused it renders a single static frame instead of stopping mid-animation.
public struct AmbientCanvas<Content: View>: View {
    private let content: (Double) -> Content

    @Environment(\.canvasRenderPolicy) private var policy

    public init(@ViewBuilder content: @escaping (Double) -> Content) {
        self.content = content
    }

    public var body: some View {
        if policy.runsAmbient {
            RunningAmbientCanvas(policy: policy, content: content)
        } else {
            // Static frame: deterministic, screenshot-friendly, zero CPU.
            content(0)
        }
    }
}

private struct RunningAmbientCanvas<Content: View>: View {
    let policy: CanvasRenderPolicy
    let content: (Double) -> Content

    @State private var origin = Date()

    var body: some View {
        TimelineView(.animation(minimumInterval: policy.frameInterval, paused: false)) { timeline in
            content(AmbientClock.elapsed(sample: timeline.date, origin: origin))
        }
    }
}
