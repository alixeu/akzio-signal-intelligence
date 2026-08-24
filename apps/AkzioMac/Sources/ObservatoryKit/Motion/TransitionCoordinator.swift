import SwiftUI

// MARK: - Transition phases
//
// Prepare 0–90ms · Transform 70–390ms · Reveal 250–590ms · Settle 520–700ms.
// Phases overlap on purpose: supporting content starts revealing while the shared
// element is still travelling.
public enum TransitionPhase: Int, Sendable, Comparable {
    case idle = 0
    case prepare
    case transform
    case reveal
    case settle

    public static func < (lhs: TransitionPhase, rhs: TransitionPhase) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    /// Milliseconds from transition start at which the phase begins.
    var onsetMillis: Int {
        switch self {
        case .idle: 0
        case .prepare: 0
        case .transform: 70
        case .reveal: 250
        case .settle: 520
        }
    }

    static let totalMillis = 700
}

public struct TransitionIntent: Sendable, Equatable {
    public let from: AppRoute
    public let to: AppRoute
    /// True when this move retraces a previous forward move.
    public let reversed: Bool
    /// Keyboard-initiated moves are the same choreography, just faster.
    public let fromKeyboard: Bool

    public init(
        from: AppRoute,
        to: AppRoute,
        reversed: Bool = false,
        fromKeyboard: Bool = false
    ) {
        self.from = from
        self.to = to
        self.reversed = reversed
        self.fromKeyboard = fromKeyboard
    }
}

// MARK: - Coordinator

/// Drives staged reveals and, critically, never queues.
///
/// A new intent arriving mid-flight retargets from the current phase instead of
/// playing two full transitions back to back. SwiftUI springs interpolate from the
/// live presentation value, so geometry continues from wherever it currently is.
@MainActor
@Observable
public final class TransitionCoordinator {
    public private(set) var intent: TransitionIntent?
    public private(set) var phase: TransitionPhase = .idle
    public private(set) var descriptor: RouteTransitionDescriptor = RouteTransitionTable.crossfade
    /// Incremented on every retarget so views can key their staggered reveals.
    public private(set) var generation: Int = 0

    private var driver: Task<Void, Never>?
    private var history: [AppRoute] = []

    public init() {}

    public var isRunning: Bool { intent != nil }

    /// Progress through the whole choreography, 0…1.
    public var normalizedPhase: Double {
        switch phase {
        case .idle, .settle: 1
        case .prepare: 0
        case .transform: 0.25
        case .reveal: 0.65
        }
    }

    public func animation(policy: MotionPolicy) -> Animation {
        var response = descriptor.response
        let keyboard = intent?.fromKeyboard == true
        if keyboard { response *= 0.6 }
        if policy.sharedElementStrength < 1 {
            response *= 0.8 + 0.2 * policy.sharedElementStrength
        }
        // Keyboard paging is the same choreography, just crisper: ⌘1–⌘7 held down
        // must not feel like it is queuing pages.
        let curve: Animation = keyboard
            ? .snappy(duration: response, extraBounce: 0)
            : .spring(response: response, dampingFraction: 0.95)
        return policy.resolve(curve)
    }

    /// Begin (or retarget) a transition. Safe to call while one is in flight.
    public func begin(_ newIntent: TransitionIntent, policy: MotionPolicy) {
        let wasRunning = isRunning
        driver?.cancel()

        let reversed = history.last == newIntent.to
        let resolved = TransitionIntent(
            from: newIntent.from,
            to: newIntent.to,
            reversed: newIntent.reversed || reversed,
            fromKeyboard: newIntent.fromKeyboard
        )

        intent = resolved
        descriptor = RouteTransitionTable.descriptor(from: resolved.from, to: resolved.to)
        generation += 1

        if reversed {
            history.removeLast()
        } else {
            history.append(resolved.from)
            if history.count > 12 { history.removeFirst() }
        }

        guard !policy.isReduced else {
            // Reduced motion: no staged choreography, just let the crossfade land.
            phase = .settle
            driver = Task { [weak self] in
                try? await Task.sleep(for: .milliseconds(220))
                guard !Task.isCancelled else { return }
                self?.finish()
            }
            return
        }

        phase = wasRunning ? max(phase, .transform) : .prepare
        let startingPhase = phase
        driver = Task { [weak self] in
            let steps: [(TransitionPhase, Int)] = [
                (.transform, 70),
                (.reveal, 180),
                (.settle, 270),
            ]
            for (next, delay) in steps where next > startingPhase {
                try? await Task.sleep(for: .milliseconds(delay))
                guard !Task.isCancelled else { return }
                self?.phase = next
            }
            try? await Task.sleep(for: .milliseconds(180))
            guard !Task.isCancelled else { return }
            self?.finish()
        }
    }

    private func finish() {
        // Preserve the same terminal presentation used by `.settle` so the
        // coordinator cleanup cannot publish a mismatched last frame.
        phase = .settle
        intent = nil
        driver = nil
    }

    /// Phase gate used by pages to stagger their own content.
    public func hasReached(_ target: TransitionPhase) -> Bool {
        phase == .idle || phase >= target
    }
}
