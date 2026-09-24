import SwiftUI

// MARK: - Transition phases
//
// Prepare 0–90ms · Transform 70–390ms · Reveal 250–590ms · Settle 520–700ms.
// Phases overlap on purpose: supporting content starts revealing while the shared
// element is still travelling.
public enum TransitionPhase: Int, Sendable, Comparable {
    // phase 是页面转场的单调阶段值，RouteHost 和 StagedSection 通过环境读取它。
    case idle = 0
    case prepare
    case transform
    case reveal
    case settle

    public static func < (lhs: TransitionPhase, rhs: TransitionPhase) -> Bool {
        // Comparable 只比较 rawValue，保证阶段筛选和 retarget 判断有稳定顺序。
        lhs.rawValue < rhs.rawValue
    }

    /// Milliseconds from transition start at which the phase begins.
    var onsetMillis: Int {
        // 每个阶段的起点用于诊断和时间编排；实际等待由 coordinator 的 Task 执行。
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
    // intent 描述一次从 from 到 to 的请求，并保留反向与键盘入口信息。
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
        // 初始化只封装请求元数据，不在值类型内改变 coordinator 状态。
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
    // 这些公开状态是 View 的只读投影；唯一可变驱动和历史留在 coordinator 内部。
    public private(set) var intent: TransitionIntent?
    public private(set) var phase: TransitionPhase = .idle
    public private(set) var descriptor: RouteTransitionDescriptor = RouteTransitionTable.crossfade
    /// Incremented on every retarget so views can key their staggered reveals.
    public private(set) var generation: Int = 0

    // driver 可取消，history 仅用于识别反向导航，不保存页面内容。
    private var driver: Task<Void, Never>?
    private var history: [AppRoute] = []

    // coordinator 初始为空闲态，首个 begin 才建立 descriptor 和阶段任务。
    public init() {}

    public var isRunning: Bool { intent != nil }

    public func animation(policy: MotionPolicy) -> Animation {
        // 动画响应由 route descriptor、键盘加速、共享元素强度和 reduced policy 共同决定。
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
        // begin 不排队：新请求取消旧 driver，并从当前阶段重新指向目标 route。
        let wasRunning = isRunning
        driver?.cancel()

        // history 与显式 reversed 合并，支持用户沿同一 route pair 返回而不重复堆栈。
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

        // 正向请求记录来源 route，反向请求移除上一条来源；长度限制避免历史无限增长。
        if reversed {
            history.removeLast()
        } else {
            history.append(resolved.from)
            if history.count > 12 { history.removeFirst() }
        }

        guard !policy.isReduced else {
            // reduced 直接进入 settle，再由可取消 Task 给 crossfade 留出固定落地时间。
            // Reduced motion: no staged choreography, just let the crossfade land.
            phase = .settle
            driver = Task { [weak self] in
                // 弱捕获避免协调器已销毁时，延迟任务反向延长其生命周期。
                try? await Task.sleep(for: .milliseconds(220))
                guard !Task.isCancelled else { return }
                self?.finish()
            }
            return
        }

        phase = wasRunning ? max(phase, .transform) : .prepare
        let startingPhase = phase
        driver = Task { [weak self] in
            // 正常模式按阶段等待并检查取消；retarget 后旧任务不会再发布阶段。
            let steps: [(TransitionPhase, Int)] = [
                (.transform, 70),
                (.reveal, 180),
                (.settle, 270),
            ]
            for (next, delay) in steps where next > startingPhase {
                // 每个循环只推进一个尚未到达的阶段，允许从当前 presentation 继续。
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

}
