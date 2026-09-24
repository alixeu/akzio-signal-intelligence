import SwiftUI

// MARK: - Canvas render policy
//
// Every ambient effect is drawn inside a single `Canvas`; there is never one View
// per particle. Budgets scale with the user's render-quality choice and drop to
// zero under Reduce Motion.
public struct CanvasRenderPolicy: Sendable, Equatable {
    public enum Quality: String, CaseIterable, Sendable {
        // 质量档位只决定预算和时间线频率，不改变 Canvas 的绘制语义。
        case low, medium, high

        public var displayName: String { rawValue.capitalized }
    }

    // 这些值由设置和窗口活动状态合成，所有 ambient view 通过同一个策略读取。
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
        // density 在入口处限制到 0...1，避免单个页面自行扩大粒子预算。
        self.quality = quality
        self.allowsAmbient = allowsAmbient
        self.density = max(0, min(density, 1))
        self.isPaused = isPaused
    }

    /// Total orbital particles in the Signal Universe.
    public var particleBudget: Int {
        // 轨道粒子预算同时受 ambient 开关、暂停状态、质量和密度限制。
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
        // DAG 路径粒子使用更小的独立预算，避免与轨道粒子相互放大。
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
        // 低质量降低刷新频率；这里仍返回时间线间隔，不直接启动或停止 TimelineView。
        switch quality {
        case .low: 1.0 / 20
        case .medium: 1.0 / 24
        case .high: 1.0 / 30
        }
    }

    /// Ambient loops must not run when paused — this is the single gate views check.
    // 这是所有 ambient loop 共用的单一 gate，暂停时也覆盖窗口遮挡等状态。
    public var runsAmbient: Bool { allowsAmbient && !isPaused }

    public func scaled(_ count: Int) -> Int {
        // 将调用方的基础数量按质量缩放；暂停时返回零，运行时至少保留一个元素。
        guard runsAmbient else { return 0 }
        switch quality {
        case .low: return max(1, count / 3)
        case .medium: return max(1, count * 2 / 3)
        case .high: return count
        }
    }
}

private struct CanvasRenderPolicyKey: EnvironmentKey {
    // 没有根策略时以高质量、允许运动的默认值渲染独立预览。
    static let defaultValue = CanvasRenderPolicy()
}

private struct LabelDensityKey: EnvironmentKey {
    // 标签密度单独注入，避免把文字显示偏好混入粒子预算。
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
        // 时间线样本早于 origin 时钳制为零，保证离屏或时钟抖动不会倒退。
        max(0, sample.timeIntervalSince(origin))
    }
}

/// Wraps `TimelineView` so ambient motion has exactly one pause switch.
/// When paused it renders a single static frame instead of stopping mid-animation.
public struct AmbientCanvas<Content: View>: View {
    // content 闭包接收经过策略处理的 elapsed 秒数，由调用方绘制当前环境帧。
    private let content: (Double) -> Content

    @Environment(\.canvasRenderPolicy) private var policy

    public init(@ViewBuilder content: @escaping (Double) -> Content) {
        // escaping 闭包需被 TimelineView 延后调用，也供暂停分支立即生成静态帧。
        self.content = content
    }

    public var body: some View {
        // 运行时交给带时间线的子视图；暂停时直接以零时刻调用同一闭包，保持确定性。
        if policy.runsAmbient {
            RunningAmbientCanvas(policy: policy, content: content)
        } else {
            // Static frame: deterministic, screenshot-friendly, zero CPU.
            content(0)
        }
    }
}

private struct RunningAmbientCanvas<Content: View>: View {
    // origin 是每个运行中的 canvas 自己的 State；重新进入运行分支时从新的起点计时。
    let policy: CanvasRenderPolicy
    let content: (Double) -> Content

    @State private var origin = Date()

    var body: some View {
        // TimelineView 闭包把日期转换成 elapsed，再把同一秒数传给调用方内容。
        TimelineView(.animation(minimumInterval: policy.frameInterval, paused: false)) { timeline in
            content(AmbientClock.elapsed(sample: timeline.date, origin: origin))
        }
    }
}
