import SwiftUI

// 文件职责：把领域状态映射为可访问的 tone、SF Symbol、label 和缺失值词汇。
// 状态枚举是 Sendable 值语义；View 只读取 computed style/detail/status，不从颜色反推业务状态。
// MARK: - Status semantics
//
// Colour never carries meaning alone: every status is a tone + SF Symbol + label.
// The `notTriggered` / `notApplicable` / `unavailable` / `stale` cases exist because
// the domain has optional steps and optional metrics — they must never render as
// "success" or as `0`.
public enum AkzioStatus: String, Sendable, CaseIterable {
    case running
    case leased
    case queued
    case succeeded
    case completed
    case completedWithRejection
    case failed
    case rejected
    case blocked
    case cancelled
    case skipped
    case notTriggered
    case notApplicable
    case unavailable
    case stale
    case observing
    case waiting
    case partial
    case accepted

    // 输入状态，输出成套 StatusStyle；同一映射同时提供颜色、图标和文案，保证非颜色表达。
    public var style: StatusStyle {
        switch self {
        case .running:
            StatusStyle(.gold, "circle.dotted", "Running")
        case .leased:
            StatusStyle(.neutral, "lock.circle", "Leased")
        case .queued:
            StatusStyle(.neutral, "clock", "Queued")
        case .succeeded:
            StatusStyle(.gold, "checkmark.circle", "Succeeded")
        case .completed:
            StatusStyle(.gold, "checkmark.circle.fill", "Completed")
        case .completedWithRejection:
            StatusStyle(.coral, "checkmark.circle.badge.xmark", "Completed · Execution Rejected")
        case .failed:
            StatusStyle(.coral, "xmark.octagon", "Failed")
        case .rejected:
            StatusStyle(.coral, "xmark.circle", "Rejected")
        case .blocked:
            StatusStyle(.coral, "hand.raised", "Blocked")
        case .cancelled:
            StatusStyle(.muted, "stop.circle", "Cancelled")
        case .skipped:
            StatusStyle(.muted, "minus.circle", "Skipped")
        case .notTriggered:
            StatusStyle(.muted, "minus.circle", "Not Triggered")
        case .notApplicable:
            StatusStyle(.muted, "slash.circle", "Not Applicable")
        case .unavailable:
            StatusStyle(.muted, "questionmark.circle", "Unavailable")
        case .stale:
            StatusStyle(.coral, "exclamationmark.arrow.circlepath", "Stale")
        case .observing:
            StatusStyle(.gold, "waveform.path.ecg", "Observing")
        case .waiting:
            StatusStyle(.neutral, "hourglass", "Waiting")
        case .partial:
            StatusStyle(.gold, "circle.lefthalf.filled", "Partially Filled")
        case .accepted:
            StatusStyle(.gold, "arrow.up.circle", "Accepted")
        }
    }

    /// Long-form explanation shown under conditional steps so they are never ambiguous.
    public var detail: String? {
        // 只有条件/缺失状态返回解释，其余状态返回 nil，调用方可用 Optional 决定是否占位。
        switch self {
        case .notTriggered: "No material conflict detected"
        case .notApplicable: "This run does not submit Paper orders"
        case .unavailable: "Metric not produced for this window"
        case .stale: "Snapshot older than the freshness budget"
        case .waiting: "Observation window not reached"
        default: nil
        }
    }

    /// Terminal states stop pulsing; only `running` / `observing` animate.
    // computed property 只根据枚举值判断是否允许 ambient pulse，不持有或修改动画状态。
    public var isLive: Bool { self == .running || self == .observing }
}

public struct StatusStyle: Sendable {
    public let tone: AkzioTone
    public let symbol: String
    public let label: String

    // 初始化复制三个值；StatusStyle 不保留外部引用，适合在 View 重算时安全传递。
    init(_ tone: AkzioTone, _ symbol: String, _ label: String) {
        self.tone = tone
        self.symbol = symbol
        self.label = label
    }

    // color 是 tone 的展示投影，不是独立状态来源。
    public var color: Color { tone.color }
    // glow 仅用于装饰光晕，业务判断仍由 status/tone 完成。
    public var glow: Color { tone.glow }
}

// MARK: - Missing value vocabulary

/// The only four ways the UI is allowed to render an absent value.
/// `0` is a real number and must never stand in for "we do not know".
public enum MissingValue: String, Sendable {
    case unavailable = "Unavailable"
    case pending = "Pending"
    case waiting = "Waiting"
    case notApplicable = "Not Applicable"

    // 输入缺失类别，输出对应的 AkzioStatus；显示层可以统一复用 StatusBadge/UnavailableValue。
    public var status: AkzioStatus {
        switch self {
        case .unavailable: .unavailable
        case .pending: .queued
        case .waiting: .waiting
        case .notApplicable: .notApplicable
        }
    }
}
