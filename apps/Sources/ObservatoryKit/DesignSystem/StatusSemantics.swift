import SwiftUI

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
    public var isLive: Bool { self == .running || self == .observing }
}

public struct StatusStyle: Sendable {
    public let tone: AkzioTone
    public let symbol: String
    public let label: String

    init(_ tone: AkzioTone, _ symbol: String, _ label: String) {
        self.tone = tone
        self.symbol = symbol
        self.label = label
    }

    public var color: Color { tone.color }
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

    public var status: AkzioStatus {
        switch self {
        case .unavailable: .unavailable
        case .pending: .queued
        case .waiting: .waiting
        case .notApplicable: .notApplicable
        }
    }
}
