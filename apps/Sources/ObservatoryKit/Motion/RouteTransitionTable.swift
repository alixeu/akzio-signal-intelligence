import SwiftUI

// MARK: - Route transition descriptors
//
// One entry per unordered page pair. Forward and reverse read the same row, which
// is what guarantees "every forward transition has a reverse".
public struct RouteTransitionDescriptor: Sendable {
    public enum Style: Sendable {
        /// Natural shared elements exist: hand geometry over between pages.
        case sharedElement
        /// No natural shared element: keep spatial position and crossfade.
        case crossfade
    }

    public let style: Style
    /// Anchors that must exist on both sides for the handoff to read correctly.
    public let anchors: [SharedElementID]
    /// Base response of the route spring; the policy can soften it.
    public let response: Double
    /// Edge the incoming supporting content slides from.
    /// Human-readable forward choreography, surfaced in the Scenario Gallery.
    public let forwardNote: String
    public let reverseNote: String

    init(
        style: Style = .sharedElement,
        anchors: [SharedElementID],
        response: Double = 0.62,
        forwardNote: String,
        reverseNote: String
    ) {
        self.style = style
        self.anchors = anchors
        self.response = response
        self.forwardNote = forwardNote
        self.reverseNote = reverseNote
    }

    public var animation: Animation {
        .spring(response: response, dampingFraction: 0.95)
    }
}

public enum RouteTransitionTable {
    /// Unordered pair key so both directions resolve to the same row.
    private static func key(_ a: AppRoute, _ b: AppRoute) -> String {
        [a.rawValue, b.rawValue].sorted().joined(separator: "↔")
    }

    public static func descriptor(from: AppRoute, to: AppRoute) -> RouteTransitionDescriptor {
        table[key(from, to)] ?? crossfade
    }

    /// True when the pair has a registered natural shared element.
    public static func hasNaturalSharedElement(from: AppRoute, to: AppRoute) -> Bool {
        table[key(from, to)]?.style == .sharedElement
    }

    static let crossfade = RouteTransitionDescriptor(
        style: .crossfade,
        anchors: [],
        response: 0.66,
        forwardNote: "The current page crossfades into the destination while supporting content reveals in place.",
        reverseNote: "The destination retraces the same in-place crossfade back to the origin."
    )

    static let table: [String: RouteTransitionDescriptor] = [
        key(.overview, .workflow): RouteTransitionDescriptor(
            anchors: [.signalUniverse, .currentNode, .workflowProgress],
            forwardNote: "Outer orbits dim, the orbital layout unfolds into the full DAG, paths extend, and Stage Inspector enters from the right.",
            reverseNote: "DAG folds back along the same paths into the Signal Universe; Inspector retracts; the current node returns to centre."
        ),
        key(.overview, .intelligence): RouteTransitionDescriptor(
            anchors: [.currentNode, .roleCard(.planner), .confidenceRing, .modelName],
            response: 0.60,
            forwardNote: "The active agent node leaves its orbit and morphs into a Role Card; other agents become council cards; the Confidence Ring moves into Selected Model Detail.",
            reverseNote: "Role Card contracts back into its orbital slot and the ring returns to the KPI strip."
        ),
        key(.overview, .portfolio): RouteTransitionDescriptor(
            anchors: [.equityValue, .sparkline, .positionCard(.tqqq)],
            response: 0.65,
            forwardNote: "Equity value stays continuous on screen, the sparkline expands into the full equity curve, asset nodes split into four position cards.",
            reverseNote: "Curve compresses back into the sparkline; cards merge into orbital asset nodes."
        ),
        key(.overview, .outcome): RouteTransitionDescriptor(
            anchors: [.horizonRing(.t1), .horizonRing(.t3), .horizonRing(.t5)],
            forwardNote: "The three horizon dots travel to the content centre and expand into rings; the active horizon keeps the gold highlight.",
            reverseNote: "Rings contract back into orbital dots."
        ),
        key(.overview, .learning): RouteTransitionDescriptor(
            anchors: [.learningNode],
            response: 0.64,
            forwardNote: "The Learning node expands into the Experience Timeline; outer paths become the Policy Transition track; Retrospective cards stagger in.",
            reverseNote: "Timeline collapses back into the Learning node."
        ),
        key(.workflow, .intelligence): RouteTransitionDescriptor(
            anchors: [.roleCard(.analyst), .modelName, .confidenceRing],
            response: 0.58,
            forwardNote: "Selected DAG node lifts, the circular node becomes a rectangular Role Card, Stage Inspector content becomes Model Detail.",
            reverseNote: "Role Card settles back into its DAG slot."
        ),
        key(.workflow, .outcome): RouteTransitionDescriptor(
            anchors: [.evaluateNode, .horizonRing(.t1), .horizonRing(.t3), .horizonRing(.t5)],
            forwardNote: "The Evaluate output path splits into three horizons; horizon nodes scale up into Outcome Rings.",
            reverseNote: "Rings converge back into the Evaluate node."
        ),
        key(.portfolio, .outcome): RouteTransitionDescriptor(
            anchors: [.equityLatestPoint, .portfolioReturn, .outcomeSummary],
            response: 0.60,
            forwardNote: "The latest equity point scales into the active horizon ring; the curve compresses into the summary comparison chart; Portfolio Return stays continuous.",
            reverseNote: "Ring shrinks back onto the curve's latest point."
        ),
        key(.outcome, .learning): RouteTransitionDescriptor(
            anchors: [.completedRing, .retrospectiveBadge, .learningNode],
            response: 0.65,
            forwardNote: "The completed ring becomes the Retrospective card's status badge; summary content extends into the Experience Timeline; the lesson candidate appears.",
            reverseNote: "Badge expands back into the completed ring."
        ),
        key(.runArchive, .overview): archiveHandoff,
        key(.runArchive, .workflow): archiveHandoff,
        key(.runArchive, .intelligence): archiveHandoff,
        key(.runArchive, .portfolio): archiveHandoff,
        key(.runArchive, .outcome): archiveHandoff,
        key(.runArchive, .learning): archiveHandoff,
    ]

    static let archiveHandoff = RouteTransitionDescriptor(
        style: .crossfade,
        anchors: [],
        response: 0.64,
        forwardNote: "The selected row lifts and widens; Run ID and Status fly into the Run Status Bar; the destination page unfolds from the row summary.",
        reverseNote: "Content folds back into the originating row, preserving scroll position."
    )

    /// Settings is a layer, not a route: it materializes in place.
    public static let settingsLayer = RouteTransitionDescriptor(
        style: .crossfade,
        anchors: [],
        response: 0.52,
        forwardNote: "The glass Settings layer materializes over the dimmed-but-visible page.",
        reverseNote: "The Settings layer dematerializes in place and restores the page."
    )
}
