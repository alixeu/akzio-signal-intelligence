import SwiftUI

// MARK: - Routes
//
// Seven content routes. Settings is deliberately *not* one of them: the spec wants
// it to open as a glass layer over a dimmed-but-visible page, so it lives in
// `ObservatoryStore.settingsPresented`; its entry lives in the sidebar.
public enum AppRoute: String, CaseIterable, Identifiable, Sendable {
    case overview
    case workflow
    case intelligence
    case portfolio
    case outcome
    case learning
    case runArchive
    /// Debug-only surface listing every mock scenario and component.
    case scenarioGallery

    public var id: String { rawValue }

    /// Routes shown in the sidebar, in order.
    public static let primary: [AppRoute] = [
        .overview, .workflow, .intelligence, .portfolio, .outcome, .learning, .runArchive,
    ]

    public var title: String {
        switch self {
        case .overview: "Overview"
        case .workflow: "Workflow"
        case .intelligence: "Intelligence"
        case .portfolio: "Portfolio"
        case .outcome: "Outcome"
        case .learning: "Learning"
        case .runArchive: "Run Archive"
        case .scenarioGallery: "Scenario Gallery"
        }
    }

    /// Page headline used by each feature view.
    public var headline: String {
        switch self {
        case .overview: "Live Overview"
        case .workflow: "Workflow Journey"
        case .intelligence: "Intelligence Council"
        case .portfolio: "Portfolio Performance"
        case .outcome: "Outcome Horizons"
        case .learning: "Learning & Experience"
        case .runArchive: "Run Archive"
        case .scenarioGallery: "Scenario Gallery"
        }
    }


    public var symbol: String {
        switch self {
        case .overview: "house"
        case .workflow: "point.topleft.down.to.point.bottomright.curvepath"
        case .intelligence: "brain"
        case .portfolio: "chart.pie"
        case .outcome: "target"
        case .learning: "sparkles.rectangle.stack"
        case .runArchive: "archivebox"
        case .scenarioGallery: "square.grid.3x3"
        }
    }

    /// ⌘1…⌘7 for content routes; ⌘8 is reserved for the Settings layer.
    public var shortcut: KeyEquivalent? {
        switch self {
        case .overview: "1"
        case .workflow: "2"
        case .intelligence: "3"
        case .portfolio: "4"
        case .outcome: "5"
        case .learning: "6"
        case .runArchive: "7"
        case .scenarioGallery: "0"
        }
    }
}
