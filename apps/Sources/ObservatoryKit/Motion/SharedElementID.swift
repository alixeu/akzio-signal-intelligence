import SwiftUI

// MARK: - Shared element identity
//
// Stable IDs for `matchedGeometryEffect`. Both directions of a transition use the
// same ID, which is what makes every forward move reversible.
public enum SharedElementID: Hashable, Sendable {
    // Overview ↔ Workflow
    case signalUniverse
    case currentNode
    case workflowProgress

    // Overview ↔ Intelligence / Workflow ↔ Intelligence
    case roleCard(AgentRole)
    case confidenceRing
    case modelName

    // Overview ↔ Portfolio
    case equityValue
    case sparkline
    case positionCard(TradableAsset)

    // Portfolio ↔ Outcome
    case equityLatestPoint
    case portfolioReturn

    // Overview / Workflow ↔ Outcome
    case horizonRing(OutcomeHorizonKind)
    case evaluateNode
    case outcomeSummary

    // Outcome ↔ Learning
    case completedRing
    case retrospectiveBadge
    case learningNode

    // Run Archive → any run page
    case archiveRow(String)
    case runIdentifier
    case runStatus

    /// Sentinel used only when a view intentionally has no shared namespace.
    case noSharedElement

}

// MARK: - Attachment helper

private struct SharedElementNamespaceKey: EnvironmentKey {
    static let defaultValue: Namespace.ID? = nil
}

extension EnvironmentValues {
    /// Single namespace injected once by `AppShell` and read by every page.
    public var sharedNamespace: Namespace.ID? {
        get { self[SharedElementNamespaceKey.self] }
        set { self[SharedElementNamespaceKey.self] = newValue }
    }
}

extension View {
    /// Attach a shared-element anchor. No-ops safely if the namespace is absent
    /// (e.g. a standalone component preview).
    public func sharedElement(
        _ id: SharedElementID,
        in namespace: Namespace.ID?,
        isSource: Bool = true,
        properties: MatchedGeometryProperties = .frame,
        anchor: UnitPoint = .center
    ) -> some View {
        modifier(
            SharedElementModifier(
                id: id,
                namespace: namespace,
                isSource: isSource,
                properties: properties,
                anchor: anchor
            )
        )
    }
}

struct SharedElementModifier: ViewModifier {
    let id: SharedElementID
    let namespace: Namespace.ID?
    let isSource: Bool
    let properties: MatchedGeometryProperties
    let anchor: UnitPoint

    func body(content: Content) -> some View {
        if let namespace {
            content.matchedGeometryEffect(
                id: id,
                in: namespace,
                properties: properties,
                anchor: anchor,
                isSource: isSource
            )
        } else {
            content
        }
    }
}
