import SwiftUI

// MARK: - Shared element identity
//
// Stable IDs for `matchedGeometryEffect`. Both directions of a transition use the
// same ID, which is what makes every forward move reversible.
public enum SharedElementID: Hashable, Sendable {
    // 每个 case 对应跨页两端约定的稳定锚点；参数值区分同类节点或资产。
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
        // AppShell 注入单一命名空间，页面只通过环境读取，不自行创建跨页命名空间。
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
        // 统一入口把锚点参数交给 modifier；namespace 为 nil 时由 modifier 保持原视图不变。
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
    // modifier 只负责把可选 namespace 转换为 matchedGeometryEffect 或 no-op。
    let id: SharedElementID
    let namespace: Namespace.ID?
    let isSource: Bool
    let properties: MatchedGeometryProperties
    let anchor: UnitPoint

    func body(content: Content) -> some View {
        // if-let 闭包确保独立组件预览没有命名空间时不会注册无效共享元素。
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
