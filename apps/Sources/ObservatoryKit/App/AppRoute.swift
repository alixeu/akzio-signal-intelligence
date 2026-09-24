import SwiftUI

// 文件职责：定义 Observatory 的页面路由值、显示顺序、标题、图标和键盘快捷键。
// AppShell/ObservatoryStore 以 AppRoute 作为状态值；Settings 不在这里，因为它是覆盖层。
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

    // Identifiable 要求稳定 ID；rawValue 与序列化路由名保持一致。
    public var id: String { rawValue }

    /// Routes shown in the sidebar, in order.
    public static let primary: [AppRoute] = [
        .overview, .workflow, .intelligence, .portfolio, .outcome, .learning, .runArchive,
    ]

    public var title: String {
        // switch 穷举所有路由，使新增页面在编译期暴露未处理分支。
        switch self {
        case .overview: "Overview"
        case .workflow: "Workflow"
        case .intelligence: "Intelligence"
        case .portfolio: "Portfolio"
        case .outcome: "Outcome"
        case .learning: "Learning"
        case .runArchive: "Run Archive"
        }
    }

    /// Page headline used by each feature view.
    public var headline: String {
        // headline 是页面正文使用的短标题，不负责导航状态或窗口标题。
        switch self {
        case .overview: "Live Overview"
        case .workflow: "Workflow Journey"
        case .intelligence: "Intelligence Council"
        case .portfolio: "Portfolio Performance"
        case .outcome: "Outcome Horizons"
        case .learning: "Learning & Experience"
        case .runArchive: "Run Archive"
        }
    }


    public var symbol: String {
        // SF Symbol 名称只是展示数据；真正的页面选择仍由 enum 值决定。
        switch self {
        case .overview: "house"
        case .workflow: "point.topleft.down.to.point.bottomright.curvepath"
        case .intelligence: "brain"
        case .portfolio: "chart.pie"
        case .outcome: "target"
        case .learning: "sparkles.rectangle.stack"
        case .runArchive: "archivebox"
        }
    }

    /// ⌘1…⌘7 for content routes; ⌘8 is reserved for the Settings layer.
    public var shortcut: KeyEquivalent? {
        // 未提供快捷键的状态应返回 nil；当前所有主路由都有固定的数字键。
        switch self {
        case .overview: "1"
        case .workflow: "2"
        case .intelligence: "3"
        case .portfolio: "4"
        case .outcome: "5"
        case .learning: "6"
        case .runArchive: "7"
        }
    }
}
