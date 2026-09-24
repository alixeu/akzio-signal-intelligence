import SwiftUI

// MARK: - Collapsible inspector
//
// At full width the right rail is an inline column. Below the compact threshold it
// becomes a popover launched from a small button, which keeps the main canvas from
// being squeezed into something unreadable at 1280×800.
//
// The same content view is used in both modes, so the inspector never has two
// implementations that can drift apart.
struct CollapsibleInspector<Content: View>: View {
    // 标题、图标和宽度描述 inspector 的呈现；Content 由调用方注入并在两种布局中复用。
    private let title: String
    private let symbol: String
    private let width: CGFloat
    private let content: Content

    // compact 决定 inline/popover 分支，policy 统一解析面板动画，presented 只管理本地弹窗状态。
    @Environment(\.akzioCompactLayout) private var compact
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var presented = false

    init(
        title: String,
        symbol: String = "sidebar.trailing",
        width: CGFloat = AkzioLayout.inspectorWidth,
        @ViewBuilder content: () -> Content
    ) {
        // @ViewBuilder 闭包在初始化时构造成单一 Content，避免 inline 与 popover 各维护一份视图。
        self.title = title
        self.symbol = symbol
        self.width = width
        self.content = content()
    }

    var body: some View {
        // 紧凑布局只显示触发按钮；宽布局直接保留 inspector 内容并固定右栏宽度。
        if compact {
            trigger
        } else {
            content
            .frame(width: width)
            .frame(maxHeight: .infinity, alignment: .top)
        }
    }

    private var trigger: some View {
        // 按钮闭包只切换 popover 状态，动画经 MotionPolicy 解析后再执行。
        Button {
            withAnimation(policy.resolve(Motion.panel)) { presented.toggle() }
        } label: {
            Label(L10n.text(title, language: language), systemImage: symbol)
                .font(.system(size: 12, weight: .medium))
                .foregroundStyle(AkzioColor.primaryText)
                .fixedSize()
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .help(L10n.text(title, language: language))
        .accessibilityLabel("\(L10n.text("Show", language: language)) \(L10n.text(title, language: language))")
        .popover(isPresented: $presented, arrowEdge: .leading) {
            // popover 闭包捕获同一个 Content；滚动和内边距只改变紧凑模式的容器。
            PageScroll {
                content
                    .padding(AkzioLayout.s2)
            }
            .frame(width: width + AkzioLayout.s4, height: 460)
            .akzioGlassBackdrop(AkzioColor.raisedSurface, radius: AkzioLayout.cardRadius)
        }
    }
}
