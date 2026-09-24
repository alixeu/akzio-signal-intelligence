import SwiftUI

// 文件导读：各业务页面通过 PageScaffold 注入内容和可选工具栏，它统一外边距与玻璃容器；
// StagedSection 再读取 RouteHost 注入的转场阶段，为区块安排入场时机。先看两个 body，
// 再看 EnvironmentValues 扩展：这里演示 SwiftUI 泛型 ViewBuilder 与 Environment 依赖传递。
// MARK: - Page scaffold
//
// Every page places its toolbar above content in a staggered reveal driven by the
// transition phase. Keeping this in one place is what makes the eight pages feel
// like one product.
public struct PageScaffold<Content: View, Toolbar: View>: View {
    // content 和 toolbar 都由页面调用方注入；骨架只负责统一间距、背景容器和布局边界。
    private let content: Content
    private let toolbar: Toolbar

    // 此 Environment 属性当前未在 PageScaffold.body 中读取；StagedSection 会从同一环境独立读取动效策略。
    @Environment(\.motionPolicy) private var policy

    public init(
        route _: AppRoute,
        @ViewBuilder content: () -> Content,
        @ViewBuilder toolbar: () -> Toolbar = { EmptyView() }
    ) {
        // route 参数当前只用于保持页面初始化协议一致；内容和 toolbar 闭包在此完成构造。
        self.content = content()
        self.toolbar = toolbar()
    }

    public var body: some View {
        // toolbar 位于内容上方，GlassEffectContainer 让同一页面内的卡片共享玻璃渲染上下文。
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            toolbar
                .frame(maxWidth: .infinity, alignment: .trailing)
            GlassEffectContainer(spacing: AkzioLayout.s3) {
                content
            }
        }
        .padding(AkzioLayout.pageMargin)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

}

// MARK: - Staged section

/// Wraps a page section so it reveals on the transition's `reveal` phase with a
/// per-index stagger. Sections never animate their own layout on data changes.
public struct StagedSection<Content: View>: View {
    // index 决定同一页面内的 reveal 顺序，content 是不拥有数据状态的页面区块。
    private let index: Int
    private let content: Content

    @Environment(\.motionPolicy) private var policy
    @Environment(\.transitionPhase) private var phase

    public init(index: Int, @ViewBuilder content: () -> Content) {
        // 区块内容只构造一次并保存为泛型 View，显示时由 transitionPhase 决定是否揭示。
        self.index = index
        self.content = content()
    }

    public var body: some View {
        // idle 已是稳定态；进入 reveal 后 staggeredReveal 才按 index 展示内容。
        content
            .staggeredReveal(index: index, isVisible: phase == .idle || phase >= .reveal, travel: 10)
    }
}

private struct TransitionPhaseKey: EnvironmentKey {
    // 根视图未注入阶段时默认 idle，页面因此按已稳定状态呈现。
    static let defaultValue = TransitionPhase.idle
}

extension EnvironmentValues {
    /// Current transition phase, injected by `RouteHost`. `idle` means "settled",
    /// which is why `StagedSection` treats it as fully visible.
    public var transitionPhase: TransitionPhase {
        // get/set 让 RouteHost 注入的阶段沿环境树传给所有 StagedSection。
        get { self[TransitionPhaseKey.self] }
        set { self[TransitionPhaseKey.self] = newValue }
    }
}
