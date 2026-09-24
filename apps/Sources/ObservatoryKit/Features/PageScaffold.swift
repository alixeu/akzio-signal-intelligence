import SwiftUI

// MARK: - Page scaffold
//
// Every page places its toolbar above content in a staggered reveal driven by the
// transition phase. Keeping this in one place is what makes the eight pages feel
// like one product.
public struct PageScaffold<Content: View, Toolbar: View>: View {
    // content 和 toolbar 都由页面调用方注入；骨架只负责统一间距、背景容器和布局边界。
    private let content: Content
    private let toolbar: Toolbar

    // policy 保留在骨架环境中，供统一页面动效的子视图读取；骨架本身不改变数据。
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
