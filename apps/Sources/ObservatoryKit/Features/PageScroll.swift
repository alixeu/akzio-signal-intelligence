import SwiftUI

// MARK: - Page scroll
//
// Pages that can exceed the window scroll; everything else fills it. This wrapper is
// the single place that decision lives.
//
// It also solves a concrete problem with `--capture`: `ImageRenderer` rasterises the
// view tree without a platform scroll view, so anything inside a `ScrollView` renders
// as an empty rectangle. When rendering off-screen the content is laid out directly,
// which produces the same pixels for everything that fits and honestly clips the
// remainder instead of dropping the whole section.
struct PageScroll<Content: View>: View {
    // axes 与 content 描述通用滚动容器；content 不拥有滚动状态，只由本包装器决定承载方式。
    private let axes: Axis.Set
    private let content: Content

    @Environment(\.akzioRendersOffscreen) private var offscreen

    init(_ axes: Axis.Set = .vertical, @ViewBuilder content: () -> Content) {
        // ViewBuilder 闭包在初始化时保存内容，保证在线 ScrollView 和离屏直排使用同一视图树。
        self.axes = axes
        self.content = content()
    }

    var body: some View {
        // 离屏渲染跳过平台 ScrollView 以保留 ImageRenderer 内容；在线模式才启用滚动和指示器策略。
        if offscreen {
            content
        } else {
            ScrollView(axes) { content }
                .scrollIndicators(.never)
                .scrollBounceBehavior(.basedOnSize)
        }
    }
}

private struct RendersOffscreenKey: EnvironmentKey {
    // 默认按正常窗口渲染；只有 capture 流程显式注入 true 才走直排分支。
    static let defaultValue = false
}

extension EnvironmentValues {
    /// True while `ImageRenderer` is rasterising the shell for `--capture`.
    public var akzioRendersOffscreen: Bool {
        // 该值沿环境树传递给所有 PageScroll，不需要页面逐层转发参数。
        get { self[RendersOffscreenKey.self] }
        set { self[RendersOffscreenKey.self] = newValue }
    }
}
