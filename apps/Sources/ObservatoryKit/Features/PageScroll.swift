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
    private let axes: Axis.Set
    private let content: Content

    @Environment(\.akzioRendersOffscreen) private var offscreen

    init(_ axes: Axis.Set = .vertical, @ViewBuilder content: () -> Content) {
        self.axes = axes
        self.content = content()
    }

    var body: some View {
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
    static let defaultValue = false
}

extension EnvironmentValues {
    /// True while `ImageRenderer` is rasterising the shell for `--capture`.
    public var akzioRendersOffscreen: Bool {
        get { self[RendersOffscreenKey.self] }
        set { self[RendersOffscreenKey.self] = newValue }
    }
}
