import SwiftUI

// MARK: - Page scaffold
//
// Every page places its toolbar above content in a staggered reveal driven by the
// transition phase. Keeping this in one place is what makes the eight pages feel
// like one product.
public struct PageScaffold<Content: View, Toolbar: View>: View {
    private let content: Content
    private let toolbar: Toolbar

    @Environment(\.motionPolicy) private var policy

    public init(
        route _: AppRoute,
        @ViewBuilder content: () -> Content,
        @ViewBuilder toolbar: () -> Toolbar = { EmptyView() }
    ) {
        self.content = content()
        self.toolbar = toolbar()
    }

    public var body: some View {
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
    private let index: Int
    private let content: Content

    @Environment(\.motionPolicy) private var policy
    @Environment(\.transitionPhase) private var phase

    public init(index: Int, @ViewBuilder content: () -> Content) {
        self.index = index
        self.content = content()
    }

    public var body: some View {
        content
            .staggeredReveal(index: index, isVisible: phase == .idle || phase >= .reveal, travel: 10)
    }
}

private struct TransitionPhaseKey: EnvironmentKey {
    static let defaultValue = TransitionPhase.idle
}

extension EnvironmentValues {
    /// Current transition phase, injected by `RouteHost`. `idle` means "settled",
    /// which is why `StagedSection` treats it as fully visible.
    public var transitionPhase: TransitionPhase {
        get { self[TransitionPhaseKey.self] }
        set { self[TransitionPhaseKey.self] = newValue }
    }
}
