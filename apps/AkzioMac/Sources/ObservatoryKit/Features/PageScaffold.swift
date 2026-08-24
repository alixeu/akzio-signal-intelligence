import SwiftUI

// MARK: - Page scaffold
//
// Every page opens the same way: headline and subtitle first, then content in a
// staggered reveal driven by the transition phase. Keeping this in one place is
// what makes the eight pages feel like one product.
public struct PageScaffold<Content: View, Toolbar: View>: View {
    private let route: AppRoute
    private let content: Content
    private let toolbar: Toolbar

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    public init(
        route: AppRoute,
        @ViewBuilder content: () -> Content,
        @ViewBuilder toolbar: () -> Toolbar = { EmptyView() }
    ) {
        self.route = route
        self.content = content()
        self.toolbar = toolbar()
    }

    public var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            header
            GlassEffectContainer(spacing: AkzioLayout.s3) {
                content
            }
        }
        .padding(AkzioLayout.pageMargin)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline, spacing: AkzioLayout.s3) {
            VStack(alignment: .leading, spacing: 3) {
                Text(L10n.text(route.headline, language: language))
                    .akzioText(.display)
                Text(L10n.text(route.subtitle, language: language))
                    .akzioText(.body, color: AkzioColor.secondaryText)
            }
            Spacer(minLength: AkzioLayout.s4)
            toolbar
        }
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
