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
    private let title: String
    private let symbol: String
    private let width: CGFloat
    private let content: Content

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
        self.title = title
        self.symbol = symbol
        self.width = width
        self.content = content()
    }

    var body: some View {
        if compact {
            trigger
        } else {
            content
            .frame(width: width)
            .frame(maxHeight: .infinity, alignment: .top)
        }
    }

    private var trigger: some View {
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
            PageScroll {
                content
                    .padding(AkzioLayout.s2)
            }
            .frame(width: width + AkzioLayout.s4, height: 460)
            .akzioGlassBackdrop(AkzioColor.raisedSurface, radius: AkzioLayout.cardRadius)
        }
    }
}
