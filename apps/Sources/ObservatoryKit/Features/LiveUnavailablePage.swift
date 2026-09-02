import SwiftUI

struct LiveUnavailablePage: View {
    let route: AppRoute

    @Environment(\.appLanguage) private var language

    var body: some View {
        PageScaffold(route: route) {
            VStack(spacing: AkzioLayout.s4) {
                Image(systemName: "rectangle.slash")
                    .font(.system(size: 30, weight: .light))
                    .foregroundStyle(AkzioColor.mutedText)
                StatusBadge(.unavailable)
                Text(L10n.text("Unavailable from observer", language: language))
                    .akzioText(.sectionTitle)
                Text(L10n.text(
                    "This page stays unavailable until Rust publishes its durable data.",
                    language: language
                ))
                .akzioText(.body, color: AkzioColor.secondaryText)
                .multilineTextAlignment(.center)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .akzioCard()
        }
    }
}
