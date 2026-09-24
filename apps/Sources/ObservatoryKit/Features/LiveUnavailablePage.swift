import SwiftUI

struct LiveUnavailablePage: View {
    // route 标记当前不可用页面，但该页面不接收或推测任何实时业务数据。
    let route: AppRoute

    // 语言来自根环境，保证不可用说明与其他页面使用同一套本地化设置。
    @Environment(\.appLanguage) private var language

    var body: some View {
        // 统一页面骨架承载明确的 unavailable 状态；这里不把缺失数据渲染成零值或成功态。
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
