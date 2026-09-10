import SwiftUI

// MARK: - Workspace sidebar
//
// The sidebar reads as a workspace navigation surface: context first,
// then the routes used to inspect the live Core.
struct PageSidebar: View {
    let route: AppRoute
    let theme: SettingsPresentation.Theme
    let onSelect: (AppRoute) -> Void
    let onOpenSettings: () -> Void
    let onToggleSidebar: () -> Void

    @Environment(\.appLanguage) private var language
    @Namespace private var highlight

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            windowToolbar
            ForEach(AppRoute.primary) { row($0) }
            Spacer(minLength: AkzioLayout.s6)
            row(.scenarioGallery)
            settingsRow
        }
        .padding(.horizontal, AkzioLayout.sidebarHorizontalPadding)
        .padding(.bottom, AkzioLayout.s3)
        .frame(width: AkzioLayout.sidebarWidth, alignment: .leading)
        .frame(maxHeight: .infinity, alignment: .topLeading)
        .akzioGlassBackdrop(AkzioColor.sidebarSurface(for: theme), radius: 0)
        .overlay(alignment: .trailing) {
            Rectangle()
                .fill(AkzioColor.sidebarHairline)
                .frame(width: AkzioLayout.hairlineWidth)
        }
    }

    private var windowToolbar: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: AkzioLayout.s2) {
                // Reserve the native traffic-light region.
                Color.clear.frame(width: 62, height: 1)
                Spacer(minLength: 0)
                Button(action: onToggleSidebar) {
                    Image(systemName: "sidebar.left")
                        .font(.system(size: 16, weight: .medium))
                        .foregroundStyle(AkzioColor.sidebarPrimaryText)
                        .frame(width: 32, height: 32)
                        .background {
                            Circle()
                                .fill(AkzioColor.elevatedSurface)
                                .overlay {
                                    Circle().stroke(AkzioColor.sidebarHairline, lineWidth: 1)
                                }
                        }
                }
                .buttonStyle(.plain)
                .help("Hide Sidebar")
                .accessibilityLabel("Hide Sidebar")
            }
            .frame(
                minHeight: AkzioLayout.statusBarHeight,
                idealHeight: AkzioLayout.statusBarHeight,
                maxHeight: AkzioLayout.statusBarHeight
            )

            .padding(.bottom, AkzioLayout.s5)
        }
        .fixedSize(horizontal: false, vertical: true)
        .layoutPriority(1)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Sidebar navigation")
    }

    private func row(_ item: AppRoute) -> some View {
        let isSelected = item == route
        return Button { onSelect(item) } label: {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: item.symbol)
                    .font(.system(size: 19, weight: .medium))
                    .frame(width: 24)
                Text(L10n.text(item.title, language: language))
                    .font(.system(size: 16, weight: .regular))
                    .lineLimit(1)
                Spacer(minLength: 0)
            }
            .foregroundStyle(isSelected ? AkzioColor.sidebarAccent : AkzioColor.sidebarSecondaryText)
            .padding(.horizontal, AkzioLayout.s3)
            .frame(height: 38)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background {
                if isSelected {
                    RoundedRectangle(cornerRadius: 23, style: .continuous)
                        .fill(AkzioColor.sidebarSelection)
                        .matchedGeometryEffect(id: "sidebar.selection", in: highlight)
                }
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(PressableButtonStyle(scale: 0.99))
        .help(L10n.text(item.headline, language: language))
        .accessibilityAddTraits(isSelected ? [.isSelected] : [])
    }

    private var settingsRow: some View {
        Button(action: onOpenSettings) {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: "gearshape")
                    .font(.system(size: 19, weight: .medium))
                    .frame(width: 24)
                Text(L10n.text("Settings", language: language))
                    .font(.system(size: 16, weight: .regular))
                Spacer(minLength: 0)
            }
            .foregroundStyle(AkzioColor.sidebarSecondaryText)
            .padding(.horizontal, AkzioLayout.s3)
            .frame(height: 38)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .buttonStyle(PressableButtonStyle(scale: 0.99))
        .help(L10n.text("Settings", language: language))
    }

}
