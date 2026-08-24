import SwiftUI

// MARK: - Workspace sidebar
//
// The sidebar reads as a workspace navigation surface: context first,
// then the routes used to inspect the live Core.
struct PageSidebar: View {
    let route: AppRoute
    let run: RunPresentation
    let onSelect: (AppRoute) -> Void
    let onOpenSettings: () -> Void

    @Environment(\.appLanguage) private var language
    @Namespace private var highlight

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s1) {
            windowToolbar
            sectionLabel("WORKSPACE")
            ForEach(AppRoute.primary) { row($0) }
            Spacer(minLength: AkzioLayout.s4)
            sectionLabel("SYSTEM")
            row(.scenarioGallery)
            settingsRow
            HairlineDivider().padding(.vertical, AkzioLayout.s2)
            footer
        }
        .padding(.horizontal, AkzioLayout.s2)
        .padding(.bottom, AkzioLayout.s3)
        .frame(
            width: AkzioLayout.sidebarWidth,
            alignment: .leading
        )
        .background(AkzioColor.raisedSurface)
        .overlay(alignment: .trailing) { HairlineDivider(.vertical) }
    }

    private var windowToolbar: some View {
        HStack(spacing: AkzioLayout.s2) {
            // Reserve the native traffic-light region. The sidebar stays
            // expanded, so there is no second control in this row.
            Color.clear.frame(width: 62, height: 1)
            Text("AKZIO")
                .font(.system(size: 15, weight: .bold))
                .tracking(1.3)
            Spacer(minLength: 0)
        }
        // Keep this row on the exact same baseline as RunStatusBar. The
        // native traffic lights occupy this titlebar-height region too.
        .frame(
            minHeight: AkzioLayout.statusBarHeight,
            idealHeight: AkzioLayout.statusBarHeight,
            maxHeight: AkzioLayout.statusBarHeight
        )
        .fixedSize(horizontal: false, vertical: true)
        .layoutPriority(1)
        .overlay(alignment: .bottom) { HairlineDivider() }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Akzio Observatory")
    }

    private func sectionLabel(_ title: String) -> some View {
        Text(L10n.text(title, language: language))
            .font(.system(size: 9, weight: .semibold))
            .tracking(1.1)
            .foregroundStyle(AkzioColor.mutedText)
            .padding(.horizontal, AkzioLayout.s2)
            .padding(.top, AkzioLayout.s1)
    }

    private func row(_ item: AppRoute) -> some View {
        let isSelected = item == route
        return Button { onSelect(item) } label: {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: item.symbol)
                    .font(.system(size: 12, weight: .medium))
                    .frame(width: 18)
                Text(L10n.text(item.title, language: language))
                    .font(AkzioFont.body)
                    .lineLimit(1)
                Spacer(minLength: 0)
                if let shortcut = item.shortcut {
                    Text("⌘\(String(shortcut.character))")
                        .akzioMono(10, color: AkzioColor.mutedText)
                }
            }
            .foregroundStyle(isSelected ? AkzioColor.primaryGold : AkzioColor.secondaryText)
            .padding(.horizontal, AkzioLayout.s2)
            .frame(height: 32)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background {
                if isSelected {
                    RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                        .fill(AkzioColor.goldFill)
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
                    .font(.system(size: 12, weight: .medium))
                    .frame(width: 18)
                Text(L10n.text("Settings", language: language)).font(AkzioFont.body)
                Spacer(minLength: 0)
                Text("⌘8").akzioMono(10, color: AkzioColor.mutedText)
            }
            .foregroundStyle(AkzioColor.secondaryText)
            .padding(.horizontal, AkzioLayout.s2)
            .frame(height: 32)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .buttonStyle(PressableButtonStyle(scale: 0.99))
        .help(L10n.text("Settings", language: language))
    }

    private var footer: some View {
        VStack(spacing: 6) {
            footerRow(
                "Market",
                run.marketOpen
                    ? L10n.text("Market Open", language: language)
                    : L10n.text("Market Closed", language: language),
                run.marketOpen ? .gold : .muted
            )
            footerRow(
                "Data",
                run.dataStale ? MissingValue.unavailable.rawValue : L10n.text("Data Live", language: language),
                run.dataStale ? .coral : .gold
            )
            footerRow("Latency", PpmFormatter.latency(millis: run.latencyMillis), .muted)
        }
        .padding(.horizontal, AkzioLayout.s2)
    }

    private func footerRow(_ label: String, _ value: String, _ tone: AkzioTone) -> some View {
        HStack(spacing: 6) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Spacer(minLength: 4)
            Text(value).akzioMono(10, color: tone.color)
        }
    }
}
