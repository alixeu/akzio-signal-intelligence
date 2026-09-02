import SwiftUI

// MARK: - Settings layer
//
// A centred glass layer over a dimmed-but-visible page. Display
// only: nothing here is persisted, and no control reaches outside the process.
struct SettingsLayer: View {
    @Bindable var store: ObservatoryStore

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Namespace private var highlight

    var body: some View {
        ZStack {
            // Scrim: dims the page without hiding it, so context is never lost.
            Color.black.opacity(0.34)
                .ignoresSafeArea()
                .onTapGesture { store.toggleSettings() }
            .accessibilityLabel(L10n.text("Dismiss settings", language: language))

            HStack(spacing: 0) {
                categoryList
                HairlineDivider(.vertical)
                detail
            }
            .frame(width: 760, height: 500)
            .akzioGlass(.modal, radius: AkzioLayout.sheetRadius)
            .materialize(isVisible: true, policy: policy)
        }
        .transition(.opacity)
        .animation(policy.resolve(Motion.settingsLayer), value: store.settingsPresented)
    }

    // MARK: Categories

    private var categoryList: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text("Settings", language: language)).akzioText(.title)
            Text(L10n.text("Application & Core", language: language)).akzioText(.caption)
                .padding(.bottom, AkzioLayout.s3)
            ForEach(SettingsPresentation.Category.allCases) { item in
                Button {
                    withAnimation(policy.resolve(Motion.highlight)) { store.settingsCategory = item }
                } label: {
                    HStack(spacing: AkzioLayout.s2) {
                        Image(systemName: item.symbol)
                            .font(.system(size: 11, weight: .medium))
                            .frame(width: 16)
                        Text(L10n.text(item.displayName, language: language)).akzioText(.body)
                        Spacer(minLength: 0)
                    }
                        .foregroundStyle(item == store.settingsCategory ? AkzioColor.primaryGold : AkzioColor.secondaryText)
                    .padding(.horizontal, AkzioLayout.s2)
                    .frame(height: 30)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background {
                            if item == store.settingsCategory {
                            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                                .fill(AkzioColor.goldFill)
                                .matchedGeometryEffect(id: "categoryHighlight", in: highlight)
                        }
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(PressableButtonStyle(scale: 0.995))
                    .accessibilityAddTraits(item == store.settingsCategory ? [.isSelected] : [])
            }
            Spacer(minLength: 0)
            Button(L10n.text("Close", language: language)) { store.toggleSettings() }
                .buttonStyle(PressableButtonStyle())
                .keyboardShortcut(.escape, modifiers: [])
        }
        .padding(AkzioLayout.s4)
        .frame(width: 214, alignment: .leading)
    }

    // MARK: Detail

    private var detail: some View {
        PageScroll {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                Text(L10n.text(store.settingsCategory.displayName, language: language)).akzioText(.sectionTitle)
                section
            }
            .padding(AkzioLayout.s5)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        // Short-travel crossfade: the highlight moves, the content arrives.
            .id(store.settingsCategory)
        .transition(.opacity)
            .animation(policy.resolve(Motion.panel), value: store.settingsCategory)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    @ViewBuilder
    private var section: some View {
        switch store.settingsCategory {
        case .appearance:
            AppearanceSection(settings: $store.settings)
        case .motion:
            MotionSection(settings: $store.settings, resolvedPolicy: store.motionPolicy)
        case .modelDisplay:
            ModelDisplaySection(settings: $store.settings, canvasPolicy: store.canvasPolicy)
        case .core:
            CoreSettingsSection(store: store)
        case .accessibility:
            AccessibilitySection(
                settings: $store.settings,
                systemReduceMotion: store.systemReduceMotion,
                systemReduceTransparency: store.systemReduceTransparency
            )
        case .environment:
            EnvironmentInfoSection()
        }
    }
}
