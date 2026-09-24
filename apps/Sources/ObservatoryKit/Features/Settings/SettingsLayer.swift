import SwiftUI

// 文件职责：承载居中的设置 modal、分类导航和按 Category 分发的 detail View。
// store 是 @Bindable 的主数据入口；Environment 只提供语言/动效，设置显隐和分类仍由 Store 管理。
// MARK: - Settings layer
//
// A centred glass layer over a dimmed-but-visible page. Display
// only: nothing here is persisted, and no control reaches outside the process.
struct SettingsLayer: View {
    // @Bindable 让本层及子 Section 共享同一个 Store/settings 引用语义；highlight 只服务 matched geometry。
    @Bindable var store: ObservatoryStore

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Namespace private var highlight

    var body: some View {
        // body 用 scrim 点击闭包关闭 layer，用 store 状态控制 transition；detail/categoryList 不直接持久化设置。
        ZStack {
            // Scrim: dims the page without hiding it, so context is never lost.
            Color.black.opacity(0.34)
                .ignoresSafeArea()
                // 点击 scrim 只切换 Store 的展示状态，不修改设置字段。
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
        // computed View 根据 allCases 生成稳定导航列表；ForEach 闭包只在点击时写回 settingsCategory。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text("Settings", language: language)).akzioText(.title)
            Text(L10n.text("Application & Core", language: language)).akzioText(.caption)
                .padding(.bottom, AkzioLayout.s3)
            ForEach(SettingsPresentation.Category.allCases) { item in
                // item 是 Sendable/Identifiable 的值；选中样式和 accessibility trait 都由 Store 当前 category 推导。
                Button {
                    // withAnimation 闭包只提交新的分类值，detail 的 id/transition 随后由 SwiftUI 重建。
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
        // computed View 以当前 category 作为 identity；切换时 PageScroll 保留容器，section 内容按枚举分发。
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
        // @ViewBuilder switch 将 Category 映射到对应 Section；每个分支接收 Store 的 Binding 或只读 policy 快照。
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
