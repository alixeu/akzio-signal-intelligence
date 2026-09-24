import SwiftUI

// 文件导读：RunArchivePage 将 query、purpose、status 与密度的 Binding 传入此筛选条；
// 菜单/文本框只改父页或 Store 的展示设置，实际行过滤仍在父页内存中完成，不发起账本查询。
// 先看 body、searchField 和两个菜单，理解 Binding 的写入如何回到调用页。
// MARK: - Archive filters
//
// 查询、目的和状态只筛选当前投影；density 调整布局，不改变匹配集合。
struct ArchiveFilterBar: View {
    // Binding 把筛选输入直接交给 RunArchivePage；本组件只负责编辑和展示，不加载数据。
    @Binding var query: String
    @Binding var purpose: RunPurpose?
    @Binding var status: WorkflowStatus?
    @Binding var density: SettingsPresentation.Density
    let activeFilters: [String]
    let resultLabel: String

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    // showsMore 当前仅被声明，body/闭包未读取；筛选菜单由 Menu 展开，选值仍通过 Binding 回流。
    @State private var showsMore = false

    var body: some View {
        // 每个控件都修改父页的 Binding，activeFilters/resultLabel 则是父页计算好的只读反馈。
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                searchField
                purposeMenu
                statusMenu
                Menu {
                    Picker(L10n.text("Density", language: language), selection: $density) {
                        ForEach(SettingsPresentation.Density.allCases) { item in
                            Text(L10n.text(item.displayName, language: language)).tag(item)
                        }
                    }
                } label: {
                    Label(L10n.text("More Filters", language: language), systemImage: "line.3.horizontal.decrease.circle")
                        .akzioText(.label)
                }
                .menuStyle(.borderlessButton)
                .frame(width: 116)
                Spacer(minLength: AkzioLayout.s2)
                Text(resultLabel).akzioMono(10, color: AkzioColor.mutedText)
            }
            if !activeFilters.isEmpty {
                HStack(spacing: 5) {
                    ForEach(activeFilters, id: \.self) { filter in
                        Chip(filter, kind: .tag, isSelected: true)
                    }
                }
            }
        }
        .akzioCard(padding: AkzioLayout.s3)
        .animation(policy.resolve(Motion.control), value: activeFilters)
    }

    private var searchField: some View {
        // TextField 绑定查询字符串；清除按钮闭包只清空本地输入，不触发额外查询。
        HStack(spacing: 5) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 10, weight: .medium))
                .foregroundStyle(AkzioColor.mutedText)
            TextField(L10n.text("Search runs", language: language), text: $query)
                .textFieldStyle(.plain)
                .font(AkzioFont.mono(11))
                .foregroundStyle(AkzioColor.primaryText)
            if !query.isEmpty {
                Button { query = "" } label: {
                    Image(systemName: "xmark.circle.fill")
                        .font(.system(size: 10))
                        .foregroundStyle(AkzioColor.mutedText)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(.horizontal, AkzioLayout.s2)
        .frame(width: 216, height: 26)
        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: AkzioLayout.chipRadius)
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: 1)
        )
    }

    private var purposeMenu: some View {
        // 菜单闭包捕获 Binding，通过 nil 表示取消目的筛选。
        Menu {
            Button(L10n.text("All Purposes", language: language)) { purpose = nil }
            Divider()
            ForEach(RunPurpose.allCases, id: \.self) { item in
                Button(L10n.text(item.displayName, language: language)) { purpose = item }
            }
        } label: {
            Text(L10n.text(purpose?.displayName ?? "Purpose", language: language)).akzioText(.label)
        }
        .menuStyle(.borderlessButton)
        .frame(width: 108)
    }

    private var statusMenu: some View {
        // 状态菜单与目的菜单保持同一数据流，只改变父页传入的状态 Binding。
        Menu {
            Button(L10n.text("All Statuses", language: language)) { status = nil }
            Divider()
            ForEach(WorkflowStatus.allCases, id: \.self) { item in
                Button(L10n.text(item.displayName, language: language)) { status = item }
            }
        } label: {
            Text(L10n.text(status?.displayName ?? "Status", language: language)).akzioText(.label)
        }
        .menuStyle(.borderlessButton)
        .frame(width: 138)
    }
}
