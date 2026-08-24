import SwiftUI

// MARK: - Archive filters
//
// Search plus five facets. Every control here is a view filter over the loaded page —
// it never queries or mutates the Store.
struct ArchiveFilterBar: View {
    @Binding var query: String
    @Binding var purpose: RunPurpose?
    @Binding var status: WorkflowStatus?
    @Binding var density: SettingsPresentation.Density
    let activeFilters: [String]
    let resultLabel: String

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var showsMore = false

    var body: some View {
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
        .background(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .fill(AkzioColor.deepBackground)
        )
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: 1)
        )
    }

    private var purposeMenu: some View {
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
