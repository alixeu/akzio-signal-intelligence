import SwiftUI

// MARK: - Run table
//
// Fixed row height, monospaced identifiers, eight columns. Sorting rotates the arrow
// and glides the rows; filtered-out rows fade and collapse instead of vanishing.
public enum RunSortKey: String, CaseIterable, Identifiable, Sendable {
    case started, duration, result, status

    public var id: String { rawValue }
    public var title: String {
        switch self {
        case .started: "Started"
        case .duration: "Duration"
        case .result: "Result"
        case .status: "Status"
        }
    }
}

struct RunTable: View {
    let rows: [ArchiveRowPresentation]
    let selectedID: String?
    let rowHeight: CGFloat
    @Binding var sortKey: RunSortKey
    @Binding var ascending: Bool
    let namespace: Namespace.ID?
    let onSelect: (String) -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            HairlineDivider()
            VStack(spacing: 0) {
                ForEach(rows) { row in
                    rowView(row).id(row.id)
                }
            }
            Spacer(minLength: 0)
        }
        .animation(policy.resolve(.smooth(duration: 0.34)), value: rows.map(\.id))
    }

    // MARK: Header

    private var header: some View {
        HStack(spacing: AkzioLayout.s2) {
            label("Run ID", width: 78)
            label("Purpose", width: 74)
            label("Topology", width: 120)
            sortable(.status, width: 104)
            sortable(.duration, width: 66)
            label("Current Stage", width: 112)
            label("Model", width: 100)
            sortable(.result, width: 62)
            sortable(.started, width: 56)
        }
        .padding(.horizontal, AkzioLayout.s2)
        .frame(height: 26)
    }

    private func label(_ title: String, width: CGFloat) -> some View {
        Text(L10n.text(title, language: language))
            .akzioText(.caption)
            .lineLimit(1)
            .fixedSize()
            .frame(width: width, alignment: .leading)
    }

    private func sortable(_ key: RunSortKey, width: CGFloat) -> some View {
        Button {
            withAnimation(policy.resolve(Motion.control)) {
                if sortKey == key { ascending.toggle() } else { sortKey = key; ascending = false }
            }
        } label: {
            HStack(spacing: 3) {
                Text(L10n.text(key.title, language: language))
                    .akzioText(.caption, color: sortKey == key ? AkzioColor.primaryGold : AkzioColor.mutedText)
                    .lineLimit(1)
                    .fixedSize()
                Image(systemName: "arrow.up")
                    .font(.system(size: 7, weight: .bold))
                    .foregroundStyle(sortKey == key ? AkzioColor.primaryGold : .clear)
                    .rotationEffect(.degrees(ascending ? 0 : 180))
            }
            .frame(width: width, alignment: .leading)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    // MARK: Rows

    private func rowView(_ row: ArchiveRowPresentation) -> some View {
        let isSelected = row.id == selectedID
        return Button { onSelect(row.id) } label: {
            HStack(spacing: AkzioLayout.s2) {
                Text(String(row.runID.prefix(8)))
                    .akzioMono(11, color: AkzioColor.primaryText)
                    .frame(width: 78, alignment: .leading)
                PillTag(row.purposeLabel, tone: row.purpose.tone)
                    .frame(width: 62, alignment: .leading)
                Text(row.topology).akzioMono(10, color: AkzioColor.secondaryText)
                    .frame(width: 120, alignment: .leading).lineLimit(1)
                StatusBadge(row.status.status, size: .compact)
                    .frame(width: 104, alignment: .leading)
                Text(L10n.text(PpmFormatter.duration(seconds: row.durationSeconds), language: language))
                    .akzioMono(10, color: AkzioColor.secondaryText)
                    .lineLimit(1)
                    .frame(width: 66, alignment: .leading)
                Text(L10n.text(row.currentStage, language: language)).akzioText(.bodySmall).frame(width: 112, alignment: .leading).lineLimit(1)
                Text(row.model).akzioMono(10, color: AkzioColor.mutedText)
                    .frame(width: 100, alignment: .leading).lineLimit(1)
                Text(L10n.text(PpmFormatter.percent(ppm: row.resultPpm), language: language))
                    .akzioMono(10, color: resultColor(row))
                    .frame(width: 74, alignment: .leading)
                Text(L10n.text(row.startedAtLabel, language: language)).akzioMono(10, color: AkzioColor.mutedText)
                    .frame(width: 56, alignment: .leading)
                Spacer(minLength: 0)
            }
            .padding(.horizontal, AkzioLayout.s2)
            .frame(height: rowHeight)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .rowHoverHighlight(isSelected: isSelected)
        .overlay {
            if isSelected {
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .strokeBorder(AkzioColor.goldHairline, lineWidth: 1)
            }
        }
        .offset(y: isSelected ? -1 : 0)
            .sharedElement(isSelected ? .archiveRow(row.id) : .noSharedElement, in: isSelected ? namespace : nil)
        .animation(policy.resolve(Motion.selection), value: isSelected)
        .accessibilityLabel("\(L10n.text("Run", language: language)) \(row.runID.prefix(8)), \(L10n.text(row.status.displayName, language: language))")
        .accessibilityAddTraits(isSelected ? [.isSelected] : [])
    }

    private func resultColor(_ row: ArchiveRowPresentation) -> Color {
        guard let result = row.resultPpm else { return AkzioColor.mutedText }
        return result >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral
    }
}
