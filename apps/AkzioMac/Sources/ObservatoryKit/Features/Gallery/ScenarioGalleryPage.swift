import SwiftUI

// MARK: - Scenario gallery
//
// Debug surface that stands in for Xcode Previews: every mock scenario on the left,
// the component library on the right. This is the visual-acceptance harness and the
// screenshot script drives it directly.
struct ScenarioGalleryPage: View {
    let store: ObservatoryStore

    var body: some View {
        PageScaffold(route: .scenarioGallery) {
            HStack(alignment: .top, spacing: AkzioLayout.s4) {
                scenarioList
                componentLibrary
            }
        }
    }

    // MARK: Scenarios

    private var scenarioList: some View {
        SectionCard(title: "Scenarios", subtitle: "\(MockScenario.allCases.count) deterministic fixtures") {
            PageScroll {
                VStack(alignment: .leading, spacing: 2) {
                    ForEach(MockScenario.allCases) { item in
                        Button { store.load(item) } label: {
                            HStack(spacing: AkzioLayout.s2) {
                                Text(item.code).akzioMono(10, color: AkzioColor.mutedText)
                                VStack(alignment: .leading, spacing: 1) {
                                    Text(item.title)
                                        .akzioText(.bodySmall, color: AkzioColor.primaryText)
                                    Text(item.routes.map(\.title).joined(separator: " · "))
                                        .akzioText(.caption)
                                }
                                Spacer(minLength: AkzioLayout.s2)
                                if item == store.scenario {
                                    Image(systemName: "checkmark.circle.fill")
                                        .font(.system(size: 11))
                                        .foregroundStyle(AkzioColor.primaryGold)
                                }
                            }
                            .padding(.vertical, 3)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(PressableButtonStyle(scale: 0.995))
                        .rowHoverHighlight(isSelected: item == store.scenario)
                    }
                }
            }
            .frame(width: 320, height: 470)
        }
    }

    // MARK: Components

    private var componentLibrary: some View {
        PageScroll {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                SectionCard(title: "Status Vocabulary", subtitle: "19 states, tone + symbol + label") {
                    LazyVGrid(
                        columns: Array(repeating: GridItem(.flexible(), alignment: .leading), count: 3),
                        alignment: .leading,
                        spacing: 6
                    ) {
                        ForEach(AkzioStatus.allCases, id: \.self) { status in
                            StatusBadge(status, size: .compact)
                        }
                    }
                }
                SectionCard(title: "Missing Values", subtitle: "never rendered as zero") {
                    HStack(spacing: AkzioLayout.s4) {
                        UnavailableValue(.unavailable)
                        UnavailableValue(.pending)
                        UnavailableValue(.waiting)
                        UnavailableValue(.notApplicable)
                    }
                }
                SectionCard(title: "Rings & Bars", subtitle: "nil progress draws a dashed track") {
                    HStack(spacing: AkzioLayout.s5) {
                        ProgressRing(progress: 0.68, tone: .gold) { Text("68%").akzioMono(10) }
                            .frame(width: 76, height: 76)
                        ProgressRing(progress: nil, tone: .muted) { Text("—").akzioMono(10) }
                            .frame(width: 76, height: 76)
                        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                            RatioBar(fraction: 0.42)
                            RatioBar(fraction: nil)
                            SegmentedProgressBar(completed: 7, total: 12)
                        }
                        .frame(width: 180)
                    }
                }
                SectionCard(title: "Transition Matrix", subtitle: "every pair is reversible") {
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(transitionRows, id: \.0) { row in
                            HStack(alignment: .top, spacing: AkzioLayout.s2) {
                                Text(row.0).akzioMono(10, color: AkzioColor.primaryText).frame(width: 190, alignment: .leading)
                                Text(row.1).akzioText(.caption)
                            }
                        }
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var transitionRows: [(String, String)] {
        var seen = Set<String>()
        var rows: [(String, String)] = []
        for from in AppRoute.primary {
            for to in AppRoute.primary where to != from {
                let key = [from.rawValue, to.rawValue].sorted().joined(separator: "↔")
                guard !seen.contains(key) else { continue }
                seen.insert(key)
                let descriptor = RouteTransitionTable.descriptor(from: from, to: to)
                let style = descriptor.style == .sharedElement ? "Shared Element" : "Crossfade"
                rows.append(("\(from.title) ↔ \(to.title)", style))
            }
        }
        return rows
    }
}
