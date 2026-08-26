import SwiftUI

// MARK: - Intelligence
//
// Durable agenda items on the left and the Observer-safe analysis record on the
// right. The page never invents a meeting from configured models: without observed
// trajectory or validated artifacts it renders an explicit empty state.
struct IntelligencePage: View {
    let store: ObservatoryStore

    @Environment(\.appLanguage) private var language

    private var council: CouncilPresentation { store.displayCouncil }

    var body: some View {
        PageScaffold(route: .intelligence) {
            if council.topics.isEmpty && council.analysisRecords.isEmpty {
                emptyState
            } else {
                PageScroll {
                    HStack(alignment: .top, spacing: AkzioLayout.s4) {
                        StagedSection(index: 0) { agenda }
                        StagedSection(index: 1) { analysisTimeline }
                    }
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                }
            }
        } toolbar: {
            HStack(spacing: AkzioLayout.s2) {
                PillTag(
                    "\(council.topics.count) \(L10n.text("Topics", language: language))",
                    tone: .neutral
                )
                PillTag(
                    "\(council.analysisRecords.count) \(L10n.text("Records", language: language))",
                    tone: .gold
                )
            }
        }
    }

    private var agenda: some View {
        SectionCard(
            title: "Observed Topics",
            subtitle: "Derived from durable deliberation and validated artifacts"
        ) {
            if council.topics.isEmpty {
                Text(L10n.text("No observed topics are available.", language: language))
                    .akzioText(.bodySmall, color: AkzioColor.mutedText)
            } else {
                LazyVStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    ForEach(council.topics) { topic in
                        VStack(alignment: .leading, spacing: 5) {
                            HStack(spacing: AkzioLayout.s2) {
                                PillTag(topic.kind.displayName, tone: topic.kind.tone)
                                Spacer(minLength: 0)
                                Text(L10n.text(topic.source, language: language))
                                    .akzioText(.caption, color: AkzioColor.mutedText)
                            }
                            Text(topic.title)
                                .akzioText(.bodySmall, color: AkzioColor.primaryText)
                                .fixedSize(horizontal: false, vertical: true)
                                .textSelection(.enabled)
                        }
                        .padding(AkzioLayout.s2)
                        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 8)
                    }
                }
            }
        }
        .frame(width: 360)
    }

    private var analysisTimeline: some View {
        SectionCard(
            title: "Analysis Process",
            subtitle: "Chronological Observer record · no hidden reasoning"
        ) {
            if council.analysisRecords.isEmpty {
                Text(L10n.text("No analysis process has been observed.", language: language))
                    .akzioText(.bodySmall, color: AkzioColor.mutedText)
            } else {
                LazyVStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    ForEach(council.analysisRecords) { record in
                        AnalysisRecordRow(record: record, showsActor: true)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity)
    }

    private var emptyState: some View {
        SectionCard(title: "Observed Intelligence") {
            ContentUnavailableView {
                Label(
                    L10n.text("No observed analysis yet", language: language),
                    systemImage: "text.bubble"
                )
            } description: {
                Text(L10n.text(
                    "Topics and analysis records appear only after Rust emits Observer-safe trajectory or validated artifacts.",
                    language: language
                ))
            }
        }
        .frame(maxWidth: .infinity, minHeight: 360)
    }
}
