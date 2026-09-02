import SwiftUI

// MARK: - Learning
//
// Five tabs over the same sealed-outcome evidence. Filters re-rank the retrospective
// stack; a degraded (`ModelUnavailable`) card keeps its numbers and drops its prose.
struct LearningPage: View {
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var cardIndex = 0
    @State private var showsCounterfactual = false
    @State private var activeCategory: RetrospectiveCategory?

    private var learning: LearningPresentation { store.displayLearning }

    private var filteredCards: [RetrospectiveCardPresentation] {
        guard let activeCategory else { return learning.cards }
        return learning.cards.filter { $0.categories.contains(activeCategory) }
    }

    var body: some View {
        PageScaffold(route: .learning) {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                if learning.availabilityStatus == .completed {
                    StagedSection(index: 0) { filters }
                    StagedSection(index: 1) { content }
                } else {
                    StagedSection(index: 0) { pendingLearning }
                }
            }
        } toolbar: {
            AkzioSegmentedControl(
                selection: Binding(
                    get: { store.learningTab },
                    set: { store.learningTab = $0 }
                ),
                options: LearningPresentation.Tab.allCases.map { (value: $0, label: $0.displayName) }
            )
            .disabled(learning.availabilityStatus != .completed)
        }
    }

    private var pendingLearning: some View {
        SectionCard(title: L10n.text("Learning Evidence", language: language)) {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                StatusBadge(learning.availabilityStatus)
                StatusExplanation(
                    learning.availabilityStatus,
                    detail: L10n.text(
                        learning.availabilityReason ?? "No canonical learning artifacts are available yet",
                        language: language
                    )
                )
                Text(L10n.text(
                    "Learning appears only after real Paper outcomes are sealed; Debug runs never create learning data.",
                    language: language
                ))
                .akzioText(.bodySmall, color: AkzioColor.secondaryText)
            }
        }
    }

    // MARK: Filters

    private var filters: some View {
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(learning.timeRangeLabel, language: language)).akzioText(.label)
            HairlineDivider(.vertical).frame(height: 14)
            ForEach(RetrospectiveCategory.allCases) { category in
                Chip(
                    category.displayName,
                    kind: .filter,
                    isSelected: activeCategory == category
                ) {
                    withAnimation(policy.resolve(Motion.control)) {
                        activeCategory = activeCategory == category ? nil : category
                        cardIndex = 0
                    }
                }
            }
            Spacer(minLength: AkzioLayout.s2)
            Text("\(filteredCards.count) / \(learning.cards.count) \(L10n.text("retrospectives", language: language))")
                .akzioMono(10, color: AkzioColor.mutedText)
        }
    }

    // MARK: Tabs

    @ViewBuilder
    private var content: some View {
        switch store.learningTab {
        case .retrospective:
            HStack(alignment: .top, spacing: AkzioLayout.s4) {
                RetrospectiveCardStack(
                    cards: filteredCards,
                    index: Binding(
                        get: { min(cardIndex, max(filteredCards.count - 1, 0)) },
                        set: { cardIndex = $0 }
                    ),
                    showsCounterfactual: $showsCounterfactual
                )
                if showsCounterfactual, let card = filteredCards.indices.contains(cardIndex) ? filteredCards[cardIndex] : nil {
                    detailPanel(card)
                }
            }
        case .timeline:
            ExperienceTimelineCanvas(nodes: learning.timeline, namespace: namespace)
        case .policy:
            PolicyTransitionTrack(tracks: learning.policyTracks)
        case .lessons:
            lessons
        case .impact:
            ImpactSummaryCard(impact: learning.impact)
        }
    }

    /// Detail expands beside the stack; the current card dims but never disappears.
    private func detailPanel(_ card: RetrospectiveCardPresentation) -> some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                Text(L10n.text("Counterfactual", language: language)).akzioText(.sectionTitle)
            Text(card.counterfactual.isEmpty ? MissingValue.unavailable.rawValue : card.counterfactual)
                .akzioText(.bodySmall)
            HairlineDivider()
                    Text(L10n.text("Diagnostic Gaps", language: language)).akzioText(.caption)
            if card.diagnosticGaps.isEmpty {
                        Text(L10n.text("None recorded", language: language)).akzioText(.bodySmall)
            } else {
                ForEach(card.diagnosticGaps, id: \.self) { gap in
                    Text(gap).akzioText(.bodySmall, color: AkzioColor.actionCoral)
                }
            }
            HairlineDivider()
            HStack(spacing: AkzioLayout.s2) {
                    Text(L10n.text("P&L Impact", language: language)).akzioText(.caption)
                Spacer(minLength: 4)
                Text(PpmFormatter.currency(micros: card.pnlMicros, signed: true))
                    .akzioMono(11, color: AkzioColor.primaryText)
            }
            Spacer(minLength: 0)
        }
        .padding(AkzioLayout.s4)
        .frame(width: 288, alignment: .leading)
        .akzioGlass(.elevated)
        .transition(.move(edge: .trailing).combined(with: .opacity))
    }

    private var lessons: some View {
        SectionCard(title: "Lessons", subtitle: "Promoted from sealed retrospectives") {
            if learning.lessonCandidates.isEmpty {
                StatusExplanation(.waiting, detail: "No lesson candidates for this window")
            } else {
                VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                    ForEach(Array(learning.lessonCandidates.enumerated()), id: \.element.id) { index, card in
                        VStack(alignment: .leading, spacing: 3) {
                            HStack(spacing: AkzioLayout.s2) {
                                Image(systemName: "lightbulb")
                                    .font(.system(size: 10, weight: .medium))
                                    .foregroundStyle(AkzioColor.primaryGold)
                                Text(card.lessonCandidate).akzioText(.body, color: AkzioColor.primaryText)
                                Spacer(minLength: AkzioLayout.s2)
                                PillTag(card.conclusion.displayName, tone: card.conclusion.tone)
                            }
                            Text("\(L10n.text("From", language: language)) “\(card.title)” · \(card.dateLabel)").akzioText(.caption)
                        }
                        .staggeredReveal(index: index)
                    }
                }
            }
        }
    }
}
