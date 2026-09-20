import SwiftUI

// MARK: - Retrospective stack
//
// One readable retrospective at a time, with explicit previous/next controls.
struct RetrospectiveCardStack: View {
    let cards: [RetrospectiveCardPresentation]
    @Binding var index: Int
    @Binding var showsCounterfactual: Bool

    @Environment(\.motionPolicy) private var policy
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    var body: some View {
        if cards.isEmpty {
            SectionCard(title: "Retrospective") {
                StatusExplanation(.waiting, detail: "Retrospectives require a sealed Paper outcome")
            }
        } else {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                stack
                controls
            }
        }
    }

    private var stack: some View {
        // Translucent cards must not overlap: neighbouring text remains visible
        // through the material even when the current card has a higher zIndex.
        cardView(cards[min(max(index, 0), cards.count - 1)], isCurrent: true)
            .frame(height: 268)
    }

    private func cardView(_ card: RetrospectiveCardPresentation, isCurrent: Bool) -> some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: card.conclusion.symbol)
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(card.conclusion.tone.color)
            .sharedElement(isCurrent ? .retrospectiveBadge : .noSharedElement, in: isCurrent ? namespace : nil)
                VStack(alignment: .leading, spacing: 1) {
                    Text(card.title).akzioText(.sectionTitle).lineLimit(2)
                    Text("\(card.dateLabel) · \(L10n.text(card.conclusion.displayName, language: language))").akzioText(.caption)
                }
                Spacer(minLength: AkzioLayout.s2)
                if card.isDegraded {
                    PillTag(RetrospectiveStatus.modelUnavailable.displayName, tone: .muted)
                }
            }
            HairlineDivider()
            HStack(alignment: .top, spacing: AkzioLayout.s4) {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    if card.isDegraded {
                        // Outcome numbers survive; invented conclusions do not.
                        StatusExplanation(.unavailable, detail: "Retrospective model was unavailable for this run")
                        ForEach(card.diagnosticGaps, id: \.self) { gap in
                            Text(gap).akzioText(.bodySmall, color: AkzioColor.mutedText)
                        }
                    } else {
                        labelled("Counterfactual", card.counterfactual)
                        labelled("Lesson Candidate", card.lessonCandidate)
                    }
                    HStack(spacing: 5) {
                        ForEach(card.tags, id: \.self) { tag in
                            PillTag(tag, tone: .neutral)
                        }
                    }
                }
                VStack(alignment: .trailing, spacing: AkzioLayout.s2) {
                    Text(PpmFormatter.currency(micros: card.pnlMicros, signed: true))
                        .akzioMetric(18, color: (card.impactPpm ?? 0) >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                    Text(PpmFormatter.percent(ppm: card.impactPpm)).akzioMono(11)
                    MiniSparkline(
                        values: card.spark,
                        tone: (card.impactPpm ?? 0) >= 0 ? .gold : .coral
                    )
                    .frame(width: 148, height: 40)
                }
            }
            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard(
            border: card.isDegraded ? AkzioColor.hairline : AkzioColor.goldHairline
        )
    }

    private func labelled(_ title: String, _ body: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(title, language: language)).akzioText(.caption)
            Text(body).akzioText(.bodySmall)
        }
    }

    private var controls: some View {
        HStack(spacing: AkzioLayout.s2) {
            Button { step(-1) } label: { Image(systemName: "chevron.left") }
                .buttonStyle(PressableButtonStyle())
                .disabled(index <= 0)
            Button { step(1) } label: { Image(systemName: "chevron.right") }
                .buttonStyle(PressableButtonStyle())
                .disabled(index >= cards.count - 1)
            Text("\(index + 1) / \(cards.count)").akzioMono(11, color: AkzioColor.mutedText)
            Spacer(minLength: AkzioLayout.s2)
            Button {
                withAnimation(policy.resolve(Motion.panel)) { showsCounterfactual.toggle() }
            } label: {
                Text(L10n.text(showsCounterfactual ? "Hide Detail" : "Show Detail", language: language))
                    .akzioText(.label, color: AkzioColor.primaryGold)
            }
            .buttonStyle(PressableButtonStyle())
        }
        .font(.system(size: 10, weight: .semibold))
        .foregroundStyle(AkzioColor.secondaryText)
    }

    private func step(_ delta: Int) {
        withAnimation(policy.resolve(.spring(response: 0.48, dampingFraction: 0.95))) {
            index = min(max(index + delta, 0), cards.count - 1)
        }
    }
}
