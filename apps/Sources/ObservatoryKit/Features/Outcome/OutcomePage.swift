import SwiftUI

// MARK: - Outcome
//
// Three horizon rings, the summary for whichever one is selected, and the full metric
// grid. "Go to Learning" hands the completed ring over to the Retrospective badge.
struct OutcomePage: View {
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    private var outcome: OutcomePresentation { store.displayOutcome }

    var body: some View {
        PageScaffold(route: .outcome) {
            PageScroll {
                VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                    StagedSection(index: 0) { rings }
                    StagedSection(index: 1) {
                        OutcomeSummaryCard(
                            window: outcome.window(store.selectedHorizon),
                            horizon: store.selectedHorizon,
                            namespace: namespace
                        )
                    }
                    StagedSection(index: 2) {
                        OutcomeMetricGrid(window: outcome.window(store.selectedHorizon))
                    }
                }
            }
        } toolbar: {
            HStack(spacing: AkzioLayout.s2) {
                Text("\(outcome.observedTradingDays)/\(outcome.totalTradingDays) \(L10n.text("Trading Sessions", language: language))")
                    .akzioMono(11, color: AkzioColor.secondaryText)
                Button {
                    store.navigate(to: .learning)
                } label: {
                    HStack(spacing: 5) {
                        Text(L10n.text("Go to Learning", language: language))
                            .akzioText(.label, color: AkzioColor.primaryGold)
                        Image(systemName: "arrow.right")
                            .font(.system(size: 9, weight: .semibold))
                            .foregroundStyle(AkzioColor.primaryGold)
                    }
                    .padding(.horizontal, AkzioLayout.s2)
                    .padding(.vertical, 5)
                    .background(
                        RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                            .fill(AkzioColor.gold(0.12))
                    )
                }
                .buttonStyle(PressableButtonStyle())
                .disabled(outcome.windows.isEmpty)
                .opacity(outcome.windows.isEmpty ? 0.45 : 1)
                .help(L10n.text(
                    outcome.windows.isEmpty ? "No sealed outcome to learn from yet" : "Open Learning & Experience",
                    language: language
                ))
            }
        }
    }

    private var rings: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            HStack(spacing: AkzioLayout.s3) {
                ForEach(outcome.horizons) { horizon in
                    HorizonRingView(
                        horizon: horizon,
                        isSelected: horizon.horizon == store.selectedHorizon,
                        namespace: namespace,
                        onSelect: { store.selectedHorizon = horizon.horizon }
                    )
                }
            }
            if let reason = outcome.availabilityReason {
                HairlineDivider()
                StatusExplanation(
                    outcome.availabilityStatus,
                    detail: L10n.text(reason, language: language)
                )
            }
        }
        .akzioCard()
    }
}
