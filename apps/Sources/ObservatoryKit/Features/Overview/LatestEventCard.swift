import SwiftUI

// MARK: - Latest event
//
// When a new event arrives the border pulses grey → coral → neutral exactly once.
// It never keeps flashing: a persistent alarm stops being information.
struct LatestEventCard: View {
    let events: [EventPresentation]

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var pulseTick = 0

    private var latest: EventPresentation? { events.first }

    var body: some View {
        SectionCard(title: "Latest Event", subtitle: latest?.relativeLabel) {
            if let latest {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    HStack(spacing: AkzioLayout.s2) {
                        Image(systemName: latest.symbol)
                            .font(.system(size: 13, weight: .medium))
                            .foregroundStyle(latest.severity.tone.color)
                        Text(L10n.text(latest.title, language: language)).akzioText(.sectionTitle)
                        Spacer(minLength: AkzioLayout.s2)
                    PillTag(L10n.text(latest.severity.label, language: language), tone: latest.severity.tone)
                    }
                Text(localizedDetail(latest.detail)).akzioText(.bodySmall)
                    if events.count > 1 {
                        HairlineDivider()
                        VStack(alignment: .leading, spacing: 5) {
                            ForEach(Array(events.dropFirst().enumerated()), id: \.element.id) { index, event in
                                HStack(spacing: AkzioLayout.s2) {
                                    Circle()
                                        .fill(event.severity.tone.color.opacity(0.7))
                                        .frame(width: 5, height: 5)
                                    Text(L10n.text(event.title, language: language)).akzioText(.bodySmall)
                                    Spacer(minLength: AkzioLayout.s2)
                                    Text(L10n.text(event.relativeLabel, language: language)).akzioMono(10, color: AkzioColor.mutedText)
                                }
                                .staggeredReveal(index: index)
                            }
                        }
                    }
                }
            } else {
                StatusExplanation(.queued, detail: "No events recorded for this run yet")
            }
        }
        .overlay {
            // One-shot border pulse, retriggered only when the newest event changes.
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(AkzioColor.actionCoral, lineWidth: 1.4)
                .opacity(0)
                .phaseAnimator([0, 1, 2], trigger: pulseTick) { view, step in
                    view.opacity(step == 1 ? 0.9 : 0)
                } animation: { step in
                    policy.resolve(.easeOut(duration: step == 1 ? 0.22 : 0.42))
                }
                .allowsHitTesting(false)
        }
        .onChange(of: latest?.id) { _, _ in pulseTick += 1 }
    }

    private func localizedDetail(_ detail: String) -> String {
        guard detail.hasPrefix("Task ") else {
            return L10n.text(detail, language: language)
        }
        return "\(L10n.text("Task", language: language)) \(detail.dropFirst(5))"
    }
}
