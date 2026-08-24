import SwiftUI

// MARK: - Impact
//
// What the learning loop actually produced: money, lessons, evolved policies and the
// areas they touched. Counts are real counts, so zero is a legitimate value here.
struct ImpactSummaryCard: View {
    let impact: ImpactSummaryPresentation

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        SectionCard(title: "Impact", subtitle: "Attributed to sealed outcomes") {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                HStack(spacing: AkzioLayout.s4) {
                    headline(
                        "Attributed Utility",
                    impact.totalImpactMicros.map {
                        PpmFormatter.currency(micros: $0, signed: true)
                    } ?? MissingValue.unavailable.rawValue,
                    Double(impact.totalImpactMicros ?? 0) / PpmFormatter.ppmPerUnit,
                    tone: impact.totalImpactPpm >= 0 ? .gold : .coral
                )
                    headline(
                        "Lessons Created",
                        PpmFormatter.count(impact.lessonsCreated),
                        Double(impact.lessonsCreated),
                        tone: .neutral,
                        delta: impact.lessonsDelta
                    )
                    headline(
                        "Policies Evolved",
                        PpmFormatter.count(impact.policiesEvolved),
                        Double(impact.policiesEvolved),
                        tone: .neutral,
                        delta: impact.policiesDelta
                    )
                }
                HairlineDivider()
            Text(L10n.text("Top Impact Areas", language: language)).akzioText(.caption)
                ForEach(Array(impact.areas.enumerated()), id: \.element.id) { index, area in
                    HStack(spacing: AkzioLayout.s2) {
                        Text(area.label).akzioText(.bodySmall).frame(width: 96, alignment: .leading)
                        RatioBar(
                            fraction: min(abs(Double(area.impactPpm)) / 20_000, 1),
                            tone: area.impactPpm >= 0 ? .gold : .coral,
                            height: 5
                        )
                        Text(PpmFormatter.percent(ppm: area.impactPpm))
                            .akzioMono(10, color: area.impactPpm >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                            .frame(width: 62, alignment: .trailing)
                    }
                    .staggeredReveal(index: index)
                }
            }
        }
    }

    private func headline(
        _ label: String,
        _ value: String,
        _ numeric: Double,
        tone: AkzioTone,
        delta: Int? = nil
    ) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value)
                .akzioMetric(22, color: tone == .neutral ? AkzioColor.primaryText : tone.color)
                .akzioCountUp(numeric, policy: policy)
            if let delta {
                Text(delta == 0
                    ? L10n.text("No change", language: language)
                    : "+\(delta) \(L10n.text("this window", language: language))")
                    .akzioMono(10, color: AkzioColor.mutedText)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
