import SwiftUI

// MARK: - Health snapshot
//
// Five vitals. A gauge only turns coral once the metric is actually in its risk
// band, and an absent metric leaves the bar empty rather than drawing 0%.
struct HealthSnapshotView: View {
    let metrics: [HealthMetric]

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        SectionCard(
            title: "Health Snapshot",
            subtitle: "Risk, data and process vitals",
            padding: AkzioLayout.s3
        ) {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                ForEach(Array(metrics.enumerated()), id: \.element.id) { index, metric in
                    row(metric)
                        .staggeredReveal(index: index)
                }
            }
        }
    }

    private func row(_ metric: HealthMetric) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: AkzioLayout.s2) {
                Text(L10n.text(metric.label, language: language)).akzioText(.label)
                Spacer(minLength: AkzioLayout.s2)
                Text(L10n.text(metric.value, language: language))
                    .akzioMono(12, color: metric.isElevatedRisk ? AkzioColor.actionCoral : AkzioColor.primaryText)
                    .akzioNumeric(metric.value, policy: policy)
            }
            if metric.fraction != nil {
                RatioBar(fraction: metric.fraction, tone: metric.tone, height: 4)
            }
            if metric.isElevatedRisk {
                HStack(spacing: 4) {
                    Image(systemName: "exclamationmark.triangle")
                        .font(.system(size: 9, weight: .medium))
                    Text(L10n.text("Elevated risk band", language: language))
                        .akzioText(.caption, color: AkzioColor.actionCoral)
                }
                .foregroundStyle(AkzioColor.actionCoral)
            }
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel(
            "\(L10n.text(metric.label, language: language)): \(L10n.text(metric.value, language: language))"
        )
    }
}
