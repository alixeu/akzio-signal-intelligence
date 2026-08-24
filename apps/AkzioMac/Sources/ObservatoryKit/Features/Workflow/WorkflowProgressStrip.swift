import SwiftUI

// MARK: - Progress strip

struct WorkflowProgressStrip: View {
    let workflow: WorkflowPresentation
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        HStack(spacing: AkzioLayout.s5) {
            ProgressRing(
                progress: workflow.progressFraction,
                tone: .gold,
                lineWidth: 6,
                diameter: 68
            ) {
                Text(PpmFormatter.share(
                    ppm: Int(workflow.progressFraction * PpmFormatter.ppmPerUnit),
                    fractionDigits: 0
                ))
                .akzioMono(11, color: AkzioColor.primaryText)
            }
            .frame(width: 68, height: 68)
            .sharedElement(.workflowProgress, in: namespace)

            counter("Completed", workflow.completedCount, tone: .gold, symbol: "checkmark.circle")
            counter("Active", workflow.activeCount, tone: .gold, symbol: "circle.dotted")
            counter("Queued", workflow.queuedCount, tone: .neutral, symbol: "clock")
            counter("Critical Alerts", workflow.alertCount, tone: .coral, symbol: "exclamationmark.triangle")

            Spacer(minLength: AkzioLayout.s3)

            VStack(alignment: .trailing, spacing: 2) {
                Text(L10n.text("Observation", language: language)).akzioText(.caption)
                Text("\(workflow.observedTradingDays)/\(workflow.totalTradingDays) \(L10n.text("Trading Sessions", language: language))")
                    .akzioMono(11, color: AkzioColor.primaryText)
            }
        }
        .akzioCard()
    }

    private func counter(_ label: String, _ value: Int, tone: AkzioTone, symbol: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: 4) {
                Image(systemName: symbol)
                    .font(.system(size: 9, weight: .medium))
                    .foregroundStyle(tone.color)
                Text(L10n.text(label, language: language)).akzioText(.caption)
            }
            Text(PpmFormatter.count(value))
                .akzioMetric(22, color: value == 0 && tone == .coral ? AkzioColor.mutedText : tone.color)
                .akzioNumeric(value, policy: policy)
        }
        .frame(minWidth: 84, alignment: .leading)
    }
}
