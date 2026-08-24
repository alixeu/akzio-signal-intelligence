import SwiftUI

// MARK: - KPI strip
//
// Four tiles. Values count up rather than swap, and the equity value plus its
// sparkline are the shared elements that fly into the Portfolio page.
struct KpiStripView: View {
    let portfolio: PortfolioPresentation
    let workflow: WorkflowPresentation
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    private var confidencePpm: Int? { workflow.inspector.confidencePpm }

    var body: some View {
        HStack(spacing: AkzioLayout.s3) {
            equityCard
            todayCard
            confidenceCard
            progressCard
        }
    }

    // MARK: Tiles

    private var equityCard: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(L10n.text("Total Equity", language: language)).akzioText(.caption)
            Text(PpmFormatter.currency(micros: portfolio.equityMicros))
                .akzioMetric(26)
                .akzioCountUp(portfolio.equityValue, policy: policy)
                .lineLimit(1)
                .minimumScaleFactor(0.7)
                .sharedElement(.equityValue, in: namespace)
            MiniSparkline(
                values: portfolio.sparkSeries,
                tone: portfolio.isGain ? .gold : .coral,
                showsLatestPoint: true
            )
            .frame(height: 26)
            .sharedElement(.sparkline, in: namespace)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard()
    }

    private var todayCard: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(L10n.text("Today P&L", language: language)).akzioText(.caption)
            Text(PpmFormatter.currency(micros: portfolio.todayPnlMicros, signed: true))
                .akzioMetric(26, color: portfolio.isGain ? AkzioColor.primaryText : AkzioColor.actionCoral)
                .akzioCountUp(portfolio.todayPnlValue, policy: policy)
                .lineLimit(1)
                .minimumScaleFactor(0.7)
            HStack(spacing: 6) {
                Text(PpmFormatter.percent(ppm: portfolio.todayPnlPpm))
                    .font(AkzioFont.mono(11))
                    .foregroundStyle(portfolio.isGain ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                    .akzioNumeric(portfolio.todayPnlPpm, policy: policy)
                Text(L10n.text("vs \(portfolio.benchmarkLabel)", language: language))
                    .akzioMono(11, color: AkzioColor.mutedText)
            }
            .frame(height: 26)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard()
    }

    private var confidenceCard: some View {
        HStack(spacing: AkzioLayout.s3) {
            VStack(alignment: .leading, spacing: 6) {
            Text(L10n.text("Decision Confidence", language: language)).akzioText(.caption)
                if let confidencePpm {
                    Text(PpmFormatter.share(ppm: confidencePpm))
                        .akzioMetric(26)
                        .akzioNumeric(confidencePpm, policy: policy)
                } else {
                    UnavailableValue(.unavailable, size: 20)
                }
            Text(L10n.text(workflow.inspector.stageTitle, language: language))
                .akzioMono(11, color: AkzioColor.mutedText)
                    .lineLimit(1)
            }
            Spacer(minLength: 0)
            ProgressRing(
                progress: PpmFormatter.fraction(ppm: confidencePpm),
                tone: .gold,
                lineWidth: 5,
                diameter: 54
            )
            .frame(width: 54, height: 54)
            .sharedElement(.confidenceRing, in: namespace)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard()
    }

    private var progressCard: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(L10n.text("Workflow Progress", language: language)).akzioText(.caption)
            Text(PpmFormatter.share(
                ppm: Int(workflow.progressFraction * PpmFormatter.ppmPerUnit),
                fractionDigits: 0
            ))
            .akzioMetric(26)
            .akzioCountUp(workflow.progressFraction, policy: policy)
            SegmentedProgressBar(completed: workflow.completedCount, total: workflow.nodes.count)
                .frame(height: 26)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard()
        .sharedElement(.workflowProgress, in: namespace)
    }
}
