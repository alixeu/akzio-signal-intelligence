import SwiftUI

let overviewKpiCardMinHeight: CGFloat = 44

// MARK: - KPI strip
//
// Four tiles. Values count up rather than swap, and the equity value plus its
// sparkline are the shared elements that fly into the Portfolio page.
struct KpiStripView: View {
    // 三份展示投影分别提供资产、流程和共享元素来源；KPI 不拥有这些数据的生命周期。
    let portfolio: PortfolioPresentation
    let workflow: WorkflowPresentation
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    private var confidencePpm: Int? { workflow.inspector.confidencePpm }

    var body: some View {
        // 四张卡读取同一批投影，Environment 只提供动效和语言策略。
        HStack(spacing: AkzioLayout.s3) {
            equityCard
            todayCard
            confidenceCard
            progressCard
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    // MARK: Tiles

    private var equityCard: some View {
        // 资产数值和 sparkline 使用同一 portfolio 快照，并把两个元素注册给 Portfolio 页面。
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
        .frame(
            maxWidth: .infinity,
            minHeight: overviewKpiCardMinHeight,
            alignment: .topLeading
        )
        .akzioCard()
    }

    private var todayCard: some View {
        // 当日盈亏同时显示绝对值、百分比和 benchmark 标签，全部来自 portfolio 投影。
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
        .frame(
            maxWidth: .infinity,
            minHeight: overviewKpiCardMinHeight,
            alignment: .topLeading
        )
        .akzioCard()
    }

    private var confidenceCard: some View {
        // confidencePpm 为空时保持 Unavailable；可用时数值和圆环共享同一置信度输入。
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
        .frame(
            maxWidth: .infinity,
            minHeight: overviewKpiCardMinHeight,
            alignment: .topLeading
        )
        .akzioCard()
    }

    private var progressCard: some View {
        // 工作流进度由节点数量和完成数派生，不把百分比反写回 WorkflowPresentation。
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
        .frame(
            maxWidth: .infinity,
            minHeight: overviewKpiCardMinHeight,
            alignment: .topLeading
        )
        .akzioCard()
        .sharedElement(.workflowProgress, in: namespace)
    }
}
