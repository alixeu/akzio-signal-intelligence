import SwiftUI

// MARK: - Portfolio
//
// KPI bar, equity curve with range switching, allocation, positions, the execution
// tables and the risk panel. The curve is rebuilt from the scenario seed per range,
// so switching ranges morphs the path instead of re-randomising it.
struct PortfolioPage: View {
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    private var portfolio: PortfolioPresentation {
        store.isLive
            ? store.displayPortfolio
            : PortfolioFixtures.portfolio(scenario: store.scenario, range: store.equityRange)
    }

    var body: some View {
        PageScaffold(route: .portfolio) {
            PageScroll {
                VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                    StagedSection(index: 0) { kpiBar }
                    StagedSection(index: 1) {
                        HStack(alignment: .top, spacing: AkzioLayout.s4) {
                            curveCard
                            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                                AllocationBars(rows: portfolio.allocations)
                                RiskPanel(risk: portfolio.risk)
                            }
                            .frame(width: AkzioLayout.rightRailWidth)
                        }
                    }
                    StagedSection(index: 2) { positions }
                    StagedSection(index: 3) {
                        AllocationFlowCanvas(stages: portfolio.flow)
                    }
                    StagedSection(index: 4) {
                        OrdersFillsTable(
                            orders: portfolio.orders,
                            fills: portfolio.fills,
                            verdict: portfolio.verdict,
                            reconciliation: portfolio.reconciliation
                        )
                    }
                }
            }
        } toolbar: {
            AkzioSegmentedControl(
                selection: Binding(
                    get: { store.equityRange },
                    set: { store.equityRange = $0 }
                ),
                options: availableRanges.map { (value: $0, label: $0.rawValue) }
            )
        }
    }

    private var availableRanges: [EquityRange] {
        store.isLive ? [.oneDay, .fiveDay, .oneMonth, .threeMonth] : EquityRange.allCases
    }

    private var kpiBar: some View {
        HStack(spacing: AkzioLayout.s3) {
            tile("Equity", PpmFormatter.currency(micros: portfolio.equityMicros), portfolio.equityValue, tone: .gold)
                .sharedElement(.equityValue, in: namespace)
            tile(
                "Today",
                PpmFormatter.currency(micros: portfolio.todayPnlMicros, signed: true),
                portfolio.todayPnlValue,
                tone: portfolio.isGain ? .gold : .coral,
                secondary: PpmFormatter.percent(ppm: portfolio.todayPnlPpm)
            )
            tile(
                "Unrealized",
                PpmFormatter.currency(micros: portfolio.unrealizedPnlMicros, signed: true),
                Double(portfolio.unrealizedPnlMicros ?? 0) / PpmFormatter.ppmPerUnit,
                tone: (portfolio.unrealizedPnlPpm ?? 0) >= 0 ? .gold : .coral,
                secondary: PpmFormatter.percent(ppm: portfolio.unrealizedPnlPpm)
            )
            tile(
                "Realized",
                PpmFormatter.currency(micros: portfolio.realizedPnlMicros, signed: true),
                Double(portfolio.realizedPnlMicros ?? 0) / PpmFormatter.ppmPerUnit,
                tone: (portfolio.realizedPnlPpm ?? 0) >= 0 ? .gold : .coral,
                secondary: PpmFormatter.percent(ppm: portfolio.realizedPnlPpm)
            )
        }
    }

    private func tile(
        _ label: String,
        _ value: String,
        _ numeric: Double,
        tone: AkzioTone,
        secondary: String? = nil
    ) -> some View {
        MetricCard(
            label: label,
            value: value,
            numericValue: numeric,
            delta: secondary,
            deltaTone: tone
        )
        .akzioCard()
    }

    private var curveCard: some View {
        SectionCard(
            title: "Equity Curve",
            subtitle: "\(store.equityRange.rawValue) · vs \(portfolio.benchmarkLabel)"
        ) {
            EquityCurveChart(
                curve: portfolio.curve,
                range: store.equityRange,
                benchmarkLabel: portfolio.benchmarkLabel,
                isGain: portfolio.isGain,
                namespace: namespace
            )
        } accessory: {
            HStack(spacing: AkzioLayout.s3) {
                legend("Portfolio", tone: portfolio.isGain ? .gold : .coral, dashed: false)
                legend(portfolio.benchmarkLabel, tone: .neutral, dashed: true)
            }
        }
    }

    private var positions: some View {
        LazyVGrid(
            columns: Array(repeating: GridItem(.flexible(), spacing: AkzioLayout.s3), count: 4),
            spacing: AkzioLayout.s3
        ) {
            ForEach(portfolio.positions) { position in
                PositionCardView(
                    position: position,
                    isSelected: store.selectedPosition == position.asset,
                    namespace: namespace,
                    onSelect: { store.selectedPosition = position.asset }
                )
            }
        }
    }

    private func legend(_ label: String, tone: AkzioTone, dashed: Bool) -> some View {
        HStack(spacing: 4) {
            Rectangle()
                .fill(tone.color.opacity(dashed ? 0.6 : 1))
                .frame(width: 14, height: dashed ? 1 : 1.8)
            Text(L10n.text(label, language: language)).akzioText(.caption)
        }
    }
}
