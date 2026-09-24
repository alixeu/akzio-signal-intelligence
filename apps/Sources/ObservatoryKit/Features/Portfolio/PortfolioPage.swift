import SwiftUI

// MARK: - Portfolio
//
// KPI bar, equity curve with range switching, allocation, positions, the execution
// tables and the risk panel. The curve is rebuilt from the scenario seed per range,
// so switching ranges morphs the path instead of re-randomising it.
struct PortfolioPage: View {
    // store 同时持有 live Observatory 投影与本地 scenario；页面在这里明确区分两者，不把 fixture 当成真实账户数据。
    let store: ObservatoryStore

    // namespace 只用于跨组件共享元素，language 只影响文案。
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    private var portfolio: PortfolioPresentation {
        // live 走 displayPortfolio；非 live 走带 scenario/range 的确定性 fixture，二者共享同一展示模型但证据边界不同。
        store.isLive
            ? store.displayPortfolio
            : PortfolioFixtures.portfolio(scenario: store.scenario, range: store.equityRange)
    }

    var body: some View {
        // 各子视图只接收 portfolio 的已投影字段；toolbar 的 Binding 只改变范围选择，不重新生成业务事实。
        PageScaffold(route: .portfolio) {
            PageScroll {
                VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                    StagedSection(index: 0) { kpiBar }
                    StagedSection(index: 1) {
                        HStack(alignment: .top, spacing: AkzioLayout.s4) {
                            curveCard
                            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                                AllocationBars(rows: portfolio.allocations, subtitle: portfolio.allocationSubtitle)
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
            // equityRange 的 get/set 连接 store；availableRanges 已在下方按 mock/live 边界筛选。
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
        // live 只暴露 provider 支持的短范围；fixture 可展示全部枚举以便离线检查 UI 投影。
        store.isLive ? [.oneDay, .fiveDay, .oneMonth, .threeMonth] : EquityRange.allCases
    }

    private var kpiBar: some View {
        // KPI 的文本、动画数值和正负 tone 都来自同一 portfolio snapshot；可选 P&L 由 formatter 保留缺失语义。
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
        // tile 只是把 label/value/numeric/delta 传给 MetricCard，不在卡片层重新计算金额。
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
        // EquityCurveChart 接收当前 range 的 curve；legend 只解释 Portfolio 与 benchmark 两条投影线。
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
        // positions 按 portfolio 投影逐项渲染，选择闭包只更新 store.selectedPosition。
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
        // legend 的 dashed 仅是视觉标记，label 仍经 language 本地化，不改变曲线数据。
        HStack(spacing: 4) {
            Rectangle()
                .fill(tone.color.opacity(dashed ? 0.6 : 1))
                .frame(width: 14, height: dashed ? 1 : 1.8)
            Text(L10n.text(label, language: language)).akzioText(.caption)
        }
    }
}
