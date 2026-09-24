import SwiftUI

// MARK: - Outcome
//
// Three horizon rings, the summary for whichever one is selected, and the full metric
// grid. "Go to Learning" hands the completed ring over to the Retrospective badge.
struct OutcomePage: View {
    // store 是 Outcome 的唯一 UI 数据源；selectedHorizon 只选择窗口，不创建或重新密封 Outcome。
    let store: ObservatoryStore

    // namespace 用于跨卡片共享元素，language 只改变本地化文本。
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    private var outcome: OutcomePresentation { store.displayOutcome }

    var body: some View {
        // 各 StagedSection 共享同一 outcome 投影；summary/grid 从 selectedHorizon 读取对应可选窗口。
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
            // toolbar 的交易日统计保留 observedTradingDays 的 Optional 边界；按钮只导航到 Learning，不宣称已完成学习。
            HStack(spacing: AkzioLayout.s2) {
                Text(outcome.observedTradingDays.map { "\($0)/\(outcome.totalTradingDays) \(L10n.text("Trading Sessions", language: language))" } ?? "已观测交易会话：暂无数据")
                    .akzioMono(11, color: AkzioColor.secondaryText)
                Button {
                    // navigate 闭包只改变应用路由，disabled/opacity 同时由是否存在 outcome window 决定。
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
        // rings 用共享 horizon 数组渲染 T+1/T+3/T+5；onSelect 闭包只更新本地选择状态。
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
