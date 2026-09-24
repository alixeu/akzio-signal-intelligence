import SwiftUI

// MARK: - Overview
//
// KPI strip on top, Signal Universe as the centrepiece, and a right rail with the
// latest event, the agent roster and the health snapshot. Every section reveals in
// the transition's `reveal` phase with a short stagger.
struct OverviewPage: View {
    // store 是 Overview 的只读数据入口；页面根据 isLive 在真实摘要和 Mock KPI 间分流。
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    var body: some View {
        // 页面按阶段顺序组合 KPI、Signal Universe 和右侧信息栏，子视图通过闭包回写选择。
        PageScaffold(route: .overview) {
            PageScroll {
                VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                    StagedSection(index: 0) {
                        if store.isLive {
                            LiveKpiStrip(store: store)
                        } else {
                            KpiStripView(
                                portfolio: store.displayPortfolio,
                                workflow: store.displayWorkflow,
                                namespace: namespace
                            )
                        }
                    }
                    HStack(alignment: .top, spacing: AkzioLayout.s4) {
                        StagedSection(index: 1) {
                            universe
                        }
                        StagedSection(index: 2) {
                            rail
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .topLeading)
            }
        } toolbar: {
            HStack(spacing: AkzioLayout.s2) {
            PillTag(store.displayRun.purpose.displayName, tone: store.displayRun.purpose.tone)
            StatusBadge(store.displayRun.hasRun ? store.displayRun.status.status : .unavailable)
                Text(store.elapsedLabel).akzioMono(12, color: AkzioColor.primaryText)
            }
        }
    }

    private var universe: some View {
        // Canvas 读取 workflow 投影，selectedStageID 和 onSelect 形成选中节点的数据回路。
        SectionCard(
            title: "Signal Universe",
            subtitle: "\(store.displayWorkflow.nodes.count) \(L10n.text("stages", language: language)) · \(L10n.text(store.displayScenarioTitle, language: language))"
        ) {
            SignalUniverseCanvas(
                workflow: store.displayWorkflow,
                namespace: namespace,
                selectedStageID: store.selectedStageID,
                onSelect: { store.selectedStageID = $0 }
            )
            .frame(height: 400)
        } accessory: {
            HStack(spacing: AkzioLayout.s2) {
                legend("Live", tone: .gold)
                legend("Blocked", tone: .coral)
                legend("Not Triggered", tone: .muted)
            }
        }
    }

    private var rail: some View {
        // 右栏按事件、Agent、健康指标顺序消费同一 Store 展示快照。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            LatestEventCard(events: store.displayEvents)
            ActiveAgentsList(agents: store.displayAgents, namespace: namespace)
            HealthSnapshotView(metrics: store.displayHealth)
        }
        .frame(width: AkzioLayout.rightRailWidth)
    }

    private func legend(_ label: String, tone: AkzioTone) -> some View {
        // 图例闭包只把固定标签和色调组合成说明项，不参与状态更新。
        HStack(spacing: 4) {
            Circle().fill(tone.color).frame(width: 5, height: 5)
            Text(L10n.text(label, language: language)).akzioText(.caption)
        }
    }
}

private struct LiveKpiStrip: View {
    // 在线状态只展示 Core 来源和数量等摘要，Portfolio 仍明确显示不可用。
    let store: ObservatoryStore

    @Environment(\.appLanguage) private var language

    var body: some View {
        // 实时摘要直接读取 Store 状态，不复用离线曲线 KPI 的数值模型。
        HStack(spacing: AkzioLayout.s3) {
            metric("Source", store.observerState.label, "dot.radiowaves.left.and.right")
            metric("Tasks", String(store.displayWorkflow.nodes.count), "point.3.connected.trianglepath.dotted")
            metric("Run Status", store.displayRun.displayStatus, "waveform.path.ecg")
            metric("Portfolio", L10n.text("Unavailable", language: language), "chart.line.downtrend.xyaxis")
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func metric(_ label: String, _ value: String, _ symbol: String) -> some View {
        // metric 闭包把一个标签、值和图标变成只读卡片，调用方决定数据来源。
        HStack(spacing: AkzioLayout.s3) {
            Image(systemName: symbol)
                .font(.system(size: 14, weight: .medium))
                .foregroundStyle(AkzioColor.primaryGold)
            VStack(alignment: .leading, spacing: 2) {
                Text(L10n.text(label, language: language)).akzioText(.caption)
                Text(L10n.text(value, language: language))
                    .akzioMono(13, color: AkzioColor.primaryText)
            }
            Spacer(minLength: 0)
        }
        .frame(
            maxWidth: .infinity,
            minHeight: overviewKpiCardMinHeight,
            alignment: .topLeading
        )
        .akzioCard(padding: AkzioLayout.s3)
    }
}
