import SwiftUI

// MARK: - Overview
//
// KPI strip on top, Signal Universe as the centrepiece, and a right rail with the
// latest event, the agent roster and the health snapshot. Every section reveals in
// the transition's `reveal` phase with a short stagger.
struct OverviewPage: View {
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    var body: some View {
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
            StatusBadge(store.displayRun.status.status)
                Text(store.elapsedLabel).akzioMono(12, color: AkzioColor.primaryText)
            }
        }
    }

    private var universe: some View {
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
            .frame(minHeight: 400)
        } accessory: {
            HStack(spacing: AkzioLayout.s2) {
                legend("Live", tone: .gold)
                legend("Blocked", tone: .coral)
                legend("Not Triggered", tone: .muted)
            }
        }
    }

    private var rail: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            LatestEventCard(events: store.displayEvents)
            ActiveAgentsList(agents: store.displayAgents, namespace: namespace)
            HealthSnapshotView(metrics: store.displayHealth)
        }
        .frame(width: AkzioLayout.rightRailWidth)
    }

    private func legend(_ label: String, tone: AkzioTone) -> some View {
        HStack(spacing: 4) {
            Circle().fill(tone.color).frame(width: 5, height: 5)
            Text(L10n.text(label, language: language)).akzioText(.caption)
        }
    }
}

private struct LiveKpiStrip: View {
    let store: ObservatoryStore

    @Environment(\.appLanguage) private var language

    var body: some View {
        HStack(spacing: AkzioLayout.s3) {
            metric("Source", store.observerState.label, "dot.radiowaves.left.and.right")
            metric("Tasks", String(store.displayWorkflow.nodes.count), "point.3.connected.trianglepath.dotted")
            metric("Run Status", store.displayRun.status.displayName, "waveform.path.ecg")
            metric("Portfolio", L10n.text("Unavailable", language: language), "chart.line.downtrend.xyaxis")
        }
    }

    private func metric(_ label: String, _ value: String, _ symbol: String) -> some View {
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
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard(padding: AkzioLayout.s3)
    }
}
