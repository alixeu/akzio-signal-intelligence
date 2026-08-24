import SwiftUI

// MARK: - Run status bar
//
// Low glass bar pinned to the top: identity on the left, live vitals in the
// middle, controls on the right. The running dot breathes slowly (2s) instead of
// blinking, so a long-running run never feels like an alarm.
struct RunStatusBar: View {
    let run: RunPresentation
    let health: [HealthMetric]
    let observerState: ObserverConnectionState
    let namespace: Namespace.ID?
    let canRun: Bool
    let runInFlight: Bool
    let runMessage: String
    let onRun: () -> Void
    let onOpenSettings: () -> Void
    let onCopyRunID: () -> Void
    let onRevealRun: () -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        HStack(spacing: AkzioLayout.s4) {
            identity
            HairlineDivider(.vertical).frame(height: 18)
            vitals
            Spacer(minLength: AkzioLayout.s4)
            controls
        }
        .padding(.trailing, AkzioLayout.s4)
        .padding(.leading, AkzioLayout.s4)
        .frame(
            minHeight: AkzioLayout.statusBarHeight,
            idealHeight: AkzioLayout.statusBarHeight,
            maxHeight: AkzioLayout.statusBarHeight
        )
        .fixedSize(horizontal: false, vertical: true)
        .layoutPriority(1)
        .akzioGlass(.base, radius: 0)
        .overlay(alignment: .bottom) { HairlineDivider() }
    }

    // MARK: Left

    private var identity: some View {
        HStack(spacing: AkzioLayout.s2) {
            Text(run.shortId)
                .akzioMono(11, color: AkzioColor.primaryText)
                .sharedElement(.runIdentifier, in: namespace)
            Text(run.idPrefix)
                .akzioMono(10, color: AkzioColor.mutedText)
                .help(run.runId)
            PillTag(L10n.text(run.purpose.displayName, language: language), tone: run.purpose.tone)
        }
    }

    // MARK: Middle

    private var vitals: some View {
        HStack(spacing: AkzioLayout.s4) {
            HStack(spacing: 6) {
                StatusDot(run.status.status)
            Text(L10n.text(run.status.displayName, language: language))
                    .akzioText(.label, color: AkzioColor.primaryText)
            }
            .sharedElement(.runStatus, in: namespace)

            metric("Elapsed", PpmFormatter.elapsed(seconds: run.elapsedSeconds))
            metric("Topology", run.topology)
            metric("Model", run.model)
            metric("Session", run.brokerSession)
            systemHealth
        }
        .lineLimit(1)
    }

    private func metric(_ label: String, _ value: String) -> some View {
        HStack(spacing: AkzioLayout.s1) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(L10n.text(value, language: language))
                .akzioMono(11, color: AkzioColor.primaryText)
        }
    }

    private var systemHealth: some View {
        HStack(spacing: 6) {
            Text(L10n.text("Health", language: language)).akzioText(.caption)
            Text(PpmFormatter.share(ppm: run.systemHealthPpm, fractionDigits: 1))
                .akzioMono(11, color: AkzioColor.primaryText)
                .akzioNumeric(run.systemHealthPpm, policy: policy)
            RatioBar(fraction: PpmFormatter.fraction(ppm: run.systemHealthPpm), tone: .gold, height: 4)
                .frame(width: 54)
        }
    }

    // MARK: Right

    private var controls: some View {
        HStack(spacing: AkzioLayout.s3) {
            marketChip
            dataChip
            latencyChip
            Button(action: onRun) {
                HStack(spacing: 5) {
                    if runInFlight {
                        ProgressView().controlSize(.mini)
                    } else {
                        Image(systemName: "play.fill")
                    }
                    Text(L10n.text(runInFlight ? "Running…" : "Run", language: language))
                }
            }
            .buttonStyle(PressableButtonStyle())
            .disabled(!canRun || runInFlight)
            .help(L10n.text(
                "Start one real-model run in Debug safety mode. Paper submission remains scheduler-owned.",
                language: language
            ))
            Menu {
                Section(L10n.text("Run controls", language: language)) {
                    Text(L10n.text("Real model · Debug safety mode", language: language))
                    if !runMessage.isEmpty {
                        Text(L10n.text(runMessage, language: language))
                    }
                }
                Button(L10n.text("Copy Run ID", language: language), action: onCopyRunID)
                Button(L10n.text("Reveal Run in Archive", language: language), action: onRevealRun)
                Divider()
                Button(L10n.text("Open Settings", language: language), action: onOpenSettings)
            } label: {
                Image(systemName: "slider.horizontal.3")
                    .font(.system(size: 12, weight: .medium))
                    .foregroundStyle(AkzioColor.secondaryText)
            }
            .menuStyle(.borderlessButton)
            .menuIndicator(.hidden)
            .frame(width: 26)
        .accessibilityLabel(L10n.text("Run controls", language: language))
        }
    }

    private var marketChip: some View {
        HStack(spacing: 5) {
            Circle()
                .fill(run.marketOpen ? AkzioColor.successDot : AkzioColor.mutedText)
                .frame(width: 6, height: 6)
            Text(L10n.text(run.marketOpen ? "Market Open" : "Market Closed", language: language)).akzioText(.label)
        }
    }

    private var dataChip: some View {
        HStack(spacing: 5) {
            StatusDot(effectiveDataStale ? .stale : run.dataStatus, diameter: 6)
            Text(L10n.text(
                dataLabel,
                language: language
            ))
                .akzioText(.label, color: effectiveDataStale ? AkzioColor.actionCoral : AkzioColor.secondaryText)
        }
    }

    private var latencyChip: some View {
        Text(PpmFormatter.latency(millis: run.latencyMillis))
            .akzioMono(11, color: effectiveDataStale ? AkzioColor.actionCoral : AkzioColor.mutedText)
            .akzioNumeric(run.latencyMillis, policy: policy)
    }

    private var effectiveDataStale: Bool {
        if case .stale = observerState { return true }
        if case .offline = observerState { return true }
        return run.dataStale
    }

    private var dataLabel: String {
        switch observerState {
        case .connecting: "Data Queued"
        case .stale, .offline: "Data Stale"
        case .mock, .connected:
            run.dataStale ? "Data Stale" : (run.dataLive ? "Data Live" : "Data Queued")
        }
    }
}
