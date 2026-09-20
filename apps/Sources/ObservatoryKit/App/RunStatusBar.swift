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
    let selectedRunPurpose: RunPurpose
    let runInFlight: Bool
    let runMessage: String
    let onSelectRunPurpose: (RunPurpose) -> Void
    let onRun: () -> Void
    let leadingPadding: CGFloat
    let onOpenSettings: () -> Void
    let onCopyRunID: () -> Void
    let onRevealRun: () -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // 状态栏只读当前投影；Run/设置/归档操作通过闭包回传给 Store，不在 View 内持有业务状态。
        HStack(spacing: AkzioLayout.s4) {
            identity
            HairlineDivider(.vertical).frame(height: 18)
            ViewThatFits(in: .horizontal) {
                vitals.fixedSize()
                HStack(spacing: AkzioLayout.s3) {
                    StatusDot(run.hasRun ? run.status.status : .unavailable)
                    Text(L10n.text(run.displayStatus, language: language)).akzioText(.label)
                    metric("Elapsed", PpmFormatter.elapsed(seconds: run.elapsedSeconds))
                }.fixedSize()
            }
            Spacer(minLength: AkzioLayout.s4)
            controls
        }
        .padding(.trailing, AkzioLayout.s4)
        .padding(.leading, leadingPadding)
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

    @ViewBuilder
    private var identity: some View {
        HStack(spacing: AkzioLayout.s2) {
            if run.runId.caseInsensitiveCompare(MissingValue.unavailable.rawValue) == .orderedSame {
                Text(L10n.text(MissingValue.unavailable.rawValue, language: language))
                    .akzioMono(11, color: AkzioColor.mutedText)
                    .help("No active run is currently available.")
            } else {
                Text(run.shortId)
                    .akzioMono(11, color: AkzioColor.primaryText)
                    .sharedElement(.runIdentifier, in: namespace)
                    .help("Run ID: \(run.runId)")
                Text(run.idPrefix)
                    .akzioMono(10, color: AkzioColor.mutedText)
                    .help("Run ID prefix: \(run.runId)")
            }
            PillTag(L10n.text(run.purpose.displayName, language: language), tone: run.purpose.tone)
        }
    }

    // MARK: Middle

    private var vitals: some View {
        HStack(spacing: AkzioLayout.s4) {
            HStack(spacing: 6) {
                StatusDot(run.hasRun ? run.status.status : .unavailable)
            Text(L10n.text(run.displayStatus, language: language))
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
        // 控件可见性由当前连接、Run purpose 和 in-flight 状态共同决定；禁用只阻止重复提交，不取消已有请求。
        HStack(spacing: AkzioLayout.s3) {
            if observerState == .mock {
                Label("Mock", systemImage: "rectangle.dashed")
                    .akzioText(.label, color: AkzioColor.primaryGold)
            } else {
                marketChip
                dataChip
                latencyChip
            }
            runModePicker
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
                selectedRunPurpose.launchModeDescription,
                language: language
            ))
            Menu {
                Section(L10n.text("Run controls", language: language)) {
                    Text(L10n.text(selectedRunPurpose.launchModeSummary, language: language))
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

    private var runModePicker: some View {
        // 仅列出 Rust 允许的 userLaunchModes；选择结果先回传 Store，再由 Store 校验是否正在运行。
        Menu {
            Section(L10n.text("Run mode", language: language)) {
                ForEach(RunPurpose.userLaunchModes, id: \.self) { purpose in
                    Button {
                        onSelectRunPurpose(purpose)
                    } label: {
                        Label(
                            L10n.text(purpose.launchModeName, language: language),
                            systemImage: purpose == selectedRunPurpose
                                ? "checkmark.circle.fill"
                                : "circle"
                        )
                    }
                }
            }
        } label: {
            HStack(spacing: 5) {
                Image(systemName: selectedRunPurpose == .positionPlan
                    ? "list.bullet.rectangle"
                    : "point.3.connected.trianglepath.dotted")
                Text(L10n.text(selectedRunPurpose.launchModeName, language: language))
                Image(systemName: "chevron.down")
                    .font(.system(size: 8, weight: .semibold))
            }
            .lineLimit(1)
        }
        .menuStyle(.borderlessButton)
        .menuIndicator(.hidden)
        .fixedSize()
        .disabled(runInFlight)
        .help(L10n.text(selectedRunPurpose.launchModeDescription, language: language))
    }

    private var marketChip: some View {
        HStack(spacing: 5) {
            Circle()
                .fill(run.marketOpen ? AkzioColor.successDot : AkzioColor.mutedText)
                .frame(width: 6, height: 6)
            Text(L10n.text(run.marketStatusKnown ? (run.marketOpen ? "Market Open" : "Market Closed") : "Market unknown", language: language)).akzioText(.label)
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
        // 连接层 stale/offline 优先级高于快照字段，防止旧快照被显示成实时数据。
        if case .stale = observerState { return true }
        if case .offline = observerState { return true }
        return run.dataStale
    }

    private var dataLabel: String {
        // connecting 明确表示数据尚未取到；mock/connected 才读取快照的 stale/live 标记。
        switch observerState {
        case .connecting: "Data Queued"
        case .stale, .offline: "Data Stale"
        case .mock, .connected:
            run.dataStale ? "Data Stale" : (run.dataLive ? "Data Live" : "Data Queued")
        }
    }
}
