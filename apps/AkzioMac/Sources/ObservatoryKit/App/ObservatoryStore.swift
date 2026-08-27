import SwiftUI

// MARK: - Store
//
// One observable source of truth for the shell: which scenario is loaded, which
// route is visible, which selections each page holds, and the resolved motion /
// render policies. Rust stays authoritative; the only App-triggered run is Debug.
@MainActor
@Observable
public final class ObservatoryStore {
    // Data
    public private(set) var scenario: MockScenario
    public private(set) var snapshot: ObservatorySnapshot
    private var dataMode: ObservatoryDataMode = .mock
    public private(set) var observerState: ObserverConnectionState = .mock
    private var observerEndpoint = "http://127.0.0.1:7342"
    private var observerToken = ""
    public let coreSupervisor = RustCoreSupervisor.shared
    public var coreConfigurationDraft = CoreCredentialStore.savedDraft()
    public private(set) var coreCredentialStatus = CoreCredentialStore.status()
    public private(set) var debugRunInFlight = false
    public private(set) var debugRunMessage = ""
    private var liveProjection: LiveProjection?
    private var livePayload: ObserverSnapshotPayload?
    private var observerTask: Task<Void, Never>?
    private var observerClient: ObserverClient?
    private var livePortfolioHistory: [EquityRange: [EquityPoint]] = [:]
    private var liveReasoningRecords: [String: LiveReasoningRecord] = [:]
    private var liveReasoningSequence: Int64 = 0
    private let autoStartsCore: Bool
    private let languageDefaults: UserDefaults
    private static let appLanguageDefaultsKey = "akzio.observatory.app-language"

    // Navigation
    public private(set) var route: AppRoute = .overview
    public var settingsPresented = false
    public var settingsCategory: SettingsPresentation.Category = .appearance
    public let transitions = TransitionCoordinator()

    // Per-page selection, kept in the shell so transitions can read it
    public var selectedStageID: String?
    public var selectedHorizon: OutcomeHorizonKind = .t1
    public var equityRange: EquityRange = .oneDay {
        didSet {
            guard isLive, oldValue != equityRange, let observerClient else { return }
            Task { await refreshPortfolioHistory(using: observerClient, range: equityRange) }
        }
    }
    public var learningTab: LearningPresentation.Tab = .retrospective
    public var selectedArchiveRowID: String?
    private(set) var selectedArchiveDetail: ObserverRunDetailPayload?
    public var selectedPosition: TradableAsset?

    // Appearance / motion, all display-only
    public var settings: SettingsPresentation {
        didSet {
            guard autoStartsCore, oldValue.language != settings.language else { return }
            languageDefaults.set(settings.language.rawValue, forKey: Self.appLanguageDefaultsKey)
        }
    }

    /// Mirrors the system Reduce Motion switch; the shell writes it every layout.
    public var systemReduceMotion = false
    public var systemReduceTransparency = false
    public var systemHighContrast = false

    /// False when the window is inactive, minimised or fully occluded. Ambient
    /// motion must stop then: nobody is watching and the energy cost is real.
    public var windowActive = true
    /// True below ~1400pt wide: the sidebar collapses and inspectors become popovers.
    public var compactLayout = false

    public init(
        scenario: MockScenario = .paperRunningSynthesizerActive,
        autoStartsCore: Bool = true,
        languageDefaults: UserDefaults = .standard
    ) {
        self.autoStartsCore = autoStartsCore
        self.languageDefaults = languageDefaults
        self.scenario = scenario
        self.snapshot = ScenarioLibrary.snapshot(scenario)
        var settings = ScenarioLibrary.settings(scenario)
        if autoStartsCore,
           let rawLanguage = languageDefaults.string(forKey: Self.appLanguageDefaultsKey),
           let language = AppLanguage(rawValue: rawLanguage)
        {
            settings.language = language
        }
        self.settings = settings
        self.dataMode = autoStartsCore ? .live : .mock
        self.observerState = autoStartsCore ? .connecting : .mock
        self.selectedStageID = snapshot.workflow.activeStageID
        self.selectedHorizon = snapshot.outcome.selected
        self.selectedArchiveRowID = snapshot.archive.selectedRowID
    }

    // MARK: Derived policy

    public var motionPolicy: MotionPolicy {
        let reduced = systemReduceMotion || settings.reduceMotionOverride || !settings.globalMotionEnabled
        return MotionPolicy(
            level: reduced ? .reduced : .full,
            intensity: settings.motionIntensity,
            routeStrength: settings.routeTransitionStrength
        )
    }

    public var canvasPolicy: CanvasRenderPolicy {
        CanvasRenderPolicy(
            quality: settings.renderQuality,
            allowsAmbient: motionPolicy.allowsAmbient,
            density: settings.particleDensity,
            // Paused during a route transition (the canvas would fight the
            // choreography) and whenever the window is not being looked at.
            isPaused: transitions.isRunning || !windowActive
        )
    }

    public var highContrast: Bool { systemHighContrast || settings.highContrast }

    /// Elapsed clock comes from the snapshot, never from `Date()`, so a screenshot
    /// taken twice shows the same time.
    public var elapsedLabel: String {
        PpmFormatter.elapsed(seconds: displayRun.elapsedSeconds)
    }

    public var isLive: Bool { dataMode == .live }
    public var coreState: RustCoreState { coreSupervisor.state }
    public var coreStorePath: String { coreSupervisor.storePath }
    public var coreApprovalStatus: String { livePayload?.core.approval.status ?? "unknown" }
    public func hasLiveData(for route: AppRoute) -> Bool {
        guard isLive else { return true }
        switch route {
        case .portfolio:
            return livePayload?.portfolio.data != nil
        case .outcome, .learning:
            return true
        default:
            return true
        }
    }
    public var displayScenarioTitle: String { isLive ? "durable tasks" : snapshot.scenarioTitle }
    public var displayRun: RunPresentation {
        isLive ? (liveProjection?.run ?? LiveProjection.unavailableRun) : snapshot.run
    }
    public var displayWorkflow: WorkflowPresentation {
        isLive ? (liveProjection?.workflow ?? LiveProjection.unavailableWorkflow) : snapshot.workflow
    }
    public var displayArchive: ArchivePresentation {
        isLive ? (liveProjection?.archive ?? LiveProjection.unavailableArchive) : snapshot.archive
    }
    public var displayEvents: [EventPresentation] {
        isLive ? (liveProjection?.events ?? []) : snapshot.events
    }
    public var displayAgents: [AgentRailItem] {
        isLive ? (liveProjection?.agents ?? []) : snapshot.agents
    }
    public var displayHealth: [HealthMetric] {
        isLive ? (liveProjection?.health ?? LiveProjection.unavailableHealth) : snapshot.health
    }
    public var displayCouncil: CouncilPresentation {
        isLive ? (liveProjection?.council ?? LiveProjection.unavailableCouncil) : snapshot.council
    }
    public var displayPortfolio: PortfolioPresentation {
        guard isLive else { return snapshot.portfolio }
        let base = liveProjection?.portfolio ?? LiveProjection.unavailablePortfolio
        guard let curve = livePortfolioHistory[equityRange] else { return base }
        return PortfolioPresentation(
            equityMicros: base.equityMicros,
            todayPnlMicros: base.todayPnlMicros,
            todayPnlPpm: base.todayPnlPpm,
            unrealizedPnlMicros: base.unrealizedPnlMicros,
            realizedPnlMicros: base.realizedPnlMicros,
            unrealizedPnlPpm: base.unrealizedPnlPpm,
            realizedPnlPpm: base.realizedPnlPpm,
            curve: curve,
            range: equityRange,
            benchmarkLabel: base.benchmarkLabel,
            allocations: base.allocations,
            positions: base.positions,
            orders: base.orders,
            fills: base.fills,
            flow: base.flow,
            risk: base.risk,
            verdict: base.verdict,
            reconciliation: base.reconciliation
        )
    }
    public var displayOutcome: OutcomePresentation {
        isLive ? (liveProjection?.outcome ?? LiveProjection.unavailableOutcome) : snapshot.outcome
    }
    public var displayLearning: LearningPresentation {
        isLive ? (liveProjection?.learning ?? LiveProjection.unavailableLearning) : snapshot.learning
    }

    // MARK: Navigation

    /// Single entry point for keyboard and sidebar so both share one transition
    /// pipeline.
    public func navigate(to destination: AppRoute, fromKeyboard: Bool = false) {
        guard destination != route else { return }
        transitions.begin(
            TransitionIntent(
                from: route,
                to: destination,
                fromKeyboard: fromKeyboard
            ),
            policy: motionPolicy
        )
        route = destination
    }

    /// Jump to a route without any choreography. Only for capture and tests: the UI
    /// always goes through `navigate` so the transition pipeline stays single-path.
    public func openDirectly(_ route: AppRoute) {
        self.route = route
    }

    public func toggleSettings() {
        settingsPresented.toggle()
    }

    private func openSettings(_ category: SettingsPresentation.Category) {
        settingsCategory = category
        settingsPresented = true
    }


    public func revealRunInArchive(_ runID: String) {
        selectedArchiveRowID = displayArchive.rows.first { $0.runID == runID }?.id
        navigate(to: .runArchive)
    }

    // MARK: Observer

    public func bootstrapCore() async {
        guard autoStartsCore else { return }
        dataMode = .live
        observerState = .connecting
        guard let connection = await coreSupervisor.start() else {
            observerState = .offline(coreSupervisor.state.detail ?? coreSupervisor.state.label)
            if coreSupervisor.state == .needsConfiguration {
                openSettings(.core)
            }
            return
        }
        connect(connection)
    }

    public func saveCoreConfigurationAndRestart() async {
        do {
            observerTask?.cancel()
            coreSupervisor.stop()
            try CoreCredentialStore.save(coreConfigurationDraft)
            coreCredentialStatus = CoreCredentialStore.status()
            guard let connection = await coreSupervisor.start() else {
                observerState = .offline(coreSupervisor.state.detail ?? coreSupervisor.state.label)
                return
            }
            connect(connection)
        } catch {
            observerState = .offline(error.localizedDescription)
        }
    }

    public func runDebug() async {
        guard !debugRunInFlight else { return }
        guard isLive else {
            debugRunMessage = "Run is unavailable in Mock mode"
            return
        }
        debugRunInFlight = true
        debugRunMessage = "Starting Rust Core…"
        defer { debugRunInFlight = false }
        if coreSupervisor.state != .ready {
            guard let connection = await coreSupervisor.start() else {
                debugRunMessage = coreSupervisor.state.detail ?? coreSupervisor.state.label
                if coreSupervisor.state == .needsConfiguration { openSettings(.core) }
                return
            }
            connect(connection)
        }
        do {
            try await coreSupervisor.submitDebugRun()
            debugRunMessage = "Run submitted"
            navigate(to: .workflow)
        } catch {
            debugRunMessage = error.localizedDescription
        }
    }

    public func clearCoreCredentials() {
        do {
            observerTask?.cancel()
            coreSupervisor.stop()
            try CoreCredentialStore.clear()
            coreCredentialStatus = CoreCredentialStore.status()
            coreConfigurationDraft = CoreCredentialStore.savedDraft()
            dataMode = .live
            observerState = .offline("Core credentials are not configured")
        } catch {
            observerState = .offline(error.localizedDescription)
        }
    }

    private func connect(_ connection: RustCoreConnection) {
        observerEndpoint = connection.endpoint.absoluteString
        observerToken = connection.observerToken
        connectObserver()
    }

    private func connectObserver() {
        observerTask?.cancel()
        dataMode = .live
        observerState = .connecting
        guard let endpoint = URL(string: observerEndpoint), !observerToken.isEmpty else {
            observerState = .offline("Endpoint and observer token are required")
            return
        }
        let token = observerToken
        observerTask = Task { [weak self] in
            guard let self else { return }
            do {
                let client = try ObserverClient(endpoint: endpoint, token: token)
                observerClient = client
                var retrySeconds: UInt64 = 1
                while !Task.isCancelled {
                    do {
                        let payload = try await client.fetchSnapshot()
                        apply(payload)
                        await refreshPortfolioHistory(using: client, range: equityRange)
                        observerState = .connected(payload.generatedAt)
                        retrySeconds = 1
                    for try await event in client.events(after: payload.eventCursor) {
                        switch event {
                        case .invalidate:
                            let refreshed = try await client.fetchSnapshot()
                            apply(refreshed)
                            await refreshPortfolioHistory(using: client, range: equityRange)
                            observerState = .connected(refreshed.generatedAt)
                        case .reasoning(let payload, let receivedAt):
                            applyReasoning(payload, receivedAt: receivedAt)
                        }
                    }
                    } catch is CancellationError {
                        return
                    } catch {
                        observerState = liveProjection == nil
                            ? .offline(error.localizedDescription)
                            : .stale(error.localizedDescription)
                        try? await Task.sleep(nanoseconds: retrySeconds * 1_000_000_000)
                        retrySeconds = min(retrySeconds * 2, 15)
                    }
                }
            } catch {
                observerState = .offline(error.localizedDescription)
            }
        }
    }

    private func useMockData() {
        observerTask?.cancel()
        observerTask = nil
        observerClient = nil
        livePortfolioHistory.removeAll()
        liveReasoningRecords.removeAll()
        liveReasoningSequence = 0
        liveProjection = nil
        dataMode = .mock
        observerState = .mock
        snapshot = ScenarioLibrary.snapshot(scenario)
        selectedStageID = snapshot.workflow.activeStageID
        selectedArchiveRowID = snapshot.archive.selectedRowID
    }

    private func apply(_ payload: ObserverSnapshotPayload) {
        let runID = payload.currentRun?.workflow.run.runID
        liveReasoningRecords = liveReasoningRecords.filter { $0.value.runID == runID }
        let projection = LiveProjection(
            payload: payload,
            reasoningRecords: liveReasoningRecords.values.sorted { $0.sequence < $1.sequence }
        )
        livePayload = payload
        liveProjection = projection
        if selectedStageID.flatMap({ projection.workflow.node(id: $0) }) == nil {
            selectedStageID = projection.workflow.activeStageID
        }
        selectedArchiveRowID = projection.archive.selectedRowID
    }

    private func applyReasoning(
        _ event: ObserverReasoningEventPayload,
        receivedAt: Date
    ) {
        guard event.runID == livePayload?.currentRun?.workflow.run.runID else { return }
        let id = "reasoning-\(event.runID)-\(event.taskID)-\(event.attemptID)-\(event.turn)"
        if liveReasoningRecords[id] == nil {
            liveReasoningSequence += 1
            liveReasoningRecords[id] = LiveReasoningRecord(
                id: id,
                sequence: (livePayload?.eventCursor ?? 0) + liveReasoningSequence,
                runID: event.runID,
                taskID: event.taskID,
                purpose: event.purpose,
                turn: event.turn,
                createdAt: receivedAt,
                body: "",
                isComplete: false
            )
        }
        switch event.type {
        case "reasoning-delta":
            liveReasoningRecords[id]?.body += event.delta ?? ""
        case "reasoning-end":
            liveReasoningRecords[id]?.isComplete = true
        default:
            break
        }
        guard let payload = livePayload else { return }
        liveProjection = LiveProjection(
            payload: payload,
            reasoningRecords: liveReasoningRecords.values.sorted { $0.sequence < $1.sequence }
        )
    }

    private func refreshPortfolioHistory(using client: ObserverClient, range: EquityRange) async {
        guard [.oneDay, .fiveDay, .oneMonth, .threeMonth].contains(range) else { return }
        do {
            let section = try await client.fetchPortfolioHistory(range: range)
            guard let points = section.data?.points else { return }
            let firstTimestamp = points.first?.timestamp
            livePortfolioHistory[range] = points.enumerated().map { index, point in
                let value = Double(point.equityMicros) / PpmFormatter.ppmPerUnit
                return EquityPoint(
                    index: index,
                    minutesFromOpen: firstTimestamp.map {
                        max(0, Int(point.timestamp.timeIntervalSince($0) / 60))
                    } ?? index,
                    timestamp: point.timestamp,
                    portfolio: value,
                    benchmark: point.benchmarkEquityMicros.map {
                        Double($0) / PpmFormatter.ppmPerUnit
                    }
                )
            }
        } catch {
            livePortfolioHistory[range] = nil
        }
    }

    // MARK: Scenario switching

    public func load(_ next: MockScenario) {
        useMockData()
        scenario = next
        snapshot = ScenarioLibrary.snapshot(next)
        settings.reduceMotionOverride = next.reduceMotionPreferred
        selectedStageID = snapshot.workflow.activeStageID
        selectedHorizon = snapshot.outcome.selected
        selectedArchiveRowID = snapshot.archive.selectedRowID
        selectedPosition = nil
        equityRange = snapshot.portfolio.range
    }

    // MARK: Convenience accessors used by the pages

    public var activeStage: WorkflowNodePresentation? {
        guard let selectedStageID else { return displayWorkflow.nodes.first(where: \.isActive) }
        return displayWorkflow.node(id: selectedStageID)
    }

    public var selectedStageInspector: StageInspectorPresentation {
        displayWorkflow.inspector(for: selectedStageID ?? displayWorkflow.activeStageID)
    }

    public var selectedArchiveRow: ArchiveRowPresentation? {
        guard let selectedArchiveRowID else { return displayArchive.rows.first }
        return displayArchive.rows.first { $0.id == selectedArchiveRowID }
    }

    public var selectedArchiveStageProgress: [ArchiveStageProgress] {
        guard isLive, let selectedArchiveDetail else {
            return selectedArchiveRow?.stageProgress ?? []
        }
        return selectedArchiveDetail.workflow.tasks.map { task in
            ArchiveStageProgress(
                label: task.node.recipeID,
                status: (TaskStatus(rawValue: task.taskStatus) ?? .pending).status(optional: false),
                timeLabel: task.finishedAt.map(Self.timeLabel) ?? MissingValue.pending.rawValue
            )
        }
    }

    public func selectArchiveRun(_ id: String) {
        let next = selectedArchiveRowID == id ? nil : id
        selectedArchiveRowID = next
        selectedArchiveDetail = nil
        guard let next, isLive, let observerClient else { return }
        Task { [weak self] in
            guard let self else { return }
            do {
                let detail = try await observerClient.fetchRun(next)
                guard selectedArchiveRowID == next else { return }
                selectedArchiveDetail = detail
            } catch {
                guard selectedArchiveRowID == next else { return }
                observerState = .stale(error.localizedDescription)
            }
        }
    }

    private static func timeLabel(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "HH:mm:ss"
        return formatter.string(from: date)
    }
}
