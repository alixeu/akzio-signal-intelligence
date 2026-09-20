import SwiftUI

// MARK: - Store
//
// One observable source of truth for the shell: which scenario is loaded, which
// route is visible, which selections each page holds, and the resolved motion /
// render policies. Rust stays authoritative over every App-triggered run purpose.
@MainActor
@Observable
public final class ObservatoryStore {
    // Data
    public private(set) var scenario: MockScenario
    public private(set) var snapshot: ObservatorySnapshot
    private var dataMode: ObservatoryDataMode = .mock
    public private(set) var observerState: ObserverConnectionState = .mock
    private var observerEndpoint = "http://127.0.0.1:7342"
    private var controlToken = ""
    public let coreSupervisor = RustCoreSupervisor.shared
    public var coreConfigurationDraft = CoreCredentialStore.savedDraft()
    public private(set) var coreCredentialStatus = CoreCredentialStore.status()
    public private(set) var selectedRunPurpose: RunPurpose = .positionPlan
    public private(set) var runInFlight = false
    public private(set) var runMessage = ""
    private var liveProjection: LiveProjection?
    private var livePayload: ObserverSnapshotPayload?
    private var observerTask: Task<Void, Never>?
    private var observerClient: ObserverClient?
    var runtimeInspection: RuntimeInspectionPayload? {
        // Debug 模式读取隔离 Core 的检查结果；正式模式读取当前 live Run 的检查投影。
        debugEnabled ? debugRun?.inspection : livePayload?.currentRun?.inspection
    }
    func previewWorkflow(_ purpose: String) async throws -> WorkflowBlueprintPayload {
        // 没有已建立的观察客户端就不能猜测拓扑；调用方收到明确的 endpoint 错误。
        guard let observerClient else { throw ObserverClientError.invalidEndpoint }
        return try await observerClient.fetchBlueprint(purpose: purpose)
    }
    func runtimeJournal(runID: String, after: Int64) async throws -> RuntimeJournalPayload {
        // `after` 是增量游标，服务端只返回游标之后的事件；本方法不把“取到日志”解释成任务完成。
        guard let observerClient else { throw ObserverClientError.invalidEndpoint }
        return try await observerClient.fetchJournal(runID: runID, after: after)
    }
    private(set) var debugListing: DebugRunList?
    private(set) var debugRun: DebugRunPayload?
    private(set) var debugBusy = false
    private(set) var debugMessage = ""
    private(set) var externalDebugCore = false
    var selectedDebugRunID: String?
    var selectedDebugTaskID: String?
    var debugEnabled: Bool { isLive && (externalDebugCore || debugListing?.enabled == true) }
    var debugEndpoint: String { observerEndpoint }
    var debugConnected: Bool { if case .connected = observerState { return true }; return false }
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
            // 属性变更只启动一次历史刷新 Task；刷新结果异步回填缓存，不能阻塞当前页面切换。
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
        // Mock 和 live 共用同一套 Store 字段；只由 autoStartsCore 决定数据源与初始连接状态。
        self.autoStartsCore = autoStartsCore
        self.languageDefaults = languageDefaults
        self.scenario = scenario
        self.snapshot = ScenarioLibrary.snapshot(scenario)
        var settings = ScenarioLibrary.settings(scenario)
        if autoStartsCore,
           let rawLanguage = languageDefaults.string(forKey: Self.appLanguageDefaultsKey),
           let language = AppLanguage(rawValue: rawLanguage)
        {
            // 只有真实 Core 启动路径读取持久化语言；离屏/测试构造保持场景给出的默认语言。
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
        // 三个开关任一要求降级就关闭完整动效，强度和路由转场力度仍来自当前设置。
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
        // Mock 始终有 fixture；live 仅对 Portfolio 检查快照中的 portfolio 数据，其他页面由各自投影表达缺失。
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
        // 先用当前投影作为基线；只有选定区间已有完整曲线缓存时才替换 curve，其余字段保持服务端快照。
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
        // 相同路由不产生转场；不同路由先记录旧/新状态和动效策略，再提交可观察的 route 改变。
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
        // 离屏截图和测试需要稳定落点，因此跳过转场协调器直接替换 route。
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

    public var coreConfigurationPath: String { CoreRuntimePaths.configurationLocation().path }

    public func reconnectCore() async {
        observerTask?.cancel()
        observerTask = nil
        observerClient = nil
        await bootstrapCore()
    }

    public func bootstrapCore() async {
        // 启动先进入 connecting；环境变量分支只连接外部隔离 Debug Core，否则启动受管的本地 Core。
        guard autoStartsCore else { return }
        dataMode = .live
        observerState = .connecting
        let environment = ProcessInfo.processInfo.environment
        if let endpoint = environment["AKZIO_DEBUG_ENDPOINT"],
           let root = environment["AKZIO_DEBUG_STORE_ROOT"] {
            externalDebugCore = true
            observerEndpoint = endpoint
            do {
                // token 来自指定 Store Root；若 endpoint、token 或隔离身份校验失败，整个连接分支转为 offline。
                controlToken = try String(contentsOf: URL(fileURLWithPath: root).appending(path: ".daemon-token"), encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)
                guard let url = URL(string: endpoint) else { throw ObserverClientError.invalidEndpoint }
                let client = try ObserverClient(endpoint: url, token: controlToken)
                let listing = try await client.debugRuns()
                guard listing.enabled, listing.store_identity != nil else { throw DebugAPIError(message: "Endpoint is not an isolated Debug Core") }
                debugListing = listing
                navigate(to: .workflow)
                connectObserver()
            } catch { observerState = .offline(error.localizedDescription) }
            return
        }
        guard let connection = await coreSupervisor.start() else {
            // Core 启动失败不伪造 live 数据；needsConfiguration 额外打开设置层让用户补齐凭据。
            observerState = .offline(coreSupervisor.state.detail ?? coreSupervisor.state.label)
            if coreSupervisor.state == .needsConfiguration {
                openSettings(.core)
            }
            return
        }
        connect(connection)
    }

    public func saveCoreConfigurationAndRestart() async {
        // 先取消旧观察任务并停止 Core，再持久化配置并重新启动；任一步失败都只更新 offline 状态。
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

    public func selectRunPurpose(_ purpose: RunPurpose) {
        // 只接受用户可见的启动模式，且运行期间锁定选择，避免请求中的 purpose 被 UI 改写。
        guard RunPurpose.userLaunchModes.contains(purpose), !runInFlight else { return }
        selectedRunPurpose = purpose
        runMessage = ""
    }

    public func runSelectedPurpose() async {
        // 该方法表示“向 Rust Core 提交 Run”；返回后只是受理/失败状态，不代表研究、Decision 或 Execution 已完成。
        guard !runInFlight else { return }
        guard isLive && !debugEnabled else {
            runMessage = "请连接日常 Core 后启动运行"
            return
        }
        let purpose = selectedRunPurpose
        runInFlight = true
        runMessage = "Starting Rust Core…"
        defer { runInFlight = false }
        if coreSupervisor.state != .ready {
            // Core 尚未 ready 时先启动并连接；连接失败提前返回，不发送提交请求。
            guard let connection = await coreSupervisor.start() else {
                runMessage = coreSupervisor.state.detail ?? coreSupervisor.state.label
                if coreSupervisor.state == .needsConfiguration { openSettings(.core) }
                return
            }
            connect(connection)
        }
        do {
            // submitRun 的成功只确认 Core 接受请求；随后切到 Workflow 观察页等待真实状态。
            let runID = try await coreSupervisor.submitRun(purpose: purpose)
            runMessage = "已受理运行：\(runID)"
            navigate(to: .workflow)
        } catch {
            runMessage = error.localizedDescription
        }
    }

    public func clearCoreCredentials() {
        // 清理凭据同时停止观察和 Core；本地 UI 保留 live 语义，但明确显示未配置而不回退到 mock。
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
        // 连接对象提供 endpoint 和 token；真正的快照循环由 connectObserver 统一创建。
        observerEndpoint = connection.endpoint.absoluteString
        controlToken = connection.controlToken
        connectObserver()
    }

    private func connectObserver() {
        // 新观察循环会取消旧 Task；每轮先取快照，再订阅其后的 SSE 事件，避免遗漏游标之前的数据。
        observerTask?.cancel()
        dataMode = .live
        observerState = .connecting
        guard let endpoint = URL(string: observerEndpoint), !controlToken.isEmpty else {
            observerState = .offline("Endpoint and daemon token are required")
            return
        }
        let token = controlToken
        observerTask = Task { [weak self] in
            guard let self else { return }
            do {
                let client = try ObserverClient(endpoint: endpoint, token: token)
                observerClient = client
                var retrySeconds: UInt64 = 1
                while !Task.isCancelled {
                    do {
                        // 一次成功快照会刷新投影、Debug 列表和曲线；SSE 期间 invalidate 重新取完整快照，reasoning 只增量合并。
                        let payload = try await client.fetchSnapshot()
                        try Task.checkCancellation()
                        apply(payload)
                        try await refreshDebug(using: client)
                        try Task.checkCancellation()
                        observerState = .connected(payload.generatedAt)
                        await refreshPortfolioHistory(using: client, range: equityRange)
                        retrySeconds = 1
                    for try await event in client.events(after: payload.eventCursor) {
                        switch event {
                        case .invalidate:
                            let refreshed = try await client.fetchSnapshot()
                            try Task.checkCancellation()
                            apply(refreshed)
                            try await refreshDebug(using: client)
                            try Task.checkCancellation()
                            observerState = .connected(refreshed.generatedAt)
                            await refreshPortfolioHistory(using: client, range: equityRange)
                        case .reasoning(let payload, let receivedAt):
                            applyReasoning(payload, receivedAt: receivedAt)
                        }
                    }
                    } catch is CancellationError {
                        // Task 取消是正常的生命周期结束，不应被显示为 offline。
                        return
                    } catch {
                        // 首次还没有投影时是 offline；已有旧投影时标记 stale，并采用指数退避但上限 15 秒。
                        observerState = liveProjection == nil
                            ? .offline(error.localizedDescription)
                            : .stale(error.localizedDescription)
                        try? await Task.sleep(nanoseconds: retrySeconds * 1_000_000_000)
                        retrySeconds = min(retrySeconds * 2, 15)
                    }
                }
            } catch {
                // 客户端创建失败无法进入循环，因此直接保留 offline 原因。
                observerState = .offline(error.localizedDescription)
            }
        }
    }

    private func useMockData() {
        // 切换 fixture 前取消 live 观察并清空所有 live 派生缓存，避免旧 Run 混入新场景。
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
        // 只保留属于当前 Run 的 reasoning；随后从新 payload 和剩余记录重建不可变投影。
        let runID = payload.currentRun?.workflow.run.runID
        liveReasoningRecords = liveReasoningRecords.filter { $0.value.runID == runID }
        let projection = LiveProjection(
            payload: payload,
            reasoningRecords: liveReasoningRecords.values.sorted { $0.sequence < $1.sequence }
        )
        let firstSnapshot = liveProjection == nil
        livePayload = payload
        liveProjection = projection
        if selectedStageID.flatMap({ projection.workflow.node(id: $0) }) == nil {
            // 当前选中的 stage 消失时按 active → failed/blocked → pending → first 的顺序选择替代项。
            selectedStageID = projection.workflow.activeStageID
                ?? projection.workflow.nodes.first { $0.taskStatus == .failed || $0.isBlocked }?.id
                ?? projection.workflow.nodes.first { $0.taskStatus == .pending }?.id
                ?? projection.workflow.nodes.first?.id
        }
        if firstSnapshot { selectedArchiveRowID = projection.archive.selectedRowID }
        if let detail = payload.currentRun, detail.workflow.run.runID == selectedArchiveRowID {
            selectedArchiveDetail = detail
        }
    }

    private func applyReasoning(
        _ event: ObserverReasoningEventPayload,
        receivedAt: Date
    ) {
        // 推送事件必须属于当前 Run；不匹配的事件直接丢弃，避免旧连接污染当前页面。
        guard event.runID == livePayload?.currentRun?.workflow.run.runID else { return }
        let id = "reasoning-\(event.runID)-\(event.taskID)-\(event.attemptID)-\(event.turn)"
        if liveReasoningRecords[id] == nil {
            // 同一事件键只建一次记录；后续 delta 只追加正文，结束事件只改变完成标记。
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
        // reasoning 记录改变后重新生成投影，页面读取到的是排序稳定的快照而非正在遍历的字典。
        liveProjection = LiveProjection(
            payload: payload,
            reasoningRecords: liveReasoningRecords.values.sorted { $0.sequence < $1.sequence }
        )
    }

    private func refreshPortfolioHistory(using client: ObserverClient, range: EquityRange) async {
        // 仅允许四个已支持区间；接口没有 points 时不覆盖旧缓存，网络/解析失败则清除该区间缓存。
        guard [.oneDay, .fiveDay, .oneMonth, .threeMonth].contains(range) else { return }
        do {
            let section = try await client.fetchPortfolioHistory(range: range)
            guard let points = section.data?.points else { return }
            let firstTimestamp = points.first?.timestamp
            // 以首个点为零点计算分钟偏移；无首点时退回枚举索引，保留曲线顺序。
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
        // 场景切换总是回到 mock，并重置选择/曲线；它不会启动真实 Core 或提交任务。
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

    var workflowEvidenceSummary: String? {
        // 这里只汇总当前投影中的 T0 节点和已展示 artifact，调度控制状态未知也必须如实保留。
        guard isLive, let detail = livePayload?.currentRun else { return nil }
        let t0 = detail.workflow.tasks.filter { WorkflowDisplay.phase($0.node.recipeID) != 2 }
        let completed = t0.filter { $0.taskStatus == "succeeded" }.count
        let artifacts = detail.artifacts.map { (kind: $0.kind, payload: $0.payload) }
        return "T0 研究与执行 \(completed)/\(t0.count) 完成 · 执行：\(WorkflowDisplay.executionLabel(artifacts, evidence: nil)) · Outcome：\(OutcomeEvidencePresentation.from(artifacts).caption) · 调度控制状态未知"
    }

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
        // 没有匹配的 live detail 时返回行内已有进度；匹配后按稳定任务顺序重建阶段摘要。
        guard isLive, let selectedArchiveDetail, selectedArchiveDetail.workflow.run.runID == selectedArchiveRow?.runID else {
            return selectedArchiveRow?.stageProgress ?? []
        }
        return WorkflowDisplay.orderedTasks(selectedArchiveDetail.workflow.tasks).map { task in
            ArchiveStageProgress(
                id: task.node.taskID,
                label: task.node.recipeID,
                horizon: task.node.horizon,
                status: (TaskStatus(rawValue: task.taskStatus) ?? .pending).status(optional: false),
                timeLabel: task.finishedAt.map(Self.timeLabel) ?? MissingValue.pending.rawValue
            )
        }
    }

    public var selectedArchiveOutcomeEvidence: OutcomeEvidencePresentation {
        guard let detail = selectedArchiveDetail, detail.workflow.run.runID == selectedArchiveRow?.runID else { return .unknown }
        return .from(detail.artifacts.map { (kind: $0.kind, payload: $0.payload) })
    }

    public func selectArchiveRun(_ id: String) {
        // 再次点击同一行取消选择；异步详情返回前若用户换行，runID 守卫会丢弃过期响应。
        let next = selectedArchiveRowID == id ? nil : id
        selectedArchiveRowID = next
        selectedArchiveDetail = nil
        guard let next, isLive, let observerClient else { return }
        Task { [weak self] in
            // 弱捕获避免详情请求反向持有 Store；Task 只在 Store 仍存在且选择未改变时写回。
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

extension ObservatoryStore {
    private func refreshDebug(using client: ObserverClient) async throws {
        // 先取列表确认 Debug 开关和隔离身份，再按当前选择加载单个 Run；列表关闭时不请求详情。
        let listing = try await client.debugRuns()
        if externalDebugCore && !listing.enabled { throw DebugAPIError(message: "Debug Core identity changed") }
        debugListing = listing
        guard listing.enabled else { return }
        if selectedDebugRunID == nil { selectedDebugRunID = listing.runs.first?.id }
        if let id = selectedDebugRunID {
            debugRun = try await client.debugRun(id)
            if selectedDebugTaskID == nil { selectedDebugTaskID = WorkflowDisplay.initialSelection(debugRun?.nodes ?? []) }
        }
    }

    func selectDebugRun(_ id: String) async {
        // 切换 Run 会清掉旧 task 选择；刷新失败只标记 stale，不伪造新的 debug payload。
        guard let observerClient else { return }
        if selectedDebugRunID != id { selectedDebugTaskID = nil }
        selectedDebugRunID = id
        do { try await refreshDebug(using: observerClient) }
        catch { debugMessage = error.localizedDescription; observerState = .stale(error.localizedDescription) }
    }

    func controlDebug(_ action: String, task: String? = nil) async {
        // CAS revision 来自当前 debugRun；请求期间用 debugBusy 串行化控制，defer 保证返回/抛错都解锁。
        guard debugConnected, !debugBusy, let client = observerClient, let run = debugRun else { return }
        if let task { selectedDebugTaskID = task }
        debugBusy = true
        defer { debugBusy = false }
        do {
            _ = try await client.debugControl(run: run.session.runID, action: action, revision: run.session.revision, task: task)
            debugMessage = "Core accepted \(action)."
            try await refreshDebug(using: client)
        } catch {
            debugMessage = error.localizedDescription
            // A CAS conflict requires fresh authority; it must never be retried with a guessed revision.
            try? await refreshDebug(using: client)
        }
    }

    func prepareDebug(session: String, purpose: String) async {
        // prepare 只创建并暂停隔离 Debug Run；成功后重新拉取状态，Broker 写入仍由 Debug Core 策略禁止。
        guard debugConnected, !debugBusy, let client = observerClient else { return }
        debugBusy = true
        defer { debugBusy = false }
        do {
            let session = try await client.debugPrepare(session: session, purpose: purpose)
            selectedDebugRunID = session.runID
            selectedDebugTaskID = nil
            try await refreshDebug(using: client)
            debugMessage = "Prepared and paused. Broker writes are disabled."
        } catch { debugMessage = error.localizedDescription }
    }

    func forkDebug(task: String?, reason: String) async {
        // fork 返回新的实验会话并保留父产物不可变；UUID 只作为本次实验标识，不代表已经执行任务。
        guard debugConnected, !debugBusy, let client = observerClient, let run = debugRun else { return }
        debugBusy = true
        defer { debugBusy = false }
        do {
            let session = try await client.debugFork(run: run.session.runID, task: task, reason: reason, experimentID: UUID().uuidString.lowercased())
            selectedDebugRunID = session.runID
            selectedDebugTaskID = nil
            try await refreshDebug(using: client)
            debugMessage = "New experiment created; parent artifacts remain immutable."
        } catch { debugMessage = error.localizedDescription }
    }
}
