import AppKit
import Combine
import SwiftUI

// MARK: - Shell
//
// Sidebar on the left, status bar above the active route on the right, and
// Settings as a glass layer over a dimmed-but-visible page.
// A single `Namespace` is created here and injected into the environment so every
// shared-element handoff in the app matches against the same geometry space.
public struct AppShell: View {
    @State private var store: ObservatoryStore
    @State private var sidebarVisible = true
    @Namespace private var shared

    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.colorSchemeContrast) private var colorSchemeContrast
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.akzioRendersOffscreen) private var rendersOffscreen

    public init(scenario: MockScenario = .paperRunningSynthesizerActive) {
        _store = State(initialValue: ObservatoryStore(scenario: scenario))
    }

    /// Deterministic entry point used by `--capture`: opens directly on a route with
    /// no transition in flight, so the rendered frame is the settled state.
    public init(
        scenario: MockScenario,
        route: AppRoute,
        settingsPresented: Bool = false,
        compactLayout: Bool = false,
        language: AppLanguage = .system
    ) {
        let store = ObservatoryStore(scenario: scenario, autoStartsCore: false)
        store.openDirectly(route)
        store.settingsPresented = settingsPresented
        store.compactLayout = compactLayout
        store.settings.language = language
        store.windowActive = false
        _store = State(initialValue: store)
    }

    public var body: some View {
        shell
            .modifier(WindowTitlebarInsetModifier(enabled: !rendersOffscreen))
            .background(windowActivityObservers)
            .background(WindowChromeConfigurator(desktopBlurEnabled: desktopBlurEnabled))
            // `task` 只负责启动异步 Core 连接；视图本身先按当前 Store 状态渲染，连接结果由 Store 回写。
            .task { await store.bootstrapCore() }
    }

    private var desktopBlurEnabled: Bool {
        !rendersOffscreen
            && !reduceTransparency
            && !store.settings.reduceTransparencyOverride
            && !store.highContrast
    }

    private var desktopShadeOpacity: Double {
        // The AppKit material performs the Gaussian blur. This scrim only lowers
        // luminance so the blurred desktop reads as atmosphere, like macOS's
        // dark translucent capsules, instead of a readable bright window.
        min(0.56, max(0.42, 0.42 + (1 - store.settings.glassTransparency) * 0.20))
    }

    private var mainContent: some View {
                    VStack(spacing: 0) {
                        if !store.isLive {
                            HStack(spacing: 8) {
                                Label("演示场景 · Mock", systemImage: "rectangle.dashed")
                                    .fontWeight(.semibold)
                                Text("\(store.displayScenarioTitle) · 数值与运行状态均为界面样例")
                                Spacer(minLength: 0)
                                Button("返回真实数据") {
                                    Task { await store.reconnectCore() }
                                }
                            }
                            .font(.caption)
                            .foregroundStyle(AkzioColor.primaryGold)
                            .padding(.horizontal, AkzioLayout.s4)
                            .padding(.vertical, 8)
                            .background(AkzioColor.deepBackground)
                            .accessibilityIdentifier("mock-scenario-banner")
                        }
                        // Debug Core 只显示调试环境提示；正式 Core 才显示可发起 Run 的状态栏。
                        if store.debugEnabled {
                            DebugEnvironmentBanner(store: store)
                        } else {
                        RunStatusBar(
                            run: store.displayRun,
                            health: store.displayHealth,
                            observerState: store.observerState,
                            namespace: shared,
                            canRun: store.isLive && !store.debugEnabled
                                && store.coreSupervisor.state != .starting
                                && store.coreSupervisor.state != .waitingReady,
                            selectedRunPurpose: store.selectedRunPurpose,
                            runInFlight: store.runInFlight,
                            runMessage: store.runMessage,
                            onSelectRunPurpose: store.selectRunPurpose,
                            onRun: { Task { await store.runSelectedPurpose() } },
                            leadingPadding: sidebarVisible
                                ? AkzioLayout.s4
                                : AkzioLayout.collapsedSidebarContentLeading,
                            onOpenSettings: store.toggleSettings,
                            onCopyRunID: copyRunID,
                            onRevealRun: { store.revealRunInArchive(store.displayRun.runId) }
                        )
                        }
                        if store.coreSupervisor.state == .starting || store.coreSupervisor.state == .waitingReady {
                            HStack(spacing: 8) {
                                ProgressView().controlSize(.small)
                                Text("正在验证模型与启动 Core，请稍候…").font(.callout)
                                Spacer()
                            }
                            .padding(.horizontal, AkzioLayout.s4)
                            .padding(.vertical, 8)
                        }
                        // `detail` 优先于本地操作消息；没有任何错误/状态文本时不占用页面高度。
                        if let message = store.observerState.detail ?? (store.runMessage.isEmpty ? nil : store.runMessage) {
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: "exclamationmark.circle")
                                Text(message).font(.callout).textSelection(.enabled)
                                Spacer(minLength: 0)
                            }
                            .foregroundStyle(AkzioColor.actionCoral)
                            .padding(12)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .background(AkzioColor.deepBackground)
                            .accessibilityIdentifier("observer-status-message")
                        }
                        RouteHost(store: store)
                    }
    }

    private var shell: some View {
        GeometryReader { proxy in
            ZStack(alignment: .topLeading) {
                // AppKit backdrop 负责模糊，这层只降低亮度；离屏截图或无障碍设置会跳过它。
                if desktopBlurEnabled {
                    Color.black
                        .opacity(desktopShadeOpacity)
                        .ignoresSafeArea()
                        .allowsHitTesting(false)
                }

                // Settings 展开时仍保留底层页面，但禁用并隐藏其无障碍树，避免误操作或重复读屏。
                shellContent
                    .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)
                    .disabled(store.settingsPresented)
                    .accessibilityHidden(store.settingsPresented)

            // 侧栏收起后只留下一个独立切换按钮，按钮仍复用同一 Store 路由状态。
            if !sidebarVisible {
                Button(action: toggleSidebar) {
                    Image(systemName: "sidebar.left")
                        .font(.system(size: 16, weight: .medium))
                        .foregroundStyle(AkzioColor.sidebarPrimaryText)
                        .frame(
                            width: AkzioLayout.collapsedSidebarToggleSize,
                            height: AkzioLayout.collapsedSidebarToggleSize
                        )
                        .background {
                            Circle()
                            .fill(AkzioColor.sidebarSurface(for: store.settings.theme))
                                .overlay {
                                    Circle().stroke(AkzioColor.sidebarHairline, lineWidth: 1)
                                }
                        }
                }
                .buttonStyle(.plain)
                .padding(.leading, AkzioLayout.collapsedSidebarToggleLeading)
                .padding(.top, AkzioLayout.collapsedSidebarToggleTop)
                .help("Show Sidebar")
                .accessibilityLabel("Show Sidebar")
            }

            // 设置层覆盖在页面之上；Store 只维护是否展示，具体布局由 SettingsLayer 决定。
            if store.settingsPresented {
                SettingsLayer(store: store)
            }
            }
            .frame(width: proxy.size.width, height: proxy.size.height)
        }
        .frame(minWidth: 1280)
        .akzioWindowBackdrop(AkzioColor.surface(for: store.settings.theme))
        .animation(store.motionPolicy.resolve(Motion.themeCrossfade), value: store.settings.theme)
        .environment(\.sharedNamespace, shared)
        .environment(\.appLanguage, store.settings.language)
        .environment(\.locale, store.settings.language.locale)
        .environment(\.motionPolicy, store.motionPolicy)
        .environment(\.canvasRenderPolicy, store.canvasPolicy)
        .environment(\.akzioLabelDensity, store.settings.labelDensity)
        .environment(\.glassIntensity, store.settings.glassIntensity)
        .environment(\.glassTransparency, store.settings.glassTransparency)
        .environment(\.akzioReduceTransparencyOverride, store.settings.reduceTransparencyOverride)
        .environment(\.akzioHighContrast, store.highContrast)
        .environment(\.akzioCompactLayout, store.compactLayout)
        .environment(\.akzioTextScale, store.settings.textScale)
        .environment(\.akzioColorIndependentStatus, store.settings.colorIndependentStatus)
        .environment(\.transitionPhase, store.transitions.phase)
        .focusEffectDisabled(!store.settings.keyboardFocusVisible)
        .background(keyboardRoutes)
        .background(widthProbe)
        .onAppear(perform: syncAccessibility)
        .onChange(of: reduceMotion) { _, _ in syncAccessibility() }
        .onChange(of: reduceTransparency) { _, _ in syncAccessibility() }
        .onChange(of: colorSchemeContrast) { _, _ in syncAccessibility() }
        .onChange(of: scenePhase) { _, phase in
            // `.background` covers minimise and hide; `.inactive` covers losing key.
            // 这是场景级信号，窗口遮挡则由下面的 AppKit 通知补充。
            store.windowActive = phase == .active
        }
        .preferredColorScheme(.dark)
    }

    @ViewBuilder
    private var shellContent: some View {
        HStack(alignment: .top, spacing: 0) {
            if sidebarVisible {
                PageSidebar(
                    route: store.route,
                    theme: store.settings.theme,
                    onSelect: { store.navigate(to: $0) },
                    onOpenSettings: store.toggleSettings,
                    onToggleSidebar: toggleSidebar
                )
                .frame(maxHeight: .infinity, alignment: .top)
            }
            mainContent
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        }
    }

    /// Window width decides the layout mode. Measured once per resize, written to the
    /// store so every page reads the same answer.
    private var widthProbe: some View {
        GeometryReader { proxy in
            // GeometryReader 只测量，不参与命中测试；每次尺寸变化都把同一个布局结论写回 Store。
            Color.clear
                .onAppear { store.compactLayout = proxy.size.width < AkzioLayout.compactWidthThreshold }
                .onChange(of: proxy.size.width) { _, width in
                    store.compactLayout = width < AkzioLayout.compactWidthThreshold
                }
        }
        .allowsHitTesting(false)
    }

    /// AppKit is the only source of truth for occlusion; `scenePhase` alone does not
    /// report a window that is fully covered by another app's window.
    private var windowActivityObservers: some View {
        // AppKit 的 occlusion 状态比 scenePhase 更细：窗口仍可 active 但可能完全被其他窗口遮挡。
        Color.clear
            .onNotification(NSApplication.didBecomeActiveNotification) { store.windowActive = true }
            .onNotification(NSApplication.willResignActiveNotification) { store.windowActive = false }
            .onNotification(NSApplication.didHideNotification) { store.windowActive = false }
            .onNotification(NSApplication.didUnhideNotification) { store.windowActive = true }
            .onNotification(NSWindow.didChangeOcclusionStateNotification) {
                store.windowActive = NSApp.windows.contains { $0.occlusionState.contains(.visible) }
            }
            .allowsHitTesting(false)
    }

    private func syncAccessibility() {
        // 系统无障碍值只在主线程同步到 Store，后续各页面读取同一份策略。
        store.systemReduceMotion = reduceMotion
        store.systemReduceTransparency = reduceTransparency
        store.systemHighContrast = colorSchemeContrast == .increased
    }

    private func toggleSidebar() {
        // 动画只包住本地可逆的可见性翻转，不触发 Core 或路由副作用。
        withAnimation(.spring(response: 0.28, dampingFraction: 1.0)) {
            sidebarVisible.toggle()
        }
    }

    private func copyRunID() {
        // 粘贴板写入是一次性 UI 副作用；ID 来源于当前展示投影，未发起新的查询。
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(store.displayRun.runId, forType: .string)
    }

    /// ⌘1–⌘7 content routes, ⌘8 Settings. Zero-opacity buttons
    /// keep every navigation path inside the same `store.navigate` pipeline.
    private var keyboardRoutes: some View {
        ZStack {
            // 隐形按钮把快捷键统一送进 Store 的导航入口，避免键盘路径绕过转场协调器。
            ForEach(AppRoute.allCases) { route in
                if let shortcut = route.shortcut {
                    Button("") { store.navigate(to: route, fromKeyboard: true) }
                        .keyboardShortcut(shortcut, modifiers: .command)
                }
            }
            Button("") { store.toggleSettings() }
                .keyboardShortcut("8", modifiers: .command)
        }
        .opacity(0)
        .allowsHitTesting(false)
        .accessibilityHidden(true)
    }
}

private struct WindowTitlebarInsetModifier: ViewModifier {
    let enabled: Bool

    @ViewBuilder
    func body(content: Content) -> some View {
        // 离屏渲染不需要避让原生标题栏；真实窗口才忽略顶部安全区。
        if enabled {
            content.ignoresSafeArea(.container, edges: .top)
        } else {
            content
        }
    }
}

// MARK: - Route host

/// Renders exactly one route. During a transition the incoming page is what is on
/// screen, and the coordinator's phase drives its staged reveal — no second copy of
/// the outgoing page is kept alive.
struct RouteHost: View {
    let store: ObservatoryStore

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        ZStack {
            page
                .id(store.route)
                .transition(.opacity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .clipped()
        .animation(store.transitions.animation(policy: policy), value: store.route)
        .environment(\.transitionPhase, store.transitions.phase)
    }

    @ViewBuilder
    private var page: some View {
        // 每次只构造当前 route 的页面；数据不可用时由页面级占位明确表达“无数据”，不伪装成完成。
        switch store.route {
        case .overview: OverviewPage(store: store)
        case .workflow: WorkflowPage(store: store)
        case .intelligence: IntelligencePage(store: store)
        case .portfolio:
            if store.hasLiveData(for: .portfolio) {
                PortfolioPage(store: store)
            } else {
                LiveUnavailablePage(route: .portfolio)
            }
        case .outcome:
            if store.hasLiveData(for: .outcome) {
                OutcomePage(store: store)
            } else {
                LiveUnavailablePage(route: .outcome)
            }
        case .learning:
            if store.hasLiveData(for: .learning) {
                LearningPage(store: store)
            } else {
                LiveUnavailablePage(route: .learning)
            }
        case .runArchive: RunArchivePage(store: store)
        }
    }
}

// MARK: - Notification helper

extension View {
    /// AppKit lifecycle notifications, delivered on the main actor.
    func onNotification(
        _ name: Notification.Name,
        perform action: @escaping () -> Void
    ) -> some View {
        // Publisher 的回调在主 actor 上更新 Store，View 层只负责把通知转换成无参闭包。
        onReceive(NotificationCenter.default.publisher(for: name)) { _ in action() }
    }
}
