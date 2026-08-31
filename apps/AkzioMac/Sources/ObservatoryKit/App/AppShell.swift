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
            .background(WindowChromeConfigurator())
            .task { await store.bootstrapCore() }
    }

    private var shell: some View {
        GeometryReader { proxy in
            ZStack(alignment: .top) {
                HStack(alignment: .top, spacing: 0) {
                    PageSidebar(
                    route: store.route,
                    onSelect: { store.navigate(to: $0) },
                        onOpenSettings: store.toggleSettings
                    )
                    .frame(maxHeight: .infinity, alignment: .top)
                VStack(spacing: 0) {
                    RunStatusBar(
                        run: store.displayRun,
                        health: store.displayHealth,
                observerState: store.observerState,
                namespace: shared,
                canRun: store.isLive,
                selectedRunPurpose: store.selectedRunPurpose,
                runInFlight: store.runInFlight,
                runMessage: store.runMessage,
                onSelectRunPurpose: store.selectRunPurpose,
                onRun: { Task { await store.runSelectedPurpose() } },
                        onOpenSettings: store.toggleSettings,
                        onCopyRunID: copyRunID,
                        onRevealRun: { store.revealRunInArchive(store.displayRun.runId) }
                    )
                        RouteHost(store: store)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            }
            .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)
            .disabled(store.settingsPresented)
            .accessibilityHidden(store.settingsPresented)

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
            store.windowActive = phase == .active
        }
        .preferredColorScheme(.dark)
    }

    /// Window width decides the layout mode. Measured once per resize, written to the
    /// store so every page reads the same answer.
    private var widthProbe: some View {
        GeometryReader { proxy in
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
        store.systemReduceMotion = reduceMotion
        store.systemReduceTransparency = reduceTransparency
        store.systemHighContrast = colorSchemeContrast == .increased
    }

    private func copyRunID() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(store.displayRun.runId, forType: .string)
    }

    /// ⌘1–⌘7 content routes, ⌘8 Settings, ⌘0 Scenario Gallery. Zero-opacity buttons
    /// keep every navigation path inside the same `store.navigate` pipeline.
    private var keyboardRoutes: some View {
        ZStack {
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
        case .scenarioGallery: ScenarioGalleryPage(store: store)
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
        onReceive(NotificationCenter.default.publisher(for: name)) { _ in action() }
    }
}
