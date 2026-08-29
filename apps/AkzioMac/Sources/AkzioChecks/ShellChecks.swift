import Foundation
import SwiftUI
import ObservatoryKit

/// Shell-level rules: one navigation pipeline for keyboard and sidebar,
/// Settings as a layer rather than a route, and policy resolution.
@MainActor
func runShellChecks() {
    Check.suite("Shell — routes") {
        Check.equal(AppRoute.primary.count, 7, "primary route count")
        Check.equal(
 WindowChromeLayout.buttonOriginY(containerHeight: 80, buttonHeight: 14),
 51,
            "native traffic lights align with the 44pt sidebar toolbar"
        )
        Check.expect(
            !AppRoute.primary.contains(.scenarioGallery),
            "the debug gallery is not a primary route"
        )
        var shortcuts: Set<String> = []
        for route in AppRoute.allCases {
            guard let shortcut = route.shortcut else {
                Check.expect(false, "\(route.rawValue) has a shortcut")
                continue
            }
            let key = String(shortcut.character)
            Check.expect(shortcuts.insert(key).inserted, "⌘\(key) is unique")
            Check.expect(key != "8", "⌘8 stays reserved for the Settings layer")
        }
    }

    Check.suite("Shell — ambient clock") {
        let origin = Date(timeIntervalSinceReferenceDate: 10_000)
        Check.equal(
            AmbientClock.elapsed(sample: origin, origin: origin),
            0,
            "the first ambient frame starts at zero rather than wall-clock phase"
        )
    }

    Check.suite("Shell — navigation pipeline") {
            let store = ObservatoryStore(autoStartsCore: false)
        Check.equal(store.route, .overview, "opens on Overview")
        Check.expect(!store.transitions.isRunning, "idle at rest")

        // Sidebar path.
        store.navigate(to: .workflow)
        Check.equal(store.route, .workflow, "sidebar navigation moves the route")
        Check.expect(store.transitions.isRunning, "sidebar navigation starts a transition")
        Check.equal(
            store.transitions.descriptor.style,
            .sharedElement,
            "overview↔workflow uses shared elements"
        )

        // Keyboard path uses the same coordinator and only shortens the response.
        let normal = store.transitions.animation(policy: .full)
        store.navigate(to: .portfolio, fromKeyboard: true)
        Check.equal(store.route, .portfolio, "keyboard navigation moves the route")
        Check.expect(store.transitions.intent?.fromKeyboard == true, "keyboard intent is flagged")
        Check.expect(String(describing: normal) != String(describing: store.transitions.animation(policy: .full)),
                     "keyboard transition is not the default timing")

        // Same-route activation is a no-op, so a double click never replays.
        let generation = store.transitions.generation
        store.navigate(to: .portfolio)
        Check.equal(store.transitions.generation, generation, "re-selecting a route does nothing")

        // Settings is a layer: it must never change the route.
        store.toggleSettings()
        Check.expect(store.settingsPresented, "sidebar Settings row opens the Settings layer")
        Check.equal(store.route, .portfolio, "Settings does not replace the page")
        store.toggleSettings()
        Check.expect(!store.settingsPresented, "sidebar Settings row closes the Settings layer")
    }

    Check.suite("Shell — archive reveal") {
            let store = ObservatoryStore(autoStartsCore: false)
        store.revealRunInArchive(store.snapshot.run.runId)
        Check.equal(store.route, .runArchive, "reveal run navigates archive")
        Check.equal(
            store.selectedArchiveRow?.runID,
            store.snapshot.run.runId,
            "reveal run selects matching archive row"
        )
    }

    Check.suite("Shell — policy resolution") {
            let store = ObservatoryStore(autoStartsCore: false)
        Check.expect(!store.motionPolicy.isReduced, "full motion by default")

        store.systemReduceMotion = true
        Check.expect(store.motionPolicy.isReduced, "system Reduce Motion wins")
        store.systemReduceMotion = false

        store.settings.reduceMotionOverride = true
        Check.expect(store.motionPolicy.isReduced, "user override reduces motion")
        store.settings.reduceMotionOverride = false

        store.settings.globalMotionEnabled = false
        Check.expect(store.motionPolicy.isReduced, "motion switch off reduces motion")
        Check.expect(!store.canvasPolicy.runsAmbient, "ambient canvases stop when motion is off")
        store.settings.globalMotionEnabled = true

        store.navigate(to: .outcome)
        Check.expect(store.canvasPolicy.isPaused, "canvases pause during a transition")
    }

    Check.suite("Shell — scenario switching") {
            let store = ObservatoryStore(autoStartsCore: false)
        store.load(.settingsReduceMotion)
        Check.equal(store.scenario, .settingsReduceMotion, "scenario loaded")
        Check.expect(store.settings.reduceMotionOverride, "scenario 17 ships reduce motion on")
        Check.expect(store.motionPolicy.isReduced, "and the policy follows")

        store.load(.dataUnavailable)
        Check.equal(store.snapshot.scenarioID, "18", "snapshot swapped")
        Check.expect(store.selectedArchiveRow != nil, "archive selection re-seeded")
        Check.equal(store.selectedHorizon, store.snapshot.outcome.selected, "horizon selection re-seeded")

        store.load(.criticTriggeredMaterialConflict)
        Check.equal(store.activeStage?.stage.id, "critic", "active stage tracks the scenario")
    }

    Check.suite("Shell — Overview inspector overlay") {
        let expanded = AkzioLayout.inspectorOverlaySize(in: CGSize(width: 900, height: 700))
        Check.equal(expanded.width, AkzioLayout.inspectorWidth, "expanded overlay uses inspector width")
        Check.equal(
            expanded.height,
            AkzioLayout.inspectorOverlayMaxHeight,
            "expanded overlay caps its height and scrolls"
        )

        let compact = AkzioLayout.inspectorOverlaySize(in: CGSize(width: 280, height: 360))
        Check.expect(compact.width <= 280 - AkzioLayout.s6, "compact overlay stays inside content width")
        Check.expect(compact.height <= 360 - AkzioLayout.s6, "compact overlay stays inside content height")

        let store = ObservatoryStore(autoStartsCore: false)
        let synthesizer = store.displayWorkflow.nodes.first { $0.stage == .synthesizer }
        store.selectedStageID = synthesizer?.id
        Check.equal(
            store.selectedStageInspector,
            store.displayWorkflow.inspector(for: synthesizer?.id),
            "Overview node selection resolves the same inspector as Workflow"
        )
    }

    Check.suite("Shell — degradation") {
        let store = ObservatoryStore()

        // Nobody watching: ambient work must stop, not merely slow down.
        store.windowActive = false
        Check.expect(store.canvasPolicy.isPaused, "an inactive window pauses the canvases")
        Check.equal(store.canvasPolicy.particleBudget, 0, "a paused canvas draws no particles")
        Check.expect(!store.canvasPolicy.runsAmbient, "a paused canvas runs no ambient loop")
        store.windowActive = true
        Check.expect(store.canvasPolicy.runsAmbient, "returning focus resumes ambient loops")

        // Narrow windows collapse chrome rather than squeezing the canvas.
        Check.expect(!store.compactLayout, "the shell starts at full width")
        store.compactLayout = true
        Check.expect(store.compactLayout, "compact layout is observable by the pages")
        store.compactLayout = false

        // High Contrast is a user override with no system source on macOS.
        Check.expect(!store.highContrast, "high contrast is off by default")
        store.settings.highContrast = true
        Check.expect(store.highContrast, "high contrast follows the setting")
        store.settings.highContrast = false

        // Reduce Motion collapses the choreography to a short crossfade.
        store.systemReduceMotion = true
        let reduced = store.motionPolicy
        Check.expect(reduced.isReduced, "the system switch reduces motion")
        Check.expect(reduced.travel(40) <= 6, "reduced travel is a 6pt hint at most")
        Check.equal(
            String(describing: reduced.resolve(Motion.route)),
            String(describing: Animation.easeOut(duration: 0.22)),
            "reduced motion resolves to the 220ms crossfade"
        )
        store.systemReduceMotion = false

        // Text Size is clamped: layout must survive the extremes.
        Check.equal(AkzioFont.scaled(13, 0.5), AkzioFont.scaled(13, 0.9), "text scale clamps at 0.9")
        Check.equal(AkzioFont.scaled(13, 3.0), AkzioFont.scaled(13, 1.3), "text scale clamps at 1.3")
        Check.expect(AkzioFont.scaled(13, 1.3) > AkzioFont.scaled(13, 1.0), "larger text is actually larger")
        Check.expect(
            AkzioLayout.compactWidthThreshold > 1280,
            "the compact threshold must trigger before the minimum window width"
        )
    }
}
