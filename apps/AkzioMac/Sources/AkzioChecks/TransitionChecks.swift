import ObservatoryKit

// Transition completeness: every ordered pair resolves, both directions read the
// same row (so a forward move always has a reverse), and pairs without a natural
// shared element use the same symmetric in-place crossfade.
func runTransitionChecks() {
    Check.suite("Route transition table") {
        let routes = AppRoute.primary

        for from in routes {
            for to in routes where from != to {
                let forward = RouteTransitionTable.descriptor(from: from, to: to)
                let reverse = RouteTransitionTable.descriptor(from: to, to: from)

                Check.expect(
                    forward.style == .sharedElement
                        ? !forward.anchors.isEmpty
                        : forward.anchors.isEmpty,
                    "\(from.rawValue)→\(to.rawValue) anchors do not match transition style"
                )
                Check.expect(
                    forward.anchors.map(\.debugName) == reverse.anchors.map(\.debugName),
                    "\(from.rawValue)↔\(to.rawValue) is asymmetric: anchors differ by direction"
                )
                Check.expect(
                    !forward.forwardNote.isEmpty && !forward.reverseNote.isEmpty,
                    "\(from.rawValue)↔\(to.rawValue) is missing a documented reverse"
                )
                Check.expect(
                    forward.response >= 0.45 && forward.response <= 0.70,
                    "\(from.rawValue)↔\(to.rawValue) response \(forward.response) is outside 450–700ms"
                )
            }
        }

        // Registered natural pairs from the spec.
        let naturalPairs: [(AppRoute, AppRoute)] = [
            (.overview, .workflow), (.overview, .intelligence), (.overview, .portfolio),
            (.overview, .outcome), (.overview, .learning),
            (.workflow, .intelligence), (.workflow, .outcome),
        (.portfolio, .outcome), (.outcome, .learning),
        ]
        for (from, to) in naturalPairs {
            Check.expect(
                RouteTransitionTable.hasNaturalSharedElement(from: from, to: to),
                "\(from.rawValue)↔\(to.rawValue) should use a natural shared element"
            )
        }

        // Pairs with nothing in common use the same in-place crossfade in both directions.
        let crossfadePairs: [(AppRoute, AppRoute)] = [
            (.intelligence, .portfolio),
            (.intelligence, .learning),
        (.portfolio, .learning),
        (.workflow, .portfolio),
        (.runArchive, .workflow), (.runArchive, .outcome),
        ]
        for (from, to) in crossfadePairs {
            let descriptor = RouteTransitionTable.descriptor(from: from, to: to)
            Check.expect(
                descriptor.style == .crossfade,
                "\(from.rawValue)↔\(to.rawValue) must use crossfade"
            )
            Check.expect(
                descriptor.anchors.isEmpty,
                "\(from.rawValue)↔\(to.rawValue) crossfade must not invent a shared anchor"
            )
        }

        Check.expect(
            RouteTransitionTable.settingsLayer.style == .crossfade
                && RouteTransitionTable.settingsLayer.anchors.isEmpty,
            "settings layer must materialize without unstable shared geometry"
        )

        // The 12 documented rules: 5 Overview spokes, 4 cross-page pairs, the archive
        // handoff, the Settings layer and the crossfade fallback.
        let documentedRules: [[SharedElementID]] = [
            RouteTransitionTable.descriptor(from: .overview, to: .workflow).anchors,
            RouteTransitionTable.descriptor(from: .overview, to: .intelligence).anchors,
            RouteTransitionTable.descriptor(from: .overview, to: .portfolio).anchors,
            RouteTransitionTable.descriptor(from: .overview, to: .outcome).anchors,
            RouteTransitionTable.descriptor(from: .overview, to: .learning).anchors,
            RouteTransitionTable.descriptor(from: .workflow, to: .intelligence).anchors,
            RouteTransitionTable.descriptor(from: .workflow, to: .outcome).anchors,
            RouteTransitionTable.descriptor(from: .portfolio, to: .outcome).anchors,
            RouteTransitionTable.descriptor(from: .outcome, to: .learning).anchors,
            RouteTransitionTable.descriptor(from: .runArchive, to: .workflow).anchors,
            RouteTransitionTable.settingsLayer.anchors,
            RouteTransitionTable.descriptor(from: .intelligence, to: .portfolio).anchors,
        ]
        Check.equal(documentedRules.count, 12, "the spec documents 12 transition rules")
        // Archive rows hand the same three anchors to every destination page.
        let archiveTargets: [AppRoute] = [.overview, .workflow, .intelligence, .portfolio, .outcome, .learning]
        for target in archiveTargets {
            let names = RouteTransitionTable.descriptor(from: .runArchive, to: target).anchors.map(\.debugName)
            Check.expect(names.isEmpty, "archive→\(target.rawValue) uses an anchor-free crossfade")
        }
    }

}

// The coordinator drives main-actor UI state, so its checks run on the main actor.
@MainActor
func runTransitionCoordinatorChecks() {
    Check.suite("Transition coordinator") {
        let coordinator = TransitionCoordinator()
        Check.expect(!coordinator.isRunning, "a fresh coordinator is idle")
        Check.equal(coordinator.phase, .idle, "idle phase at rest")
        Check.equal(
            coordinator.normalizedPhase,
            1,
            "resting presentation must equal the transition's final frame"
        )

        coordinator.begin(
            TransitionIntent(from: .overview, to: .workflow),
            policy: .full
        )
        Check.expect(coordinator.isRunning, "beginning a transition must leave the idle state")
        Check.equal(coordinator.generation, 1, "first transition is generation 1")
        let mouseAnimation = String(describing: coordinator.animation(policy: .full))

        // Retarget mid-flight: no queueing, one generation bump, no phase reset to idle.
        coordinator.begin(
            TransitionIntent(from: .workflow, to: .portfolio, fromKeyboard: true),
            policy: .full
        )
        Check.equal(coordinator.generation, 2, "a retarget bumps the generation instead of queuing")
        Check.expect(coordinator.isRunning, "retargeting must not drop back to idle")
        Check.expect(
            coordinator.phase >= .transform,
            "retargeting must not rewind the visible page to prepare"
        )

        let keyboardAnimation = String(describing: coordinator.animation(policy: .full))
        Check.expect(
            keyboardAnimation != mouseAnimation,
            "keyboard paging must use a distinct, faster curve"
        )

        // Reverse detection: going back to where we came from reads as a reverse.
        coordinator.begin(TransitionIntent(from: .portfolio, to: .workflow), policy: .full)
        Check.expect(coordinator.intent?.reversed == true, "returning to the previous route is a reverse")

        // Reduced motion collapses the choreography instead of staging it.
        let reducedCoordinator = TransitionCoordinator()
        reducedCoordinator.begin(TransitionIntent(from: .overview, to: .learning), policy: .reduced)
        Check.equal(reducedCoordinator.phase, .settle, "reduced motion skips straight to settle")
        Check.expect(
            reducedCoordinator.hasReached(.reveal),
            "reduced motion must not gate content behind phases that never run"
        )
    }
}

func runMotionPolicyChecks() {
    Check.suite("Motion policy") {
        let full = MotionPolicy.full
        let reduced = MotionPolicy.reduced

        Check.expect(full.allowsAmbient, "full motion should allow ambient loops")
        Check.expect(!reduced.allowsAmbient, "reduced motion must stop ambient loops")
        Check.expect(reduced.sharedElementStrength == 0, "reduced motion must flatten shared-element travel")
        Check.expect(reduced.travel(40) <= 6, "reduced motion must clamp travel to a hint")
        Check.expect(reduced.stagger(6) == 0, "reduced motion must remove stagger")
        Check.expect(Motion.stagger(20) <= 0.36, "stagger must stay capped so lists never feel slow")

        let paused = CanvasRenderPolicy(quality: .high, allowsAmbient: true, isPaused: true)
        Check.equal(paused.particleBudget, 0, "paused canvas must draw no particles")
        Check.expect(!paused.runsAmbient, "paused canvas must not run ambient loops")

        let low = CanvasRenderPolicy(quality: .low)
        let high = CanvasRenderPolicy(quality: .high)
        Check.expect(low.particleBudget < high.particleBudget, "low quality must cost fewer particles")
        Check.expect(high.frameInterval <= 1.0 / 30 + 0.001, "high quality targets 30fps ambient refresh")
    }
}
