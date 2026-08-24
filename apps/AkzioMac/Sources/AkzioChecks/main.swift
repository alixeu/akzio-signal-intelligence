// Entry point for the check suite: `swift run AkzioChecks`.
print("Akzio Observatory — check suite\n")

runColorTokenChecks()
runFormattingChecks()
runTransitionChecks()
runMotionPolicyChecks()
runPresentationChecks()
runScenarioChecks()
runObserverContractChecks()
runLocalizationChecks()

// The shell store is main-actor isolated, like the UI it drives.
MainActor.assumeIsolated {
    runTransitionCoordinatorChecks()
    runShellChecks()
}

Check.finish()
