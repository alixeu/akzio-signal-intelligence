import AppKit
import ObservatoryKit

// Thin entry point. All UI lives in ObservatoryKit so it stays check-testable.
//
// Two modes: open the window, or render one deterministic frame and exit.
let arguments = Array(CommandLine.arguments.dropFirst())

if CaptureCommand.handles(arguments) {
    let status = MainActor.assumeIsolated { CaptureCommand.run(arguments) }
    exit(status)
}

ObservatoryLauncher.main()
