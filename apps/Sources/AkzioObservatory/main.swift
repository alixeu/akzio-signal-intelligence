import AppKit
import ObservatoryKit

// Thin entry point. All UI lives in ObservatoryKit so it stays check-testable.
//
// Two modes: open the window, or render one deterministic frame and exit.
let arguments = Array(CommandLine.arguments.dropFirst())

// 捕获模式在主线程生成一帧并退出；没有该 flag 才进入常驻的 App 生命周期。
if CaptureCommand.handles(arguments) {
    let status = MainActor.assumeIsolated { CaptureCommand.run(arguments) }
    exit(status)
}

ObservatoryLauncher.main()
