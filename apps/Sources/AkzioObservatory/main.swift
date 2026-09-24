import AppKit
import ObservatoryKit

// 文件职责：这是 macOS App 的最薄启动层。它只解析进程参数并把控制权交给
// ObservatoryKit；界面、Core 连接和截图逻辑都不在这里实现。
// Thin entry point. All UI lives in ObservatoryKit so it stays check-testable.
//
// Two modes: open the window, or render one deterministic frame and exit.
let arguments = Array(CommandLine.arguments.dropFirst())

// dropFirst() 返回不包含可执行文件名的数组；Array 在这里取得独立的值，
// 方便把同一组参数交给捕获入口或常驻 App 入口。
// 捕获模式在主线程生成一帧并退出；没有该 flag 才进入常驻的 App 生命周期。
if CaptureCommand.handles(arguments) {
    // assumeIsolated 只是在已知当前位于 MainActor 的启动路径中恢复隔离类型，
    // 让捕获命令可以安全访问 AppKit/SwiftUI 的主线程状态；它不会启动异步任务。
    let status = MainActor.assumeIsolated { CaptureCommand.run(arguments) }
    // 捕获模式把命令结果转换成进程退出码，避免继续创建常驻窗口。
    exit(status)
}

// 非捕获模式进入 SwiftUI/AppKit 的生命周期；返回后的窗口和 Core 连接由框架驱动。
ObservatoryLauncher.main()
