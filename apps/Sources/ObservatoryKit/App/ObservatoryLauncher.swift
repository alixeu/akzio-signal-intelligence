import SwiftUI

/// Entry point owned by the library so the executable target stays a one-liner.
///
/// No launch animation by design: the window opens straight into Overview.
public enum ObservatoryLauncher {
    public static func main() {
        // SwiftPM 可执行目标只调用这一层；真正的 App 生命周期由 SwiftUI 接管。
        ObservatoryApp.main()
    }
}

struct ObservatoryApp: App {
    @NSApplicationDelegateAdaptor(ObservatoryAppDelegate.self) private var appDelegate

    var body: some Scene {
        Window("Akzio Observatory", id: "observatory") {
            AppShell()
        }
        .defaultSize(width: 1512, height: 982)
        .windowStyle(.hiddenTitleBar)

        WindowGroup("Run Detail", for: DetachedRunPayload.self) { $payload in
            if let payload {
                DetachedRunWindow(payload: payload)
            }
        }
        .defaultSize(width: 760, height: 560)
    }
}

private final class ObservatoryAppDelegate: NSObject, NSApplicationDelegate {
    func applicationWillTerminate(_ notification: Notification) {
        // 终止通知到达时只停止共享 Core；不会在退出阶段启动新的请求或等待后台任务完成。
        MainActor.assumeIsolated {
            RustCoreSupervisor.shared.stop()
        }
    }
}
