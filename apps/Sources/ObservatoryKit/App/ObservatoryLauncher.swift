import SwiftUI

/// Entry point owned by the library so the executable target stays a one-liner.
///
/// No launch animation by design: the window opens straight into Overview.
public enum ObservatoryLauncher {
    public static func main() {
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
        MainActor.assumeIsolated {
            RustCoreSupervisor.shared.stop()
        }
    }
}
