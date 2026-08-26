import AppKit
import SwiftUI

public enum WindowChromeLayout {
    public static func buttonOriginY(containerHeight: CGFloat, buttonHeight: CGFloat) -> CGFloat {
        max(0, containerHeight - AkzioLayout.statusBarHeight / 2 - buttonHeight / 2 - 3)
    }
}

/// Extends SwiftUI content through the native title bar so the traffic lights
/// sit inside Akzio's status bar instead of reserving a separate strip.
struct WindowChromeConfigurator: NSViewRepresentable {
    func makeNSView(context: Context) -> NSView { ChromeProbeView() }
    func updateNSView(_ nsView: NSView, context: Context) {
        (nsView as? ChromeProbeView)?.configureWindow()
    }
}

private final class ChromeProbeView: NSView {
    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        configureWindow()
    }

    func configureWindow() {
        guard let window else { return }
        window.styleMask.insert(.fullSizeContentView)
        window.titleVisibility = .hidden
        window.titlebarAppearsTransparent = true
        window.titlebarSeparatorStyle = .none
        window.isOpaque = false
        window.backgroundColor = .clear
        window.isMovableByWindowBackground = true

        for kind in [NSWindow.ButtonType.closeButton, .miniaturizeButton, .zoomButton] {
            guard let button = window.standardWindowButton(kind),
                  let container = button.superview else { continue }
            var frame = button.frame
            frame.origin.y = WindowChromeLayout.buttonOriginY(
                containerHeight: container.bounds.height,
                buttonHeight: frame.height
            )
            button.setFrameOrigin(frame.origin)
        }
    }
}
