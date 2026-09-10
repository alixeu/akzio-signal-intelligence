import AppKit
import SwiftUI

public enum WindowChromeLayout {
    public static let trafficLightVerticalOffset: CGFloat = -8

    public static func buttonOriginY(containerHeight: CGFloat, buttonHeight: CGFloat) -> CGFloat {
        // AppKit's title-bar coordinates grow upward; the negative offset lowers
        // the native traffic lights into visual alignment with the AKZIO wordmark.
        max(
            0,
            containerHeight
                - AkzioLayout.statusBarHeight / 2
                - buttonHeight / 2
                + trafficLightVerticalOffset
        )
    }
}

/// Extends SwiftUI content through the native title bar so the traffic lights
/// sit inside Akzio's status bar instead of reserving a separate strip.
struct WindowChromeConfigurator: NSViewRepresentable {
    let desktopBlurEnabled: Bool

    init(desktopBlurEnabled: Bool = true) {
        self.desktopBlurEnabled = desktopBlurEnabled
    }

    func makeNSView(context: Context) -> NSView { ChromeProbeView(frame: .zero) }
    func updateNSView(_ nsView: NSView, context: Context) {
        (nsView as? ChromeProbeView)?.configureWindow(desktopBlurEnabled: desktopBlurEnabled)
    }
}

private final class ChromeProbeView: NSView {
    private var desktopBlurEnabled = true
    private var desktopEffectView: NSVisualEffectView?

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        configureWindow(desktopBlurEnabled: desktopBlurEnabled)
    }

    func configureWindow(desktopBlurEnabled: Bool) {
        self.desktopBlurEnabled = desktopBlurEnabled
        guard let window else { return }
        window.styleMask.insert(.fullSizeContentView)
        window.titleVisibility = .hidden
        window.titlebarAppearsTransparent = true
        window.titlebarSeparatorStyle = .none
        window.isOpaque = false
        window.backgroundColor = .clear
        window.appearance = NSAppearance(named: .darkAqua)
        window.isMovableByWindowBackground = true

        // Insert the effect into the actual content view, rather than relying on
        // SwiftUI's safe-area background. That makes the desktop blur cover the
        // titlebar/full-size content region as well as the page body.
        if let contentView = window.contentView {
            if desktopEffectView?.superview !== contentView {
                desktopEffectView?.removeFromSuperview()
                let effectView = NSVisualEffectView(frame: contentView.bounds)
                effectView.autoresizingMask = [.width, .height]
                effectView.wantsLayer = true
                effectView.layer?.zPosition = -1
                effectView.appearance = NSAppearance(named: .darkAqua)
                if let firstContentView = contentView.subviews.first {
                    contentView.addSubview(effectView, positioned: .below, relativeTo: firstContentView)
                } else {
                    contentView.addSubview(effectView)
                }
                desktopEffectView = effectView
            }

            desktopEffectView?.appearance = desktopBlurEnabled
                ? NSAppearance(named: .darkAqua)
                : nil
            desktopEffectView?.material = desktopBlurEnabled ? .hudWindow : .windowBackground
            desktopEffectView?.blendingMode = desktopBlurEnabled ? .behindWindow : .withinWindow
            desktopEffectView?.state = desktopBlurEnabled ? .active : .inactive
            desktopEffectView?.isHidden = !desktopBlurEnabled
        }

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
