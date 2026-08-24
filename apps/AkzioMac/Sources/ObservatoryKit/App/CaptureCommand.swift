import AppKit
import SwiftUI

// MARK: - Capture command
//
// `AkzioObservatory --capture --scenario 03 --route workflow --out shot.png`
//
// Renders a settled frame off-screen with `ImageRenderer`. Because every scenario is
// built from a frozen clock and a seeded generator, and no transition is in flight,
// running the same command twice produces the same pixels.
//
// Caveat worth knowing: `ImageRenderer` rasterises without a live backdrop, so glass
// chrome renders as its opaque fallback rather than as blurred material. The layout,
// density, palette and copy are faithful; the blur is not.
@MainActor
public enum CaptureCommand {
    public struct Options: Sendable {
        public var scenario: MockScenario
        public var route: AppRoute
        public var output: String
        public var width: CGFloat
        public var height: CGFloat
        public var scale: CGFloat
        public var settings: Bool
        public var compact: Bool
        public var language: AppLanguage
    }

    /// Returns true when the arguments asked for a capture, so `main` knows not to
    /// open a window. Deliberately not actor-isolated: it only reads the argv array.
    public nonisolated static func handles(_ arguments: [String]) -> Bool {
        arguments.contains("--capture")
    }

    public static func run(_ arguments: [String]) -> Int32 {
        guard let options = parse(arguments) else {
            FileHandle.standardError.write(Data(usage.utf8))
            return 2
        }
        do {
            try render(options)
            print("captured \(options.output)")
            return 0
        } catch {
            FileHandle.standardError.write(Data("capture failed: \(error)\n".utf8))
            return 1
        }
    }

    // MARK: Rendering

    enum CaptureError: Error, CustomStringConvertible {
        case renderFailed
        case encodeFailed

        var description: String {
            switch self {
            case .renderFailed: "ImageRenderer produced no image"
            case .encodeFailed: "could not encode PNG data"
            }
        }
    }

    static func render(_ options: Options) throws {
        let content = AppShell(
            scenario: options.scenario,
            route: options.route,
            settingsPresented: options.settings,
            compactLayout: options.compact,
            language: options.language
        )
        .frame(width: options.width, height: options.height)
        .environment(\.colorScheme, .dark)
        .environment(\.akzioRendersOffscreen, true)

        let renderer = ImageRenderer(content: content)
        renderer.scale = options.scale
        renderer.isOpaque = true

        guard let cgImage = renderer.cgImage else { throw CaptureError.renderFailed }
        let bitmap = NSBitmapImageRep(cgImage: cgImage)
        bitmap.size = NSSize(width: options.width, height: options.height)
        guard let data = bitmap.representation(using: .png, properties: [:]) else {
            throw CaptureError.encodeFailed
        }

        let url = URL(fileURLWithPath: options.output)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try data.write(to: url, options: .atomic)
    }

    // MARK: Argument parsing

    static let usage = """
    usage: AkzioObservatory --capture --scenario <NN|name> --route <route> --out <path>
                            [--size WxH] [--scale N] [--settings] [--compact]
                            [--language en|zh-Hans]

      --scenario  two-digit scenario code (01–20) or its raw name
      --route     \(AppRoute.allCases.map(\.rawValue).joined(separator: " | "))
      --size      point size of the window frame, default 1512x982
      --scale     backing scale factor, default 2
      --settings  render with the Settings layer open
      --compact   render in the narrow (popover inspector) layout
      --language  force capture language; defaults to system

    """

    static func parse(_ arguments: [String]) -> Options? {
        var values: [String: String] = [:]
        var flags: Set<String> = []
        var index = 0
        while index < arguments.count {
            let argument = arguments[index]
            guard argument.hasPrefix("--") else { index += 1; continue }
            let key = String(argument.dropFirst(2))
            let next = index + 1 < arguments.count ? arguments[index + 1] : nil
            if let next, !next.hasPrefix("--") {
                values[key] = next
                index += 2
            } else {
                flags.insert(key)
                index += 1
            }
        }

        guard let scenarioToken = values["scenario"],
              let scenario = MockScenario.named(scenarioToken),
              let routeToken = values["route"],
              let route = AppRoute(rawValue: routeToken),
              let output = values["out"]
        else { return nil }

        var width: CGFloat = 1512
        var height: CGFloat = 982
        if let size = values["size"] {
            let parts = size.lowercased().split(separator: "x").compactMap { Double($0) }
            if parts.count == 2 {
                width = CGFloat(parts[0])
                height = CGFloat(parts[1])
            }
        }

        return Options(
            scenario: scenario,
            route: route,
            output: output,
            width: width,
            height: height,
            scale: CGFloat(Double(values["scale"] ?? "2") ?? 2),
            settings: flags.contains("settings"),
            compact: flags.contains("compact"),
            language: values["language"] == "zh-Hans"
                ? .simplifiedChinese
                : (values["language"] == "en" ? .english : .system)
        )
    }
}
