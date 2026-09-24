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
        // Options 是一次截图请求的值快照；Sendable 让参数可安全地从命令入口传到主 actor 渲染。
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
        // 这里只做纯数组查询，不触碰 AppKit/SwiftUI 状态，所以可以在任意执行上下文判断入口。
        arguments.contains("--capture")
    }

    public static func run(_ arguments: [String]) -> Int32 {
        // run 把解析、主 actor 渲染和文件写入的 throws 结果压缩成 CLI 约定的退出码。
        // 参数不完整返回 2；渲染/写文件失败返回 1；只有 PNG 已写入才返回 0。
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
        // render 只接收已解析的值类型 Options；失败通过 throws 返回给 run，不在 UI 中显示错误。
        // 这里构造的是关闭 Core 自动启动的离屏 AppShell，因此截图不会启动真实任务或网络请求。
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

        // ImageRenderer 可能在布局失败时不给图像；该错误在 run 中转换为非零退出码。
        guard let cgImage = renderer.cgImage else { throw CaptureError.renderFailed }
        let bitmap = NSBitmapImageRep(cgImage: cgImage)
        bitmap.size = NSSize(width: options.width, height: options.height)
        guard let data = bitmap.representation(using: .png, properties: [:]) else {
            throw CaptureError.encodeFailed
        }

        let url = URL(fileURLWithPath: options.output)
        // 先确保输出目录存在，再用 atomic 写入；写入失败不会报告 captured。
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
        // parse 的 Optional 表示“是否具备最小可渲染输入”；尺寸、scale 等可选字段各自回退默认值。
        var values: [String: String] = [:]
        var flags: Set<String> = []
        var index = 0
        // 解析器只识别 --key value 和无值 flag；未知 key 保留在字典中，最终由必需字段校验淘汰。
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

        // 场景、路由和输出路径缺一不可；这里的 nil 会让调用方打印完整 usage，而不是部分渲染。
        guard let scenarioToken = values["scenario"],
              let scenario = MockScenario.named(scenarioToken),
              let routeToken = values["route"],
              let route = AppRoute(rawValue: routeToken),
              let output = values["out"]
        else { return nil }

        var width: CGFloat = 1512
        var height: CGFloat = 982
        // 尺寸非法时保留默认值；scale 同样回退到 2，避免一个坏参数改变渲染目标的可用性。
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
