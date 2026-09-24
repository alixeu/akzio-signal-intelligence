// swift-tools-version:6.2
import PackageDescription

// 文件职责：声明 macOS Observatory 的 Swift Package 目标、最低系统版本、源码目录、
// Swift 语言模式和 Security framework 依赖；它只影响 Swift 构建拓扑，不启动 Rust Core。
// Akzio Observatory — native macOS SwiftUI shell with deterministic Mock,
// authenticated Observer reads, and a bundled Rust daemon supervisor.
let package = Package(
    name: "AkzioObservatory",
    platforms: [.macOS("26.0")],
    targets: [
        // ObservatoryKit 承担 UI/数据层；主 App 复用它。
        .target(
            name: "ObservatoryKit",
            path: "Sources/ObservatoryKit",
            swiftSettings: [.swiftLanguageMode(.v5)],
            linkerSettings: [.linkedFramework("Security")]
        ),
        // 这个 executable 只提供正式 App 入口，业务界面由 ObservatoryKit 承担。
        .executableTarget(
            name: "AkzioObservatory",
            dependencies: ["ObservatoryKit"],
            path: "Sources/AkzioObservatory",
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
    ]
)
