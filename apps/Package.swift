// swift-tools-version:6.2
import PackageDescription

// Akzio Observatory — native macOS SwiftUI shell with deterministic Mock,
// authenticated Observer reads, and a bundled Rust daemon supervisor.
let package = Package(
    name: "AkzioObservatory",
    platforms: [.macOS("26.0")],
    targets: [
        .target(
            name: "ObservatoryKit",
            path: "Sources/ObservatoryKit",
            swiftSettings: [.swiftLanguageMode(.v5)],
            linkerSettings: [.linkedFramework("Security")]
        ),
        .executableTarget(
            name: "AkzioObservatory",
            dependencies: ["ObservatoryKit"],
            path: "Sources/AkzioObservatory",
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
    ]
)
