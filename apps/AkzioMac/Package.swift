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
        // The installed toolchain is Command Line Tools only: neither XCTest nor
        // swift-testing ships with it, so `swift test` cannot run. Checks are a
        // plain executable instead — `swift run AkzioChecks` exits non-zero on failure.
        .executableTarget(
            name: "AkzioChecks",
            dependencies: ["ObservatoryKit"],
            path: "Sources/AkzioChecks",
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
    ]
)
