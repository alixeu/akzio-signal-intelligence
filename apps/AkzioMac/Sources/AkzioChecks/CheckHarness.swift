import Foundation

/// Minimal assertion harness.
///
/// Command Line Tools ships no XCTest and no swift-testing module, so the check
/// suite is a normal executable: `swift run AkzioChecks`. Exit code 1 on failure.
enum Check {
    nonisolated(unsafe) private(set) static var failures: [String] = []
    nonisolated(unsafe) private(set) static var passed = 0
    nonisolated(unsafe) private static var currentSuite = ""

    static func suite(_ name: String, _ body: () -> Void) {
        currentSuite = name
        print("• \(name)")
        body()
    }

    static func expect(
        _ condition: Bool,
        _ message: String,
        file: StaticString = #fileID,
        line: UInt = #line
    ) {
        if condition {
            passed += 1
        } else {
            failures.append("[\(currentSuite)] \(message)  (\(file):\(line))")
        }
    }

    static func equal<T: Equatable>(
        _ actual: T,
        _ expected: T,
        _ label: String,
        file: StaticString = #fileID,
        line: UInt = #line
    ) {
        expect(
            actual == expected,
            "\(label): expected \(expected), got \(actual)",
            file: file,
            line: line
        )
    }

    static func finish() -> Never {
        print("")
        if failures.isEmpty {
            print("✓ \(passed) checks passed")
            exit(0)
        }
        print("✗ \(failures.count) failed, \(passed) passed")
        for failure in failures {
            print("  - \(failure)")
        }
        exit(1)
    }
}
