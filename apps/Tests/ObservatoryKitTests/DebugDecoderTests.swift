import Foundation
@testable import ObservatoryKit

@main
struct DebugDecoderTests {
    static func main() throws {
        let checks = DebugDecoderTests()
        try checks.testCoreEligibilityAndIdentityDecodeWithoutInferringReadiness()
        checks.testMissingCoreAuthorityDoesNotDefaultToReady()
        try checks.testDebugTransportOnlyAcceptsLoopback()
        if CommandLine.arguments.count > 1 {
            let data = try Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[1]))
            let run = try ObserverClient.decoder().decode(DebugRunPayload.self, from: data)
            print("Live Core projection decoded: \(run.session.runID), \(run.nodes.count) nodes, \(run.session.status)")
        }
        if CommandLine.arguments.count > 2 {
            let data = try Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[2]))
            let payload = try ObserverClient.decoder().decode(ObserverSnapshotPayload.self, from: data)
            let projection = LiveProjection(payload: payload)
            precondition(Set(projection.workflow.nodes.map(\.id)).count == projection.workflow.nodes.count)
            precondition(projection.workflow.nodes.filter { $0.stage == .critic }.count == 3)
            let universe = SignalUniverseLayout.nodes(from: projection.workflow)
            precondition(Set(universe.map(\.id)).count == universe.count)
            precondition(Set(universe.map(\.id)) == Set(projection.workflow.nodes.map(\.id)))
            print("PASS: formal three-Critic Observer projection preserves unique Task IDs")
        }
        print("PASS: 3 Swift Debug contract checks")
    }
    func expectEqual<T: Equatable>(_ a: T, _ b: T) { precondition(a == b) }
    func expectTrue(_ value: Bool) { precondition(value) }
    func expectFalse(_ value: Bool) { precondition(!value) }
    func expectThrows<T>(_ operation: @autoclosure () throws -> T) {
        do { _ = try operation(); preconditionFailure("Expected rejection") } catch {}
    }
    func expectNoThrow<T>(_ operation: @autoclosure () throws -> T) throws { _ = try operation() }
    func testCoreEligibilityAndIdentityDecodeWithoutInferringReadiness() throws {
        let data = Data(#"""
        {"session":{"identity":{"run_id":"run-a","llm_mode":"real","broker_write_policy":"forbidden","learning_scope":"isolated"},"revision":7,"status":"paused","execution_mode":"manual","updated_at":"2026-09-09T00:00:00Z"},"workflow_status":"running","nodes":[{"task":{"node":{"task_id":"exact-t1","dependencies":["evidence"]},"status":"pending"},"role":"research.analyst","horizon":"t1","attempts":[],"business_ready":true,"step_eligible":false,"retry_eligible":false,"blocked_reason":"runtime_identity_changed","output_refs":[],"budget":{}}],"events":[],"artifacts":[],"acceptance":[{"business_result":"Rejected","test_result":"PASS","checks":[]}],"observed_at":"2026-09-09T00:00:00.123Z","allowed_actions":["pause"]}
        """#.utf8)
        let run = try ObserverClient.decoder().decode(DebugRunPayload.self, from: data)
        expectEqual(run.session.revision, 7)
        expectEqual(run.nodes[0].id, "exact-t1")
        expectTrue(run.nodes[0].business_ready)
        expectFalse(run.nodes[0].step_eligible)
        expectEqual(run.acceptance[0]["business_result"]?.string, "Rejected")
        expectEqual(run.acceptance[0]["test_result"]?.string, "PASS")
        expectEqual(run.session.identity["broker_write_policy"]?.string, "forbidden")
    }

    func testMissingCoreAuthorityDoesNotDefaultToReady() {
        let data = Data(#"{"task":{},"role":"analyst","attempts":[],"output_refs":[],"budget":{}}"#.utf8)
        expectThrows(try ObserverClient.decoder().decode(DebugNodePayload.self, from: data))
    }

    func testDebugTransportOnlyAcceptsLoopback() throws {
        expectThrows(try ObserverClient(endpoint: URL(string: "https://example.com")!, token: "test"))
        try expectNoThrow(try ObserverClient(endpoint: URL(string: "http://127.0.0.1:17342")!, token: "test"))
    }
}
