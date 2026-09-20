import Foundation
@testable import ObservatoryKit

@main
struct DebugDecoderTests {
    static func main() async throws {
        let checks = DebugDecoderTests()
        if CommandLine.arguments.count == 4 && CommandLine.arguments[1] == "--render-runtime-inspection" {
            try checks.renderRuntimeInspection(source: CommandLine.arguments[2], directory: CommandLine.arguments[3])
            return
        }
        if CommandLine.arguments.count == 3 && CommandLine.arguments[1] == "--render-display-fixtures" {
            try checks.renderDisplayFixtures(directory: CommandLine.arguments[2])
            return
        }
        if CommandLine.arguments.count == 3 && CommandLine.arguments[1] == "--transport-check" {
            try await checks.testRedirectTransport(endpoint: URL(string: CommandLine.arguments[2])!)
            print("PASS: Observer transport rejects redirects and accepts direct responses")
            return
        }
        precondition(RunPurpose.userLaunchModes == [.positionPlan, .paper])
        checks.testConfigurationLocationMatchesRuntimeOverrides()
        checks.testWorkflowFitKeepsEntireGraphInViewport()
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
        try checks.testArchiveTaskIdentityAndDetachedConsistency()
        try checks.testOutcomeRequiresEvidenceAndNoOrderStillEvaluates()
        try checks.testLatestEventsAndMissingSamples()
        try checks.testWorkflowOrderingAndInitialInspection()
        try checks.testResearchAuditUsesRustRecordsAndPreservesLegacyAbsence()
        try checks.testTypedNodeSpecOverridesHistoricalProse()
        try checks.testRuntimeInspectionAndJournalDecode()
        print("PASS: 12 Swift Debug/display contract checks")
    }
    func expectEqual<T: Equatable>(_ a: T, _ b: T) { precondition(a == b) }
    func testTypedNodeSpecOverridesHistoricalProse() throws {
        let data = Data(#"{"task_id":"node-a","recipe_id":"research.analyst","objective":"[research_horizon=t5]","dependencies":[],"spec":{"key":"analyst_t1","horizon":"t1","research_round":0,"proposal_revision":null}}"#.utf8)
        let node = try ObserverClient.decoder().decode(ObserverNodePayload.self, from: data)
        expectEqual(node.horizon, "t1")
        expectEqual(node.spec?.research_round, 0)
        print("PASS: typed scope takes precedence over contradictory historical prose")
    }

    func testRuntimeInspectionAndJournalDecode() throws {
        let inspection = Data(#"{"workflow":{"run":{"run_id":"run-typed"}},"blueprint":{"definition_version":1,"definition_hash":"hash","purpose":"position_plan","nodes":[],"entry":[],"finish":[]},"control":{"revision":7,"status":"paused","execution_mode":"manual"},"checkpoint":{"event_cursor":99,"boundary":"debug.control_changed","control_revision":7,"created_at":"2026-09-23T00:00:00Z"},"recovery":"manual_control","allowed_actions":["step"]}"#.utf8)
        let value = try ObserverClient.decoder().decode(RuntimeInspectionPayload.self, from: inspection)
        expectEqual(value.runID, "run-typed")
        expectEqual(value.checkpoint?.event_cursor, 99)
        expectEqual(value.allowed_actions, ["step"])
        let journal = Data(#"{"events":[{"cursor":100,"event_type":"runtime.checkpoint_saved","task_id":null,"attempt_id":null,"artifact":{"artifact_id":"checkpoint-id","kind":"runtime_checkpoint"},"source_refs":[{"artifact_id":"graph-id","kind":"workflow_graph"}],"created_at":"2026-09-23T00:00:00Z"}],"next_cursor":100,"has_more":true}"#.utf8)
        let page = try ObserverClient.decoder().decode(RuntimeJournalPayload.self, from: journal)
        expectEqual(page.next_cursor, 100)
        expectEqual(page.events[0].source_refs[0]["artifact_id"]?.string, "graph-id")
        expectTrue(page.has_more)
        print("PASS: runtime control, checkpoint cursor and journal provenance retain Core values")
    }
    func testConfigurationLocationMatchesRuntimeOverrides() {
        let home = URL(fileURLWithPath: "/Users/example", isDirectory: true)
        expectEqual(CoreRuntimePaths.configurationLocation(environment: [:], homeDirectory: home).path,
                    "/Users/example/.akzio/config.toml")
        expectEqual(CoreRuntimePaths.configurationLocation(environment: ["AKZIO_HOME": "/workspace/review"], homeDirectory: home).path,
                    "/workspace/review/config.toml")
        expectEqual(CoreRuntimePaths.configurationLocation(environment: ["AKZIO_HOME": "/workspace/review", "AKZIO_CORE_CONFIG": "/workspace/custom.toml"], homeDirectory: home).path,
                    "/workspace/custom.toml")
        print("PASS: displayed config location matches canonical and isolated runtime paths")
    }

    func expectTrue(_ value: Bool) { precondition(value) }
    func expectFalse(_ value: Bool) { precondition(!value) }
    func expectThrows<T>(_ operation: @autoclosure () throws -> T) {
        do { _ = try operation(); preconditionFailure("Expected rejection") } catch {}
    }
    func expectNoThrow<T>(_ operation: @autoclosure () throws -> T) throws { _ = try operation() }
    func testCoreEligibilityAndIdentityDecodeWithoutInferringReadiness() throws {
        let data = Data(#"""
        {"session":{"identity":{"run_id":"run-a","llm_mode":"real","broker_write_policy":"forbidden","learning_scope":"isolated"},"revision":7,"status":"paused","execution_mode":"manual","updated_at":"2026-09-09T00:00:00Z"},"workflow_status":"running","research":{"research_status":"completed","review_status":"accepted","display":"研究完成 · 终稿审查通过 · 缺 Policy，Decision 阻断"},"nodes":[{"task":{"node":{"task_id":"exact-t1","dependencies":["evidence"]},"status":"pending"},"role":"research.analyst","horizon":"t1","attempts":[],"business_ready":true,"step_eligible":false,"retry_eligible":false,"blocked_reason":"runtime_identity_changed","output_refs":[],"budget":{}}],"events":[],"artifacts":[],"acceptance":[{"business_result":"Rejected","test_result":"PASS","checks":[]}],"observed_at":"2026-09-09T00:00:00.123Z","allowed_actions":["pause"]}
        """#.utf8)
        let run = try ObserverClient.decoder().decode(DebugRunPayload.self, from: data)
        expectEqual(run.session.revision, 7)
        expectEqual(run.research?["display"]?.string, "研究完成 · 终稿审查通过 · 缺 Policy，Decision 阻断")
        expectEqual(run.workflow_status, "running")
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

    func testRedirectTransport(endpoint: URL) async throws {
        let direct = try ObserverClient(endpoint: endpoint.appending(path: "direct"), token: "fixture-token")
        let runs = try await direct.debugRuns()
        expectFalse(runs.enabled)
        let redirected = try ObserverClient(endpoint: endpoint.appending(path: "redirect"), token: "fixture-token")
        do {
            _ = try await redirected.debugRuns()
            preconditionFailure("Observer followed a redirect carrying its control token")
        } catch {
            precondition(error.localizedDescription.contains("307"))
        }
        do {
            _ = try await redirected.fetchSnapshot()
            preconditionFailure("Snapshot followed a redirect")
        } catch {
            precondition(error.localizedDescription.contains("307"))
        }
        do {
            for try await _ in redirected.events(after: 0) {}
            preconditionFailure("SSE followed a redirect")
        } catch {
            precondition(error.localizedDescription.contains("307"))
        }
    }
}
