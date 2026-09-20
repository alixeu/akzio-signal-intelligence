import AppKit
import SwiftUI
import Foundation
@testable import ObservatoryKit

extension DebugDecoderTests {
    func testWorkflowFitKeepsEntireGraphInViewport() {
        let layout = DagLayout.layout(for: ScenarioLibrary.snapshot(.paperRunningSynthesizerActive).workflow.nodes)
        for size in [CGSize(width: 500, height: 430), CGSize(width: 850, height: 430), CGSize(width: 1200, height: 300)] {
            let fit = layout.fitScale(in: size)
            expectTrue(fit > 0 && fit <= 1)
            expectTrue(layout.contentSize.width * fit <= size.width - 24 + 0.001)
            expectTrue(layout.contentSize.height * fit <= size.height - 24 + 0.001)
        }
        print("PASS: Fit contains the entire graph at compact and wide viewport sizes")
    }

    func displaySnapshot(status: String = "completed", artifacts: [[String: Any]] = [], hasRun: Bool = true, researchAudit: [String: Any]? = nil) throws -> ObserverSnapshotPayload {
        let start = "2026-09-23T06:00:00Z"
        let tasks: [[String: Any]] = ["research.analyst", "research.critic"].flatMap { role in
            [1, 3, 5].map { horizon in
                ["node": ["task_id": "\(role)-t\(horizon)", "recipe_id": role,
                          "objective": "[research_horizon=t\(horizon)]", "dependencies": []],
                 "status": horizon == 1 ? "succeeded" : (horizon == 3 ? "failed" : "running"),
                 "ready_at": start, "attempt_count": 1,
                 "finished_at": "2026-09-23T06:0\(horizon):00Z"] as [String: Any]
            }
        }
        let workflow: [String: Any] = ["run": ["run_id": "display-test", "purpose": "paper", "topology_id": "paper", "created_at": start],
                                      "status": status, "finished_at": start, "tasks": tasks, "event_cursor": 3]
        let events = [1, 3, 2].map { ["cursor": $0, "event_type": "task.succeeded", "created_at": start] as [String: Any] }
        var value: [String: Any] = [
            "schema_version": 1, "generated_at": start, "event_cursor": 3,
            "core": ["ready": false, "auto_paper": false, "health": ["status": "unknown", "frozen": false, "alerts": []], "approval": ["status": "unknown"]],
            "recent_runs": hasRun ? [workflow] : [],
            "portfolio": ["status": "unavailable"], "learning": ["status": "unavailable"]
        ]
        if hasRun {
            var detail: [String: Any] = ["workflow": workflow, "events": events, "trajectory": [], "artifacts": artifacts]
            if let researchAudit { detail["research_audit"] = researchAudit }
            value["current_run"] = detail
        }
        return try ObserverClient.decoder().decode(ObserverSnapshotPayload.self, from: JSONSerialization.data(withJSONObject: value))
    }

    func testArchiveTaskIdentityAndDetachedConsistency() throws {
        let snapshot = try displaySnapshot()
        let row = LiveProjection(payload: snapshot).archive.rows[0]
        expectEqual(Set(row.stageProgress.map(\.id)).count, 6)
        let detached = DetachedRunPayload(row, stages: row.stageProgress, language: .simplifiedChinese)
        let restored = try JSONDecoder().decode(DetachedRunPayload.self, from: JSONEncoder().encode(detached))
        expectEqual(restored.language, AppLanguage.simplifiedChinese.rawValue)
        expectEqual(restored.stages.map(\.id), row.stageProgress.map(\.id))
        expectEqual(detached.stages.map(\.id), row.stageProgress.map(\.id))
        expectEqual(detached.stages.map(\.label), row.stageProgress.map(\.displayLabel))
        expectEqual(detached.stages.map(\.time), row.stageProgress.map(\.timeLabel))
        for role in ["research.analyst", "research.critic"] {
            let stages = row.stageProgress.filter { $0.label == role }
            expectEqual(stages.map(\.horizon), ["t1", "t3", "t5"])
            expectEqual(stages.map(\.status), [.succeeded, .failed, .running])
            expectEqual(Set(stages.map(\.timeLabel)).count, 3)
        }
        expectEqual(WorkflowDisplay.horizon(objective: "no horizon supplied"), nil)
        print("PASS: archive preserves six Task IDs, horizons, distinct status/time, and detached payload")
    }

    func testOutcomeRequiresEvidenceAndNoOrderStillEvaluates() throws {
        let row = LiveProjection(payload: try displaySnapshot()).archive.rows[0]
        expectEqual(row.status, .completed)
        expectEqual(DetachedRunPayload(row).outcomeCaption, "尚未确认封存")
        let noOrder: [(kind: String, payload: JSONValue)] = [
            ("execution_verdict", .object(["verdict": .string("no_order")])),
            ("outcome_schedule", .object([:]))
        ]
        expectEqual(WorkflowDisplay.executionLabel(noOrder, evidence: "blocked"), "未下单")
        let scheduled = OutcomeEvidencePresentation.from(noOrder)
        expectTrue(scheduled.scheduled)
        expectTrue(scheduled.sealedHorizons.isEmpty)
        expectTrue(scheduled.caption.contains("后续评估"))
        expectTrue(scheduled.caption.contains("尚未确认封存"))
        let sealed = OutcomeEvidencePresentation.from(noOrder + [("outcome", .object(["windows": .array([.object(["horizon": .string("t1")])])]))])
        expectEqual(sealed.sealedHorizons, ["t1"])
        expectFalse(sealed.caption.contains("T5"))
        print("PASS: completed does not imply sealed; NoOrder retains evaluation; only evidenced horizons are sealed")
    }

    func testLatestEventsAndMissingSamples() throws {
        let projection = LiveProjection(payload: try displaySnapshot())
        expectEqual(projection.events.map(\.id), ["event-3", "event-2", "event-1"])
        let empty = LiveProjection(payload: try displaySnapshot(hasRun: false))
        expectEqual(empty.archive.successRatePpm, nil)
        expectEqual(empty.run.latencyMillis, nil)
        expectEqual(empty.run.systemHealthPpm, nil)
        expectEqual(empty.outcome.observedTradingDays, nil)
        expectFalse(empty.run.marketStatusKnown)
        expectFalse(empty.run.hasRun)
        expectEqual(empty.run.displayStatus, "No active run")
        let pending = LiveProjection(payload: try displaySnapshot(status: "running"))
        expectEqual(pending.archive.successRatePpm, nil)
        print("PASS: cursor-descending events with equal timestamps; missing samples remain unknown")
    }

    func testWorkflowOrderingAndInitialInspection() throws {
        let source: [[String: Any]] = [
            ["id": "critic5", "role": "research.critic", "h": "t5", "deps": ["analyst5"], "status": "pending"],
            ["id": "analyst5", "role": "research.analyst", "h": "t5", "deps": ["evidence"], "status": "failed"],
            ["id": "analyst1", "role": "research.analyst", "h": "t1", "deps": ["evidence"], "status": "running"],
            ["id": "evidence", "role": "gate.evidence", "h": "", "deps": [], "status": "succeeded"]
        ]
        let nodes = try source.map { item -> DebugNodePayload in
            let value: [String: Any] = ["task": ["node": ["task_id": item["id"]!, "dependencies": item["deps"]!], "status": item["status"]!], "role": item["role"]!, "horizon": item["h"]!, "attempts": [], "business_ready": false, "step_eligible": false, "retry_eligible": false, "output_refs": [], "budget": [:]]
            return try ObserverClient.decoder().decode(DebugNodePayload.self, from: JSONSerialization.data(withJSONObject: value))
        }
        expectEqual(WorkflowDisplay.ordered(nodes).map(\.id), ["evidence", "analyst1", "analyst5", "critic5"])
        expectEqual(WorkflowDisplay.initialSelection(nodes), "analyst1")
        expectEqual(WorkflowDisplay.initialSelection(nodes.filter { $0.status != "running" }), "analyst5")
        expectTrue(WorkflowDisplay.ordered(nodes).allSatisfy { !$0.step_eligible && !$0.retry_eligible })
        print("PASS: dependency-first display, horizon ordering, running/failure inspection, unchanged eligibility")
    }
}


extension DebugDecoderTests {
    @MainActor
    func renderDisplayFixtures(directory: String) throws {
        let snapshot = try displaySnapshot()
        let row = LiveProjection(payload: snapshot).archive.rows[0]
        let preview = RunPreviewPanel(row: row, stageProgress: row.stageProgress, outcomeEvidence: .unknown,
                                     onViewDetails: {}, onOpenInNewWindow: {}, onDismiss: {})
        let detail = DetachedRunWindow(payload: DetachedRunPayload(row))
        try render(HStack(alignment: .top, spacing: 20) { preview; detail },
                   size: CGSize(width: 1160, height: 820), name: "archive-distinct-stages.png", directory: directory)
        let record = AnalysisRecordPresentation(id: "markdown-fixture", sequence: 1, kind: .researchMemo, actor: "Analyst",
            title: "离线排版样本", body: "# 研究记录\n\n这是 **加粗**、*强调* 与 `行内代码`。\n\n1. 第一项\n2. 第二项\n\n- 证据待补充\n- 结论尚未确认\n\n```text\nT1: succeeded\nT3: failed\nT5: running\n```", taskID: "fixture-analyst-t3", horizon: "t3")
        try render(AnalysisRecordRow(record: record, showsActor: true),
                   size: CGSize(width: 700, height: 550), name: "markdown-record.png", directory: directory)
        let audit = LiveProjection(payload: try displaySnapshot(researchAudit: researchAuditFixture()))
        let inspector = audit.workflow.stageInspectors["research.critic-t3"]!
        try render(StageInspectorPanel(inspector: inspector, node: nil, namespace: nil, width: 440),
                   size: CGSize(width: 440, height: 900), name: "research-audit.png", directory: directory)
        print("PASS: rendered display-only fixtures; no runtime or broker calls")
    }

    @MainActor
    func renderRuntimeInspection(source: String, directory: String) throws {
        let data = try Data(contentsOf: URL(fileURLWithPath: source))
        let inspection = try ObserverClient.decoder().decode(RuntimeInspectionPayload.self, from: data)
        precondition(inspection.blueprint.nodes.count == 21)
        precondition(inspection.control.status == "paused")
        let view = VStack(alignment: .leading, spacing: 16) {
            Text("Runtime Inspector · Core 离线实例").font(.title2)
            RuntimeInspectionSummary(inspection: inspection)
            Divider()
            ForEach(inspection.blueprint.nodes) { node in
                HStack(alignment: .top) {
                    Text(node.spec.key).frame(width: 190, alignment: .leading)
                    Text(node.dependencies.isEmpty ? "入口" : node.dependencies.joined(separator: "、"))
                        .foregroundStyle(.secondary)
                }.font(.system(size: 11, design: .monospaced))
            }
        }.padding(24).background(Color.black.opacity(0.9))
        try render(view, size: CGSize(width: 940, height: 1040), name: "runtime-inspector.png", directory: directory)
        print("PASS: actual Core inspection payload decoded and rendered by SwiftUI")
    }

    func researchAuditFixture() -> [String: Any] {
        let issue: [String: Any] = ["category": "unit_error", "field_path": "forecast.TQQQ.t1.expected_return_ppm",
            "correction_criterion": "将 2% 正确换算为 20000 ppm，或修正估计说明。", "evidence_refs": [["artifact_id": "evidence-units-123"]]]
        let assessment: [String: Any] = ["scope": "forecast.TQQQ.t1", "accepted": false, "rationale": "单位换算不一致", "issues": [issue]]
        let review: [String: Any] = ["artifact_id": "review-proof", "producer": "agent.research.proposal_reviewer",
            "task_id": "research.critic-t3", "proposal_revision": 1, "payload": ["assessments": [assessment]]]
        let stop: [String: Any] = ["artifact_id": "stop-proof", "producer": "research.revision.stop",
            "task_id": "research.critic-t3", "payload": ["reason": "unchanged_rejected_scopes"]]
        let lesson: [String: Any] = ["artifact_id": "lesson-proof", "producer": "learning.revalidation.suggestion",
            "task_id": "research.critic-t3", "payload": ["reason": "source_updated_requires_revalidation"]]
        return ["version": 1, "records": [review, stop, lesson]]
    }

    func testResearchAuditUsesRustRecordsAndPreservesLegacyAbsence() throws {
        let historic = LiveProjection(payload: try displaySnapshot())
        expectTrue(historic.workflow.stageInspectors.values.allSatisfy { $0.researchAudit.isEmpty })
        let projection = LiveProjection(payload: try displaySnapshot(researchAudit: researchAuditFixture()))
        let rows = projection.workflow.stageInspectors["research.critic-t3"]!.researchAudit
        expectEqual(rows.count, 3)
        expectEqual(rows[0].references, ["evidence-units-123"])
        expectTrue(rows[1].detail.contains("Decision 仍被阻断"))
        expectEqual(projection.learning.researchAudit.count, 1)
        expectEqual(projection.learning.availabilityStatus, historic.learning.availabilityStatus)
        print("PASS: typed research audit preserves references, stop reason and historical absence without claiming canonical learning")
    }

    @MainActor
    private func render<V: View>(_ view: V, size: CGSize, name: String, directory: String) throws {
        let content = view.frame(width: size.width, height: size.height)
            .environment(\.appLanguage, .simplifiedChinese)
            .environment(\.akzioRendersOffscreen, true)
            .environment(\.colorScheme, .dark)
        let renderer = ImageRenderer(content: content)
        renderer.scale = 1
        guard let image = renderer.cgImage,
              let data = NSBitmapImageRep(cgImage: image).representation(using: .png, properties: [:]) else {
            throw NSError(domain: "DisplayFixture", code: 1)
        }
        let folder = URL(fileURLWithPath: directory)
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        try data.write(to: folder.appendingPathComponent(name))
    }
}
