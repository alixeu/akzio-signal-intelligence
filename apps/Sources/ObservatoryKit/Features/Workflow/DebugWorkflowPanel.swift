import SwiftUI

struct DebugEnvironmentBanner: View {
    let store: ObservatoryStore
    private var identity: JSONValue? { store.debugRun?.session.identity }
    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Label("ISOLATED DEBUG", systemImage: "ladybug")
                Text(identity?["llm_mode"]?.string.map { $0 == "real" ? "REAL LLM" : "FIXTURE CONTROLLER" } ?? "LLM MODE UNVERIFIED")
                Text(identity?["broker_write_policy"]?.string.map { $0 == "paper_allowed" ? "PAPER WRITE ALLOWED · gates required" : "BROKER WRITE DISABLED" } ?? "BROKER POLICY UNVERIFIED")
                    .foregroundStyle(identity?["broker_write_policy"]?.string == "paper_allowed" ? Color.red : Color.orange)
                Spacer()
                Text(store.observerState.label)
            }.font(.system(size: 11, weight: .semibold))
            Text("Core: \(store.debugEndpoint) · Store: \(store.debugListing?.store_identity ?? "unverified")")
            Text("Purpose: \(identity?["run_purpose"]?.string ?? "—") · Learning: \(identity?["learning_scope"]?.string ?? "unverified") · Revision: \(identity?["code_revision"]?.string ?? "—")")
                .lineLimit(1).truncationMode(.middle)
        }
        .font(.system(size: 10, design: .monospaced))
        .textSelection(.enabled)
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 0)
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(AkzioColor.actionCoral.opacity(0.24))
                .frame(height: AkzioLayout.hairlineWidth)
        }
    }
}

struct DebugWorkflowPanel: View {
    let store: ObservatoryStore
    @State private var sessionDate = String(ISO8601DateFormatter().string(from: Date()).prefix(10))
    @State private var fixture = false
    @State private var purpose = "paper"
    @State private var experimentReason = ""
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.akzioReduceTransparencyOverride) private var reduceTransparencyOverride
    @Environment(\.akzioHighContrast) private var highContrast
    @Environment(\.akzioRendersOffscreen) private var rendersOffscreen
    private var selected: DebugNodePayload? { store.debugRun?.nodes.first { $0.id == store.selectedDebugTaskID } }

    private var usesOpaqueBackdrop: Bool {
        reduceTransparency || reduceTransparencyOverride || highContrast || rendersOffscreen
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Debug Control").font(.title2.weight(.semibold))
                Spacer()
                Text(store.debugRun?.session.status ?? "No Run").font(.headline)
                Button("Refresh") {
                    if let id = store.selectedDebugRunID { Task { await store.selectDebugRun(id) } }
                }.disabled(!store.debugConnected || store.debugBusy)
                ForEach(["pause", "resume"], id: \.self) { action in
                    Button(action.capitalized) { Task { await store.controlDebug(action) } }
                        .disabled(!store.debugConnected || store.debugBusy || store.debugRun?.allowed_actions.contains(action) != true)
                }
            }
            if !store.debugConnected {
                Label("\(store.observerState.label) · Controls disabled · Last update: \(store.debugRun?.observed_at.formatted() ?? "never")", systemImage: "wifi.slash")
                    .foregroundStyle(.orange)
            }
            if !store.debugMessage.isEmpty { Text(store.debugMessage).font(.callout).textSelection(.enabled) }
            DisclosureGroup("Prepare a paused Run · broker writes disabled") {
                HStack {
                    TextField("Session YYYY-MM-DD", text: $sessionDate).frame(width: 160)
                    Picker("Purpose", selection: $purpose) {
                        Text("Paper").tag("paper")
                        Text("Position Plan · No Execution").tag("position_plan")
                    }.frame(width: 260)
                    Toggle("Deterministic controller fixture", isOn: $fixture).toggleStyle(.checkbox)
                    Button("Prepare") { Task { await store.prepareDebug(session: sessionDate, fixture: fixture, purpose: purpose) } }
                        .disabled(!store.debugConnected || store.debugBusy)
                }.padding(.vertical, 6)
                Text("Default: formal Paper DAG with T1/T3/T5 pairs. The fixture option tests only controller mechanics.").font(.caption).foregroundStyle(.secondary)
            }
            if let runs = store.debugListing?.runs, !runs.isEmpty {
                DisclosureGroup("Run timelines · T0 research and historical T1 / T3 / T5") {
                    ForEach(runs) { entry in
                        HStack(alignment: .top) {
                            Button(entry.id) { Task { await store.selectDebugRun(entry.id) } }
                                .font(.system(size: 10, design: .monospaced))
                            Text(entry.session.status).font(.caption)
                            Text("T0: \(entry.lifecycle["execution_status"]?.string ?? "unknown")").font(.caption)
                            ForEach(["t1", "t3", "t5"], id: \.self) { horizon in
                                Text("\(horizon.uppercased()): \(entry.lifecycle["retrospective_status"]?[horizon]?.string ?? "not_scheduled")").font(.caption)
                            }
                            Text("Learning: \(entry.lifecycle["learning_recorded"]?.bool == true ? "recorded" : "not recorded")").font(.caption)
                        }.padding(.vertical, 4)
                    }
                }
            }
            if let run = store.debugRun {
                Text(run.session.identity["run_purpose"]?.string == "position_plan" ? "POSITION PLAN · NO EXECUTION · Execution N/A" : "PAPER · Research → Execution")
                    .font(.headline)
                Text("Execution Evidence: \(run.execution_evidence?.replacingOccurrences(of: "_", with: " ").uppercased() ?? "UNKNOWN")")
                    .font(.callout).foregroundStyle(.secondary)
                if let researchSummary = researchPlanSummary(run) {
                    Text(researchSummary)
                        .font(.callout.weight(.semibold))
                        .foregroundStyle(Color.orange)
                        .textSelection(.enabled)
                }
                Text(run.session.runID).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
                HStack(alignment: .top, spacing: 16) {
                    VStack(alignment: .leading, spacing: 8) {
                        ForEach(run.nodes) { node in
                            if node.role == "gate.execution" {
                                Divider()
                                Text("EXECUTION · Fresh snapshots required").font(.caption.weight(.semibold))
                            }
                            nodeRow(node)
                        }
                    }.frame(minWidth: 300, maxWidth: .infinity, alignment: .topLeading)
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Stage Inspector").font(.headline)
                        if let selected {
                            DebugStageInspector(run: run, node: selected)
                            DisclosureGroup("New experiment / successful stage fork") {
                                TextField("Reason", text: $experimentReason)
                                Button("Fork with immutable parent lineage") {
                                    Task { await store.forkDebug(task: selected.id, reason: experimentReason) }
                                }.disabled(!store.debugConnected || store.debugBusy || experimentReason.isEmpty)
                                Button("New experiment from Run") {
                                    Task { await store.forkDebug(task: nil, reason: experimentReason) }
                                }.disabled(!store.debugConnected || store.debugBusy || experimentReason.isEmpty)
                                Text("Core verifies successful parent output. A new research graph recollects evidence; old artifacts remain unchanged.").font(.caption)
                            }
                        } else { Text("Select a Task to inspect persisted evidence.").foregroundStyle(.secondary) }
                    }.frame(minWidth: 360, maxWidth: .infinity, alignment: .topLeading)
                }
                DebugAcceptancePanel(values: run.acceptance)
                DisclosureGroup("Persisted event timeline (\(run.events.count))") {
                    Text(JSONValue.array(run.events).prettyPrinted).font(.system(size: 10, design: .monospaced)).textSelection(.enabled)
                }
            }
        }
        .foregroundStyle(AkzioColor.primaryText)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(AkzioLayout.s4)
        .background {
            let shape = RoundedRectangle(cornerRadius: AkzioLayout.sheetRadius, style: .continuous)
            if usesOpaqueBackdrop {
                shape.fill(AkzioColor.raisedSurface)
            } else {
                shape
                    .fill(.ultraThinMaterial)
                    .opacity(0.18)
            }
        }
        .overlay {
            RoundedRectangle(cornerRadius: AkzioLayout.sheetRadius, style: .continuous)
                .strokeBorder(AkzioColor.glassTopEdge, lineWidth: AkzioLayout.hairlineWidth)
        }
    }

    private func researchPlanSummary(_ run: DebugRunPayload) -> String? {
        guard run.session.identity["run_purpose"]?.string == "position_plan",
              let context = run.artifacts.last(where: { $0["artifact"]?["kind"]?.string == "decision_context" }),
              let plan = context["payload"]?["research_plan"]
        else { return nil }
        let status = plan["status"]?.string?.replacingOccurrences(of: "_", with: " ").uppercased() ?? "UNKNOWN"
        let execution = plan["execution_status"]?.string?.replacingOccurrences(of: "_", with: " ").uppercased() ?? "UNKNOWN"
        return "RESEARCH TARGET · \(status) · EXECUTION \(execution) · not a broker position"
    }

    private func nodeRow(_ node: DebugNodePayload) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack {
                Text(node.role).font(.subheadline.weight(.semibold))
                Text(node.horizon?.uppercased() ?? "").foregroundStyle(.secondary)
                Spacer()
                Text(node.status).font(.caption)
            }
            Text(node.id).font(.system(size: 9, design: .monospaced)).textSelection(.enabled)
            Text("Attempts: \(node.attempts.count) · \(node.blocked_reason ?? "Ready · Core verified")")
                .font(.caption).foregroundStyle(node.blocked_reason == nil ? Color.secondary : Color.orange)
            HStack {
                Button("Inspect") { store.selectedDebugTaskID = node.id }
                    .accessibilityIdentifier("debug-inspect-\(node.id)")
                Button("Step") { Task { await store.controlDebug("step", task: node.id) } }
                    .disabled(!store.debugConnected || store.debugBusy || !node.step_eligible)
                Button("Retry Failed Stage") { Task { await store.controlDebug("retry_node", task: node.id) } }
                    .disabled(!store.debugConnected || store.debugBusy || !node.retry_eligible)
            }.controlSize(.small)
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlass(.base, radius: AkzioLayout.chipRadius)
        .overlay {
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(
                    store.selectedDebugTaskID == node.id
                        ? AkzioColor.primaryGold.opacity(0.52)
                        : AkzioColor.hairline,
                    lineWidth: store.selectedDebugTaskID == node.id ? 1.5 : AkzioLayout.hairlineWidth
                )
        }
    }
}

// Extends the existing Workflow inspector surface with the same Store-backed
// artifacts used by CLI inspect; no synthetic success is projected here.
struct DebugStageInspector: View {
    let run: DebugRunPayload
    let node: DebugNodePayload
    private var artifacts: [JSONValue] {
        run.artifacts.filter { $0["artifact"]?["origin"]?["task_id"]?.string == node.id }
    }
    var body: some View {
        VStack(alignment: .leading, spacing: 9) {
            detail("Task / Dependencies", node.task)
            detail("Input Artifacts", node.task["node"]?["input_artifacts"])
            detail("Attempts / Recovery", .array(node.attempts))
            detail("Attempt cumulative budget · not provider context window", node.budget)
            ForEach(["context_manifest", "agent_turn", "tool_call", "tool_result", "semantic_detail", "debug_record"], id: \.self) { kind in
                let values = artifacts.filter { $0["artifact"]?["kind"]?.string == kind }
                detail(title(kind), .array(values))
            }
            detail("Output Artifact refs", .array(node.output_refs))
            detail("Output payloads", .array(artifacts.filter { value in node.output_refs.contains { $0["artifact_id"]?.string == value["artifact"]?["artifact_id"]?.string } }))
            DebugAcceptancePanel(values: run.acceptance.filter { $0["task_id"]?.string == node.id })
        }
        .padding(AkzioLayout.s4)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlass(.elevated, radius: AkzioLayout.cardRadius)
    }
    private func title(_ kind: String) -> String {
        switch kind {
        case "context_manifest": "Context Manifest / authorized inputs"
        case "agent_turn": "Model Call / Read Grant / Draft / Submit / usage"
        case "tool_call": "Tool Calls"
        case "tool_result": "Tool Results"
        default: "Validation / persisted diagnostics"
        }
    }
    private func detail(_ title: String, _ value: JSONValue?) -> some View {
        DisclosureGroup(title) {
            Text(value?.prettyPrinted ?? "Not recorded / not applicable")
                .font(.system(size: 10, design: .monospaced)).textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

struct DebugAcceptancePanel: View {
    let values: [JSONValue]
    private var checks: [JSONValue] { values.flatMap { $0["checks"]?.array ?? [] } }
    var body: some View {
        DisclosureGroup {
            ForEach(Array(values.enumerated()), id: \.offset) { _, acceptance in
                VStack(alignment: .leading, spacing: 5) {
                    Text("\(acceptance["stage"]?.string ?? "Stage") · Business: \(acceptance["business_result"]?.string ?? "unknown") · Test: \(acceptance["test_result"]?.string ?? "NOT_RUN")")
                        .font(.caption.weight(.semibold))
                    ForEach(Array((acceptance["checks"]?.array ?? []).enumerated()), id: \.offset) { _, check in
                        DisclosureGroup("\(check["result"]?.string ?? "NOT_RUN") · \(check["check_id"]?.string ?? "check")") {
                            Text(check.prettyPrinted).font(.system(size: 10, design: .monospaced)).textSelection(.enabled)
                        }
                    }
                }.padding(.vertical, 4)
            }
        } label: {
            HStack {
                Text("Acceptance · business status is separate")
                ForEach(["PASS", "FAIL", "BLOCKED", "NOT_RUN"], id: \.self) { result in
                    Text("Controller \(result) \(checks.filter { $0["result"]?.string == result }.count)")
                        .font(.caption).foregroundStyle(result == "FAIL" ? Color.red : Color.secondary)
                }
            }
        }
    }
}
