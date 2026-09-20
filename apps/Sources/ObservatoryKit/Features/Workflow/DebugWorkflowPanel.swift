import SwiftUI

struct DebugEnvironmentBanner: View {
    let store: ObservatoryStore
    private var identity: JSONValue? { store.debugRun?.session.identity }
    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Label("隔离检查", systemImage: "ladybug")
                Text(identity?["llm_mode"]?.string.map { mode in
                    switch mode {
                    case "real": "真实模型"
                    case "fixture": "离线场景"
                    default: "模型模式未确认"
                    }
                } ?? "模型模式未确认")
                Text(identity?["broker_write_policy"]?.string.map {
                    switch $0 {
                    case "paper_allowed": "允许 Paper 写入 · 仍需通过门控"
                    case "simulated_only": "模拟模式 · 禁止外部发单"
                    case "forbidden": "禁止外部发单"
                    default: "发单权限未确认"
                    }
                } ?? "发单权限未确认")
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
            RuntimeInspectorPanel(store: store)
            HStack {
                Text("调度控制").font(.title2.weight(.semibold))
                Spacer()
                Text(WorkflowDisplay.status(store.debugRun?.session.status ?? "unknown")).font(.headline)
                Button("刷新") {
                    if let id = store.selectedDebugRunID { Task { await store.selectDebugRun(id) } }
                }.disabled(!store.debugConnected || store.debugBusy)
                ForEach(["pause", "resume"], id: \.self) { action in
                    Button(action == "pause" ? "暂停" : "继续") { Task { await store.controlDebug(action) } }
                        .disabled(!store.debugConnected || store.debugBusy || store.debugRun?.allowed_actions.contains(action) != true)
                }
            }
            if let display = store.debugRun?.research?["display"]?.string {
                Text(display).font(.callout).textSelection(.enabled)
            }
            if !store.debugConnected {
                Label("\(store.observerState.label) · Controls disabled · Last update: \(store.debugRun?.observed_at.formatted() ?? "never")", systemImage: "wifi.slash")
                    .foregroundStyle(.orange)
            }
            if !store.debugMessage.isEmpty { Text(store.debugMessage).font(.callout).textSelection(.enabled) }
            DisclosureGroup("准备暂停的运行 · 禁止外部发单") {
                HStack {
                    TextField("Session YYYY-MM-DD", text: $sessionDate).frame(width: 160)
                    Picker("Purpose", selection: $purpose) {
                        Text("Paper").tag("paper")
                        Text("Position Plan · No Execution").tag("position_plan")
                    }.frame(width: 260)
                    Button("Prepare") { Task { await store.prepareDebug(session: sessionDate, purpose: purpose) } }
                        .disabled(!store.debugConnected || store.debugBusy)
                }.padding(.vertical, 6)
                Text("Paper and Position Plan share the formal T1/T3/T5 research workflow.").font(.caption).foregroundStyle(.secondary)
            }
            if let runs = store.debugListing?.runs, !runs.isEmpty {
                DisclosureGroup("运行时间线 · T0 研究与历史 T1 / T3 / T5") {
                    ForEach(runs) { entry in
                        HStack(alignment: .top) {
                            Button(entry.id) { Task { await store.selectDebugRun(entry.id) } }
                                .font(.system(size: 10, design: .monospaced))
                            Text(WorkflowDisplay.status(entry.session.status)).font(.caption)
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
                Text(run.session.identity["run_purpose"]?.string == "position_plan" ? "POSITION PLAN · NO EXECUTION · Execution N/A" : "PAPER · 研究 → 执行")
                    .font(.headline)
                Text(progressSummary(run))
                    .font(.body).foregroundStyle(AkzioColor.secondaryText)
                if let researchSummary = researchPlanSummary(run) {
                    Text(researchSummary)
                        .font(.callout.weight(.semibold))
                        .foregroundStyle(Color.orange)
                        .textSelection(.enabled)
                }
                Text(run.session.runID).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
                HStack(alignment: .top, spacing: 16) {
                    VStack(alignment: .leading, spacing: 8) {
                        ForEach(0..<3) { phase in
                            let nodes = WorkflowDisplay.ordered(run.nodes).filter { WorkflowDisplay.phase($0.role) == phase }
                            if !nodes.isEmpty {
                                Text(["研究与决策 · T0", "执行结果", "后续评估 · T1 / T3 / T5"][phase])
                                    .font(.subheadline.weight(.semibold)).padding(.top, 8)
                                ForEach(nodes) { node in nodeRow(node) }
                            }
                        }
                    }.frame(minWidth: 300, maxWidth: .infinity, alignment: .topLeading)
                    CollapsibleInspector(title: "Stage Inspector", width: 360) {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("阶段检查器").font(.headline)
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
                    }.frame(maxWidth: .infinity, alignment: .topLeading)
                    }
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

    private func progressSummary(_ run: DebugRunPayload) -> String {
        let t0 = run.nodes.filter { WorkflowDisplay.phase($0.role) != 2 }
        let completed = t0.filter { $0.status == "succeeded" }.count
        let artifacts: [(kind: String, payload: JSONValue)] = run.artifacts.compactMap { value in
            guard let kind = value["artifact"]?["kind"]?.string, let payload = value["payload"] else { return nil }
            return (kind: kind, payload: payload)
        }
        let evidence = OutcomeEvidencePresentation.from(artifacts)
        return "T0 研究与执行 \(completed)/\(t0.count) 完成 · 执行：\(WorkflowDisplay.executionLabel(artifacts, evidence: run.execution_evidence)) · Outcome：\(evidence.caption) · 控制\(WorkflowDisplay.status(run.session.status))"
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
                Text(WorkflowDisplay.status(node.status)).font(.callout)
            }
            Text(node.id).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
            Text("尝试 \(node.attempts.count) 次 · \(node.status == "succeeded" ? "已完成" : WorkflowDisplay.status(node.blocked_reason ?? (node.business_ready ? "前置条件已满足" : "就绪状态未知")))")
                .font(.callout).foregroundStyle(node.status == "succeeded" ? AkzioColor.secondaryText : (node.blocked_reason == nil ? AkzioColor.secondaryText : AkzioColor.actionCoral))
            if let reason = node.blocked_reason {
                DisclosureGroup("原始状态说明") {
                    Text(reason).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
                }.font(.caption).foregroundStyle(AkzioColor.secondaryText)
            }
            HStack {
                Button("检查") { store.selectedDebugTaskID = node.id }
                    .accessibilityIdentifier("debug-inspect-\(node.id)")
                    .accessibilityLabel("检查 \(node.role) \(node.horizon ?? "") · \(node.id)")
                Button("单步执行") { Task { await store.controlDebug("step", task: node.id) } }
                    .accessibilityIdentifier("debug-step-\(node.id)")
                    .accessibilityLabel("单步执行 \(node.role) \(node.horizon ?? "") · \(node.id)")
                    .disabled(!store.debugConnected || store.debugBusy || !node.step_eligible)
                Button("重试失败阶段") { Task { await store.controlDebug("retry_node", task: node.id) } }
                    .accessibilityIdentifier("debug-retry-\(node.id)")
                    .accessibilityLabel("重试 \(node.role) \(node.horizon ?? "") · \(node.id)")
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
            Text("\(node.role) \(node.horizon?.uppercased() ?? "")").font(.headline)
            Text(node.id).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
            detail("任务与依赖", node.task)
            detail("输入产物", node.task["node"]?["input_artifacts"])
            detail("尝试与恢复记录", .array(node.attempts))
            detail("本次尝试的累计预算", node.budget)
            ForEach(["context_manifest", "agent_turn", "tool_call", "tool_result", "semantic_detail", "debug_record"], id: \.self) { kind in
                let values = artifacts.filter { $0["artifact"]?["kind"]?.string == kind }
                detail(title(kind), .array(values))
            }
            detail("输出产物引用", .array(node.output_refs))
            detail("输出内容", .array(artifacts.filter { value in node.output_refs.contains { $0["artifact_id"]?.string == value["artifact"]?["artifact_id"]?.string } }))
            DebugAcceptancePanel(values: run.acceptance.filter { $0["task_id"]?.string == node.id })
        }
        .padding(AkzioLayout.s4)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlass(.elevated, radius: AkzioLayout.cardRadius)
    }
    private func title(_ kind: String) -> String {
        switch kind {
        case "context_manifest": "上下文清单与授权输入"
        case "agent_turn": "模型调用与提交记录"
        case "tool_call": "工具调用"
        case "tool_result": "工具结果"
        case "semantic_detail": "语义校验反馈"
        default: "持久化诊断记录"
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
            VStack(alignment: .leading, spacing: 5) {
                Text("验收记录 · 独立于业务状态")
                HStack(spacing: 6) {
                ForEach(["PASS", "FAIL", "BLOCKED", "NOT_RUN"], id: \.self) { result in
                    Text("\(result) \(checks.filter { $0["result"]?.string == result }.count)")
                        .font(.caption).foregroundStyle(result == "FAIL" ? Color.red : Color.secondary)
                }
                }
            }
        }
    }
}
