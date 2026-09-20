import SwiftUI

/// Read-only projections. Buttons fetch Core data; none creates or resumes a run.
struct RuntimeInspectorPanel: View {
    let store: ObservatoryStore
    @State private var blueprint: WorkflowBlueprintPayload?
    @State private var events: [RuntimeJournalPayload.Event] = []
    @State private var cursor: Int64 = 0
    @State private var hasMore = true
    @State private var busy = false
    @State private var error: String?

    @State private var selectedTab = "summary"

    var body: some View {
        DisclosureGroup("运行检查器") {
            VStack(alignment: .leading, spacing: 12) {
                Picker("检查内容", selection: $selectedTab) {
                    Text("运行概况").tag("summary")
                    Text("流程定义").tag("definition")
                    Text("事件记录").tag("events")
                }
                .pickerStyle(.segmented)
                if busy { ProgressView().controlSize(.small) }
                if !store.debugConnected {
                    Label("尚未连接 Core · 连接后可读取流程定义和运行记录", systemImage: "wifi.slash")
                        .font(.callout).foregroundStyle(.secondary)
                }
                switch selectedTab {
                case "definition": definition
                case "events": journal
                default:
                    if let inspection = store.runtimeInspection {
                        RuntimeInspectionSummary(inspection: inspection)
                    } else {
                        Label("尚未选择可检查的运行", systemImage: "list.bullet.rectangle")
                            .foregroundStyle(.secondary)
                    }
                }
                if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.top, 8)
        }
        .onChange(of: store.runtimeInspection?.runID) { _, _ in
            events = []; cursor = 0; hasMore = true; blueprint = nil; error = nil
        }
    }

    private var definition: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                ForEach(["position_plan", "paper"], id: \.self) { purpose in
                    Button(purpose == "paper" ? "预览 Paper 流程" : "预览 PositionPlan 流程") {
                        Task { await preview(purpose) }
                    }.disabled(busy || !store.debugConnected)
                }
            }
            if let graph = blueprint ?? store.runtimeInspection?.blueprint {
                Text(blueprint == nil ? "本次运行冻结的定义" : "当前流程预览 · 不代表本次运行定义")
                    .font(.caption).foregroundStyle(.secondary)
                Text("\(graph.purpose) · 定义版本 \(graph.definition_version.map(String.init) ?? "历史") · \(graph.nodes.count) 个节点")
                DisclosureGroup("定义标识与节点依赖") {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 10) {
                            Text(graph.definition_hash).font(.system(.caption, design: .monospaced))
                            ForEach(graph.nodes) { node in
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(node.spec.key).fontWeight(.medium)
                                    Text(node.dependencies.isEmpty ? "入口" : "依赖：" + node.dependencies.joined(separator: "、"))
                                        .foregroundStyle(.secondary)
                                }.font(.caption)
                            }
                        }.frame(maxWidth: .infinity, alignment: .leading).textSelection(.enabled)
                    }.frame(maxHeight: 240)
                }
                if blueprint != nil {
                    Button("返回本次运行定义") { blueprint = nil }
                }
            } else {
                Text("选择一个流程以查看定义；预览不会启动运行。")
                    .font(.callout).foregroundStyle(.secondary)
            }
        }
    }

    private var journal: some View {
        VStack(alignment: .leading, spacing: 10) {
            if events.isEmpty {
                Text(cursor == 0 && hasMore ? "尚未读取事件记录" : "当前没有可显示的事件")
                    .foregroundStyle(.secondary)
            } else {
                Text("已读取 \(events.count) 条事件 · 最新游标 #\(cursor)")
                    .font(.caption).foregroundStyle(.secondary)
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 10) {
                        ForEach(events) { event in
                            VStack(alignment: .leading, spacing: 6) {
                                HStack(alignment: .firstTextBaseline) {
                                    Text("#\(event.cursor)").foregroundStyle(.secondary)
                                    Text(event.event_type).fontWeight(.medium)
                                    Spacer()
                                    Text(event.created_at.formatted(date: .omitted, time: .standard))
                                        .foregroundStyle(.secondary)
                                }
                                DisclosureGroup("任务与来源引用") {
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text(event.created_at.formatted())
                                        if let task = event.task_id { Text("Task: \(task)") }
                                        if let attempt = event.attempt_id { Text("Attempt: \(attempt)") }
                                        if let artifact = event.artifact?["artifact_id"]?.string { Text("Artifact: \(artifact)") }
                                        ForEach(Array(event.source_refs.enumerated()), id: \.offset) { _, source in
                                            Text("来源：\(source["artifact_id"]?.string ?? "未知")")
                                        }
                                    }.textSelection(.enabled)
                                }
                                Divider()
                            }.font(.system(.caption, design: .monospaced))
                        }
                    }
                }.frame(maxHeight: 240)
            }
            Button(events.isEmpty ? "读取事件" : (hasMore ? "加载下一页" : "检查新事件")) {
                Task { await loadEvents() }
            }.disabled(busy || !store.debugConnected || store.runtimeInspection?.runID == nil)
        }
    }

    private func preview(_ purpose: String) async {
        busy = true; defer { busy = false }
        let runID = store.runtimeInspection?.runID
        do {
            let result = try await store.previewWorkflow(purpose)
            guard store.runtimeInspection?.runID == runID else { return }
            blueprint = result; error = nil
        } catch {
            guard store.runtimeInspection?.runID == runID else { return }
            self.error = error.localizedDescription
        }
    }

    private func loadEvents() async {
        guard let runID = store.runtimeInspection?.runID else { return }
        busy = true; defer { busy = false }
        do {
            let page = try await store.runtimeJournal(runID: runID, after: cursor)
            guard store.runtimeInspection?.runID == runID else { return }
            events.append(contentsOf: page.events); cursor = page.next_cursor; hasMore = page.has_more; error = nil
        } catch {
            guard store.runtimeInspection?.runID == runID else { return }
            self.error = error.localizedDescription
        }
    }

}

struct RuntimeInspectionSummary: View {
    let inspection: RuntimeInspectionPayload
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
                    Text("控制：\(WorkflowDisplay.status(inspection.control.status)) · 修订 \(inspection.control.revision)")
                    Text(recoveryLabel(inspection.recovery))
                    if let checkpoint = inspection.checkpoint {
                        Text("恢复记录：事件 #\(checkpoint.event_cursor) · \(checkpoint.created_at.formatted())")
                        Text(checkpoint.boundary).font(.system(.caption, design: .monospaced))
                    } else {
                        Text("历史运行尚无恢复记录")
                    }
            Text("流程定义版本：\(inspection.blueprint.definition_version.map(String.init) ?? "历史") · \(inspection.blueprint.nodes.count) 个节点")
            if let runID = inspection.runID {
                Text("Run: \(runID)").font(.system(.caption, design: .monospaced)).textSelection(.enabled)
            }
        }
    }
    private func recoveryLabel(_ value: String) -> String {
        switch value {
        case "lease_governed": "恢复方式：由任务租约与既有恢复协议处理"
        case "manual_control": "恢复方式：等待控制操作"
        case "dependency_or_due_time": "恢复方式：等待前置任务或到期时间"
        case "terminal": "运行已到达终态"
        default: "恢复状态未确认"
        }
    }
}
