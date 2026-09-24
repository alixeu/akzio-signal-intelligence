import Foundation

/// Presentation only. These helpers never change Core eligibility or task dependencies.
enum WorkflowDisplay {
    static func horizon(objective: String) -> String? {
        // 先读取显式 research_horizon 标记，再按 Claim 文案兜底；这是 UI 解析而非协议修复。
        if let marker = objective.range(of: "[research_horizon="),
           let end = objective[marker.upperBound...].firstIndex(of: "]") {
            return String(objective[marker.upperBound..<end])
        }
        return ["t1", "t3", "t5"].first { objective.contains(" \($0) Claim") }
    }

    static func phase(_ role: String) -> Int {
        // phase 只用于页面排序分层，把角色字符串映射为稳定的视觉层级。
        if role.hasPrefix("learning.") || role.contains("outcome") { return 2 }
        if ["gate.execution", "gate.paper", "gate.reconcile", "gate.evaluate"].contains(role) || role.hasPrefix("execution.") || role.hasPrefix("broker.") { return 1 }
        return 0
    }

    static func ordered(_ nodes: [DebugNodePayload]) -> [DebugNodePayload] {
        // ordered 对 Observer 节点做拓扑可读排序；Set/闭包只追踪已放置 ID，不修复输入图。
        let ids = Set(nodes.map(\.id))
        var remaining = nodes
        var result: [DebugNodePayload] = []
        var placed = Set<String>()
        while !remaining.isEmpty {
            let ready = remaining.filter { node in
                (node.task["node"]?["dependencies"]?.array ?? []).compactMap(\.string)
                    .allSatisfy { !ids.contains($0) || placed.contains($0) }
            }
            // Malformed/cyclic input remains visible; the UI does not repair the graph.
            let layer = (ready.isEmpty ? remaining : ready).sorted {
                // 同一层按 phase、horizon、role、id 排序，确保读取结果可复现。
                let lhs = (phase($0.role), $0.horizon ?? "", $0.role, $0.id)
                let rhs = (phase($1.role), $1.horizon ?? "", $1.role, $1.id)
                return lhs < rhs
            }
            result.append(contentsOf: layer)
            placed.formUnion(layer.map(\.id))
            remaining.removeAll { placed.contains($0.id) }
        }
        return result
    }

    static func executionLabel(_ artifacts: [(kind: String, payload: JSONValue)], evidence: String?) -> String {
        // 只有明确的 execution_verdict=no_order 直接显示未下单，其余状态仍等待证据映射。
        if let verdict = artifacts.last(where: { $0.kind == "execution_verdict" })?.payload["verdict"]?.string,
           verdict == "no_order" { return "未下单" }
        // A passed gate or plan alone is not evidence of a fill.
        return status(evidence ?? "unknown")
    }

    static func orderedTasks(_ tasks: [ObserverTaskPayload]) -> [ObserverTaskPayload] {
        // ObserverTaskPayload 使用 taskID/dependencies 排序，和 DebugNodePayload 保持同一读取边界。
        let ids = Set(tasks.map(\.node.taskID))
        var pending = tasks
        var result: [ObserverTaskPayload] = []
        var placed = Set<String>()
        while !pending.isEmpty {
            let ready = pending.filter { $0.node.dependencies.allSatisfy { !ids.contains($0) || placed.contains($0) } }
            let layer = (ready.isEmpty ? pending : ready).sorted {
                (phase($0.node.recipeID), $0.node.horizon ?? "", $0.node.recipeID, $0.node.taskID)
                < (phase($1.node.recipeID), $1.node.horizon ?? "", $1.node.recipeID, $1.node.taskID)
            }
            result.append(contentsOf: layer)
            placed.formUnion(layer.map(\.node.taskID))
            pending.removeAll { placed.contains($0.node.taskID) }
        }
        return result
    }

    static func initialSelection(_ nodes: [DebugNodePayload]) -> String? {
        // 初始选中优先 running/leased，再选失败或带 blocker 的节点，最后才取首节点。
        let ordered = ordered(nodes)
        return ordered.first { ["running", "leased"].contains($0.status) }?.id
            ?? ordered.first { $0.status == "failed" || ($0.status != "succeeded" && $0.blocked_reason != nil) }?.id
            ?? ordered.first?.id
    }

    static func status(_ value: String) -> String {
        // status 是 Rust/Observer 状态到页面中文文案的单向词典，未知值原样保留。
        switch value {
        case "pause_requested": "正在暂停，等待当前任务结束"
        case "paused": "已暂停"
        case "running": "运行中"
        case "leased": "已领取"
        case "pending", "queued": "等待执行"
        case "succeeded", "completed": "已完成"
        case "failed": "失败"
        case "cancelled": "已取消"
        case "blocked": "已阻断"
        case "dependencies_not_satisfied", "dependencies_not_ready": "等待前置任务完成"
        case "outcome_processing_disabled_or_adapter_unavailable": "等待评估：评估处理未启用或适配器不可用"
        case "debug_paused": "控制已暂停"
        case "debug_running": "控制运行中"
        case "task_already_succeeded", "task_succeeded": "任务已完成"
        case "not_scheduled": "尚未安排"
        case "unknown": "未知"
        case "no_order": "未下单"
        case "pass": "执行门控通过 · 成交尚未确认"
        case "refreshing": "正在更新执行证据"
        case "deferred": "等待执行检查"
        case "not_applicable": "不适用"
        case "simulated_fills": "模拟成交"
        default: value
        }
    }
}

/// Only artifacts belonging to the selected run can establish its sealed horizons.
/// Workflow completion and NoOrder are deliberately not inputs to this evidence.
public struct OutcomeEvidencePresentation: Sendable, Hashable {
    // 该模型只接受所选 run 的 outcome artifact，避免把 workflow 完成误当作封存证据。
    public let sealedHorizons: [String]
    public let scheduled: Bool

    static let unknown = OutcomeEvidencePresentation(sealedHorizons: [], scheduled: false)

    static func from(_ artifacts: [(kind: String, payload: JSONValue)]) -> Self {
        // filter/flatMap/compactMap/filter 闭包只抽取合法 horizon；Set 去重后排序保证稳定读取。
        let horizons = artifacts.filter { $0.kind == "outcome" }
            .flatMap { $0.payload["windows"]?.array ?? [] }
            .compactMap { $0["horizon"]?.string }
            .filter { ["t1", "t3", "t5"].contains($0) }
        return Self(sealedHorizons: Array(Set(horizons)).sorted(),
                    scheduled: artifacts.contains { $0.kind == "outcome_schedule" })
    }

    public var caption: String {
        // caption 只描述已抽取的封存/排程边界，不由页面推测 Outcome 是否完成。
        if !sealedHorizons.isEmpty {
            return "已有封存证据：\(sealedHorizons.map { $0.uppercased() }.joined(separator: " / "))"
        }
        return scheduled ? "已安排后续评估 · 尚未确认封存" : "尚未确认封存"
    }
}
