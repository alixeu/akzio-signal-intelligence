import Foundation

/// Presentation only. These helpers never change Core eligibility or task dependencies.
enum WorkflowDisplay {
    static func horizon(objective: String) -> String? {
        if let marker = objective.range(of: "[research_horizon="),
           let end = objective[marker.upperBound...].firstIndex(of: "]") {
            return String(objective[marker.upperBound..<end])
        }
        return ["t1", "t3", "t5"].first { objective.contains(" \($0) Claim") }
    }

    static func phase(_ role: String) -> Int {
        if role.hasPrefix("learning.") || role.contains("outcome") { return 2 }
        if ["gate.execution", "gate.paper", "gate.reconcile", "gate.evaluate"].contains(role) || role.hasPrefix("execution.") || role.hasPrefix("broker.") { return 1 }
        return 0
    }

    static func ordered(_ nodes: [DebugNodePayload]) -> [DebugNodePayload] {
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
        if let verdict = artifacts.last(where: { $0.kind == "execution_verdict" })?.payload["verdict"]?.string,
           verdict == "no_order" { return "未下单" }
        // A passed gate or plan alone is not evidence of a fill.
        return status(evidence ?? "unknown")
    }

    static func orderedTasks(_ tasks: [ObserverTaskPayload]) -> [ObserverTaskPayload] {
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
        let ordered = ordered(nodes)
        return ordered.first { ["running", "leased"].contains($0.status) }?.id
            ?? ordered.first { $0.status == "failed" || ($0.status != "succeeded" && $0.blocked_reason != nil) }?.id
            ?? ordered.first?.id
    }

    static func status(_ value: String) -> String {
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
    public let sealedHorizons: [String]
    public let scheduled: Bool

    static let unknown = OutcomeEvidencePresentation(sealedHorizons: [], scheduled: false)

    static func from(_ artifacts: [(kind: String, payload: JSONValue)]) -> Self {
        let horizons = artifacts.filter { $0.kind == "outcome" }
            .flatMap { $0.payload["windows"]?.array ?? [] }
            .compactMap { $0["horizon"]?.string }
            .filter { ["t1", "t3", "t5"].contains($0) }
        return Self(sealedHorizons: Array(Set(horizons)).sorted(),
                    scheduled: artifacts.contains { $0.kind == "outcome_schedule" })
    }

    public var caption: String {
        if !sealedHorizons.isEmpty {
            return "已有封存证据：\(sealedHorizons.map { $0.uppercased() }.joined(separator: " / "))"
        }
        return scheduled ? "已安排后续评估 · 尚未确认封存" : "尚未确认封存"
    }
}
