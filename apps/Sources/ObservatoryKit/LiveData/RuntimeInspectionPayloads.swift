import Foundation

struct NodeSpecPayload: Decodable, Sendable {
    // NodeSpec 直接使用 Rust 的协议字段名，保留 horizon/revision 可选性以兼容不同 workflow 版本。
    let key: String
    let horizon: String?
    let research_round: Int?
    let proposal_revision: Int?
}

struct WorkflowBlueprintPayload: Decodable, Sendable {
    struct Node: Decodable, Sendable, Identifiable {
        // blueprint node 的 id 使用 spec.key，避免把 recipe_id 或数组位置误当成节点身份。
        let spec: NodeSpecPayload
        let recipe_id: String
        let dependencies: [String]
        var id: String { spec.key }
    }
    // Blueprint 同时携带版本/hash、purpose、节点依赖和 entry/finish 边界，全部为只读解码值。
    let definition_version: Int?
    let definition_hash: String
    let purpose: String
    let nodes: [Node]
    let entry: [String]
    let finish: [String]
}

struct RuntimeInspectionPayload: Decodable, Sendable {
    struct Control: Decodable, Sendable {
        // control 描述运行控制版本、状态和 execution mode，UI 不根据 mode 发起动作。
        let revision: UInt64
        let status: String
        let execution_mode: String
    }
    struct Checkpoint: Decodable, Sendable {
        // checkpoint 把 event cursor、boundary、control revision 和创建时间绑定在一起，可为空表示尚无检查点。
        let event_cursor: Int64
        let boundary: String
        let control_revision: UInt64
        let created_at: Date
    }
    // inspection 的 workflow 是 JSONValue 以保留原始结构；runID 通过安全的可选路径读取。
    let workflow: JSONValue
    let blueprint: WorkflowBlueprintPayload
    let control: Control
    let checkpoint: Checkpoint?
    let recovery: String
    let allowed_actions: [String]
    var runID: String? { workflow["run"]?["run_id"]?.string }
}

struct RuntimeJournalPayload: Decodable, Sendable {
    struct Event: Decodable, Identifiable, Sendable {
        // journal event 用 cursor 作为 Identifiable key，artifact/source_refs 可缺失但原始事件顺序保留。
        let cursor: Int64
        let event_type: String
        let task_id: String?
        let attempt_id: String?
        let artifact: JSONValue?
        let source_refs: [JSONValue]
        let created_at: Date
        var id: Int64 { cursor }
    }
    // next_cursor/has_more 控制增量分页；Payload 本身不负责继续请求或合并日志。
    let events: [Event]
    let next_cursor: Int64
    let has_more: Bool
}
