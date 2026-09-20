import Foundation

struct NodeSpecPayload: Decodable, Sendable {
    let key: String
    let horizon: String?
    let research_round: Int?
    let proposal_revision: Int?
}

struct WorkflowBlueprintPayload: Decodable, Sendable {
    struct Node: Decodable, Sendable, Identifiable {
        let spec: NodeSpecPayload
        let recipe_id: String
        let dependencies: [String]
        var id: String { spec.key }
    }
    let definition_version: Int?
    let definition_hash: String
    let purpose: String
    let nodes: [Node]
    let entry: [String]
    let finish: [String]
}

struct RuntimeInspectionPayload: Decodable, Sendable {
    struct Control: Decodable, Sendable {
        let revision: UInt64
        let status: String
        let execution_mode: String
    }
    struct Checkpoint: Decodable, Sendable {
        let event_cursor: Int64
        let boundary: String
        let control_revision: UInt64
        let created_at: Date
    }
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
        let cursor: Int64
        let event_type: String
        let task_id: String?
        let attempt_id: String?
        let artifact: JSONValue?
        let source_refs: [JSONValue]
        let created_at: Date
        var id: Int64 { cursor }
    }
    let events: [Event]
    let next_cursor: Int64
    let has_more: Bool
}
