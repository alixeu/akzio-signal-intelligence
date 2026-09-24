import Foundation

// Core payload 全部是不可变 Decodable/Sendable 值快照；CodingKeys 负责 snake_case 协议到 Swift 命名的单向投影。

struct ObserverSnapshotPayload: Decodable, Sendable {
    // eventCursor 是快照与 SSE 增量的衔接点；currentRun/section data 可为空，表示服务端暂时没有该事实。
    let schemaVersion: Int
    let generatedAt: Date
    let eventCursor: Int64
    let core: ObserverCorePayload
    let currentRun: ObserverRunDetailPayload?
    let recentRuns: [ObserverWorkflowPayload]
    let runSummaries: [ObserverRunSummaryPayload]?
    let portfolio: ObserverSectionPayload<ObserverPortfolioPayload>
    let outcome: ObserverSectionPayload<ObserverOutcomePayload>?
    let learning: ObserverSectionPayload<ObserverLearningPayload>

    var health: ObserverHealthPayload { core.health }
    var runs: [ObserverWorkflowPayload] { recentRuns }

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedAt = "generated_at"
        case eventCursor = "event_cursor"
        case core
        case currentRun = "current_run"
        case recentRuns = "recent_runs"
        case runSummaries = "run_summaries"
        case portfolio
        case outcome
        case learning
    }
}

struct ObserverCorePayload: Decodable, Sendable {
    // ready/readiness 只描述 Core 自身可用性，approval/health 仍分别保留，不能合并成 UI 的单一成功布尔值。
    let ready: Bool
    let readinessPpm: UInt32?
    let autoPaper: Bool
    let health: ObserverHealthPayload
    let approval: ObserverApprovalPayload

    enum CodingKeys: String, CodingKey {
        case ready
        case readinessPpm = "readiness_ppm"
        case autoPaper = "auto_paper"
        case health
        case approval
    }
}

struct ObserverApprovalPayload: Decodable, Sendable {
    // approval 的 operator/reason/expiresAt 可为空；状态本身不被客户端推断为 Paper 授权。
    let status: String
    let operatorIdentity: String?
    let reason: String?
    let expiresAt: Date?

    enum CodingKeys: String, CodingKey {
        case status
        case operatorIdentity = "operator_identity"
        case reason
        case expiresAt = "expires_at"
    }
}

struct ObserverHealthPayload: Decodable, Sendable {
    // health 将冻结、Policy、NewsWeb、scheduler 和 alerts 分字段传递，Optional 表示对应服务尚未报告。
    let status: String
    let frozen: Bool
    let decisionPolicyStatus: String?
    let decisionCapable: Bool?
    let newsWebStatus: String?
    let newsWebRoute: String?
    let schedulerOwner: String?
    let schedulerEpoch: UInt64?
    let alerts: [ObserverAlertPayload]

    enum CodingKeys: String, CodingKey {
        case status
        case frozen
        case decisionPolicyStatus = "decision_policy_status"
        case decisionCapable = "decision_capable"
        case newsWebStatus = "news_web_status"
        case newsWebRoute = "news_web_route"
        case schedulerOwner = "scheduler_owner"
        case schedulerEpoch = "scheduler_epoch"
        case alerts
    }
}

struct ObserverAlertPayload: Decodable, Sendable {
    // alert 是 Core 已聚合的只读计数；Swift 不合并或提升 severity。
    let code: String
    let severity: String
    let count: UInt64
}

struct ObserverSectionPayload<T: Decodable & Sendable>: Decodable, Sendable {
    // 泛型 section 共享 status/observedAt/reason/data；data 为 nil 时 reason/status 是唯一可展示依据。
    let status: String
    let observedAt: Date?
    let reason: String?
    let data: T?

    enum CodingKeys: String, CodingKey {
        case status
        case observedAt = "observed_at"
        case reason
        case data
    }
}

struct ObserverRunDetailPayload: Decodable, Sendable {
    // detail 把 workflow、事件、trajectory、artifact 和 telemetry 绑定在同一 Run 快照内，避免跨 Run 拼接。
    let inspection: RuntimeInspectionPayload?
    let workflow: ObserverWorkflowPayload
    let events: [ObserverEventPayload]
    let trajectory: [ObserverTrajectoryPayload]
    let artifacts: [ObserverArtifactPayload]
    let telemetry: ObserverRunTelemetryPayload?
    let researchAudit: ResearchAuditPayload?

    enum CodingKeys: String, CodingKey {
        case inspection, workflow, events, trajectory, artifacts, telemetry
        case researchAudit = "research_audit"
    }
}

struct ObserverRunTelemetryPayload: Decodable, Sendable {
    // telemetry 的模型/耗时/token 字段可缺失，toolCalls/turns 则保留 Rust 给出的整数值。
    let modelID: String?
    let latencyMillis: UInt64?
    let inputTokens: UInt64?
    let outputTokens: UInt64?
    let toolCalls: Int
    let turns: Int

    enum CodingKeys: String, CodingKey {
        case modelID = "model_id"
        case latencyMillis = "latency_millis"
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case toolCalls = "tool_calls"
        case turns
    }
}

struct ObserverRunSummaryPayload: Decodable, Sendable {
    // summary 通过 runID 与 workflow 关联；brokerSession 和 utility 缺失时保持 Optional。
    let runID: String
    let modelID: String?
    let latencyMillis: UInt64?
    let brokerSession: String?
    let resultUtilityPpm: Int64?

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
        case modelID = "model_id"
        case latencyMillis = "latency_millis"
        case brokerSession = "broker_session"
        case resultUtilityPpm = "result_utility_ppm"
    }
}

struct ObserverEventPayload: Decodable, Sendable {
    // event 的 cursor 是稳定身份，taskID 可为空以覆盖 Run 级生命周期事件。
    let cursor: Int64
    let eventType: String
    let taskID: String?
    let createdAt: Date

    enum CodingKeys: String, CodingKey {
        case cursor
        case eventType = "event_type"
        case taskID = "task_id"
        case createdAt = "created_at"
    }
}

struct ObserverTrajectoryPayload: Decodable, Sendable {
    // trajectory 的 model/tool/deliberation/outputRefs 都是 Optional，因为 Rust 事件阶段可能只提供其中一类信息。
    let cursor: Int64
    let taskID: String?
    let turn: UInt32?
    let phase: String?
    let assistantText: String?
    let eventType: String
    let artifactID: String?
    let artifactKind: String?
    let model: ObserverModelMetadataPayload?
    let latencyMillis: UInt64?
    let inputTokens: UInt64?
    let outputTokens: UInt64?
    let tool: ObserverToolLifecyclePayload?
    let deliberation: ObserverDeliberationPayload?
    let outputRefs: [ObserverArtifactReferencePayload]?

    enum CodingKeys: String, CodingKey {
        case cursor
        case taskID = "task_id"
        case turn
        case phase
        case assistantText = "assistant_text"
        case eventType = "event_type"
        case artifactID = "artifact_id"
        case artifactKind = "artifact_kind"
        case model
        case latencyMillis = "latency_millis"
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case tool
        case deliberation
        case outputRefs = "output_refs"
    }
}

struct ObserverToolLifecyclePayload: Decodable, Sendable {
    // tool lifecycle 只保存 provider/Runtime 已报告的阶段；callID 和 name 缺失不被 UI 补造。
    let callID: String?
    let name: String?
    let lifecycle: String

    enum CodingKeys: String, CodingKey {
        case callID = "call_id"
        case name
        case lifecycle
    }
}

struct ObserverArtifactReferencePayload: Decodable, Sendable {
    // output reference 只携带 CAS artifact 的 ID/kind，实际内容仍由 artifact payload 单独解码。
    let artifactID: String
    let kind: String

    enum CodingKeys: String, CodingKey {
        case artifactID = "artifact_id"
        case kind
    }
}

struct ObserverModelMetadataPayload: Decodable, Sendable {
    // model metadata 的 snake_case 字段映射为 Swift camelCase，provider/model/reasoning 均可未观测。
    let providerID: String?
    let modelID: String?
    let reasoningEffort: String?

    enum CodingKeys: String, CodingKey {
        case providerID = "provider_id"
        case modelID = "model_id"
        case reasoningEffort = "reasoning_effort"
    }
}

struct ObserverDeliberationPayload: Decodable, Sendable {
    // 不确定性与备选分数保持数组原貌；投影层按 index 对齐，缺少对应权重时保留 nil。
    let selectedPath: String
    let alternatives: [String]
    let alternativeMatchPpm: [UInt32]?
    let uncertainties: [String]
    let uncertaintyWeightPpm: [UInt32]?
    let assessmentSource: String?
    let basisArtifactIDs: [String]
    let confidencePpm: UInt32

    enum CodingKeys: String, CodingKey {
        case selectedPath = "selected_path"
        case alternatives
        case alternativeMatchPpm = "alternative_match_ppm"
        case uncertainties
        case uncertaintyWeightPpm = "uncertainty_weight_ppm"
        case assessmentSource = "assessment_source"
        case basisArtifactIDs = "basis_artifact_ids"
        case confidencePpm = "confidence_ppm"
    }
}

struct ObserverArtifactPayload: Decodable, Sendable {
    // artifact payload 使用 JSONValue 保留未知 schema；artifactID/kind/createdAt 仍提供稳定展示身份。
    let artifactID: String
    let kind: String
    let createdAt: Date
    let payload: JSONValue

    enum CodingKeys: String, CodingKey {
        case artifactID = "artifact_id"
        case kind
        case createdAt = "created_at"
        case payload
    }
}

struct ObserverLearningPayload: Decodable, Sendable {
    // Learning section 同时承载不可变 artifact、transition 和可选 summary/metric 投影；缺一不互相推导。
    let artifacts: [ObserverArtifactPayload]
    let policyTransitions: [JSONValue]
    let summary: ObserverLearningSummaryPayload?
    let policyMetrics: [ObserverPolicyMetricPayload]?

    enum CodingKeys: String, CodingKey {
        case artifacts
        case policyTransitions = "policy_transitions"
        case summary
        case policyMetrics = "policy_metrics"
    }
}

struct ObserverLearningSummaryPayload: Decodable, Sendable {
    // summary 的金额、Ppm 和 delta 字段分别保留可选性，避免把没有 canonical learning 的窗口显示为零。
    let rangeDays: UInt32
    let attributedUtilityMicros: Int64?
    let attributedUtilityPpm: Int64?
    let lessonCandidates: Int
    let lessonCandidatesDelta: Int64
    let policiesEvolved: Int
    let policiesEvolvedDelta: Int64
    let impactAreas: [ObserverImpactAreaPayload]

    enum CodingKeys: String, CodingKey {
        case rangeDays = "range_days"
        case attributedUtilityMicros = "attributed_utility_micros"
        case attributedUtilityPpm = "attributed_utility_ppm"
        case lessonCandidates = "lesson_candidates"
        case lessonCandidatesDelta = "lesson_candidates_delta"
        case policiesEvolved = "policies_evolved"
        case policiesEvolvedDelta = "policies_evolved_delta"
        case impactAreas = "impact_areas"
    }
}

struct ObserverImpactAreaPayload: Decodable, Sendable {
    // impact area 是 Rust 归因后的值语义记录，category 保留原始类别，impactPpm 仅做类型映射。
    let category: String
    let impactPpm: Int64

    enum CodingKeys: String, CodingKey {
        case category
        case impactPpm = "impact_ppm"
    }
}
