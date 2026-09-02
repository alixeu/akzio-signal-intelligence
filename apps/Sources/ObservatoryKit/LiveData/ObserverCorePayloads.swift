import Foundation

struct ObserverSnapshotPayload: Decodable, Sendable {
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
    let status: String
    let frozen: Bool
    let schedulerOwner: String?
    let schedulerEpoch: UInt64?
    let alerts: [ObserverAlertPayload]

    enum CodingKeys: String, CodingKey {
        case status
        case frozen
        case schedulerOwner = "scheduler_owner"
        case schedulerEpoch = "scheduler_epoch"
        case alerts
    }
}

struct ObserverAlertPayload: Decodable, Sendable {
    let code: String
    let severity: String
    let count: UInt64
}

struct ObserverSectionPayload<T: Decodable & Sendable>: Decodable, Sendable {
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
    let workflow: ObserverWorkflowPayload
    let events: [ObserverEventPayload]
    let trajectory: [ObserverTrajectoryPayload]
    let artifacts: [ObserverArtifactPayload]
    let telemetry: ObserverRunTelemetryPayload?
}

struct ObserverRunTelemetryPayload: Decodable, Sendable {
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
    let artifactID: String
    let kind: String

    enum CodingKeys: String, CodingKey {
        case artifactID = "artifact_id"
        case kind
    }
}

struct ObserverModelMetadataPayload: Decodable, Sendable {
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
    let category: String
    let impactPpm: Int64

    enum CodingKeys: String, CodingKey {
        case category
        case impactPpm = "impact_ppm"
    }
}
