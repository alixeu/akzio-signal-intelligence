import Foundation

// MARK: - Council (Intelligence page)

/// A named uncertainty with a weight. Weights are ppm so the bars format like
/// every other ratio in the app.
public struct UncertaintyPresentation: Sendable, Hashable, Identifiable {
    public let label: String
    public let weightPpm: Int?

    public var id: String { label }

    public init(label: String, weightPpm: Int?) {
        self.label = label
        self.weightPpm = weightPpm
    }
}

/// Supporting material a conclusion leans on. Never the model's hidden reasoning —
/// only the artifacts it cited.
public struct BasisArtifact: Sendable, Hashable, Identifiable {
    public let label: String
    public let symbol: String

    public var id: String { label }

    public init(label: String, symbol: String) {
        self.label = label
        self.symbol = symbol
    }
}

public struct AlternativePresentation: Sendable, Hashable, Identifiable {
    public let tag: String
    public let label: String
    public let matchPpm: Int?

    public var id: String { tag }

    public init(tag: String, label: String, matchPpm: Int?) {
        self.tag = tag
        self.label = label
        self.matchPpm = matchPpm
    }
}

public struct ModelOption: Sendable, Hashable, Identifiable {
    public let name: String
    public let tier: String
    public let isSelected: Bool

    public var id: String { name }

    public init(name: String, tier: String, isSelected: Bool) {
        self.name = name
        self.tier = tier
        self.isSelected = isSelected
    }
}

// MARK: - Observed analysis

public enum AnalysisRecordKind: String, Sendable, Hashable {
    case reasoningSummary = "reasoning_summary"
    case researchMemo = "research_memo"
    case analysis
    case tool
    case llmOutput = "llm_output"
    case rustOutput = "rust_output"
    case conclusion

    public var displayName: String {
        switch self {
        case .reasoningSummary, .researchMemo, .analysis, .llmOutput, .conclusion: "LLM"
        case .tool: "Tool"
        case .rustOutput: "Rust"
        }
    }

    public var tone: AkzioTone {
        switch self {
        case .tool: .coral
        case .analysis: .neutral
        case .reasoningSummary, .researchMemo, .llmOutput, .rustOutput, .conclusion: .gold
        }
    }
}

/// Observer-safe conversation row. `body` is limited to durable deliberation,
/// tool lifecycle metadata, or a validated artifact; hidden reasoning, provider
/// envelopes, tool arguments, and secrets are never represented by this type.
public struct AnalysisRecordPresentation: Sendable, Hashable, Identifiable {
    public let id: String
    public let sequence: Int64
    public let kind: AnalysisRecordKind
    public let actor: String
    public let title: String
    public let body: String
    public let createdAt: Date?
    public let model: String?
    public let reasoningMode: String?
    public let latencyMillis: Int?
    public let inputTokens: Int?
    public let outputTokens: Int?
    public let isStreaming: Bool

    public init(
        id: String,
        sequence: Int64,
        kind: AnalysisRecordKind,
        actor: String,
        title: String,
        body: String,
        createdAt: Date? = nil,
        model: String? = nil,
        reasoningMode: String? = nil,
        latencyMillis: Int? = nil,
        inputTokens: Int? = nil,
        outputTokens: Int? = nil,
        isStreaming: Bool = false
    ) {
        self.id = id
        self.sequence = sequence
        self.kind = kind
        self.actor = actor
        self.title = title
        self.body = body
        self.createdAt = createdAt
        self.model = model
        self.reasoningMode = reasoningMode
        self.latencyMillis = latencyMillis
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.isStreaming = isStreaming
    }

    public var metadata: String? {
        let values = [
            model,
            reasoningMode,
            latencyMillis.map { PpmFormatter.latency(millis: $0) },
            inputTokens.map { "↓ \(PpmFormatter.count($0))" },
            outputTokens.map { "↑ \(PpmFormatter.count($0))" },
        ].compactMap { $0 }.filter { !$0.isEmpty && $0 != MissingValue.unavailable.rawValue }
        return values.isEmpty ? nil : values.joined(separator: " · ")
    }
}

struct LiveReasoningRecord: Sendable, Hashable, Identifiable {
    let id: String
    let sequence: Int64
    let runID: String
    let taskID: String
    let purpose: String
    let turn: UInt16
    let createdAt: Date
    var body: String
    var isComplete: Bool

    var presentation: AnalysisRecordPresentation {
        AnalysisRecordPresentation(
            id: id,
            sequence: sequence,
            kind: .reasoningSummary,
            actor: CoreModelStage(rawValue: purpose)?.displayName ?? purpose,
            title: "Reasoning Summary",
            body: body.isEmpty ? "Generating reasoning summary…" : body,
            createdAt: createdAt,
            isStreaming: !isComplete
        )
    }
}

public enum IntelligenceTopicKind: String, Sendable, Hashable {
    case topic
    case issue
    case alternative
    case conclusion

    public var displayName: String { rawValue.capitalized }

    public var tone: AkzioTone {
        switch self {
        case .issue: .coral
        case .conclusion: .gold
        case .topic, .alternative: .neutral
        }
    }
}

public struct IntelligenceTopicPresentation: Sendable, Hashable, Identifiable {
    public let id: String
    public let kind: IntelligenceTopicKind
    public let title: String
    public let source: String

    public init(id: String, kind: IntelligenceTopicKind, title: String, source: String) {
        self.id = id
        self.kind = kind
        self.title = title
        self.source = source
    }
}

// MARK: - Role card

public struct RoleCardPresentation: Sendable, Hashable, Identifiable {
    public let role: AgentRole
    public let model: String
    public let status: AkzioStatus
    public let tokensIn: Int?
    public let tokensOut: Int?
    public let toolCalls: Int?
    public let latencyMillis: Int?
    public let confidencePpm: Int?
    public let intensity: ReasoningIntensity

    public var id: String { role.rawValue }

    public init(
        role: AgentRole,
        model: String,
        status: AkzioStatus,
        tokensIn: Int?,
        tokensOut: Int?,
        toolCalls: Int?,
        latencyMillis: Int?,
        confidencePpm: Int?,
        intensity: ReasoningIntensity
    ) {
        self.role = role
        self.model = model
        self.status = status
        self.tokensIn = tokensIn
        self.toolCalls = toolCalls
        self.tokensOut = tokensOut
        self.latencyMillis = latencyMillis
        self.confidencePpm = confidencePpm
        self.intensity = intensity
    }

    /// A Critic that never fired shows `Not Triggered`, not an idle success card.
    public var isNotTriggered: Bool { status == .notTriggered }
}

// MARK: - Selected model detail

public struct CouncilPresentation: Sendable, Hashable {
    public let roles: [RoleCardPresentation]
    public let selectedRole: AgentRole
    public let selectedModelName: String
    public let selectedModelSummary: String
    public let selectedPath: [String]
    public let intensity: ReasoningIntensity
    public let gallery: [ModelOption]
    public let alternatives: [AlternativePresentation]
    public let uncertainties: [UncertaintyPresentation]
    public let basisArtifacts: [BasisArtifact]
    public let overallUncertaintyPpm: Int?
    public let topics: [IntelligenceTopicPresentation]
    public let analysisRecords: [AnalysisRecordPresentation]

    public init(
        roles: [RoleCardPresentation],
        selectedRole: AgentRole,
        selectedModelName: String,
        selectedModelSummary: String,
        selectedPath: [String],
        intensity: ReasoningIntensity,
        gallery: [ModelOption],
        alternatives: [AlternativePresentation],
        uncertainties: [UncertaintyPresentation],
        basisArtifacts: [BasisArtifact],
        overallUncertaintyPpm: Int?,
        topics: [IntelligenceTopicPresentation] = [],
        analysisRecords: [AnalysisRecordPresentation] = []
    ) {
        self.roles = roles
        self.selectedRole = selectedRole
        self.selectedModelName = selectedModelName
        self.selectedModelSummary = selectedModelSummary
        self.selectedPath = selectedPath
        self.intensity = intensity
        self.gallery = gallery
        self.alternatives = alternatives
        self.uncertainties = uncertainties
        self.basisArtifacts = basisArtifacts
        self.overallUncertaintyPpm = overallUncertaintyPpm
        self.topics = topics
        self.analysisRecords = analysisRecords
    }

    public func role(_ role: AgentRole) -> RoleCardPresentation? {
        roles.first { $0.role == role }
    }

    public var selectedCard: RoleCardPresentation? { role(selectedRole) }
}
