import Foundation

// MARK: - Workflow graph

public enum WorkflowEdgeKind: String, Sendable, CaseIterable {
    case sequential
    case parallel
    case optional
    case loopBack = "loop_back"
    case criticalPath = "critical_path"
    case conflict

    public var displayName: String {
        switch self {
        case .loopBack: "Loop Back"
        case .criticalPath: "Critical Path"
        default: rawValue.capitalized
        }
    }

    public var isDashed: Bool { self == .optional || self == .loopBack }
    public var tone: AkzioTone {
        switch self {
        case .conflict: .coral
        case .criticalPath: .gold
        case .optional, .loopBack: .muted
        case .sequential, .parallel: .neutral
        }
    }
}

public struct WorkflowNodePresentation: Sendable, Hashable, Identifiable {
    public let stage: WorkflowStageKind
    public let taskStatus: TaskStatus
    public let isApplicable: Bool
    public let confidencePpm: Int?
    public let blockers: [HardBlocker]
    public let warnings: [SoftWarning]
    /// Layout column / row assigned by `DagLayout` (kept in the model so the canvas
    /// and the accessibility overlay agree on positions).
    public let column: Int
    public let row: Int

    public var id: String { stage.id }

    public init(
        stage: WorkflowStageKind,
        taskStatus: TaskStatus,
        isApplicable: Bool = true,
        confidencePpm: Int? = nil,
        blockers: [HardBlocker] = [],
        warnings: [SoftWarning] = [],
        column: Int,
        row: Int
    ) {
        self.stage = stage
        self.taskStatus = taskStatus
        self.isApplicable = isApplicable
        self.confidencePpm = confidencePpm
        self.blockers = blockers
        self.warnings = warnings
        self.column = column
        self.row = row
    }

    /// The single place where `Skipped` + optional becomes `Not Triggered`, and a
    /// non-Paper Paper Commit becomes `Not Applicable`.
    public var status: AkzioStatus {
        taskStatus.status(optional: stage.isOptional, applicable: isApplicable)
    }

    public var isActive: Bool { status == .running }
    public var isBlocked: Bool { !blockers.isEmpty }
}

public struct WorkflowEdgePresentation: Sendable, Hashable, Identifiable {
    public let from: String
    public let to: String
    public let kind: WorkflowEdgeKind

    public var id: String { "\(from)->\(to)" }

    public init(from: WorkflowStageKind, to: WorkflowStageKind, kind: WorkflowEdgeKind) {
        self.from = from.id
        self.to = to.id
        self.kind = kind
    }
}

// MARK: - Stage inspector

public struct StageToolEventPresentation: Sendable, Hashable, Identifiable {
    public let id: String
    public let sequence: Int64
    public let callID: String?
    public let name: String
    public let lifecycle: String
    public let turn: Int?

    public init(cursor: Int64, callID: String?, name: String, lifecycle: String, turn: Int?) {
        self.id = "\(callID ?? "tool")-\(cursor)"
        self.sequence = cursor
        self.callID = callID
        self.name = name
        self.lifecycle = lifecycle
        self.turn = turn
    }
}

public struct StageLLMOutputPresentation: Sendable, Hashable, Identifiable {
    public let id: String
    public let sequence: Int64
    public let kind: String
    public let createdAt: Date
    public let body: String

    public init(id: String, kind: String, createdAt: Date, body: String, sequence: Int64 = 0) {
        self.id = id
        self.sequence = sequence
        self.kind = kind
        self.createdAt = createdAt
        self.body = body
    }

    public var displayKind: String {
        kind.replacingOccurrences(of: "_", with: " ").capitalized
    }
}

public struct StageInspectorPresentation: Sendable, Hashable {
    public let stageTitle: String
    public let status: AkzioStatus
    public let model: String
    public let provider: String
    public let reasoningMode: String
    public let turn: Int
    public let totalTurns: Int
    public let toolCalls: Int
    public let latencyMillis: Int?
    public let inputTokens: Int?
    public let outputTokens: Int?
    public let confidencePpm: Int?
    public let summary: String
    public let conclusion: String?
    public let alternatives: [String]
    public let uncertainties: [UncertaintyPresentation]
    public let toolEvents: [StageToolEventPresentation]
    public let llmOutputs: [StageLLMOutputPresentation]
    public let blockers: [HardBlocker]
    public let warnings: [SoftWarning]
    public let transientAnalysisRecords: [AnalysisRecordPresentation]

    /// Chronological, Observer-safe rows for the inspector and Overview popover.
    /// This is a projection of already-redacted telemetry and validated artifacts,
    /// never the provider's hidden chain of thought.
    public var analysisRecords: [AnalysisRecordPresentation] {
        let sequences = toolEvents.map(\.sequence) + llmOutputs.map(\.sequence)
        let firstSequence = (sequences.min() ?? 1) - 1
        let lastSequence = (sequences.max() ?? firstSequence) + 1
        let observedAt = llmOutputs.map(\.createdAt).min()
        let metadata = (
            model: model == "Rust" ? nil : model,
            reasoning: reasoningMode == "N/A" ? nil : reasoningMode,
            latency: latencyMillis,
            input: inputTokens,
            output: outputTokens
        )
        var rows = transientAnalysisRecords

        let hasResearchMemo = rows.contains { $0.kind == .researchMemo }
        if !hasResearchMemo,
           !summary.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        {
            rows.append(
                AnalysisRecordPresentation(
                    id: "\(stageTitle)-analysis",
                    sequence: firstSequence,
                    kind: .analysis,
                actor: stageTitle,
                title: "Analysis Summary",
                body: summary,
                createdAt: observedAt,
                    model: metadata.model,
                    reasoningMode: metadata.reasoning,
                    latencyMillis: metadata.latency,
                    inputTokens: metadata.input,
                    outputTokens: metadata.output
                )
            )
        }

        rows.append(contentsOf: toolEvents.map { event in
            let details = [
                event.lifecycle.capitalized,
                event.turn.map { "T\($0)" },
                event.callID,
            ].compactMap { $0 }
            return AnalysisRecordPresentation(
                id: event.id,
                sequence: event.sequence,
                kind: .tool,
                actor: stageTitle,
                title: event.name,
                body: details.joined(separator: " · "),
                createdAt: observedAt
            )
        })
        if let conclusion, !conclusion.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            rows.append(
                AnalysisRecordPresentation(
                    id: "\(stageTitle)-conclusion",
                    sequence: lastSequence,
                kind: .rustOutput,
                actor: "Rust",
                title: "Validated result",
                    body: conclusion,
                createdAt: llmOutputs.last?.createdAt ?? observedAt,
                    model: metadata.model,
                    reasoningMode: metadata.reasoning
                )
            )
        }
        return rows.sorted {
            $0.sequence == $1.sequence ? $0.kind.rawValue < $1.kind.rawValue : $0.sequence < $1.sequence
        }
    }

    public init(
        stageTitle: String,
        status: AkzioStatus,
        model: String,
        provider: String = MissingValue.unavailable.rawValue,
        reasoningMode: String,
        turn: Int,
        totalTurns: Int,
        toolCalls: Int,
        latencyMillis: Int?,
        inputTokens: Int? = nil,
        outputTokens: Int? = nil,
        confidencePpm: Int?,
        summary: String,
        conclusion: String? = nil,
        alternatives: [String],
        uncertainties: [UncertaintyPresentation],
        toolEvents: [StageToolEventPresentation] = [],
        llmOutputs: [StageLLMOutputPresentation] = [],
        blockers: [HardBlocker] = [],
        warnings: [SoftWarning] = [],
        transientAnalysisRecords: [AnalysisRecordPresentation] = []
    ) {
        self.stageTitle = stageTitle
        self.status = status
        self.model = model
        self.provider = provider
        self.reasoningMode = reasoningMode
        self.turn = turn
        self.totalTurns = totalTurns
        self.toolCalls = toolCalls
        self.latencyMillis = latencyMillis
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.confidencePpm = confidencePpm
        self.summary = summary
        self.conclusion = conclusion
        self.alternatives = alternatives
        self.uncertainties = uncertainties
        self.toolEvents = toolEvents
        self.llmOutputs = llmOutputs
        self.blockers = blockers
        self.warnings = warnings
        self.transientAnalysisRecords = transientAnalysisRecords
    }
}

public struct WorkflowPresentation: Sendable, Hashable {
    public let nodes: [WorkflowNodePresentation]
    public let edges: [WorkflowEdgePresentation]
    public let activeStageID: String?
    public let inspector: StageInspectorPresentation
    public let stageInspectors: [String: StageInspectorPresentation]
    public let observedTradingDays: Int
    public let totalTradingDays: Int

    public init(
        nodes: [WorkflowNodePresentation],
        edges: [WorkflowEdgePresentation],
        activeStageID: String?,
        inspector: StageInspectorPresentation,
        observedTradingDays: Int,
        totalTradingDays: Int,
        stageInspectors: [String: StageInspectorPresentation] = [:]
    ) {
        self.nodes = nodes
        self.edges = edges
        self.activeStageID = activeStageID
        self.inspector = inspector
        self.stageInspectors = stageInspectors
        self.observedTradingDays = observedTradingDays
        self.totalTradingDays = totalTradingDays
    }

    public func node(id: String) -> WorkflowNodePresentation? {
        nodes.first { $0.id == id }
    }

    public func inspector(for stageID: String?) -> StageInspectorPresentation {
        guard let stageID else { return inspector }
        return stageInspectors[stageID] ?? inspector
    }

    public var completedCount: Int { nodes.filter { $0.taskStatus == .succeeded }.count }
    public var activeCount: Int { nodes.filter(\.isActive).count }
    public var queuedCount: Int { nodes.filter { $0.taskStatus == .pending || $0.taskStatus == .leased }.count }
    public var alertCount: Int { nodes.filter { $0.isBlocked || $0.taskStatus == .failed }.count }
    public var progressFraction: Double {
        guard !nodes.isEmpty else { return 0 }
        return Double(completedCount) / Double(nodes.count)
    }
}
