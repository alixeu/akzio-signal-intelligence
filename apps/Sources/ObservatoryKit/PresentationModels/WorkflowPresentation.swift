import Foundation

// MARK: - Workflow graph

public enum WorkflowEdgeKind: String, Sendable, CaseIterable {
    // edge kind 保留 Rust workflow 拓扑语义；虚线和色调只是 Canvas 的派生呈现。
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

    // optional/loopBack 使用虚线，冲突与关键路径的语义由 tone 单独映射。
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
    // 节点是 Rust/Mock workflow 的值语义投影；taskID 可缺失时由 stage.id 提供 UI 稳定 ID。
    public let taskID: String?
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

    public var id: String { taskID ?? stage.id }

    public init(
        taskID: String? = nil,
        stage: WorkflowStageKind,
        taskStatus: TaskStatus,
        isApplicable: Bool = true,
        confidencePpm: Int? = nil,
        blockers: [HardBlocker] = [],
        warnings: [SoftWarning] = [],
        column: Int,
        row: Int
    ) {
        // 初始化保留 task status、适用性和诊断 Optional，布局坐标与节点一起冻结。
        self.taskID = taskID
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
        // status 是 TaskStatus 加 stage 上下文的唯一展示转换入口。
        taskStatus.status(optional: stage.isOptional, applicable: isApplicable)
    }

    // 页面读取派生状态，不改变 taskStatus 原值。
    public var isActive: Bool { status == .running }
    public var isBlocked: Bool { !blockers.isEmpty }
}

public struct WorkflowEdgePresentation: Sendable, Hashable, Identifiable {
    // 边只保存两端稳定字符串 ID 和 edge kind，页面可直接按节点投影绘制。
    public let from: String
    public let to: String
    public let kind: WorkflowEdgeKind

    public var id: String { "\(from)->\(to)" }

    public init(from: WorkflowStageKind, to: WorkflowStageKind, kind: WorkflowEdgeKind) {
        // 阶段输入在初始化时转成 stage.id；这一步不访问或修改 Rust 依赖图。
        self.from = from.id
        self.to = to.id
        self.kind = kind
    }
    public init(fromTask: String, toTask: String, kind: WorkflowEdgeKind) {
        // 该初始化器用于已有 Observer task ID 的边，保留原始字符串以便回溯。
        self.from = fromTask
        self.to = toTask
        self.kind = kind
    }
}

// MARK: - Stage inspector

public struct StageToolEventPresentation: Sendable, Hashable, Identifiable {
    // 工具事件是已脱敏的生命周期投影；sequence 作为 inspector 的时间排序依据。
    public let id: String
    public let sequence: Int64
    public let callID: String?
    public let name: String
    public let lifecycle: String
    public let turn: Int?

    public init(cursor: Int64, callID: String?, name: String, lifecycle: String, turn: Int?) {
        // callID 缺失时使用固定 tool 前缀构成可识别 ID，不伪造调用 ID。
        self.id = "\(callID ?? "tool")-\(cursor)"
        self.sequence = cursor
        self.callID = callID
        self.name = name
        self.lifecycle = lifecycle
        self.turn = turn
    }
}

public struct StageLLMOutputPresentation: Sendable, Hashable, Identifiable {
    // LLM output 只承载可展示的正文和元数据，不代表隐藏推理链或 provider envelope。
    public let id: String
    public let sequence: Int64
    public let kind: String
    public let createdAt: Date
    public let body: String

    public init(id: String, kind: String, createdAt: Date, body: String, sequence: Int64 = 0) {
        // sequence 默认从零开始，调用方有真实序列时再覆盖以参与排序。
        self.id = id
        self.sequence = sequence
        self.kind = kind
        self.createdAt = createdAt
        self.body = body
    }

}

public struct ResearchAuditRowPresentation: Sendable, Hashable, Identifiable {
    // research audit row 是已验证/脱敏的依据摘要，references 仍由上游决定。
    public let id: String
    public let title: String
    public let detail: String
    public let references: [String]
    public let isFailure: Bool
}

public struct StageInspectorPresentation: Sendable, Hashable {
    // inspector 聚合节点的模型、证据、工具和诊断投影，页面只读取这些公开字段。
    public let researchAudit: [ResearchAuditRowPresentation]
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
    public let taskID: String?
    public let horizon: String?

    /// Chronological, Observer-safe rows for the inspector and Overview popover.
    /// This is a projection of already-redacted telemetry and validated artifacts,
    /// never the provider's hidden chain of thought.
    public var analysisRecords: [AnalysisRecordPresentation] {
        // 先用 tool/LLM sequence 计算边界，再将摘要、工具事件和 Rust conclusion 合并排序。
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
            // map 闭包把 Observer 工具生命周期压缩成可见详情，不展开工具参数或秘密。
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
        return rows.map { row in
            // map 闭包把 inspector 自身的 taskID/horizon 补到每条记录，随后按 sequence 稳定排序。
            var value = row
            value.taskID = taskID ?? row.taskID
            value.horizon = horizon ?? row.horizon
            return value
        }.sorted {
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
        transientAnalysisRecords: [AnalysisRecordPresentation] = [],
        taskID: String? = nil,
        horizon: String? = nil,
        researchAudit: [ResearchAuditRowPresentation] = []
    ) {
        // 初始化保留所有 Optional 诊断和已脱敏记录；analysisRecords 在读取时才派生。
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
        self.taskID = taskID
        self.horizon = horizon
        self.researchAudit = researchAudit
    }
}

public struct WorkflowPresentation: Sendable, Hashable {
    // WorkflowPresentation 是页面读取的聚合快照；nodes/edges 与 inspector 共享同一 run 投影。
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
        // 初始化不重排或修复输入数组；顺序和 stageInspectors 映射由上游投影负责。
        self.nodes = nodes
        self.edges = edges
        self.activeStageID = activeStageID
        self.inspector = inspector
        self.stageInspectors = stageInspectors
        self.observedTradingDays = observedTradingDays
        self.totalTradingDays = totalTradingDays
    }

    public func node(id: String) -> WorkflowNodePresentation? {
        // first 闭包按展示 ID 查询节点；缺失时返回 nil，让页面明确处理不可用选择。
        nodes.first { $0.id == id }
    }

    public func inspector(for stageID: String?) -> StageInspectorPresentation {
        // 指定阶段优先读取专属 inspector，缺失或 nil 时回退聚合 inspector。
        guard let stageID else { return inspector }
        return stageInspectors[stageID] ?? inspector
    }

    // 计数 computed property 只读取节点数组；不把展示计数写回 Observer 状态。
    public var completedCount: Int { nodes.filter { $0.taskStatus == .succeeded }.count }
    public var activeCount: Int { nodes.filter(\.isActive).count }
    public var queuedCount: Int { nodes.filter { $0.taskStatus == .pending || $0.taskStatus == .leased }.count }
    public var alertCount: Int { nodes.filter { $0.isBlocked || $0.taskStatus == .failed }.count }
    public var progressFraction: Double {
        // 空节点图返回零；否则按 succeeded 数量计算页面进度，不混入 skipped/notApplicable。
        guard !nodes.isEmpty else { return 0 }
        return Double(completedCount) / Double(nodes.count)
    }
}
