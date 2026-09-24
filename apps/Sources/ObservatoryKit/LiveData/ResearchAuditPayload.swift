import Foundation

struct ResearchAuditPayload: Decodable, Sendable {
    // research audit 是 Rust 产出的版本化审计容器；records 仍保留 producer/task/artifact provenance。
    let version: Int
    let records: [ResearchAuditRecordPayload]
}

struct ResearchAuditRecordPayload: Decodable, Sendable {
    // record 的 artifactID 是稳定关联键，task/revision 可缺失，payload 按记录类型承载不同分支。
    let artifactID: String
    let producer: String
    let taskID: String?
    let proposalRevision: Int?
    let payload: ResearchAuditBodyPayload
    enum CodingKeys: String, CodingKey {
        case producer, payload
        case artifactID = "artifact_id", taskID = "task_id", proposalRevision = "proposal_revision"
    }
}

struct ResearchAuditBodyPayload: Decodable, Sendable {
    // body 在 assessments、records、reason 三种审计形状间保持 Optional，rows 负责选择可读投影。
    let reason: String?
    let assessments: [ResearchAssessmentPayload]?
    let records: [ResearchSelectionPayload]?
}

struct ResearchAssessmentPayload: Decodable, Sendable {
    // assessment 明确 scope、accepted、rationale 和可选 issues，不由 UI 重算通过条件。
    let scope: String
    let accepted: Bool
    let rationale: String
    let issues: [ResearchIssuePayload]?
}

struct ResearchIssuePayload: Decodable, Sendable {
    // issue 保留 field/correction/evidence refs，issueID 缺失时仍可用字段路径生成展示行。
    let issueID: String?
    let category: String
    let fieldPath: String
    let correctionCriterion: String
    let evidenceRefs: [ResearchReferencePayload]
    enum CodingKeys: String, CodingKey {
        case category
        case issueID = "issue_id"
        case fieldPath = "field_path", correctionCriterion = "correction_criterion", evidenceRefs = "evidence_refs"
    }
}

struct ResearchReferencePayload: Decodable, Sendable {
    // reference 只携带 artifactID，来源闭包仍由 Rust 审计记录定义。
    let artifactID: String
    enum CodingKeys: String, CodingKey { case artifactID = "artifact_id" }
}

struct ResearchSelectionPayload: Decodable, Sendable {
    // selection 的 status/selectionStatus/exclusionReason 可按历史版本缺失，projectionOmitsDetail 保留省略边界。
    let artifactID: String
    let status: String?
    let selectionStatus: String?
    let exclusionReason: String?
    let projectionOmitsDetail: Bool?
    enum CodingKeys: String, CodingKey {
        case status
        case artifactID = "artifact_id", selectionStatus = "selection_status"
        case exclusionReason = "exclusion_reason", projectionOmitsDetail = "projection_omits_detail"
    }
}

extension ResearchAuditRecordPayload {
    var rows: [ResearchAuditRowPresentation] {
        // rows 是纯值转换：优先展开 assessments，其次展开 selections，最后把 reason 转成单条说明。
        if let assessments = payload.assessments {
            return assessments.flatMap { assessment -> [ResearchAuditRowPresentation] in
                let title = "\(assessment.scope) · \(assessment.accepted ? "通过" : "需修订")"
                let issues = assessment.issues ?? []
                if issues.isEmpty {
                    // 没有 issue 时保留 assessment rationale；accepted=false 仍明确标记为 failure。
                    return [ResearchAuditRowPresentation(id: artifactID + assessment.scope, title: title,
                        detail: assessment.rationale, references: [], isFailure: !assessment.accepted)]
                }
                return issues.map { issue in
                    ResearchAuditRowPresentation(id: artifactID + assessment.scope + issue.fieldPath + issue.category,
                        title: title, detail: "\(issue.fieldPath)\n\(issue.correctionCriterion)",
                        references: issue.evidenceRefs.map(\.artifactID) + (issue.issueID.map { ["issue: " + $0] } ?? []), isFailure: true)
                }
            }
        }
        if let selections = payload.records {
            // selection 统计只计算已选状态，并把排除原因作为附加行，不把 projection omission 当作失败。
            let selected = selections.filter { $0.status == "selected" || $0.selectionStatus == "provided_material" }.count
            return [ResearchAuditRowPresentation(id: artifactID, title: producer == "context.coverage" ? "上下文选择" : "Lesson 召回",
                detail: "\(selected) / \(selections.count) 入选 · \(selections.filter { $0.projectionOmitsDetail == true }.count) 份投影省略细节",
                references: [], isFailure: false)] + selections.compactMap { selection in
                    guard let reason = selection.exclusionReason ?? selection.status,
                          reason != "selected" else { return nil }
                    return ResearchAuditRowPresentation(id: artifactID + selection.artifactID,
                        title: reason.replacingOccurrences(of: "_", with: " "), detail: selection.artifactID,
                        references: [selection.artifactID], isFailure: false)
                }
        }
        // reason 分支表示停止或重验建议；它描述审计状态，不改变 Lesson 生命周期。
        guard let reason = payload.reason else { return [] }
        return [ResearchAuditRowPresentation(id: artifactID,
            title: producer == "research.revision.stop" ? "修订已停止" : "需要重新验证 Lesson",
            detail: reason == "unchanged_rejected_scopes" ? "连续两轮问题、引用和被拒绝内容没有进展；Decision 仍被阻断。" : "来源已有更新；当前记录是重验建议，未改变 Lesson 生命周期。",
            references: [], isFailure: producer == "research.revision.stop")]
    }
}
