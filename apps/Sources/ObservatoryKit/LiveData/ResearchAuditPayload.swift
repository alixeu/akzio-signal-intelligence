import Foundation

struct ResearchAuditPayload: Decodable, Sendable {
    let version: Int
    let records: [ResearchAuditRecordPayload]
}

struct ResearchAuditRecordPayload: Decodable, Sendable {
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
    let reason: String?
    let assessments: [ResearchAssessmentPayload]?
    let records: [ResearchSelectionPayload]?
}

struct ResearchAssessmentPayload: Decodable, Sendable {
    let scope: String
    let accepted: Bool
    let rationale: String
    let issues: [ResearchIssuePayload]?
}

struct ResearchIssuePayload: Decodable, Sendable {
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
    let artifactID: String
    enum CodingKeys: String, CodingKey { case artifactID = "artifact_id" }
}

struct ResearchSelectionPayload: Decodable, Sendable {
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
        if let assessments = payload.assessments {
            return assessments.flatMap { assessment -> [ResearchAuditRowPresentation] in
                let title = "\(assessment.scope) · \(assessment.accepted ? "通过" : "需修订")"
                let issues = assessment.issues ?? []
                if issues.isEmpty {
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
        guard let reason = payload.reason else { return [] }
        return [ResearchAuditRowPresentation(id: artifactID,
            title: producer == "research.revision.stop" ? "修订已停止" : "需要重新验证 Lesson",
            detail: reason == "unchanged_rejected_scopes" ? "连续两轮问题、引用和被拒绝内容没有进展；Decision 仍被阻断。" : "来源已有更新；当前记录是重验建议，未改变 Lesson 生命周期。",
            references: [], isFailure: producer == "research.revision.stop")]
    }
}
