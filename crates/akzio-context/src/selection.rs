use super::*;

pub(super) fn context_rank(artifact: &Artifact) -> u8 {
    match artifact.kind {
        ArtifactKind::NormalizedEvidence => 0,
        ArtifactKind::SemanticDetail => 1,
        ArtifactKind::Claim | ArtifactKind::Critique => 2,
        ArtifactKind::Lesson
        | ArtifactKind::Retrospective
        | ArtifactKind::Experience
        | ArtifactKind::CandidatePolicy
        | ArtifactKind::Evaluation => 3,
        _ => 4,
    }
}

pub(super) fn is_trace_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

pub(super) fn is_safe_deliberation_summary(kind: ArtifactKind) -> bool {
    kind == ArtifactKind::DeliberationNote
}

pub(super) fn overlay_state_is_eligible(kind: ArtifactKind, state: PolicyState) -> bool {
    state.permits_influence_kind(kind)
}

#[cfg(test)]
pub(super) fn projection_artifact_ids(projection: &ContextProjection) -> Vec<ArtifactId> {
    projection
        .allowed
        .iter()
        .map(|reference| reference.artifact_id.clone())
        .collect()
}

pub(super) fn derive_child_projection(
    proof: &SucceededAttemptProof,
    parent_manifest: ArtifactRef,
    child_contract: &AgentContract,
) -> ContextProjection {
    let policy = &child_contract.context;
    let mut allowed = BTreeSet::new();
    for output in &proof.outputs {
        if output.kind != ArtifactKind::RawEvidence
            && output.kind != ArtifactKind::ContextManifest
            && !is_trace_kind(output.kind)
            && policy.permitted_kinds.contains(&output.kind)
            && (policy.permitted_source_families.is_empty()
                || policy
                    .permitted_source_families
                    .contains(&output.provenance.source_family))
        {
            allowed.insert(ArtifactRef {
                artifact_id: output.artifact_id.clone(),
                kind: output.kind,
            });
        }
        if policy
            .permitted_kinds
            .contains(&ArtifactKind::DeliberationNote)
        {
            allowed.extend(
                output
                    .source_refs
                    .iter()
                    .filter(|source| is_safe_deliberation_summary(source.kind))
                    .cloned(),
            );
        }
    }

    ContextProjection {
        parent_manifest,
        allowed: allowed.into_iter().collect(),
        reason: "parent_attempt_projection".to_owned(),
    }
}

pub(super) fn selection_reason(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::NormalizedEvidence => "normalized_evidence",
        ArtifactKind::SemanticDetail => "semantic_detail",
        ArtifactKind::Claim => "claim",
        ArtifactKind::Critique => "critique",
        ArtifactKind::Experience => "experience",
        ArtifactKind::Lesson => "lesson",
        ArtifactKind::Retrospective => "retrospective",
        ArtifactKind::CandidatePolicy => "candidate_policy",
        ArtifactKind::Evaluation => "evaluation",
        _ => "contract_permitted",
    }
}

pub(super) fn manifest_input_hash(
    selections: &[ContextSelection],
) -> Result<akzio_domain::ContentHash, serde_json::Error> {
    content_hash_json(&serde_json::to_value(
        selections
            .iter()
            .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
            .collect::<Vec<_>>(),
    )?)
}

pub(super) fn estimate_tokens(bytes: u64) -> u32 {
    akzio_domain::estimate_tokens_from_bytes(bytes)
}
