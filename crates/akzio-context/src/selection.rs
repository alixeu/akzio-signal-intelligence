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

pub(super) fn purpose_rank(purpose: &str, artifact: &Artifact) -> u8 {
    match purpose {
        RESEARCH_CRITIC_RECIPE_ID => match artifact.kind {
            ArtifactKind::Claim => 0,
            ArtifactKind::NormalizedEvidence => 1,
            ArtifactKind::SemanticDetail => 2,
            ArtifactKind::DeliberationNote => 3,
            _ => 4,
        },
        RESEARCH_SYNTHESIZER_RECIPE_ID => match artifact.kind {
            ArtifactKind::Critique => 0,
            ArtifactKind::Claim => 1,
            ArtifactKind::NormalizedEvidence => 2,
            ArtifactKind::SemanticDetail => 3,
            ArtifactKind::DeliberationNote => 4,
            _ => 5,
        },
        _ => context_rank(artifact),
    }
}

pub(super) const fn context_trust(kind: ArtifactKind) -> ContextTrust {
    match kind {
        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail => {
            ContextTrust::UntrustedEvidence
        }
        _ => ContextTrust::GovernedArtifact,
    }
}

pub(super) fn instruction_indicators(value: &Value) -> Vec<String> {
    let mut strings = Vec::new();
    collect_strings(value, &mut strings);
    let mut indicators = BTreeSet::new();
    for text in strings {
        let normalized = text.to_ascii_lowercase();
        for indicator in [
            "ignore previous",
            "ignore all previous",
            "ignore prior instructions",
            "system prompt",
            "developer message",
            "reveal credentials",
            "reveal secrets",
            "call the tool",
            "execute tool",
            "run command",
            "override risk",
            "bypass risk",
            "disable risk",
            "ignore risk",
            "place an order",
            "buy soxl",
            "sell soxl",
        ] {
            if normalized.contains(indicator) {
                indicators.insert(indicator.to_owned());
            }
        }
    }
    indicators.into_iter().take(8).collect()
}

pub(super) fn is_trace_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

pub(super) fn is_safe_deliberation_summary(kind: ArtifactKind) -> bool {
    matches!(kind, ArtifactKind::DeliberationNote | ArtifactKind::Claim)
}

pub(super) fn overlay_state_is_eligible(kind: ArtifactKind, state: PolicyState) -> bool {
    state.permits_influence_kind(kind)
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
        allowed.extend(
            output
                .source_refs
                .iter()
                .filter(|source| {
                    is_safe_deliberation_summary(source.kind)
                        && policy.permitted_kinds.contains(&source.kind)
                })
                .cloned(),
        );
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
