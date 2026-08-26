//! Typed v2 DecisionGate.
//!
//! The model produces only a schema-bounded `DecisionDraft`. Rust reloads the
//! persisted manifest closure, binds the draft to the run, and atomically
//! commits the resulting `DecisionContext` and `Decision`.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    content_hash_json, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactRef, Asset,
    CandidatePolicy, ContextManifestPayload, Decision, DecisionContext, DecisionDraft,
    DecisionHorizon, DomainError, Experience, Forecast, HardBlocker, PolicySubject, RunPurpose,
    TargetPortfolio, TaskStatus, TaskWritePermit, WeightPpm, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, ArtifactProvenance};

#[derive(Debug, Error)]
pub enum DecisionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision proposal provenance is invalid")]
    InvalidProposalProvenance,
    #[error("decision proposal must retain exactly one ContextManifest")]
    InvalidManifestReference,
    #[error("decision ContextManifest closure is invalid")]
    InvalidManifestClosure,
    #[error("decision proposal reference {0} is outside its ContextManifest")]
    ReferenceOutsideManifest(ArtifactId),
    #[error("policy influence {0} is not eligible")]
    InvalidPolicyInfluence(ArtifactId),
    #[error("learning artifact {0} was selected but not explicitly applied or rejected")]
    MissingLearningAttribution(ArtifactId),
}

pub type DecisionGateResult<T> = std::result::Result<T, DecisionGateError>;

#[derive(Debug, Clone)]
pub struct DecisionGateInput {
    pub permit: TaskWritePermit,
    pub proposal: ArtifactRef,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DecisionGateOutput {
    pub decision_context: Artifact,
    pub decision: Artifact,
}

/// Rust-owned conversion from schema-bounded forecasts to target exposure.
///
/// The synthesizer can only supply forecasts and confidence. This policy is
/// configured by Rust, hashed into every DecisionContext, and is the sole
/// authority that creates portfolio weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicy {
    pub min_confidence_ppm: u32,
    pub max_gross_weight: WeightPpm,
    pub horizon_weights: BTreeMap<DecisionHorizon, WeightPpm>,
}

impl DecisionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.min_confidence_ppm > WeightPpm::SCALE
            || self.max_gross_weight.0 > WeightPpm::SCALE
            || self.horizon_weights.len() != 3
            || [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .any(|horizon| !self.horizon_weights.contains_key(&horizon))
            || self
                .horizon_weights
                .values()
                .any(|weight| weight.0 > WeightPpm::SCALE)
            || self
                .horizon_weights
                .values()
                .try_fold(0_u32, |sum, weight| sum.checked_add(weight.0))
                != Some(WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_policy",
            });
        }
        Ok(())
    }

    pub fn policy_hash(&self) -> Result<akzio_domain::ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn target_for(
        &self,
        confidence_ppm: u32,
        forecasts: &[Forecast],
    ) -> Result<TargetPortfolio, DomainError> {
        self.validate()?;
        if confidence_ppm > WeightPpm::SCALE {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        if confidence_ppm < self.min_confidence_ppm {
            return Ok(TargetPortfolio::zeroed());
        }

        let mut scores = BTreeMap::new();
        for asset in Asset::EXECUTABLE {
            let score = forecasts
                .iter()
                .filter(|forecast| forecast.asset == asset)
                .try_fold(0_i128, |total, forecast| {
                    let weight = i128::from(self.horizon_weights[&forecast.horizon].0);
                    let probability_signal = i128::from(forecast.positive_return_probability_ppm)
                        .saturating_mul(2)
                        .saturating_sub(i128::from(WeightPpm::SCALE));
                    let return_signal = i128::from(forecast.expected_return_ppm)
                        .clamp(-i128::from(WeightPpm::SCALE), i128::from(WeightPpm::SCALE));
                    Ok::<_, DomainError>(
                        total.saturating_add(
                            probability_signal
                                .saturating_add(return_signal)
                                .saturating_mul(weight)
                                / i128::from(WeightPpm::SCALE),
                        ),
                    )
                })?;
            scores.insert(asset, score.max(0));
        }

        let total_score = scores.values().copied().sum::<i128>();
        let strongest_signal = scores.values().copied().max().unwrap_or_default();
        if total_score == 0 || strongest_signal == 0 {
            return Ok(TargetPortfolio::zeroed());
        }

        let confidence_scale = if self.min_confidence_ppm == WeightPpm::SCALE {
            WeightPpm::SCALE
        } else {
            (u64::from(confidence_ppm - self.min_confidence_ppm) * u64::from(WeightPpm::SCALE)
                / u64::from(WeightPpm::SCALE - self.min_confidence_ppm)) as u32
        };
        let signal_scale = strongest_signal.min(i128::from(WeightPpm::SCALE));
        let gross = i128::from(self.max_gross_weight.0)
            .saturating_mul(i128::from(confidence_scale))
            .saturating_mul(signal_scale)
            / i128::from(WeightPpm::SCALE)
            / i128::from(WeightPpm::SCALE);
        let mut target = TargetPortfolio::zeroed();
        for asset in Asset::EXECUTABLE {
            let weight = gross.saturating_mul(scores[&asset]) / total_score;
            target.weights.insert(
                asset,
                WeightPpm(
                    u32::try_from(weight).map_err(|_| DomainError::InvalidBudget {
                        field: "decision_policy.target",
                    })?,
                ),
            );
        }
        target.validate_universe()?;
        Ok(target)
    }
}

impl Default for DecisionPolicy {
    fn default() -> Self {
        Self {
            min_confidence_ppm: 250_000,
            max_gross_weight: WeightPpm(500_000),
            horizon_weights: BTreeMap::from([
                (DecisionHorizon::T1, WeightPpm(333_333)),
                (DecisionHorizon::T3, WeightPpm(333_333)),
                (DecisionHorizon::T5, WeightPpm(333_334)),
            ]),
        }
    }
}

#[derive(Debug, Clone)]
pub struct V2DecisionRuntime {
    store: V2Store,
    policy: DecisionPolicy,
}

impl V2DecisionRuntime {
    pub fn new(store: V2Store, policy: DecisionPolicy) -> DecisionGateResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &DecisionPolicy {
        &self.policy
    }

    /// Validate, bind, and atomically complete the DecisionGate attempt.
    pub fn decide(&self, input: &DecisionGateInput) -> DecisionGateResult<DecisionGateOutput> {
        self.store.validate_task_permit(&input.permit)?;

        let proposal = self.load_expected(&input.proposal, ArtifactKind::DecisionProposal)?;
        let proposal_contract = self.validate_proposal(&proposal, &input.permit)?;
        let manifest_ref = unique_manifest_ref(&proposal)?;
        let manifest = self.load_expected(manifest_ref, ArtifactKind::ContextManifest)?;
        let selected =
            self.validate_manifest(&manifest, &proposal, &proposal_contract, &input.permit)?;

        let draft: DecisionDraft = serde_json::from_slice(&self.store.read_blob(&proposal.blob)?)?;
        draft.validate()?;
        self.validate_draft_closure(&draft, &selected)?;

        let policy_influences = draft
            .applied_learning_refs
            .iter()
            .filter(|reference| {
                matches!(
                    reference.kind,
                    ArtifactKind::Experience | ArtifactKind::CandidatePolicy
                )
            })
            .map(|reference| {
                self.validate_policy_influence(reference)?;
                Ok(reference.clone())
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;

        let mut hard_blockers = draft.hard_blockers.iter().copied().collect::<BTreeSet<_>>();
        if !draft.material_conflicts.is_empty() {
            hard_blockers.insert(HardBlocker::MaterialConflict);
        }

        let policy_hash = self.policy.policy_hash()?;
        let target = self
            .policy
            .target_for(draft.confidence_ppm, &draft.forecasts)?;
        let context_payload = DecisionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            decision_id: akzio_domain::DecisionId::new(),
            run_id: input.permit.run_id.clone(),
            claims: draft.claims.clone(),
            critiques: draft.critiques.clone(),
            evidence: draft.evidence.clone(),
            policy_influences,
            applied_learning_refs: draft.applied_learning_refs.clone(),
            rejected_learning_refs: draft.rejected_learning_refs.clone(),
            material_conflicts: draft.material_conflicts.clone(),
            hard_blockers: hard_blockers.into_iter().collect(),
            soft_warnings: draft.soft_warnings.clone(),
            decision_policy_hash: policy_hash,
            target: target.clone(),
            created_at: input.now,
        };
        context_payload.validate()?;

        let lifecycle = match self.store.run_purpose(&input.permit.run_id)? {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            _ => ArtifactLifecycle::RunScoped,
        };
        let mut context_sources = Vec::with_capacity(selected.len() + 2);
        context_sources.push(input.proposal.clone());
        context_sources.push(manifest_ref.clone());
        context_sources.extend(selected.iter().cloned());
        let decision_context = self.artifact(
            ArtifactKind::DecisionContext,
            "decision.context",
            &context_payload,
            lifecycle,
            context_sources,
            input,
        )?;
        let context_ref = ArtifactRef {
            artifact_id: decision_context.artifact_id.clone(),
            kind: ArtifactKind::DecisionContext,
        };
        let decision_payload = Decision {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            decision_context: context_ref.clone(),
            summary: draft.summary,
            targets: target,
            confidence_ppm: draft.confidence_ppm,
            forecasts: draft.forecasts,
            created_at: input.now,
        };
        decision_payload.validate()?;
        let decision = self.artifact(
            ArtifactKind::Decision,
            "decision.bound",
            &decision_payload,
            lifecycle,
            vec![context_ref, input.proposal.clone()],
            input,
        )?;

        self.store.commit_attempt(
            &input.permit,
            &[decision_context.clone(), decision.clone()],
            TaskStatus::Succeeded,
            input.now,
        )?;
        Ok(DecisionGateOutput {
            decision_context,
            decision,
        })
    }

    fn validate_proposal(
        &self,
        proposal: &Artifact,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<akzio_domain::ContentHash> {
        let Some(origin) = proposal.origin.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        let Some(contract_hash) = origin.contract_hash.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        if proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal.producer != "agent.research.synthesizer"
            || proposal.provenance.source_family != "akzio.agent"
            || proposal.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id.is_none()
            || origin.attempt_id.is_none()
        {
            return Err(DecisionGateError::InvalidProposalProvenance);
        }
        Ok(contract_hash.clone())
    }

    fn validate_manifest(
        &self,
        manifest: &Artifact,
        proposal: &Artifact,
        contract_hash: &akzio_domain::ContentHash,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<BTreeSet<ArtifactRef>> {
        let proposal_origin = proposal
            .origin
            .as_ref()
            .ok_or(DecisionGateError::InvalidProposalProvenance)?;
        let Some(origin) = manifest.origin.as_ref() else {
            return Err(DecisionGateError::InvalidManifestClosure);
        };
        if manifest.lifecycle != ArtifactLifecycle::RunScoped
            || manifest.producer != "context.research.synthesizer"
            || manifest.provenance.source_family != "akzio.context"
            || manifest.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id != proposal_origin.task_id
            || origin.attempt_id != proposal_origin.attempt_id
            || origin.contract_hash.as_ref() != Some(contract_hash)
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let payload: ContextManifestPayload =
            serde_json::from_slice(&self.store.read_blob(&manifest.blob)?)?;
        if payload.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || payload.contract_hash != *contract_hash
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        self.validate_manifest_source_closure(manifest, &payload, permit, &mut BTreeSet::new())?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        Ok(selected)
    }

    fn validate_manifest_source_closure(
        &self,
        manifest: &Artifact,
        payload: &ContextManifestPayload,
        permit: &TaskWritePermit,
        visiting: &mut BTreeSet<ArtifactId>,
    ) -> DecisionGateResult<()> {
        if !visiting.insert(manifest.artifact_id.clone()) {
            return Err(DecisionGateError::InvalidManifestClosure);
        }
        if payload.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let ancestors = manifest
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .cloned()
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut expected = selected.clone();
        expected.extend(ancestors.iter().cloned());
        if selected.len() != payload.selections.len()
            || declared.len() != manifest.source_refs.len()
            || declared != expected
            || payload.input_hash != manifest_input_hash(&payload.selections)?
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for selection in &payload.selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            if artifact.kind != selection.artifact.kind
                || matches!(
                    artifact.kind,
                    ArtifactKind::RawEvidence
                        | ArtifactKind::AgentTurn
                        | ArtifactKind::ToolCall
                        | ArtifactKind::ToolResult
                )
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let tokens = estimate_tokens(artifact.blob.bytes);
            if tokens != selection.estimated_tokens {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
        }
        if total_bytes != payload.total_bytes || estimated_tokens != payload.estimated_tokens {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        for parent_ref in ancestors {
            if selected.contains(&parent_ref) {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent = self.load_expected(&parent_ref, ArtifactKind::ContextManifest)?;
            let Some(origin) = parent.origin.as_ref() else {
                return Err(DecisionGateError::InvalidManifestClosure);
            };
            if parent.lifecycle != ArtifactLifecycle::RunScoped
                || !parent.producer.starts_with("context.")
                || parent.provenance.source_family != "akzio.context"
                || origin.run_id.as_ref() != Some(&permit.run_id)
                || origin.task_id.is_none()
                || origin.attempt_id.is_none()
                || origin.contract_hash.is_none()
                || parent.provenance.producer_contract_hash != origin.contract_hash
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent_payload: ContextManifestPayload =
                serde_json::from_slice(&self.store.read_blob(&parent.blob)?)?;
            if parent_payload.contract_hash != origin.contract_hash.clone().unwrap() {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            self.validate_manifest_source_closure(&parent, &parent_payload, permit, visiting)?;
        }
        visiting.remove(&manifest.artifact_id);
        Ok(())
    }

    fn validate_draft_closure(
        &self,
        draft: &DecisionDraft,
        selected: &BTreeSet<ArtifactRef>,
    ) -> DecisionGateResult<()> {
        for reference in draft
            .claims
            .iter()
            .chain(draft.critiques.iter())
            .chain(draft.evidence.iter())
            .chain(draft.applied_learning_refs.iter())
            .chain(draft.rejected_learning_refs.iter())
            .chain(
                draft
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            if !selected.contains(reference) {
                return Err(DecisionGateError::ReferenceOutsideManifest(
                    reference.artifact_id.clone(),
                ));
            }
        }
        for reference in selected.iter().filter(|reference| {
            matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience
            )
        }) {
            if !draft.applied_learning_refs.contains(reference)
                && !draft.rejected_learning_refs.contains(reference)
            {
                return Err(DecisionGateError::MissingLearningAttribution(
                    reference.artifact_id.clone(),
                ));
            }
        }
        Ok(())
    }

    fn validate_policy_influence(&self, reference: &ArtifactRef) -> DecisionGateResult<()> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        let canonical_paper = artifact.lifecycle == ArtifactLifecycle::Canonical
            && artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                .is_some_and(|run_id| {
                    self.store
                        .run_purpose(run_id)
                        .is_ok_and(|purpose| purpose == RunPurpose::Paper)
                });
        if artifact.kind != reference.kind || !canonical_paper {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }

        let subject = match artifact.kind {
            ArtifactKind::Experience => {
                let payload: Experience =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            ArtifactKind::CandidatePolicy => {
                let payload: CandidatePolicy =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            _ => {
                return Err(DecisionGateError::InvalidPolicyInfluence(
                    reference.artifact_id.clone(),
                ));
            }
        };
        self.require_active_policy_influence(reference, &subject)
    }

    fn require_active_policy_influence(
        &self,
        reference: &ArtifactRef,
        subject: &PolicySubject,
    ) -> DecisionGateResult<()> {
        if self
            .store
            .recorded_policy_influence_subject(&reference.artifact_id)?
            .is_some_and(|recorded| recorded != *subject)
            || !self
                .store
                .policy_head(subject)?
                .is_some_and(|head| head.state.permits_influence_kind(reference.kind))
        {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> DecisionGateResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(DecisionGateError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        lifecycle: ArtifactLifecycle,
        source_refs: Vec<ArtifactRef>,
        input: &DecisionGateInput,
    ) -> DecisionGateResult<Artifact> {
        Ok(Artifact::new(
            kind,
            self.store.put_json(payload)?,
            producer,
            lifecycle,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}

fn unique_manifest_ref(proposal: &Artifact) -> DecisionGateResult<&ArtifactRef> {
    let mut manifests = proposal
        .source_refs
        .iter()
        .filter(|reference| reference.kind == ArtifactKind::ContextManifest);
    let manifest = manifests
        .next()
        .ok_or(DecisionGateError::InvalidManifestReference)?;
    if manifests.next().is_some() {
        return Err(DecisionGateError::InvalidManifestReference);
    }
    Ok(manifest)
}

fn manifest_input_hash(
    selections: &[akzio_domain::ContextSelection],
) -> Result<akzio_domain::ContentHash, serde_json::Error> {
    content_hash_json(&serde_json::to_value(
        selections
            .iter()
            .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
            .collect::<Vec<_>>(),
    )?)
}

fn estimate_tokens(bytes: u64) -> u32 {
    akzio_domain::estimate_tokens_from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        ArtifactLifecycle, Asset, ContentHash, ContextSelection, DecisionHorizon,
        FailureDisposition, Forecast, LifecycleEventType, RetryPolicy, RunId, TaskBudget, TaskId,
        TaskRecipeId, WeightPpm, WorkflowGraph, WorkflowNode,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use chrono::Duration;
    use tempfile::tempdir;

    use super::*;

    #[derive(Clone, Copy)]
    enum DraftMode {
        Accepted,
        Blocked,
        ForgedReference,
    }

    struct GateCase {
        permit: TaskWritePermit,
        proposal: ArtifactRef,
    }

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 128,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        }
    }

    fn node(
        recipe: &str,
        dependencies: Vec<TaskId>,
        contract_hash: Option<ContentHash>,
        priority: u8,
    ) -> WorkflowNode {
        WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new(recipe).unwrap(),
            contract_hash,
            objective: recipe.to_owned(),
            dependencies,
            input_artifacts: vec![],
            priority,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        }
    }

    fn provenance(
        source_family: &str,
        contract_hash: Option<ContentHash>,
        now: DateTime<Utc>,
    ) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: source_family.to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: contract_hash,
        }
    }

    fn origin(permit: &TaskWritePermit) -> ArtifactOrigin {
        ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }
    }

    fn reference(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn decision_policy() -> DecisionPolicy {
        DecisionPolicy {
            min_confidence_ppm: 250_000,
            max_gross_weight: WeightPpm(500_000),
            horizon_weights: std::collections::BTreeMap::from([
                (DecisionHorizon::T1, WeightPpm(333_333)),
                (DecisionHorizon::T3, WeightPpm(333_333)),
                (DecisionHorizon::T5, WeightPpm(333_334)),
            ]),
        }
    }

    fn forecasts() -> Vec<Forecast> {
        Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    DecisionHorizon::T1,
                    DecisionHorizon::T3,
                    DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: if asset == Asset::Tqqq {
                        800_000
                    } else {
                        400_000
                    },
                    expected_return_ppm: if asset == Asset::Tqqq {
                        100_000
                    } else {
                        -100_000
                    },
                })
            })
            .collect()
    }

    #[test]
    fn rust_owned_policy_derives_target_from_forecasts_without_model_weights() {
        let policy = decision_policy();
        let target = policy.target_for(500_000, &forecasts()).unwrap();

        assert!(target.weights[&Asset::Tqqq].0 > 0);
        assert_eq!(target.weights[&Asset::Qqq], WeightPpm::ZERO);
        assert_eq!(target.weights[&Asset::Soxx], WeightPpm::ZERO);
        assert_eq!(target.weights[&Asset::Soxl], WeightPpm::ZERO);
        assert_ne!(
            policy.policy_hash().unwrap(),
            ContentHash::of_bytes(b"other-policy")
        );
    }

    fn draft(claim: &ArtifactRef, mode: DraftMode) -> DecisionDraft {
        let claims = match mode {
            DraftMode::ForgedReference => vec![ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"forged-claim")),
                kind: ArtifactKind::Claim,
            }],
            _ => vec![claim.clone()],
        };
        DecisionDraft {
            summary: "fixture decision".to_owned(),
            confidence_ppm: 500_000,
            forecasts: forecasts(),
            claims,
            critiques: vec![],
            evidence: vec![],
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
            material_conflicts: vec![],
            hard_blockers: matches!(mode, DraftMode::Blocked)
                .then_some(HardBlocker::MissingEvidence)
                .into_iter()
                .collect(),
            soft_warnings: vec![],
        }
    }

    fn seed_case(
        store: &V2Store,
        purpose: RunPurpose,
        mode: DraftMode,
        manifest_contract_matches: bool,
        include_manifest_ref: bool,
        now: DateTime<Utc>,
    ) -> GateCase {
        let contract_hash = ContentHash::of_bytes(b"synthesizer-contract");
        let source = node("fixture.source", vec![], None, 100);
        let synthesizer = node(
            "research.synthesizer",
            vec![source.task_id.clone()],
            Some(contract_hash.clone()),
            90,
        );
        let gate = node("gate.decision", vec![synthesizer.task_id.clone()], None, 80);
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: format!("decision-gate-{}", RunId::new()),
            nodes: vec![source, synthesizer, gate],
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            provenance("fixture.workflow", None, now),
            None,
            vec![],
            now,
        )
        .unwrap();
        let run = StoredRun {
            run_id: RunId::new(),
            purpose,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();

        let source_permit = store
            .claim_next_task("source", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let evidence = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store
                .put_json(&serde_json::json!({"evidence": "fixture"}))
                .unwrap(),
            "fixture.evidence",
            ArtifactLifecycle::RunScoped,
            provenance("fixture.evidence", None, now),
            Some(origin(&source_permit)),
            vec![],
            now,
        )
        .unwrap();
        let claim = Artifact::new(
            ArtifactKind::Claim,
            store
                .put_json(&serde_json::json!({"claim": "fixture"}))
                .unwrap(),
            "fixture.claim",
            ArtifactLifecycle::RunScoped,
            provenance("akzio.agent", None, now),
            Some(origin(&source_permit)),
            vec![reference(&evidence)],
            now,
        )
        .unwrap();
        store
            .commit_attempt(
                &source_permit,
                &[evidence, claim.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        let synth_permit = store
            .claim_next_task("synthesizer", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let claim_ref = reference(&claim);
        let selection = ContextSelection {
            artifact: claim_ref.clone(),
            reason: "claim".to_owned(),
            estimated_tokens: estimate_tokens(claim.blob.bytes),
        };
        let manifest_contract = if manifest_contract_matches {
            contract_hash.clone()
        } else {
            ContentHash::of_bytes(b"forged-contract")
        };
        let manifest_payload = ContextManifestPayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            contract_hash: manifest_contract,
            selections: vec![selection],
            total_bytes: claim.blob.bytes,
            estimated_tokens: estimate_tokens(claim.blob.bytes),
            input_hash: manifest_input_hash(&[ContextSelection {
                artifact: claim_ref.clone(),
                reason: "claim".to_owned(),
                estimated_tokens: estimate_tokens(claim.blob.bytes),
            }])
            .unwrap(),
        };
        let manifest = Artifact::new(
            ArtifactKind::ContextManifest,
            store.put_json(&manifest_payload).unwrap(),
            "context.research.synthesizer",
            ArtifactLifecycle::RunScoped,
            provenance("akzio.context", Some(contract_hash.clone()), now),
            Some(origin(&synth_permit)),
            vec![claim_ref.clone()],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(
                &synth_permit,
                &manifest,
                LifecycleEventType::ContextManifestCreated,
                now,
            )
            .unwrap();

        let proposal = Artifact::new(
            ArtifactKind::DecisionProposal,
            store.put_json(&draft(&claim_ref, mode)).unwrap(),
            "agent.research.synthesizer",
            ArtifactLifecycle::RunScoped,
            provenance("akzio.agent", Some(contract_hash), now),
            Some(origin(&synth_permit)),
            include_manifest_ref
                .then(|| reference(&manifest))
                .into_iter()
                .collect(),
            now,
        )
        .unwrap();
        store
            .commit_attempt(
                &synth_permit,
                std::slice::from_ref(&proposal),
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        let permit = store
            .claim_next_task("decision-gate", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        GateCase {
            permit,
            proposal: reference(&proposal),
        }
    }

    #[test]
    fn accepted_paper_proposal_commits_canonical_context_and_decision() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let case = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Accepted,
            true,
            true,
            now,
        );
        let output = V2DecisionRuntime::new(store.clone(), decision_policy())
            .unwrap()
            .decide(&DecisionGateInput {
                permit: case.permit.clone(),
                proposal: case.proposal,
                now,
            })
            .unwrap();

        assert_eq!(
            output.decision_context.lifecycle,
            ArtifactLifecycle::Canonical
        );
        assert_eq!(output.decision.lifecycle, ArtifactLifecycle::Canonical);
        let context: DecisionContext =
            serde_json::from_slice(&store.read_blob(&output.decision_context.blob).unwrap())
                .unwrap();
        assert!(context.accepted());
        assert_eq!(
            context.decision_policy_hash,
            decision_policy().policy_hash().unwrap()
        );
        assert!(context.target.weights[&Asset::Tqqq].0 > 0);
        let decision: Decision =
            serde_json::from_slice(&store.read_blob(&output.decision.blob).unwrap()).unwrap();
        assert_eq!(
            decision.decision_context,
            reference(&output.decision_context)
        );
        store
            .verify_attempt_terminal(&case.permit, TaskStatus::Succeeded)
            .unwrap();
    }

    #[test]
    fn model_blocker_is_preserved_and_cannot_create_acceptance() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let case = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Blocked,
            true,
            true,
            now,
        );
        let output = V2DecisionRuntime::new(store.clone(), decision_policy())
            .unwrap()
            .decide(&DecisionGateInput {
                permit: case.permit,
                proposal: case.proposal,
                now,
            })
            .unwrap();
        let context: DecisionContext =
            serde_json::from_slice(&store.read_blob(&output.decision_context.blob).unwrap())
                .unwrap();

        assert!(!context.accepted());
        assert_eq!(context.hard_blockers, vec![HardBlocker::MissingEvidence]);
    }

    #[test]
    fn forged_reference_manifest_contract_and_run_are_rejected() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let runtime = V2DecisionRuntime::new(store.clone(), decision_policy()).unwrap();

        let forged = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::ForgedReference,
            true,
            true,
            now,
        );
        assert!(matches!(
            runtime.decide(&DecisionGateInput {
                permit: forged.permit,
                proposal: forged.proposal,
                now,
            }),
            Err(DecisionGateError::ReferenceOutsideManifest(_))
        ));

        let no_manifest = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Accepted,
            true,
            false,
            now,
        );
        assert!(matches!(
            runtime.decide(&DecisionGateInput {
                permit: no_manifest.permit,
                proposal: no_manifest.proposal,
                now,
            }),
            Err(DecisionGateError::InvalidManifestReference)
        ));

        let bad_contract = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Accepted,
            false,
            true,
            now,
        );
        assert!(matches!(
            runtime.decide(&DecisionGateInput {
                permit: bad_contract.permit,
                proposal: bad_contract.proposal,
                now,
            }),
            Err(DecisionGateError::InvalidManifestClosure)
        ));

        let source_run = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Accepted,
            true,
            true,
            now,
        );
        let target_run = seed_case(
            &store,
            RunPurpose::Paper,
            DraftMode::Accepted,
            true,
            true,
            now,
        );
        assert!(matches!(
            runtime.decide(&DecisionGateInput {
                permit: target_run.permit,
                proposal: source_run.proposal,
                now,
            }),
            Err(DecisionGateError::InvalidProposalProvenance)
        ));
    }

    #[test]
    fn nonpaper_decisions_remain_run_scoped() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let case = seed_case(
            &store,
            RunPurpose::Debug,
            DraftMode::Accepted,
            true,
            true,
            now,
        );
        let output = V2DecisionRuntime::new(store, decision_policy())
            .unwrap()
            .decide(&DecisionGateInput {
                permit: case.permit,
                proposal: case.proposal,
                now,
            })
            .unwrap();

        assert_eq!(
            output.decision_context.lifecycle,
            ArtifactLifecycle::RunScoped
        );
        assert_eq!(output.decision.lifecycle, ArtifactLifecycle::RunScoped);
    }

    #[test]
    fn selected_learning_requires_explicit_attribution() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime = V2DecisionRuntime::new(store, decision_policy()).unwrap();
        let claim = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"claim")),
            kind: ArtifactKind::Claim,
        };
        let lesson = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"lesson")),
            kind: ArtifactKind::Lesson,
        };
        let selected = BTreeSet::from([claim.clone(), lesson.clone()]);
        let draft_without_attribution = draft(&claim, DraftMode::Accepted);

        assert!(matches!(
            runtime.validate_draft_closure(&draft_without_attribution, &selected),
            Err(DecisionGateError::MissingLearningAttribution(artifact_id))
                if artifact_id == lesson.artifact_id
        ));

        let mut draft_with_rejection = draft_without_attribution;
        draft_with_rejection.rejected_learning_refs.push(lesson);
        runtime
            .validate_draft_closure(&draft_with_rejection, &selected)
            .unwrap();
    }
}
