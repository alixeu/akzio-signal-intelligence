//! Canonical, outcome-backed learning runtime for Akzio v2.
//!
//! Callers provide governed observations, never precomputed learning metrics.
//! Rust materializes T+1/T+3/T+5 windows and Store-owned run purpose remains
//! the canonicality authority.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use thiserror::Error;

use akzio_domain::{
    content_hash_json, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, CandidatePolicy, CandidatePolicyState, ContentHash,
    DecisionHorizon, DomainError, Evaluation, EvaluationId, Experience, ExperienceId, Forecast,
    Lesson, LessonId, LessonLifecycle, LessonOrigin, LessonScope, MemoryLifecycle, MoneyMicros,
    Outcome, OutcomeCostModel, OutcomeExecutionLineage, OutcomeHorizon, OutcomeSchedule,
    OutcomeWindow, PolicyState, PolicySubject, PolicyTransition, PolicyTransitionId, Retrospective,
    RetrospectiveDraft, RetrospectiveStatus, RunPurpose, TargetPortfolio, TaskWritePermit,
    TopologyId, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{
    DaemonLease, PolicyEvaluationCommit, PolicyHead, ShadowPairCompletion, ShadowPairWriteResult,
    StoreError, V2Store,
};

const PPM_ONE: u32 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationPolicy {
    pub minimum_evidence_completeness_ppm: u32,
    pub minimum_risk_recall_ppm: u32,
    pub minimum_fresh_pairs_per_horizon: u64,
}

impl Default for EvaluationPolicy {
    fn default() -> Self {
        Self {
            minimum_evidence_completeness_ppm: 900_000,
            minimum_risk_recall_ppm: 900_000,
            minimum_fresh_pairs_per_horizon: 1,
        }
    }
}

impl EvaluationPolicy {
    pub fn outcome_is_degraded(&self, outcome: &Outcome) -> bool {
        outcome.windows.iter().any(|window| {
            window.evidence_completeness_ppm < self.minimum_evidence_completeness_ppm
                || window
                    .risk_recall_ppm
                    .is_none_or(|value| value < self.minimum_risk_recall_ppm)
        })
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        if self.minimum_evidence_completeness_ppm > PPM_ONE
            || self.minimum_risk_recall_ppm > PPM_ONE
            || self.minimum_fresh_pairs_per_horizon == 0
        {
            return Err(EvaluationError::InvalidPolicy);
        }
        Ok(())
    }
}

/// One governed future price surface for a due schedule horizon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedHorizonObservation {
    pub horizon: OutcomeHorizon,
    pub completed_trading_sessions: u8,
    pub observed_trading_day: NaiveDate,
    pub future_prices: BTreeMap<Asset, MoneyMicros>,
    pub expected_evidence_count: u64,
    pub observed_evidence_count: u64,
    pub expected_risk_count: u64,
    pub detected_risk_count: Option<u64>,
}

pub fn horizon_observations(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    common_dates: &[NaiveDate],
    expected_risk_count: u64,
) -> EvaluationRuntimeResult<Vec<GovernedHorizonObservation>> {
    let completed_sessions = u8::try_from(common_dates.len()).unwrap_or(u8::MAX);
    OutcomeHorizon::ALL
        .into_iter()
        .filter(|horizon| horizon.is_due_after(completed_sessions))
        .map(|horizon| {
            let index = usize::from(horizon.trading_days()) - 1;
            let observed_trading_day = *common_dates
                .get(index)
                .ok_or(EvaluationError::UnalignedBars)?;
            let future_prices =
                Asset::EXECUTABLE
                    .into_iter()
                    .try_fold(BTreeMap::new(), |mut prices, asset| {
                        let price = bars_by_asset
                            .get(&asset)
                            .and_then(|bars| bars.get(&observed_trading_day))
                            .copied()
                            .ok_or(EvaluationError::UnalignedBars)?;
                        prices.insert(asset, price);
                        Ok::<_, EvaluationError>(prices)
                    })?;
            Ok(GovernedHorizonObservation {
                horizon,
                completed_trading_sessions: completed_sessions,
                observed_trading_day,
                future_prices,
                expected_evidence_count: Asset::EXECUTABLE.len() as u64,
                observed_evidence_count: Asset::EXECUTABLE.len() as u64,
                expected_risk_count,
                detected_risk_count: None,
            })
        })
        .collect()
}

/// Raw inputs from which Rust deterministically materializes a sealed Outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeMaterializationInput {
    pub schedule: OutcomeSchedule,
    pub schedule_artifact: ArtifactRef,
    pub target: TargetPortfolio,
    pub forecasts: Vec<Forecast>,
    pub baseline_prices: BTreeMap<Asset, MoneyMicros>,
    pub observations: Vec<GovernedHorizonObservation>,
    pub market_evidence: Vec<ArtifactRef>,
    pub cost_model: OutcomeCostModel,
    pub sealed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowObservation {
    pub parent_decision: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub candidate_decision: ArtifactRef,
    pub candidate_contract_hash: ContentHash,
    pub candidate_topology_id: String,
    pub horizon: OutcomeHorizon,
    pub parent_outcome: ArtifactRef,
    pub candidate_outcome: ArtifactRef,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidatePolicyInput {
    pub baseline: ArtifactRef,
    pub candidate: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationInput {
    pub permit: TaskWritePermit,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub materialization: OutcomeMaterializationInput,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub candidate_policy: Option<CandidatePolicyInput>,
    pub token_cost: Option<u64>,
    pub latency_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResult {
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub evaluation: ArtifactRef,
    pub candidate_policy: Option<ArtifactRef>,
    pub policy_head: Option<PolicyHead>,
    pub fresh_pairs_by_horizon: [u64; 3],
}

#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("canonical learning rejects non-Paper run purpose {0:?}")]
    NonCanonicalPurpose(RunPurpose),
    #[error("evaluation policy has an invalid threshold")]
    InvalidPolicy,
    #[error("policy subject does not match persisted state")]
    SubjectStateMismatch,
    #[error("hypothesis id must be non-empty")]
    EmptyHypothesis,
    #[error("candidate policy input invalid: {0}")]
    InvalidCandidatePolicy(&'static str),
    #[error("outcome materialization is invalid: {0}")]
    InvalidMaterialization(&'static str),
    #[error("outcome materialization arithmetic overflow")]
    ArithmeticOverflow,
    #[error("Paper outcome bars are not aligned")]
    UnalignedBars,
}

pub type EvaluationRuntimeResult<T> = Result<T, EvaluationError>;

#[derive(Debug, Clone)]
pub struct EvaluationRuntime {
    store: V2Store,
    policy: EvaluationPolicy,
}

impl EvaluationRuntime {
    pub fn new(store: V2Store, policy: EvaluationPolicy) -> EvaluationRuntimeResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &EvaluationPolicy {
        &self.policy
    }

    /// Persists a candidate/production comparison without changing policy.
    pub fn record_shadow_pair(
        &self,
        permit: &TaskWritePermit,
        subject: &PolicySubject,
        observation: ShadowObservation,
    ) -> EvaluationRuntimeResult<ShadowPairWriteResult> {
        self.require_paper(&permit.run_id)?;
        if let PolicySubject::Topology(topology_id) = subject {
            if observation.candidate_topology_id != topology_id.0 {
                return Err(EvaluationError::InvalidCandidatePolicy(
                    "shadow_topology_id",
                ));
            }
        }
        Ok(self.store.complete_shadow_pair(
            permit,
            &ShadowPairCompletion {
                subject: subject.clone(),
                parent_decision: observation.parent_decision,
                execution_context: observation.execution_context,
                candidate_decision: observation.candidate_decision,
                candidate_contract_hash: observation.candidate_contract_hash,
                candidate_topology_id: observation.candidate_topology_id,
                horizon: observation.horizon,
                parent_outcome: observation.parent_outcome,
                candidate_outcome: observation.candidate_outcome,
                completed_at: observation.completed_at,
            },
        )?)
    }

    /// Materializes governed observations, then commits immutable learning
    /// artifacts. Schedule creation is a separate earlier step.
    pub fn evaluate(&self, input: EvaluationInput) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_lease(None, input)
    }

    /// Materializes and commits learning while optionally fencing a daemon
    /// worker lease in the Store transaction.
    pub fn evaluate_with_lease(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective(lease, input, None)
    }

    pub fn evaluate_with_lease_and_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        draft: &RetrospectiveDraft,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective(lease, input, Some(draft))
    }

    pub fn evaluate_with_lease_at_state(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        target_state: PolicyState,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective_at_state(
            lease,
            input,
            retrospective_draft,
            Some(target_state),
        )
    }

    fn evaluate_with_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective_at_state(lease, input, retrospective_draft, None)
    }

    fn evaluate_with_retrospective_at_state(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        target_state: Option<PolicyState>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.require_paper(&input.permit.run_id)?;
        if input.hypothesis_id.trim().is_empty() {
            return Err(EvaluationError::EmptyHypothesis);
        }
        match (&input.subject, &input.candidate_policy) {
            (PolicySubject::Memory(_), None)
            | (PolicySubject::Contract(_), Some(_))
            | (PolicySubject::Topology(_), Some(_)) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(EvaluationError::InvalidCandidatePolicy("memory_subject"));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(EvaluationError::InvalidCandidatePolicy("missing_candidate"));
            }
        }
        let outcome = materialize_outcome(&input.materialization)?;
        outcome.validate_sealed()?;

        let previous_head = self.store.policy_head(&input.subject)?;
        let current = previous_head
            .as_ref()
            .map(|head| head.state)
            .unwrap_or_else(|| input.subject.initial_state());
        if !input.subject.accepts_state(current) {
            return Err(EvaluationError::SubjectStateMismatch);
        }

        let created_at = outcome
            .sealed_at
            .expect("materialized outcome always has sealed_at");
        let pair_snapshot = self.store.policy_shadow_pair_snapshot(&input.subject)?;
        let fresh_pairs_by_horizon = pair_snapshot.counts_by_horizon;
        let degraded = self.policy.outcome_is_degraded(&outcome);

        let origin = input.permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(&input.permit, created_at);

        let outcome_artifact = if let Some(existing) = self
            .store
            .outcome_for(&input.permit.run_id, &outcome.outcome_id)?
        {
            existing
        } else {
            let outcome_sources = std::iter::once(input.materialization.schedule_artifact.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect();
            self.artifact(
                ArtifactKind::Outcome,
                &outcome,
                outcome_sources,
                &origin,
                &provenance,
                created_at,
            )?
        };
        let outcome_ref = reference(&outcome_artifact);
        let retrospective_artifact = if let Some(existing) = self.store.retrospective_for(
            &input.permit.run_id,
            &outcome.outcome_id,
            OutcomeHorizon::T5,
        )? {
            existing
        } else {
            let mut retrospective = Retrospective {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                outcome_id: outcome.outcome_id.clone(),
                horizon: OutcomeHorizon::T5,
                status: RetrospectiveStatus::Complete,
                summary:
                    "Rust-sealed outcome retrospective; model narrative unavailable in this commit"
                        .to_owned(),
                findings: Vec::new(),
                counterfactuals: Vec::new(),
                lesson_candidates: Vec::new(),
                diagnostic_gaps: vec![
                    "governed retrospective model narrative not installed".to_owned()
                ],
                source_refs: vec![outcome_ref.clone()],
                outcome: outcome_ref.clone(),
                created_at,
                sealed_at: Some(created_at),
            };
            if let Some(draft) = retrospective_draft {
                if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
                    return Err(EvaluationError::InvalidMaterialization(
                        "retrospective draft identity",
                    ));
                }
                retrospective.summary = draft.summary.clone();
                retrospective.findings = draft.findings.clone();
                retrospective.counterfactuals = draft.counterfactuals.clone();
                retrospective.lesson_candidates = draft.lesson_candidates.clone();
                retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
                retrospective.source_refs = draft.source_refs.clone();
                retrospective.source_refs.extend(
                    draft
                        .findings
                        .iter()
                        .flat_map(|finding| finding.artifact_refs.iter().cloned()),
                );
                retrospective.source_refs.push(outcome_ref.clone());
                retrospective.source_refs.sort();
                retrospective.source_refs.dedup();
            }
            for prior in self.store.retrospectives(&input.permit.run_id)? {
                retrospective.source_refs.push(reference(&prior));
            }
            retrospective.source_refs.sort();
            retrospective.source_refs.dedup();
            retrospective.validate()?;
            self.artifact(
                ArtifactKind::Retrospective,
                &retrospective,
                retrospective.source_refs.clone(),
                &origin,
                &provenance,
                created_at,
            )?
        };
        let retrospective_ref = reference(&retrospective_artifact);
        let schedule = &input.materialization.schedule;
        let policy_verdict = execution_verdict(&schedule.execution).clone();
        let experience = Experience {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            experience_id: ExperienceId(stable_id(&serde_json::json!({
                "subject": &input.subject,
                "hypothesis_id": &input.hypothesis_id,
                "decision": &schedule.decision,
                "outcome": &outcome_ref,
                "contract_hash": &input.contract_hash,
                "topology_id": &input.topology_id,
            }))?),
            subject: input.subject.clone(),
            hypothesis_id: input.hypothesis_id.clone(),
            decision: schedule.decision.clone(),
            decision_context: schedule.decision_context.clone(),
            execution_context: schedule.execution_context.clone(),
            policy_verdict,
            outcome: outcome_ref.clone(),
            contract_hash: input.contract_hash.clone(),
            topology_id: input.topology_id.clone(),
            policy_state: current,
            created_at,
        };
        experience.validate()?;
        let experience_artifact = self.artifact(
            ArtifactKind::Experience,
            &experience,
            vec![
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
                retrospective_ref.clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let experience_ref = reference(&experience_artifact);
        let evaluation = Evaluation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            evaluation_id: EvaluationId(stable_id(&serde_json::json!({
                "subject": &input.subject,
                "outcome": &outcome_ref,
                "experience": &experience_ref,
                "candidate_policy": &input.candidate_policy,
                "token_cost": input.token_cost,
                "latency_millis": input.latency_millis,
            }))?),
            outcome: outcome_ref.clone(),
            experience: experience_ref.clone(),
            marginal_utility_ppm: marginal_utility(&outcome),
            token_cost: input.token_cost,
            latency_millis: input.latency_millis,
            created_at,
        };
        let evaluation_artifact = self.artifact(
            ArtifactKind::Evaluation,
            &evaluation,
            vec![
                evaluation.outcome.clone(),
                evaluation.experience.clone(),
                retrospective_ref.clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let evaluation_ref = reference(&evaluation_artifact);
        let candidate_policy_artifact = input
            .candidate_policy
            .as_ref()
            .map(|candidate| {
                let policy = CandidatePolicy {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    subject: input.subject.clone(),
                    baseline: candidate.baseline.clone(),
                    candidate: candidate.candidate.clone(),
                    source_evaluation: evaluation_ref.clone(),
                    created_at,
                };
                policy.validate()?;
                self.artifact(
                    ArtifactKind::CandidatePolicy,
                    &policy,
                    vec![
                        policy.baseline.clone(),
                        policy.candidate.clone(),
                        policy.source_evaluation.clone(),
                    ],
                    &origin,
                    &provenance,
                    created_at,
                )
            })
            .transpose()?;
        let candidate_policy_ref = candidate_policy_artifact.as_ref().map(reference);
        let next = target_state.unwrap_or_else(|| {
            next_state_with_fresh_pairs(current, degraded, fresh_pairs_by_horizon)
        });
        if !input.subject.accepts_state(next) {
            return Err(EvaluationError::SubjectStateMismatch);
        }

        let transition = if next == current {
            None
        } else {
            Some(PolicyTransition {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                transition_id: PolicyTransitionId(stable_id(&serde_json::json!({
                    "subject": &input.subject,
                    "from": current,
                    "to": next,
                    "evaluation": &evaluation_ref,
                }))?),
                subject: input.subject.clone(),
                from: current,
                to: next,
                evaluation: evaluation_ref.clone(),
                created_at,
            })
        };
        let retrospective_for_lessons = retrospective_artifact.clone();
        let retrospective_payload: Retrospective =
            serde_json::from_slice(&self.store.read_blob(&retrospective_for_lessons.blob)?)?;
        let policy_head = self
            .store
            .record_policy_evaluation_fenced(
                lease,
                &PolicyEvaluationCommit {
                    permit: input.permit,
                    outcome: outcome_artifact,
                    final_retrospective: retrospective_artifact,
                    experience: experience_artifact,
                    evaluation: evaluation_artifact,
                    candidate_policy: candidate_policy_artifact,
                    subject: input.subject,
                    from: current,
                    to: next,
                    pair_snapshot,
                    transition,
                    completed_at: created_at,
                },
            )?
            .policy_head;
        self.materialize_retrospective_lessons(
            &retrospective_for_lessons,
            &retrospective_payload,
            created_at,
        )?;

        Ok(EvaluationResult {
            outcome: outcome_ref,
            experience: experience_ref,
            evaluation: evaluation_ref,
            candidate_policy: candidate_policy_ref,
            policy_head,
            fresh_pairs_by_horizon,
        })
    }

    fn materialize_retrospective_lessons(
        &self,
        retrospective_artifact: &Artifact,
        retrospective: &Retrospective,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<()> {
        for (index, candidate) in retrospective.lesson_candidates.iter().enumerate() {
            let statement = candidate.trim();
            if statement.is_empty() {
                continue;
            }
            let lesson = Lesson {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                lesson_id: LessonId(stable_id(&serde_json::json!({
                    "retrospective": retrospective_artifact.artifact_id,
                    "index": index,
                    "statement": statement,
                }))?),
                origin: LessonOrigin::OutcomeDerived,
                lifecycle: LessonLifecycle::Draft,
                title: format!("Outcome lesson {}", index + 1),
                statement: statement.to_owned(),
                rationale: retrospective.summary.clone(),
                recommended_behavior: "Treat as a hypothesis until a reviewer approves it and Paper outcomes support it.".to_owned(),
                exclusions: retrospective.diagnostic_gaps.clone(),
                scope: LessonScope::default(),
                source_refs: vec![reference(retrospective_artifact)],
                supersedes: Vec::new(),
                conflicts_with: Vec::new(),
                confidence_ppm: 500_000,
                authored_by: None,
                approved_by: None,
                created_at,
                updated_at: created_at,
            };
            self.store
                .write_lesson(&lesson, retrospective_artifact, created_at)?;
        }
        Ok(())
    }

    fn require_paper(&self, run_id: &akzio_domain::RunId) -> EvaluationRuntimeResult<()> {
        require_canonical_purpose(self.store.run_purpose(run_id)?)
    }

    /// Seal an outcome and a Rust-only retrospective without creating any
    /// Experience, Evaluation, or policy influence.
    pub fn seal_outcome_with_rust_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        diagnostic_gap: &str,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        self.seal_outcome_with_retrospective_fenced(
            lease,
            permit,
            materialization,
            None,
            diagnostic_gap,
            now,
        )
    }

    pub fn seal_outcome_with_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        diagnostic_gap: &str,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        self.require_paper(&permit.run_id)?;
        let outcome = materialize_outcome(&materialization)?;
        outcome.validate_sealed()?;
        let origin = permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(permit, now);
        let outcome_artifact = if let Some(existing) = self
            .store
            .outcome_for(&permit.run_id, &outcome.outcome_id)?
        {
            existing
        } else {
            let sources = std::iter::once(materialization.schedule_artifact.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect();
            self.artifact(
                ArtifactKind::Outcome,
                &outcome,
                sources,
                &origin,
                &provenance,
                now,
            )?
        };
        let outcome_ref = reference(&outcome_artifact);
        let mut retrospective_source_refs = vec![outcome_ref.clone()];
        retrospective_source_refs.extend(
            self.store
                .retrospectives(&permit.run_id)?
                .into_iter()
                .map(|artifact| reference(&artifact)),
        );
        retrospective_source_refs.sort();
        retrospective_source_refs.dedup();
        let mut retrospective = Retrospective {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome.outcome_id.clone(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "Rust-sealed retrospective; governed model unavailable".to_owned(),
            findings: Vec::new(),
            counterfactuals: Vec::new(),
            lesson_candidates: Vec::new(),
            diagnostic_gaps: vec![diagnostic_gap.to_owned()],
            source_refs: retrospective_source_refs,
            outcome: outcome_ref,
            created_at: now,
            sealed_at: Some(now),
        };
        if let Some(draft) = retrospective_draft {
            if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
                return Err(EvaluationError::InvalidMaterialization(
                    "retrospective draft identity",
                ));
            }
            retrospective.status = RetrospectiveStatus::Complete;
            retrospective.summary = draft.summary.clone();
            retrospective.findings = draft.findings.clone();
            retrospective.counterfactuals = draft.counterfactuals.clone();
            retrospective.lesson_candidates = draft.lesson_candidates.clone();
            retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
            retrospective.source_refs.extend(draft.source_refs.clone());
            retrospective.source_refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
            retrospective.source_refs.sort();
            retrospective.source_refs.dedup();
        }
        retrospective.validate()?;
        let retrospective_artifact = if let Some(existing) =
            self.store
                .retrospective_for(&permit.run_id, &outcome.outcome_id, OutcomeHorizon::T5)?
        {
            existing
        } else {
            self.artifact(
                ArtifactKind::Retrospective,
                &retrospective,
                retrospective.source_refs.clone(),
                &origin,
                &provenance,
                now,
            )?
        };
        self.store.commit_outcome_retrospective_fenced(
            lease,
            permit,
            &outcome_artifact,
            &retrospective_artifact,
            now,
        )?;
        Ok((outcome_artifact, retrospective_artifact))
    }

    /// Materializes and atomically records a RunScoped T+1/T+3 snapshot with
    /// its bounded retrospective narrative. No Experience or Evaluation is
    /// created from this path.
    #[allow(clippy::too_many_arguments)]
    pub fn record_partial_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        horizon: OutcomeHorizon,
        draft: Option<&RetrospectiveDraft>,
        prior_retrospectives: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        let outcome = materialize_partial_outcome(&materialization)?;
        if !outcome
            .windows
            .iter()
            .any(|window| window.horizon == horizon)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "partial retrospective horizon",
            ));
        }
        let origin = permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(permit, now);
        let outcome_artifact = self.artifact_with_lifecycle(
            ArtifactKind::Outcome,
            &outcome,
            std::iter::once(materialization.schedule_artifact.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect(),
            ArtifactLifecycle::RunScoped,
            &origin,
            &provenance,
            now,
        )?;
        let outcome_ref = reference(&outcome_artifact);

        let mut status = RetrospectiveStatus::ModelUnavailable;
        let mut summary = format!("Rust-sealed {horizon:?} retrospective");
        let mut findings = Vec::new();
        let mut counterfactuals = Vec::new();
        let mut lesson_candidates = Vec::new();
        let mut diagnostic_gaps =
            vec!["governed retrospective model unavailable for this horizon".to_owned()];
        let mut source_refs = prior_retrospectives.to_vec();
        if let Some(draft) = draft {
            if draft.outcome_id != outcome.outcome_id || draft.horizon != horizon {
                return Err(EvaluationError::InvalidMaterialization(
                    "retrospective draft identity",
                ));
            }
            status = RetrospectiveStatus::Complete;
            summary = draft.summary.clone();
            findings = draft.findings.clone();
            counterfactuals = draft.counterfactuals.clone();
            lesson_candidates = draft.lesson_candidates.clone();
            diagnostic_gaps = draft.diagnostic_gaps.clone();
            source_refs.extend(draft.source_refs.clone());
            source_refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
        }
        source_refs.push(outcome_ref.clone());
        source_refs.sort();
        source_refs.dedup();
        let retrospective = Retrospective {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome.outcome_id.clone(),
            horizon,
            status,
            summary,
            findings,
            counterfactuals,
            lesson_candidates,
            diagnostic_gaps,
            source_refs: source_refs.clone(),
            outcome: outcome_ref,
            created_at: now,
            sealed_at: Some(now),
        };
        retrospective.validate()?;
        let retrospective_artifact = self.artifact_with_lifecycle(
            ArtifactKind::Retrospective,
            &retrospective,
            source_refs,
            ArtifactLifecycle::RunScoped,
            &origin,
            &provenance,
            now,
        )?;
        self.store.record_partial_outcome_retrospective_fenced(
            lease,
            permit,
            &outcome_artifact,
            &retrospective_artifact,
            now,
        )?;
        Ok((outcome_artifact, retrospective_artifact))
    }

    fn artifact<T: Serialize>(
        &self,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        origin: &ArtifactOrigin,
        provenance: &ArtifactProvenance,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Artifact> {
        self.artifact_with_lifecycle(
            kind,
            payload,
            source_refs,
            ArtifactLifecycle::Canonical,
            origin,
            provenance,
            created_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn artifact_with_lifecycle<T: Serialize>(
        &self,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        lifecycle: ArtifactLifecycle,
        origin: &ArtifactOrigin,
        provenance: &ArtifactProvenance,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Artifact> {
        let blob = self.store.put_json(payload)?;
        Ok(Artifact::new(
            kind,
            blob,
            "akzio-learning.evaluation",
            lifecycle,
            provenance.clone(),
            Some(origin.clone()),
            source_refs,
            created_at,
        )?)
    }
}

/// Deterministically derives all OutcomeWindow metrics from governed facts.
pub fn materialize_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    input.schedule.validate()?;
    if input.schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
        return Err(EvaluationError::InvalidMaterialization(
            "schedule artifact kind",
        ));
    }
    input.target.validate_universe()?;
    input.cost_model.validate()?;
    validate_prices(&input.baseline_prices)?;

    let forecasts = index_forecasts(&input.target, &input.forecasts)?;
    let observations = index_observations(&input.schedule, &input.observations)?;
    let mut market_evidence = input.market_evidence.clone();
    market_evidence.sort();
    market_evidence.dedup();

    let mut windows = Vec::with_capacity(OutcomeHorizon::ALL.len());
    for horizon in OutcomeHorizon::ALL {
        let _forecast_probability_ppm = forecasts
            .get(&horizon)
            .expect("index_forecasts requires all horizons");
        let observation = observations
            .get(&horizon)
            .expect("index_observations requires all horizons");
        let portfolio_return_ppm = portfolio_return_ppm(
            &input.target,
            &input.baseline_prices,
            &observation.future_prices,
        )?;
        let benchmark_return_ppm = return_ppm(
            price(&input.baseline_prices, Asset::Qqq)?,
            price(&observation.future_prices, Asset::Qqq)?,
        )?;
        let utility_ppm = portfolio_return_ppm
            .checked_sub(benchmark_return_ppm)
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.transaction_cost_ppm)))
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.slippage_ppm)))
            .ok_or(EvaluationError::ArithmeticOverflow)?;
        windows.push(OutcomeWindow {
            horizon,
            observed_trading_day: observation.observed_trading_day,
            portfolio_return_ppm,
            benchmark_return_ppm,
            transaction_cost_ppm: input.cost_model.transaction_cost_ppm,
            slippage_ppm: input.cost_model.slippage_ppm,
            utility_ppm,
            calibration_ppm: None,
            evidence_completeness_ppm: bounded_ratio_ppm(
                observation.expected_evidence_count,
                observation.observed_evidence_count,
            ),
            risk_recall_ppm: observation
                .detected_risk_count
                .map(|detected| bounded_ratio_ppm(observation.expected_risk_count, detected)),
        });
    }

    let outcome = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: input.schedule.outcome_id.clone(),
        schedule: input.schedule_artifact.clone(),
        market_evidence,
        windows,
        sealed_at: Some(input.sealed_at),
    };
    outcome.validate_sealed()?;
    Ok(outcome)
}

/// Materializes the currently due prefix of an outcome for T+1/T+3
/// diagnostics.  These snapshots remain RunScoped and unsealed; only the
/// complete three-window result is eligible for canonical learning.
pub fn materialize_partial_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    input.schedule.validate()?;
    if input.schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
        return Err(EvaluationError::InvalidMaterialization(
            "schedule artifact kind",
        ));
    }
    input.target.validate_universe()?;
    input.cost_model.validate()?;
    validate_prices(&input.baseline_prices)?;

    let forecasts = index_forecasts(&input.target, &input.forecasts)?;
    let mut observations = BTreeMap::new();
    for observation in &input.observations {
        if !observation
            .horizon
            .is_due_after(observation.completed_trading_sessions)
            || observation.observed_trading_day <= input.schedule.baseline_trading_day
        {
            return Err(EvaluationError::InvalidMaterialization("horizon not due"));
        }
        validate_prices(&observation.future_prices)?;
        if observations
            .insert(observation.horizon, observation)
            .is_some()
        {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate observation horizon",
            ));
        }
    }
    if observations.is_empty() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing due observation",
        ));
    }

    let mut market_evidence = input.market_evidence.clone();
    market_evidence.sort();
    market_evidence.dedup();
    let mut windows = Vec::with_capacity(observations.len());
    for (horizon, observation) in observations {
        let _forecast_probability_ppm = forecasts
            .get(&horizon)
            .expect("index_forecasts requires all horizons");
        let portfolio_return_ppm = portfolio_return_ppm(
            &input.target,
            &input.baseline_prices,
            &observation.future_prices,
        )?;
        let benchmark_return_ppm = return_ppm(
            price(&input.baseline_prices, Asset::Qqq)?,
            price(&observation.future_prices, Asset::Qqq)?,
        )?;
        let utility_ppm = portfolio_return_ppm
            .checked_sub(benchmark_return_ppm)
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.transaction_cost_ppm)))
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.slippage_ppm)))
            .ok_or(EvaluationError::ArithmeticOverflow)?;
        windows.push(OutcomeWindow {
            horizon,
            observed_trading_day: observation.observed_trading_day,
            portfolio_return_ppm,
            benchmark_return_ppm,
            transaction_cost_ppm: input.cost_model.transaction_cost_ppm,
            slippage_ppm: input.cost_model.slippage_ppm,
            utility_ppm,
            calibration_ppm: None,
            evidence_completeness_ppm: bounded_ratio_ppm(
                observation.expected_evidence_count,
                observation.observed_evidence_count,
            ),
            risk_recall_ppm: observation
                .detected_risk_count
                .map(|detected| bounded_ratio_ppm(observation.expected_risk_count, detected)),
        });
    }
    windows.sort_by_key(|window| window.horizon);

    let outcome = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: input.schedule.outcome_id.clone(),
        schedule: input.schedule_artifact.clone(),
        market_evidence,
        windows,
        sealed_at: None,
    };
    outcome.validate()?;
    Ok(outcome)
}

#[path = "metrics.rs"]
mod metrics;
use metrics::{
    bounded_ratio_ppm, execution_verdict, index_forecasts, index_observations, marginal_utility,
    next_state_with_fresh_pairs, portfolio_return_ppm, price, reference, require_canonical_purpose,
    return_ppm, stable_id, validate_prices,
};
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
