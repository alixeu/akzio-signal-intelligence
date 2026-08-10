//! Canonical, outcome-backed learning runtime for the v2 rebuild path.
//!
//! This module never accepts a caller-supplied "canonical" flag. The Store's
//! recorded Run purpose is checked before any learning artifact or policy head
//! can be written.

use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;

use akzio_domain::{
    content_hash_json, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, CandidatePolicyState, ContentHash, DomainError, Evaluation,
    EvaluationId, Experience, ExperienceId, MemoryId, MemoryLifecycle, Outcome, OutcomeHorizon,
    PolicyState, PolicyTransition, PolicyTransitionId, RunPurpose, TaskStatus, TaskWritePermit,
    TopologyId, REBUILD_SCHEMA_VERSION,
};
use akzio_store::v2::{
    PolicyHead, PolicyTransitionCommit, PolicyTransitionResult, ShadowPairCompletion,
    ShadowPairWriteResult, StoreError, V2Store,
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
    fn validate(&self) -> Result<(), RebuildEvaluationError> {
        if self.minimum_evidence_completeness_ppm > PPM_ONE
            || self.minimum_risk_recall_ppm > PPM_ONE
            || self.minimum_fresh_pairs_per_horizon == 0
        {
            return Err(RebuildEvaluationError::InvalidPolicy);
        }
        Ok(())
    }
}

/// Stable policy namespace. Prefixes prevent accidental collisions between a
/// UUID-backed memory, a content-addressed contract, and a topology ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicySubject {
    Memory(MemoryId),
    Contract(ContentHash),
    Topology(TopologyId),
}

impl PolicySubject {
    pub fn subject_id(&self) -> String {
        match self {
            Self::Memory(memory_id) => format!("memory:{}", memory_id.0),
            Self::Contract(contract_hash) => format!("contract:{}", contract_hash.as_str()),
            Self::Topology(topology_id) => format!("topology:{}", topology_id.0),
        }
    }

    fn initial_state(&self) -> PolicyState {
        match self {
            Self::Memory(_) => PolicyState::Memory(MemoryLifecycle::Candidate),
            Self::Contract(_) => PolicyState::Contract(CandidatePolicyState::Candidate),
            Self::Topology(_) => PolicyState::Topology(CandidatePolicyState::Candidate),
        }
    }

    fn accepts_state(&self, state: PolicyState) -> bool {
        matches!(
            (self, state),
            (Self::Memory(_), PolicyState::Memory(_))
                | (Self::Contract(_), PolicyState::Contract(_))
                | (Self::Topology(_), PolicyState::Topology(_))
        )
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationInput {
    pub permit: TaskWritePermit,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub outcome: Outcome,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub policy_verdict: ArtifactRef,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub token_cost: u64,
    pub latency_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResult {
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub evaluation: ArtifactRef,
    pub policy_head: Option<PolicyHead>,
    pub fresh_pairs_by_horizon: [u64; 3],
}

#[derive(Debug, Error)]
pub enum RebuildEvaluationError {
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
}

pub type RebuildEvaluationResult<T> = Result<T, RebuildEvaluationError>;

#[derive(Debug, Clone)]
pub struct RebuildEvaluationRuntime {
    store: V2Store,
    policy: EvaluationPolicy,
}

impl RebuildEvaluationRuntime {
    pub fn new(store: V2Store, policy: EvaluationPolicy) -> RebuildEvaluationResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &EvaluationPolicy {
        &self.policy
    }

    /// Persists a candidate/production comparison without changing policy.
    /// State promotion remains a later, separately explicit evaluation step.
    pub fn record_shadow_pair(
        &self,
        permit: &TaskWritePermit,
        subject: &PolicySubject,
        observation: ShadowObservation,
    ) -> RebuildEvaluationResult<ShadowPairWriteResult> {
        self.require_paper(&permit.run_id)?;
        Ok(self.store.complete_shadow_pair(
            permit,
            &ShadowPairCompletion {
                subject_id: subject.subject_id(),
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

    /// Lowers a sealed Paper outcome into Experience and Evaluation artifacts.
    /// Only an outcome that is Paper-owned, sealed, and backed by fresh pairs
    /// can advance a policy head.
    pub fn evaluate(&self, input: EvaluationInput) -> RebuildEvaluationResult<EvaluationResult> {
        self.require_paper(&input.permit.run_id)?;
        if input.hypothesis_id.trim().is_empty() {
            return Err(RebuildEvaluationError::EmptyHypothesis);
        }
        input.outcome.validate_sealed()?;

        let subject_id = input.subject.subject_id();
        let previous_head = self.store.policy_head(&subject_id)?;
        let current = previous_head
            .as_ref()
            .map(|head| head.state)
            .unwrap_or_else(|| input.subject.initial_state());
        if !input.subject.accepts_state(current) {
            return Err(RebuildEvaluationError::SubjectStateMismatch);
        }
        let created_at = input
            .outcome
            .sealed_at
            .expect("validate_sealed checked sealed_at");
        let fresh_after = previous_head
            .as_ref()
            .map(|head| head.updated_at)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        let fresh_pairs_by_horizon = [
            self.store
                .fresh_shadow_pair_count(&subject_id, OutcomeHorizon::T1, fresh_after)?,
            self.store
                .fresh_shadow_pair_count(&subject_id, OutcomeHorizon::T3, fresh_after)?,
            self.store
                .fresh_shadow_pair_count(&subject_id, OutcomeHorizon::T5, fresh_after)?,
        ];
        let has_fresh_pairs = fresh_pairs_by_horizon
            .iter()
            .all(|count| *count >= self.policy.minimum_fresh_pairs_per_horizon);
        let degraded = input.outcome.windows.iter().any(|window| {
            window.evidence_completeness_ppm < self.policy.minimum_evidence_completeness_ppm
                || window.risk_recall_ppm < self.policy.minimum_risk_recall_ppm
        });

        let origin = ArtifactOrigin {
            run_id: Some(input.permit.run_id.clone()),
            task_id: Some(input.permit.task_id.clone()),
            attempt_id: Some(input.permit.attempt_id.clone()),
            contract_hash: input.permit.contract_hash.clone(),
        };
        let provenance = ArtifactProvenance {
            source_family: "akzio-learning".to_owned(),
            observed_at: Some(created_at),
            retrieved_at: created_at,
            source_uri: None,
            confidence_ppm: PPM_ONE,
            producer_contract_hash: input.permit.contract_hash.clone(),
        };
        let outcome_artifact = self.artifact(
            ArtifactKind::Outcome,
            &input.outcome,
            vec![input.outcome.execution_context.clone()]
                .into_iter()
                .chain(input.outcome.market_evidence.iter().cloned())
                .collect(),
            &origin,
            &provenance,
            created_at,
        )?;
        let outcome_ref = reference(&outcome_artifact);
        let experience = Experience {
            schema_version: REBUILD_SCHEMA_VERSION,
            experience_id: ExperienceId(stable_id(&serde_json::json!({
                "subject_id": &subject_id,
                "hypothesis_id": &input.hypothesis_id,
                "decision": &input.decision,
                "outcome": &outcome_ref,
                "contract_hash": &input.contract_hash,
                "topology_id": &input.topology_id,
            }))?),
            hypothesis_id: input.hypothesis_id.clone(),
            decision: input.decision.clone(),
            decision_context: input.decision_context.clone(),
            execution_context: input.outcome.execution_context.clone(),
            policy_verdict: input.policy_verdict.clone(),
            outcome: outcome_ref.clone(),
            contract_hash: input.contract_hash.clone(),
            topology_id: input.topology_id.clone(),
            lifecycle: memory_lifecycle(current),
            created_at,
        };
        let experience_artifact = self.artifact(
            ArtifactKind::Experience,
            &experience,
            vec![
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let experience_ref = reference(&experience_artifact);
        let evaluation = Evaluation {
            schema_version: REBUILD_SCHEMA_VERSION,
            evaluation_id: EvaluationId(stable_id(&serde_json::json!({
                "subject_id": &subject_id,
                "outcome": &outcome_ref,
                "experience": &experience_ref,
                "token_cost": input.token_cost,
                "latency_millis": input.latency_millis,
            }))?),
            outcome: outcome_ref.clone(),
            experience: experience_ref.clone(),
            marginal_utility_ppm: marginal_utility(&input.outcome),
            token_cost: input.token_cost,
            latency_millis: input.latency_millis,
            created_at,
        };
        let evaluation_artifact = self.artifact(
            ArtifactKind::Evaluation,
            &evaluation,
            vec![evaluation.outcome.clone(), evaluation.experience.clone()],
            &origin,
            &provenance,
            created_at,
        )?;
        let evaluation_ref = reference(&evaluation_artifact);
        let next = next_state(current, has_fresh_pairs, degraded);

        let policy_head = if next == current {
            self.store.commit_attempt(
                &input.permit,
                &[outcome_artifact, experience_artifact, evaluation_artifact],
                TaskStatus::Succeeded,
                created_at,
            )?;
            previous_head
        } else {
            let transition = PolicyTransition {
                schema_version: REBUILD_SCHEMA_VERSION,
                transition_id: PolicyTransitionId(stable_id(&serde_json::json!({
                    "subject_id": &subject_id,
                    "from": current,
                    "to": next,
                    "evaluation": &evaluation_ref,
                }))?),
                subject_id,
                from: current,
                to: next,
                evaluation: evaluation_ref.clone(),
                created_at,
            };
            match self
                .store
                .record_policy_transition(&PolicyTransitionCommit {
                    permit: input.permit,
                    outcome: outcome_artifact,
                    experience: experience_artifact,
                    evaluation: evaluation_artifact,
                    transition,
                    completed_at: created_at,
                })? {
                PolicyTransitionResult::Applied(head) | PolicyTransitionResult::Existing(head) => {
                    Some(head)
                }
            }
        };

        Ok(EvaluationResult {
            outcome: outcome_ref,
            experience: experience_ref,
            evaluation: evaluation_ref,
            policy_head,
            fresh_pairs_by_horizon,
        })
    }

    fn require_paper(&self, run_id: &akzio_domain::RunId) -> RebuildEvaluationResult<()> {
        let purpose = self.store.run_purpose(run_id)?;
        if !purpose.is_canonical_learning() {
            return Err(RebuildEvaluationError::NonCanonicalPurpose(purpose));
        }
        Ok(())
    }

    fn artifact<T: Serialize>(
        &self,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        origin: &ArtifactOrigin,
        provenance: &ArtifactProvenance,
        created_at: DateTime<Utc>,
    ) -> RebuildEvaluationResult<Artifact> {
        let blob = self.store.put_json(payload)?;
        Ok(Artifact::new(
            kind,
            blob,
            "akzio-learning.evaluation",
            ArtifactLifecycle::Canonical,
            provenance.clone(),
            Some(origin.clone()),
            source_refs,
            created_at,
        )?)
    }
}

fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn stable_id(value: &serde_json::Value) -> RebuildEvaluationResult<String> {
    Ok(content_hash_json(value)?.as_str().to_owned())
}

fn marginal_utility(outcome: &Outcome) -> i64 {
    let total = outcome
        .windows
        .iter()
        .fold(0_i128, |sum, window| sum + i128::from(window.utility_ppm));
    let average = total / i128::try_from(outcome.windows.len()).unwrap_or(1);
    average.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn memory_lifecycle(state: PolicyState) -> MemoryLifecycle {
    match state {
        PolicyState::Memory(lifecycle) => lifecycle,
        PolicyState::Contract(_) | PolicyState::Topology(_) => MemoryLifecycle::Candidate,
    }
}

fn next_state(current: PolicyState, has_fresh_pairs: bool, degraded: bool) -> PolicyState {
    use CandidatePolicyState as Candidate;
    use MemoryLifecycle as Memory;

    if degraded {
        return match current {
            PolicyState::Memory(Memory::Contested) => PolicyState::Memory(Memory::Retired),
            PolicyState::Memory(Memory::Retired) => current,
            PolicyState::Memory(_) => PolicyState::Memory(Memory::Contested),
            PolicyState::Contract(Candidate::Candidate)
            | PolicyState::Topology(Candidate::Candidate) => current,
            PolicyState::Contract(_) => PolicyState::Contract(Candidate::Candidate),
            PolicyState::Topology(_) => PolicyState::Topology(Candidate::Candidate),
        };
    }
    if !has_fresh_pairs {
        return current;
    }
    match current {
        PolicyState::Memory(Memory::Candidate) => PolicyState::Memory(Memory::Active),
        PolicyState::Memory(Memory::Active) => PolicyState::Memory(Memory::Proven),
        PolicyState::Memory(Memory::Contested) => PolicyState::Memory(Memory::Active),
        PolicyState::Memory(Memory::Proven | Memory::Retired) => current,
        PolicyState::Contract(Candidate::Candidate) => PolicyState::Contract(Candidate::Canary10),
        PolicyState::Contract(Candidate::Canary10) => PolicyState::Contract(Candidate::Canary25),
        PolicyState::Contract(Candidate::Canary25) => PolicyState::Contract(Candidate::Canary50),
        PolicyState::Contract(Candidate::Canary50) => PolicyState::Contract(Candidate::Active),
        PolicyState::Contract(Candidate::Active) => current,
        PolicyState::Topology(Candidate::Candidate) => PolicyState::Topology(Candidate::Canary10),
        PolicyState::Topology(Candidate::Canary10) => PolicyState::Topology(Candidate::Canary25),
        PolicyState::Topology(Candidate::Canary25) => PolicyState::Topology(Candidate::Canary50),
        PolicyState::Topology(Candidate::Canary50) => PolicyState::Topology(Candidate::Active),
        PolicyState::Topology(Candidate::Active) => current,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;
    use tempfile::tempdir;

    use akzio_domain::{
        ArtifactLifecycle, ArtifactProvenance, FailureDisposition, OutcomeId, OutcomeWindow,
        RetryPolicy, RunId, TaskBudget, TaskId, TaskRecipeId, TopologyId, WorkflowGraph,
        WorkflowNode,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};

    use super::*;

    #[derive(Clone)]
    struct SeededArtifacts {
        execution_context: ArtifactRef,
        parent_decision: ArtifactRef,
        candidate_decision: ArtifactRef,
        decision_context: ArtifactRef,
        policy_verdict: ArtifactRef,
        parent_outcome: ArtifactRef,
        candidate_outcome: ArtifactRef,
        candidate_outcome_payload: Outcome,
        contract_hash: ContentHash,
        topology_id: TopologyId,
    }

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 64,
            max_output_tokens: 64,
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

    fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: PPM_ONE,
            producer_contract_hash: None,
        }
    }

    fn graph(nodes: usize) -> WorkflowGraph {
        let mut previous = None;
        let nodes = (0..nodes)
            .map(|index| {
                let task_id = TaskId::new();
                let dependencies = previous.iter().cloned().collect();
                previous = Some(task_id.clone());
                WorkflowNode {
                    task_id,
                    recipe_id: TaskRecipeId::new("evaluation.fixture").unwrap(),
                    contract_hash: None,
                    objective: format!("fixture task {index}"),
                    dependencies,
                    input_artifacts: vec![],
                    priority: 50,
                    budget: budget(),
                    retry: retry(),
                    on_failure: FailureDisposition::FailRun,
                    parent_task_id: None,
                }
            })
            .collect();
        WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id: "fixture-topology".to_owned(),
            nodes,
        }
    }

    fn submit_run(store: &V2Store, purpose: RunPurpose, task_count: usize) -> StoredRun {
        let now = Utc::now();
        let graph = graph(task_count);
        graph.validate().unwrap();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(now),
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
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        run
    }

    fn claim(store: &V2Store) -> TaskWritePermit {
        store
            .claim_next_task("fixture-worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit
    }

    fn task_artifact<T: Serialize>(
        store: &V2Store,
        permit: &TaskWritePermit,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> Artifact {
        Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "fixture",
            ArtifactLifecycle::Canonical,
            provenance(now),
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            source_refs,
            now,
        )
        .unwrap()
    }

    fn ref_of(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn sealed_outcome(
        execution_context: ArtifactRef,
        evidence: ArtifactRef,
        utility: i64,
        evidence_completeness_ppm: u32,
        risk_recall_ppm: u32,
        sealed_at: DateTime<Utc>,
    ) -> Outcome {
        Outcome {
            schema_version: REBUILD_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            execution_context,
            market_evidence: vec![evidence],
            windows: [OutcomeHorizon::T1, OutcomeHorizon::T3, OutcomeHorizon::T5]
                .into_iter()
                .map(|horizon| OutcomeWindow {
                    horizon,
                    portfolio_return_ppm: utility,
                    benchmark_return_ppm: 0,
                    utility_ppm: utility,
                    calibration_ppm: PPM_ONE,
                    evidence_completeness_ppm,
                    risk_recall_ppm,
                })
                .collect(),
            sealed_at: Some(sealed_at),
        }
    }

    fn seed_paper_inputs(store: &V2Store, permit: &TaskWritePermit) -> SeededArtifacts {
        let now = Utc::now();
        let raw = task_artifact(
            store,
            permit,
            ArtifactKind::RawEvidence,
            &json!({"raw": true}),
            vec![],
            now,
        );
        let raw_ref = ref_of(&raw);
        let normalized = task_artifact(
            store,
            permit,
            ArtifactKind::NormalizedEvidence,
            &json!({"normalized": true}),
            vec![raw_ref],
            now,
        );
        let evidence = ref_of(&normalized);
        let execution = task_artifact(
            store,
            permit,
            ArtifactKind::ExecutionContext,
            &json!({"execution": true}),
            vec![],
            now,
        );
        let execution_context = ref_of(&execution);
        let parent = task_artifact(
            store,
            permit,
            ArtifactKind::Decision,
            &json!({"candidate": false}),
            vec![],
            now,
        );
        let parent_decision = ref_of(&parent);
        let candidate = task_artifact(
            store,
            permit,
            ArtifactKind::Decision,
            &json!({"candidate": true}),
            vec![],
            now,
        );
        let candidate_decision = ref_of(&candidate);
        let context = task_artifact(
            store,
            permit,
            ArtifactKind::DecisionContext,
            &json!({"context": true}),
            vec![],
            now,
        );
        let decision_context = ref_of(&context);
        let verdict = task_artifact(
            store,
            permit,
            ArtifactKind::ExecutionVerdict,
            &json!({"approved": true}),
            vec![],
            now,
        );
        let policy_verdict = ref_of(&verdict);
        let parent_outcome_payload = sealed_outcome(
            execution_context.clone(),
            evidence.clone(),
            100,
            PPM_ONE,
            PPM_ONE,
            now,
        );
        let parent_outcome_artifact = task_artifact(
            store,
            permit,
            ArtifactKind::Outcome,
            &parent_outcome_payload,
            vec![execution_context.clone(), evidence.clone()],
            now,
        );
        let candidate_outcome_payload = sealed_outcome(
            execution_context.clone(),
            evidence.clone(),
            200,
            PPM_ONE,
            PPM_ONE,
            now,
        );
        let candidate_outcome_artifact = task_artifact(
            store,
            permit,
            ArtifactKind::Outcome,
            &candidate_outcome_payload,
            vec![execution_context.clone(), evidence.clone()],
            now,
        );
        store
            .commit_attempt(
                permit,
                &[
                    raw,
                    normalized,
                    execution,
                    parent,
                    candidate,
                    context,
                    verdict,
                    parent_outcome_artifact.clone(),
                    candidate_outcome_artifact.clone(),
                ],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        SeededArtifacts {
            execution_context,
            parent_decision,
            candidate_decision,
            decision_context,
            policy_verdict,
            parent_outcome: ref_of(&parent_outcome_artifact),
            candidate_outcome: ref_of(&candidate_outcome_artifact),
            candidate_outcome_payload,
            contract_hash: ContentHash::of_bytes(b"fixture-candidate-contract"),
            topology_id: TopologyId("fixture-candidate-topology".to_owned()),
        }
    }

    fn observation(
        seed: &SeededArtifacts,
        horizon: OutcomeHorizon,
        at: DateTime<Utc>,
    ) -> ShadowObservation {
        ShadowObservation {
            parent_decision: seed.parent_decision.clone(),
            execution_context: seed.execution_context.clone(),
            candidate_decision: seed.candidate_decision.clone(),
            candidate_contract_hash: seed.contract_hash.clone(),
            candidate_topology_id: seed.topology_id.0.clone(),
            horizon,
            parent_outcome: seed.parent_outcome.clone(),
            candidate_outcome: seed.candidate_outcome.clone(),
            completed_at: at,
        }
    }

    fn evaluation_input(
        permit: TaskWritePermit,
        subject: PolicySubject,
        seed: &SeededArtifacts,
        outcome: Outcome,
    ) -> EvaluationInput {
        EvaluationInput {
            permit,
            subject,
            hypothesis_id: "fixture hypothesis".to_owned(),
            outcome,
            decision: seed.candidate_decision.clone(),
            decision_context: seed.decision_context.clone(),
            policy_verdict: seed.policy_verdict.clone(),
            contract_hash: seed.contract_hash.clone(),
            topology_id: seed.topology_id.clone(),
            token_cost: 10,
            latency_millis: 20,
        }
    }

    #[test]
    fn paper_pairs_promote_canary_then_quality_regression_rolls_it_back() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime =
            RebuildEvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
        submit_run(&store, RunPurpose::Paper, 4);

        let seed_permit = claim(&store);
        let seed = seed_paper_inputs(&store, &seed_permit);
        let subject = PolicySubject::Contract(seed.contract_hash.clone());

        let pair_permit = claim(&store);
        let first = runtime
            .record_shadow_pair(
                &pair_permit,
                &subject,
                observation(&seed, OutcomeHorizon::T1, Utc::now()),
            )
            .unwrap();
        assert!(matches!(first, ShadowPairWriteResult::Inserted(_)));
        let duplicate = runtime
            .record_shadow_pair(
                &pair_permit,
                &subject,
                observation(&seed, OutcomeHorizon::T1, Utc::now()),
            )
            .unwrap();
        assert!(matches!(duplicate, ShadowPairWriteResult::Existing(_)));
        for horizon in [OutcomeHorizon::T3, OutcomeHorizon::T5] {
            runtime
                .record_shadow_pair(
                    &pair_permit,
                    &subject,
                    observation(&seed, horizon, Utc::now()),
                )
                .unwrap();
        }
        store
            .finish_task(&pair_permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();

        let first_evaluation = runtime
            .evaluate(evaluation_input(
                claim(&store),
                subject.clone(),
                &seed,
                seed.candidate_outcome_payload.clone(),
            ))
            .unwrap();
        assert_eq!(first_evaluation.fresh_pairs_by_horizon, [1, 1, 1]);
        assert_eq!(
            first_evaluation.policy_head.unwrap().state,
            PolicyState::Contract(CandidatePolicyState::Canary10)
        );

        let mut harmful = seed.candidate_outcome_payload.clone();
        harmful.outcome_id = OutcomeId::new();
        harmful.sealed_at = Some(harmful.sealed_at.unwrap() + Duration::seconds(1));
        for window in &mut harmful.windows {
            window.risk_recall_ppm = 1;
        }
        let second_evaluation = runtime
            .evaluate(evaluation_input(
                claim(&store),
                subject.clone(),
                &seed,
                harmful,
            ))
            .unwrap();
        assert_eq!(
            second_evaluation.policy_head.unwrap().state,
            PolicyState::Contract(CandidatePolicyState::Candidate)
        );

        let history = store.policy_transitions(&subject.subject_id()).unwrap();
        assert_eq!(history.len(), 2);
        let reconstructed = history
            .iter()
            .fold(subject.initial_state(), |state, record| {
                assert_eq!(record.transition.from, state);
                record.transition.to
            });
        assert_eq!(
            reconstructed,
            store
                .policy_head(&subject.subject_id())
                .unwrap()
                .unwrap()
                .state
        );
        store.verify_integrity().unwrap();
    }

    #[test]
    fn non_paper_purposes_and_unsealed_outcomes_cannot_promote() {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::PaperDryRun,
            RunPurpose::Shadow,
        ] {
            let root = tempdir().unwrap();
            let store = V2Store::open(root.path()).unwrap();
            let runtime =
                RebuildEvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
            submit_run(&store, purpose, 1);
            let permit = claim(&store);
            let reference = ArtifactRef {
                artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"fixture-reference")),
                kind: ArtifactKind::ExecutionContext,
            };
            let outcome = sealed_outcome(
                reference.clone(),
                ArtifactRef {
                    artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                        b"fixture-evidence",
                    )),
                    kind: ArtifactKind::NormalizedEvidence,
                },
                1,
                PPM_ONE,
                PPM_ONE,
                Utc::now(),
            );
            let error = runtime
                .evaluate(EvaluationInput {
                    permit,
                    subject: PolicySubject::Memory(MemoryId::new()),
                    hypothesis_id: "fixture".to_owned(),
                    outcome,
                    decision: ArtifactRef {
                        artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                            b"fixture-decision",
                        )),
                        kind: ArtifactKind::Decision,
                    },
                    decision_context: ArtifactRef {
                        artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                            b"fixture-context",
                        )),
                        kind: ArtifactKind::DecisionContext,
                    },
                    policy_verdict: ArtifactRef {
                        artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                            b"fixture-verdict",
                        )),
                        kind: ArtifactKind::ExecutionVerdict,
                    },
                    contract_hash: ContentHash::of_bytes(b"fixture-contract"),
                    topology_id: TopologyId("fixture-topology".to_owned()),
                    token_cost: 1,
                    latency_millis: 1,
                })
                .unwrap_err();
            assert!(matches!(
                error,
                RebuildEvaluationError::NonCanonicalPurpose(actual) if actual == purpose
            ));
        }

        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime =
            RebuildEvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
        submit_run(&store, RunPurpose::Paper, 1);
        let permit = claim(&store);
        let execution = ArtifactRef {
            artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"fixture-execution")),
            kind: ArtifactKind::ExecutionContext,
        };
        let mut outcome = sealed_outcome(
            execution.clone(),
            ArtifactRef {
                artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"fixture-evidence")),
                kind: ArtifactKind::NormalizedEvidence,
            },
            1,
            PPM_ONE,
            PPM_ONE,
            Utc::now(),
        );
        outcome.sealed_at = None;
        let error = runtime
            .evaluate(EvaluationInput {
                permit,
                subject: PolicySubject::Memory(MemoryId::new()),
                hypothesis_id: "fixture".to_owned(),
                outcome,
                decision: ArtifactRef {
                    artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                        b"fixture-decision",
                    )),
                    kind: ArtifactKind::Decision,
                },
                decision_context: ArtifactRef {
                    artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                        b"fixture-context",
                    )),
                    kind: ArtifactKind::DecisionContext,
                },
                policy_verdict: ArtifactRef {
                    artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                        b"fixture-verdict",
                    )),
                    kind: ArtifactKind::ExecutionVerdict,
                },
                contract_hash: ContentHash::of_bytes(b"fixture-contract"),
                topology_id: TopologyId("fixture-topology".to_owned()),
                token_cost: 1,
                latency_millis: 1,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            RebuildEvaluationError::Domain(DomainError::EmptyField {
                field: "outcome.sealed_at"
            })
        ));
    }

    #[test]
    fn memory_lifecycle_requires_pairs_and_degrades_to_retirement() {
        assert_eq!(
            next_state(
                PolicyState::Memory(MemoryLifecycle::Candidate),
                false,
                false
            ),
            PolicyState::Memory(MemoryLifecycle::Candidate)
        );
        assert_eq!(
            next_state(PolicyState::Memory(MemoryLifecycle::Candidate), true, false),
            PolicyState::Memory(MemoryLifecycle::Active)
        );
        assert_eq!(
            next_state(PolicyState::Memory(MemoryLifecycle::Active), true, false),
            PolicyState::Memory(MemoryLifecycle::Proven)
        );
        assert_eq!(
            next_state(PolicyState::Memory(MemoryLifecycle::Proven), false, true),
            PolicyState::Memory(MemoryLifecycle::Contested)
        );
        assert_eq!(
            next_state(PolicyState::Memory(MemoryLifecycle::Contested), false, true),
            PolicyState::Memory(MemoryLifecycle::Retired)
        );
    }
}
