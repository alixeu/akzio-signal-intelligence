//! Canonical, outcome-backed learning runtime v2 rebuild path.
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
    MemoryLifecycle, MoneyMicros, Outcome, OutcomeExecutionLineage, OutcomeHorizon,
    OutcomeSchedule, OutcomeWindow, PolicyState, PolicySubject, PolicyTransition,
    PolicyTransitionId, RunPurpose, TargetPortfolio, TaskWritePermit, TopologyId,
    REBUILD_SCHEMA_VERSION,
};
use akzio_store::v2::{
    PolicyEvaluationCommit, PolicyHead, ShadowPairCompletion, ShadowPairWriteResult, StoreError,
    V2Store,
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
    pub detected_risk_count: u64,
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
    pub token_cost: u64,
    pub latency_millis: u64,
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
    #[error("candidate policy input invalid: {0}")]
    InvalidCandidatePolicy(&'static str),
    #[error("outcome materialization is invalid: {0}")]
    InvalidMaterialization(&'static str),
    #[error("outcome materialization arithmetic overflow")]
    ArithmeticOverflow,
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
    pub fn evaluate(&self, input: EvaluationInput) -> RebuildEvaluationResult<EvaluationResult> {
        self.require_paper(&input.permit.run_id)?;
        if input.hypothesis_id.trim().is_empty() {
            return Err(RebuildEvaluationError::EmptyHypothesis);
        }
        match (&input.subject, &input.candidate_policy) {
            (PolicySubject::Memory(_), None)
            | (PolicySubject::Contract(_), Some(_))
            | (PolicySubject::Topology(_), Some(_)) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(RebuildEvaluationError::InvalidCandidatePolicy(
                    "memory_subject",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(RebuildEvaluationError::InvalidCandidatePolicy(
                    "missing_candidate",
                ));
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
            return Err(RebuildEvaluationError::SubjectStateMismatch);
        }

        let created_at = outcome
            .sealed_at
            .expect("materialized outcome always has sealed_at");
        let pair_snapshot = self.store.policy_shadow_pair_snapshot(&input.subject)?;
        let fresh_pairs_by_horizon = pair_snapshot.counts_by_horizon;
        let has_fresh_pairs = fresh_pairs_by_horizon
            .iter()
            .all(|count| *count >= self.policy.minimum_fresh_pairs_per_horizon);
        let degraded = outcome.windows.iter().any(|window| {
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

        let outcome_sources = std::iter::once(input.materialization.schedule_artifact.clone())
            .chain(outcome.market_evidence.iter().cloned())
            .collect();
        let outcome_artifact = self.artifact(
            ArtifactKind::Outcome,
            &outcome,
            outcome_sources,
            &origin,
            &provenance,
            created_at,
        )?;
        let outcome_ref = reference(&outcome_artifact);
        let schedule = &input.materialization.schedule;
        let policy_verdict = execution_verdict(&schedule.execution).clone();
        let experience = Experience {
            schema_version: REBUILD_SCHEMA_VERSION,
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
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let experience_ref = reference(&experience_artifact);
        let evaluation = Evaluation {
            schema_version: REBUILD_SCHEMA_VERSION,
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
            vec![evaluation.outcome.clone(), evaluation.experience.clone()],
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
                    schema_version: REBUILD_SCHEMA_VERSION,
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
        let next = next_state(current, has_fresh_pairs, degraded);

        let transition = if next == current {
            None
        } else {
            Some(PolicyTransition {
                schema_version: REBUILD_SCHEMA_VERSION,
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
        let policy_head = self
            .store
            .record_policy_evaluation(&PolicyEvaluationCommit {
                permit: input.permit,
                outcome: outcome_artifact,
                experience: experience_artifact,
                evaluation: evaluation_artifact,
                candidate_policy: candidate_policy_artifact,
                subject: input.subject,
                from: current,
                to: next,
                pair_snapshot,
                transition,
                completed_at: created_at,
            })?
            .policy_head;

        Ok(EvaluationResult {
            outcome: outcome_ref,
            experience: experience_ref,
            evaluation: evaluation_ref,
            candidate_policy: candidate_policy_ref,
            policy_head,
            fresh_pairs_by_horizon,
        })
    }

    fn require_paper(&self, run_id: &akzio_domain::RunId) -> RebuildEvaluationResult<()> {
        require_canonical_purpose(self.store.run_purpose(run_id)?)
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

/// Deterministically derives all OutcomeWindow metrics from governed facts.
pub fn materialize_outcome(
    input: &OutcomeMaterializationInput,
) -> RebuildEvaluationResult<Outcome> {
    input.schedule.validate()?;
    if input.schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
        return Err(RebuildEvaluationError::InvalidMaterialization(
            "schedule artifact kind",
        ));
    }
    input.target.validate_universe()?;
    validate_prices(&input.baseline_prices)?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let observations = index_observations(&input.schedule, &input.observations)?;
    let mut market_evidence = input.market_evidence.clone();
    market_evidence.sort();
    market_evidence.dedup();

    let mut windows = Vec::with_capacity(OutcomeHorizon::ALL.len());
    for horizon in OutcomeHorizon::ALL {
        let forecast = forecasts
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
            .ok_or(RebuildEvaluationError::ArithmeticOverflow)?;
        windows.push(OutcomeWindow {
            horizon,
            observed_trading_day: observation.observed_trading_day,
            portfolio_return_ppm,
            benchmark_return_ppm,
            utility_ppm,
            calibration_ppm: directional_calibration_ppm(
                forecast.positive_return_probability_ppm,
                portfolio_return_ppm,
            ),
            evidence_completeness_ppm: bounded_ratio_ppm(
                observation.expected_evidence_count,
                observation.observed_evidence_count,
            ),
            risk_recall_ppm: bounded_ratio_ppm(
                observation.expected_risk_count,
                observation.detected_risk_count,
            ),
        });
    }

    let outcome = Outcome {
        schema_version: REBUILD_SCHEMA_VERSION,
        outcome_id: input.schedule.outcome_id.clone(),
        schedule: input.schedule_artifact.clone(),
        market_evidence,
        windows,
        sealed_at: Some(input.sealed_at),
    };
    outcome.validate_sealed()?;
    Ok(outcome)
}

fn index_forecasts(
    forecasts: &[Forecast],
) -> RebuildEvaluationResult<BTreeMap<OutcomeHorizon, Forecast>> {
    let mut indexed = BTreeMap::new();
    for forecast in forecasts {
        forecast.validate()?;
        let horizon = match forecast.horizon {
            DecisionHorizon::T1 => OutcomeHorizon::T1,
            DecisionHorizon::T3 => OutcomeHorizon::T3,
            DecisionHorizon::T5 => OutcomeHorizon::T5,
        };
        if indexed.insert(horizon, forecast.clone()).is_some() {
            return Err(RebuildEvaluationError::InvalidMaterialization(
                "duplicate forecast horizon",
            ));
        }
    }
    if indexed.len() != OutcomeHorizon::ALL.len() {
        return Err(RebuildEvaluationError::InvalidMaterialization(
            "missing forecast horizon",
        ));
    }
    Ok(indexed)
}

fn index_observations<'a>(
    schedule: &OutcomeSchedule,
    observations: &'a [GovernedHorizonObservation],
) -> RebuildEvaluationResult<BTreeMap<OutcomeHorizon, &'a GovernedHorizonObservation>> {
    let mut indexed = BTreeMap::new();
    for observation in observations {
        if !observation
            .horizon
            .is_due_after(observation.completed_trading_sessions)
            || observation.observed_trading_day <= schedule.baseline_trading_day
        {
            return Err(RebuildEvaluationError::InvalidMaterialization(
                "horizon is not due",
            ));
        }
        validate_prices(&observation.future_prices)?;
        if indexed.insert(observation.horizon, observation).is_some() {
            return Err(RebuildEvaluationError::InvalidMaterialization(
                "duplicate observation horizon",
            ));
        }
    }
    if indexed.len() != OutcomeHorizon::ALL.len() {
        return Err(RebuildEvaluationError::InvalidMaterialization(
            "missing observation horizon",
        ));
    }
    Ok(indexed)
}

fn validate_prices(prices: &BTreeMap<Asset, MoneyMicros>) -> RebuildEvaluationResult<()> {
    if prices.len() != Asset::EXECUTABLE.len()
        || Asset::EXECUTABLE
            .into_iter()
            .any(|asset| prices.get(&asset).is_none_or(|price| price.0 <= 0))
    {
        return Err(RebuildEvaluationError::InvalidMaterialization(
            "price surface must contain positive prices for the exact universe",
        ));
    }
    Ok(())
}

fn price(
    prices: &BTreeMap<Asset, MoneyMicros>,
    asset: Asset,
) -> RebuildEvaluationResult<MoneyMicros> {
    prices
        .get(&asset)
        .copied()
        .ok_or(RebuildEvaluationError::InvalidMaterialization(
            "price surface is incomplete",
        ))
}

fn return_ppm(baseline: MoneyMicros, future: MoneyMicros) -> RebuildEvaluationResult<i64> {
    if baseline.0 <= 0 || future.0 <= 0 {
        return Err(RebuildEvaluationError::InvalidMaterialization(
            "prices must be positive",
        ));
    }
    i64::try_from(
        (i128::from(future.0) - i128::from(baseline.0)) * i128::from(PPM_ONE)
            / i128::from(baseline.0),
    )
    .map_err(|_| RebuildEvaluationError::ArithmeticOverflow)
}

fn portfolio_return_ppm(
    target: &TargetPortfolio,
    baseline: &BTreeMap<Asset, MoneyMicros>,
    future: &BTreeMap<Asset, MoneyMicros>,
) -> RebuildEvaluationResult<i64> {
    let weighted = target
        .weights
        .iter()
        .try_fold(0_i128, |sum, (asset, weight)| {
            let asset_return = return_ppm(price(baseline, *asset)?, price(future, *asset)?)?;
            sum.checked_add(i128::from(weight.0) * i128::from(asset_return))
                .ok_or(RebuildEvaluationError::ArithmeticOverflow)
        })?;
    i64::try_from(weighted / i128::from(PPM_ONE))
        .map_err(|_| RebuildEvaluationError::ArithmeticOverflow)
}

fn directional_calibration_ppm(probability_ppm: u32, realized_return_ppm: i64) -> u32 {
    let realized = if realized_return_ppm > 0 { PPM_ONE } else { 0 };
    PPM_ONE - probability_ppm.abs_diff(realized)
}

fn bounded_ratio_ppm(expected: u64, observed: u64) -> u32 {
    if expected == 0 {
        return PPM_ONE;
    }
    let numerator = u128::from(observed.min(expected)) * u128::from(PPM_ONE);
    u32::try_from(numerator / u128::from(expected)).unwrap_or(PPM_ONE)
}

fn execution_verdict(lineage: &OutcomeExecutionLineage) -> &ArtifactRef {
    match lineage {
        OutcomeExecutionLineage::NoOrder { execution_verdict }
        | OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict, ..
        } => execution_verdict,
    }
}

fn require_canonical_purpose(purpose: RunPurpose) -> RebuildEvaluationResult<()> {
    if purpose.is_canonical_learning() {
        Ok(())
    } else {
        Err(RebuildEvaluationError::NonCanonicalPurpose(purpose))
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
    use std::collections::{BTreeMap, BTreeSet};

    use akzio_context::ContextBroker;
    use akzio_domain::{
        AgentContract, ArtifactId, ContextPolicy, ContractId, ContractPurpose, ExecutionVerdict,
        FailureDisposition, HardBlocker, NoOrder, OutputContract, RetryPolicy, RunId, TaskBudget,
        TaskId, TaskRecipeId, TaskStatus, TaskWritePermit, TerminationPolicy, WeightPpm,
        WorkflowGraph, WorkflowNode,
    };
    use akzio_domain::{DecisionHorizon, Forecast, MemoryId, OutcomeId};
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use chrono::{Duration, NaiveDate, TimeZone, Utc};
    use tempfile::{tempdir, TempDir};

    use super::*;

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn day(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
    }

    fn prices(tqqq: i64, qqq: i64) -> BTreeMap<Asset, MoneyMicros> {
        BTreeMap::from([
            (Asset::Tqqq, MoneyMicros(tqqq)),
            (Asset::Qqq, MoneyMicros(qqq)),
            (Asset::Soxx, MoneyMicros(100_000_000)),
            (Asset::Soxl, MoneyMicros(100_000_000)),
        ])
    }

    fn forecast(horizon: DecisionHorizon, probability: u32) -> Forecast {
        Forecast {
            horizon,
            positive_return_probability_ppm: probability,
            expected_return_ppm: 0,
        }
    }

    fn observation(
        horizon: OutcomeHorizon,
        sessions: u8,
        observed_day: u32,
        future_prices: BTreeMap<Asset, MoneyMicros>,
    ) -> GovernedHorizonObservation {
        GovernedHorizonObservation {
            horizon,
            completed_trading_sessions: sessions,
            observed_trading_day: day(observed_day),
            future_prices,
            expected_evidence_count: 4,
            observed_evidence_count: 3,
            expected_risk_count: 2,
            detected_risk_count: 1,
        }
    }

    fn materialization() -> OutcomeMaterializationInput {
        let outcome_id = OutcomeId::new();
        OutcomeMaterializationInput {
            schedule: OutcomeSchedule {
                schema_version: REBUILD_SCHEMA_VERSION,
                outcome_id,
                decision: reference(ArtifactKind::Decision, b"decision"),
                decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
                execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
                execution: OutcomeExecutionLineage::NoOrder {
                    execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"no-order"),
                },
                baseline_trading_day: day(3),
                created_at: Utc::now(),
            },
            schedule_artifact: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
            target: TargetPortfolio {
                weights: BTreeMap::from([
                    (Asset::Tqqq, WeightPpm(1_000_000)),
                    (Asset::Qqq, WeightPpm::ZERO),
                    (Asset::Soxx, WeightPpm::ZERO),
                    (Asset::Soxl, WeightPpm::ZERO),
                ]),
            },
            forecasts: vec![
                forecast(DecisionHorizon::T1, 800_000),
                forecast(DecisionHorizon::T3, 200_000),
                forecast(DecisionHorizon::T5, 500_000),
            ],
            baseline_prices: prices(100_000_000, 100_000_000),
            observations: vec![
                observation(OutcomeHorizon::T1, 1, 4, prices(110_000_000, 105_000_000)),
                observation(OutcomeHorizon::T3, 3, 6, prices(90_000_000, 95_000_000)),
                observation(OutcomeHorizon::T5, 5, 10, prices(100_000_000, 100_000_000)),
            ],
            market_evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"market")],
            sealed_at: Utc::now(),
        }
    }

    fn fixture_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn artifact_reference(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn fixture_artifact<T: Serialize>(
        store: &V2Store,
        permit: Option<&TaskWritePermit>,
        kind: ArtifactKind,
        lifecycle: ArtifactLifecycle,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        created_at: DateTime<Utc>,
    ) -> Artifact {
        let origin = permit.map(|permit| ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        });
        Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "learning.fixture",
            lifecycle,
            ArtifactProvenance {
                source_family: "learning.fixture".to_owned(),
                observed_at: Some(created_at),
                retrieved_at: created_at,
                source_uri: None,
                confidence_ppm: PPM_ONE,
                producer_contract_hash: permit.and_then(|permit| permit.contract_hash.clone()),
            },
            origin,
            source_refs,
            created_at,
        )
        .unwrap()
    }

    fn fixture_contract(
        store: &V2Store,
        label: &str,
        now: DateTime<Utc>,
    ) -> (AgentContract, ArtifactRef) {
        let contract = AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            format!("{label} contract"),
            store
                .put_bytes(format!("{label} prompt").as_bytes(), "text/plain")
                .unwrap(),
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
            vec![],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store
                    .put_json(&serde_json::json!({"type": "object"}))
                    .unwrap(),
            },
            TaskBudget {
                max_input_tokens: 256,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 0,
            },
            RetryPolicy::none(),
            TerminationPolicy::leaf(),
            FailureDisposition::FailTask,
        )
        .unwrap();
        let artifact = Artifact::new(
            ArtifactKind::Contract,
            store.put_json(&contract).unwrap(),
            "learning.fixture.contract",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "learning.fixture".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: PPM_ONE,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        store.write_bootstrap_artifact(&artifact).unwrap();
        (contract, artifact_reference(&artifact))
    }

    fn fixture_overlay_contract(store: &V2Store, now: DateTime<Utc>) -> AgentContract {
        let (mut contract, _) = fixture_contract(store, "overlay", now);
        contract.context = ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::CandidatePolicy]),
            permitted_source_families: BTreeSet::from(["akzio-learning".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: false,
        };
        contract.tool_grants.clear();
        contract.candidate_capability_ceiling.context = contract.context.clone();
        contract.candidate_capability_ceiling.tool_grants.clear();
        contract.contract_hash = contract.expected_hash().unwrap();
        contract.validate().unwrap();
        contract
    }

    fn fixture_workflow(
        store: &V2Store,
        purpose: RunPurpose,
        task_count: usize,
        contract_hash: Option<ContentHash>,
        created_at: DateTime<Utc>,
    ) -> StoredRun {
        let run_id = RunId::new();
        let topology_id = format!("fixture-{}", run_id.0);
        let mut previous: Option<TaskId> = None;
        let nodes = (0..task_count)
            .map(|index| {
                let task_id = TaskId::new();
                let dependencies = previous.iter().cloned().collect();
                previous = Some(task_id.clone());
                WorkflowNode {
                    task_id,
                    recipe_id: TaskRecipeId::new(format!("fixture.task.{index}")).unwrap(),
                    contract_hash: contract_hash.clone(),
                    objective: format!("fixture task {index}"),
                    dependencies,
                    input_artifacts: vec![],
                    priority: 50,
                    budget: TaskBudget {
                        max_input_tokens: 32,
                        max_output_tokens: 16,
                        max_wall_time_secs: 10,
                        max_tool_calls: 1,
                    },
                    retry: RetryPolicy::none(),
                    on_failure: FailureDisposition::FailRun,
                    parent_task_id: None,
                }
            })
            .collect::<Vec<_>>();
        let graph = WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id: topology_id.clone(),
            nodes: nodes.clone(),
        };
        let graph_artifact = fixture_artifact(
            store,
            None,
            ArtifactKind::WorkflowGraph,
            ArtifactLifecycle::RunScoped,
            &graph,
            vec![],
            created_at,
        );
        let run = StoredRun {
            run_id,
            purpose,
            topology_id,
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes,
            })
            .unwrap();
        run
    }

    fn claim_fixture_task(store: &V2Store, worker: &str, now: DateTime<Utc>) -> TaskWritePermit {
        store
            .claim_next_task(worker, now, Duration::minutes(5))
            .unwrap()
            .unwrap()
            .permit
    }

    struct RuntimeFixture {
        _root: TempDir,
        store: V2Store,
        runtime: RebuildEvaluationRuntime,
        paper_run_id: RunId,
        subject: PolicySubject,
        materialization: OutcomeMaterializationInput,
        parent_decision: ArtifactRef,
        execution_context: ArtifactRef,
        parent_outcome: ArtifactRef,
        candidates: Vec<(ArtifactRef, ArtifactRef)>,
        active_topology: ArtifactRef,
        candidate_topology: ArtifactRef,
        candidate_contract_hash: ContentHash,
        candidate_topology_id: String,
        pair_completed_at: DateTime<Utc>,
    }

    impl RuntimeFixture {
        fn new() -> Self {
            let root = tempdir().unwrap();
            let store = V2Store::open(root.path()).unwrap();
            let now = fixture_time();
            let sealed_at = now + Duration::hours(1);
            let candidate_contract_hash = ContentHash::of_bytes(b"candidate-contract");

            let shadow_run = fixture_workflow(
                &store,
                RunPurpose::Shadow,
                1,
                Some(candidate_contract_hash.clone()),
                now,
            );
            let shadow_permit = claim_fixture_task(&store, "shadow-worker", now);
            assert_eq!(shadow_permit.run_id, shadow_run.run_id);
            let candidate_decisions = (0..5)
                .map(|index| {
                    let artifact = fixture_artifact(
                        &store,
                        Some(&shadow_permit),
                        ArtifactKind::Decision,
                        ArtifactLifecycle::RunScoped,
                        &serde_json::json!({"candidate": index}),
                        vec![],
                        now,
                    );
                    store
                        .write_task_artifact(
                            &shadow_permit,
                            &artifact,
                            "shadow.decision.created",
                            now,
                        )
                        .unwrap();
                    artifact
                })
                .collect::<Vec<_>>();

            let paper_run = fixture_workflow(&store, RunPurpose::Paper, 7, None, now);
            let seed_permit = claim_fixture_task(&store, "paper-seed", now);
            assert_eq!(seed_permit.run_id, paper_run.run_id);
            let evidence = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::NormalizedEvidence,
                ArtifactLifecycle::Canonical,
                &serde_json::json!({"prices": "governed"}),
                vec![],
                now,
            );
            let parent_decision = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::Decision,
                ArtifactLifecycle::Canonical,
                &serde_json::json!({"decision": "parent"}),
                vec![artifact_reference(&evidence)],
                now,
            );
            let decision_context = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::DecisionContext,
                ArtifactLifecycle::Canonical,
                &serde_json::json!({"context": "parent"}),
                vec![artifact_reference(&evidence)],
                now,
            );
            let execution_context = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::ExecutionContext,
                ArtifactLifecycle::Canonical,
                &serde_json::json!({"execution_context": "paper"}),
                vec![artifact_reference(&decision_context)],
                now,
            );
            let execution_context_ref = artifact_reference(&execution_context);
            let verdict_payload = ExecutionVerdict::NoOrder {
                no_order: NoOrder {
                    execution_context: execution_context_ref.clone(),
                    blockers: vec![HardBlocker::Frozen],
                    created_at: now,
                },
            };
            let verdict = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::ExecutionVerdict,
                ArtifactLifecycle::Canonical,
                &verdict_payload,
                vec![execution_context_ref.clone()],
                now,
            );
            let verdict_ref = artifact_reference(&verdict);
            let parent_schedule = OutcomeSchedule {
                schema_version: REBUILD_SCHEMA_VERSION,
                outcome_id: OutcomeId::new(),
                decision: artifact_reference(&parent_decision),
                decision_context: artifact_reference(&decision_context),
                execution_context: execution_context_ref.clone(),
                execution: OutcomeExecutionLineage::NoOrder {
                    execution_verdict: verdict_ref.clone(),
                },
                baseline_trading_day: day(3),
                created_at: now,
            };
            let parent_schedule_artifact = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::OutcomeSchedule,
                ArtifactLifecycle::Canonical,
                &parent_schedule,
                vec![
                    parent_schedule.decision.clone(),
                    parent_schedule.decision_context.clone(),
                    parent_schedule.execution_context.clone(),
                    verdict_ref.clone(),
                ],
                now,
            );
            let evidence_ref = artifact_reference(&evidence);
            let mut parent_materialization = materialization();
            parent_materialization.schedule = parent_schedule;
            parent_materialization.schedule_artifact =
                artifact_reference(&parent_schedule_artifact);
            parent_materialization.market_evidence = vec![evidence_ref.clone()];
            parent_materialization.sealed_at = sealed_at;
            for observation in &mut parent_materialization.observations {
                observation.observed_evidence_count = observation.expected_evidence_count;
                observation.detected_risk_count = observation.expected_risk_count;
            }
            let parent_outcome_payload = materialize_outcome(&parent_materialization).unwrap();
            let parent_outcome = fixture_artifact(
                &store,
                Some(&seed_permit),
                ArtifactKind::Outcome,
                ArtifactLifecycle::Canonical,
                &parent_outcome_payload,
                vec![
                    parent_materialization.schedule_artifact.clone(),
                    evidence_ref.clone(),
                ],
                sealed_at,
            );

            let candidate_schedules = candidate_decisions
                .iter()
                .map(|candidate_decision| {
                    let schedule = OutcomeSchedule {
                        schema_version: REBUILD_SCHEMA_VERSION,
                        outcome_id: OutcomeId::new(),
                        decision: artifact_reference(candidate_decision),
                        decision_context: artifact_reference(&decision_context),
                        execution_context: execution_context_ref.clone(),
                        execution: OutcomeExecutionLineage::NoOrder {
                            execution_verdict: verdict_ref.clone(),
                        },
                        baseline_trading_day: day(3),
                        created_at: now,
                    };
                    let artifact = fixture_artifact(
                        &store,
                        Some(&shadow_permit),
                        ArtifactKind::OutcomeSchedule,
                        ArtifactLifecycle::RunScoped,
                        &schedule,
                        vec![
                            schedule.decision.clone(),
                            schedule.decision_context.clone(),
                            schedule.execution_context.clone(),
                            verdict_ref.clone(),
                        ],
                        now,
                    );
                    (schedule, artifact)
                })
                .collect::<Vec<_>>();

            let seed_artifacts = vec![
                evidence,
                parent_decision.clone(),
                decision_context,
                execution_context,
                verdict,
                parent_schedule_artifact,
            ];
            for artifact in &seed_artifacts {
                store
                    .write_task_artifact(&seed_permit, artifact, "paper.seed_artifact.created", now)
                    .unwrap();
            }
            store
                .commit_outcomes(
                    &seed_permit,
                    std::slice::from_ref(&parent_outcome),
                    sealed_at,
                )
                .unwrap();

            for (_, artifact) in &candidate_schedules {
                store
                    .write_task_artifact(
                        &shadow_permit,
                        artifact,
                        "shadow.outcome_schedule.created",
                        now,
                    )
                    .unwrap();
            }

            let candidate_outcomes = candidate_schedules
                .iter()
                .map(|(schedule, schedule_artifact)| {
                    let mut input = materialization();
                    input.schedule = schedule.clone();
                    input.schedule_artifact = artifact_reference(schedule_artifact);
                    input.market_evidence = vec![evidence_ref.clone()];
                    input.sealed_at = sealed_at;
                    let outcome = materialize_outcome(&input).unwrap();
                    fixture_artifact(
                        &store,
                        Some(&shadow_permit),
                        ArtifactKind::Outcome,
                        ArtifactLifecycle::RunScoped,
                        &outcome,
                        vec![input.schedule_artifact, evidence_ref.clone()],
                        sealed_at,
                    )
                })
                .collect::<Vec<_>>();
            store
                .commit_outcomes(&shadow_permit, &candidate_outcomes, sealed_at)
                .unwrap();

            let candidates = candidate_decisions
                .iter()
                .zip(candidate_outcomes.iter())
                .map(|(decision, outcome)| {
                    (artifact_reference(decision), artifact_reference(outcome))
                })
                .collect();
            let runtime =
                RebuildEvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
            let active_topology = ArtifactRef {
                artifact_id: paper_run.graph_artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            };
            let candidate_topology = ArtifactRef {
                artifact_id: shadow_run.graph_artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            };
            Self {
                _root: root,
                store,
                runtime,
                paper_run_id: paper_run.run_id,
                subject: PolicySubject::Memory(MemoryId::new()),
                materialization: parent_materialization,
                parent_decision: artifact_reference(&parent_decision),
                execution_context: execution_context_ref,
                parent_outcome: artifact_reference(&parent_outcome),
                candidates,
                active_topology,
                candidate_topology,
                candidate_contract_hash,
                candidate_topology_id: shadow_run.topology_id,
                pair_completed_at: sealed_at,
            }
        }

        fn claim_evaluation(&self, worker: &str) -> TaskWritePermit {
            let permit = claim_fixture_task(&self.store, worker, fixture_time());
            assert_eq!(permit.run_id, self.paper_run_id);
            permit
        }

        fn record_pair_batch(&self, permit: &TaskWritePermit, batch: usize) {
            self.record_pair_batch_for(permit, batch, &self.subject);
        }

        fn record_pair_batch_for(
            &self,
            permit: &TaskWritePermit,
            batch: usize,
            subject: &PolicySubject,
        ) {
            let (candidate_decision, candidate_outcome) = &self.candidates[batch];
            for horizon in OutcomeHorizon::ALL {
                self.runtime
                    .record_shadow_pair(
                        permit,
                        subject,
                        ShadowObservation {
                            parent_decision: self.parent_decision.clone(),
                            execution_context: self.execution_context.clone(),
                            candidate_decision: candidate_decision.clone(),
                            candidate_contract_hash: self.candidate_contract_hash.clone(),
                            candidate_topology_id: self.candidate_topology_id.clone(),
                            horizon,
                            parent_outcome: self.parent_outcome.clone(),
                            candidate_outcome: candidate_outcome.clone(),
                            completed_at: self.pair_completed_at,
                        },
                    )
                    .unwrap();
            }
        }

        fn evaluate(&self, permit: TaskWritePermit, hypothesis_id: &str) -> EvaluationResult {
            self.evaluate_for(
                permit,
                hypothesis_id,
                self.subject.clone(),
                None,
                self.materialization.clone(),
            )
        }

        fn evaluate_for(
            &self,
            permit: TaskWritePermit,
            hypothesis_id: &str,
            subject: PolicySubject,
            candidate_policy: Option<CandidatePolicyInput>,
            materialization: OutcomeMaterializationInput,
        ) -> EvaluationResult {
            let contract_hash = match &subject {
                PolicySubject::Contract(hash) => hash.clone(),
                _ => ContentHash::of_bytes(b"active-contract"),
            };
            let topology_id = match &subject {
                PolicySubject::Topology(topology_id) => topology_id.clone(),
                _ => TopologyId("active-topology".to_owned()),
            };
            self.runtime
                .evaluate(EvaluationInput {
                    permit,
                    subject,
                    hypothesis_id: hypothesis_id.to_owned(),
                    materialization,
                    contract_hash,
                    topology_id,
                    candidate_policy,
                    token_cost: 10,
                    latency_millis: 20,
                })
                .unwrap()
        }
    }

    #[test]
    fn rust_materializes_returns_calibration_completeness_and_recall() {
        let outcome = materialize_outcome(&materialization()).unwrap();
        assert_eq!(outcome.schedule.kind, ArtifactKind::OutcomeSchedule);
        assert_eq!(outcome.windows.len(), 3);

        let t1 = &outcome.windows[0];
        assert_eq!(t1.portfolio_return_ppm, 100_000);
        assert_eq!(t1.benchmark_return_ppm, 50_000);
        assert_eq!(t1.utility_ppm, 50_000);
        assert_eq!(t1.calibration_ppm, 800_000);
        assert_eq!(t1.evidence_completeness_ppm, 750_000);
        assert_eq!(t1.risk_recall_ppm, 500_000);

        let t3 = &outcome.windows[1];
        assert_eq!(t3.portfolio_return_ppm, -100_000);
        assert_eq!(t3.benchmark_return_ppm, -50_000);
        assert_eq!(t3.utility_ppm, -50_000);
        assert_eq!(t3.calibration_ppm, 800_000);
    }

    #[test]
    fn materializer_rejects_duplicate_and_missing_horizons() {
        let mut missing = materialization();
        missing.observations.pop();
        assert!(matches!(
            materialize_outcome(&missing),
            Err(RebuildEvaluationError::InvalidMaterialization(
                "missing observation horizon"
            ))
        ));

        let mut duplicate = materialization();
        duplicate
            .observations
            .push(duplicate.observations[0].clone());
        assert!(matches!(
            materialize_outcome(&duplicate),
            Err(RebuildEvaluationError::InvalidMaterialization(
                "duplicate observation horizon"
            ))
        ));

        let mut duplicate_forecast = materialization();
        duplicate_forecast
            .forecasts
            .push(forecast(DecisionHorizon::T1, 500_000));
        assert!(matches!(
            materialize_outcome(&duplicate_forecast),
            Err(RebuildEvaluationError::InvalidMaterialization(
                "duplicate forecast horizon"
            ))
        ));
    }

    #[test]
    fn materializer_rejects_not_due_and_incomplete_price_surfaces() {
        let mut not_due = materialization();
        not_due.observations[2].completed_trading_sessions = 4;
        assert!(matches!(
            materialize_outcome(&not_due),
            Err(RebuildEvaluationError::InvalidMaterialization(
                "horizon is not due"
            ))
        ));

        let mut incomplete = materialization();
        incomplete.observations[0].future_prices.remove(&Asset::Qqq);
        assert!(matches!(
            materialize_outcome(&incomplete),
            Err(RebuildEvaluationError::InvalidMaterialization(_))
        ));
    }

    #[test]
    fn every_nonpaper_purpose_is_rejected_for_canonical_learning() {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::Replay,
            RunPurpose::PaperDryRun,
            RunPurpose::Shadow,
        ] {
            assert!(matches!(
                require_canonical_purpose(purpose),
                Err(RebuildEvaluationError::NonCanonicalPurpose(actual)) if actual == purpose
            ));
        }
        require_canonical_purpose(RunPurpose::Paper).unwrap();
    }

    #[test]
    fn nonpaper_evaluation_cannot_write_learning_state_or_events() {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::Replay,
            RunPurpose::PaperDryRun,
            RunPurpose::Shadow,
        ] {
            let fixture = RuntimeFixture::new();
            let blocked_paper = fixture.claim_evaluation("block-paper-queue");
            fixture
                .store
                .finish_task(&blocked_paper, TaskStatus::Cancelled, fixture_time())
                .unwrap();
            let run = fixture_workflow(&fixture.store, purpose, 1, None, fixture_time());
            let permit = claim_fixture_task(&fixture.store, "nonpaper", fixture_time());
            assert_eq!(permit.run_id, run.run_id);
            let subject = PolicySubject::Memory(MemoryId::new());
            let error = fixture
                .runtime
                .evaluate(EvaluationInput {
                    permit: permit.clone(),
                    subject: subject.clone(),
                    hypothesis_id: "must-not-persist".to_owned(),
                    materialization: fixture.materialization.clone(),
                    contract_hash: ContentHash::of_bytes(b"active-contract"),
                    topology_id: TopologyId("active-topology".to_owned()),
                    candidate_policy: None,
                    token_cost: 1,
                    latency_millis: 1,
                })
                .unwrap_err();
            assert!(matches!(
                error,
                RebuildEvaluationError::NonCanonicalPurpose(actual) if actual == purpose
            ));
            assert!(fixture.store.policy_head(&subject).unwrap().is_none());
            assert_eq!(
                fixture
                    .store
                    .policy_shadow_pair_snapshot(&subject)
                    .unwrap()
                    .through_cursor,
                0
            );
            assert!(fixture
                .store
                .events_after(&run.run_id, 0, 100)
                .unwrap()
                .iter()
                .all(|event| !matches!(
                    event.event_type.as_str(),
                    "policy.evaluated" | "policy.transitioned" | "artifact.committed"
                )));
            fixture
                .store
                .finish_task(&permit, TaskStatus::Cancelled, fixture_time())
                .unwrap();
            fixture.store.verify_integrity().unwrap();
        }
    }

    #[test]
    fn memory_lifecycle_requires_pairs_and_degrades_to_retirement() {
        let subject = PolicySubject::Memory(MemoryId::new());
        assert_eq!(
            subject.initial_state(),
            PolicyState::Memory(MemoryLifecycle::Candidate)
        );
        assert_eq!(
            next_state(subject.initial_state(), true, false),
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

    #[test]
    fn noop_canonical_evaluation_consumes_fresh_pairs_once() {
        let fixture = RuntimeFixture::new();

        let first_permit = fixture.claim_evaluation("evaluation-1");
        fixture.record_pair_batch(&first_permit, 0);
        let first = fixture.evaluate(first_permit, "candidate-to-active");
        assert_eq!(first.fresh_pairs_by_horizon, [1, 1, 1]);
        assert_eq!(
            first.policy_head.as_ref().unwrap().state,
            PolicyState::Memory(MemoryLifecycle::Active)
        );

        let second_permit = fixture.claim_evaluation("evaluation-2");
        fixture.record_pair_batch(&second_permit, 1);
        let second = fixture.evaluate(second_permit, "active-to-proven");
        assert_eq!(second.fresh_pairs_by_horizon, [1, 1, 1]);
        assert_eq!(
            second.policy_head.as_ref().unwrap().state,
            PolicyState::Memory(MemoryLifecycle::Proven)
        );
        let cursor_before_noop = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap()
            .through_cursor;

        let noop_permit = fixture.claim_evaluation("evaluation-noop");
        fixture.record_pair_batch(&noop_permit, 2);
        let noop = fixture.evaluate(noop_permit, "proven-noop");
        let cursor_after_noop = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap()
            .through_cursor;
        assert_eq!(noop.fresh_pairs_by_horizon, [1, 1, 1]);
        assert_eq!(noop.policy_head, second.policy_head);
        assert!(cursor_after_noop > cursor_before_noop);
        assert_eq!(
            fixture
                .store
                .artifact(&noop.evaluation.artifact_id)
                .unwrap()
                .kind,
            ArtifactKind::Evaluation
        );

        let replay_permit = fixture.claim_evaluation("evaluation-old-pairs");
        let old_pairs = fixture.evaluate(replay_permit, "old-pairs-cannot-replay");
        assert_eq!(old_pairs.fresh_pairs_by_horizon, [0, 0, 0]);
        assert_eq!(old_pairs.policy_head, noop.policy_head);
        assert_ne!(old_pairs.evaluation, noop.evaluation);
        assert_eq!(
            fixture
                .store
                .policy_shadow_pair_snapshot(&fixture.subject)
                .unwrap()
                .through_cursor,
            cursor_after_noop
        );
        assert_eq!(
            fixture
                .store
                .policy_transitions(&fixture.subject)
                .unwrap()
                .len(),
            2
        );

        let evaluated = fixture
            .store
            .events_after(&fixture.paper_run_id, 0, 100)
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "policy.evaluated")
            .collect::<Vec<_>>();
        assert_eq!(evaluated.len(), 4);
        assert!(evaluated
            .iter()
            .any(|event| event.artifact_id.as_ref() == Some(&noop.evaluation.artifact_id)));
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn topology_canary_requires_fresh_pairs_and_rolls_back_on_degradation() {
        let fixture = RuntimeFixture::new();
        let subject = PolicySubject::Topology(TopologyId(fixture.candidate_topology_id.clone()));
        let candidate_policy = CandidatePolicyInput {
            baseline: fixture.active_topology.clone(),
            candidate: fixture.candidate_topology.clone(),
        };
        let expected = [
            CandidatePolicyState::Canary10,
            CandidatePolicyState::Canary25,
            CandidatePolicyState::Canary50,
            CandidatePolicyState::Active,
        ];

        let mut active_policy = None;
        for (batch, state) in expected.into_iter().enumerate() {
            let permit = fixture.claim_evaluation(&format!("topology-canary-{batch}"));
            fixture.record_pair_batch_for(&permit, batch, &subject);
            if batch == 0 {
                fixture.record_pair_batch_for(&permit, batch, &subject);
            }
            let task_id = permit.task_id.clone();
            let result = fixture.evaluate_for(
                permit,
                &format!("topology-canary-{batch}"),
                subject.clone(),
                Some(candidate_policy.clone()),
                fixture.materialization.clone(),
            );
            assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 1]);
            assert_eq!(
                result.policy_head.as_ref().unwrap().state,
                PolicyState::Topology(state)
            );
            let policy_ref = result.candidate_policy.unwrap();
            let policy_artifact = fixture.store.artifact(&policy_ref.artifact_id).unwrap();
            let policy: CandidatePolicy =
                serde_json::from_slice(&fixture.store.read_blob(&policy_artifact.blob).unwrap())
                    .unwrap();
            policy.validate().unwrap();
            assert_eq!(policy.subject, subject);
            assert_eq!(policy.source_evaluation, result.evaluation);
            assert!(fixture
                .store
                .committed_task_outputs(&fixture.paper_run_id, &task_id)
                .unwrap()
                .iter()
                .any(|artifact| artifact.artifact_id == policy_ref.artifact_id));
            if state == CandidatePolicyState::Active {
                active_policy = Some(policy_ref);
            }
        }
        let rollback_permit = fixture.claim_evaluation("topology-rollback");
        let active_policy = active_policy.unwrap();
        let broker = ContextBroker::new(fixture.store.clone());
        let overlay_contract = fixture_overlay_contract(&fixture.store, fixture_time());
        let overlay_run = fixture_workflow(
            &fixture.store,
            RunPurpose::Paper,
            1,
            Some(overlay_contract.contract_hash.clone()),
            fixture_time(),
        );
        let overlay_permit = claim_fixture_task(&fixture.store, "topology-overlay", fixture_time());
        assert_eq!(overlay_permit.run_id, overlay_run.run_id);
        let manifest = broker
            .assemble(
                &overlay_permit,
                &overlay_contract,
                [active_policy.clone()],
                fixture_time(),
                Duration::minutes(5),
            )
            .unwrap();
        assert_eq!(
            broker
                .policy_influences(
                    &overlay_permit,
                    &overlay_contract,
                    &manifest,
                    fixture_time(),
                )
                .unwrap(),
            vec![active_policy]
        );
        let mut forged = manifest.clone();
        forged.payload.selections.clear();
        assert!(matches!(
            broker.policy_influences(&overlay_permit, &overlay_contract, &forged, fixture_time(),),
            Err(akzio_context::ContextError::InvalidManifestClosure)
        ));

        fixture.record_pair_batch_for(&rollback_permit, 4, &subject);
        let mut degraded = fixture.materialization.clone();
        for observation in &mut degraded.observations {
            observation.expected_risk_count = 1;
            observation.detected_risk_count = 0;
        }
        let rollback = fixture.evaluate_for(
            rollback_permit,
            "topology-rollback",
            subject.clone(),
            Some(candidate_policy),
            degraded,
        );
        assert_eq!(
            rollback.policy_head.unwrap().state,
            PolicyState::Topology(CandidatePolicyState::Candidate)
        );
        assert_eq!(fixture.store.policy_transitions(&subject).unwrap().len(), 5);
        let completed_pairs = fixture
            .store
            .events_after(&fixture.paper_run_id, 0, 500)
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "shadow_pair.completed")
            .count();
        assert_eq!(completed_pairs, 15);
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn contract_candidate_materializes_a_bound_policy_artifact() {
        let fixture = RuntimeFixture::new();
        let now = fixture_time();
        let (baseline, baseline_ref) = fixture_contract(&fixture.store, "baseline", now);
        let (candidate, candidate_ref) = fixture_contract(&fixture.store, "candidate", now);
        assert!(baseline.permits_candidate(&candidate));
        let subject = PolicySubject::Contract(candidate.contract_hash.clone());
        let permit = fixture.claim_evaluation("contract-candidate");
        let task_id = permit.task_id.clone();
        let result = fixture.evaluate_for(
            permit,
            "contract-candidate",
            subject.clone(),
            Some(CandidatePolicyInput {
                baseline: baseline_ref.clone(),
                candidate: candidate_ref.clone(),
            }),
            fixture.materialization.clone(),
        );
        assert_eq!(result.fresh_pairs_by_horizon, [0, 0, 0]);
        assert!(result.policy_head.is_none());
        let policy_ref = result.candidate_policy.unwrap();
        let artifact = fixture.store.artifact(&policy_ref.artifact_id).unwrap();
        let policy: CandidatePolicy =
            serde_json::from_slice(&fixture.store.read_blob(&artifact.blob).unwrap()).unwrap();
        assert_eq!(policy.subject, subject);
        assert_eq!(policy.baseline, baseline_ref);
        assert_eq!(policy.candidate, candidate_ref);
        assert_eq!(policy.source_evaluation, result.evaluation);
        assert!(fixture
            .store
            .committed_task_outputs(&fixture.paper_run_id, &task_id)
            .unwrap()
            .iter()
            .any(|output| output.artifact_id == policy_ref.artifact_id));
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn shadow_outcome_schedule_requires_run_scoped_mixed_closure() {
        let fixture = RuntimeFixture::new();
        let now = fixture_time();
        fixture
            .store
            .request_run_cancel(&fixture.paper_run_id, "isolate schedule boundary test", now)
            .unwrap();

        let debug_run = fixture_workflow(
            &fixture.store,
            RunPurpose::Debug,
            1,
            None,
            now - Duration::days(2),
        );
        let debug_permit = claim_fixture_task(&fixture.store, "debug-context", now);
        assert_eq!(debug_permit.run_id, debug_run.run_id);
        let debug_context = fixture_artifact(
            &fixture.store,
            Some(&debug_permit),
            ArtifactKind::DecisionContext,
            ArtifactLifecycle::RunScoped,
            &serde_json::json!({"context": "debug"}),
            vec![],
            now,
        );
        fixture
            .store
            .commit_attempt(
                &debug_permit,
                std::slice::from_ref(&debug_context),
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        let shadow_run = fixture_workflow(
            &fixture.store,
            RunPurpose::Shadow,
            1,
            Some(fixture.candidate_contract_hash.clone()),
            now - Duration::days(1),
        );
        let shadow_permit = claim_fixture_task(&fixture.store, "shadow-schedule", now);
        assert_eq!(shadow_permit.run_id, shadow_run.run_id);
        let candidate_decision = fixture_artifact(
            &fixture.store,
            Some(&shadow_permit),
            ArtifactKind::Decision,
            ArtifactLifecycle::RunScoped,
            &serde_json::json!({"candidate": "schedule-boundary"}),
            vec![],
            now,
        );
        fixture
            .store
            .write_task_artifact(
                &shadow_permit,
                &candidate_decision,
                "shadow.decision.created",
                now,
            )
            .unwrap();

        let build_outcome = |decision_context: ArtifactRef,
                             schedule_lifecycle: ArtifactLifecycle| {
            let mut schedule = fixture.materialization.schedule.clone();
            schedule.outcome_id = OutcomeId::new();
            schedule.decision = artifact_reference(&candidate_decision);
            schedule.decision_context = decision_context;
            schedule.created_at = now;
            let schedule_artifact = fixture_artifact(
                &fixture.store,
                Some(&shadow_permit),
                ArtifactKind::OutcomeSchedule,
                schedule_lifecycle,
                &schedule,
                vec![
                    schedule.decision.clone(),
                    schedule.decision_context.clone(),
                    schedule.execution_context.clone(),
                    execution_verdict(&schedule.execution).clone(),
                ],
                now,
            );
            fixture
                .store
                .write_task_artifact(
                    &shadow_permit,
                    &schedule_artifact,
                    "shadow.outcome_schedule.created",
                    now,
                )
                .unwrap();

            let mut materialization = fixture.materialization.clone();
            materialization.schedule = schedule;
            materialization.schedule_artifact = artifact_reference(&schedule_artifact);
            let outcome = materialize_outcome(&materialization).unwrap();
            let outcome_artifact = fixture_artifact(
                &fixture.store,
                Some(&shadow_permit),
                ArtifactKind::Outcome,
                ArtifactLifecycle::RunScoped,
                &outcome,
                std::iter::once(materialization.schedule_artifact.clone())
                    .chain(materialization.market_evidence.iter().cloned())
                    .collect(),
                materialization.sealed_at,
            );
            (schedule_artifact, outcome_artifact)
        };

        let paper_decision_context = fixture.materialization.schedule.decision_context.clone();
        let (_, canonical_schedule_outcome) =
            build_outcome(paper_decision_context.clone(), ArtifactLifecycle::Canonical);
        assert!(matches!(
            fixture.store.commit_outcomes(
                &shadow_permit,
                &[canonical_schedule_outcome],
                fixture.materialization.sealed_at,
            ),
            Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact"
            ))
        ));

        let (_, debug_closure_outcome) = build_outcome(
            artifact_reference(&debug_context),
            ArtifactLifecycle::RunScoped,
        );
        assert!(matches!(
            fixture.store.commit_outcomes(
                &shadow_permit,
                &[debug_closure_outcome],
                fixture.materialization.sealed_at,
            ),
            Err(StoreError::InvalidLearningCommit(
                "learning_artifact.run_purpose"
            ))
        ));

        let (mixed_schedule_artifact, mixed_outcome) =
            build_outcome(paper_decision_context, ArtifactLifecycle::RunScoped);
        fixture
            .store
            .commit_outcomes(
                &shadow_permit,
                &[mixed_outcome],
                fixture.materialization.sealed_at,
            )
            .unwrap();
        assert_eq!(
            mixed_schedule_artifact.lifecycle,
            ArtifactLifecycle::RunScoped
        );
        let schedule: OutcomeSchedule = serde_json::from_slice(
            &fixture
                .store
                .read_blob(&mixed_schedule_artifact.blob)
                .unwrap(),
        )
        .unwrap();
        let purpose = |reference: &ArtifactRef| {
            let artifact = fixture.store.artifact(&reference.artifact_id).unwrap();
            let run_id = artifact.origin.unwrap().run_id.unwrap();
            fixture.store.run_purpose(&run_id).unwrap()
        };
        assert_eq!(purpose(&schedule.decision), RunPurpose::Shadow);
        assert_eq!(purpose(&schedule.decision_context), RunPurpose::Paper);
        assert_eq!(purpose(&schedule.execution_context), RunPurpose::Paper);
        assert_eq!(
            purpose(execution_verdict(&schedule.execution)),
            RunPurpose::Paper
        );
    }
}
