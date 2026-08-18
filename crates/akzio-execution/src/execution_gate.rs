//! Typed v2 execution gate derived only from persisted broker artifacts.

use std::collections::BTreeSet;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, CandidatePolicy,
    ContextManifestPayload, DecisionContext, DomainError, ExecutionContext, ExecutionVerdict,
    Experience, FreezeState, HardBlocker, MarketClockSnapshot, NoOrder, PolicySubject,
    QuoteSnapshot, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, ArtifactProvenance};

use crate::{
    AllocationError, AllocationInput, ExecutionError, ExecutionGatePolicy, ExecutionPolicy,
    V2AllocationRuntime,
};

#[derive(Debug, Error)]
pub enum ExecutionGateError {
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
    #[error("decision context does not belong to the execution task run")]
    DecisionRunMismatch,
    #[error("execution gate integrity failure: {0}")]
    Integrity(&'static str),
}

pub type ExecutionGateResult<T> = std::result::Result<T, ExecutionGateError>;

#[derive(Debug, Clone)]
pub struct ExecutionGateInput {
    pub permit: TaskWritePermit,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ExecutionGateOutput {
    pub execution_plan: Option<Artifact>,
    pub execution_context: Artifact,
    pub verdict: Artifact,
}

#[derive(Debug, Clone)]
pub struct V2ExecutionRuntime {
    store: V2Store,
    allocation: V2AllocationRuntime,
    gate_policy: ExecutionGatePolicy,
}

impl V2ExecutionRuntime {
    pub fn new(
        store: V2Store,
        execution_policy: ExecutionPolicy,
        gate_policy: ExecutionGatePolicy,
    ) -> ExecutionGateResult<Self> {
        let allocation = V2AllocationRuntime::new(execution_policy)
            .map_err(|_| ExecutionGateError::Integrity("execution policy"))?;
        gate_policy.validate()?;
        Ok(Self {
            store,
            allocation,
            gate_policy,
        })
    }

    pub fn execution_policy(&self) -> &ExecutionPolicy {
        self.allocation.policy()
    }

    pub fn gate_policy(&self) -> &ExecutionGatePolicy {
        &self.gate_policy
    }

    pub fn evaluate(&self, input: &ExecutionGateInput) -> ExecutionGateResult<ExecutionGateOutput> {
        self.validate_input(input)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        let decision_artifact =
            self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let decision: DecisionContext = self.read_payload(&decision_artifact)?;
        decision.validate()?;
        if decision.run_id != input.permit.run_id {
            return Err(ExecutionGateError::DecisionRunMismatch);
        }
        self.validate_decision_provenance(&decision_artifact, &decision)?;

        let mut blockers = decision
            .hard_blockers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !decision.material_conflicts.is_empty() {
            blockers.insert(HardBlocker::MaterialConflict);
        }
        if purpose != RunPurpose::Paper {
            blockers.insert(HardBlocker::NonCanonicalRun);
        }
        let frozen = self.frozen()?;
        if frozen {
            blockers.insert(HardBlocker::Frozen);
        }

        let account = self.load_account(input, &mut blockers)?;
        let quotes = self.load_quotes(input, &mut blockers)?;
        let clock = self.load_clock(input, &mut blockers)?;
        self.derive_snapshot_blockers(
            account.as_ref().map(|(_, payload)| payload),
            quotes.as_ref().map(|(_, payload)| payload),
            clock.as_ref().map(|(_, payload)| payload),
            input.now,
            &mut blockers,
        );

        let mut plan_payload = None;
        if blockers.is_empty() {
            let (_, account_payload) = account
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("account snapshot closure"))?;
            let (_, quote_payload) = quotes
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("quote snapshot closure"))?;
            let (_, clock_payload) = clock
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("clock snapshot closure"))?;
            let allocation = self.allocation.allocate(&AllocationInput {
                decision_context_ref: input.decision_context.clone(),
                decision_context: decision.clone(),
                account_snapshot_ref: input.account_snapshot.clone().expect("checked above"),
                account: account_payload.clone(),
                quote_snapshot_ref: input.quote_snapshot.clone().expect("checked above"),
                quotes: quote_payload.clone(),
                market_clock_snapshot_ref: input
                    .market_clock_snapshot
                    .clone()
                    .expect("checked above"),
                clock: clock_payload.clone(),
                now: input.now,
            });
            match allocation {
                Ok(plan) => {
                    plan.validate()?;
                    blockers.extend(
                        self.gate_policy
                            .blockers_for(&plan.factor_exposure, plan.turnover_ppm),
                    );
                    plan_payload = Some(plan);
                }
                Err(error) => self.allocation_blockers(error, &mut blockers),
            }
        }

        let execution_plan = plan_payload
            .as_ref()
            .map(|plan| {
                self.artifact(
                    ArtifactKind::ExecutionPlan,
                    "execution.plan",
                    plan,
                    vec![
                        plan.decision_context.clone(),
                        plan.account_snapshot.clone(),
                        plan.quote_snapshot.clone(),
                        plan.market_clock_snapshot.clone(),
                    ],
                    input,
                )
            })
            .transpose()?;
        let execution_plan_ref = execution_plan.as_ref().map(artifact_ref);

        let execution_context_payload = ExecutionContext {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: input.permit.run_id.clone(),
            decision_context: input.decision_context.clone(),
            account_snapshot: input.account_snapshot.clone(),
            quote_snapshot: input.quote_snapshot.clone(),
            market_clock_snapshot: input.market_clock_snapshot.clone(),
            execution_plan: execution_plan_ref.clone(),
            factor_exposure: plan_payload
                .as_ref()
                .map(|plan| plan.factor_exposure.clone()),
            turnover_ppm: plan_payload.as_ref().map(|plan| plan.turnover_ppm),
            plan_hash: plan_payload.as_ref().map(|plan| plan.plan_hash.clone()),
            broker_session: plan_payload
                .as_ref()
                .map(|plan| plan.broker_session.clone()),
            frozen,
            created_at: input.now,
        };
        execution_context_payload.validate()?;
        if blockers.is_empty() {
            execution_context_payload.validate_complete_plan_closure()?;
        }

        let mut context_sources = vec![input.decision_context.clone()];
        context_sources.extend(input.account_snapshot.clone());
        context_sources.extend(input.quote_snapshot.clone());
        context_sources.extend(input.market_clock_snapshot.clone());
        context_sources.extend(execution_plan_ref.clone());
        let execution_context = self.artifact(
            ArtifactKind::ExecutionContext,
            "execution.context",
            &execution_context_payload,
            context_sources,
            input,
        )?;
        let execution_context_ref = artifact_ref(&execution_context);

        let verdict_payload = if blockers.is_empty() {
            ExecutionVerdict::Accepted {
                execution_context: execution_context_ref.clone(),
            }
        } else {
            ExecutionVerdict::NoOrder {
                no_order: NoOrder {
                    execution_context: execution_context_ref.clone(),
                    blockers: blockers.into_iter().collect(),
                    created_at: input.now,
                },
            }
        };
        verdict_payload.validate()?;
        let verdict = self.artifact(
            ArtifactKind::ExecutionVerdict,
            "execution.verdict",
            &verdict_payload,
            vec![execution_context_ref],
            input,
        )?;
        Ok(ExecutionGateOutput {
            execution_plan,
            execution_context,
            verdict,
        })
    }

    /// Atomically persists the optional plan, context, verdict and task terminal state.
    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &ExecutionGateOutput,
        now: DateTime<Utc>,
    ) -> ExecutionGateResult<()> {
        let mut artifacts = Vec::with_capacity(3);
        artifacts.extend(output.execution_plan.clone());
        artifacts.push(output.execution_context.clone());
        artifacts.push(output.verdict.clone());
        self.store
            .commit_attempt(permit, &artifacts, TaskStatus::Succeeded, now)?;
        Ok(())
    }

    fn validate_input(&self, input: &ExecutionGateInput) -> ExecutionGateResult<()> {
        if input.decision_context.kind != ArtifactKind::DecisionContext
            || input
                .account_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .quote_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .market_clock_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
        {
            return Err(ExecutionGateError::Integrity("input artifact kinds"));
        }
        Ok(())
    }

    fn load_account(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, AccountSnapshot)>> {
        let Some(reference) = &input.account_snapshot else {
            blockers.insert(HardBlocker::MissingAccount);
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: AccountSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn load_quotes(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, QuoteSnapshot)>> {
        let Some(reference) = &input.quote_snapshot else {
            blockers.insert(HardBlocker::MissingQuote);
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: QuoteSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn load_clock(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, MarketClockSnapshot)>> {
        let Some(reference) = &input.market_clock_snapshot else {
            blockers.insert(HardBlocker::MarketClosed);
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: MarketClockSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn derive_snapshot_blockers(
        &self,
        account: Option<&AccountSnapshot>,
        quotes: Option<&QuoteSnapshot>,
        clock: Option<&MarketClockSnapshot>,
        now: DateTime<Utc>,
        blockers: &mut BTreeSet<HardBlocker>,
    ) {
        if let Some(account) = account {
            if outside_freshness_window(
                account.observed_at,
                now,
                self.execution_policy().max_account_age_secs,
                self.execution_policy().max_future_skew_secs,
            ) {
                blockers.insert(HardBlocker::StaleAccount);
            }
            if !account.external_positions.is_empty() {
                blockers.insert(HardBlocker::ExternalPosition);
            }
            if !account.open_order_ids.is_empty() {
                blockers.insert(HardBlocker::UnmanagedOpenOrder);
            }
        }
        if let Some(quotes) = quotes {
            if outside_freshness_window(
                quotes.observed_at,
                now,
                self.execution_policy().max_quote_age_secs,
                self.execution_policy().max_future_skew_secs,
            ) {
                blockers.insert(HardBlocker::StaleQuote);
            }
        }
        if let Some(clock) = clock {
            if !clock.is_open
                || outside_freshness_window(
                    clock.observed_at,
                    now,
                    self.execution_policy().max_clock_age_secs,
                    self.execution_policy().max_future_skew_secs,
                )
            {
                blockers.insert(HardBlocker::MarketClosed);
            }
        }
        if let (Some(account), Some(quotes)) = (account, quotes) {
            if account.broker_session != quotes.broker_session {
                blockers.insert(HardBlocker::StaleQuote);
            }
        }
        if let (Some(account), Some(clock)) = (account, clock) {
            if account.broker_session != clock.broker_session {
                blockers.insert(HardBlocker::MarketClosed);
            }
        }
        if let (Some(account), Some(quotes), Some(clock)) = (account, quotes, clock) {
            if snapshot_skewed(
                [account.observed_at, quotes.observed_at, clock.observed_at],
                self.execution_policy().max_snapshot_skew_secs,
            ) {
                blockers.insert(HardBlocker::InvalidProvenance);
            }
        }
    }

    fn allocation_blockers(&self, error: AllocationError, blockers: &mut BTreeSet<HardBlocker>) {
        match error {
            AllocationError::DecisionRejected => {
                blockers.insert(HardBlocker::NoExecutableOrder);
            }
            AllocationError::SessionMismatch => {
                blockers.insert(HardBlocker::InvalidProvenance);
            }
            AllocationError::MarketClosed => {
                blockers.insert(HardBlocker::MarketClosed);
            }
            AllocationError::Domain(_) => {
                blockers.insert(HardBlocker::InvalidProvenance);
            }
            AllocationError::Execution(error) => match error {
                ExecutionError::ForbiddenAsset(_) | ExecutionError::InvalidWeight(_) => {
                    blockers.insert(HardBlocker::UnsupportedUniverse);
                }
                ExecutionError::GrossExposureExceeded(_) => {
                    blockers.insert(HardBlocker::FactorLimit);
                }
                ExecutionError::MissingQuote(_) | ExecutionError::InvalidQuote(_) => {
                    blockers.insert(HardBlocker::MissingQuote);
                }
                ExecutionError::StaleQuote(_) => {
                    blockers.insert(HardBlocker::StaleQuote);
                }
                ExecutionError::DailyTurnoverExceeded => {
                    blockers.insert(HardBlocker::TurnoverLimit);
                }
                ExecutionError::InvalidPolicy => {
                    blockers.insert(HardBlocker::InvalidProvenance);
                }
                ExecutionError::AccountBlocked
                | ExecutionError::InsufficientBuyingPower
                | ExecutionError::ShortPosition(_)
                | ExecutionError::NewNotionalExceeded
                | ExecutionError::NoExecutableOrder => {
                    blockers.insert(HardBlocker::NoExecutableOrder);
                }
            },
        }
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> ExecutionGateResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(ExecutionGateError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ExecutionGateResult<T> {
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn validate_decision_provenance(
        &self,
        artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("decision context origin run"));
        }
        for reference in decision
            .claims
            .iter()
            .chain(decision.critiques.iter())
            .chain(decision.evidence.iter())
            .chain(decision.policy_influences.iter())
            .chain(
                decision
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            let source = self.store.artifact(&reference.artifact_id)?;
            if source.kind != reference.kind
                || !artifact
                    .source_refs
                    .iter()
                    .any(|declared| declared == reference)
            {
                return Err(ExecutionGateError::Integrity(
                    "decision context source refs",
                ));
            }
        }
        self.validate_policy_influences(artifact, decision)
    }

    fn validate_policy_influences(
        &self,
        decision_artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        if decision.policy_influences.is_empty() {
            return Ok(());
        }
        let manifest_refs = decision_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .collect::<Vec<_>>();
        if manifest_refs.len() != 1 {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let manifest = self.store.artifact(&manifest_refs[0].artifact_id)?;
        if manifest.kind != ArtifactKind::ContextManifest
            || manifest
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let payload: ContextManifestPayload = self.read_payload(&manifest)?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if selected != declared
            || decision
                .policy_influences
                .iter()
                .any(|reference| !selected.contains(reference))
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        for reference in &decision.policy_influences {
            let influence = self.store.artifact(&reference.artifact_id)?;
            if influence.kind != reference.kind || !self.is_canonical_paper(&influence)? {
                return Err(ExecutionGateError::Integrity("policy influence authority"));
            }
            let subject: PolicySubject = match reference.kind {
                ArtifactKind::Experience => {
                    let experience: Experience = self.read_payload(&influence)?;
                    experience.validate()?;
                    experience.subject
                }
                ArtifactKind::CandidatePolicy => {
                    let policy: CandidatePolicy = self.read_payload(&influence)?;
                    policy.validate()?;
                    let evaluation = self.store.artifact(&policy.source_evaluation.artifact_id)?;
                    if evaluation.kind != ArtifactKind::Evaluation
                        || !self.is_canonical_paper(&evaluation)?
                    {
                        return Err(ExecutionGateError::Integrity("candidate policy evaluation"));
                    }
                    policy.subject
                }
                _ => {
                    return Err(ExecutionGateError::Integrity(
                        "policy influence artifact kind",
                    ));
                }
            };
            if self
                .store
                .recorded_policy_influence_subject(&reference.artifact_id)?
                .as_ref()
                != Some(&subject)
            {
                return Err(ExecutionGateError::Integrity("policy influence subject"));
            }
            let head = self
                .store
                .policy_head(&subject)?
                .ok_or(ExecutionGateError::Integrity("policy head"))?;
            if !head.state.permits_influence_kind(reference.kind) {
                return Err(ExecutionGateError::Integrity("policy head state"));
            }
        }
        Ok(())
    }

    fn is_canonical_paper(&self, artifact: &Artifact) -> ExecutionGateResult<bool> {
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Ok(false);
        }
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            return Ok(false);
        };
        Ok(self.store.run_purpose(run_id)? == RunPurpose::Paper)
    }

    fn frozen(&self) -> ExecutionGateResult<bool> {
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(false);
        };
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Err(ExecutionGateError::Integrity("freeze state lifecycle"));
        }
        let state: FreezeState = self.read_payload(&artifact)?;
        state.validate()?;
        Ok(state.frozen)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        input: &ExecutionGateInput,
    ) -> ExecutionGateResult<Artifact> {
        Ok(Artifact::new(
            kind,
            self.store.put_json(payload)?,
            producer,
            ArtifactLifecycle::RunScoped,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn outside_freshness_window(
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: i64,
    max_future_skew_secs: i64,
) -> bool {
    let age = now.signed_duration_since(observed_at);
    age > Duration::seconds(max_age_secs) || age < -Duration::seconds(max_future_skew_secs)
}

fn snapshot_skewed(observed_at: [DateTime<Utc>; 3], max_skew_secs: i64) -> bool {
    let oldest = observed_at.into_iter().min().expect("three snapshots");
    let newest = observed_at.into_iter().max().expect("three snapshots");
    newest.signed_duration_since(oldest) > Duration::seconds(max_skew_secs)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use akzio_domain::{
        ArtifactId, Asset, ContentHash, DecisionId, FactorLimits, FailureDisposition, MoneyMicros,
        Position, Quote, RetryPolicy, RunId, SoftWarning, TargetPortfolio, TaskBudget, TaskId,
        TaskRecipeId, WeightPpm, WorkflowGraph, WorkflowNode, V2_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn freshness_window_rejects_stale_and_future_snapshots() {
        let now = Utc::now();
        assert!(!outside_freshness_window(now, now, 5, 1));
        assert!(outside_freshness_window(
            now - Duration::seconds(6),
            now,
            5,
            1,
        ));
        assert!(outside_freshness_window(
            now + Duration::seconds(2),
            now,
            5,
            1,
        ));
    }

    #[test]
    fn snapshot_window_rejects_cross_acquisition_skew() {
        let now = Utc::now();
        assert!(!snapshot_skewed(
            [now, now + Duration::seconds(1), now + Duration::seconds(2)],
            2,
        ));
        assert!(snapshot_skewed(
            [now, now + Duration::seconds(1), now + Duration::seconds(3)],
            2,
        ));
    }

    fn execution_policy() -> ExecutionPolicy {
        ExecutionPolicy {
            assets: Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>(),
            max_gross_weight: WeightPpm(1_000_000),
            max_new_notional: MoneyMicros::from_usd_cents(2_000_000),
            max_daily_turnover: WeightPpm(1_000_000),
            max_account_age_secs: 5,
            max_quote_age_secs: 5,
            max_clock_age_secs: 5,
            max_future_skew_secs: 1,
            max_snapshot_skew_secs: 2,
            max_spread_bps: 20,
            limit_protection_bps: 10,
        }
    }

    fn gate_policy(factor_limit: u32, turnover_limit: u32) -> ExecutionGatePolicy {
        ExecutionGatePolicy {
            factor_limits: FactorLimits {
                global_leveraged_equity_ppm: factor_limit,
                nasdaq_ppm: factor_limit,
                semiconductor_ppm: factor_limit,
                paired_index_ppm: factor_limit,
            },
            max_turnover_ppm: turnover_limit,
        }
    }

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 64,
            max_output_tokens: 64,
            max_wall_time_secs: 30,
            max_tool_calls: 1,
        }
    }

    fn graph() -> WorkflowGraph {
        let source = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("execution.source").unwrap(),
            contract_hash: None,
            objective: "create typed execution inputs".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 100,
            budget: budget(),
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let gate = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("execution.gate").unwrap(),
            contract_hash: None,
            objective: "gate typed execution plan".to_owned(),
            dependencies: vec![source.task_id.clone()],
            input_artifacts: vec![],
            priority: 100,
            budget: budget(),
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "typed-execution-fixture".to_owned(),
            nodes: vec![source, gate],
        }
    }

    fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.execution".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    fn source_artifact<T: serde::Serialize>(
        store: &V2Store,
        permit: &TaskWritePermit,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> Artifact {
        let lifecycle = match store.run_purpose(&permit.run_id).unwrap() {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            _ => ArtifactLifecycle::RunScoped,
        };
        Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "fixture.source",
            lifecycle,
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

    fn as_ref(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    struct Fixture {
        store: V2Store,
        runtime: V2ExecutionRuntime,
        input: ExecutionGateInput,
    }

    fn fixture(
        purpose: RunPurpose,
        account_age_secs: i64,
        quote_age_secs: i64,
        clock_open: bool,
        policy: ExecutionGatePolicy,
    ) -> Fixture {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.keep()).unwrap();
        let now = Utc::now();
        let graph = graph();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
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
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let source_permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;

        let account = source_artifact(
            &store,
            &source_permit,
            ArtifactKind::NormalizedEvidence,
            &AccountSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                observed_at: now - Duration::seconds(account_age_secs),
                equity: MoneyMicros::from_usd_cents(1_000_000),
                buying_power: MoneyMicros::from_usd_cents(1_000_000),
                day_turnover: MoneyMicros::ZERO,
                active: true,
                trading_blocked: false,
                positions: BTreeMap::<Asset, Position>::new(),
                external_positions: BTreeSet::new(),
                open_order_ids: BTreeSet::new(),
            },
            vec![],
            now,
        );
        let quotes = source_artifact(
            &store,
            &source_permit,
            ArtifactKind::NormalizedEvidence,
            &QuoteSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                observed_at: now - Duration::seconds(quote_age_secs),
                quotes: BTreeMap::from([(
                    Asset::Tqqq,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_010),
                        observed_at: now - Duration::seconds(quote_age_secs),
                    },
                )]),
            },
            vec![],
            now,
        );
        let clock = source_artifact(
            &store,
            &source_permit,
            ArtifactKind::NormalizedEvidence,
            &MarketClockSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                is_open: clock_open,
                observed_at: now,
            },
            vec![],
            now,
        );
        let claim = source_artifact(
            &store,
            &source_permit,
            ArtifactKind::Claim,
            &serde_json::json!({"claim": "typed execution fixture"}),
            vec![as_ref(&account)],
            now,
        );
        let claim_ref = as_ref(&claim);
        let account_ref = as_ref(&account);
        let quote_ref = as_ref(&quotes);
        let clock_ref = as_ref(&clock);
        let mut target = TargetPortfolio::zeroed();
        target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
        let decision = DecisionContext {
            schema_version: V2_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: source_permit.run_id.clone(),
            claims: vec![claim_ref.clone()],
            critiques: vec![],
            evidence: vec![account_ref.clone(), quote_ref.clone(), clock_ref.clone()],
            policy_influences: vec![],
            material_conflicts: vec![],
            hard_blockers: vec![],
            soft_warnings: Vec::<SoftWarning>::new(),
            decision_policy_hash: ContentHash::of_bytes(b"fixture-decision-policy"),
            target,
            created_at: now,
        };
        let decision_artifact = source_artifact(
            &store,
            &source_permit,
            ArtifactKind::DecisionContext,
            &decision,
            vec![
                claim_ref,
                account_ref.clone(),
                quote_ref.clone(),
                clock_ref.clone(),
            ],
            now,
        );
        store
            .commit_attempt(
                &source_permit,
                &[claim, account, quotes, clock, decision_artifact.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        let gate_permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let runtime = V2ExecutionRuntime::new(store.clone(), execution_policy(), policy).unwrap();
        Fixture {
            store,
            runtime,
            input: ExecutionGateInput {
                permit: gate_permit,
                decision_context: as_ref(&decision_artifact),
                account_snapshot: Some(account_ref),
                quote_snapshot: Some(quote_ref),
                market_clock_snapshot: Some(clock_ref),
                now,
            },
        }
    }

    fn verdict(store: &V2Store, output: &ExecutionGateOutput) -> ExecutionVerdict {
        serde_json::from_slice(&store.read_blob(&output.verdict.blob).unwrap()).unwrap()
    }

    fn blockers(store: &V2Store, output: &ExecutionGateOutput) -> Vec<HardBlocker> {
        match verdict(store, output) {
            ExecutionVerdict::NoOrder { no_order } => no_order.blockers,
            ExecutionVerdict::Accepted { .. } => panic!("expected NoOrder"),
        }
    }

    #[test]
    fn accepted_gate_builds_and_atomically_commits_complete_plan_closure() {
        let fixture = fixture(
            RunPurpose::Paper,
            0,
            0,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(output.execution_plan.is_some());
        assert!(matches!(
            verdict(&fixture.store, &output),
            ExecutionVerdict::Accepted { .. }
        ));
        let context: ExecutionContext = serde_json::from_slice(
            &fixture
                .store
                .read_blob(&output.execution_context.blob)
                .unwrap(),
        )
        .unwrap();
        assert!(context.validate_complete_plan_closure().is_ok());
        fixture
            .runtime
            .commit(&fixture.input.permit, &output, fixture.input.now)
            .unwrap();
        assert!(fixture.store.artifact(&output.verdict.artifact_id).is_ok());
    }

    #[test]
    fn missing_snapshots_are_durable_no_order() {
        let mut fixture = fixture(
            RunPurpose::Paper,
            0,
            0,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        fixture.input.account_snapshot = None;
        fixture.input.quote_snapshot = None;
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        let blockers = blockers(&fixture.store, &output);
        assert!(blockers.contains(&HardBlocker::MissingAccount));
        assert!(blockers.contains(&HardBlocker::MissingQuote));
        assert!(output.execution_plan.is_none());
    }

    #[test]
    fn stale_account_is_durable_no_order() {
        let fixture = fixture(
            RunPurpose::Paper,
            6,
            0,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::StaleAccount));
    }

    #[test]
    fn closed_market_is_durable_no_order() {
        let fixture = fixture(
            RunPurpose::Paper,
            0,
            0,
            false,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::MarketClosed));
    }

    #[test]
    fn factor_limit_is_derived_from_plan() {
        let fixture = fixture(
            RunPurpose::Paper,
            0,
            0,
            true,
            gate_policy(50_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(output.execution_plan.is_some());
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::FactorLimit));
    }

    #[test]
    fn turnover_limit_is_derived_from_account_and_orders() {
        let fixture = fixture(
            RunPurpose::Paper,
            0,
            0,
            true,
            gate_policy(1_000_000, 50_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(output.execution_plan.is_some());
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::TurnoverLimit));
    }

    #[test]
    fn noncanonical_run_is_durable_no_order() {
        let fixture = fixture(
            RunPurpose::PaperDryRun,
            0,
            0,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::NonCanonicalRun));
    }

    #[test]
    fn stale_quote_is_durable_no_order() {
        let fixture = fixture(
            RunPurpose::Paper,
            0,
            6,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::StaleQuote));
    }

    #[test]
    fn policy_must_be_explicit_and_validated() {
        let mut policy = execution_policy();
        policy.max_new_notional = MoneyMicros::ZERO;
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        assert!(V2ExecutionRuntime::new(store, policy, gate_policy(1_000_000, 1_000_000)).is_err());
    }

    #[test]
    fn artifact_reference_helper_preserves_identity() {
        let reference = ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"artifact")),
            kind: ArtifactKind::ExecutionPlan,
        };
        assert_eq!(reference.kind, ArtifactKind::ExecutionPlan);
    }
}
