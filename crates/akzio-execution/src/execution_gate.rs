//! Typed v2 execution gate.
//!
//! It converts a persisted `DecisionContext` into a run-scoped
//! `ExecutionContext` plus exactly one typed verdict. The approved branch is
//! the only input eligible for a later Paper commitment; rejection remains an
//! auditable `NoOrder` artifact.

use std::collections::BTreeSet;

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    DecisionContext, DomainError, ExecutionContext, ExecutionVerdict, FactorExposure, FreezeState,
    HardBlocker, NoOrder, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{policy::ExecutionGatePolicy, ExecutionPlan};

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
    #[error("execution gate input invalid: {0}")]
    InvalidInput(&'static str),
}

pub type ExecutionGateResult<T> = std::result::Result<T, ExecutionGateError>;

/// Rust-supplied gate inputs. Freshness and market state originate from the
/// trusted adapter/scheduler, never a model turn.
#[derive(Debug, Clone)]
pub struct ExecutionGateInput {
    pub permit: TaskWritePermit,
    pub decision_context: ArtifactRef,
    pub account_snapshot: ArtifactRef,
    pub quote_snapshot: ArtifactRef,
    pub allocation_plan: ArtifactRef,
    pub broker_session: String,
    pub plan_hash: akzio_domain::ContentHash,
    pub market_open: bool,
    pub account_fresh: bool,
    pub quotes_fresh: bool,
    pub factor_exposure: FactorExposure,
    pub turnover_ppm: u32,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ExecutionGateOutput {
    pub execution_context: Artifact,
    pub verdict: Artifact,
}

#[derive(Debug, Clone)]
pub struct V2ExecutionRuntime {
    store: V2Store,
    policy: ExecutionGatePolicy,
}

impl V2ExecutionRuntime {
    pub fn new(store: V2Store, policy: ExecutionGatePolicy) -> ExecutionGateResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &ExecutionGatePolicy {
        &self.policy
    }

    /// Build the two immutable gate artifacts. The caller commits both through
    /// `commit` in the task attempt transaction.
    pub fn evaluate(&self, input: &ExecutionGateInput) -> ExecutionGateResult<ExecutionGateOutput> {
        self.validate_input(input)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        let decision_artifact =
            self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let decision: DecisionContext =
            serde_json::from_slice(&self.store.read_blob(&decision_artifact.blob)?)?;
        decision.validate()?;
        if decision.run_id != input.permit.run_id {
            return Err(ExecutionGateError::DecisionRunMismatch);
        }
        self.validate_decision_provenance(&decision_artifact, &decision)?;

        let account =
            self.load_expected(&input.account_snapshot, ArtifactKind::NormalizedEvidence)?;
        let quotes = self.load_expected(&input.quote_snapshot, ArtifactKind::NormalizedEvidence)?;
        let allocation_artifact =
            self.load_expected(&input.allocation_plan, ArtifactKind::ExecutionPlan)?;
        let allocation: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&allocation_artifact.blob)?)?;

        let frozen = self.frozen()?;
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
        if frozen {
            blockers.insert(HardBlocker::Frozen);
        }
        if !input.market_open {
            blockers.insert(HardBlocker::MarketClosed);
        }
        if !input.account_fresh {
            blockers.insert(HardBlocker::StaleAccount);
        }
        if !input.quotes_fresh {
            blockers.insert(HardBlocker::StaleQuote);
        }
        if account.lifecycle != ArtifactLifecycle::Canonical
            || quotes.lifecycle != ArtifactLifecycle::Canonical
        {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        if allocation.plan_hash != input.plan_hash {
            blockers.insert(HardBlocker::PlanHashMismatch);
        }
        if allocation.orders.is_empty() {
            blockers.insert(HardBlocker::NoExecutableOrder);
        }
        if allocation_artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(&input.permit.run_id)
            || [
                &input.decision_context,
                &input.account_snapshot,
                &input.quote_snapshot,
            ]
            .into_iter()
            .any(|reference| {
                !allocation_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == reference)
            })
        {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        blockers.extend(
            self.policy
                .blockers_for(&input.factor_exposure, input.turnover_ppm),
        );

        let execution_context_payload = ExecutionContext {
            schema_version: akzio_domain::REBUILD_SCHEMA_VERSION,
            run_id: input.permit.run_id.clone(),
            decision_context: input.decision_context.clone(),
            account_snapshot: input.account_snapshot.clone(),
            quote_snapshot: input.quote_snapshot.clone(),
            factor_exposure: input.factor_exposure.clone(),
            turnover_ppm: input.turnover_ppm,
            plan_hash: input.plan_hash.clone(),
            broker_session: input.broker_session.clone(),
            frozen,
            created_at: input.now,
        };
        execution_context_payload.validate()?;
        let execution_context = self.artifact(
            ArtifactKind::ExecutionContext,
            "execution.context",
            &execution_context_payload,
            vec![
                input.decision_context.clone(),
                input.account_snapshot.clone(),
                input.quote_snapshot.clone(),
                input.allocation_plan.clone(),
            ],
            input,
        )?;
        let execution_context_ref = ArtifactRef {
            artifact_id: execution_context.artifact_id.clone(),
            kind: execution_context.kind,
        };

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
            execution_context,
            verdict,
        })
    }

    /// Atomically persist the complete gate result and finish the task. A
    /// `NoOrder` is a successful, terminal execution-gate result.
    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &ExecutionGateOutput,
        now: DateTime<Utc>,
    ) -> ExecutionGateResult<()> {
        self.store.commit_attempt(
            permit,
            &[output.execution_context.clone(), output.verdict.clone()],
            TaskStatus::Succeeded,
            now,
        )?;
        Ok(())
    }

    fn validate_input(&self, input: &ExecutionGateInput) -> ExecutionGateResult<()> {
        if input.broker_session.trim().is_empty() {
            return Err(ExecutionGateError::InvalidInput("broker_session"));
        }
        if input.turnover_ppm > 1_000_000 {
            return Err(ExecutionGateError::InvalidInput("turnover_ppm"));
        }
        input.factor_exposure.validate()?;
        if input.decision_context.kind != ArtifactKind::DecisionContext
            || input.account_snapshot.kind != ArtifactKind::NormalizedEvidence
            || input.quote_snapshot.kind != ArtifactKind::NormalizedEvidence
            || input.allocation_plan.kind != ArtifactKind::ExecutionPlan
        {
            return Err(ExecutionGateError::InvalidInput("input artifact kinds"));
        }
        Ok(())
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
            return Err(ExecutionGateError::Domain(DomainError::EmptyField {
                field: "decision_context.origin.run_id",
            }));
        }

        for reference in decision
            .claims
            .iter()
            .chain(decision.critiques.iter())
            .chain(decision.evidence.iter())
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
                return Err(ExecutionGateError::Domain(DomainError::EmptyField {
                    field: "decision_context.source_refs",
                }));
            }
        }
        Ok(())
    }

    fn frozen(&self) -> ExecutionGateResult<bool> {
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(false);
        };
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Err(ExecutionGateError::Domain(DomainError::EmptyField {
                field: "freeze_state.lifecycle",
            }));
        }
        let state: FreezeState = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
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
            ArtifactProvenance {
                source_family: "akzio.execution".to_owned(),
                observed_at: Some(input.now),
                retrieved_at: input.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: input.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(input.permit.run_id.clone()),
                task_id: Some(input.permit.task_id.clone()),
                attempt_id: Some(input.permit.attempt_id.clone()),
                contract_hash: input.permit.contract_hash.clone(),
            }),
            source_refs,
            input.now,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use tempfile::tempdir;

    use akzio_domain::{
        ArtifactLifecycle, ArtifactProvenance, DecisionId, FactorLimits, FailureDisposition,
        RetryPolicy, RunId, SoftWarning, TargetPortfolio, TaskBudget, TaskId, TaskRecipeId,
        WorkflowGraph, WorkflowNode, REBUILD_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};

    use super::*;

    fn policy(limit: u32) -> ExecutionGatePolicy {
        ExecutionGatePolicy {
            factor_limits: FactorLimits {
                global_leveraged_equity_ppm: limit,
                nasdaq_ppm: limit,
                semiconductor_ppm: limit,
                paired_index_ppm: limit,
            },
            max_turnover_ppm: limit,
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
            source_family: "fixture.execution".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
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

    fn graph() -> WorkflowGraph {
        let source = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("execution.source").unwrap(),
            contract_hash: None,
            objective: "create inputs".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 100,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let gate = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("execution.gate").unwrap(),
            contract_hash: None,
            objective: "gate paper execution".to_owned(),
            dependencies: vec![source.task_id.clone()],
            input_artifacts: vec![],
            priority: 100,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id: "execution-fixture".to_owned(),
            nodes: vec![source, gate],
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
        Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "fixture.source",
            ArtifactLifecycle::Canonical,
            provenance(now),
            Some(origin(permit)),
            source_refs,
            now,
        )
        .unwrap()
    }

    fn record_freeze(store: &V2Store, frozen: bool, now: DateTime<Utc>) {
        let state = FreezeState {
            schema_version: REBUILD_SCHEMA_VERSION,
            frozen,
            reason: "fixture freeze".to_owned(),
            changed_at: now,
        };
        let artifact = Artifact::new(
            ArtifactKind::FreezeState,
            store.put_json(&state).unwrap(),
            "fixture.freeze",
            ArtifactLifecycle::Canonical,
            provenance(now),
            None,
            vec![],
            now,
        )
        .unwrap();
        store.write_bootstrap_artifact(&artifact).unwrap();
    }

    fn seed_inputs(
        store: &V2Store,
        source_permit: &TaskWritePermit,
        now: DateTime<Utc>,
        hard_blockers: Vec<HardBlocker>,
        include_order: bool,
    ) -> (
        ArtifactRef,
        ArtifactRef,
        ArtifactRef,
        ArtifactRef,
        akzio_domain::ContentHash,
        TaskWritePermit,
    ) {
        let claim = source_artifact(
            store,
            source_permit,
            ArtifactKind::Claim,
            &serde_json::json!({"claim": "fixture"}),
            vec![],
            now,
        );
        let account = source_artifact(
            store,
            source_permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"account": "fixture"}),
            vec![],
            now,
        );
        let quote = source_artifact(
            store,
            source_permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"quote": "fixture"}),
            vec![],
            now,
        );
        let claim_ref = ArtifactRef {
            artifact_id: claim.artifact_id.clone(),
            kind: claim.kind,
        };
        let account_ref = ArtifactRef {
            artifact_id: account.artifact_id.clone(),
            kind: account.kind,
        };
        let quote_ref = ArtifactRef {
            artifact_id: quote.artifact_id.clone(),
            kind: quote.kind,
        };
        let decision = DecisionContext {
            schema_version: REBUILD_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: source_permit.run_id.clone(),
            claims: vec![claim_ref.clone()],
            critiques: vec![],
            evidence: vec![account_ref.clone(), quote_ref.clone()],
            material_conflicts: vec![],
            hard_blockers,
            soft_warnings: Vec::<SoftWarning>::new(),
            target: TargetPortfolio::zeroed(),
            created_at: now,
        };
        let decision_artifact = source_artifact(
            store,
            source_permit,
            ArtifactKind::DecisionContext,
            &decision,
            vec![claim_ref, account_ref.clone(), quote_ref.clone()],
            now,
        );
        let plan = ExecutionPlan {
            policy: crate::ExecutionPolicy::default(),
            targets: vec![],
            orders: include_order
                .then_some(crate::OrderIntent {
                    asset: akzio_domain::Asset::Tqqq,
                    side: crate::OrderSide::Buy,
                    notional: crate::MoneyMicros::from_usd_cents(10_000),
                    limit_price: crate::MoneyMicros::from_usd_cents(2_500),
                })
                .into_iter()
                .collect(),
            plan_hash: akzio_domain::ContentHash::of_bytes(b"fixture-allocation"),
        };
        let allocation_artifact = source_artifact(
            store,
            source_permit,
            ArtifactKind::ExecutionPlan,
            &plan,
            vec![
                ArtifactRef {
                    artifact_id: decision_artifact.artifact_id.clone(),
                    kind: decision_artifact.kind,
                },
                account_ref.clone(),
                quote_ref.clone(),
            ],
            now,
        );
        store
            .commit_attempt(
                source_permit,
                &[
                    claim,
                    account,
                    quote,
                    decision_artifact.clone(),
                    allocation_artifact.clone(),
                ],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        let gate_permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let decision_ref = ArtifactRef {
            artifact_id: decision_artifact.artifact_id,
            kind: ArtifactKind::DecisionContext,
        };
        let allocation_ref = ArtifactRef {
            artifact_id: allocation_artifact.artifact_id,
            kind: ArtifactKind::ExecutionPlan,
        };
        (
            decision_ref,
            account_ref,
            quote_ref,
            allocation_ref,
            plan.plan_hash,
            gate_permit,
        )
    }

    #[test]
    fn accepted_paper_gate_persists_a_typed_accepted_verdict() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
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
            purpose: RunPurpose::Paper,
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
        let (
            decision_context,
            account_snapshot,
            quote_snapshot,
            allocation_plan,
            plan_hash,
            gate_permit,
        ) = seed_inputs(&store, &source_permit, now, vec![], true);
        let runtime = V2ExecutionRuntime::new(store.clone(), policy(100)).unwrap();
        let output = runtime
            .evaluate(&ExecutionGateInput {
                permit: gate_permit.clone(),
                decision_context,
                account_snapshot,
                quote_snapshot,
                allocation_plan,
                broker_session: "paper:fixture".to_owned(),
                plan_hash,
                market_open: true,
                account_fresh: true,
                quotes_fresh: true,
                factor_exposure: FactorExposure {
                    leveraged_equity_ppm: 100,
                    nasdaq_ppm: 100,
                    semiconductor_ppm: 100,
                    tqqq_qqq_pair_ppm: 100,
                    soxl_soxx_pair_ppm: 100,
                },
                turnover_ppm: 100,
                now,
            })
            .unwrap();
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&store.read_blob(&output.verdict.blob).unwrap()).unwrap();
        assert!(matches!(verdict, ExecutionVerdict::Accepted { .. }));
        runtime.commit(&gate_permit, &output, now).unwrap();
        assert_eq!(
            store.artifact(&output.verdict.artifact_id).unwrap().kind,
            ArtifactKind::ExecutionVerdict
        );
    }

    #[test]
    fn noncanonical_or_unsafe_inputs_become_audited_no_order() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
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
            purpose: RunPurpose::PaperDryRun,
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
        let (
            decision_context,
            account_snapshot,
            quote_snapshot,
            allocation_plan,
            plan_hash,
            gate_permit,
        ) = seed_inputs(
            &store,
            &source_permit,
            now,
            vec![HardBlocker::MissingEvidence],
            false,
        );
        let runtime = V2ExecutionRuntime::new(store.clone(), policy(100)).unwrap();
        record_freeze(&store, true, now);
        let output = runtime
            .evaluate(&ExecutionGateInput {
                permit: gate_permit,
                decision_context,
                account_snapshot,
                quote_snapshot,
                allocation_plan,
                broker_session: "paper:fixture".to_owned(),
                plan_hash,
                market_open: false,
                account_fresh: false,
                quotes_fresh: false,
                factor_exposure: FactorExposure {
                    leveraged_equity_ppm: 101,
                    nasdaq_ppm: 0,
                    semiconductor_ppm: 0,
                    tqqq_qqq_pair_ppm: 101,
                    soxl_soxx_pair_ppm: 0,
                },
                turnover_ppm: 101,
                now,
            })
            .unwrap();
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&store.read_blob(&output.verdict.blob).unwrap()).unwrap();
        let ExecutionVerdict::NoOrder { no_order } = verdict else {
            panic!("expected no-order verdict");
        };
        assert_eq!(
            no_order.blockers,
            vec![
                HardBlocker::NoExecutableOrder,
                HardBlocker::Frozen,
                HardBlocker::MissingEvidence,
                HardBlocker::StaleQuote,
                HardBlocker::StaleAccount,
                HardBlocker::MarketClosed,
                HardBlocker::FactorLimit,
                HardBlocker::PairExposureLimit,
                HardBlocker::TurnoverLimit,
                HardBlocker::NonCanonicalRun,
            ]
        );
    }
}
