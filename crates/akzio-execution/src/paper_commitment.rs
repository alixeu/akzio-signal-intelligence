//! Fenced durable Paper commitment for the v2 execution path.
//!
//! This module deliberately stops before network I/O. It proves an accepted
//! Rust verdict, the scheduler-owned session slot, the daemon epoch and the
//! active task permit in one Store transaction. The adapter may only receive
//! a commitment returned from here.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, DomainError, ExecutionContext, ExecutionVerdict, FreezeState, PaperCommitment,
    PaperCommitmentId, RunPurpose, TaskWritePermit,
};
use akzio_store::v2::{DaemonLease, ExecutionCommit, StoreError, V2Store};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PaperCommitmentError {
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
    #[error("Paper commitment requires a Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("Paper commitment requires an accepted execution verdict")]
    VerdictRejected,
    #[error("accepted verdict execution context does not match the stored context")]
    VerdictContextMismatch,
    #[error("Paper commitment session does not match execution context")]
    SessionMismatch,
    #[error("frozen execution context cannot create a Paper commitment")]
    Frozen,
    #[error("execution context has no persisted allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match execution context")]
    PlanHashMismatch,
    #[error("allocation plan contains multiple orders for {0}")]
    DuplicateAssetOrder(Asset),
    #[error("session already contains a different Paper commitment")]
    ExistingCommitmentMismatch,
}

pub type PaperCommitmentResult<T> = std::result::Result<T, PaperCommitmentError>;

#[derive(Debug, Clone)]
pub struct PaperCommitmentInput {
    pub lease: DaemonLease,
    pub permit: TaskWritePermit,
    pub verdict: ArtifactRef,
    pub session_key: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PaperCommitmentOutput {
    pub commitment: Artifact,
    pub newly_committed: bool,
}

#[derive(Debug, Clone)]
pub struct V2PaperCommitmentRuntime {
    store: V2Store,
}

impl V2PaperCommitmentRuntime {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    /// Persist or recover the one commitment permitted in this broker session.
    /// The result is durable before a Paper adapter can receive its client IDs.
    pub fn commit(
        &self,
        input: &PaperCommitmentInput,
    ) -> PaperCommitmentResult<PaperCommitmentOutput> {
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperCommitmentError::NonPaperRun(purpose));
        }
        if let Some(freeze_artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        {
            let freeze: FreezeState =
                serde_json::from_slice(&self.store.read_blob(&freeze_artifact.blob)?)?;
            freeze.validate()?;
            if freeze.frozen {
                return Err(PaperCommitmentError::Frozen);
            }
        }

        let verdict_artifact =
            self.load_expected(&input.verdict, ArtifactKind::ExecutionVerdict)?;
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.store.read_blob(&verdict_artifact.blob)?)?;
        verdict.validate()?;
        let ExecutionVerdict::Accepted { execution_context } = verdict else {
            return Err(PaperCommitmentError::VerdictRejected);
        };
        let context_artifact =
            self.load_expected(&execution_context, ArtifactKind::ExecutionContext)?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        if context.run_id != input.permit.run_id
            || !verdict_artifact
                .source_refs
                .iter()
                .any(|source| source == &execution_context)
        {
            return Err(PaperCommitmentError::VerdictContextMismatch);
        }
        if context.broker_session != input.session_key {
            return Err(PaperCommitmentError::SessionMismatch);
        }
        if context.frozen {
            return Err(PaperCommitmentError::Frozen);
        }
        let allocation_reference = context_artifact
            .source_refs
            .iter()
            .find(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .cloned()
            .ok_or(PaperCommitmentError::MissingAllocationPlan)?;
        let allocation_artifact =
            self.load_expected(&allocation_reference, ArtifactKind::ExecutionPlan)?;
        let allocation: crate::ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&allocation_artifact.blob)?)?;
        if allocation.plan_hash != context.plan_hash {
            return Err(PaperCommitmentError::PlanHashMismatch);
        }
        let mut client_order_ids = std::collections::BTreeMap::new();
        for (index, order) in allocation.orders.iter().enumerate() {
            let client_order_id = crate::paper::client_order_id(&allocation.plan_hash, index, 0);
            if client_order_ids
                .insert(order.asset, client_order_id)
                .is_some()
            {
                return Err(PaperCommitmentError::DuplicateAssetOrder(order.asset));
            }
        }

        if let Some(slot) = self.store.session_slot(&input.session_key)? {
            if let Some(existing_id) = slot.commitment_artifact_id {
                let existing_artifact = self.store.artifact(&existing_id)?;
                if existing_artifact.kind != ArtifactKind::ExecutionCommitment {
                    return Err(PaperCommitmentError::WrongArtifactKind {
                        expected: ArtifactKind::ExecutionCommitment,
                        actual: existing_artifact.kind,
                    });
                }
                let existing: PaperCommitment =
                    serde_json::from_slice(&self.store.read_blob(&existing_artifact.blob)?)?;
                existing.validate()?;
                if existing.execution_context != execution_context
                    || existing.plan_hash != context.plan_hash
                    || existing.broker_session != input.session_key
                    || existing.client_order_ids != client_order_ids
                {
                    return Err(PaperCommitmentError::ExistingCommitmentMismatch);
                }
                return Ok(PaperCommitmentOutput {
                    commitment: existing_artifact,
                    newly_committed: false,
                });
            }
        }

        let payload = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: execution_context.clone(),
            plan_hash: context.plan_hash.clone(),
            broker_session: input.session_key.clone(),
            client_order_ids,
            created_at: input.now,
        };
        payload.validate()?;
        let commitment = Artifact::new(
            ArtifactKind::ExecutionCommitment,
            self.store.put_json(&payload)?,
            "execution.paper_commitment",
            ArtifactLifecycle::Canonical,
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
            vec![input.verdict.clone(), execution_context],
            input.now,
        )?;
        let result = self.store.commit_execution(
            &input.lease,
            &ExecutionCommit {
                session_key: input.session_key.clone(),
                permit: input.permit.clone(),
                commitment: commitment.clone(),
                committed_at: input.now,
            },
        )?;
        let commitment = if result.newly_committed {
            commitment
        } else {
            self.store.artifact(&result.commitment_artifact_id)?
        };

        Ok(PaperCommitmentOutput {
            commitment,
            newly_committed: result.newly_committed,
        })
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> PaperCommitmentResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(PaperCommitmentError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use tempfile::tempdir;

    use akzio_domain::{
        ArtifactId, ContentHash, FactorExposure, FailureDisposition, RetryPolicy, RunId,
        TaskBudget, TaskId, TaskRecipeId, WorkflowGraph, WorkflowNode, REBUILD_SCHEMA_VERSION,
    };
    use akzio_store::v2::{SessionReservation, StoredRun, WorkflowCommit};

    use super::*;

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

    fn workflow() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id: "paper-commitment-fixture".to_owned(),
            nodes: vec![WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("execution.commitment").unwrap(),
                contract_hash: None,
                objective: "commit paper execution".to_owned(),
                dependencies: vec![],
                input_artifacts: vec![],
                priority: 100,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            }],
        }
    }

    fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.paper".to_owned(),
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

    #[test]
    fn accepted_verdict_creates_one_fenced_commitment_and_reuses_it() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "fixture-daemon",
                now,
                now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let graph = workflow();
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
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        let reservation = store
            .reserve_session_slot(
                &lease,
                &SessionReservation {
                    session_key: "paper:fixture".to_owned(),
                    workflow,
                    reserved_at: now,
                },
            )
            .unwrap();
        store.commit_workflow(&reservation.slot.workflow).unwrap();
        let permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;

        let allocation = crate::ExecutionPlan {
            policy: crate::ExecutionPolicy::default(),
            targets: vec![],
            orders: vec![crate::OrderIntent {
                asset: Asset::Qqq,
                side: crate::OrderSide::Buy,
                notional: crate::MoneyMicros::from_usd_cents(10_000),
                limit_price: crate::MoneyMicros::from_usd_cents(2_500),
            }],
            plan_hash: ContentHash::of_bytes(b"plan"),
        };
        let allocation_artifact = Artifact::new(
            ArtifactKind::ExecutionPlan,
            store.put_json(&allocation).unwrap(),
            "fixture.allocation",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            Some(origin(&permit)),
            vec![],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(
                &permit,
                &allocation_artifact,
                "execution.allocation_created",
                now,
            )
            .unwrap();
        let allocation_ref = ArtifactRef {
            artifact_id: allocation_artifact.artifact_id.clone(),
            kind: ArtifactKind::ExecutionPlan,
        };
        let execution_context_payload = ExecutionContext {
            schema_version: REBUILD_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            decision_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"decision")),
                kind: ArtifactKind::DecisionContext,
            },
            account_snapshot: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"account")),
                kind: ArtifactKind::NormalizedEvidence,
            },
            quote_snapshot: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"quote")),
                kind: ArtifactKind::NormalizedEvidence,
            },
            factor_exposure: FactorExposure {
                leveraged_equity_ppm: 0,
                nasdaq_ppm: 0,
                semiconductor_ppm: 0,
                tqqq_qqq_pair_ppm: 0,
                soxl_soxx_pair_ppm: 0,
            },
            turnover_ppm: 0,
            plan_hash: allocation.plan_hash.clone(),
            broker_session: "paper:fixture".to_owned(),
            frozen: false,
            created_at: now,
        };
        let context = Artifact::new(
            ArtifactKind::ExecutionContext,
            store.put_json(&execution_context_payload).unwrap(),
            "fixture.execution_context",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            Some(origin(&permit)),
            vec![allocation_ref],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(&permit, &context, "execution.context_created", now)
            .unwrap();
        let context_ref = ArtifactRef {
            artifact_id: context.artifact_id.clone(),
            kind: ArtifactKind::ExecutionContext,
        };
        let verdict = Artifact::new(
            ArtifactKind::ExecutionVerdict,
            store
                .put_json(&ExecutionVerdict::Accepted {
                    execution_context: context_ref.clone(),
                })
                .unwrap(),
            "fixture.execution_verdict",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            Some(origin(&permit)),
            vec![context_ref],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(&permit, &verdict, "execution.verdict_created", now)
            .unwrap();
        let verdict_ref = ArtifactRef {
            artifact_id: verdict.artifact_id.clone(),
            kind: ArtifactKind::ExecutionVerdict,
        };
        let input = PaperCommitmentInput {
            lease,
            permit,
            verdict: verdict_ref,
            session_key: "paper:fixture".to_owned(),
            now,
        };
        let runtime = V2PaperCommitmentRuntime::new(store.clone());
        let first = runtime.commit(&input).unwrap();
        assert!(first.newly_committed);
        assert_eq!(first.commitment.kind, ArtifactKind::ExecutionCommitment);

        let retry = runtime.commit(&input).unwrap();
        assert!(!retry.newly_committed);
        assert_eq!(retry.commitment.artifact_id, first.commitment.artifact_id);
        assert_eq!(
            store
                .session_slot("paper:fixture")
                .unwrap()
                .unwrap()
                .commitment_artifact_id,
            Some(first.commitment.artifact_id)
        );
    }

    #[test]
    fn non_paper_run_is_rejected_before_any_session_or_broker_commitment_work() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "fixture-daemon",
                now,
                now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let graph = workflow();
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
        let permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let error = V2PaperCommitmentRuntime::new(store)
            .commit(&PaperCommitmentInput {
                lease,
                permit,
                verdict: ArtifactRef {
                    artifact_id: ArtifactId(ContentHash::of_bytes(b"unread-verdict")),
                    kind: ArtifactKind::ExecutionVerdict,
                },
                session_key: "paper:fixture".to_owned(),
                now,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            PaperCommitmentError::NonPaperRun(RunPurpose::PaperDryRun)
        ));
    }

    #[test]
    fn durable_freeze_blocks_commitment_before_any_verdict_or_broker_work() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "fixture-daemon",
                now,
                now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let graph = workflow();
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
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: RunId::new(),
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let permit = store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let freeze = Artifact::new(
            ArtifactKind::FreezeState,
            store
                .put_json(&FreezeState {
                    schema_version: REBUILD_SCHEMA_VERSION,
                    frozen: true,
                    reason: "fixture safety freeze".to_owned(),
                    changed_at: now,
                })
                .unwrap(),
            "fixture.freeze",
            ArtifactLifecycle::Canonical,
            provenance(now),
            None,
            vec![],
            now,
        )
        .unwrap();
        store.write_bootstrap_artifact(&freeze).unwrap();

        let error = V2PaperCommitmentRuntime::new(store.clone())
            .commit(&PaperCommitmentInput {
                lease,
                permit: permit.clone(),
                verdict: ArtifactRef {
                    artifact_id: ArtifactId(ContentHash::of_bytes(b"unread-verdict")),
                    kind: ArtifactKind::ExecutionVerdict,
                },
                session_key: "paper:fixture".to_owned(),
                now,
            })
            .unwrap_err();
        assert!(matches!(error, PaperCommitmentError::Frozen));
        store.validate_task_permit(&permit).unwrap();
    }
}
