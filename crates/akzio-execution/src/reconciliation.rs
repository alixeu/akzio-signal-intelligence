//! Typed reconciliation for durable Paper commitments.

use std::collections::BTreeSet;

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, DomainError, OrderReceipt, OrderReceiptState, PaperCommitment, PaperReprice,
    Reconciliation, ReconciliationId, ReconciliationState, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::v2::{DaemonLease, StoreError, V2Store};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReconciliationError {
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
    #[error("reconciliation requires a Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("broker receipt does not belong to the committed plan")]
    PlanHashMismatch,
    #[error("broker receipt client order ID does not match commitment for {0}")]
    ClientOrderMismatch(Asset),
    #[error("reprice does not belong to the committed order")]
    RepriceMismatch,
    #[error("broker reconciliation returned multiple receipts for {0}")]
    DuplicateReceipt(Asset),
}

pub type ReconciliationResult<T> = std::result::Result<T, ReconciliationError>;

#[derive(Debug, Clone)]
pub struct ReconciliationInput {
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
    pub reprice: Option<ArtifactRef>,
    pub broker_receipts: Vec<OrderReceipt>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReconciliationOutput {
    pub receipts: Vec<Artifact>,
    pub reconciliation: Artifact,
}

#[derive(Debug, Clone)]
pub struct V2ReconciliationRuntime {
    store: V2Store,
}

impl V2ReconciliationRuntime {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    pub fn reconcile(
        &self,
        input: &ReconciliationInput,
    ) -> ReconciliationResult<ReconciliationOutput> {
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(ReconciliationError::NonPaperRun(purpose));
        }
        let commitment_artifact =
            self.load_expected(&input.commitment, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let reprice = input
            .reprice
            .as_ref()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::ExecutionReprice)?;
                let payload: PaperReprice =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                let durable = self
                    .store
                    .reprice_for(&payload.commitment, payload.asset)?
                    .ok_or(ReconciliationError::RepriceMismatch)?;
                if payload.commitment != input.commitment
                    || durable.artifact_id != reference.artifact_id
                {
                    return Err(ReconciliationError::RepriceMismatch);
                }
                Ok(payload)
            })
            .transpose()?;

        let mut seen = BTreeSet::new();
        let mut receipts = Vec::with_capacity(input.broker_receipts.len());
        for receipt in &input.broker_receipts {
            receipt.validate()?;
            if receipt.plan_hash != commitment.plan_hash {
                return Err(ReconciliationError::PlanHashMismatch);
            }
            let is_committed_client_id =
                commitment.client_order_ids.get(&receipt.asset) == Some(&receipt.client_order_id);
            let is_reprice_client_id = reprice.as_ref().is_some_and(|reprice| {
                reprice.asset == receipt.asset
                    && reprice.replacement_client_order_id == receipt.client_order_id
            });
            if !is_committed_client_id && !is_reprice_client_id {
                return Err(ReconciliationError::ClientOrderMismatch(receipt.asset));
            }
            if !seen.insert(receipt.asset) {
                return Err(ReconciliationError::DuplicateReceipt(receipt.asset));
            }
            let mut source_refs = vec![input.commitment.clone()];
            if let Some(reprice) = &input.reprice {
                source_refs.push(reprice.clone());
            }
            receipts.push(self.artifact(
                ArtifactKind::OrderReceipt,
                "execution.order_receipt",
                receipt,
                source_refs,
                input,
            )?);
        }

        let receipt_refs = receipts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        let state = reconciliation_state(&commitment, &input.broker_receipts, receipt_refs.len());
        let payload = Reconciliation {
            reconciliation_id: ReconciliationId::new(),
            commitment: input.commitment.clone(),
            state,
            broker_receipts: receipt_refs.clone(),
            reconciled_at: input.now,
        };
        payload.validate()?;
        let mut source_refs = Vec::with_capacity(receipt_refs.len() + 2);
        source_refs.push(input.commitment.clone());
        if let Some(reprice) = &input.reprice {
            source_refs.push(reprice.clone());
        }
        source_refs.extend(receipt_refs);
        let reconciliation = self.artifact(
            ArtifactKind::Reconciliation,
            "execution.reconciliation",
            &payload,
            source_refs,
            input,
        )?;

        Ok(ReconciliationOutput {
            receipts,
            reconciliation,
        })
    }

    pub fn commit(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        output: &ReconciliationOutput,
        now: DateTime<Utc>,
    ) -> ReconciliationResult<()> {
        let mut artifacts = output.receipts.clone();
        artifacts.push(output.reconciliation.clone());
        self.store
            .commit_fenced_attempt(lease, permit, &artifacts, TaskStatus::Succeeded, now)?;
        Ok(())
    }

    pub fn commit_with_effect(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        output: &ReconciliationOutput,
        effect: &ArtifactRef,
        recovered: bool,
        now: DateTime<Utc>,
    ) -> ReconciliationResult<()> {
        let mut artifacts = output.receipts.clone();
        artifacts.push(output.reconciliation.clone());
        self.store
            .commit_fenced_attempt_with_effect(lease, permit, &artifacts, effect, recovered, now)?;
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> ReconciliationResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(ReconciliationError::WrongArtifactKind {
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
        source_refs: Vec<ArtifactRef>,
        input: &ReconciliationInput,
    ) -> ReconciliationResult<Artifact> {
        Ok(Artifact::new(
            kind,
            self.store.put_json(payload)?,
            producer,
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
            source_refs,
            input.now,
        )?)
    }
}

fn reconciliation_state(
    commitment: &PaperCommitment,
    receipts: &[OrderReceipt],
    receipt_count: usize,
) -> ReconciliationState {
    if receipts.iter().any(|receipt| {
        matches!(
            receipt.state,
            OrderReceiptState::Canceled | OrderReceiptState::Rejected | OrderReceiptState::Failed
        )
    }) {
        ReconciliationState::Failed
    } else if receipt_count == commitment.client_order_ids.len()
        && receipts
            .iter()
            .all(|receipt| receipt.state == OrderReceiptState::Filled)
    {
        ReconciliationState::Complete
    } else if receipt_count == 0 {
        ReconciliationState::Pending
    } else if receipts.iter().any(|receipt| {
        matches!(
            receipt.state,
            OrderReceiptState::PartiallyFilled | OrderReceiptState::Filled
        )
    }) {
        ReconciliationState::Partial
    } else {
        ReconciliationState::Pending
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use tempfile::tempdir;

    use akzio_domain::{
        ArtifactId, ContentHash, FailureDisposition, PaperCommitmentId, RetryPolicy, RunId,
        TaskBudget, TaskId, TaskRecipeId, WorkflowGraph, WorkflowNode, V2_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};

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

    fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.reconciliation".to_owned(),
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

    fn claimed_paper_task(store: &V2Store, now: DateTime<Utc>) -> TaskWritePermit {
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "reconciliation-fixture".to_owned(),
            nodes: vec![WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("execution.reconcile").unwrap(),
                contract_hash: None,
                objective: "reconcile paper orders".to_owned(),
                dependencies: vec![],
                input_artifacts: vec![],
                priority: 100,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            }],
        };
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
        store
            .claim_next_task("fixture", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit
    }

    #[test]
    fn valid_broker_receipt_materializes_complete_reconciliation() {
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
            .expect("fresh fixture must acquire the scheduler lease");
        let permit = claimed_paper_task(&store, now);
        let plan_hash = ContentHash::of_bytes(b"plan");
        let commitment = Artifact::new(
            ArtifactKind::ExecutionCommitment,
            store
                .put_json(&PaperCommitment {
                    commitment_id: PaperCommitmentId::new(),
                    execution_context: ArtifactRef {
                        artifact_id: ArtifactId(ContentHash::of_bytes(b"context")),
                        kind: ArtifactKind::ExecutionContext,
                    },
                    plan_hash: plan_hash.clone(),
                    broker_session: "paper:fixture".to_owned(),
                    client_order_ids: std::collections::BTreeMap::from([(
                        Asset::Qqq,
                        "client-qqq".to_owned(),
                    )]),
                    created_at: now,
                })
                .unwrap(),
            "fixture.commitment",
            ArtifactLifecycle::Canonical,
            provenance(now),
            Some(origin(&permit)),
            vec![],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(&permit, &commitment, "execution.committed", now)
            .unwrap();
        let output = V2ReconciliationRuntime::new(store.clone())
            .reconcile(&ReconciliationInput {
                permit: permit.clone(),
                commitment: ArtifactRef {
                    artifact_id: commitment.artifact_id.clone(),
                    kind: ArtifactKind::ExecutionCommitment,
                },
                reprice: None,
                broker_receipts: vec![OrderReceipt {
                    plan_hash,
                    asset: Asset::Qqq,
                    client_order_id: "client-qqq".to_owned(),
                    broker_order_id: "broker-qqq".to_owned(),
                    state: OrderReceiptState::Filled,
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 1_000_000,
                    remaining_quantity_micros: 0,
                    average_fill_price: Some(akzio_domain::MoneyMicros(1_000_000)),
                    broker_updated_at: now,
                    reason: None,
                    observed_at: now,
                }],
                now,
            })
            .unwrap();
        let reconciliation: Reconciliation =
            serde_json::from_slice(&store.read_blob(&output.reconciliation.blob).unwrap()).unwrap();
        assert_eq!(reconciliation.state, ReconciliationState::Complete);
        V2ReconciliationRuntime::new(store.clone())
            .commit(&lease, &permit, &output, now)
            .unwrap();
        assert_eq!(
            store
                .artifact(&output.reconciliation.artifact_id)
                .unwrap()
                .kind,
            ArtifactKind::Reconciliation
        );
    }
}
