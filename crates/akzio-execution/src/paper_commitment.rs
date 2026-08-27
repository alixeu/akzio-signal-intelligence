//! Fenced durable Paper commitment for the v2 execution path.
//!
//! This module deliberately stops before network I/O. It proves an accepted
//! Rust verdict, the scheduler-owned session slot, the daemon epoch and the
//! active task permit in one Store transaction. The adapter may only receive
//! a commitment returned from here.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, Asset, DomainError, ExecutionContext,
    ExecutionVerdict, FreezeState, PaperCommitment, PaperCommitmentId, RunPurpose, TaskWritePermit,
};
use akzio_store::v2::{DaemonLease, ExecutionCommit, StoreError, V2Store};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, ArtifactProvenance};

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
    #[error("Paper approval expired before commitment")]
    ApprovalExpired,
    #[error("execution plan exceeds approved maximum notional")]
    ApprovalNotionalExceeded,
    #[error("Paper approval is missing")]
    ApprovalMissing,
    #[error("execution plan notional overflow")]
    ApprovalNotionalOverflow,
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
        context.validate_complete_plan_closure()?;
        if context.run_id != input.permit.run_id
            || !verdict_artifact
                .source_refs
                .iter()
                .any(|source| source == &execution_context)
        {
            return Err(PaperCommitmentError::VerdictContextMismatch);
        }
        if context.broker_session.as_deref() != Some(input.session_key.as_str()) {
            return Err(PaperCommitmentError::SessionMismatch);
        }
        if context.frozen {
            return Err(PaperCommitmentError::Frozen);
        }
        let allocation_reference = context
            .execution_plan
            .clone()
            .ok_or(PaperCommitmentError::MissingAllocationPlan)?;
        if !context_artifact.source_refs.contains(&allocation_reference) {
            return Err(PaperCommitmentError::MissingAllocationPlan);
        }
        let allocation_artifact =
            self.load_expected(&allocation_reference, ArtifactKind::ExecutionPlan)?;
        let allocation: crate::ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&allocation_artifact.blob)?)?;
        allocation.validate()?;
        if allocation.plan_hash
            != context
                .plan_hash
                .as_ref()
                .ok_or(PaperCommitmentError::PlanHashMismatch)?
                .clone()
            || allocation.broker_session != input.session_key
        {
            return Err(PaperCommitmentError::PlanHashMismatch);
        }
        if let Some((manifest, approval)) =
            self.store.paper_approval_for_run(&input.permit.run_id)?
        {
            if approval.expires_at < input.now {
                return Err(PaperCommitmentError::ApprovalExpired);
            }
            let total_notional = allocation
                .orders
                .iter()
                .try_fold(0_i64, |total, order| total.checked_add(order.notional.0))
                .ok_or(PaperCommitmentError::ApprovalNotionalOverflow)?;
            if total_notional > manifest.maximum_notional.0 {
                return Err(PaperCommitmentError::ApprovalNotionalExceeded);
            }
        } else {
            return Err(PaperCommitmentError::ApprovalMissing);
        }
        let mut client_order_ids = std::collections::BTreeMap::new();
        for (index, order) in allocation.orders.iter().enumerate() {
            let client_order_id =
                crate::paper::client_order_id(&input.session_key, &allocation.plan_hash, index, 0);
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
                    || existing.plan_hash
                        != context
                            .plan_hash
                            .as_ref()
                            .ok_or(PaperCommitmentError::PlanHashMismatch)?
                            .clone()
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
            plan_hash: context
                .plan_hash
                .clone()
                .ok_or(PaperCommitmentError::PlanHashMismatch)?,
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
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            vec![input.verdict.clone(), execution_context],
            input.now,
        )?;
        let result = self.store.commit_execution(
            &input.lease,
            &ExecutionCommit {
                session_key: input.session_key.clone(),
                permit: input.permit.clone(),
                commitment: commitment.clone(),
                committed_at: Utc::now(),
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
#[path = "paper_commitment/tests.rs"]
mod tests;
