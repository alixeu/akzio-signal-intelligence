//! Bounded Rust-owned Paper reprice preparation.
//!
//! A reprice never mutates a commitment or invents another session plan. It
//! derives one r0 -> r1 replacement from the committed plan and fresh quote,
//! records that intent atomically, and leaves broker I/O to the committed
//! dispatch boundary.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, DomainError, ExecutionContext,
    FreezeState, OrderReceipt, OrderReceiptState, PaperCommitment, PaperReprice, PaperRepriceId,
    RunPurpose, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{DaemonLease, RepriceCommit, StoreError, V2Store};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{
    protected_limit_price, validate_quote, ExecutionError, ExecutionPlan, ExecutionPolicy,
    OrderIntent, Quote,
};

#[derive(Debug, Error)]
pub enum RepriceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("Paper reprice requires Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("execution is frozen")]
    Frozen,
    #[error("commitment is not the durable session commitment")]
    CommitmentNotDurable,
    #[error("commitment execution context does not retain an allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match commitment")]
    PlanHashMismatch,
    #[error("receipt does not belong to the committed order")]
    ReceiptMismatch,
    #[error("only accepted or partially-filled orders can be repriced")]
    ReceiptNotRepriceable,
    #[error("committed plan has no order for receipt asset")]
    MissingOrder,
    #[error("fresh Rust policy produced no replacement price change")]
    NoRepriceNeeded,
}

pub type RepriceResult<T> = std::result::Result<T, RepriceError>;

#[derive(Debug, Clone)]
pub struct RepriceInput {
    pub lease: DaemonLease,
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
    pub prior_receipt: ArtifactRef,
    pub quote: Quote,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RepriceOutput {
    pub reprice: Artifact,
    pub replacement: OrderIntent,
    pub newly_committed: bool,
}

#[derive(Debug, Clone)]
pub struct V2RepriceRuntime {
    store: V2Store,
    policy: ExecutionPolicy,
}

impl V2RepriceRuntime {
    pub fn new(store: V2Store, policy: ExecutionPolicy) -> Self {
        Self { store, policy }
    }

    pub fn prepare(&self, input: &RepriceInput) -> RepriceResult<RepriceOutput> {
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(RepriceError::NonPaperRun(purpose));
        }
        if let Some(freeze_artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        {
            let freeze: FreezeState =
                serde_json::from_slice(&self.store.read_blob(&freeze_artifact.blob)?)?;
            freeze.validate()?;
            if freeze.frozen {
                return Err(RepriceError::Frozen);
            }
        }

        let commitment_artifact =
            self.load_expected(&input.commitment, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let slot = self
            .store
            .session_slot(&commitment.broker_session)?
            .ok_or(RepriceError::CommitmentNotDurable)?;
        if slot.workflow.run.run_id != input.permit.run_id
            || slot.commitment_artifact_id.as_ref() != Some(&input.commitment.artifact_id)
        {
            return Err(RepriceError::CommitmentNotDurable);
        }

        let prior_artifact =
            self.load_expected(&input.prior_receipt, ArtifactKind::OrderReceipt)?;
        if !prior_artifact
            .source_refs
            .iter()
            .any(|source| source == &input.commitment)
        {
            return Err(RepriceError::ReceiptMismatch);
        }
        let prior: OrderReceipt =
            serde_json::from_slice(&self.store.read_blob(&prior_artifact.blob)?)?;
        prior.validate()?;
        if prior.plan_hash != commitment.plan_hash
            || commitment.client_order_ids.get(&prior.asset) != Some(&prior.client_order_id)
        {
            return Err(RepriceError::ReceiptMismatch);
        }
        if !matches!(
            prior.state,
            OrderReceiptState::Accepted | OrderReceiptState::PartiallyFilled
        ) {
            return Err(RepriceError::ReceiptNotRepriceable);
        }

        let context_artifact = self.load_expected(
            &commitment.execution_context,
            ArtifactKind::ExecutionContext,
        )?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        context.validate_complete_plan_closure()?;
        if context.run_id != input.permit.run_id
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
            || context.broker_session.as_deref() != Some(commitment.broker_session.as_str())
        {
            return Err(RepriceError::PlanHashMismatch);
        }
        let plan_reference = context
            .execution_plan
            .clone()
            .ok_or(RepriceError::MissingAllocationPlan)?;
        if !context_artifact.source_refs.contains(&plan_reference) {
            return Err(RepriceError::MissingAllocationPlan);
        }
        let plan_artifact = self.load_expected(&plan_reference, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if plan.plan_hash != commitment.plan_hash
            || plan.broker_session != commitment.broker_session
        {
            return Err(RepriceError::PlanHashMismatch);
        }
        let (order_index, original) = plan
            .orders
            .iter()
            .enumerate()
            .find(|(_, order)| order.asset == prior.asset)
            .ok_or(RepriceError::MissingOrder)?;
        validate_quote(
            self.policy.max_quote_age_secs,
            self.policy.max_future_skew_secs,
            self.policy.max_spread_bps,
            prior.asset,
            input.quote,
            input.now,
        )?;
        let replacement_limit_price =
            protected_limit_price(input.quote, original.side, self.policy.limit_protection_bps);
        if replacement_limit_price == original.limit_price {
            return Err(RepriceError::NoRepriceNeeded);
        }
        let replacement = OrderIntent {
            asset: original.asset,
            side: original.side,
            notional: original.notional,
            limit_price: replacement_limit_price,
        };
        let reprice_payload = PaperReprice {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            reprice_id: PaperRepriceId::new(),
            commitment: input.commitment.clone(),
            prior_receipt: input.prior_receipt.clone(),
            asset: prior.asset,
            prior_client_order_id: prior.client_order_id.clone(),
            replacement_client_order_id: crate::paper::client_order_id(
                &commitment.broker_session,
                &plan.plan_hash,
                order_index,
                1,
            ),
            prior_broker_order_id: prior.broker_order_id.clone(),
            replacement_limit_price,
            created_at: input.now,
        };
        reprice_payload.validate()?;
        let reprice = Artifact::new(
            ArtifactKind::ExecutionReprice,
            self.store.put_json(&reprice_payload)?,
            "execution.paper_reprice",
            ArtifactLifecycle::Canonical,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            vec![
                input.commitment.clone(),
                input.prior_receipt.clone(),
                plan_reference,
            ],
            input.now,
        )?;
        let result = self.store.commit_reprice(
            &input.lease,
            &RepriceCommit {
                permit: input.permit.clone(),
                reprice: reprice.clone(),
                committed_at: Utc::now(),
            },
        )?;
        let reprice = if result.newly_committed {
            reprice
        } else {
            self.store.artifact(&result.reprice_artifact_id)?
        };
        Ok(RepriceOutput {
            reprice,
            replacement,
            newly_committed: result.newly_committed,
        })
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> RepriceResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(RepriceError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }
}
