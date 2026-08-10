//! Thin, idempotent Alpaca Paper adapter.
//!
//! It intentionally owns broker protocol details only.  Target construction,
//! risk limits, quote freshness, and all approval decisions stay in the parent
//! execution module and Runtime gates.

use std::{env, future::Future, pin::Pin};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactRef, Asset, ContentHash, DomainError, ExecutionContext,
    FreezeState, OrderReceipt, OrderReceiptState, PaperCommitment, PaperReprice, RunPurpose,
    TaskWritePermit,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    ExecutionPlan, MoneyMicros, OrderIntent, OrderSide, ReconciliationError, ReconciliationInput,
    ReconciliationOutput, V2ReconciliationRuntime,
};

#[derive(Debug, Error)]
pub enum PaperError {
    #[error("ALPACA_API_KEY is not set")]
    MissingKey,
    #[error("ALPACA_API_SECRET is not set")]
    MissingSecret,
    #[error("Paper execution rejects non-Paper endpoint {0}")]
    NonPaperEndpoint(String),
    #[error("broker request to {url} failed: {source}")]
    Transport { url: String, source: reqwest::Error },
    #[error("broker request to {url} returned HTTP {status}: {body}")]
    Http {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error("Alpaca Paper market is closed")]
    MarketClosed,
    #[error("broker response omitted {0}")]
    MissingField(&'static str),
    #[error("broker clock timestamp is invalid: {0}")]
    InvalidClock(String),
    #[error("order quantity rounds to zero")]
    ZeroQuantity,
    #[error("one repricing attempt is already consumed")]
    RepriceConsumed,
    #[error("original Paper order is no longer eligible for the durable reprice")]
    RepricePriorClosed,
    #[error("Paper commitment is invalid: {0}")]
    InvalidCommitment(String),
    #[error("Paper commitment plan hash does not match the submitted plan")]
    CommitmentPlanHashMismatch,
    #[error("Paper commitment client order ID does not match plan order for {0}")]
    CommitmentClientOrderMismatch(Asset),
}

pub type Result<T> = std::result::Result<T, PaperError>;

#[derive(Debug, Clone)]
pub struct PaperCredentials {
    pub key_id: String,
    pub secret_key: String,
}

impl PaperCredentials {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            key_id: env::var("ALPACA_API_KEY").map_err(|_| PaperError::MissingKey)?,
            secret_key: env::var("ALPACA_API_SECRET").map_err(|_| PaperError::MissingSecret)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperOrderReceipt {
    pub client_order_id: String,
    pub broker_order_id: String,
    pub symbol: String,
    pub status: String,
    pub reused: bool,
    pub reprice_count: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperExecution {
    pub plan_hash: ContentHash,
    pub orders: Vec<PaperOrderReceipt>,
}

/// Minimal broker protocol used by Rust-gated execution. The production
/// implementation is Alpaca Paper only; fixtures use an in-memory fake rather
/// than weakening endpoint validation with localhost exceptions.
pub trait PaperBroker: Send + Sync {
    fn execute<'a>(
        &'a self,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;

    fn reconcile<'a>(
        &'a self,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;
}

/// v2 broker protocol. It accepts only a durable Rust-owned commitment and
/// the allocation plan it commits to; callers cannot submit a naked plan.
pub trait CommittedPaperBroker: Send + Sync {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;

    fn replace_commitment_once<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        reprice: &'a PaperReprice,
        replacement: &'a OrderIntent,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>>;
}

/// Input for the task that is allowed to submit an already durable Paper
/// commitment. Creating the commitment is a separate, scheduler-fenced task;
/// this task can only replay that exact commitment through a broker.
#[derive(Debug, Clone)]
pub struct PaperDispatchInput {
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
    pub now: DateTime<Utc>,
}

/// Submission input for a durable, one-time r0 -> r1 replacement intent.
#[derive(Debug, Clone)]
pub struct PaperRepriceDispatchInput {
    pub permit: TaskWritePermit,
    pub reprice: ArtifactRef,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PaperDispatchOutput {
    pub commitment: Artifact,
    pub execution: PaperExecution,
    pub reconciliation: ReconciliationOutput,
}

#[derive(Debug, Error)]
pub enum PaperDispatchError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Broker(#[from] PaperError),
    #[error(transparent)]
    Reconciliation(#[from] ReconciliationError),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("Paper dispatch requires Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("execution is frozen")]
    Frozen,
    #[error("commitment is not the durable session commitment")]
    CommitmentNotDurable,
    #[error("reprice is not the durable r0 -> r1 lineage for this commitment")]
    RepriceNotDurable,
    #[error("commitment does not retain its execution context")]
    CommitmentContextMissing,
    #[error("commitment execution context does not match dispatch run or plan")]
    ContextMismatch,
    #[error("execution context has no persisted allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match commitment")]
    PlanHashMismatch,
    #[error("reprice does not match the committed allocation")]
    RepricePlanMismatch,
    #[error("broker response plan hash does not match commitment")]
    BrokerPlanHashMismatch,
    #[error("broker returned unsupported order status {0}")]
    UnsupportedReceiptStatus(String),
    #[error("broker returned a reprice lineage that is not in the commitment")]
    UnexpectedReprice,
}

pub type PaperDispatchResult<T> = std::result::Result<T, PaperDispatchError>;

/// Dispatches only a persisted commitment, then atomically persists the
/// resulting receipt/reconciliation artifacts and closes the dispatch task.
/// If the process dies after broker I/O but before the final Store transaction,
/// a retry replays the same deterministic client order IDs.
#[derive(Debug, Clone)]
pub struct V2PaperDispatchRuntime {
    store: V2Store,
}

impl V2PaperDispatchRuntime {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    pub async fn dispatch<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
    ) -> PaperDispatchResult<PaperDispatchOutput> {
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperDispatchError::NonPaperRun(purpose));
        }

        let commitment_artifact =
            self.load_expected(&input.commitment, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let slot = self
            .store
            .session_slot(&commitment.broker_session)?
            .ok_or(PaperDispatchError::CommitmentNotDurable)?;
        if slot.workflow.run.run_id != input.permit.run_id
            || slot.commitment_artifact_id.as_ref() != Some(&input.commitment.artifact_id)
        {
            return Err(PaperDispatchError::CommitmentNotDurable);
        }
        if !commitment_artifact
            .source_refs
            .iter()
            .any(|source| source == &commitment.execution_context)
        {
            return Err(PaperDispatchError::CommitmentContextMissing);
        }

        let context_artifact = self.load_expected(
            &commitment.execution_context,
            ArtifactKind::ExecutionContext,
        )?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        if context.run_id != input.permit.run_id
            || context.broker_session != commitment.broker_session
            || context.plan_hash != commitment.plan_hash
        {
            return Err(PaperDispatchError::ContextMismatch);
        }
        let plan_reference = context_artifact
            .source_refs
            .iter()
            .find(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .cloned()
            .ok_or(PaperDispatchError::MissingAllocationPlan)?;
        let plan_artifact = self.load_expected(&plan_reference, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        if plan.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::PlanHashMismatch);
        }

        self.ensure_unfrozen()?;
        self.store.validate_task_permit(&input.permit)?;
        let execution = broker.execute_commitment(&commitment, &plan).await?;
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        let broker_receipts = execution
            .orders
            .iter()
            .map(|receipt| broker_receipt(receipt, &commitment, None, input.now))
            .collect::<PaperDispatchResult<Vec<_>>>()?;
        let reconciliation_runtime = V2ReconciliationRuntime::new(self.store.clone());
        let reconciliation = reconciliation_runtime.reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: input.commitment.clone(),
            reprice: None,
            broker_receipts,
            now: input.now,
        })?;
        reconciliation_runtime.commit(&input.permit, &reconciliation, input.now)?;

        Ok(PaperDispatchOutput {
            commitment: commitment_artifact,
            execution,
            reconciliation,
        })
    }

    pub async fn dispatch_reprice<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperRepriceDispatchInput,
    ) -> PaperDispatchResult<PaperDispatchOutput> {
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperDispatchError::NonPaperRun(purpose));
        }
        let reprice_artifact =
            self.load_expected(&input.reprice, ArtifactKind::ExecutionReprice)?;
        let reprice: PaperReprice =
            serde_json::from_slice(&self.store.read_blob(&reprice_artifact.blob)?)?;
        reprice.validate()?;
        let durable_reprice = self
            .store
            .reprice_for(&reprice.commitment, reprice.asset)?
            .ok_or(PaperDispatchError::RepriceNotDurable)?;
        if durable_reprice.artifact_id != input.reprice.artifact_id
            || !reprice_artifact
                .source_refs
                .iter()
                .any(|source| source == &reprice.commitment)
            || !reprice_artifact
                .source_refs
                .iter()
                .any(|source| source == &reprice.prior_receipt)
        {
            return Err(PaperDispatchError::RepriceNotDurable);
        }

        let commitment_artifact =
            self.load_expected(&reprice.commitment, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let slot = self
            .store
            .session_slot(&commitment.broker_session)?
            .ok_or(PaperDispatchError::CommitmentNotDurable)?;
        if slot.workflow.run.run_id != input.permit.run_id
            || slot.commitment_artifact_id.as_ref() != Some(&reprice.commitment.artifact_id)
        {
            return Err(PaperDispatchError::CommitmentNotDurable);
        }
        let context_artifact = self.load_expected(
            &commitment.execution_context,
            ArtifactKind::ExecutionContext,
        )?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        if context.run_id != input.permit.run_id
            || context.broker_session != commitment.broker_session
            || context.plan_hash != commitment.plan_hash
        {
            return Err(PaperDispatchError::ContextMismatch);
        }
        let plan_reference = context_artifact
            .source_refs
            .iter()
            .find(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .cloned()
            .ok_or(PaperDispatchError::MissingAllocationPlan)?;
        let plan_artifact = self.load_expected(&plan_reference, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        if plan.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::PlanHashMismatch);
        }
        let (order_index, original) = plan
            .orders
            .iter()
            .enumerate()
            .find(|(_, order)| order.asset == reprice.asset)
            .ok_or(PaperDispatchError::RepricePlanMismatch)?;
        if commitment.client_order_ids.get(&reprice.asset) != Some(&reprice.prior_client_order_id)
            || reprice.replacement_client_order_id
                != client_order_id(&plan.plan_hash, order_index, 1)
        {
            return Err(PaperDispatchError::RepricePlanMismatch);
        }
        let replacement = OrderIntent {
            asset: original.asset,
            side: original.side,
            notional: original.notional,
            limit_price: reprice.replacement_limit_price,
        };

        self.ensure_unfrozen()?;
        self.store.validate_task_permit(&input.permit)?;
        let receipt = broker
            .replace_commitment_once(&commitment, &reprice, &replacement)
            .await?;
        let broker_receipt = broker_receipt(&receipt, &commitment, Some(&reprice), input.now)?;
        let execution = PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders: vec![receipt],
        };
        let reconciliation_runtime = V2ReconciliationRuntime::new(self.store.clone());
        let reconciliation = reconciliation_runtime.reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: reprice.commitment.clone(),
            reprice: Some(input.reprice.clone()),
            broker_receipts: vec![broker_receipt],
            now: input.now,
        })?;
        reconciliation_runtime.commit(&input.permit, &reconciliation, input.now)?;

        Ok(PaperDispatchOutput {
            commitment: commitment_artifact,
            execution,
            reconciliation,
        })
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> PaperDispatchResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(PaperDispatchError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn ensure_unfrozen(&self) -> PaperDispatchResult<()> {
        let Some(freeze_artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(());
        };
        let freeze: FreezeState =
            serde_json::from_slice(&self.store.read_blob(&freeze_artifact.blob)?)?;
        freeze.validate()?;
        if freeze.frozen {
            return Err(PaperDispatchError::Frozen);
        }
        Ok(())
    }
}

fn broker_receipt(
    receipt: &PaperOrderReceipt,
    commitment: &PaperCommitment,
    reprice: Option<&PaperReprice>,
    observed_at: DateTime<Utc>,
) -> PaperDispatchResult<OrderReceipt> {
    let expected_reprice_count = u8::from(reprice.is_some());
    if receipt.reprice_count != expected_reprice_count {
        return Err(PaperDispatchError::UnexpectedReprice);
    }
    let asset = Asset::try_from(receipt.symbol.as_str())?;
    if let Some(reprice) = reprice {
        if asset != reprice.asset || receipt.client_order_id != reprice.replacement_client_order_id
        {
            return Err(PaperDispatchError::UnexpectedReprice);
        }
    }
    Ok(OrderReceipt {
        plan_hash: commitment.plan_hash.clone(),
        asset,
        client_order_id: receipt.client_order_id.clone(),
        broker_order_id: receipt.broker_order_id.clone(),
        state: receipt_state(&receipt.status)?,
        observed_at,
    })
}

fn receipt_state(status: &str) -> PaperDispatchResult<OrderReceiptState> {
    match status.trim().to_ascii_lowercase().as_str() {
        "new"
        | "accepted"
        | "pending_new"
        | "accepted_for_bidding"
        | "pending_replace"
        | "pending_cancel" => Ok(OrderReceiptState::Accepted),
        "partially_filled" => Ok(OrderReceiptState::PartiallyFilled),
        "filled" => Ok(OrderReceiptState::Filled),
        "canceled" | "expired" | "done_for_day" | "stopped" | "suspended" => {
            Ok(OrderReceiptState::Canceled)
        }
        "rejected" => Ok(OrderReceiptState::Rejected),
        "failed" => Ok(OrderReceiptState::Failed),
        other => Err(PaperDispatchError::UnsupportedReceiptStatus(
            other.to_owned(),
        )),
    }
}

/// Broker-authoritative open-session state. `session_date` comes from the
/// broker timestamp, so Paper scheduling does not depend on a local timezone
/// or a hand-maintained holiday calendar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketClock {
    pub is_open: bool,
    pub session_date: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct AlpacaPaper {
    client: Client,
    base_url: String,
    credentials: PaperCredentials,
}

impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
        let supplied = base_url.into();
        let parsed = reqwest::Url::parse(supplied.trim())
            .map_err(|_| PaperError::NonPaperEndpoint(supplied.clone()))?;
        if parsed.scheme() != "https"
            || parsed.host_str() != Some("paper-api.alpaca.markets")
            || parsed.port().is_some()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(PaperError::NonPaperEndpoint(supplied));
        }
        Ok(Self {
            client: Client::new(),
            base_url: "https://paper-api.alpaca.markets".to_owned(),
            credentials,
        })
    }

    pub fn from_env() -> Result<Self> {
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        Self::new(base_url, PaperCredentials::from_env()?)
    }

    pub async fn account(&self) -> Result<Value> {
        self.get_json("/v2/account").await
    }

    pub async fn positions(&self) -> Result<Value> {
        self.get_json("/v2/positions").await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
        let clock = self.get_json("/v2/clock").await?;
        market_clock_from_value(&clock)
    }

    pub async fn execute(&self, plan: &ExecutionPlan) -> Result<PaperExecution> {
        self.assert_market_open().await?;
        let mut orders = Vec::with_capacity(plan.orders.len());
        for (index, order) in plan.orders.iter().enumerate() {
            let client_order_id = client_order_id(&plan.plan_hash, index, 0);
            let receipt = match self.lookup(&client_order_id).await? {
                Some(receipt) => PaperOrderReceipt {
                    reused: true,
                    reprice_count: 0,
                    ..receipt
                },
                None => self.submit_order(order, &client_order_id, 0).await?,
            };
            orders.push(receipt);
        }
        Ok(PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders,
        })
    }

    pub async fn execute_committed(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
    ) -> Result<PaperExecution> {
        self.validate_commitment(commitment, plan)?;
        self.execute(plan).await
    }

    pub async fn reconcile(&self, execution: &PaperExecution) -> Result<PaperExecution> {
        let mut orders = Vec::with_capacity(execution.orders.len());
        for receipt in &execution.orders {
            orders.push(
                self.get_order(
                    &receipt.broker_order_id,
                    &receipt.client_order_id,
                    receipt.reprice_count,
                )
                .await?,
            );
        }
        Ok(PaperExecution {
            plan_hash: execution.plan_hash.clone(),
            orders,
        })
    }

    /// The caller supplies a newly gate-validated replacement intent.  This
    /// adapter guarantees exactly one cancellation/replacement lineage.
    pub async fn cancel_and_replace_once(
        &self,
        receipt: &PaperOrderReceipt,
        replacement: &OrderIntent,
    ) -> Result<PaperOrderReceipt> {
        if receipt.reprice_count >= 1 {
            return Err(PaperError::RepriceConsumed);
        }
        self.delete(&format!("/v2/orders/{}", receipt.broker_order_id))
            .await?;
        let client_order_id = replacement_client_order_id(&receipt.client_order_id);
        self.submit_order(replacement, &client_order_id, 1).await
    }

    /// Execute only the pre-recorded r0 -> r1 reprice lineage. A retry first
    /// looks up the deterministic replacement ID, so a crash after submission
    /// never creates another Paper order.
    pub async fn execute_reprice_committed(
        &self,
        commitment: &PaperCommitment,
        reprice: &PaperReprice,
        replacement: &OrderIntent,
    ) -> Result<PaperOrderReceipt> {
        self.validate_reprice(commitment, reprice, replacement)?;
        if let Some(existing) = self.lookup(&reprice.replacement_client_order_id).await? {
            return Ok(PaperOrderReceipt {
                reused: true,
                reprice_count: 1,
                ..existing
            });
        }
        self.assert_market_open().await?;
        let prior = self
            .get_order(
                &reprice.prior_broker_order_id,
                &reprice.prior_client_order_id,
                0,
            )
            .await?;
        match prior.status.trim().to_ascii_lowercase().as_str() {
            "new"
            | "accepted"
            | "pending_new"
            | "accepted_for_bidding"
            | "pending_replace"
            | "pending_cancel"
            | "partially_filled" => {
                self.delete(&format!("/v2/orders/{}", reprice.prior_broker_order_id))
                    .await?;
            }
            "canceled" | "expired" | "done_for_day" => {}
            _ => return Err(PaperError::RepricePriorClosed),
        }
        self.submit_order(replacement, &reprice.replacement_client_order_id, 1)
            .await
    }

    fn validate_commitment(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
    ) -> Result<()> {
        commitment
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        if commitment.plan_hash != plan.plan_hash {
            return Err(PaperError::CommitmentPlanHashMismatch);
        }
        if commitment.client_order_ids.len() != plan.orders.len() {
            return Err(PaperError::InvalidCommitment(
                "client order count does not match allocation plan".to_owned(),
            ));
        }
        for (index, order) in plan.orders.iter().enumerate() {
            let expected = client_order_id(&plan.plan_hash, index, 0);
            if commitment.client_order_ids.get(&order.asset) != Some(&expected) {
                return Err(PaperError::CommitmentClientOrderMismatch(order.asset));
            }
        }
        Ok(())
    }

    fn validate_reprice(
        &self,
        commitment: &PaperCommitment,
        reprice: &PaperReprice,
        replacement: &OrderIntent,
    ) -> Result<()> {
        commitment
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        reprice
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        if commitment.client_order_ids.get(&reprice.asset) != Some(&reprice.prior_client_order_id)
            || replacement.asset != reprice.asset
            || replacement.limit_price != reprice.replacement_limit_price
            || reprice.replacement_client_order_id
                != replacement_client_order_id(&reprice.prior_client_order_id)
        {
            return Err(PaperError::InvalidCommitment(
                "reprice does not match committed order lineage".to_owned(),
            ));
        }
        Ok(())
    }

    async fn assert_market_open(&self) -> Result<()> {
        if !self.market_clock().await?.is_open {
            return Err(PaperError::MarketClosed);
        }
        Ok(())
    }

    async fn lookup(&self, client_order_id: &str) -> Result<Option<PaperOrderReceipt>> {
        let url = self.url("/v2/orders:by_client_order_id");
        let response = self
            .authorized(
                self.client
                    .get(&url)
                    .query(&[("client_order_id", client_order_id)]),
            )
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        let value = parse_value(&body);
        Ok(Some(receipt_from_value(value, client_order_id, false, 0)?))
    }

    async fn submit_order(
        &self,
        order: &OrderIntent,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let url = self.url("/v2/orders");
        let body = serde_json::json!({
            "symbol": order.asset.symbol(),
            "qty": quantity_string(order)?,
            "side": side_name(order.side),
            "type": "limit",
            "time_in_force": "day",
            "limit_price": money_string(order.limit_price),
            "extended_hours": false,
            "client_order_id": client_order_id,
        });
        let value = self.post_json(&url, body).await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_order(
        &self,
        broker_order_id: &str,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let value = self
            .get_json(&format!("/v2/orders/{broker_order_id}"))
            .await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let url = self.url(path);
        let response = self
            .authorized(self.client.get(&url))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        self.response_json(url, response).await
    }

    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        let response = self
            .authorized(self.client.post(url).json(&body))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        self.response_json(url.to_owned(), response).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let response = self
            .authorized(self.client.delete(&url))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        Ok(())
    }

    async fn response_json(&self, url: String, response: reqwest::Response) -> Result<Value> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        Ok(parse_value(&body))
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("APCA-API-KEY-ID", &self.credentials.key_id)
            .header("APCA-API-SECRET-KEY", &self.credentials.secret_key)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl PaperBroker for AlpacaPaper {
    fn execute<'a>(
        &'a self,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute(self, plan))
    }

    fn reconcile<'a>(
        &'a self,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::reconcile(self, execution))
    }
}

impl CommittedPaperBroker for AlpacaPaper {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute_committed(self, commitment, plan))
    }

    fn replace_commitment_once<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        reprice: &'a PaperReprice,
        replacement: &'a OrderIntent,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute_reprice_committed(
            self,
            commitment,
            reprice,
            replacement,
        ))
    }
}

fn receipt_from_value(
    value: Value,
    fallback_client_order_id: &str,
    reused: bool,
    reprice_count: u8,
) -> Result<PaperOrderReceipt> {
    let broker_order_id = required_string(&value, "id")?;
    let symbol = required_string(&value, "symbol")?;
    let status = required_string(&value, "status")?;
    let client_order_id = value
        .get("client_order_id")
        .and_then(Value::as_str)
        .unwrap_or(fallback_client_order_id)
        .to_owned();
    Ok(PaperOrderReceipt {
        client_order_id,
        broker_order_id,
        symbol,
        status,
        reused,
        reprice_count,
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(PaperError::MissingField(field))
}

fn parse_value(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_owned()))
}

fn market_clock_from_value(clock: &Value) -> Result<MarketClock> {
    let is_open = clock
        .get("is_open")
        .and_then(Value::as_bool)
        .ok_or(PaperError::MissingField("clock.is_open"))?;
    let timestamp = clock
        .get("timestamp")
        .and_then(Value::as_str)
        .ok_or(PaperError::MissingField("clock.timestamp"))?;
    let observed_at = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| PaperError::InvalidClock(error.to_string()))?;
    Ok(MarketClock {
        is_open,
        session_date: observed_at.date_naive(),
    })
}

fn side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn money_string(value: MoneyMicros) -> String {
    let whole = value.0 / 1_000_000;
    let fraction = value.0.unsigned_abs() % 1_000_000;
    format!("{whole}.{fraction:06}")
}

fn quantity_string(order: &OrderIntent) -> Result<String> {
    let quantity_millionths = order
        .notional
        .0
        .saturating_mul(1_000_000)
        .checked_div(order.limit_price.0)
        .unwrap_or_default();
    if quantity_millionths <= 0 {
        return Err(PaperError::ZeroQuantity);
    }
    let whole = quantity_millionths / 1_000_000;
    let fraction = quantity_millionths.unsigned_abs() % 1_000_000;
    Ok(format!("{whole}.{fraction:06}"))
}

pub fn client_order_id(plan_hash: &ContentHash, order_index: usize, reprice_count: u8) -> String {
    let prefix = &plan_hash.as_str()[..16];
    format!("akzio-v2-{prefix}-{order_index}-r{reprice_count}")
}

fn replacement_client_order_id(previous: &str) -> String {
    let base = previous.split("-r").next().unwrap_or(previous);
    format!("{base}-r1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::{ExecutionPlan, ExecutionPolicy, MoneyMicros, OrderIntent};
    use akzio_domain::{
        ArtifactId, ArtifactKind, ArtifactRef, Asset, ContentHash, PaperCommitmentId,
    };

    #[test]
    fn ids_are_deterministic_and_bounded() {
        let hash = ContentHash::of_bytes(b"plan");
        let id = client_order_id(&hash, 12, 0);
        assert!(id.starts_with("akzio-v2-"));
        assert!(id.len() <= 48);
        assert_eq!(
            replacement_client_order_id(&id),
            format!("{}-r1", id.split("-r").next().unwrap())
        );
    }

    #[test]
    fn limit_notional_becomes_fractional_quantity() {
        let order = OrderIntent {
            asset: Asset::Tqqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(2_500),
        };
        assert_eq!(quantity_string(&order).unwrap(), "4.000000");
    }

    #[test]
    fn committed_adapter_rejects_mismatched_plan_or_client_id_before_http() {
        let plan = ExecutionPlan {
            policy: ExecutionPolicy::default(),
            targets: vec![],
            orders: vec![OrderIntent {
                asset: Asset::Tqqq,
                side: OrderSide::Buy,
                notional: MoneyMicros::from_usd_cents(10_000),
                limit_price: MoneyMicros::from_usd_cents(2_500),
            }],
            plan_hash: ContentHash::of_bytes(b"committed-plan"),
        };
        let credentials = PaperCredentials {
            key_id: "key".to_owned(),
            secret_key: "secret".to_owned(),
        };
        let adapter = AlpacaPaper::new("https://paper-api.alpaca.markets", credentials).unwrap();
        let mut commitment = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"context")),
                kind: ArtifactKind::ExecutionContext,
            },
            plan_hash: ContentHash::of_bytes(b"other-plan"),
            broker_session: "paper:fixture".to_owned(),
            client_order_ids: BTreeMap::from([(
                Asset::Tqqq,
                client_order_id(&plan.plan_hash, 0, 0),
            )]),
            created_at: chrono::Utc::now(),
        };
        assert!(matches!(
            adapter.validate_commitment(&commitment, &plan),
            Err(PaperError::CommitmentPlanHashMismatch)
        ));

        commitment.plan_hash = plan.plan_hash.clone();
        commitment
            .client_order_ids
            .insert(Asset::Tqqq, "forged-client-order-id".to_owned());
        assert!(matches!(
            adapter.validate_commitment(&commitment, &plan),
            Err(PaperError::CommitmentClientOrderMismatch(Asset::Tqqq))
        ));
    }

    #[test]
    fn adapter_accepts_only_the_exact_alpaca_paper_origin() {
        let credentials = PaperCredentials {
            key_id: "key".to_owned(),
            secret_key: "secret".to_owned(),
        };
        assert!(AlpacaPaper::new("https://paper-api.alpaca.markets/", credentials.clone()).is_ok());
        for endpoint in [
            "https://api.alpaca.markets",
            "http://paper-api.alpaca.markets",
            "https://paper-api.alpaca.markets.evil.test",
            "https://evil.test/paper-api.alpaca.markets",
            "https://paper-api.alpaca.markets/v2",
            "http://127.0.0.1:9999",
        ] {
            assert!(matches!(
                AlpacaPaper::new(endpoint, credentials.clone()),
                Err(PaperError::NonPaperEndpoint(_))
            ));
        }
    }

    #[test]
    fn market_clock_uses_broker_session_date() {
        let clock = market_clock_from_value(&serde_json::json!({
            "is_open": true,
            "timestamp": "2026-08-06T10:00:00-04:00",
        }))
        .unwrap();
        assert!(clock.is_open);
        assert_eq!(
            clock.session_date,
            chrono::NaiveDate::from_ymd_opt(2026, 8, 6).unwrap()
        );
        assert!(matches!(
            market_clock_from_value(&serde_json::json!({"is_open": true})),
            Err(PaperError::MissingField("clock.timestamp"))
        ));
    }
}
