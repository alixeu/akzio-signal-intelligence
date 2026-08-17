//! Stable Rust-owned Paper execution schemas.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    content_hash_json,
    decision::HardBlocker,
    ids::{PaperCommitmentId, PaperRepriceId, ReconciliationId},
    Asset, ContentHash, DomainError, MoneyMicros, RunId, TargetPortfolio, V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub quantity_micros: i64,
    pub market_value: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub schema_version: u32,
    pub broker_session: String,
    pub observed_at: DateTime<Utc>,
    pub equity: MoneyMicros,
    pub buying_power: MoneyMicros,
    pub day_turnover: MoneyMicros,
    pub active: bool,
    pub trading_blocked: bool,
    pub positions: BTreeMap<Asset, Position>,
    pub external_positions: BTreeSet<String>,
    pub open_order_ids: BTreeSet<String>,
}

impl AccountSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "account_snapshot.identity",
            });
        }
        if self.equity.0 <= 0 || self.buying_power.0 < 0 || self.day_turnover.0 < 0 {
            return Err(DomainError::InvalidBudget {
                field: "account_snapshot.money",
            });
        }
        if self
            .positions
            .values()
            .any(|position| position.quantity_micros < 0 || position.market_value.0 < 0)
            || self
                .external_positions
                .iter()
                .any(|symbol| symbol.trim().is_empty())
            || self
                .open_order_ids
                .iter()
                .any(|order_id| order_id.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "account_snapshot.positions",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub bid: MoneyMicros,
    pub ask: MoneyMicros,
    pub observed_at: DateTime<Utc>,
}

impl Quote {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.bid.0 <= 0 || self.ask.0 <= self.bid.0 {
            return Err(DomainError::InvalidBudget {
                field: "quote.price",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteSnapshot {
    pub schema_version: u32,
    pub broker_session: String,
    pub observed_at: DateTime<Utc>,
    pub quotes: BTreeMap<Asset, Quote>,
}

impl QuoteSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "quote_snapshot.identity",
            });
        }
        self.quotes.values().try_for_each(Quote::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketClockSnapshot {
    pub schema_version: u32,
    pub broker_session: String,
    pub is_open: bool,
    pub observed_at: DateTime<Utc>,
}

impl MarketClockSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "market_clock_snapshot.identity",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExposure {
    pub leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub tqqq_qqq_pair_ppm: u32,
    pub soxl_soxx_pair_ppm: u32,
}

impl FactorExposure {
    pub fn from_target(target: &TargetPortfolio) -> Result<Self, DomainError> {
        target.validate_universe()?;
        let weight = |asset| target.weights[&asset].0;
        let add = |left: u32, right: u32| {
            left.checked_add(right).ok_or(DomainError::InvalidBudget {
                field: "factor_exposure",
            })
        };
        let nasdaq_ppm = add(weight(Asset::Tqqq), weight(Asset::Qqq))?;
        let semiconductor_ppm = add(weight(Asset::Soxl), weight(Asset::Soxx))?;
        Ok(Self {
            leveraged_equity_ppm: add(weight(Asset::Tqqq), weight(Asset::Soxl))?,
            nasdaq_ppm,
            semiconductor_ppm,
            tqqq_qqq_pair_ppm: nasdaq_ppm,
            soxl_soxx_pair_ppm: semiconductor_ppm,
        })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.tqqq_qqq_pair_ppm,
            self.soxl_soxx_pair_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_exposure",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub asset: Asset,
    pub side: OrderSide,
    pub notional: MoneyMicros,
    pub limit_price: MoneyMicros,
}

impl OrderIntent {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.notional.0 <= 0 || self.limit_price.0 <= 0 {
            return Err(DomainError::InvalidBudget {
                field: "order_intent",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub schema_version: u32,
    pub decision_context: ArtifactRef,
    pub account_snapshot: ArtifactRef,
    pub quote_snapshot: ArtifactRef,
    pub market_clock_snapshot: ArtifactRef,
    pub policy_hash: ContentHash,
    pub target: TargetPortfolio,
    pub orders: Vec<OrderIntent>,
    pub gross_exposure_ppm: u32,
    pub net_exposure_ppm: i64,
    pub factor_exposure: FactorExposure,
    pub turnover_ppm: u32,
    pub broker_session: String,
    pub created_at: DateTime<Utc>,
    pub plan_hash: ContentHash,
}

#[derive(Serialize)]
struct ExecutionPlanHashPayload<'a> {
    schema_version: u32,
    decision_context: &'a ArtifactRef,
    account_snapshot: &'a ArtifactRef,
    quote_snapshot: &'a ArtifactRef,
    market_clock_snapshot: &'a ArtifactRef,
    policy_hash: &'a ContentHash,
    target: &'a TargetPortfolio,
    orders: &'a [OrderIntent],
    gross_exposure_ppm: u32,
    net_exposure_ppm: i64,
    factor_exposure: &'a FactorExposure,
    turnover_ppm: u32,
    broker_session: &'a str,
    created_at: DateTime<Utc>,
}

impl ExecutionPlan {
    pub fn expected_hash(&self) -> Result<ContentHash, DomainError> {
        let payload = ExecutionPlanHashPayload {
            schema_version: self.schema_version,
            decision_context: &self.decision_context,
            account_snapshot: &self.account_snapshot,
            quote_snapshot: &self.quote_snapshot,
            market_clock_snapshot: &self.market_clock_snapshot,
            policy_hash: &self.policy_hash,
            target: &self.target,
            orders: &self.orders,
            gross_exposure_ppm: self.gross_exposure_ppm,
            net_exposure_ppm: self.net_exposure_ppm,
            factor_exposure: &self.factor_exposure,
            turnover_ppm: self.turnover_ppm,
            broker_session: &self.broker_session,
            created_at: self.created_at,
        };
        let value = serde_json::to_value(payload).map_err(|_| DomainError::InvalidContentHash)?;
        content_hash_json(&value).map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn refresh_hash(&mut self) -> Result<(), DomainError> {
        self.plan_hash = self.expected_hash()?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.identity",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self.account_snapshot.kind != ArtifactKind::NormalizedEvidence
            || self.quote_snapshot.kind != ArtifactKind::NormalizedEvidence
            || self.market_clock_snapshot.kind != ArtifactKind::NormalizedEvidence
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.references",
            });
        }
        self.target.validate_universe()?;
        self.factor_exposure.validate()?;
        if self.orders.is_empty()
            || self.gross_exposure_ppm > 1_000_000
            || self.net_exposure_ppm.unsigned_abs() > 1_000_000
            || self.turnover_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.exposure",
            });
        }
        self.orders.iter().try_for_each(OrderIntent::validate)?;
        if self
            .orders
            .iter()
            .map(|order| order.asset)
            .collect::<BTreeSet<_>>()
            .len()
            != self.orders.len()
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.orders",
            });
        }
        let gross = self
            .target
            .weights
            .values()
            .try_fold(0_u32, |sum, weight| {
                sum.checked_add(weight.0).ok_or(DomainError::InvalidBudget {
                    field: "execution_plan.gross_exposure_ppm",
                })
            })?;
        if self.gross_exposure_ppm != gross
            || self.net_exposure_ppm != i64::from(gross)
            || self.factor_exposure != FactorExposure::from_target(&self.target)?
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.derived_exposure",
            });
        }
        if self.plan_hash != self.expected_hash()? {
            return Err(DomainError::ExecutionPlanHashMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub schema_version: u32,
    pub run_id: RunId,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub execution_plan: Option<ArtifactRef>,
    pub factor_exposure: Option<FactorExposure>,
    pub turnover_ppm: Option<u32>,
    pub plan_hash: Option<ContentHash>,
    pub broker_session: Option<String>,
    pub frozen: bool,
    pub created_at: DateTime<Utc>,
}

impl ExecutionContext {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.run_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "execution_context.identity",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self
                .account_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .quote_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .market_clock_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .execution_plan
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::ExecutionPlan)
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.references",
            });
        }
        if self
            .turnover_ppm
            .is_some_and(|turnover| turnover > 1_000_000)
            || self
                .broker_session
                .as_ref()
                .is_some_and(|session| session.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_context.derived",
            });
        }
        if let Some(exposure) = &self.factor_exposure {
            exposure.validate()?;
        }
        Ok(())
    }

    pub fn validate_complete_plan_closure(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.account_snapshot.is_none()
            || self.quote_snapshot.is_none()
            || self.market_clock_snapshot.is_none()
            || self.execution_plan.is_none()
            || self.factor_exposure.is_none()
            || self.turnover_ppm.is_none()
            || self.plan_hash.is_none()
            || self.broker_session.is_none()
            || self.frozen
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.plan_closure",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoOrder {
    pub execution_context: ArtifactRef,
    pub blockers: Vec<HardBlocker>,
    pub created_at: DateTime<Utc>,
}

impl NoOrder {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.execution_context.kind != ArtifactKind::ExecutionContext || self.blockers.is_empty()
        {
            return Err(DomainError::EmptyField { field: "no_order" });
        }
        Ok(())
    }
}

/// Rust-owned result of the execution gate. A Paper adapter may consume only
/// the accepted branch; every rejection remains a durable, typed `NoOrder`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum ExecutionVerdict {
    Accepted { execution_context: ArtifactRef },
    NoOrder { no_order: NoOrder },
}

impl ExecutionVerdict {
    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Accepted { execution_context } => {
                if execution_context.kind != ArtifactKind::ExecutionContext {
                    return Err(DomainError::EmptyField {
                        field: "execution_verdict.execution_context",
                    });
                }
            }
            Self::NoOrder { no_order } => no_order.validate()?,
        }
        Ok(())
    }
}

/// Immutable freeze-state history. The latest canonical artifact controls
/// execution; an unfreeze is a new state record rather than an in-place edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreezeState {
    pub schema_version: u32,
    pub frozen: bool,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
}

impl FreezeState {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.reason.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "freeze_state",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperCommitment {
    pub commitment_id: PaperCommitmentId,
    pub execution_context: ArtifactRef,
    pub plan_hash: ContentHash,
    pub broker_session: String,
    pub client_order_ids: BTreeMap<Asset, String>,
    pub created_at: DateTime<Utc>,
}

impl PaperCommitment {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.commitment_id.0.trim().is_empty()
            || self.broker_session.trim().is_empty()
            || self.client_order_ids.is_empty()
            || self.execution_context.kind != ArtifactKind::ExecutionContext
            || self
                .client_order_ids
                .values()
                .any(|client_order_id| client_order_id.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "paper_commitment",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderReceiptState {
    Accepted,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderReceipt {
    pub plan_hash: ContentHash,
    pub asset: Asset,
    pub client_order_id: String,
    pub broker_order_id: String,
    pub state: OrderReceiptState,
    pub requested_quantity_micros: i64,
    pub filled_quantity_micros: i64,
    pub remaining_quantity_micros: i64,
    pub average_fill_price: Option<MoneyMicros>,
    pub broker_updated_at: DateTime<Utc>,
    pub reason: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl OrderReceipt {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.client_order_id.trim().is_empty()
            || self.broker_order_id.trim().is_empty()
            || self
                .reason
                .as_ref()
                .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "order_receipt.identity",
            });
        }
        if self.requested_quantity_micros <= 0
            || self.filled_quantity_micros < 0
            || self.remaining_quantity_micros < 0
            || self
                .filled_quantity_micros
                .checked_add(self.remaining_quantity_micros)
                != Some(self.requested_quantity_micros)
            || self.average_fill_price.is_some_and(|price| price.0 <= 0)
        {
            return Err(DomainError::InvalidBudget {
                field: "order_receipt.quantity",
            });
        }
        let state_is_consistent = match self.state {
            OrderReceiptState::Accepted => {
                self.filled_quantity_micros == 0
                    && self.remaining_quantity_micros == self.requested_quantity_micros
                    && self.average_fill_price.is_none()
            }
            OrderReceiptState::PartiallyFilled => {
                self.filled_quantity_micros > 0
                    && self.remaining_quantity_micros > 0
                    && self.average_fill_price.is_some()
            }
            OrderReceiptState::Filled => {
                self.filled_quantity_micros == self.requested_quantity_micros
                    && self.remaining_quantity_micros == 0
                    && self.average_fill_price.is_some()
            }
            OrderReceiptState::Canceled
            | OrderReceiptState::Rejected
            | OrderReceiptState::Failed => {
                self.average_fill_price.is_some() == (self.filled_quantity_micros > 0)
            }
        };
        if !state_is_consistent {
            return Err(DomainError::InvalidBudget {
                field: "order_receipt.state",
            });
        }
        Ok(())
    }
}

/// One and only one Rust-owned cancellation/replacement lineage for an order
/// in a committed Paper session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperReprice {
    pub schema_version: u32,
    pub reprice_id: PaperRepriceId,
    pub commitment: ArtifactRef,
    pub prior_receipt: ArtifactRef,
    pub asset: Asset,
    pub prior_client_order_id: String,
    pub replacement_client_order_id: String,
    pub prior_broker_order_id: String,
    pub replacement_limit_price: MoneyMicros,
    pub created_at: DateTime<Utc>,
}

impl PaperReprice {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.reprice_id.0.trim().is_empty()
            || self.commitment.kind != ArtifactKind::ExecutionCommitment
            || self.prior_receipt.kind != ArtifactKind::OrderReceipt
            || self.prior_broker_order_id.trim().is_empty()
            || self.replacement_limit_price.0 <= 0
        {
            return Err(DomainError::EmptyField {
                field: "paper_reprice",
            });
        }
        let Some(base) = self.prior_client_order_id.strip_suffix("-r0") else {
            return Err(DomainError::InvalidRepriceLineage);
        };
        if base.is_empty() || self.replacement_client_order_id != format!("{base}-r1") {
            return Err(DomainError::InvalidRepriceLineage);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    Pending,
    Partial,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reconciliation {
    pub reconciliation_id: ReconciliationId,
    pub commitment: ArtifactRef,
    pub state: ReconciliationState,
    pub broker_receipts: Vec<ArtifactRef>,
    pub reconciled_at: DateTime<Utc>,
}

impl Reconciliation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.reconciliation_id.0.trim().is_empty()
            || self.commitment.kind != ArtifactKind::ExecutionCommitment
            || self
                .broker_receipts
                .iter()
                .any(|receipt| receipt.kind != ArtifactKind::OrderReceipt)
        {
            return Err(DomainError::EmptyField {
                field: "reconciliation",
            });
        }
        if self.state == ReconciliationState::Complete && self.broker_receipts.is_empty() {
            return Err(DomainError::EmptyField {
                field: "reconciliation.receipts",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{artifact::ArtifactId, WeightPpm};

    fn reference(kind: ArtifactKind, name: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(name)),
            kind,
        }
    }

    fn plan() -> ExecutionPlan {
        let mut target = TargetPortfolio::zeroed();
        target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
        let mut plan = ExecutionPlan {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            decision_context: reference(ArtifactKind::DecisionContext, b"decision"),
            account_snapshot: reference(ArtifactKind::NormalizedEvidence, b"account"),
            quote_snapshot: reference(ArtifactKind::NormalizedEvidence, b"quotes"),
            market_clock_snapshot: reference(ArtifactKind::NormalizedEvidence, b"clock"),
            policy_hash: ContentHash::of_bytes(b"policy"),
            target: target.clone(),
            orders: vec![OrderIntent {
                asset: Asset::Tqqq,
                side: OrderSide::Buy,
                notional: MoneyMicros::from_usd_cents(10_000),
                limit_price: MoneyMicros::from_usd_cents(5_000),
            }],
            gross_exposure_ppm: 100_000,
            net_exposure_ppm: 100_000,
            factor_exposure: FactorExposure::from_target(&target).unwrap(),
            turnover_ppm: 100_000,
            broker_session: "2026-08-10".to_owned(),
            created_at: Utc::now(),
            plan_hash: ContentHash::of_bytes(b"pending"),
        };
        plan.refresh_hash().unwrap();
        plan
    }

    #[test]
    fn execution_plan_hash_covers_every_payload_field() {
        let mut plan = plan();
        assert!(plan.validate().is_ok());
        plan.orders[0].notional = MoneyMicros::from_usd_cents(20_000);
        assert_eq!(plan.validate(), Err(DomainError::ExecutionPlanHashMismatch));
    }

    #[test]
    fn accepted_context_requires_complete_plan_closure() {
        let context = ExecutionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            run_id: RunId::new(),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision"),
            account_snapshot: None,
            quote_snapshot: None,
            market_clock_snapshot: None,
            execution_plan: None,
            factor_exposure: None,
            turnover_ppm: None,
            plan_hash: None,
            broker_session: None,
            frozen: false,
            created_at: Utc::now(),
        };
        assert!(context.validate().is_ok());
        assert!(context.validate_complete_plan_closure().is_err());
    }

    #[test]
    fn order_receipt_requires_quantity_conservation() {
        let now = Utc::now();
        let mut receipt = OrderReceipt {
            plan_hash: ContentHash::of_bytes(b"plan"),
            asset: Asset::Qqq,
            client_order_id: "client-order".to_owned(),
            broker_order_id: "broker-order".to_owned(),
            state: OrderReceiptState::PartiallyFilled,
            requested_quantity_micros: 10,
            filled_quantity_micros: 4,
            remaining_quantity_micros: 6,
            average_fill_price: Some(MoneyMicros::from_usd_cents(10_000)),
            broker_updated_at: now,
            reason: None,
            observed_at: now,
        };
        assert!(receipt.validate().is_ok());
        receipt.remaining_quantity_micros = 5;
        assert!(receipt.validate().is_err());

        receipt.remaining_quantity_micros = 6;
        receipt.state = OrderReceiptState::Filled;
        assert_eq!(
            receipt.validate(),
            Err(DomainError::InvalidBudget {
                field: "order_receipt.state",
            })
        );
    }

    #[test]
    fn no_order_requires_a_typed_blocker() {
        let no_order = NoOrder {
            execution_context: reference(ArtifactKind::ExecutionContext, b"context"),
            blockers: vec![],
            created_at: Utc::now(),
        };
        assert!(no_order.validate().is_err());
    }
}
