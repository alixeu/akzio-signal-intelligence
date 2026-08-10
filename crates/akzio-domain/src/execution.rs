//! Rust-gated Paper execution vocabulary.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    decision::HardBlocker,
    ids::{PaperCommitmentId, PaperRepriceId, ReconciliationId},
    Asset, ContentHash, DomainError, MoneyMicros, RunId, V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExposure {
    pub leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub tqqq_qqq_pair_ppm: u32,
    pub soxl_soxx_pair_ppm: u32,
}

impl FactorExposure {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub schema_version: u32,
    pub run_id: RunId,
    pub decision_context: ArtifactRef,
    pub account_snapshot: ArtifactRef,
    pub quote_snapshot: ArtifactRef,
    pub factor_exposure: FactorExposure,
    pub turnover_ppm: u32,
    pub plan_hash: ContentHash,
    pub broker_session: String,
    pub frozen: bool,
    pub created_at: DateTime<Utc>,
}

impl ExecutionContext {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "execution_context.schema_version",
            });
        }
        if self.run_id.0.trim().is_empty() || self.broker_session.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "execution_context.identity",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self.account_snapshot.kind != ArtifactKind::NormalizedEvidence
            || self.quote_snapshot.kind != ArtifactKind::NormalizedEvidence
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.references",
            });
        }
        if self.turnover_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "execution_context.turnover_ppm",
            });
        }
        self.factor_exposure.validate()
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
    pub observed_at: DateTime<Utc>,
}

impl OrderReceipt {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.client_order_id.trim().is_empty() || self.broker_order_id.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "order_receipt.identity",
            });
        }
        Ok(())
    }
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

/// One and only one Rust-owned cancellation/replacement lineage for an order
/// in a committed Paper session. The original commitment remains immutable;
/// this document records the bounded r0 -> r1 transition before any broker
/// request is made.
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

    use super::{
        ExecutionVerdict, FactorExposure, FreezeState, NoOrder, OrderReceipt, OrderReceiptState,
    };
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        decision::HardBlocker,
        ContentHash,
    };

    #[test]
    fn no_order_requires_a_typed_blocker() {
        let no_order = NoOrder {
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"context")),
                kind: ArtifactKind::ExecutionContext,
            },
            blockers: vec![],
            created_at: Utc::now(),
        };

        assert!(no_order.validate().is_err());
    }

    #[test]
    fn accepted_execution_verdict_requires_execution_context() {
        let verdict = ExecutionVerdict::Accepted {
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"wrong")),
                kind: ArtifactKind::Decision,
            },
        };

        assert!(verdict.validate().is_err());
    }

    #[test]
    fn freeze_state_requires_a_reason() {
        let state = FreezeState {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            frozen: true,
            reason: String::new(),
            changed_at: Utc::now(),
        };

        assert!(state.validate().is_err());
    }

    #[test]
    fn order_receipt_requires_broker_and_client_identity() {
        let receipt = OrderReceipt {
            plan_hash: ContentHash::of_bytes(b"plan"),
            asset: crate::Asset::Qqq,
            client_order_id: String::new(),
            broker_order_id: "broker-order".to_owned(),
            state: OrderReceiptState::Accepted,
            observed_at: Utc::now(),
        };

        assert!(receipt.validate().is_err());
    }

    #[test]
    fn factor_exposure_rejects_values_above_one() {
        let exposure = FactorExposure {
            leveraged_equity_ppm: 1_000_001,
            nasdaq_ppm: 0,
            semiconductor_ppm: 0,
            tqqq_qqq_pair_ppm: 0,
            soxl_soxx_pair_ppm: 0,
        };

        assert!(exposure.validate().is_err());
        assert_eq!(HardBlocker::Frozen, HardBlocker::Frozen);
    }
}
