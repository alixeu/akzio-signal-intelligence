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
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.reason.trim().is_empty() {
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
    PendingReplace,
    PendingCancel,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    Stopped,
    Rejected,
    Suspended,
    Calculated,
    Failed,
}

impl OrderReceiptState {
    pub const fn is_final_without_successor(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Expired | Self::Rejected | Self::Failed
        )
    }

    pub const fn may_receive_further_updates(self) -> bool {
        matches!(
            self,
            Self::Accepted
                | Self::PendingReplace
                | Self::PendingCancel
                | Self::PartiallyFilled
                | Self::DoneForDay
                | Self::Replaced
                | Self::Stopped
                | Self::Suspended
                | Self::Calculated
        )
    }
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
            OrderReceiptState::PendingReplace
            | OrderReceiptState::PendingCancel
            | OrderReceiptState::DoneForDay
            | OrderReceiptState::Replaced
            | OrderReceiptState::Stopped
            | OrderReceiptState::Suspended
            | OrderReceiptState::Calculated
            | OrderReceiptState::Canceled
            | OrderReceiptState::Expired
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperCancelReason {
    SettlementTimeout,
    RiskReduction,
    OperatorRequested,
}

/// Rust-owned cancellation intent committed before the broker side effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperCancel {
    pub schema_version: u32,
    pub cancel_id: PaperCancelId,
    pub commitment: ArtifactRef,
    pub prior_receipt: ArtifactRef,
    pub asset: Asset,
    pub client_order_id: String,
    pub broker_order_id: String,
    pub reason: PaperCancelReason,
    pub created_at: DateTime<Utc>,
}

impl PaperCancel {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.cancel_id.0.trim().is_empty()
            || self.commitment.kind != ArtifactKind::ExecutionCommitment
            || self.prior_receipt.kind != ArtifactKind::OrderReceipt
            || self.client_order_id.trim().is_empty()
            || self.broker_order_id.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "paper_cancel",
            });
        }
        Ok(())
    }
}

/// One and only one Rust-owned replacement lineage for an order in a
/// committed Paper session.
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
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub achieved_target: Option<TargetPortfolio>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub achieved_factor_exposure: Option<FactorExposure>,
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
        match (&self.achieved_target, &self.achieved_factor_exposure) {
            (Some(target), Some(exposure)) => {
                target.validate_universe()?;
                exposure.validate()?;
                if *exposure != FactorExposure::from_target(target)? {
                    return Err(DomainError::InvalidBudget {
                        field: "reconciliation.achieved_factor_exposure",
                    });
                }
            }
            (None, None) => {}
            _ => {
                return Err(DomainError::EmptyField {
                    field: "reconciliation.achieved_portfolio",
                });
            }
        }
        Ok(())
    }
}
