// 文件导读：记录执行 Gate 的 NoOrder/Accepted verdict、冻结状态、Commitment、
// Broker receipt、取消/重价 lineage 和 Reconciliation；这些只是意图与回执的类型，
// “先持久化 Commitment 再允许 Paper I/O”的顺序由 Store/execution 调用链保证。
// 这些记录描述已授权执行边界与实际回执，domain 只校验一致性；真正的 Store 提交及 Alpaca I/O 位于 execution/daemon 层。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoOrder {
    pub execution_context: ArtifactRef,
    pub blockers: Vec<HardBlocker>,
    pub created_at: DateTime<Utc>,
}

impl NoOrder {
    // NoOrder 必须关联 ExecutionContext，并至少记录一个硬 blocker。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.execution_context.kind != ArtifactKind::ExecutionContext || self.blockers.is_empty()
        {
            return Err(DomainError::EmptyField { field: "no_order" });
        }
        Ok(())
    }
}

/// Rust-owned result of the execution gate. A Paper adapter may consume only
/// an accepted branch after the separate Store/approval checks. This enum
/// represents a rejection as `NoOrder`; durability requires a Store commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum ExecutionVerdict {
    Accepted { execution_context: ArtifactRef },
    NoOrder { no_order: NoOrder },
}

impl ExecutionVerdict {
    // Accepted 只检查上下文 kind；NoOrder 复用其完整 blocker 校验。
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
    // 校验 schema 和非空冻结/解冻原因；历史状态以新 Artifact 追加而非原地修改。
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
    // 校验 commitment 身份、执行上下文、broker session 和所有 client order ID 非空。
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
    // 终态不应再有后继 receipt；这些状态之外仍可能收到 Broker 更新。
    pub const fn is_final_without_successor(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Expired | Self::Rejected | Self::Failed
        )
    }

    // 判断状态是否允许后续对账/更新事件继续到达。
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
    // 校验订单身份、数量守恒、价格符号以及 state 与成交/剩余数量的一致性。
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
        // match 把 Broker 状态映射到精确的成交量/均价条件，避免只看 status 字符串。
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

/// Rust-owned cancellation intent. Its type does not itself commit anything;
/// the execution path must persist it before the broker side effect.
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
    // 取消意图必须绑定原 Commitment、Prior receipt 和两侧 broker/client ID。
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

/// Rust-owned r0→r1 replacement shape for an order in a Paper session.
/// Uniqueness across retries or broker operations is enforced outside this
/// local value check by the Store/dispatch boundary.
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
    // 校验重价对象身份，并强制 client order ID 只能沿 r0 -> r1 单一 lineage。
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
        // let-else 要求 prior ID 明确以 -r0 结尾，否则不能构成合法重价链。
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperUnsubmittedReason {
    SubmissionWindowExpired,
    BatchObservationOnly,
    RunCancelled,
}

/// A reference to one authoritative per-order journal transition. The enclosing
/// Reconciliation supplies the exact Commitment identity for the transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsubmittedPaperOrder {
    pub asset: Asset,
    pub client_order_id: String,
    pub transition_index: u32,
    pub reason: PaperUnsubmittedReason,
    /// Present for the successor of a durable reprice intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reprice: Option<ArtifactRef>,
}

impl UnsubmittedPaperOrder {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.client_order_id.trim().is_empty()
            || self.transition_index == 0
            || !Asset::EXECUTABLE.contains(&self.asset)
            || self
                .reprice
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::ExecutionReprice)
        {
            return Err(DomainError::EmptyField {
                field: "reconciliation.unsubmitted_order",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reconciliation {
    pub reconciliation_id: ReconciliationId,
    pub commitment: ArtifactRef,
    pub state: ReconciliationState,
    pub broker_receipts: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsubmitted_orders: Vec<UnsubmittedPaperOrder>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub achieved_target: Option<TargetPortfolio>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub achieved_factor_exposure: Option<FactorExposure>,
    pub reconciled_at: DateTime<Utc>,
}

impl Reconciliation {
    // 校验 Commitment、receipt 和未发凭证形状，成就组合和因子暴露要成对出现。
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
        let mut witness_assets = BTreeSet::new();
        let mut witness_ids = BTreeSet::new();
        for witness in &self.unsubmitted_orders {
            witness.validate()?;
            if !witness_assets.insert(witness.asset)
                || !witness_ids.insert(&witness.client_order_id)
            {
                return Err(DomainError::EmptyField {
                    field: "reconciliation.duplicate_unsubmitted_order",
                });
            }
        }
        if self.state == ReconciliationState::Complete
            && self.broker_receipts.is_empty()
            && !self
                .unsubmitted_orders
                .iter()
                .any(|order| order.reprice.is_none())
        {
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

/// Verify the identities of observed and proven-unsubmitted orders, then
/// determine whether all original orders and durable reprices are closed.
/// The caller loads the order evidence and checks journal proofs in the Store.
pub fn reconciliation_state_for_paper_orders(
    commitment_ref: &ArtifactRef,
    commitment: &PaperCommitment,
    receipts: &[OrderReceipt],
    reprices: &BTreeMap<Asset, (ArtifactRef, PaperReprice)>,
    unsubmitted_orders: &[UnsubmittedPaperOrder],
) -> Result<ReconciliationState, DomainError> {
    let invalid = || DomainError::InvalidBudget {
        field: "reconciliation.order_closure",
    };
    if commitment_ref.kind != ArtifactKind::ExecutionCommitment {
        return Err(invalid());
    }
    for (asset, (reference, reprice)) in reprices {
        if reference.kind != ArtifactKind::ExecutionReprice
            || reprice.commitment != *commitment_ref
            || reprice.asset != *asset
            || commitment.client_order_ids.get(asset) != Some(&reprice.prior_client_order_id)
        {
            return Err(invalid());
        }
    }
    let mut covered_assets = BTreeSet::new();
    let mut closed_successors = BTreeSet::new();
    let mut receipt_ids = BTreeSet::new();
    for receipt in receipts {
        let original_order_id = commitment
            .client_order_ids
            .get(&receipt.asset)
            .ok_or_else(invalid)?;
        let is_successor = reprices.get(&receipt.asset).is_some_and(|(_, reprice)| {
            reprice.replacement_client_order_id == receipt.client_order_id
        });
        if receipt.plan_hash != commitment.plan_hash
            || (!is_successor && receipt.client_order_id != *original_order_id)
            || !covered_assets.insert(receipt.asset)
            || !receipt_ids.insert(receipt.client_order_id.as_str())
        {
            return Err(invalid());
        }
        if is_successor {
            closed_successors.insert(receipt.asset);
        }
    }
    for proof in unsubmitted_orders {
        proof.validate()?;
        if !receipt_ids.insert(&proof.client_order_id) {
            return Err(invalid());
        }
        if let Some(reference) = &proof.reprice {
            let (durable_ref, reprice) = reprices.get(&proof.asset).ok_or_else(invalid)?;
            if reference != durable_ref
                || proof.client_order_id != reprice.replacement_client_order_id
                || !closed_successors.insert(proof.asset)
            {
                return Err(invalid());
            }
        } else if commitment.client_order_ids.get(&proof.asset) != Some(&proof.client_order_id)
            || reprices.contains_key(&proof.asset)
            || !covered_assets.insert(proof.asset)
        {
            return Err(invalid());
        }
    }
    if closed_successors.len() == reprices.len()
        && covered_assets.len() == commitment.client_order_ids.len()
        && receipts
            .iter()
            .all(|receipt| receipt.state.is_final_without_successor())
    {
        Ok(ReconciliationState::Complete)
    } else if receipts.iter().any(|receipt| {
        matches!(
            receipt.state,
            OrderReceiptState::PartiallyFilled | OrderReceiptState::Filled
        )
    }) {
        Ok(ReconciliationState::Partial)
    } else {
        Ok(ReconciliationState::Pending)
    }
}

/// Aggregate the original and replacement observations in original order units.
pub fn normalize_paper_replacement_receipt(
    reprice: &PaperReprice,
    prior: &OrderReceipt,
    replacement: &OrderReceipt,
) -> Result<OrderReceipt, DomainError> {
    prior.validate()?;
    replacement.validate()?;
    if prior.asset != replacement.asset
        || prior.asset != reprice.asset
        || prior.client_order_id != reprice.prior_client_order_id
        || prior.broker_order_id != reprice.prior_broker_order_id
        || replacement.client_order_id != reprice.replacement_client_order_id
        || prior.plan_hash != replacement.plan_hash
    {
        return Err(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        });
    }

    // 两种 broker wire 语义分支：完整原数量要求 replacement 累计 filled 不倒退；
    // 剩余数量只在 requested == prior.remaining 时才允许与 prior fill 相加。
    if replacement.requested_quantity_micros == prior.requested_quantity_micros {
        if replacement.filled_quantity_micros < prior.filled_quantity_micros {
            return Err(DomainError::InvalidBudget {
                field: "paper_reprice.receipt",
            });
        }
        return Ok(replacement.clone());
    }
    if replacement.requested_quantity_micros != prior.remaining_quantity_micros {
        return Err(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        });
    }

    // 对“仅剩余量”形式进行 checked 累加并验证不超过原申请量；加权价缺失或算术溢出
    // 会返回 RepriceMismatch，不生成半合并回执。
    let filled_quantity_micros = prior
        .filled_quantity_micros
        .checked_add(replacement.filled_quantity_micros)
        .ok_or(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        })?;
    if filled_quantity_micros > prior.requested_quantity_micros {
        return Err(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        });
    }
    let remaining_quantity_micros = prior.requested_quantity_micros - filled_quantity_micros;
    let average_fill_price = weighted_fill_price(prior, replacement)?;
    // 完全成交强制归 Filled；否则若先前已有成交且 successor 仍 Accepted，则合并状态
    // 为 PartiallyFilled；其它终态沿用 Broker successor 状态。
    let state = if remaining_quantity_micros == 0 {
        OrderReceiptState::Filled
    } else if filled_quantity_micros > 0 && replacement.state == OrderReceiptState::Accepted {
        OrderReceiptState::PartiallyFilled
    } else {
        replacement.state
    };
    let merged = OrderReceipt {
        requested_quantity_micros: prior.requested_quantity_micros,
        filled_quantity_micros,
        remaining_quantity_micros,
        average_fill_price,
        state,
        ..replacement.clone()
    };
    merged.validate()?;
    Ok(merged)
}

fn weighted_fill_price(
    prior: &OrderReceipt,
    replacement: &OrderReceipt,
) -> Result<Option<MoneyMicros>, DomainError> {
    // 以两段成交数量加权平均价格，零成交返回 None；缺少有成交段的 average price 或
    // 乘加溢出都保持 RepriceMismatch，不能用零价填充。
    // 输入只共享借用；total_quantity 为两段填充数量之和，溢出代表无法可信合并而报错。
    let total_quantity = prior
        .filled_quantity_micros
        .checked_add(replacement.filled_quantity_micros)
        .ok_or(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        })?;
    if total_quantity == 0 {
        return Ok(None);
    }
    // 闭包按 receipt 借用计算 quantity×price 的 i128 贡献；零成交不要求价格，有成交缺价
    // 返回 Err。两个 Result 由 `?` 顺序传播，再以总数量整除得到截尾整数均价。
    let contribution = |receipt: &OrderReceipt| -> Result<i128, DomainError> {
        if receipt.filled_quantity_micros == 0 {
            return Ok(0);
        }
        let price = receipt
            .average_fill_price
            .ok_or(DomainError::InvalidBudget {
                field: "paper_reprice.receipt",
            })?;
        i128::from(receipt.filled_quantity_micros)
            .checked_mul(i128::from(price.0))
            .ok_or(DomainError::InvalidBudget {
                field: "paper_reprice.receipt",
            })
    };
    let weighted = contribution(prior)?
        .checked_add(contribution(replacement)?)
        .and_then(|value| value.checked_div(i128::from(total_quantity)))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(DomainError::InvalidBudget {
            field: "paper_reprice.receipt",
        })?;
    Ok(Some(MoneyMicros(weighted)))
}

#[cfg(test)]
mod paper_reconciliation_serialization_tests {
    use super::*;

    fn fixture_ref(kind: ArtifactKind) -> ArtifactRef {
        ArtifactRef {
            artifact_id: crate::ArtifactId(ContentHash::of_bytes(format!("{kind:?}").as_bytes())),
            kind,
        }
    }

    /// WHY THIS TEST EXISTS (Task #30): Settlement requires coverage of each exact order, including any durable replacement.
    /// Do not weaken or delete this assertion.
    #[test]
    fn paper_settlement_requires_disjoint_terminal_receipts_or_exact_journal_proofs() {
        let now = Utc::now();
        let commitment = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: fixture_ref(ArtifactKind::ExecutionContext),
            plan_hash: ContentHash::of_bytes(b"frozen plan"),
            broker_session: "2026-11-19".into(),
            client_order_ids: [
                (Asset::Qqq, "qqq-r0".into()),
                (Asset::Soxx, "soxx-r0".into()),
            ]
            .into(),
            created_at: now,
        };
        let receipt = OrderReceipt {
            plan_hash: commitment.plan_hash.clone(),
            asset: Asset::Qqq,
            client_order_id: "qqq-r0".into(),
            broker_order_id: "observed-broker-id".into(),
            state: OrderReceiptState::Filled,
            requested_quantity_micros: 1_000_000,
            filled_quantity_micros: 1_000_000,
            remaining_quantity_micros: 0,
            average_fill_price: Some(MoneyMicros(1_000_000)),
            broker_updated_at: now,
            reason: None,
            observed_at: now,
        };
        let receipts = std::slice::from_ref(&receipt);
        let unsubmitted = UnsubmittedPaperOrder {
            asset: Asset::Soxx,
            client_order_id: "soxx-r0".into(),
            transition_index: 1,
            reason: PaperUnsubmittedReason::BatchObservationOnly,
            reprice: None,
        };
        let originals = std::slice::from_ref(&unsubmitted);
        let no_reprices = BTreeMap::new();
        let state = |reprices: &BTreeMap<Asset, (ArtifactRef, PaperReprice)>,
                     witnesses: &[UnsubmittedPaperOrder]| {
            reconciliation_state_for_paper_orders(
                &fixture_ref(ArtifactKind::ExecutionCommitment),
                &commitment,
                receipts,
                reprices,
                witnesses,
            )
        };
        assert_eq!(
            state(&no_reprices, &[]).unwrap(),
            ReconciliationState::Partial
        );
        assert_eq!(
            state(&no_reprices, originals).unwrap(),
            ReconciliationState::Complete
        );

        let replacement = fixture_ref(ArtifactKind::ExecutionReprice);
        let reprices = [(
            Asset::Qqq,
            (
                replacement.clone(),
                PaperReprice {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    reprice_id: PaperRepriceId::new(),
                    commitment: fixture_ref(ArtifactKind::ExecutionCommitment),
                    prior_receipt: fixture_ref(ArtifactKind::OrderReceipt),
                    asset: Asset::Qqq,
                    prior_client_order_id: "qqq-r0".into(),
                    replacement_client_order_id: "qqq-r1".into(),
                    prior_broker_order_id: "observed-broker-id".into(),
                    replacement_limit_price: MoneyMicros(1_000_000),
                    created_at: now,
                },
            ),
        )]
        .into();
        assert_eq!(
            state(&reprices, originals).unwrap(),
            ReconciliationState::Partial
        );
        let successor_unsubmitted = UnsubmittedPaperOrder {
            asset: Asset::Qqq,
            client_order_id: "qqq-r1".into(),
            reprice: Some(replacement),
            ..unsubmitted.clone()
        };
        let wrong_ref = UnsubmittedPaperOrder {
            reprice: Some(ArtifactRef {
                artifact_id: crate::ArtifactId(ContentHash::of_bytes(b"other replacement")),
                kind: ArtifactKind::ExecutionReprice,
            }),
            ..successor_unsubmitted.clone()
        };
        assert!(state(&reprices, &[unsubmitted.clone(), wrong_ref]).is_err());
        assert_eq!(
            state(&reprices, &[unsubmitted.clone(), successor_unsubmitted]).unwrap(),
            ReconciliationState::Complete
        );
        let contradicting = UnsubmittedPaperOrder {
            asset: Asset::Qqq,
            client_order_id: "qqq-r0".into(),
            ..unsubmitted.clone()
        };
        let wrong_original = UnsubmittedPaperOrder {
            client_order_id: "other-soxx-r0".into(),
            ..unsubmitted.clone()
        };
        assert!(state(&no_reprices, &[unsubmitted, contradicting]).is_err());
        assert!(state(&no_reprices, &[wrong_original]).is_err());
    }

    /// WHY THIS TEST EXISTS (Task #30): A complete all-unsubmitted batch must carry its durable evidence without a fabricated broker receipt.
    /// Do not weaken or delete this assertion.
    #[test]
    fn complete_reconciliation_can_retain_exact_never_submitted_evidence() {
        let now = Utc::now();
        let commitment = ArtifactRef {
            artifact_id: crate::ArtifactId(crate::ContentHash::of_bytes(b"durable commitment")),
            kind: ArtifactKind::ExecutionCommitment,
        };
        let witness = serde_json::json!({
            "asset": Asset::Qqq,
            "client_order_id": "original-qqq",
            "transition_index": 1,
            "reason": "batch_observation_only",
        });
        let mut wire = serde_json::json!({
            "reconciliation_id": "reconciled-batch",
            "commitment": commitment,
            "state": "complete",
            "broker_receipts": [],
            "unsubmitted_orders": [witness],
            "reconciled_at": now,
        });
        let complete: Reconciliation = serde_json::from_value(wire.clone()).unwrap();
        complete.validate().unwrap();
        assert_eq!(serde_json::to_value(complete).unwrap(), wire);

        wire.as_object_mut().unwrap().remove("unsubmitted_orders");
        let invalid: Reconciliation = serde_json::from_value(wire.clone()).unwrap();
        assert!(invalid.validate().is_err());
        wire["state"] = serde_json::json!("pending");
        let legacy: Reconciliation = serde_json::from_value(wire.clone()).unwrap();
        legacy.validate().unwrap();
        assert_eq!(serde_json::to_value(legacy).unwrap(), wire);
    }
}
