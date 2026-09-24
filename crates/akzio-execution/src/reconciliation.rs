//! Typed reconciliation for durable Paper commitments.

// 文件导读：ReconciliationRuntime 负责把已持久化 Commitment、订单 action intent 和
// Broker 回执重建为 CAS Artifact。它不发请求；先确认 Paper purpose、原/替换/取消引用
// 与 plan hash，再按资产去重回执、合并 repriced 数量和加权成交价，重建执行后目标。
// Complete 只有在所有订单有最终状态且每个 replacement successor 已被观察时成立；
// write_progress 允许 partial/pending 继续恢复，commit/commit_with_effect 才推进 task。

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, Asset, DomainError,
    ExecutionContext, ExecutionPlan, FactorExposure, LifecycleEventType, MoneyMicros, OrderReceipt,
    OrderReceiptState, OrderSide, PaperCancel, PaperCommitment, PaperReprice, Reconciliation,
    ReconciliationId, ReconciliationState, RunPurpose, TargetPortfolio, TaskStatus,
    TaskWritePermit, WeightPpm,
};
use akzio_store::{DaemonLease, Store, StoreError};
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
    #[error("cancel does not belong to the committed order")]
    CancelMismatch,
    #[error("broker reconciliation returned multiple receipts for {0}")]
    DuplicateReceipt(Asset),
}

pub type ReconciliationResult<T> = std::result::Result<T, ReconciliationError>;

#[derive(Debug, Clone)]
pub struct ReconciliationInput {
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
    pub reprices: Vec<ArtifactRef>,
    pub cancels: Vec<ArtifactRef>,
    pub broker_receipts: Vec<OrderReceipt>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReconciliationOutput {
    pub receipts: Vec<Artifact>,
    pub reconciliation: Artifact,
}

#[derive(Debug, Clone)]
pub struct ReconciliationRuntime {
    store: Store,
}

impl ReconciliationRuntime {
    pub fn new(store: Store) -> Self {
        // 对账读取同一个 V2Store，避免内存中的 broker 结果成为第二持久化权威。
        Self { store }
    }

    pub fn reconcile(
        &self,
        input: &ReconciliationInput,
    ) -> ReconciliationResult<ReconciliationOutput> {
        // 输入→加载 Commitment/action artifacts→校验每个回执的 plan/client ID→合并
        // replacement→重建 achieved portfolio→生成 Receipt/Reconciliation Artifact。该方法
        // 只 stage 结果，不改变 task 状态，调用方随后按 settled 与否选择提交或写进度。
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(ReconciliationError::NonPaperRun(purpose));
        }
        let commitment_artifact =
            self.load_expected(&input.commitment, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let mut reprices = BTreeMap::new();
        for reference in &input.reprices {
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
                || reprices
                    .insert(payload.asset, (reference.clone(), payload))
                    .is_some()
            {
                return Err(ReconciliationError::RepriceMismatch);
            }
        }

        let mut cancels = BTreeMap::new();
        for reference in &input.cancels {
            let artifact = self.load_expected(reference, ArtifactKind::ExecutionCancel)?;
            let payload: PaperCancel =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            payload.validate()?;
            let durable = self
                .store
                .cancel_for(&payload.commitment, payload.asset)?
                .ok_or(ReconciliationError::CancelMismatch)?;
            if payload.commitment != input.commitment
                || durable.artifact_id != reference.artifact_id
                || cancels
                    .insert(payload.asset, (reference.clone(), payload))
                    .is_some()
            {
                return Err(ReconciliationError::CancelMismatch);
            }
        }

        let normalized_receipts = input
            .broker_receipts
            .iter()
            .map(|receipt| {
                if let Some((_, reprice)) = reprices.get(&receipt.asset).filter(|(_, reprice)| {
                    reprice.replacement_client_order_id == receipt.client_order_id
                }) {
                    self.merge_reprice_receipt(reprice, receipt)
                } else {
                    Ok(receipt.clone())
                }
            })
            .collect::<ReconciliationResult<Vec<_>>>()?;

        let mut seen = BTreeSet::new();
        let mut receipts = Vec::with_capacity(normalized_receipts.len());
        for receipt in &normalized_receipts {
            receipt.validate()?;
            if receipt.plan_hash != commitment.plan_hash {
                return Err(ReconciliationError::PlanHashMismatch);
            }
            let is_committed_client_id =
                commitment.client_order_ids.get(&receipt.asset) == Some(&receipt.client_order_id);
            let is_reprice_client_id = reprices.get(&receipt.asset).is_some_and(|(_, reprice)| {
                reprice.replacement_client_order_id == receipt.client_order_id
            });
            if !is_committed_client_id && !is_reprice_client_id {
                return Err(ReconciliationError::ClientOrderMismatch(receipt.asset));
            }
            if !seen.insert(receipt.asset) {
                return Err(ReconciliationError::DuplicateReceipt(receipt.asset));
            }
            let mut source_refs = vec![input.commitment.clone()];
            if let Some((reprice, _)) = reprices
                .get(&receipt.asset)
                .filter(|_| is_reprice_client_id)
            {
                source_refs.push(reprice.clone());
            }
            if let Some((cancel, _)) = cancels.get(&receipt.asset) {
                source_refs.push(cancel.clone());
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
        let state = reconciliation_state_with_reprices(
            &commitment,
            &normalized_receipts,
            &reprices
                .values()
                .map(|(_, reprice)| reprice.clone())
                .collect::<Vec<_>>(),
        );
        let (achieved_target, achieved_factor_exposure) =
            self.achieved_portfolio(&commitment, &normalized_receipts)?;
        let payload = Reconciliation {
            reconciliation_id: ReconciliationId::new(),
            commitment: input.commitment.clone(),
            state,
            broker_receipts: receipt_refs.clone(),
            achieved_target: Some(achieved_target),
            achieved_factor_exposure: Some(achieved_factor_exposure),
            reconciled_at: input.now,
        };
        payload.validate()?;
        let mut source_refs = Vec::with_capacity(receipt_refs.len() + 2);
        source_refs.push(input.commitment.clone());
        source_refs.extend(input.reprices.iter().cloned());
        source_refs.extend(input.cancels.iter().cloned());
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
        // 没有 broker effect intent 的普通完成路径，把所有 receipt 与 reconciliation 在
        // 同一个 fenced attempt 中发布并结束 task。
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
        // 已记录 Paper effect intent 的路径同时结算 effect，标记 recovered 与否，保证
        // 请求前/请求后崩溃恢复不会提前发布不完整的成功输出。
        let mut artifacts = output.receipts.clone();
        artifacts.push(output.reconciliation.clone());
        self.store
            .commit_fenced_attempt_with_effect(lease, permit, &artifacts, effect, recovered, now)?;
        Ok(())
    }

    pub fn write_progress(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        output: &ReconciliationOutput,
        now: DateTime<Utc>,
    ) -> ReconciliationResult<()> {
        // partial/pending 只逐个写入中间 Artifact；若 payload 已 Complete/Failed 则拒绝
        // 当作“进度”覆盖终态，保留 CAS 历史和下一次恢复入口。
        let payload: Reconciliation =
            serde_json::from_slice(&self.store.read_blob(&output.reconciliation.blob)?)?;
        if matches!(
            payload.state,
            ReconciliationState::Complete | ReconciliationState::Failed
        ) {
            return Err(ReconciliationError::Domain(DomainError::InvalidBudget {
                field: "reconciliation.progress_state",
            }));
        }
        let mut artifacts = output.receipts.clone();
        artifacts.push(output.reconciliation.clone());
        for artifact in artifacts {
            self.store.write_task_artifact_fenced(
                Some(lease),
                permit,
                &artifact,
                LifecycleEventType::ArtifactCommitted,
                now,
            )?;
        }
        Ok(())
    }

    fn merge_reprice_receipt(
        &self,
        reprice: &PaperReprice,
        replacement: &OrderReceipt,
    ) -> ReconciliationResult<OrderReceipt> {
        // 读取 prior receipt 后区分“broker 重复返回原数量”和“replacement 只剩余数量”两种
        // wire 语义；仅在数量、ID、资产和 plan 都闭合时累加成交量并计算加权成交价。
        let prior_artifact =
            self.load_expected(&reprice.prior_receipt, ArtifactKind::OrderReceipt)?;
        let prior: OrderReceipt =
            serde_json::from_slice(&self.store.read_blob(&prior_artifact.blob)?)?;
        prior.validate()?;
        if prior.asset != replacement.asset
            || prior.asset != reprice.asset
            || prior.client_order_id != reprice.prior_client_order_id
            || prior.broker_order_id != reprice.prior_broker_order_id
            || replacement.client_order_id != reprice.replacement_client_order_id
            || prior.plan_hash != replacement.plan_hash
        {
            return Err(ReconciliationError::RepriceMismatch);
        }

        if replacement.requested_quantity_micros == prior.requested_quantity_micros {
            if replacement.filled_quantity_micros < prior.filled_quantity_micros {
                return Err(ReconciliationError::RepriceMismatch);
            }
            return Ok(replacement.clone());
        }
        if replacement.requested_quantity_micros != prior.remaining_quantity_micros {
            return Err(ReconciliationError::RepriceMismatch);
        }

        let filled_quantity_micros = prior
            .filled_quantity_micros
            .checked_add(replacement.filled_quantity_micros)
            .ok_or(ReconciliationError::RepriceMismatch)?;
        if filled_quantity_micros > prior.requested_quantity_micros {
            return Err(ReconciliationError::RepriceMismatch);
        }
        let remaining_quantity_micros = prior.requested_quantity_micros - filled_quantity_micros;
        let average_fill_price = weighted_fill_price(&prior, replacement)?;
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

    fn achieved_portfolio(
        &self,
        commitment: &PaperCommitment,
        receipts: &[OrderReceipt],
    ) -> ReconciliationResult<(TargetPortfolio, FactorExposure)> {
        // 从 ExecutionContext→ExecutionPlan→账户快照恢复初始市值，再把实际 filled quantity
        // × average price 按买卖方向应用；这描述执行后敞口，不是后续账户 NAV 或学习结果。
        let context_artifact = self.load_expected(
            &commitment.execution_context,
            ArtifactKind::ExecutionContext,
        )?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        let plan_ref = context.execution_plan.ok_or(DomainError::EmptyField {
            field: "reconciliation.execution_plan",
        })?;
        let plan_artifact = self.load_expected(&plan_ref, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if plan.plan_hash != commitment.plan_hash {
            return Err(ReconciliationError::PlanHashMismatch);
        }
        let account_artifact =
            self.load_expected(&plan.account_snapshot, ArtifactKind::NormalizedEvidence)?;
        let account: AccountSnapshot =
            serde_json::from_slice(&self.store.read_blob(&account_artifact.blob)?)?;
        account.validate()?;

        let mut target = TargetPortfolio::zeroed();
        for asset in Asset::EXECUTABLE {
            let mut market_value = i128::from(
                account
                    .positions
                    .get(&asset)
                    .map_or(MoneyMicros::ZERO, |position| position.market_value)
                    .0,
            );
            if let Some(receipt) = receipts.iter().find(|receipt| receipt.asset == asset) {
                let filled_notional =
                    match (receipt.filled_quantity_micros, receipt.average_fill_price) {
                        (0, _) => 0_i128,
                        (quantity, Some(price)) => i128::from(quantity)
                            .checked_mul(i128::from(price.0))
                            .and_then(|value| value.checked_div(1_000_000))
                            .ok_or(DomainError::InvalidBudget {
                                field: "reconciliation.fill_notional",
                            })?,
                        _ => {
                            return Err(ReconciliationError::Domain(DomainError::InvalidBudget {
                                field: "reconciliation.fill_notional",
                            }));
                        }
                    };
                let order = plan
                    .orders
                    .iter()
                    .find(|order| order.asset == asset)
                    .ok_or(DomainError::EmptyField {
                        field: "reconciliation.order",
                    })?;
                market_value = match order.side {
                    OrderSide::Buy => market_value.checked_add(filled_notional),
                    OrderSide::Sell => market_value.checked_sub(filled_notional),
                }
                .ok_or(DomainError::InvalidBudget {
                    field: "reconciliation.achieved_market_value",
                })?;
            }
            if market_value < 0 {
                return Err(ReconciliationError::Domain(DomainError::InvalidBudget {
                    field: "reconciliation.achieved_market_value",
                }));
            }
            let weight = market_value
                .checked_mul(1_000_000)
                .and_then(|value| value.checked_div(i128::from(account.equity.0)))
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(DomainError::InvalidBudget {
                    field: "reconciliation.achieved_weight",
                })?;
            if weight > WeightPpm::SCALE {
                return Err(ReconciliationError::Domain(DomainError::InvalidBudget {
                    field: "reconciliation.achieved_weight",
                }));
            }
            target.weights.insert(asset, WeightPpm(weight));
        }
        target.validate_universe()?;
        let factor_exposure = FactorExposure::from_target(&target)?;
        Ok((target, factor_exposure))
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> ReconciliationResult<Artifact> {
        // 读取前同时检查引用 kind 与 Store kind，保持 CAS lineage 的类型闭包。
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
        // 统一 stage 对账 payload，并继承当前 permit 的 provenance；发布仍由 fenced Store
        // 方法完成。
        Ok(Artifact::new(
            kind,
            self.store.stage_json(payload)?,
            producer,
            ArtifactLifecycle::Canonical,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}

fn weighted_fill_price(
    prior: &OrderReceipt,
    replacement: &OrderReceipt,
) -> ReconciliationResult<Option<MoneyMicros>> {
    // 以两段成交数量加权平均价格，零成交返回 None；缺少有成交段的 average price 或
    // 乘加溢出都保持 RepriceMismatch，不能用零价填充。
    let total_quantity = prior
        .filled_quantity_micros
        .checked_add(replacement.filled_quantity_micros)
        .ok_or(ReconciliationError::RepriceMismatch)?;
    if total_quantity == 0 {
        return Ok(None);
    }
    let contribution = |receipt: &OrderReceipt| -> ReconciliationResult<i128> {
        if receipt.filled_quantity_micros == 0 {
            return Ok(0);
        }
        let price = receipt
            .average_fill_price
            .ok_or(ReconciliationError::RepriceMismatch)?;
        i128::from(receipt.filled_quantity_micros)
            .checked_mul(i128::from(price.0))
            .ok_or(ReconciliationError::RepriceMismatch)
    };
    let weighted = contribution(prior)?
        .checked_add(contribution(replacement)?)
        .and_then(|value| value.checked_div(i128::from(total_quantity)))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ReconciliationError::RepriceMismatch)?;
    Ok(Some(MoneyMicros(weighted)))
}

fn reconciliation_state_with_reprices(
    commitment: &PaperCommitment,
    receipts: &[OrderReceipt],
    reprices: &[PaperReprice],
) -> ReconciliationState {
    // 先要求所有 durable successor 都被观察，再判断回执数量与每个状态；原单终态但
    // successor 未知时保留 Pending，防止把“取消原单”误报为整体完成。
    let receipt_count = receipts.len();
    // A canceled/filled original cannot prove an uncertain replacement never
    // reached the broker. Only observing its durable successor closes that gap.
    let successors_observed = reprices.iter().all(|reprice| {
        receipts.iter().any(|receipt| {
            receipt.asset == reprice.asset
                && receipt.client_order_id == reprice.replacement_client_order_id
        })
    });
    if successors_observed
        && receipt_count == commitment.client_order_ids.len()
        && receipts
            .iter()
            .all(|receipt| receipt.state.is_final_without_successor())
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
mod recovery_tests {
    use super::*;

    #[test]
    fn terminal_original_does_not_settle_unknown_reprice_successor() {
        // 回归测试覆盖不确定替换：原订单已取消仍不能在 successor 未观察时关闭 commitment。
        let reference = |kind| ArtifactRef {
            artifact_id: akzio_domain::ArtifactId(akzio_domain::ContentHash::of_bytes(
                format!("{kind:?}").as_bytes(),
            )),
            kind,
        };
        let now = Utc::now();
        let commitment = PaperCommitment {
            commitment_id: akzio_domain::PaperCommitmentId::new(),
            execution_context: reference(ArtifactKind::ExecutionContext),
            plan_hash: akzio_domain::ContentHash::of_bytes(b"plan"),
            broker_session: "2026-09-09".into(),
            client_order_ids: [(Asset::Qqq, "order-r0".into())].into(),
            created_at: now,
        };
        let reprice = PaperReprice {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            reprice_id: akzio_domain::PaperRepriceId::new(),
            commitment: reference(ArtifactKind::ExecutionCommitment),
            prior_receipt: reference(ArtifactKind::OrderReceipt),
            asset: Asset::Qqq,
            prior_client_order_id: "order-r0".into(),
            replacement_client_order_id: "order-r1".into(),
            prior_broker_order_id: "original".into(),
            replacement_limit_price: MoneyMicros(10_000_000),
            created_at: now,
        };
        let mut receipt = OrderReceipt {
            plan_hash: commitment.plan_hash.clone(),
            asset: Asset::Qqq,
            client_order_id: "order-r0".into(),
            broker_order_id: "original".into(),
            state: OrderReceiptState::Canceled,
            requested_quantity_micros: 1_000_000,
            filled_quantity_micros: 0,
            remaining_quantity_micros: 1_000_000,
            average_fill_price: None,
            broker_updated_at: now,
            reason: None,
            observed_at: now,
        };
        receipt.validate().unwrap();
        reprice.validate().unwrap();
        assert_eq!(
            reconciliation_state_with_reprices(&commitment, &[receipt.clone()], &[]),
            ReconciliationState::Complete
        );
        assert_eq!(
            reconciliation_state_with_reprices(
                &commitment,
                &[receipt.clone()],
                std::slice::from_ref(&reprice),
            ),
            ReconciliationState::Pending
        );
        receipt.client_order_id = "order-r1".into();
        receipt.broker_order_id = "successor".into();
        assert_eq!(
            reconciliation_state_with_reprices(&commitment, &[receipt], &[reprice]),
            ReconciliationState::Complete
        );
    }
}
