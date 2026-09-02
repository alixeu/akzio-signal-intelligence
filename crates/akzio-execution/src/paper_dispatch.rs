use super::*;

#[derive(Debug, Clone)]
pub struct PaperDispatchInput {
    pub lease: DaemonLease,
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
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
    #[error("commitment does not retain its execution context")]
    CommitmentContextMissing,
    #[error("commitment execution context does not match dispatch run or plan")]
    ContextMismatch,
    #[error("execution context has no persisted allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match commitment")]
    PlanHashMismatch,
    #[error("broker response plan hash does not match commitment")]
    BrokerPlanHashMismatch,
    #[error("broker returned unsupported order status {0}")]
    UnsupportedReceiptStatus(String),
}

pub type PaperDispatchResult<T> = std::result::Result<T, PaperDispatchError>;

/// Explicit, opt-in process crash used to verify recovery after the durable
/// broker effect intent commits and before the first broker request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PaperDispatchFailpoint {
    #[default]
    Disabled,
    ExitAfterEffectIntent,
}

impl PaperDispatchFailpoint {
    pub fn from_env() -> Self {
        Self::from_value(
            std::env::var("AKZIO_DIAGNOSTIC_CRASH_AFTER_EFFECT_INTENT")
                .ok()
                .as_deref(),
        )
    }

    fn from_value(value: Option<&str>) -> Self {
        match value {
            Some("1") => Self::ExitAfterEffectIntent,
            _ => Self::Disabled,
        }
    }

    fn trigger_after_effect_intent(self) {
        if matches!(self, Self::ExitAfterEffectIntent) {
            eprintln!("[akzio-diagnostic] exiting after durable execution.effect.intent (code 86)");
            std::process::exit(86);
        }
    }
}

#[cfg(test)]
mod failpoint_tests {
    use super::PaperDispatchFailpoint;

    #[test]
    fn failpoint_is_disabled_unless_explicitly_enabled() {
        assert_eq!(
            PaperDispatchFailpoint::from_value(None),
            PaperDispatchFailpoint::Disabled
        );
        assert_eq!(
            PaperDispatchFailpoint::from_value(Some("0")),
            PaperDispatchFailpoint::Disabled
        );
        assert_eq!(
            PaperDispatchFailpoint::from_value(Some("1")),
            PaperDispatchFailpoint::ExitAfterEffectIntent
        );
    }
}

/// Dispatches only a persisted commitment, then atomically persists the
/// resulting receipt/reconciliation artifacts and closes the dispatch task.
/// If the process dies after broker I/O but before the final Store transaction,
/// a retry replays the same deterministic client order IDs.
#[derive(Debug, Clone)]
pub struct V2PaperDispatchRuntime {
    store: V2Store,
    settlement_timeout: std::time::Duration,
    failpoint: PaperDispatchFailpoint,
}

struct CommittedPlanContext {
    commitment_artifact: Artifact,
    commitment: PaperCommitment,
    plan: ExecutionPlan,
}

impl V2PaperDispatchRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            settlement_timeout: std::time::Duration::from_secs(15),
            failpoint: PaperDispatchFailpoint::Disabled,
        }
    }

    pub fn with_settlement_timeout(mut self, settlement_timeout: std::time::Duration) -> Self {
        self.settlement_timeout = settlement_timeout;
        self
    }

    pub fn with_failpoint(mut self, failpoint: PaperDispatchFailpoint) -> Self {
        self.failpoint = failpoint;
        self
    }

    pub async fn dispatch<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
    ) -> PaperDispatchResult<PaperDispatchOutput> {
        self.require_paper_run(&input.permit)?;
        let CommittedPlanContext {
            commitment_artifact,
            commitment,
            plan,
        } = self.load_committed_plan(&input.permit, &input.commitment)?;

        self.ensure_unfrozen()?;
        self.store.validate_daemon_lease(&input.lease, Utc::now())?;
        self.store.validate_task_permit(&input.permit)?;
        let recovered = self.store.record_paper_effect_intent(
            &input.lease,
            &input.permit,
            &input.commitment,
            input.now,
        )?;
        self.failpoint.trigger_after_effect_intent();
        let submitted = broker.execute_commitment(&commitment, &plan).await?;
        if submitted.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        self.store.validate_daemon_lease(&input.lease, Utc::now())?;
        let execution = reconcile_until_settled(
            &self.store,
            &input.lease,
            broker,
            &commitment,
            &submitted,
            self.settlement_timeout,
        )
        .await?;
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        let broker_receipts = execution
            .orders
            .iter()
            .map(|receipt| broker_receipt(receipt, &commitment, input.now))
            .collect::<PaperDispatchResult<Vec<_>>>()?;
        let reconciliation_runtime = V2ReconciliationRuntime::new(self.store.clone());
        let reconciliation = reconciliation_runtime.reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: input.commitment.clone(),
            reprice: None,
            broker_receipts,
            now: input.now,
        })?;
        reconciliation_runtime.commit_with_effect(
            &input.lease,
            &input.permit,
            &reconciliation,
            &input.commitment,
            recovered,
            Utc::now(),
        )?;

        Ok(PaperDispatchOutput {
            commitment: commitment_artifact,
            execution,
            reconciliation,
        })
    }

    fn require_paper_run(&self, permit: &TaskWritePermit) -> PaperDispatchResult<()> {
        let purpose = self.store.run_purpose(&permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperDispatchError::NonPaperRun(purpose));
        }
        Ok(())
    }

    fn load_committed_plan(
        &self,
        permit: &TaskWritePermit,
        commitment_reference: &ArtifactRef,
    ) -> PaperDispatchResult<CommittedPlanContext> {
        let commitment_artifact =
            self.load_expected(commitment_reference, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let slot = self
            .store
            .session_slot(&commitment.broker_session)?
            .ok_or(PaperDispatchError::CommitmentNotDurable)?;
        if slot.workflow.run.run_id != permit.run_id
            || slot.commitment_artifact_id.as_ref() != Some(&commitment_reference.artifact_id)
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
        context.validate_complete_plan_closure()?;
        if context.run_id != permit.run_id
            || context.broker_session.as_deref() != Some(commitment.broker_session.as_str())
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(PaperDispatchError::ContextMismatch);
        }
        let plan_reference = context
            .execution_plan
            .ok_or(PaperDispatchError::MissingAllocationPlan)?;
        if !context_artifact.source_refs.contains(&plan_reference) {
            return Err(PaperDispatchError::MissingAllocationPlan);
        }
        let plan_artifact = self.load_expected(&plan_reference, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if plan.plan_hash != commitment.plan_hash
            || plan.broker_session != commitment.broker_session
        {
            return Err(PaperDispatchError::PlanHashMismatch);
        }

        Ok(CommittedPlanContext {
            commitment_artifact,
            commitment,
            plan,
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

async fn reconcile_until_settled<B: CommittedPaperBroker + ?Sized>(
    store: &V2Store,
    lease: &DaemonLease,
    broker: &B,
    commitment: &PaperCommitment,
    submitted: &PaperExecution,
    settlement_timeout: std::time::Duration,
) -> PaperDispatchResult<PaperExecution> {
    let deadline = tokio::time::Instant::now() + settlement_timeout;
    loop {
        let execution = broker.reconcile_commitment(commitment, submitted).await?;
        if execution_is_settled(&execution)? || tokio::time::Instant::now() >= deadline {
            return Ok(execution);
        }
        store.validate_daemon_lease(lease, Utc::now())?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn execution_is_settled(execution: &PaperExecution) -> PaperDispatchResult<bool> {
    execution.orders.iter().try_fold(true, |settled, receipt| {
        Ok(settled
            && matches!(
                receipt_state(&receipt.status)?,
                OrderReceiptState::Filled
                    | OrderReceiptState::Canceled
                    | OrderReceiptState::Rejected
                    | OrderReceiptState::Failed
            ))
    })
}

fn broker_receipt(
    receipt: &PaperOrderReceipt,
    commitment: &PaperCommitment,
    observed_at: DateTime<Utc>,
) -> PaperDispatchResult<OrderReceipt> {
    let asset = Asset::try_from(receipt.symbol.as_str())?;
    Ok(OrderReceipt {
        plan_hash: commitment.plan_hash.clone(),
        asset,
        client_order_id: receipt.client_order_id.clone(),
        broker_order_id: receipt.broker_order_id.clone(),
        state: receipt_state(&receipt.status)?,
        requested_quantity_micros: receipt.requested_quantity_micros,
        filled_quantity_micros: receipt.filled_quantity_micros,
        remaining_quantity_micros: receipt.remaining_quantity_micros,
        average_fill_price: receipt.average_fill_price,
        broker_updated_at: receipt.broker_updated_at,
        reason: receipt.reason.clone(),
        observed_at,
    })
}
#[cfg(test)]
mod settle_tests {
    use super::*;

    fn execution(statuses: &[&str]) -> PaperExecution {
        PaperExecution {
            plan_hash: ContentHash::of_bytes(b"settlement-test"),
            orders: statuses
                .iter()
                .enumerate()
                .map(|(index, status)| PaperOrderReceipt {
                    client_order_id: format!("client-{index}"),
                    broker_order_id: format!("broker-{index}"),
                    symbol: "QQQ".to_owned(),
                    status: (*status).to_owned(),
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: i64::from(*status == "filled") * 1_000_000,
                    remaining_quantity_micros: i64::from(*status != "filled") * 1_000_000,
                    average_fill_price: None,
                    broker_updated_at: Utc::now(),
                    reason: None,
                    reused: false,
                    reprice_count: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn settlement_waits_for_open_or_partial_orders() {
        assert!(!execution_is_settled(&execution(&["accepted", "filled"])).unwrap());
        assert!(!execution_is_settled(&execution(&["partially_filled"])).unwrap());
        assert!(execution_is_settled(&execution(&["filled", "filled"])).unwrap());
        assert!(execution_is_settled(&execution(&["rejected"])).unwrap());
    }
}
