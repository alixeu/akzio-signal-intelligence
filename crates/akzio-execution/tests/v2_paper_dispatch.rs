use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
    ArtifactRef, Asset, ContentHash, ExecutionContext, ExecutionVerdict, FactorExposure,
    FailureDisposition, LifecycleEventType, PaperCommitment, PaperCommitmentId, Reconciliation,
    ReconciliationState, RetryPolicy, RunId, RunPurpose, TargetPortfolio, TaskBudget, TaskId,
    TaskRecipeId, TaskWritePermit, WeightPpm, WorkflowGraph, WorkflowNode, V2_SCHEMA_VERSION,
};
use akzio_execution::{
    paper::{
        client_order_id, CommittedPaperBroker, PaperError, PaperExecution, PaperOrderReceipt,
        Result as PaperResult,
    },
    ExecutionPlan, ExecutionPolicy, MoneyMicros, OrderIntent, OrderSide, PaperCommitmentInput,
    PaperDispatchError, PaperDispatchInput, PaperRepriceDispatchInput, Quote, RepriceError,
    RepriceInput, V2PaperCommitmentRuntime, V2PaperDispatchRuntime, V2RepriceRuntime,
};
use akzio_store::v2::{
    DaemonLease, SessionReservation, StoreError, StoredRun, V2Store, WorkflowCommit,
};
use chrono::{DateTime, Duration, Utc};
use tempfile::{tempdir, TempDir};

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
        max_attempts: 2,
        initial_backoff_ms: 1,
        retry_transport: false,
        retry_rate_limited: false,
        retry_invalid_output: false,
    }
}

fn workflow() -> WorkflowGraph {
    let commitment_task_id = TaskId::new();
    let dispatch_task_id = TaskId::new();
    let reprice_task_id = TaskId::new();
    let reprice_dispatch_task_id = TaskId::new();
    let duplicate_dispatch_task_id = TaskId::new();
    WorkflowGraph {
        schema_version: V2_SCHEMA_VERSION,
        topology_id: "paper-dispatch-fixture".to_owned(),
        nodes: vec![
            WorkflowNode {
                task_id: commitment_task_id.clone(),
                recipe_id: TaskRecipeId::new("execution.commitment").unwrap(),
                contract_hash: None,
                objective: "commit paper execution".to_owned(),
                dependencies: vec![],
                input_artifacts: vec![],
                priority: 100,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: dispatch_task_id.clone(),
                recipe_id: TaskRecipeId::new("execution.dispatch").unwrap(),
                contract_hash: None,
                objective: "dispatch paper execution".to_owned(),
                dependencies: vec![commitment_task_id],
                input_artifacts: vec![],
                priority: 90,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: reprice_task_id.clone(),
                recipe_id: TaskRecipeId::new("execution.reprice.prepare").unwrap(),
                contract_hash: None,
                objective: "prepare one paper reprice".to_owned(),
                dependencies: vec![dispatch_task_id],
                input_artifacts: vec![],
                priority: 80,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: reprice_dispatch_task_id.clone(),
                recipe_id: TaskRecipeId::new("execution.reprice.dispatch").unwrap(),
                contract_hash: None,
                objective: "dispatch one paper reprice".to_owned(),
                dependencies: vec![reprice_task_id],
                input_artifacts: vec![],
                priority: 70,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: duplicate_dispatch_task_id.clone(),
                recipe_id: TaskRecipeId::new("execution.dispatch.duplicate").unwrap(),
                contract_hash: None,
                objective: "reject duplicate paper dispatch".to_owned(),
                dependencies: vec![reprice_dispatch_task_id.clone()],
                input_artifacts: vec![],
                priority: 65,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("execution.reprice.duplicate").unwrap(),
                contract_hash: None,
                objective: "reject duplicate paper reprice".to_owned(),
                dependencies: vec![reprice_dispatch_task_id.clone()],
                input_artifacts: vec![],
                priority: 60,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
        ],
    }
}

fn policy() -> ExecutionPolicy {
    ExecutionPolicy {
        assets: Asset::EXECUTABLE.iter().copied().collect(),
        max_gross_weight: WeightPpm(1_000_000),
        max_new_notional: MoneyMicros::from_usd_cents(1_000_000),
        max_daily_turnover: WeightPpm(1_000_000),
        max_account_age_secs: 60,
        max_quote_age_secs: 60,
        max_clock_age_secs: 60,
        max_spread_bps: 500,
        limit_protection_bps: 100,
    }
}

fn plan(
    now: DateTime<Utc>,
    decision_context: ArtifactRef,
    account_snapshot: ArtifactRef,
    quote_snapshot: ArtifactRef,
    market_clock_snapshot: ArtifactRef,
) -> ExecutionPlan {
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(100_000));
    let mut plan = ExecutionPlan {
        schema_version: V2_SCHEMA_VERSION,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
        policy_hash: policy().policy_hash().unwrap(),
        target: target.clone(),
        orders: vec![OrderIntent {
            asset: Asset::Qqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(2_500),
        }],
        gross_exposure_ppm: 100_000,
        net_exposure_ppm: 100_000,
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        turnover_ppm: 100_000,
        broker_session: "paper:fixture".to_owned(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan.refresh_hash().unwrap();
    plan
}

fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: "fixture.paper".to_owned(),
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

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn artifact_ref_for(kind: ArtifactKind, name: &[u8]) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(name)),
        kind,
    }
}

fn write_source(
    store: &V2Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    label: &str,
    now: DateTime<Utc>,
) -> ArtifactRef {
    let artifact = Artifact::new(
        kind,
        store
            .put_json(&serde_json::json!({ "fixture": label }))
            .unwrap(),
        "fixture.execution_source",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(permit)),
        vec![],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::FixtureExecutionSourceCreated,
            now,
        )
        .unwrap();
    artifact_ref(&artifact)
}

#[derive(Default)]
struct FakeBrokerState {
    statuses: VecDeque<String>,
    orders: BTreeMap<String, PaperOrderReceipt>,
}

struct FakeCommittedBroker {
    state: Mutex<FakeBrokerState>,
    execute_calls: AtomicUsize,
    actual_submit_calls: AtomicUsize,
    lookup_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
    reprice_calls: AtomicUsize,
    fail_reconcile_once: AtomicBool,
}

impl FakeCommittedBroker {
    fn new(statuses: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            state: Mutex::new(FakeBrokerState {
                statuses: statuses.into_iter().map(ToOwned::to_owned).collect(),
                orders: BTreeMap::new(),
            }),
            execute_calls: AtomicUsize::new(0),
            actual_submit_calls: AtomicUsize::new(0),
            lookup_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            reprice_calls: AtomicUsize::new(0),
            fail_reconcile_once: AtomicBool::new(false),
        }
    }

    fn fail_next_reconcile(&self) {
        self.fail_reconcile_once.store(true, Ordering::SeqCst);
    }

    fn receipt(
        client_order_id: String,
        broker_order_id: String,
        symbol: String,
        status: &str,
        reprice_count: u8,
        now: DateTime<Utc>,
    ) -> PaperOrderReceipt {
        let requested_quantity_micros = 4_000_000;
        let filled_quantity_micros = match status {
            "partially_filled" => 2_000_000,
            "filled" => requested_quantity_micros,
            _ => 0,
        };
        PaperOrderReceipt {
            client_order_id,
            broker_order_id,
            symbol,
            status: status.to_owned(),
            requested_quantity_micros,
            filled_quantity_micros,
            remaining_quantity_micros: requested_quantity_micros - filled_quantity_micros,
            average_fill_price: (filled_quantity_micros > 0)
                .then_some(MoneyMicros::from_usd_cents(2_500)),
            broker_updated_at: now,
            reason: match status {
                "canceled" => Some("fixture cancellation".to_owned()),
                "rejected" => Some("fixture rejection".to_owned()),
                _ => None,
            },
            reused: false,
            reprice_count,
        }
    }

    fn set_status(receipt: &mut PaperOrderReceipt, status: &str, now: DateTime<Utc>) {
        let next = Self::receipt(
            receipt.client_order_id.clone(),
            receipt.broker_order_id.clone(),
            receipt.symbol.clone(),
            status,
            receipt.reprice_count,
            now,
        );
        *receipt = next;
    }
}

impl CommittedPaperBroker for FakeCommittedBroker {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            let now = Utc::now();
            let mut state = self.state.lock().unwrap();
            let orders = plan
                .orders
                .iter()
                .map(|order| {
                    let client_order_id = commitment.client_order_ids[&order.asset].clone();
                    if let Some(existing) = state.orders.get(&client_order_id) {
                        self.lookup_calls.fetch_add(1, Ordering::SeqCst);
                        return PaperOrderReceipt {
                            reused: true,
                            ..existing.clone()
                        };
                    }
                    self.actual_submit_calls.fetch_add(1, Ordering::SeqCst);
                    let receipt = Self::receipt(
                        client_order_id.clone(),
                        format!("fixture-{}", order.asset.symbol()),
                        order.asset.symbol().to_owned(),
                        "accepted",
                        0,
                        now,
                    );
                    state.orders.insert(client_order_id, receipt.clone());
                    receipt
                })
                .collect();
            Ok(PaperExecution {
                plan_hash: plan.plan_hash.clone(),
                orders,
            })
        })
    }

    fn replace_commitment_once<'a>(
        &'a self,
        _commitment: &'a PaperCommitment,
        reprice: &'a akzio_domain::PaperReprice,
        replacement: &'a OrderIntent,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperOrderReceipt>> + Send + 'a>> {
        Box::pin(async move {
            self.reprice_calls.fetch_add(1, Ordering::SeqCst);
            let receipt = Self::receipt(
                reprice.replacement_client_order_id.clone(),
                format!("fixture-reprice-{}", replacement.asset.symbol()),
                replacement.asset.symbol().to_owned(),
                "accepted",
                1,
                Utc::now(),
            );
            self.state
                .lock()
                .unwrap()
                .orders
                .insert(receipt.client_order_id.clone(), receipt.clone());
            Ok(receipt)
        })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_reconcile_once.swap(false, Ordering::SeqCst) {
                return Err(PaperError::InvalidCommitment(
                    "fixture crash after submit".to_owned(),
                ));
            }
            if execution.plan_hash != commitment.plan_hash {
                return Err(PaperError::CommitmentPlanHashMismatch);
            }
            let now = Utc::now();
            let mut state = self.state.lock().unwrap();
            let status = state
                .statuses
                .pop_front()
                .unwrap_or_else(|| "filled".to_owned());
            let orders = execution
                .orders
                .iter()
                .map(|submitted| {
                    let mut receipt = state
                        .orders
                        .get(&submitted.client_order_id)
                        .cloned()
                        .ok_or_else(|| PaperError::InvalidCommitment("missing order".to_owned()))?;
                    receipt.reused = submitted.reused;
                    Self::set_status(&mut receipt, &status, now);
                    receipt.reused = submitted.reused;
                    state
                        .orders
                        .insert(receipt.client_order_id.clone(), receipt.clone());
                    Ok(receipt)
                })
                .collect::<PaperResult<Vec<_>>>()?;
            Ok(PaperExecution {
                plan_hash: execution.plan_hash.clone(),
                orders,
            })
        })
    }
}

struct PreparedCommitment {
    _directory: TempDir,
    store: V2Store,
    now: DateTime<Utc>,
    lease: DaemonLease,
    commitment: Artifact,
}

fn prepared_commitment() -> PreparedCommitment {
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
        .unwrap();
    let graph = workflow();
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
    store
        .reserve_session_slot(
            &lease,
            &SessionReservation {
                session_key: "paper:fixture".to_owned(),
                workflow: WorkflowCommit {
                    run: StoredRun {
                        run_id: RunId::new(),
                        purpose: RunPurpose::Paper,
                        topology_id: graph.topology_id.clone(),
                        graph_artifact_id: graph_artifact.artifact_id.clone(),
                        created_at: now,
                    },
                    graph: graph_artifact,
                    nodes: graph.nodes,
                },
                setup_artifacts: vec![],
                reserved_at: now,
            },
        )
        .unwrap();
    let permit = store
        .claim_next_task("commitment-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let decision_context = write_source(
        &store,
        &permit,
        ArtifactKind::DecisionContext,
        "decision-context",
        now,
    );
    let account_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "account",
        now,
    );
    let quote_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "quote",
        now,
    );
    let market_clock_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "clock",
        now,
    );
    let allocation = plan(
        now,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
    );
    let allocation_artifact = Artifact::new(
        ArtifactKind::ExecutionPlan,
        store.put_json(&allocation).unwrap(),
        "fixture.allocation",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![
            allocation.decision_context.clone(),
            allocation.account_snapshot.clone(),
            allocation.quote_snapshot.clone(),
            allocation.market_clock_snapshot.clone(),
        ],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &allocation_artifact,
            LifecycleEventType::ExecutionAllocationCreated,
            now,
        )
        .unwrap();
    let allocation_ref = artifact_ref(&allocation_artifact);
    let context = ExecutionContext {
        schema_version: V2_SCHEMA_VERSION,
        run_id: permit.run_id.clone(),
        decision_context: allocation.decision_context.clone(),
        account_snapshot: Some(allocation.account_snapshot.clone()),
        quote_snapshot: Some(allocation.quote_snapshot.clone()),
        market_clock_snapshot: Some(allocation.market_clock_snapshot.clone()),
        execution_plan: Some(allocation_ref.clone()),
        factor_exposure: Some(allocation.factor_exposure.clone()),
        turnover_ppm: Some(allocation.turnover_ppm),
        plan_hash: Some(allocation.plan_hash.clone()),
        broker_session: Some(allocation.broker_session.clone()),
        frozen: false,
        created_at: now,
    };
    context.validate_complete_plan_closure().unwrap();
    let context_artifact = Artifact::new(
        ArtifactKind::ExecutionContext,
        store.put_json(&context).unwrap(),
        "fixture.execution_context",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![
            context.decision_context.clone(),
            context.account_snapshot.clone().unwrap(),
            context.quote_snapshot.clone().unwrap(),
            context.market_clock_snapshot.clone().unwrap(),
            allocation_ref,
        ],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &context_artifact,
            LifecycleEventType::ExecutionContextCreatedLegacy,
            now,
        )
        .unwrap();
    let context_ref = artifact_ref(&context_artifact);
    let verdict = Artifact::new(
        ArtifactKind::ExecutionVerdict,
        store
            .put_json(&ExecutionVerdict::Accepted {
                execution_context: context_ref.clone(),
            })
            .unwrap(),
        "fixture.execution_verdict",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![context_ref],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreatedLegacy,
            now,
        )
        .unwrap();
    let commitment = V2PaperCommitmentRuntime::new(store.clone())
        .commit(&PaperCommitmentInput {
            lease: lease.clone(),
            permit,
            verdict: artifact_ref(&verdict),
            session_key: "paper:fixture".to_owned(),
            now,
        })
        .unwrap()
        .commitment;
    PreparedCommitment {
        _directory: directory,
        store,
        now,
        lease,
        commitment,
    }
}

#[tokio::test]
async fn partial_then_filled_reprice_uses_one_durable_lineage() {
    let PreparedCommitment {
        _directory,
        store,
        now,
        lease,
        commitment,
    } = prepared_commitment();
    let dispatch_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let run_id = dispatch_permit.run_id.clone();
    let broker = FakeCommittedBroker::new(["partially_filled", "filled"]);
    let runtime = V2PaperDispatchRuntime::new(store.clone());
    let output = runtime
        .dispatch(
            &broker,
            &PaperDispatchInput {
                lease: lease.clone(),
                permit: dispatch_permit.clone(),
                commitment: artifact_ref(&commitment),
                now,
            },
        )
        .await
        .unwrap();
    let reconciliation: Reconciliation = serde_json::from_slice(
        &store
            .read_blob(&output.reconciliation.reconciliation.blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reconciliation.state, ReconciliationState::Partial);
    let receipt = store
        .artifact(&output.reconciliation.receipts[0].artifact_id)
        .unwrap();
    let receipt: akzio_domain::OrderReceipt =
        serde_json::from_slice(&store.read_blob(&receipt.blob).unwrap()).unwrap();
    assert_eq!(
        receipt.state,
        akzio_domain::OrderReceiptState::PartiallyFilled
    );
    assert!(store
        .events_after(&run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
        }));

    let reprice_permit = store
        .claim_next_task("reprice-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let reprice_runtime = V2RepriceRuntime::new(store.clone(), policy());
    let reprice = reprice_runtime
        .prepare(&RepriceInput {
            lease: lease.clone(),
            permit: reprice_permit,
            commitment: artifact_ref(&commitment),
            prior_receipt: artifact_ref(&output.reconciliation.receipts[0]),
            quote: Quote {
                bid: MoneyMicros::from_usd_cents(2_600),
                ask: MoneyMicros::from_usd_cents(2_601),
                observed_at: now,
            },
            now,
        })
        .unwrap();
    let reprice_dispatch_permit = store
        .claim_next_task("reprice-dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let reprice_dispatch = runtime
        .dispatch_reprice(
            &broker,
            &PaperRepriceDispatchInput {
                lease: lease.clone(),
                permit: reprice_dispatch_permit,
                reprice: artifact_ref(&reprice.reprice),
                now,
            },
        )
        .await
        .unwrap();
    let reconciliation: Reconciliation = serde_json::from_slice(
        &store
            .read_blob(&reprice_dispatch.reconciliation.reconciliation.blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reconciliation.state, ReconciliationState::Complete);
    assert_eq!(broker.actual_submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.reprice_calls.load(Ordering::SeqCst), 1);
    assert!(store
        .events_after(&run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&reprice.reprice.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
        }));

    let duplicate_dispatch_permit = store
        .claim_next_task("duplicate-dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let execute_calls = broker.execute_calls.load(Ordering::SeqCst);
    let reconcile_calls = broker.reconcile_calls.load(Ordering::SeqCst);
    assert!(matches!(
        runtime
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    lease: lease.clone(),
                    permit: duplicate_dispatch_permit,
                    commitment: artifact_ref(&commitment),
                    now,
                },
            )
            .await,
        Err(PaperDispatchError::Store(
            StoreError::PaperEffectAlreadySettled(_)
        ))
    ));
    assert_eq!(broker.execute_calls.load(Ordering::SeqCst), execute_calls);
    assert_eq!(
        broker.reconcile_calls.load(Ordering::SeqCst),
        reconcile_calls
    );
    assert_eq!(
        store
            .events_after(&run_id, 0, 100)
            .unwrap()
            .iter()
            .filter(|event| {
                event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                    && matches!(
                        event.event_type.as_str(),
                        "execution.effect.settled" | "execution.effect.recovered"
                    )
            })
            .count(),
        1
    );

    let duplicate_permit = store
        .claim_next_task("duplicate-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    assert!(matches!(
        reprice_runtime.prepare(&RepriceInput {
            lease: lease.clone(),
            permit: duplicate_permit,
            commitment: artifact_ref(&commitment),
            prior_receipt: artifact_ref(&output.reconciliation.receipts[0]),
            quote: Quote {
                bid: MoneyMicros::from_usd_cents(2_700),
                ask: MoneyMicros::from_usd_cents(2_701),
                observed_at: now,
            },
            now,
        }),
        Err(RepriceError::Store(StoreError::DuplicateExecutionReprice(
            _
        )))
    ));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn stale_scheduler_epoch_never_calls_broker() {
    let PreparedCommitment {
        _directory,
        store,
        now,
        lease,
        commitment,
    } = prepared_commitment();
    let dispatch_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let run_id = dispatch_permit.run_id.clone();
    let takeover_at = now + Duration::seconds(31);
    store
        .acquire_daemon_lease(
            "scheduler",
            "successor-daemon",
            takeover_at,
            takeover_at + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let broker = FakeCommittedBroker::new(["filled"]);
    assert!(matches!(
        V2PaperDispatchRuntime::new(store.clone())
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    lease,
                    permit: dispatch_permit,
                    commitment: artifact_ref(&commitment),
                    now: takeover_at,
                },
            )
            .await,
        Err(PaperDispatchError::Store(StoreError::SchedulerFenced(_)))
    ));
    assert_eq!(broker.execute_calls.load(Ordering::SeqCst), 0);
    assert_eq!(broker.reconcile_calls.load(Ordering::SeqCst), 0);
    assert_eq!(broker.reprice_calls.load(Ordering::SeqCst), 0);
    assert!(!store
        .events_after(&run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
        }));
}

#[tokio::test]
async fn crash_after_submit_reuses_durable_client_order_id() {
    let PreparedCommitment {
        _directory,
        store,
        now,
        lease,
        commitment,
    } = prepared_commitment();
    let first_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let run_id = first_permit.run_id.clone();
    let broker = FakeCommittedBroker::new(["filled"]);
    broker.fail_next_reconcile();
    let runtime = V2PaperDispatchRuntime::new(store.clone());
    assert!(matches!(
        runtime
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    lease,
                    permit: first_permit,
                    commitment: artifact_ref(&commitment),
                    now,
                },
            )
            .await,
        Err(PaperDispatchError::Broker(PaperError::InvalidCommitment(_)))
    ));
    let retry_at = now + Duration::seconds(31);
    assert_eq!(store.recover_expired_tasks(retry_at).unwrap(), 1);
    let retry_lease = store
        .acquire_daemon_lease(
            "scheduler",
            "recovered-daemon",
            retry_at,
            retry_at + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let retry_permit = store
        .claim_next_task("recovered-worker", retry_at, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let output = runtime
        .dispatch(
            &broker,
            &PaperDispatchInput {
                lease: retry_lease,
                permit: retry_permit,
                commitment: artifact_ref(&commitment),
                now: retry_at,
            },
        )
        .await
        .unwrap();
    let payload: PaperCommitment =
        serde_json::from_slice(&store.read_blob(&commitment.blob).unwrap()).unwrap();
    assert_eq!(
        output.execution.orders[0].client_order_id,
        payload.client_order_ids[&Asset::Qqq]
    );
    assert!(output.execution.orders[0].reused);
    assert_eq!(broker.actual_submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.lookup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.execute_calls.load(Ordering::SeqCst), 2);
    assert_eq!(broker.reconcile_calls.load(Ordering::SeqCst), 2);
    let events = store.events_after(&run_id, 0, 100).unwrap();
    let intent = events
        .iter()
        .find(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
        })
        .expect("Paper effect intent is durable before broker I/O");
    let recovered = events
        .iter()
        .find(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectRecovered.as_str()
        })
        .expect("retry settles the existing Paper effect as recovered");
    assert!(intent.cursor < recovered.cursor);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                    && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn fake_broker_reports_lifecycle_and_terminal_statuses() {
    let now = Utc::now();
    let plan = plan(
        now,
        artifact_ref_for(ArtifactKind::DecisionContext, b"decision-context"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"account"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"quote"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"clock"),
    );
    let commitment = PaperCommitment {
        commitment_id: PaperCommitmentId::new(),
        execution_context: artifact_ref_for(ArtifactKind::ExecutionContext, b"context"),
        plan_hash: plan.plan_hash.clone(),
        broker_session: plan.broker_session.clone(),
        client_order_ids: BTreeMap::from([(
            Asset::Qqq,
            client_order_id(&plan.broker_session, &plan.plan_hash, 0, 0),
        )]),
        created_at: now,
    };
    let broker = FakeCommittedBroker::new([
        "accepted",
        "partially_filled",
        "filled",
        "canceled",
        "rejected",
    ]);
    let submitted = broker.execute_commitment(&commitment, &plan).await.unwrap();
    for (status, filled, remaining) in [
        ("accepted", 0, 4_000_000),
        ("partially_filled", 2_000_000, 2_000_000),
        ("filled", 4_000_000, 0),
        ("canceled", 0, 4_000_000),
        ("rejected", 0, 4_000_000),
    ] {
        let observation = broker
            .reconcile_commitment(&commitment, &submitted)
            .await
            .unwrap();
        let receipt = &observation.orders[0];
        assert_eq!(receipt.status, status);
        assert_eq!(receipt.filled_quantity_micros, filled);
        assert_eq!(receipt.remaining_quantity_micros, remaining);
        assert_eq!(
            receipt.requested_quantity_micros,
            receipt.filled_quantity_micros + receipt.remaining_quantity_micros
        );
        assert_eq!(
            receipt.reason.is_some(),
            matches!(status, "canceled" | "rejected")
        );
    }
}
