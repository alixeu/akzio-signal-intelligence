use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
    ArtifactRef, Asset, ContentHash, ExecutionContext, ExecutionVerdict, FactorExposure,
    FailureDisposition, PaperCommitment, Reconciliation, ReconciliationState, RetryPolicy, RunId,
    RunPurpose, TaskBudget, TaskId, TaskRecipeId, TaskWritePermit, WorkflowGraph, WorkflowNode,
    REBUILD_SCHEMA_VERSION,
};
use akzio_execution::{
    paper::{CommittedPaperBroker, PaperExecution, PaperOrderReceipt, Result as PaperResult},
    ExecutionPlan, ExecutionPolicy, MoneyMicros, OrderIntent, OrderSide, PaperCommitmentInput,
    PaperDispatchError, PaperDispatchInput, PaperRepriceDispatchInput, Quote, RepriceError,
    RepriceInput, V2PaperCommitmentRuntime, V2PaperDispatchRuntime, V2RepriceRuntime,
};
use akzio_store::v2::{SessionReservation, StoreError, StoredRun, V2Store, WorkflowCommit};
use chrono::{Duration, Utc};
use tempfile::tempdir;

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
        max_attempts: 1,
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
    WorkflowGraph {
        schema_version: REBUILD_SCHEMA_VERSION,
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
                objective: "submit durable paper commitment".to_owned(),
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
                objective: "record one durable Paper reprice".to_owned(),
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
                objective: "submit one durable Paper reprice".to_owned(),
                dependencies: vec![reprice_task_id],
                input_artifacts: vec![],
                priority: 70,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            },
            WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("execution.reprice.duplicate").unwrap(),
                contract_hash: None,
                objective: "reject a second Paper reprice lineage".to_owned(),
                dependencies: vec![reprice_dispatch_task_id],
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

fn provenance(now: chrono::DateTime<Utc>) -> ArtifactProvenance {
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

#[derive(Default)]
struct FakeCommittedBroker {
    calls: AtomicUsize,
    reprice_calls: AtomicUsize,
}

impl CommittedPaperBroker for FakeCommittedBroker {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(PaperExecution {
                plan_hash: plan.plan_hash.clone(),
                orders: plan
                    .orders
                    .iter()
                    .map(|order| PaperOrderReceipt {
                        client_order_id: commitment.client_order_ids[&order.asset].clone(),
                        broker_order_id: format!("fixture-{}", order.asset.symbol()),
                        symbol: order.asset.symbol().to_owned(),
                        status: "accepted".to_owned(),
                        reused: false,
                        reprice_count: 0,
                    })
                    .collect(),
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
            Ok(PaperOrderReceipt {
                client_order_id: reprice.replacement_client_order_id.clone(),
                broker_order_id: format!("fixture-reprice-{}", replacement.asset.symbol()),
                symbol: replacement.asset.symbol().to_owned(),
                status: "partially_filled".to_owned(),
                reused: false,
                reprice_count: 1,
            })
        })
    }
}

#[tokio::test]
async fn durable_commitment_dispatches_once_and_atomically_reconciles() {
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
    let reservation = store
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
                reserved_at: now,
            },
        )
        .unwrap();
    store.commit_workflow(&reservation.slot.workflow).unwrap();
    let commitment_permit = store
        .claim_next_task("commitment-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;

    let plan = ExecutionPlan {
        policy: ExecutionPolicy::default(),
        targets: vec![],
        orders: vec![OrderIntent {
            asset: Asset::Qqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(2_500),
        }],
        plan_hash: ContentHash::of_bytes(b"v2-paper-dispatch-plan"),
    };
    let plan_artifact = Artifact::new(
        ArtifactKind::ExecutionPlan,
        store.put_json(&plan).unwrap(),
        "fixture.allocation",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&commitment_permit)),
        vec![],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &commitment_permit,
            &plan_artifact,
            "execution.allocation_created",
            now,
        )
        .unwrap();
    let plan_ref = artifact_ref(&plan_artifact);
    let context_payload = ExecutionContext {
        schema_version: REBUILD_SCHEMA_VERSION,
        run_id: commitment_permit.run_id.clone(),
        decision_context: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"decision-context")),
            kind: ArtifactKind::DecisionContext,
        },
        account_snapshot: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"account")),
            kind: ArtifactKind::NormalizedEvidence,
        },
        quote_snapshot: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"quote")),
            kind: ArtifactKind::NormalizedEvidence,
        },
        factor_exposure: FactorExposure {
            leveraged_equity_ppm: 0,
            nasdaq_ppm: 0,
            semiconductor_ppm: 0,
            tqqq_qqq_pair_ppm: 0,
            soxl_soxx_pair_ppm: 0,
        },
        turnover_ppm: 0,
        plan_hash: plan.plan_hash.clone(),
        broker_session: "paper:fixture".to_owned(),
        frozen: false,
        created_at: now,
    };
    let context_artifact = Artifact::new(
        ArtifactKind::ExecutionContext,
        store.put_json(&context_payload).unwrap(),
        "fixture.execution_context",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&commitment_permit)),
        vec![plan_ref],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &commitment_permit,
            &context_artifact,
            "execution.context_created",
            now,
        )
        .unwrap();
    let context_ref = artifact_ref(&context_artifact);
    let verdict_artifact = Artifact::new(
        ArtifactKind::ExecutionVerdict,
        store
            .put_json(&ExecutionVerdict::Accepted {
                execution_context: context_ref.clone(),
            })
            .unwrap(),
        "fixture.execution_verdict",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&commitment_permit)),
        vec![context_ref],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &commitment_permit,
            &verdict_artifact,
            "execution.verdict_created",
            now,
        )
        .unwrap();
    let commitment = V2PaperCommitmentRuntime::new(store.clone())
        .commit(&PaperCommitmentInput {
            lease: lease.clone(),
            permit: commitment_permit,
            verdict: artifact_ref(&verdict_artifact),
            session_key: "paper:fixture".to_owned(),
            now,
        })
        .unwrap()
        .commitment;

    let dispatch_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let broker = FakeCommittedBroker::default();
    let runtime = V2PaperDispatchRuntime::new(store.clone());
    let output = runtime
        .dispatch(
            &broker,
            &PaperDispatchInput {
                permit: dispatch_permit.clone(),
                commitment: artifact_ref(&commitment),
                now,
            },
        )
        .await
        .unwrap();

    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    assert_eq!(output.reconciliation.receipts.len(), 1);
    let reconciliation: Reconciliation = serde_json::from_slice(
        &store
            .read_blob(&output.reconciliation.reconciliation.blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reconciliation.state, ReconciliationState::Complete);
    assert!(store
        .events_after(&dispatch_permit.run_id, 0, 50)
        .unwrap()
        .iter()
        .any(|event| {
            event.event_type == "task.succeeded"
                && event.task_id.as_ref() == Some(&dispatch_permit.task_id)
                && event.attempt_id.as_ref() == Some(&dispatch_permit.attempt_id)
                && event.artifact_id.as_ref()
                    == Some(&output.reconciliation.reconciliation.artifact_id)
        }));

    let reprice_permit = store
        .claim_next_task("reprice-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let reprice_runtime = V2RepriceRuntime::new(store.clone(), ExecutionPolicy::default());
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
                permit: reprice_dispatch_permit.clone(),
                reprice: artifact_ref(&reprice.reprice),
                now,
            },
        )
        .await
        .unwrap();
    assert_eq!(broker.reprice_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reprice_dispatch.reconciliation.receipts.len(), 1);
    assert!(reprice_dispatch.reconciliation.receipts[0]
        .source_refs
        .iter()
        .any(|source| source == &artifact_ref(&reprice.reprice)));

    let duplicate_reprice_permit = store
        .claim_next_task("duplicate-reprice-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    assert!(matches!(
        reprice_runtime.prepare(&RepriceInput {
            lease,
            permit: duplicate_reprice_permit,
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

    assert!(matches!(
        runtime
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    permit: dispatch_permit,
                    commitment: artifact_ref(&commitment),
                    now,
                },
            )
            .await,
        Err(PaperDispatchError::Store(StoreError::StalePermit(_)))
    ));
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.reprice_calls.load(Ordering::SeqCst), 1);
    store.verify_integrity().unwrap();
}
