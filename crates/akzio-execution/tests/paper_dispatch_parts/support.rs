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
    FailureDisposition, LifecycleEventType, PaperApprovalScope, PaperCommitment, PaperCommitmentId,
    PaperLaunchApproval, RetryPolicy, RunId, RunPurpose,
    RuntimeManifest, TargetPortfolio, TaskBudget, TaskId, TaskRecipeId, TaskWritePermit, WeightPpm,
    WorkflowGraph, WorkflowNode, WorkflowProposal, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::{
    paper::{
        client_order_id, CommittedPaperBroker, PaperError, PaperExecution, PaperOrderReceipt,
        Result as PaperResult,
    },
    ExecutionPlan, ExecutionPolicy, MoneyMicros, OrderIntent, OrderSide, PaperCommitmentInput,
    PaperDispatchError, PaperDispatchInput,
    V2PaperCommitmentRuntime, V2PaperDispatchRuntime,
};
use akzio_store::v2::{
    DaemonLease, SessionReservation, StoreError, StoredRun, V2Store, WorkflowCommit,
};
use chrono::{DateTime, Duration, Utc};
use tempfile::{tempdir, TempDir};

const SESSION_KEY: &str = "2026-08-25";

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
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
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
        max_future_skew_secs: 1,
        max_snapshot_skew_secs: 2,
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
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
        policy_hash: policy().policy_hash().unwrap(),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
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
        broker_session: SESSION_KEY.to_owned(),
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
    fail_reconcile_once: AtomicBool,
}
