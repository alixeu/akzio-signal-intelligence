use chrono::Duration;
use tempfile::tempdir;

use akzio_domain::{
    ArtifactId, ContentHash, FactorExposure, FailureDisposition, LifecycleEventType, MoneyMicros,
    PaperApprovalScope, PaperLaunchApproval, RetryPolicy, RunId, RuntimeManifest, TargetPortfolio,
    TaskBudget, TaskId, TaskRecipeId, WeightPpm, WorkflowGraph, WorkflowNode, WorkflowProposal,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{SessionReservation, StoredRun, WorkflowCommit};

use super::*;

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
        max_attempts: 1,
        initial_backoff_ms: 1,
        retry_transport: false,
        retry_rate_limited: false,
        retry_invalid_output: false,
    }
}

fn workflow() -> WorkflowGraph {
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "paper-commitment-fixture".to_owned(),
        nodes: vec![WorkflowNode {
            task_id: TaskId::new(),
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
        }],
    }
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
    ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind: artifact.kind,
    }
}

fn reserve_approved_slot(
    store: &V2Store,
    lease: &DaemonLease,
    workflow: WorkflowCommit,
    now: DateTime<Utc>,
) {
    let run_id = workflow.run.run_id.clone();
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: workflow.run.topology_id.clone(),
        tasks: std::collections::BTreeMap::new(),
        stop_reason: Some("fixture approved Paper session".to_owned()),
    };
    let proposal = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal_payload).unwrap(),
        "runtime.paper_provisioning",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(ArtifactOrigin {
            run_id: Some(run_id),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let session = chrono::NaiveDate::parse_from_str(SESSION_KEY, "%Y-%m-%d").unwrap();
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: "fixture-revision".to_owned(),
        cargo_lock_hash: ContentHash::of_bytes(b"fixture-cargo"),
        config_hash: ContentHash::of_bytes(b"fixture-config"),
        provider_id: "fixture-provider".to_owned(),
        model_id: "fixture-model".to_owned(),
        prompt_hash: ContentHash::of_bytes(b"fixture-prompt"),
        contract_hash: ContentHash::of_bytes(b"fixture-contract"),
        topology_hash: ContentHash::of_bytes(b"fixture-topology"),
        decision_policy_hash: ContentHash::of_bytes(b"fixture-decision"),
        execution_policy_hash: ContentHash::of_bytes(b"fixture-execution"),
        evaluation_policy_hash: ContentHash::of_bytes(b"fixture-evaluation"),
        market_data_feed: "iex".to_owned(),
        broker_account_id: "fixture-account".to_owned(),
        maximum_notional: MoneyMicros::from_usd_cents(100_000),
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at: now + Duration::hours(8),
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash().unwrap();
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        provenance(now),
        None,
        vec![],
        now,
    )
    .unwrap();
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: "fixture-operator".to_owned(),
        runtime_manifest: ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::RuntimeManifest,
        },
        runtime_manifest_hash: manifest_hash,
        scope: PaperApprovalScope::Canary,
        reason: "fixture approval".to_owned(),
        approved_at: now,
        expires_at: manifest_payload.expires_at,
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.put_json(&approval_payload).unwrap(),
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        provenance(now),
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )
    .unwrap();
    store
        .reserve_paper_session_with_approval(
            lease,
            &SessionReservation {
                session_key: SESSION_KEY.to_owned(),
                workflow,
                setup_artifacts: vec![],
                reserved_at: now,
            },
            &proposal,
            &manifest,
            &approval,
        )
        .unwrap();
}

fn fixture_plan(
    now: DateTime<Utc>,
    decision_context: ArtifactRef,
    account_snapshot: ArtifactRef,
    quote_snapshot: ArtifactRef,
    market_clock_snapshot: ArtifactRef,
) -> crate::ExecutionPlan {
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(100_000));
    let mut plan = crate::ExecutionPlan {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
        policy_hash: ContentHash::of_bytes(b"fixture-policy"),
        maximum_total_notional: crate::MoneyMicros::from_usd_cents(100_000),
        target: target.clone(),
        orders: vec![crate::OrderIntent {
            asset: Asset::Qqq,
            side: crate::OrderSide::Buy,
            notional: crate::MoneyMicros::from_usd_cents(10_000),
            limit_price: crate::MoneyMicros::from_usd_cents(2_500),
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

#[test]
fn accepted_verdict_creates_one_fenced_commitment_and_reuses_it() {
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
    let workflow = WorkflowCommit {
        run: StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    reserve_approved_slot(&store, &lease, workflow, now);
    let permit = store
        .claim_next_task("fixture", now, Duration::seconds(30))
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
    let allocation = fixture_plan(
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
    let allocation_ref = ArtifactRef {
        artifact_id: allocation_artifact.artifact_id,
        kind: ArtifactKind::ExecutionPlan,
    };
    let execution_context_payload = ExecutionContext {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        run_id: permit.run_id.clone(),
        decision_context: allocation.decision_context.clone(),
        account_snapshot: Some(allocation.account_snapshot.clone()),
        quote_snapshot: Some(allocation.quote_snapshot.clone()),
        market_clock_snapshot: Some(allocation.market_clock_snapshot.clone()),
        execution_plan: Some(allocation_ref.clone()),
        factor_exposure: Some(allocation.factor_exposure.clone()),
        turnover_ppm: Some(allocation.turnover_ppm),
        plan_hash: Some(allocation.plan_hash.clone()),
        broker_session: Some(allocation.broker_session),
        frozen: false,
        created_at: now,
    };
    let context = Artifact::new(
        ArtifactKind::ExecutionContext,
        store.put_json(&execution_context_payload).unwrap(),
        "fixture.execution_context",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![
            execution_context_payload.decision_context.clone(),
            execution_context_payload.account_snapshot.clone().unwrap(),
            execution_context_payload.quote_snapshot.clone().unwrap(),
            execution_context_payload
                .market_clock_snapshot
                .clone()
                .unwrap(),
            allocation_ref,
        ],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &context,
            LifecycleEventType::ExecutionContextCreated,
            now,
        )
        .unwrap();
    let context_ref = ArtifactRef {
        artifact_id: context.artifact_id,
        kind: ArtifactKind::ExecutionContext,
    };
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
            LifecycleEventType::ExecutionVerdictCreated,
            now,
        )
        .unwrap();
    let verdict_ref = ArtifactRef {
        artifact_id: verdict.artifact_id,
        kind: ArtifactKind::ExecutionVerdict,
    };
    let input = PaperCommitmentInput {
        lease,
        permit,
        verdict: verdict_ref,
        session_key: SESSION_KEY.to_owned(),
        now,
    };
    let runtime = V2PaperCommitmentRuntime::new(store.clone());
    let first = runtime.commit(&input).unwrap();
    assert!(first.newly_committed);
    assert_eq!(first.commitment.kind, ArtifactKind::ExecutionCommitment);

    let retry = runtime.commit(&input).unwrap();
    assert!(!retry.newly_committed);
    assert_eq!(retry.commitment.artifact_id, first.commitment.artifact_id);
    assert_eq!(
        store
            .session_slot(SESSION_KEY)
            .unwrap()
            .unwrap()
            .commitment_artifact_id,
        Some(first.commitment.artifact_id)
    );
}

#[test]
fn non_paper_run_is_rejected_before_any_session_or_broker_commitment_work() {
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
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::PaperDryRun,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let error = V2PaperCommitmentRuntime::new(store)
        .commit(&PaperCommitmentInput {
            lease,
            permit,
            verdict: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"unread-verdict")),
                kind: ArtifactKind::ExecutionVerdict,
            },
            session_key: "paper:fixture".to_owned(),
            now,
        })
        .unwrap_err();
    assert!(matches!(
        error,
        PaperCommitmentError::NonPaperRun(RunPurpose::PaperDryRun)
    ));
}

#[test]
fn durable_freeze_blocks_commitment_before_any_verdict_or_broker_work() {
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
        .commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let freeze = Artifact::new(
        ArtifactKind::FreezeState,
        store
            .put_json(&FreezeState {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                frozen: true,
                reason: "fixture safety freeze".to_owned(),
                changed_at: now,
            })
            .unwrap(),
        "fixture.freeze",
        ArtifactLifecycle::Canonical,
        provenance(now),
        None,
        vec![],
        now,
    )
    .unwrap();
    store.write_bootstrap_artifact(&freeze).unwrap();

    let error = V2PaperCommitmentRuntime::new(store.clone())
        .commit(&PaperCommitmentInput {
            lease,
            permit: permit.clone(),
            verdict: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"unread-verdict")),
                kind: ArtifactKind::ExecutionVerdict,
            },
            session_key: "paper:fixture".to_owned(),
            now,
        })
        .unwrap_err();
    assert!(matches!(error, PaperCommitmentError::Frozen));
    store.validate_task_permit(&permit).unwrap();
}
