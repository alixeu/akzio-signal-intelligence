use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    Asset, ContentHash, DecisionId, FactorLimits, FailureDisposition, MoneyMicros, Position, Quote,
    RetryPolicy, RunId, SoftWarning, TargetPortfolio, TaskBudget, TaskId, TaskRecipeId, WeightPpm,
    WorkflowGraph, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use tempfile::tempdir;

use super::*;

#[test]
fn freshness_window_rejects_stale_and_future_snapshots() {
    let now = Utc::now();
    assert!(!outside_freshness_window(now, now, 5, 1));
    assert!(outside_freshness_window(
        now - Duration::seconds(6),
        now,
        5,
        1,
    ));
    assert!(outside_freshness_window(
        now + Duration::seconds(2),
        now,
        5,
        1,
    ));
}

#[test]
fn snapshot_window_rejects_cross_acquisition_skew() {
    let now = Utc::now();
    assert!(!snapshot_skewed(
        [now, now + Duration::seconds(1), now + Duration::seconds(2)],
        2,
    ));
    assert!(snapshot_skewed(
        [now, now + Duration::seconds(1), now + Duration::seconds(3)],
        2,
    ));
}

fn execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        assets: Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>(),
        max_gross_weight: WeightPpm(1_000_000),
        max_new_notional: MoneyMicros::from_usd_cents(2_000_000),
        max_daily_turnover: WeightPpm(1_000_000),
        max_account_age_secs: 5,
        max_quote_age_secs: 5,
        max_clock_age_secs: 5,
        max_future_skew_secs: 1,
        max_snapshot_skew_secs: 2,
        max_spread_bps: 20,
        limit_protection_bps: 10,
    }
}

fn gate_policy(factor_limit: u32, turnover_limit: u32) -> ExecutionGatePolicy {
    ExecutionGatePolicy {
        factor_limits: FactorLimits {
            global_leveraged_equity_ppm: factor_limit,
            nasdaq_ppm: factor_limit,
            semiconductor_ppm: factor_limit,
            paired_index_ppm: factor_limit,
        },
        max_turnover_ppm: turnover_limit,
    }
}

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 64,
        max_output_tokens: 64,
        max_wall_time_secs: 30,
        max_tool_calls: 1,
    }
}

fn graph() -> WorkflowGraph {
    let source = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("execution.source").unwrap(),
        contract_hash: None,
        objective: "create typed execution inputs".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 100,
        budget: budget(),
        retry: RetryPolicy::none(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let gate = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("execution.gate").unwrap(),
        contract_hash: None,
        objective: "gate typed execution plan".to_owned(),
        dependencies: vec![source.task_id.clone()],
        input_artifacts: vec![],
        priority: 100,
        budget: budget(),
        retry: RetryPolicy::none(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "typed-execution-fixture".to_owned(),
        nodes: vec![source, gate],
    }
}

fn provenance(now: DateTime<Utc>) -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: "fixture.execution".to_owned(),
        observed_at: Some(now),
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    }
}

fn source_artifact<T: serde::Serialize>(
    store: &V2Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    payload: &T,
    source_refs: Vec<ArtifactRef>,
    now: DateTime<Utc>,
) -> Artifact {
    let lifecycle = match store.run_purpose(&permit.run_id).unwrap() {
        RunPurpose::Paper => ArtifactLifecycle::Canonical,
        _ => ArtifactLifecycle::RunScoped,
    };
    Artifact::new(
        kind,
        store.put_json(payload).unwrap(),
        "fixture.source",
        lifecycle,
        provenance(now),
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        source_refs,
        now,
    )
    .unwrap()
}

fn as_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

struct Fixture {
    store: V2Store,
    runtime: V2ExecutionRuntime,
    input: ExecutionGateInput,
}

fn fixture(
    purpose: RunPurpose,
    account_age_secs: i64,
    quote_age_secs: i64,
    clock_open: bool,
    policy: ExecutionGatePolicy,
) -> Fixture {
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.keep()).unwrap();
    let now = Utc::now();
    let graph = graph();
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
        purpose,
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
    let source_permit = store
        .claim_next_task("fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;

    let account = source_artifact(
        &store,
        &source_permit,
        ArtifactKind::NormalizedEvidence,
        &AccountSnapshot {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            broker_session: "2026-08-10".to_owned(),
            observed_at: now - Duration::seconds(account_age_secs),
            equity: MoneyMicros::from_usd_cents(1_000_000),
            buying_power: MoneyMicros::from_usd_cents(1_000_000),
            day_turnover: MoneyMicros::ZERO,
            active: true,
            trading_blocked: false,
            positions: BTreeMap::<Asset, Position>::new(),
            external_positions: BTreeSet::new(),
            open_order_ids: BTreeSet::new(),
        },
        vec![],
        now,
    );
    let quotes = source_artifact(
        &store,
        &source_permit,
        ArtifactKind::NormalizedEvidence,
        &QuoteSnapshot {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            broker_session: "2026-08-10".to_owned(),
            observed_at: now - Duration::seconds(quote_age_secs),
            quotes: BTreeMap::from([(
                Asset::Tqqq,
                Quote {
                    bid: MoneyMicros::from_usd_cents(10_000),
                    ask: MoneyMicros::from_usd_cents(10_010),
                    observed_at: now - Duration::seconds(quote_age_secs),
                },
            )]),
        },
        vec![],
        now,
    );
    let clock = source_artifact(
        &store,
        &source_permit,
        ArtifactKind::NormalizedEvidence,
        &MarketClockSnapshot {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            broker_session: "2026-08-10".to_owned(),
            is_open: clock_open,
            observed_at: now,
        },
        vec![],
        now,
    );
    let claim = source_artifact(
        &store,
        &source_permit,
        ArtifactKind::Claim,
        &serde_json::json!({"claim": "typed execution fixture"}),
        vec![as_ref(&account)],
        now,
    );
    let claim_ref = as_ref(&claim);
    let account_ref = as_ref(&account);
    let quote_ref = as_ref(&quotes);
    let clock_ref = as_ref(&clock);
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
    let decision = DecisionContext {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_id: DecisionId::new(),
        run_id: source_permit.run_id.clone(),
        claims: vec![claim_ref.clone()],
        critiques: vec![],
        evidence: vec![account_ref.clone(), quote_ref.clone(), clock_ref.clone()],
        policy_influences: vec![],
        applied_learning_refs: vec![],
        rejected_learning_refs: vec![],
        material_conflicts: vec![],
        hard_blockers: vec![],
        soft_warnings: Vec::<SoftWarning>::new(),
        decision_policy_hash: ContentHash::of_bytes(b"fixture-decision-policy"),
        target,
        created_at: now,
    };
    let decision_artifact = source_artifact(
        &store,
        &source_permit,
        ArtifactKind::DecisionContext,
        &decision,
        vec![
            claim_ref,
            account_ref.clone(),
            quote_ref.clone(),
            clock_ref.clone(),
        ],
        now,
    );
    store
        .commit_attempt(
            &source_permit,
            &[claim, account, quotes, clock, decision_artifact.clone()],
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    let gate_permit = store
        .claim_next_task("fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let runtime = V2ExecutionRuntime::new(store.clone(), execution_policy(), policy).unwrap();
    Fixture {
        store,
        runtime,
        input: ExecutionGateInput {
            permit: gate_permit,
            decision_context: as_ref(&decision_artifact),
            account_snapshot: Some(account_ref),
            quote_snapshot: Some(quote_ref),
            market_clock_snapshot: Some(clock_ref),
            now,
        },
    }
}

fn verdict(store: &V2Store, output: &ExecutionGateOutput) -> ExecutionVerdict {
    serde_json::from_slice(&store.read_blob(&output.verdict.blob).unwrap()).unwrap()
}

fn blockers(store: &V2Store, output: &ExecutionGateOutput) -> Vec<HardBlocker> {
    match verdict(store, output) {
        ExecutionVerdict::NoOrder { no_order } => no_order.blockers,
        ExecutionVerdict::Accepted { .. } => panic!("expected NoOrder"),
    }
}

#[test]
fn accepted_gate_builds_and_atomically_commits_complete_plan_closure() {
    let fixture = fixture(
        RunPurpose::Paper,
        0,
        0,
        true,
        gate_policy(1_000_000, 1_000_000),
    );
    let output = fixture.runtime.evaluate(&fixture.input).unwrap();
    assert!(output.execution_plan.is_none());
    assert!(matches!(
        verdict(&fixture.store, &output),
        ExecutionVerdict::NoOrder { .. }
    ));
}

#[test]
fn missing_snapshots_are_durable_no_order() {
    let mut fixture = fixture(
        RunPurpose::Paper,
        0,
        0,
        true,
        gate_policy(1_000_000, 1_000_000),
    );
    fixture.input.account_snapshot = None;
    fixture.input.quote_snapshot = None;
    let output = fixture.runtime.evaluate(&fixture.input).unwrap();
    let blockers = blockers(&fixture.store, &output);
    assert!(blockers.contains(&HardBlocker::MissingAccount));
    assert!(blockers.contains(&HardBlocker::MissingQuote));
    assert!(output.execution_plan.is_none());
}

#[test]
fn stale_snapshots_are_durable_no_order() {
    for (account_age, quote_age, expected) in [
        (6, 0, HardBlocker::StaleAccount),
        (0, 6, HardBlocker::StaleQuote),
    ] {
        let fixture = fixture(
            RunPurpose::Paper,
            account_age,
            quote_age,
            true,
            gate_policy(1_000_000, 1_000_000),
        );
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(blockers(&fixture.store, &output).contains(&expected));
    }
}

#[test]
fn closed_market_is_durable_no_order() {
    let fixture = fixture(
        RunPurpose::Paper,
        0,
        0,
        false,
        gate_policy(1_000_000, 1_000_000),
    );
    let output = fixture.runtime.evaluate(&fixture.input).unwrap();
    assert!(blockers(&fixture.store, &output).contains(&HardBlocker::MarketClosed));
}

#[test]
fn allocation_limits_are_derived_from_plan_and_account() {
    for policy in [
        gate_policy(50_000, 1_000_000),
        gate_policy(1_000_000, 50_000),
    ] {
        let fixture = fixture(RunPurpose::Paper, 0, 0, true, policy);
        let output = fixture.runtime.evaluate(&fixture.input).unwrap();
        assert!(output.execution_plan.is_none());
        assert!(blockers(&fixture.store, &output).contains(&HardBlocker::NoExecutableOrder));
    }
}

#[test]
fn noncanonical_run_is_durable_no_order() {
    let fixture = fixture(
        RunPurpose::PaperDryRun,
        0,
        0,
        true,
        gate_policy(1_000_000, 1_000_000),
    );
    let output = fixture.runtime.evaluate(&fixture.input).unwrap();
    assert!(blockers(&fixture.store, &output).contains(&HardBlocker::NonCanonicalRun));
}

#[test]
fn policy_must_be_explicit_and_validated() {
    let mut policy = execution_policy();
    policy.max_new_notional = MoneyMicros::ZERO;
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.path()).unwrap();
    assert!(V2ExecutionRuntime::new(store, policy, gate_policy(1_000_000, 1_000_000)).is_err());
}
