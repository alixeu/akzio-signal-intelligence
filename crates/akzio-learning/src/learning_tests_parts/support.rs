use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    AgentContract, ArtifactId, ContextPolicy, ContractId, ContractPurpose, ExecutionVerdict,
    FailureDisposition, HardBlocker, LifecycleEventType, NoOrder, OutputContract, PromptBundle,
    RetryPolicy, RunId, TaskBudget, TaskId, TaskRecipeId, TaskStatus, TaskWritePermit,
    TerminationPolicy, WeightPpm, WorkflowGraph, WorkflowNode,
};
use akzio_domain::{
    DecisionHorizon, Forecast, MemoryId, OutcomeId, STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use tempfile::{tempdir, TempDir};

use super::*;

fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(value)),
        kind,
    }
}

fn day(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
}

fn prices(tqqq: i64, qqq: i64) -> BTreeMap<Asset, MoneyMicros> {
    BTreeMap::from([
        (Asset::Tqqq, MoneyMicros(tqqq)),
        (Asset::Qqq, MoneyMicros(qqq)),
        (Asset::Soxx, MoneyMicros(100_000_000)),
        (Asset::Soxl, MoneyMicros(100_000_000)),
    ])
}

fn forecast(horizon: DecisionHorizon, probability: u32) -> Forecast {
    Forecast {
        asset: Asset::Tqqq,
        horizon,
        positive_return_probability_ppm: probability,
        expected_return_ppm: 0,
    }
}

fn observation(
    horizon: OutcomeHorizon,
    sessions: u8,
    observed_day: u32,
    future_prices: BTreeMap<Asset, MoneyMicros>,
) -> GovernedHorizonObservation {
    GovernedHorizonObservation {
        horizon,
        completed_trading_sessions: sessions,
        observed_trading_day: day(observed_day),
        future_prices,
        expected_evidence_count: 4,
        observed_evidence_count: 3,
        expected_risk_count: 2,
        detected_risk_count: Some(1),
    }
}

fn materialization() -> OutcomeMaterializationInput {
    let outcome_id = OutcomeId::new();
    OutcomeMaterializationInput {
        schedule: OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id,
            decision: reference(ArtifactKind::Decision, b"decision"),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"no-order"),
            },
            baseline_trading_day: day(3),
            created_at: Utc::now(),
        },
        schedule_artifact: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
        target: TargetPortfolio {
            weights: BTreeMap::from([
                (Asset::Tqqq, WeightPpm(1_000_000)),
                (Asset::Qqq, WeightPpm::ZERO),
                (Asset::Soxx, WeightPpm::ZERO),
                (Asset::Soxl, WeightPpm::ZERO),
            ]),
        },
        forecasts: vec![
            forecast(DecisionHorizon::T1, 800_000),
            forecast(DecisionHorizon::T3, 200_000),
            forecast(DecisionHorizon::T5, 500_000),
        ],
        baseline_prices: prices(100_000_000, 100_000_000),
        observations: vec![
            observation(OutcomeHorizon::T1, 1, 4, prices(110_000_000, 105_000_000)),
            observation(OutcomeHorizon::T3, 3, 6, prices(90_000_000, 95_000_000)),
            observation(OutcomeHorizon::T5, 5, 10, prices(100_000_000, 100_000_000)),
        ],
        market_evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"market")],
        cost_model: OutcomeCostModel {
            transaction_cost_ppm: 100,
            slippage_ppm: 50,
        },
        sealed_at: Utc::now(),
    }
}

fn fixture_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0)
        .single()
        .unwrap()
}

fn artifact_reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn fixture_artifact<T: Serialize>(
    store: &V2Store,
    permit: Option<&TaskWritePermit>,
    kind: ArtifactKind,
    lifecycle: ArtifactLifecycle,
    payload: &T,
    source_refs: Vec<ArtifactRef>,
    created_at: DateTime<Utc>,
) -> Artifact {
    let origin = permit.map(|permit| ArtifactOrigin {
        run_id: Some(permit.run_id.clone()),
        task_id: Some(permit.task_id.clone()),
        attempt_id: Some(permit.attempt_id.clone()),
        contract_hash: permit.contract_hash.clone(),
    });
    Artifact::new(
        kind,
        store.put_json(payload).unwrap(),
        "learning.fixture",
        lifecycle,
        ArtifactProvenance {
            source_family: "learning.fixture".to_owned(),
            observed_at: Some(created_at),
            retrieved_at: created_at,
            source_uri: None,
            confidence_ppm: PPM_ONE,
            producer_contract_hash: permit.and_then(|permit| permit.contract_hash.clone()),
        },
        origin,
        source_refs,
        created_at,
    )
    .unwrap()
}

fn fixture_contract(
    store: &V2Store,
    label: &str,
    now: DateTime<Utc>,
) -> (AgentContract, ArtifactRef) {
    let contract = AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.analyst").unwrap(),
        format!("{label} contract"),
        PromptBundle {
            version: 1,
            governance: store
                .put_bytes(format!("{label} governance").as_bytes(), "text/plain")
                .unwrap(),
            role: store
                .put_bytes(format!("{label} prompt").as_bytes(), "text/plain")
                .unwrap(),
        },
        ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: false,
        },
        vec![],
        vec![],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store
                .put_json(&serde_json::json!({"type": "object"}))
                .unwrap(),
        },
        TaskBudget {
            max_input_tokens: 256,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 0,
        },
        RetryPolicy::none(),
        TerminationPolicy::leaf(),
        FailureDisposition::FailTask,
    )
    .unwrap();
    let artifact = Artifact::new(
        ArtifactKind::Contract,
        store.put_json(&contract).unwrap(),
        "learning.fixture.contract",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "learning.fixture".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: PPM_ONE,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )
    .unwrap();
    store.write_bootstrap_artifact(&artifact).unwrap();
    (contract, artifact_reference(&artifact))
}

fn fixture_workflow(
    store: &V2Store,
    purpose: RunPurpose,
    task_count: usize,
    contract_hash: Option<ContentHash>,
    created_at: DateTime<Utc>,
) -> StoredRun {
    let run_id = RunId::new();
    let topology_id = format!("fixture-{}", run_id.0);
    let mut previous: Option<TaskId> = None;
    let nodes = (0..task_count)
        .map(|index| {
            let task_id = TaskId::new();
            let dependencies = previous.iter().cloned().collect();
            previous = Some(task_id.clone());
            WorkflowNode {
                task_id,
                recipe_id: TaskRecipeId::new(format!("fixture.task.{index}")).unwrap(),
                contract_hash: contract_hash.clone(),
                objective: format!("fixture task {index}"),
                dependencies,
                input_artifacts: vec![],
                priority: 50,
                budget: TaskBudget {
                    max_input_tokens: 32,
                    max_output_tokens: 16,
                    max_wall_time_secs: 10,
                    max_tool_calls: 1,
                },
                retry: RetryPolicy::none(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            }
        })
        .collect::<Vec<_>>();
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: topology_id.clone(),
        nodes: nodes.clone(),
    };
    let graph_artifact = fixture_artifact(
        store,
        None,
        ArtifactKind::WorkflowGraph,
        ArtifactLifecycle::RunScoped,
        &graph,
        vec![],
        created_at,
    );
    let run = StoredRun {
        run_id,
        purpose,
        topology_id,
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes,
        })
        .unwrap();
    run
}

fn claim_fixture_task(store: &V2Store, worker: &str, now: DateTime<Utc>) -> TaskWritePermit {
    store
        .claim_next_task(worker, now, Duration::minutes(5))
        .unwrap()
        .unwrap()
        .permit
}

struct RuntimeFixture {
    _root: TempDir,
    store: V2Store,
    runtime: EvaluationRuntime,
    paper_run_id: RunId,
    subject: PolicySubject,
    materialization: OutcomeMaterializationInput,
    parent_decision: ArtifactRef,
    execution_context: ArtifactRef,
    parent_outcome: ArtifactRef,
    candidates: Vec<(ArtifactRef, ArtifactRef)>,
    active_topology: ArtifactRef,
    candidate_topology: ArtifactRef,
    candidate_contract_hash: ContentHash,
    candidate_topology_id: String,
    pair_completed_at: DateTime<Utc>,
}
