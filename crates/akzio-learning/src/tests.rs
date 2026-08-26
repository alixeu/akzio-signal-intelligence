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

impl RuntimeFixture {
    fn new() -> Self {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = fixture_time();
        let sealed_at = now + Duration::hours(1);
        let candidate_contract_hash = ContentHash::of_bytes(b"candidate-contract");

        let shadow_run = fixture_workflow(
            &store,
            RunPurpose::Shadow,
            1,
            Some(candidate_contract_hash.clone()),
            now,
        );
        let shadow_permit = claim_fixture_task(&store, "shadow-worker", now);
        assert_eq!(shadow_permit.run_id, shadow_run.run_id);
        let candidate_decisions = (0..5)
            .map(|index| {
                let artifact = fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::Decision,
                    ArtifactLifecycle::RunScoped,
                    &serde_json::json!({"candidate": index}),
                    vec![],
                    now,
                );
                store
                    .write_task_artifact(
                        &shadow_permit,
                        &artifact,
                        LifecycleEventType::ShadowDecisionCreated,
                        now,
                    )
                    .unwrap();
                artifact
            })
            .collect::<Vec<_>>();

        let paper_run = fixture_workflow(&store, RunPurpose::Paper, 7, None, now);
        let seed_permit = claim_fixture_task(&store, "paper-seed", now);
        assert_eq!(seed_permit.run_id, paper_run.run_id);
        let evidence = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::NormalizedEvidence,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"prices": "governed"}),
            vec![],
            now,
        );
        let parent_decision = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::Decision,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"decision": "parent"}),
            vec![artifact_reference(&evidence)],
            now,
        );
        let decision_context = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::DecisionContext,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"context": "parent"}),
            vec![artifact_reference(&evidence)],
            now,
        );
        let execution_context = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::ExecutionContext,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"execution_context": "paper"}),
            vec![artifact_reference(&decision_context)],
            now,
        );
        let execution_context_ref = artifact_reference(&execution_context);
        let verdict_payload = ExecutionVerdict::NoOrder {
            no_order: NoOrder {
                execution_context: execution_context_ref.clone(),
                blockers: vec![HardBlocker::Frozen],
                created_at: now,
            },
        };
        let verdict = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::ExecutionVerdict,
            ArtifactLifecycle::Canonical,
            &verdict_payload,
            vec![execution_context_ref.clone()],
            now,
        );
        let verdict_ref = artifact_reference(&verdict);
        let parent_schedule = OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: artifact_reference(&parent_decision),
            decision_context: artifact_reference(&decision_context),
            execution_context: execution_context_ref.clone(),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: verdict_ref.clone(),
            },
            baseline_trading_day: day(3),
            created_at: now,
        };
        let parent_schedule_artifact = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::OutcomeSchedule,
            ArtifactLifecycle::Canonical,
            &parent_schedule,
            vec![
                parent_schedule.decision.clone(),
                parent_schedule.decision_context.clone(),
                parent_schedule.execution_context.clone(),
                verdict_ref.clone(),
            ],
            now,
        );
        let evidence_ref = artifact_reference(&evidence);
        let mut parent_materialization = materialization();
        parent_materialization.schedule = parent_schedule;
        parent_materialization.schedule_artifact = artifact_reference(&parent_schedule_artifact);
        parent_materialization.market_evidence = vec![evidence_ref.clone()];
        parent_materialization.sealed_at = sealed_at;
        for observation in &mut parent_materialization.observations {
            observation.observed_evidence_count = observation.expected_evidence_count;
            observation.detected_risk_count = Some(observation.expected_risk_count);
        }
        let parent_outcome_payload = materialize_outcome(&parent_materialization).unwrap();
        let parent_outcome = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::Outcome,
            ArtifactLifecycle::Canonical,
            &parent_outcome_payload,
            vec![
                parent_materialization.schedule_artifact.clone(),
                evidence_ref.clone(),
            ],
            sealed_at,
        );

        let candidate_schedules = candidate_decisions
            .iter()
            .map(|candidate_decision| {
                let schedule = OutcomeSchedule {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    outcome_id: OutcomeId::new(),
                    decision: artifact_reference(candidate_decision),
                    decision_context: artifact_reference(&decision_context),
                    execution_context: execution_context_ref.clone(),
                    execution: OutcomeExecutionLineage::NoOrder {
                        execution_verdict: verdict_ref.clone(),
                    },
                    baseline_trading_day: day(3),
                    created_at: now,
                };
                let artifact = fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::OutcomeSchedule,
                    ArtifactLifecycle::RunScoped,
                    &schedule,
                    vec![
                        schedule.decision.clone(),
                        schedule.decision_context.clone(),
                        schedule.execution_context.clone(),
                        verdict_ref.clone(),
                    ],
                    now,
                );
                (schedule, artifact)
            })
            .collect::<Vec<_>>();

        let seed_artifacts = vec![
            evidence,
            parent_decision.clone(),
            decision_context,
            execution_context,
            verdict,
            parent_schedule_artifact,
        ];
        for artifact in &seed_artifacts {
            store
                .write_task_artifact(
                    &seed_permit,
                    artifact,
                    LifecycleEventType::PaperSeedArtifactCreated,
                    now,
                )
                .unwrap();
        }
        store
            .commit_outcomes(
                &seed_permit,
                std::slice::from_ref(&parent_outcome),
                sealed_at,
            )
            .unwrap();

        for (_, artifact) in &candidate_schedules {
            store
                .write_task_artifact(
                    &shadow_permit,
                    artifact,
                    LifecycleEventType::ShadowOutcomeScheduleCreated,
                    now,
                )
                .unwrap();
        }

        let candidate_outcomes = candidate_schedules
            .iter()
            .map(|(schedule, schedule_artifact)| {
                let mut input = materialization();
                input.schedule = schedule.clone();
                input.schedule_artifact = artifact_reference(schedule_artifact);
                input.market_evidence = vec![evidence_ref.clone()];
                input.sealed_at = sealed_at;
                let outcome = materialize_outcome(&input).unwrap();
                fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::Outcome,
                    ArtifactLifecycle::RunScoped,
                    &outcome,
                    vec![input.schedule_artifact, evidence_ref.clone()],
                    sealed_at,
                )
            })
            .collect::<Vec<_>>();
        store
            .commit_outcomes(&shadow_permit, &candidate_outcomes, sealed_at)
            .unwrap();

        let candidates = candidate_decisions
            .iter()
            .zip(candidate_outcomes.iter())
            .map(|(decision, outcome)| (artifact_reference(decision), artifact_reference(outcome)))
            .collect();
        let runtime = EvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
        let active_topology = ArtifactRef {
            artifact_id: paper_run.graph_artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        };
        let candidate_topology = ArtifactRef {
            artifact_id: shadow_run.graph_artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        };
        Self {
            _root: root,
            store,
            runtime,
            paper_run_id: paper_run.run_id,
            subject: PolicySubject::Memory(MemoryId::new()),
            materialization: parent_materialization,
            parent_decision: artifact_reference(&parent_decision),
            execution_context: execution_context_ref,
            parent_outcome: artifact_reference(&parent_outcome),
            candidates,
            active_topology,
            candidate_topology,
            candidate_contract_hash,
            candidate_topology_id: shadow_run.topology_id,
            pair_completed_at: sealed_at,
        }
    }

    fn claim_evaluation(&self, worker: &str) -> TaskWritePermit {
        let permit = claim_fixture_task(&self.store, worker, fixture_time());
        assert_eq!(permit.run_id, self.paper_run_id);
        permit
    }

    fn record_pair_batch(&self, permit: &TaskWritePermit, batch: usize) {
        self.record_pair_batch_for(permit, batch, &self.subject);
    }

    fn record_pair_batch_for(
        &self,
        permit: &TaskWritePermit,
        batch: usize,
        subject: &PolicySubject,
    ) {
        let (candidate_decision, candidate_outcome) = &self.candidates[batch];
        for horizon in OutcomeHorizon::ALL {
            self.runtime
                .record_shadow_pair(
                    permit,
                    subject,
                    ShadowObservation {
                        parent_decision: self.parent_decision.clone(),
                        execution_context: self.execution_context.clone(),
                        candidate_decision: candidate_decision.clone(),
                        candidate_contract_hash: self.candidate_contract_hash.clone(),
                        candidate_topology_id: self.candidate_topology_id.clone(),
                        horizon,
                        parent_outcome: self.parent_outcome.clone(),
                        candidate_outcome: candidate_outcome.clone(),
                        completed_at: self.pair_completed_at,
                    },
                )
                .unwrap();
        }
    }

    fn evaluate(&self, permit: TaskWritePermit, hypothesis_id: &str) -> EvaluationResult {
        self.evaluate_for(
            permit,
            hypothesis_id,
            self.subject.clone(),
            None,
            self.materialization.clone(),
        )
    }

    fn evaluate_for(
        &self,
        permit: TaskWritePermit,
        hypothesis_id: &str,
        subject: PolicySubject,
        candidate_policy: Option<CandidatePolicyInput>,
        materialization: OutcomeMaterializationInput,
    ) -> EvaluationResult {
        let contract_hash = match &subject {
            PolicySubject::Contract(hash) => hash.clone(),
            _ => ContentHash::of_bytes(b"active-contract"),
        };
        let topology_id = match &subject {
            PolicySubject::Topology(topology_id) => topology_id.clone(),
            _ => TopologyId("active-topology".to_owned()),
        };
        self.runtime
            .evaluate(EvaluationInput {
                permit,
                subject,
                hypothesis_id: hypothesis_id.to_owned(),
                materialization,
                contract_hash,
                topology_id,
                candidate_policy,
                token_cost: Some(10),
                latency_millis: Some(20),
            })
            .unwrap()
    }
}

#[test]
fn rust_materializes_returns_calibration_completeness_and_recall() {
    let outcome = materialize_outcome(&materialization()).unwrap();
    assert_eq!(outcome.schedule.kind, ArtifactKind::OutcomeSchedule);
    assert_eq!(outcome.windows.len(), 3);

    let t1 = &outcome.windows[0];
    assert_eq!(t1.portfolio_return_ppm, 100_000);
    assert_eq!(t1.benchmark_return_ppm, 50_000);
    assert_eq!(t1.transaction_cost_ppm, 100);
    assert_eq!(t1.slippage_ppm, 50);
    assert_eq!(t1.utility_ppm, 49_850);
    assert_eq!(t1.calibration_ppm, None);
    assert_eq!(t1.evidence_completeness_ppm, 750_000);
    assert_eq!(t1.risk_recall_ppm, Some(500_000));

    let t3 = &outcome.windows[1];
    assert_eq!(t3.portfolio_return_ppm, -100_000);
    assert_eq!(t3.benchmark_return_ppm, -50_000);
    assert_eq!(t3.utility_ppm, -50_150);
    assert_eq!(t3.calibration_ppm, None);
}

#[test]
fn partial_materializer_seals_only_the_due_prefix() {
    let mut input = materialization();
    input
        .observations
        .retain(|observation| observation.horizon == OutcomeHorizon::T1);
    let outcome = materialize_partial_outcome(&input).unwrap();
    assert_eq!(outcome.windows.len(), 1);
    assert_eq!(outcome.windows[0].horizon, OutcomeHorizon::T1);
    assert!(outcome.sealed_at.is_none());
    assert!(materialize_outcome(&input).is_err());
}

#[test]
fn materializer_rejects_duplicate_and_missing_horizons() {
    let mut missing = materialization();
    missing.observations.pop();
    assert!(matches!(
        materialize_outcome(&missing),
        Err(EvaluationError::InvalidMaterialization(
            "missing observation horizon"
        ))
    ));

    let mut duplicate = materialization();
    duplicate
        .observations
        .push(duplicate.observations[0].clone());
    assert!(matches!(
        materialize_outcome(&duplicate),
        Err(EvaluationError::InvalidMaterialization(
            "duplicate observation horizon"
        ))
    ));

    let mut duplicate_forecast = materialization();
    duplicate_forecast
        .forecasts
        .push(forecast(DecisionHorizon::T1, 500_000));
    assert!(matches!(
        materialize_outcome(&duplicate_forecast),
        Err(EvaluationError::InvalidMaterialization(
            "duplicate forecast horizon"
        ))
    ));
}

#[test]
fn materializer_rejects_not_due_and_incomplete_price_surfaces() {
    let mut not_due = materialization();
    not_due.observations[2].completed_trading_sessions = 4;
    assert!(matches!(
        materialize_outcome(&not_due),
        Err(EvaluationError::InvalidMaterialization(
            "horizon is not due"
        ))
    ));

    let mut incomplete = materialization();
    incomplete.observations[0].future_prices.remove(&Asset::Qqq);
    assert!(matches!(
        materialize_outcome(&incomplete),
        Err(EvaluationError::InvalidMaterialization(_))
    ));
}

#[test]
fn materializer_rejects_cost_model_above_one_hundred_percent() {
    let mut input = materialization();
    input.cost_model.transaction_cost_ppm = 1_000_001;

    assert!(matches!(
        materialize_outcome(&input),
        Err(EvaluationError::Domain(DomainError::InvalidBudget {
            field: "outcome.cost_model"
        }))
    ));
}

#[test]
fn every_nonpaper_purpose_is_rejected_for_canonical_learning() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Replay,
        RunPurpose::PaperDryRun,
        RunPurpose::Shadow,
    ] {
        assert!(matches!(
            require_canonical_purpose(purpose),
            Err(EvaluationError::NonCanonicalPurpose(actual)) if actual == purpose
        ));
    }
    require_canonical_purpose(RunPurpose::Paper).unwrap();
}

#[test]
fn nonpaper_evaluation_cannot_write_learning_state_or_events() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Replay,
        RunPurpose::PaperDryRun,
        RunPurpose::Shadow,
    ] {
        let fixture = RuntimeFixture::new();
        let blocked_paper = fixture.claim_evaluation("block-paper-queue");
        fixture
            .store
            .finish_task(&blocked_paper, TaskStatus::Cancelled, fixture_time())
            .unwrap();
        let run = fixture_workflow(&fixture.store, purpose, 1, None, fixture_time());
        let permit = claim_fixture_task(&fixture.store, "nonpaper", fixture_time());
        assert_eq!(permit.run_id, run.run_id);
        let subject = PolicySubject::Memory(MemoryId::new());
        let error = fixture
            .runtime
            .evaluate(EvaluationInput {
                permit: permit.clone(),
                subject: subject.clone(),
                hypothesis_id: "must-not-persist".to_owned(),
                materialization: fixture.materialization.clone(),
                contract_hash: ContentHash::of_bytes(b"active-contract"),
                topology_id: TopologyId("active-topology".to_owned()),
                candidate_policy: None,
                token_cost: Some(1),
                latency_millis: Some(1),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            EvaluationError::NonCanonicalPurpose(actual) if actual == purpose
        ));
        assert!(fixture.store.policy_head(&subject).unwrap().is_none());
        assert_eq!(
            fixture
                .store
                .policy_shadow_pair_snapshot(&subject)
                .unwrap()
                .through_cursor,
            0
        );
        assert!(fixture
            .store
            .events_after(&run.run_id, 0, 100)
            .unwrap()
            .iter()
            .all(|event| !matches!(
                event.event_type.as_str(),
                "policy.evaluated" | "policy.transitioned" | "artifact.committed"
            )));
        fixture
            .store
            .finish_task(&permit, TaskStatus::Cancelled, fixture_time())
            .unwrap();
        fixture.store.verify_integrity().unwrap();
    }
}

#[test]
fn memory_lifecycle_requires_pairs_and_degrades_to_retirement() {
    let subject = PolicySubject::Memory(MemoryId::new());
    assert_eq!(
        subject.initial_state(),
        PolicyState::Memory(MemoryLifecycle::Candidate)
    );
    assert_eq!(
        next_state_with_fresh_pairs(subject.initial_state(), false, [0, 0, 0]),
        PolicyState::Memory(MemoryLifecycle::Candidate)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Active),
            false,
            [1, 1, 1],
        ),
        PolicyState::Memory(MemoryLifecycle::Proven)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Proven),
            true,
            [1, 1, 1],
        ),
        PolicyState::Memory(MemoryLifecycle::Contested)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Contested),
            true,
            [1, 1, 1],
        ),
        PolicyState::Memory(MemoryLifecycle::Retired)
    );
}

#[test]
fn canonical_evaluation_promotes_memory_only_after_fresh_pairs() {
    let fixture = RuntimeFixture::new();
    let mut prior_cursor = 0;

    for batch in 0..3 {
        let permit = fixture.claim_evaluation(&format!("evaluation-disabled-{batch}"));
        fixture.record_pair_batch(&permit, batch);
        let result = fixture.evaluate(permit, "forward-transition-disabled");
        assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 1]);
        let expected_state = if batch == 0 {
            PolicyState::Memory(MemoryLifecycle::Active)
        } else {
            PolicyState::Memory(MemoryLifecycle::Proven)
        };
        assert_eq!(
            result.policy_head.as_ref().map(|head| head.state),
            Some(expected_state)
        );

        let cursor = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap()
            .through_cursor;
        assert!(cursor > prior_cursor);
        prior_cursor = cursor;
    }

    let replay_permit = fixture.claim_evaluation("evaluation-old-pairs");
    let old_pairs = fixture.evaluate(replay_permit, "old-pairs-cannot-replay");
    assert_eq!(old_pairs.fresh_pairs_by_horizon, [0, 0, 0]);
    assert_eq!(
        old_pairs.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Memory(MemoryLifecycle::Proven))
    );
    assert_eq!(
        fixture
            .store
            .policy_transitions(&fixture.subject)
            .unwrap()
            .len(),
        2
    );

    let evaluated = fixture
        .store
        .events_after(&fixture.paper_run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == "policy.evaluated")
        .count();
    assert_eq!(evaluated, 4);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn topology_shadow_pair_must_name_the_candidate_subject() {
    let fixture = RuntimeFixture::new();
    let permit = fixture.claim_evaluation("structured-critique-mismatch");
    let subject = PolicySubject::Topology(TopologyId(
        STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
    ));
    let (candidate_decision, candidate_outcome) = &fixture.candidates[0];

    assert!(matches!(
        fixture.runtime.record_shadow_pair(
            &permit,
            &subject,
            ShadowObservation {
                parent_decision: fixture.parent_decision.clone(),
                execution_context: fixture.execution_context.clone(),
                candidate_decision: candidate_decision.clone(),
                candidate_contract_hash: fixture.candidate_contract_hash.clone(),
                candidate_topology_id: fixture.candidate_topology_id.clone(),
                horizon: OutcomeHorizon::T1,
                parent_outcome: fixture.parent_outcome.clone(),
                candidate_outcome: candidate_outcome.clone(),
                completed_at: fixture.pair_completed_at,
            },
        ),
        Err(EvaluationError::InvalidCandidatePolicy(
            "shadow_topology_id"
        ))
    ));
    assert!(fixture.store.policy_head(&subject).unwrap().is_none());
}

#[test]
fn topology_forward_promotion_is_disabled_and_degradation_rolls_back() {
    let fixture = RuntimeFixture::new();
    let subject = PolicySubject::Topology(TopologyId(fixture.candidate_topology_id.clone()));
    let permit = fixture.claim_evaluation("topology-forward-disabled");
    fixture.record_pair_batch_for(&permit, 0, &subject);

    let result = fixture.evaluate_for(
        permit,
        "topology-forward-disabled",
        subject.clone(),
        Some(CandidatePolicyInput {
            baseline: fixture.active_topology.clone(),
            candidate: fixture.candidate_topology.clone(),
        }),
        fixture.materialization.clone(),
    );

    assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 1]);
    assert!(result.policy_head.is_none());
    assert!(result.candidate_policy.is_some());
    assert!(fixture
        .store
        .policy_transitions(&subject)
        .unwrap()
        .is_empty());
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Topology(CandidatePolicyState::Canary50),
            true,
            [1, 1, 1],
        ),
        PolicyState::Topology(CandidatePolicyState::Candidate)
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn contract_candidate_materializes_a_bound_policy_artifact() {
    let fixture = RuntimeFixture::new();
    let now = fixture_time();
    let (baseline, baseline_ref) = fixture_contract(&fixture.store, "baseline", now);
    let (candidate, candidate_ref) = fixture_contract(&fixture.store, "candidate", now);
    assert!(baseline.permits_candidate(&candidate));
    let subject = PolicySubject::Contract(candidate.contract_hash.clone());
    let permit = fixture.claim_evaluation("contract-candidate");
    let task_id = permit.task_id.clone();
    let result = fixture.evaluate_for(
        permit,
        "contract-candidate",
        subject.clone(),
        Some(CandidatePolicyInput {
            baseline: baseline_ref.clone(),
            candidate: candidate_ref.clone(),
        }),
        fixture.materialization.clone(),
    );
    assert_eq!(result.fresh_pairs_by_horizon, [0, 0, 0]);
    assert!(result.policy_head.is_none());
    let policy_ref = result.candidate_policy.unwrap();
    let artifact = fixture.store.artifact(&policy_ref.artifact_id).unwrap();
    let policy: CandidatePolicy =
        serde_json::from_slice(&fixture.store.read_blob(&artifact.blob).unwrap()).unwrap();
    assert_eq!(policy.subject, subject);
    assert_eq!(policy.baseline, baseline_ref);
    assert_eq!(policy.candidate, candidate_ref);
    assert_eq!(policy.source_evaluation, result.evaluation);
    assert!(fixture
        .store
        .committed_task_outputs(&fixture.paper_run_id, &task_id)
        .unwrap()
        .iter()
        .any(|output| output.artifact_id == policy_ref.artifact_id));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn shadow_outcome_schedule_requires_run_scoped_mixed_closure() {
    let fixture = RuntimeFixture::new();
    let now = fixture_time();
    fixture
        .store
        .request_run_cancel(&fixture.paper_run_id, "isolate schedule boundary test", now)
        .unwrap();

    let debug_run = fixture_workflow(
        &fixture.store,
        RunPurpose::Debug,
        1,
        None,
        now - Duration::days(2),
    );
    let debug_permit = claim_fixture_task(&fixture.store, "debug-context", now);
    assert_eq!(debug_permit.run_id, debug_run.run_id);
    let debug_context = fixture_artifact(
        &fixture.store,
        Some(&debug_permit),
        ArtifactKind::DecisionContext,
        ArtifactLifecycle::RunScoped,
        &serde_json::json!({"context": "debug"}),
        vec![],
        now,
    );
    fixture
        .store
        .commit_attempt(
            &debug_permit,
            std::slice::from_ref(&debug_context),
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let shadow_run = fixture_workflow(
        &fixture.store,
        RunPurpose::Shadow,
        1,
        Some(fixture.candidate_contract_hash.clone()),
        now - Duration::days(1),
    );
    let shadow_permit = claim_fixture_task(&fixture.store, "shadow-schedule", now);
    assert_eq!(shadow_permit.run_id, shadow_run.run_id);
    let candidate_decision = fixture_artifact(
        &fixture.store,
        Some(&shadow_permit),
        ArtifactKind::Decision,
        ArtifactLifecycle::RunScoped,
        &serde_json::json!({"candidate": "schedule-boundary"}),
        vec![],
        now,
    );
    fixture
        .store
        .write_task_artifact(
            &shadow_permit,
            &candidate_decision,
            LifecycleEventType::ShadowDecisionCreated,
            now,
        )
        .unwrap();

    let build_outcome = |decision_context: ArtifactRef,
                         schedule_lifecycle: ArtifactLifecycle|
     -> Result<(Artifact, Artifact), StoreError> {
        let mut schedule = fixture.materialization.schedule.clone();
        schedule.outcome_id = OutcomeId::new();
        schedule.decision = artifact_reference(&candidate_decision);
        schedule.decision_context = decision_context;
        schedule.created_at = now;
        let schedule_artifact = fixture_artifact(
            &fixture.store,
            Some(&shadow_permit),
            ArtifactKind::OutcomeSchedule,
            schedule_lifecycle,
            &schedule,
            vec![
                schedule.decision.clone(),
                schedule.decision_context.clone(),
                schedule.execution_context.clone(),
                execution_verdict(&schedule.execution).clone(),
            ],
            now,
        );
        fixture.store.write_task_artifact(
            &shadow_permit,
            &schedule_artifact,
            LifecycleEventType::ShadowOutcomeScheduleCreated,
            now,
        )?;

        let mut materialization = fixture.materialization.clone();
        materialization.schedule = schedule;
        materialization.schedule_artifact = artifact_reference(&schedule_artifact);
        let outcome = materialize_outcome(&materialization).unwrap();
        let outcome_artifact = fixture_artifact(
            &fixture.store,
            Some(&shadow_permit),
            ArtifactKind::Outcome,
            ArtifactLifecycle::RunScoped,
            &outcome,
            std::iter::once(materialization.schedule_artifact.clone())
                .chain(materialization.market_evidence.iter().cloned())
                .collect(),
            materialization.sealed_at,
        );
        Ok((schedule_artifact, outcome_artifact))
    };

    let paper_decision_context = fixture.materialization.schedule.decision_context.clone();
    assert!(matches!(
        build_outcome(paper_decision_context.clone(), ArtifactLifecycle::Canonical),
        Err(StoreError::InvalidTaskArtifactLifecycle {
            purpose: RunPurpose::Shadow,
            lifecycle: ArtifactLifecycle::Canonical,
        })
    ));

    let (_, debug_closure_outcome) = build_outcome(
        artifact_reference(&debug_context),
        ArtifactLifecycle::RunScoped,
    )
    .unwrap();
    assert!(matches!(
        fixture.store.commit_outcomes(
            &shadow_permit,
            &[debug_closure_outcome],
            fixture.materialization.sealed_at,
        ),
        Err(StoreError::InvalidLearningCommit(
            "learning_artifact.run_purpose"
        ))
    ));

    let (mixed_schedule_artifact, mixed_outcome) =
        build_outcome(paper_decision_context, ArtifactLifecycle::RunScoped).unwrap();
    fixture
        .store
        .commit_outcomes(
            &shadow_permit,
            &[mixed_outcome],
            fixture.materialization.sealed_at,
        )
        .unwrap();
    assert_eq!(
        mixed_schedule_artifact.lifecycle,
        ArtifactLifecycle::RunScoped
    );
    let schedule: OutcomeSchedule = serde_json::from_slice(
        &fixture
            .store
            .read_blob(&mixed_schedule_artifact.blob)
            .unwrap(),
    )
    .unwrap();
    let purpose = |reference: &ArtifactRef| {
        let artifact = fixture.store.artifact(&reference.artifact_id).unwrap();
        let run_id = artifact.origin.unwrap().run_id.unwrap();
        fixture.store.run_purpose(&run_id).unwrap()
    };
    assert_eq!(purpose(&schedule.decision), RunPurpose::Shadow);
    assert_eq!(purpose(&schedule.decision_context), RunPurpose::Paper);
    assert_eq!(purpose(&schedule.execution_context), RunPurpose::Paper);
    assert_eq!(
        purpose(execution_verdict(&schedule.execution)),
        RunPurpose::Paper
    );
}
