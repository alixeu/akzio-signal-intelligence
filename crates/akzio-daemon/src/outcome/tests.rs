//! 真实 Daemon + Store + 离线 adapter 的 Outcome 协议回归。
//! 不访问网络、正式配置或 Broker；市场与模型输入均为可丢弃 fixture。

use super::*;
use akzio_domain::{OutcomeWindow, RetrospectiveStatus};

struct RegressionSession;

impl BrokerSessionClock for RegressionSession {
    fn open_session_key(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = crate::scheduler::SchedulerResult<Option<String>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(Some("2026-10-09".into())) })
    }

    fn paper_account_id(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = crate::scheduler::SchedulerResult<String>> + Send + '_,
        >,
    > {
        Box::pin(async { panic!("cold-start fixture must not read approval account") })
    }
}

fn daemon() -> Daemon {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/outcome-worker-tests")
        .join(RunId::new().0);
    std::fs::create_dir_all(&root).unwrap();
    let run_log = RunLogger::for_test(root.join("log"));
    Daemon::with_model(
        DaemonConfig {
            agent_budget: Default::default(),
            research_settings: Default::default(),
            outcome_processing: false,
            store_root: root,
            http_token: "outcome-regression-token".into(),
            worker_count: 1,
            auto_paper: false,
            market_data_feed: Some(AlpacaMarketDataFeed::Sip),
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: 0,
                slippage_ppm: 0,
            },
            decision_policy: akzio_execution::DecisionPolicy::default(),
            runtime_identity_hash: None,
            historical_evaluation_condition: None,
            model_knowledge_cutoff: None,
            run_log: Some(run_log),
        },
        fixture_model_client(),
    )
    .unwrap()
}

fn install_execution_snapshots(daemon: &mut Daemon, now: DateTime<Utc>) {
    let responses = akzio_domain::paper_session_evidence_needs("2026-10-09")
        .into_iter()
        .map(|need| {
            let acquired =
                crate::debug_fixture_evidence(EvidenceSource::Alpaca, &need.resource, now);
            (need.resource, acquired)
        });
    let adapter: Arc<dyn AsyncEvidenceAdapter> = Arc::new(FixtureEvidenceAdapter::new(
        EvidenceSource::Alpaca,
        responses,
    ));
    daemon.production_evidence = Arc::new(BTreeMap::from([(EvidenceSource::Alpaca, adapter)]));
}

async fn scheduled() -> (Daemon, RunId, Artifact, ClaimedAttempt) {
    let mut daemon = daemon();
    install_execution_snapshots(&mut daemon, Utc::now());
    let id = daemon
        .paper
        .scheduler
        .tick(
            &RegressionSession,
            &daemon.paper_workflow_source(),
            Utc::now(),
        )
        .await
        .unwrap()
        .unwrap()
        .slot
        .workflow
        .run
        .run_id;
    for _ in 0..40 {
        if !daemon.run_one("outcome-regression-t0").await.unwrap() {
            break;
        }
    }
    let schedule = daemon.store.outcome_schedule_for_run(&id).unwrap().unwrap();
    daemon
        .store
        .ensure_pending_outcome_workers(Utc::now())
        .unwrap();
    let task = daemon
        .store
        .claim_next_task_for_workload(
            "outcome-regression-worker",
            Utc::now(),
            Duration::minutes(5),
            akzio_store::TaskWorkload::Outcome,
        )
        .unwrap()
        .unwrap();
    (daemon, id, schedule, task)
}

fn claim_outcome(daemon: &Daemon) -> ClaimedAttempt {
    daemon
        .store
        .claim_next_task_for_workload(
            "outcome-regression-worker",
            Utc::now(),
            Duration::minutes(5),
            akzio_store::TaskWorkload::Outcome,
        )
        .unwrap()
        .unwrap()
}

fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn artifact<T: Serialize>(
    daemon: &Daemon,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    lifecycle: ArtifactLifecycle,
    payload: &T,
    sources: Vec<ArtifactRef>,
) -> Artifact {
    let now = Utc::now();
    let (source_family, producer) =
        if matches!(kind, ArtifactKind::Outcome | ArtifactKind::Retrospective) {
            ("akzio-learning", "akzio-learning.evaluation")
        } else {
            ("akzio.learning", "outcome.regression")
        };
    Artifact::new(
        kind,
        daemon.store.stage_json(payload).unwrap(),
        producer,
        lifecycle,
        ArtifactProvenance {
            source_family: source_family.into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(permit.artifact_origin()),
        sources,
        now,
    )
    .unwrap()
}

fn install_bars(daemon: &mut Daemon, now: DateTime<Utc>) {
    let market_day = akzio_ingest::market_session_day(now);
    let sessions = [
        "2026-10-12",
        "2026-10-13",
        "2026-10-14",
        "2026-10-15",
        "2026-10-16",
    ];
    let bars = sessions
        .into_iter()
        .map(|date| {
            serde_json::json!({
                "t":date,"o":"99.0","h":"101.0","l":"98.0","c":"100.0","v":1000
            })
        })
        .collect::<Vec<_>>();
    let responses = Asset::EXECUTABLE.into_iter().map(|asset| {
        let resource = format!("bars:{}:1d:2026-10-09:252:raw:{market_day}", asset.symbol());
        let source_uri = format!("fixture://outcome-bars/{}", asset.symbol());
        let normalized = serde_json::json!({
            "symbol":asset.symbol(),
            "bars":bars.clone(),
            "price_basis":"raw_requires_stage_action_check",
            "corporate_actions_response":{"corporate_actions":{},"next_page_token":null},
            "content_available_at":now-Duration::minutes(1),
            "latest_completed_session":"2026-10-16",
            "session_closes":sessions.into_iter().map(|date| {
                (date.to_owned(),format!("{date}T20:00:00Z"))
            }).collect::<BTreeMap<_,_>>()
        });
        let acquired = AcquiredEvidence {
            raw: serde_json::to_vec(&normalized).unwrap(),
            media_type: "application/json".into(),
            source_uri: source_uri.clone(),
            observed_at: now,
            normalized,
            provenance: EvidenceProvenance {
                document_id: Some(resource.clone()),
                published_at: None,
                observed_at: now,
                revision: Some("outcome-regression-v1".into()),
                source_uri,
                dedupe_key: format!("outcome-regression:{resource}"),
                citations: vec![],
            },
            quality: EvidenceQuality::default(),
        };
        (resource, acquired)
    });
    let adapter: Arc<dyn AsyncEvidenceAdapter> = Arc::new(FixtureEvidenceAdapter::new(
        EvidenceSource::Alpaca,
        responses,
    ));
    daemon.production_evidence = Arc::new(BTreeMap::from([(EvidenceSource::Alpaca, adapter)]));
}

fn install_narratives(
    daemon: &mut Daemon,
    schedule: &OutcomeSchedule,
    schedule_artifact: &Artifact,
    horizons: &[OutcomeHorizon],
) {
    let now = Utc::now();
    let schedule_ref = reference(schedule_artifact);
    let mut responses = Vec::new();
    for horizon in horizons {
        responses.push(serde_json::json!({"output_text":"Offline narrative memo"}));
        let result = serde_json::json!({
            "schema_version":DOMAIN_SCHEMA_VERSION,
            "outcome_id":schedule.outcome_id,
            "horizon":horizon,
            "summary":"Offline narrative accepted by the frozen Outcome node.",
            "findings":[],
            "counterfactuals":[],
            "lesson_candidates":[],
            "lesson_proposals":[],
            "diagnostic_gaps":[],
            "source_refs":[{"artifact_id":schedule_ref.artifact_id.clone(),"kind":"outcome_schedule"}],
            "created_at":now,
        });
        let output = serde_json::json!({
            "result":result,
            "deliberation":{
                "selected_path":"frozen-node regression",
                "alternatives":[],
                "alternative_match_ppm":[],
                "uncertainties":[],
                "uncertainty_weight_ppm":[],
                "basis_artifact_ids":[schedule_ref.artifact_id.clone()],
                "confidence_ppm":1_000_000,
            }
        });
        responses.push(serde_json::json!({
            "output":[{
                "type":"function_call",
                "call_id":format!("outcome-submit-{}",responses.len()/2),
                "name":"submit_result",
                "arguments":serde_json::to_string(&output).unwrap(),
            }]
        }));
    }
    daemon.model = ModelClientAdapter::new(ModelClient::fixture_sequence(responses));
}

fn assert_complete_narrative(
    daemon: &Daemon,
    task: &ClaimedAttempt,
    schedule: &OutcomeSchedule,
    horizon: OutcomeHorizon,
) {
    let artifact = daemon
        .store
        .retrospective_for(&task.run_id, &schedule.outcome_id, horizon)
        .unwrap()
        .unwrap();
    let retrospective: Retrospective =
        serde_json::from_slice(&daemon.store.read_blob(&artifact.blob).unwrap()).unwrap();
    assert_eq!(
        retrospective.status,
        RetrospectiveStatus::Complete,
        "the actual Outcome entry point must preserve and submit the model narrative: {retrospective:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outcome_worker_uses_frozen_node_for_every_narrative_stage() {
    let (mut daemon, id, schedule_artifact, mut task) = scheduled().await;
    let schedule: OutcomeSchedule =
        serde_json::from_slice(&daemon.store.read_blob(&schedule_artifact.blob).unwrap()).unwrap();
    install_bars(&mut daemon, Utc::now());
    install_narratives(
        &mut daemon,
        &schedule,
        &schedule_artifact,
        &OutcomeHorizon::ALL,
    );

    for horizon in OutcomeHorizon::ALL {
        let completion = daemon
            .execute_outcome_worker(&task, Utc::now())
            .await
            .unwrap();
        assert_complete_narrative(&daemon, &task, &schedule, horizon);
        if horizon == OutcomeHorizon::T5 {
            assert!(matches!(completion, TaskCompletion::Committed));
        } else {
            assert!(matches!(completion, TaskCompletion::DeferredUntil(_)));
            let ready_at = Utc::now() + Duration::seconds(1);
            daemon
                .store
                .defer_task(&task.permit, ready_at, Utc::now())
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
            task = claim_outcome(&daemon);
        }
    }
    let outcome = daemon.store.outcome_for_run(&id).unwrap().unwrap();
    let outcome: Outcome =
        serde_json::from_slice(&daemon.store.read_blob(&outcome.blob).unwrap()).unwrap();
    outcome.validate_sealed().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn narrative_repair_uses_the_frozen_repair_node() {
    let (mut daemon, id, schedule_artifact, task) = scheduled().await;
    let schedule: OutcomeSchedule =
        serde_json::from_slice(&daemon.store.read_blob(&schedule_artifact.blob).unwrap()).unwrap();
    let now = Utc::now();
    let market = artifact(
        &daemon,
        &task.permit,
        ArtifactKind::NormalizedEvidence,
        ArtifactLifecycle::RunScoped,
        &serde_json::json!({"fixture":"offline outcome windows"}),
        vec![],
    );
    daemon
        .store
        .write_task_artifact(
            &task.permit,
            &market,
            LifecycleEventType::OutcomeEvidence,
            now,
        )
        .unwrap();
    let outcome = artifact(
        &daemon,
        &task.permit,
        ArtifactKind::Outcome,
        ArtifactLifecycle::Canonical,
        &Outcome {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: schedule.outcome_id.clone(),
            schedule: reference(&schedule_artifact),
            market_evidence: vec![reference(&market)],
            metric_basis: Some(akzio_domain::OutcomeMetricBasis::FrozenPostExecutionExposureV3),
            windows: OutcomeHorizon::ALL
                .into_iter()
                .map(|horizon| {
                    serde_json::from_value::<OutcomeWindow>(serde_json::json!({
                        "horizon":horizon,
                        "observed_trading_day":match horizon {
                            OutcomeHorizon::T1=>"2026-10-12",
                            OutcomeHorizon::T3=>"2026-10-14",
                            OutcomeHorizon::T5=>"2026-10-16",
                        },
                        "portfolio_return_ppm":0,
                        "benchmark_return_ppm":1000,
                        "transaction_cost_ppm":0,
                        "slippage_ppm":0,
                        "utility_ppm":-1000,
                        "calibration_ppm":null,
                        "evidence_completeness_ppm":null,
                        "risk_recall_ppm":null,
                    }))
                    .unwrap()
                })
                .collect(),
            sealed_at: Some(now),
        },
        vec![reference(&schedule_artifact), reference(&market)],
    );
    let retrospective = artifact(
        &daemon,
        &task.permit,
        ArtifactKind::Retrospective,
        ArtifactLifecycle::Canonical,
        &Retrospective {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: schedule.outcome_id.clone(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "Offline numeric seal without a model narrative.".into(),
            findings: vec![],
            counterfactuals: vec![],
            lesson_candidates: vec![],
            lesson_proposals: vec![],
            diagnostic_gaps: vec!["model unavailable before repair".into()],
            source_refs: vec![reference(&outcome)],
            outcome: reference(&outcome),
            created_at: now,
            sealed_at: Some(now),
        },
        vec![reference(&outcome)],
    );
    let lease = daemon
        .store
        .acquire_daemon_lease(
            "outcome.regression.seal",
            "offline",
            now,
            now + Duration::minutes(5),
        )
        .unwrap()
        .unwrap();
    daemon
        .store
        .write_outcome_retrospective_fenced(
            &lease,
            &task.permit,
            &outcome,
            &retrospective,
            now,
            true,
        )
        .unwrap();
    let repair_id = daemon
        .store
        .request_outcome_narrative_repair(&id, Utc::now())
        .unwrap();
    let repair = claim_outcome(&daemon);
    assert_eq!(repair.permit.task_id, repair_id);
    install_narratives(
        &mut daemon,
        &schedule,
        &schedule_artifact,
        &[OutcomeHorizon::T5],
    );

    let completion = daemon
        .execute_outcome_worker(&repair, Utc::now())
        .await
        .unwrap();
    assert!(matches!(completion, TaskCompletion::Committed));
    assert_complete_narrative(&daemon, &repair, &schedule, OutcomeHorizon::T5);
}
