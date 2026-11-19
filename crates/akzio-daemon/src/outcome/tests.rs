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
            startup_decision_policy_hash: None,
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
    let before_seal = daemon.observer_run_detail(&id).unwrap().operator_report;
    assert_eq!(before_seal["execution"]["status"], "no_order");
    assert_eq!(before_seal["execution"]["planned_order_count"], 0);
    assert_eq!(before_seal["execution"]["observed_receipt_count"], 0);
    assert!(!before_seal["execution"]["blockers"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(before_seal["outcome"].is_null());
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
    let outcome_artifact = daemon.store.outcome_for_run(&id).unwrap().unwrap();
    let outcome: Outcome =
        serde_json::from_slice(&daemon.store.read_blob(&outcome_artifact.blob).unwrap()).unwrap();
    outcome.validate_sealed().unwrap();
    let after_seal = daemon.observer_run_detail(&id).unwrap().operator_report;
    assert_eq!(
        after_seal["outcome"]["artifact"],
        serde_json::to_value(reference(&outcome_artifact)).unwrap()
    );
    assert_eq!(
        after_seal["outcome"]["schedule"],
        serde_json::to_value(reference(&schedule_artifact)).unwrap()
    );
    assert_eq!(
        after_seal["outcome"]["sealed_at"],
        serde_json::to_value(outcome.sealed_at).unwrap()
    );
    assert!(after_seal["outcome"]["horizons"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(OutcomeHorizon::T5)));
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

/// WHY THIS TEST EXISTS (Task #30): The Daemon must materialize a complete mixed Paper reconciliation from exact CAS, attributing cost and quantity only to actual fills.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn outcome_materializes_actual_fill_but_no_cost_for_proven_unsent_order() {
    use akzio_domain::{
        FactorExposure, OrderIntent, OrderReceipt, OrderReceiptState, OrderSide, PaperCommitment,
        PaperCommitmentId, PaperUnsubmittedReason, ReconciliationId, TargetPortfolio,
        UnsubmittedPaperOrder,
    };

    let (daemon, _, schedule_artifact, task) = scheduled().await;
    let mut schedule: OutcomeSchedule = daemon
        .read_artifact_payload(&reference(&schedule_artifact))
        .unwrap();
    let mut context: ExecutionContext = daemon
        .read_artifact_payload(&schedule.execution_context)
        .unwrap();
    let account_reference = context.account_snapshot.clone().unwrap();
    let quote_reference = context.quote_snapshot.clone().unwrap();
    let account: AccountSnapshot = daemon.read_artifact_payload(&account_reference).unwrap();
    let quotes: QuoteSnapshot = daemon.read_artifact_payload(&quote_reference).unwrap();
    let quote = &quotes.quotes[&Asset::Qqq];
    let price = MoneyMicros((quote.bid.0 + quote.ask.0) / 2);
    let now = Utc::now();
    let persist = |artifact: Artifact| {
        daemon
            .store
            .write_task_artifact(
                &task.permit,
                &artifact,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        reference(&artifact)
    };
    let target = TargetPortfolio::zeroed();
    let mut plan = akzio_domain::ExecutionPlan {
        schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
        decision_context: context.decision_context.clone(),
        account_snapshot: account_reference,
        quote_snapshot: quote_reference,
        market_clock_snapshot: context.market_clock_snapshot.clone().unwrap(),
        policy_hash: ContentHash::of_bytes(b"outcome policy fixture"),
        maximum_total_notional: MoneyMicros(price.0 * 2),
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        target,
        orders: [Asset::Qqq, Asset::Soxx]
            .into_iter()
            .map(|asset| OrderIntent {
                asset,
                side: OrderSide::Buy,
                notional: price,
                limit_price: price,
                extended_hours: false,
            })
            .collect(),
        gross_exposure_ppm: 0,
        net_exposure_ppm: 0,
        turnover_ppm: 0,
        broker_session: account.broker_session.clone(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"outcome plan fixture"),
    };
    plan.refresh_hash().unwrap();
    let plan_reference = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::ExecutionPlan,
        ArtifactLifecycle::Canonical,
        &plan,
        vec![
            context.decision_context.clone(),
            plan.account_snapshot.clone(),
            plan.quote_snapshot.clone(),
            plan.market_clock_snapshot.clone(),
        ],
    ));
    context.execution_plan = Some(plan_reference.clone());
    context.plan_hash = Some(plan.plan_hash.clone());
    schedule.execution_context = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::ExecutionContext,
        ArtifactLifecycle::Canonical,
        &context,
        vec![schedule.execution_context.clone(), plan_reference],
    ));
    let execution_verdict = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::ExecutionVerdict,
        ArtifactLifecycle::Canonical,
        &akzio_domain::ExecutionVerdict::Accepted {
            execution_context: schedule.execution_context.clone(),
        },
        vec![schedule.execution_context.clone()],
    ));
    let commitment = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::ExecutionCommitment,
        ArtifactLifecycle::Canonical,
        &PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: schedule.execution_context.clone(),
            plan_hash: plan.plan_hash.clone(),
            broker_session: account.broker_session,
            client_order_ids: [
                (Asset::Qqq, "offline-first".into()),
                (Asset::Soxx, "offline-unsent".into()),
            ]
            .into(),
            created_at: now,
        },
        vec![
            schedule.execution_context.clone(),
            execution_verdict.clone(),
        ],
    ));
    let receipt = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::OrderReceipt,
        ArtifactLifecycle::Canonical,
        &OrderReceipt {
            plan_hash: plan.plan_hash,
            asset: Asset::Qqq,
            client_order_id: "offline-first".into(),
            broker_order_id: "offline-order".into(),
            state: OrderReceiptState::Filled,
            requested_quantity_micros: 1_000_000,
            filled_quantity_micros: 1_000_000,
            remaining_quantity_micros: 0,
            average_fill_price: Some(price),
            broker_updated_at: now,
            reason: None,
            observed_at: now,
        },
        vec![commitment.clone()],
    ));
    let reconciliation = persist(artifact(
        &daemon,
        &task.permit,
        ArtifactKind::Reconciliation,
        ArtifactLifecycle::Canonical,
        &Reconciliation {
            reconciliation_id: ReconciliationId::new(),
            commitment: commitment.clone(),
            state: ReconciliationState::Complete,
            broker_receipts: vec![receipt.clone()],
            unsubmitted_orders: vec![UnsubmittedPaperOrder {
                asset: Asset::Soxx,
                client_order_id: "offline-unsent".into(),
                transition_index: 1,
                reason: PaperUnsubmittedReason::RunCancelled,
                reprice: None,
            }],
            achieved_target: None,
            achieved_factor_exposure: None,
            reconciled_at: now,
        },
        vec![commitment.clone(), receipt],
    ));
    schedule.execution = OutcomeExecutionLineage::ReconciledPaper {
        execution_verdict: execution_verdict.clone(),
        commitment,
        reconciliation: reconciliation.clone(),
    };
    let realized = daemon.realized_execution(&schedule, &context).unwrap();
    let initial = |asset| {
        account
            .positions
            .get(&asset)
            .map_or(0, |position| position.quantity_micros)
    };
    assert_eq!(
        realized.quantities[&Asset::Qqq],
        initial(Asset::Qqq) + 1_000_000
    );
    assert_eq!(realized.quantities[&Asset::Soxx], initial(Asset::Soxx));
    assert_eq!(realized.metrics.order_cost_attributions.len(), 1);
    assert_eq!(
        realized.metrics.order_cost_attributions[0].asset,
        Asset::Qqq
    );
    let incorrect_commitment = ArtifactRef {
        artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"another commitment")),
        kind: ArtifactKind::ExecutionCommitment,
    };
    schedule.execution = OutcomeExecutionLineage::ReconciledPaper {
        execution_verdict,
        commitment: incorrect_commitment,
        reconciliation,
    };
    assert!(matches!(
        daemon.realized_execution(&schedule, &context),
        Err(DaemonError::InvalidInput(_))
    ));
}
