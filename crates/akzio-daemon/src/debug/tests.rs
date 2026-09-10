use std::future::IntoFuture;

use super::*;
use akzio_domain::{DebugAction, DebugStatus};

fn daemon(debug: bool) -> Daemon {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug-api-tests")
        .join(RunId::new().0);
    Daemon::with_model(
        DaemonConfig {
            agent_budget: Default::default(),
            debug_control: debug.then(|| DebugCoreConfig {
                code_revision: "test".into(),
                runtime_identity: ContentHash::of_bytes(b"test"),
                decision_policy_status: "unconfigured".into(),
                decision_policy_input_hash: None,
            }),
            outcome_processing: true,
            store_root: root,
            http_token: "test-native-token".into(),
            worker_count: 8,
            auto_paper: false,
            market_data_feed: Some(AlpacaMarketDataFeed::Sip),
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: 0,
                slippage_ppm: 0,
            },
            decision_policy: DecisionPolicy::default(),
            runtime_identity_hash: None,
            historical_evaluation_condition: None,
            model_knowledge_cutoff: None,
        },
        fixture_model_client(),
    )
    .unwrap()
}

fn step(d: &Daemon, run: &RunId, task: &TaskId) {
    let session = d.store.debug_session(run).unwrap().unwrap();
    d.control_debug(
        run,
        &DebugControlRequest {
            action: DebugAction::Step,
            expected_revision: session.revision,
            task_id: Some(task.clone()),
        },
    )
    .unwrap();
}

#[test]
fn formal_prepare_reuses_approved_topology_and_forty_needs() {
    let d = daemon(true);
    let request = DebugPrepareRequest {
        purpose: RunPurpose::Paper,
        session_key: "2026-09-09".into(),
        paper_allowed: false,
        fixture_controller: false,
    };
    let session = d.prepare_debug(&request).unwrap();
    assert_eq!(session.identity.run_purpose, RunPurpose::Paper);
    assert_eq!(session.status, DebugStatus::Paused);
    assert_eq!(session.identity.dataset.len(), 40);
    assert!(matches!(
        d.store.assert_debug_broker_write(&session.identity.run_id),
        Err(StoreError::DebugBrokerWriteForbidden)
    ));
    let view = d
        .inspect_debug(&session.identity.run_id, None, None)
        .unwrap();
    assert_eq!(
        view.nodes
            .iter()
            .filter(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
            .count(),
        3
    );
    assert_eq!(
        view.nodes
            .iter()
            .filter(|n| n.role.contains("critic"))
            .count(),
        3
    );
    assert!(!view.nodes.iter().any(|n| n.role.contains("planner")));
    assert!(view
        .nodes
        .iter()
        .any(|n| n.role == akzio_runtime::PAPER_COMMIT_RECIPE_ID));
    for horizon in ["t1", "t3", "t5"] {
        assert_eq!(
            view.nodes
                .iter()
                .filter(|n| n.horizon.as_deref() == Some(horizon))
                .count(),
            2
        );
    }
    assert_eq!(
        d.prepare_debug(&request).unwrap().identity.run_id,
        session.identity.run_id
    );
    assert!(view.nodes.iter().all(|n| n.attempts.is_empty()));
}

#[test]
fn new_experiment_does_not_require_fabricating_a_successful_parent() {
    let d = daemon(true);
    let parent = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let fork = d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
                read_range_probe: false,
                experiment_id: RunId::new(),
                reason: "new model routing experiment".into(),
            },
        )
        .unwrap();
    assert_eq!(fork.identity.parent_task_id, None);
    assert_eq!(
        fork.identity.parent_artifacts[0].kind,
        ArtifactKind::WorkflowGraph
    );
    assert_eq!(fork.identity.dataset.len(), 34);
    assert_eq!(fork.identity.run_purpose, RunPurpose::PositionPlan);
    d.store.verify_integrity().unwrap();
    assert!(d
        .inspect_debug(&parent.identity.run_id, None, None)
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.attempts.is_empty()));
}

#[tokio::test]
async fn t09_successful_fork_keeps_parent_outputs_and_new_lineage() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "fixture".into(),
            paper_allowed: false,
            fixture_controller: true,
        })
        .unwrap();
    let run = session.identity.run_id;
    let task = d
        .inspect_debug(&run, None, None)
        .unwrap()
        .nodes
        .into_iter()
        .find(|n| n.step_eligible)
        .unwrap()
        .task
        .node
        .task_id;
    step(&d, &run, &task);
    assert!(d.run_one("fixture-worker").await.unwrap());
    let before = d.store.current_succeeded_attempt(&run, &task).unwrap();
    let request = DebugForkRequest {
        task_id: Some(task.clone()),
        read_range_probe: false,
        experiment_id: RunId::new(),
        reason: "different experiment".into(),
    };
    let fork = d.fork_debug(&run, &request).unwrap();
    assert_ne!(fork.identity.run_id, run);
    assert_eq!(fork.identity.parent_run_id, Some(run.clone()));
    assert!(!fork.identity.parent_artifacts.is_empty());
    assert_eq!(
        d.fork_debug(&run, &request).unwrap().identity.run_id,
        fork.identity.run_id
    );
    assert_eq!(
        d.store
            .current_succeeded_attempt(&run, &task)
            .unwrap()
            .outputs,
        before.outputs
    );
    assert!(d
        .inspect_debug(&fork.identity.run_id, None, None)
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.attempts.is_empty()));
    assert_eq!(
        fork.identity.broker_write_policy,
        DebugBrokerPolicy::Forbidden
    );
    d.store.verify_integrity().unwrap();
}

#[tokio::test]
async fn t14_http_control_inspect_acceptance_match_store_and_reject_browser() {
    let d = daemon(true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, done) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        axum::serve(listener, d.router())
            .with_graceful_shutdown(async {
                let _ = done.await;
            })
            .into_future(),
    );
    let client = reqwest::Client::new();
    let url = format!("{base}/v1/debug/runs");
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let request = DebugPrepareRequest {
        purpose: RunPurpose::Paper,
        session_key: "fixture".into(),
        paper_allowed: false,
        fixture_controller: true,
    };
    assert_eq!(
        client
            .post(&url)
            .header("x-akzio-token", "test-native-token")
            .header("Origin", "http://evil.example")
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let session: DebugSession = client
        .post(&url)
        .header("x-akzio-token", "test-native-token")
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let run = &session.identity.run_id;
    let task = d
        .inspect_debug(run, None, None)
        .unwrap()
        .nodes
        .into_iter()
        .find(|n| n.step_eligible)
        .unwrap()
        .task
        .node
        .task_id;
    let control_url = format!("{url}/{run}/control");
    let step_request = DebugControlRequest {
        action: DebugAction::Step,
        expected_revision: 0,
        task_id: Some(task.clone()),
    };
    assert_eq!(
        client
            .post(&control_url)
            .header("x-akzio-token", "test-native-token")
            .json(&step_request)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .post(&control_url)
            .header("x-akzio-token", "test-native-token")
            .json(&step_request)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    assert!(d.run_one("test").await.unwrap());
    assert!(!d.run_one("sibling").await.unwrap());
    let value: serde_json::Value = client
        .get(format!("{url}/{run}"))
        .header("x-akzio-token", "test-native-token")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let store = d.inspect_debug(run, None, None).unwrap();
    assert_eq!(
        value["session"],
        serde_json::to_value(&store.session).unwrap()
    );
    assert_eq!(value["session"]["status"], "paused");
    assert!(!value["acceptance"].as_array().unwrap().is_empty());
    assert_eq!(
        store.nodes.iter().map(|n| n.attempts.len()).sum::<usize>(),
        1
    );
    let resume = DebugControlRequest {
        action: DebugAction::Resume,
        expected_revision: store.session.revision,
        task_id: None,
    };
    assert_eq!(
        client
            .post(&control_url)
            .header("x-akzio-token", "test-native-token")
            .json(&resume)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        d.store.debug_session(run).unwrap().unwrap().status,
        DebugStatus::Running
    );
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn production_observer_cannot_prepare_debug() {
    let d = daemon(false);
    assert!(d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false
        })
        .is_err());
    assert!(d.store.debug_environment().unwrap().is_none());
}

#[tokio::test]
async fn observer_and_run_sse_close_on_graceful_shutdown_without_changing_control() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, done) = watch::channel(false);
    let server_daemon = d.clone();
    let server =
        tokio::spawn(async move { server_daemon.serve_http_listener(listener, done).await });
    let client = reqwest::Client::new();
    let mut observer = client
        .get(format!("{base}/v1/observer/events"))
        .header("x-akzio-token", "test-native-token")
        .send()
        .await
        .unwrap();
    let mut run = client
        .get(format!("{base}/runs/{}/events", session.identity.run_id.0))
        .header("x-akzio-token", "test-native-token")
        .send()
        .await
        .unwrap();
    assert!(observer.chunk().await.unwrap().is_some());
    assert!(run.chunk().await.unwrap().is_some());
    stop.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("SSE must not keep graceful HTTP shutdown alive")
        .unwrap()
        .unwrap();
    assert_eq!(
        d.store
            .debug_session(&session.identity.run_id)
            .unwrap()
            .unwrap(),
        session
    );
    assert!(d
        .inspect_debug(&session.identity.run_id, None, None)
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.attempts.is_empty()));
}

#[tokio::test]
async fn research_gate_defers_all_execution_safety_and_readies_analysts() {
    let mut d = daemon(true);
    let mut invalid =
        debug_fixture_evidence(EvidenceSource::Alpaca, PAPER_QUOTES_RESOURCE, Utc::now());
    invalid.normalized["quotes"]["SOXX"]["bp"] = serde_json::json!(514.05);
    invalid.normalized["quotes"]["SOXX"]["ap"] = serde_json::json!(0);
    invalid.raw = serde_json::to_vec(&invalid.normalized).unwrap();
    Arc::make_mut(&mut d.fixture_evidence)
        .entry(EvidenceSource::Alpaca)
        .or_default()
        .insert(PAPER_QUOTES_RESOURCE.into(), invalid);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let run = session.identity.run_id;
    let task = d
        .inspect_debug(&run, None, None)
        .unwrap()
        .nodes
        .into_iter()
        .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap()
        .task
        .node
        .task_id;
    step(&d, &run, &task);
    d.run_one("research-boundary").await.unwrap();
    let succeeded = d.store.current_succeeded_attempt(&run, &task).unwrap();
    let status = succeeded
        .outputs
        .iter()
        .find(|a| a.producer == "evidence.collection_status")
        .unwrap();
    let payload: serde_json::Value =
        serde_json::from_slice(&d.store.read_blob(&status.blob).unwrap()).unwrap();
    let rows = payload["requirements"].as_array().unwrap();
    let execution = rows
        .iter()
        .filter(|r| r["criticality"] == "execution_safety")
        .collect::<Vec<_>>();
    assert_eq!(execution.len(), 6);
    assert!(
        execution
            .iter()
            .all(|r| r["status"] == "deferred_to_execution"),
        "{execution:?}"
    );
    assert!(!succeeded
        .outputs
        .iter()
        .any(|a| a.producer.starts_with("execution.snapshot.")));
    assert_eq!(
        d.inspect_debug(&run, None, None)
            .unwrap()
            .nodes
            .iter()
            .filter(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID && n.step_eligible)
            .count(),
        3
    );
}

#[tokio::test]
async fn position_plan_shares_research_contracts_and_has_no_execution_requirements() {
    let d = daemon(true);
    let paper = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let plan = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    assert_eq!(plan.identity.dataset.len(), 34);
    assert!(d
        .store
        .session_slot_for_run(&plan.identity.run_id)
        .unwrap()
        .is_none());
    let paper_view = d.inspect_debug(&paper.identity.run_id, None, None).unwrap();
    let plan_view = d.inspect_debug(&plan.identity.run_id, None, None).unwrap();
    for node in plan_view
        .nodes
        .iter()
        .filter(|n| n.role.starts_with("research."))
    {
        let other = paper_view
            .nodes
            .iter()
            .find(|n| n.role == node.role && n.horizon == node.horizon)
            .unwrap();
        assert_eq!(node.task.node.contract_hash, other.task.node.contract_hash);
        assert_eq!(node.task.node.objective, other.task.node.objective);
        assert_eq!(node.task.node.budget, other.task.node.budget);
        assert_eq!(
            node.task.node.dependencies.len(),
            other.task.node.dependencies.len()
        );
    }
    assert_eq!(plan_view.nodes.len(), 9);
    assert!(!plan_view.nodes.iter().any(|n| [
        "gate.execution",
        "gate.paper",
        "gate.reconcile",
        "gate.evaluate",
        "research.planner"
    ]
    .contains(&n.role.as_str())));
    for reference in &plan.identity.dataset {
        let need: EvidenceNeed = d.read_artifact_payload(reference).unwrap();
        assert_ne!(
            need.criticality(),
            akzio_domain::EvidenceCriticality::ExecutionSafety
        );
    }
    let gate = plan_view
        .nodes
        .iter()
        .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap();
    step(&d, &plan.identity.run_id, &gate.task.node.task_id);
    d.run_one("plan-boundary").await.unwrap();
    d.store
        .current_succeeded_attempt(&plan.identity.run_id, &gate.task.node.task_id)
        .unwrap();
    assert_eq!(
        d.inspect_debug(&plan.identity.run_id, None, None)
            .unwrap()
            .nodes
            .iter()
            .filter(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID && n.step_eligible)
            .count(),
        3
    );
    assert!(d
        .store
        .assert_debug_broker_write(&plan.identity.run_id)
        .is_err());
    d.store.verify_integrity().unwrap();
}

struct ExecutionProbe {
    calls: Arc<std::sync::Mutex<Vec<String>>>,
    invalid_quote: bool,
    future_time: bool,
}
impl AsyncEvidenceAdapter for ExecutionProbe {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }
    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> futures::future::BoxFuture<
        'a,
        std::result::Result<AcquiredEvidence, akzio_ingest::EvidenceAdapterError>,
    > {
        self.acquire_at(request, Utc::now())
    }
    fn acquire_at<'a>(
        &'a self,
        request: &'a EvidenceRequest,
        _cutoff: DateTime<Utc>,
    ) -> futures::future::BoxFuture<
        'a,
        std::result::Result<AcquiredEvidence, akzio_ingest::EvidenceAdapterError>,
    > {
        Box::pin(async move {
            self.calls.lock().unwrap().push(request.resource.clone());
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let cutoff = Utc::now()
                + if self.future_time {
                    Duration::hours(1)
                } else {
                    Duration::zero()
                };
            let mut evidence =
                debug_fixture_evidence(EvidenceSource::Alpaca, &request.resource, cutoff);
            if request.resource == PAPER_QUOTES_RESOURCE && self.invalid_quote {
                evidence.normalized["quotes"]["SOXX"]["bp"] = serde_json::json!(514.05);
                evidence.normalized["quotes"]["SOXX"]["ap"] = serde_json::json!(0);
                evidence.raw = serde_json::to_vec(&evidence.normalized).unwrap();
            }
            Ok(evidence)
        })
    }
}

#[tokio::test]
async fn execution_refresh_acquires_six_new_snapshots_and_rejects_zero_ask() {
    let mut d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let run = session.identity.run_id;
    // Exercise the refresh using an actual claimed, fenced Evidence permit. It
    // shares the scheduler needs; full ExecutionGate dispatch is tested separately.
    let view = d.inspect_debug(&run, None, None).unwrap();
    let gate = view
        .nodes
        .iter()
        .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap();
    step(&d, &run, &gate.task.node.task_id);
    let task = d
        .store
        .claim_next_task_for_workload_with_identity(
            "refresh-probe",
            Utc::now(),
            Duration::seconds(60),
            akzio_store::TaskWorkload::Session,
            Some(&session.identity.runtime_identity),
        )
        .unwrap()
        .unwrap();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    for (invalid_quote, future_time) in [(true, false), (false, false), (false, true)] {
        calls.lock().unwrap().clear();
        Arc::make_mut(&mut d.production_evidence).insert(
            EvidenceSource::Alpaca,
            Arc::new(ExecutionProbe {
                calls: calls.clone(),
                invalid_quote,
                future_time,
            }),
        );
        let result = d.refresh_execution_snapshots(&task, Utc::now()).await;
        if future_time {
            assert_eq!(calls.lock().unwrap().len(), 4);
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("evidence crosses the decision cutoff"));
        } else if invalid_quote {
            assert_eq!(calls.lock().unwrap().len(), 6);
            let refreshed = result.unwrap();
            assert!(refreshed.quotes.is_none());
            assert!(refreshed
                .quote_error
                .as_deref()
                .is_some_and(|error| error.contains("InvalidQuote")));
        } else {
            assert_eq!(calls.lock().unwrap().len(), 6);
            let refreshed = result.unwrap();
            assert!(
                refreshed.account.is_some()
                    && refreshed.quotes.is_some()
                    && refreshed.clock.is_some()
                    && refreshed.quote_error.is_none()
            );
        }
        assert!(d.store.assert_debug_broker_write(&run).is_err());
    }
}

#[tokio::test]
async fn research_deferral_does_not_admit_future_data() {
    for purpose in [RunPurpose::Paper, RunPurpose::PositionPlan] {
        let mut d = daemon(true);
        let resource = "bars:QQQ:1d:2025-08-05:252";
        let mut evidence = debug_fixture_evidence(EvidenceSource::Alpaca, resource, Utc::now());
        evidence.observed_at = Utc::now() + Duration::hours(1);
        evidence.provenance.observed_at = evidence.observed_at;
        evidence.normalized["bars"][0]["t"] =
            serde_json::json!((Utc::now() + Duration::days(1)).to_rfc3339());
        Arc::make_mut(&mut d.fixture_evidence)
            .entry(EvidenceSource::Alpaca)
            .or_default()
            .insert(resource.into(), evidence);
        let session = d
            .prepare_debug(&DebugPrepareRequest {
                purpose,
                session_key: "2026-09-09".into(),
                paper_allowed: false,
                fixture_controller: false,
            })
            .unwrap();
        let run = session.identity.run_id;
        let gate = d
            .inspect_debug(&run, None, None)
            .unwrap()
            .nodes
            .into_iter()
            .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .unwrap();
        step(&d, &run, &gate.task.node.task_id);
        d.run_one("future-boundary").await.unwrap();
        let view = d.inspect_debug(&run, None, None).unwrap();
        assert_eq!(
            view.nodes
                .iter()
                .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
                .unwrap()
                .task
                .status,
            akzio_domain::TaskStatus::Failed
        );
        assert!(!view
            .nodes
            .iter()
            .any(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID && n.step_eligible));
        let statuses = &view
            .artifacts
            .iter()
            .find(|a| a.artifact.producer == "evidence.collection_status")
            .unwrap()
            .payload;
        assert!(statuses["requirements"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["diagnostic"] == "temporal_contamination"));
    }
}

struct NeverCalledBroker;
impl akzio_execution::paper::CommittedPaperBroker for NeverCalledBroker {
    fn execute_commitment<'a>(
        &'a self,
        _: &'a akzio_domain::PaperCommitment,
        _: &'a akzio_execution::ExecutionPlan,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = akzio_execution::paper::Result<akzio_execution::paper::PaperExecution>,
                > + Send
                + 'a,
        >,
    > {
        panic!("broker write must not be reached")
    }
    fn reconcile_commitment<'a>(
        &'a self,
        _: &'a akzio_domain::PaperCommitment,
        _: &'a akzio_execution::paper::PaperExecution,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = akzio_execution::paper::Result<akzio_execution::paper::PaperExecution>,
                > + Send
                + 'a,
        >,
    > {
        panic!("broker must not be reached")
    }
    fn cancel_order<'a>(
        &'a self,
        _: &'a akzio_domain::PaperCancel,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = akzio_execution::paper::Result<
                        akzio_execution::paper::PaperOrderReceipt,
                    >,
                > + Send
                + 'a,
        >,
    > {
        panic!("broker cancel must not be reached")
    }
    fn replace_order<'a>(
        &'a self,
        _: &'a akzio_domain::PaperReprice,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = akzio_execution::paper::Result<
                        akzio_execution::paper::PaperOrderReceipt,
                    >,
                > + Send
                + 'a,
        >,
    > {
        panic!("broker replace must not be reached")
    }
}

#[tokio::test]
async fn position_plan_commit_and_dispatch_reject_and_paper_debug_forbids_broker() {
    for purpose in [RunPurpose::PositionPlan, RunPurpose::Paper] {
        let d = daemon(true);
        let session = d
            .prepare_debug(&DebugPrepareRequest {
                purpose,
                session_key: "2026-09-09".into(),
                paper_allowed: false,
                fixture_controller: false,
            })
            .unwrap();
        let run = session.identity.run_id;
        let gate = d
            .inspect_debug(&run, None, None)
            .unwrap()
            .nodes
            .into_iter()
            .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .unwrap();
        step(&d, &run, &gate.task.node.task_id);
        let task = d
            .store
            .claim_next_task_for_workload_with_identity(
                "malicious-direct-call",
                Utc::now(),
                Duration::seconds(60),
                akzio_store::TaskWorkload::Session,
                Some(&session.identity.runtime_identity),
            )
            .unwrap()
            .unwrap();
        let lease = d.paper.scheduler.active_lease(Utc::now()).unwrap();
        let reference = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"not-a-commitment")),
            kind: ArtifactKind::ExecutionCommitment,
        };
        if purpose == RunPurpose::PositionPlan {
            let error = d
                .paper_commitment_runtime
                .commit(&akzio_execution::PaperCommitmentInput {
                    lease: lease.clone(),
                    permit: task.permit.clone(),
                    verdict: reference.clone(),
                    session_key: "2026-09-09".into(),
                    now: Utc::now(),
                })
                .unwrap_err();
            assert!(matches!(
                error,
                akzio_execution::PaperCommitmentError::NonPaperRun(RunPurpose::PositionPlan)
            ));
        }
        let error = akzio_execution::PaperDispatchRuntime::new(d.store.clone())
            .dispatch(
                &NeverCalledBroker,
                &akzio_execution::PaperDispatchInput {
                    lease,
                    permit: task.permit,
                    commitment: reference,
                    now: Utc::now(),
                },
            )
            .await
            .unwrap_err();
        if purpose == RunPurpose::PositionPlan {
            assert!(matches!(
                error,
                akzio_execution::PaperDispatchError::NonPaperRun(RunPurpose::PositionPlan)
            ));
        } else {
            assert!(matches!(
                error,
                akzio_execution::PaperDispatchError::Store(StoreError::DebugBrokerWriteForbidden)
            ));
        }
    }
}

#[tokio::test]
async fn heartbeat_store_contention_keeps_handler_and_observer_progressing() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "heartbeat".into(),
            paper_allowed: false,
            fixture_controller: true,
        })
        .unwrap();
    let run = session.identity.run_id;
    let node = d
        .inspect_debug(&run, None, None)
        .unwrap()
        .nodes
        .into_iter()
        .find(|n| n.step_eligible)
        .unwrap();
    step(&d, &run, &node.task.node.task_id);
    let runtime = TaskRuntime::new(d.store.clone())
        .with_store_executor(d.store_executor.clone())
        .with_debug_identity(Some(session.identity.runtime_identity))
        .with_lease_duration(Duration::milliseconds(30))
        .unwrap();
    let executor = d.store_executor.clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runtime.run_one("contention", move |_| async move {
            for _ in 0..100 {
                executor
                    .execute(|_| std::thread::sleep(std::time::Duration::from_millis(2)))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
            TaskCompletion::Failed
        }),
    )
    .await
    .expect("heartbeat must not suspend the handler behind its own queued permit")
    .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        d.store_executor.execute(|_| ()),
    )
    .await
    .unwrap()
    .unwrap();
}

#[test]
fn position_plan_range_experiment_preserves_purpose_and_isolates_objective() {
    let d = daemon(true);
    let parent = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let fork = d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
                experiment_id: RunId::new(),
                reason: "range coverage".into(),
                read_range_probe: true,
            },
        )
        .unwrap();
    assert_eq!(fork.identity.run_purpose, RunPurpose::PositionPlan);
    assert_eq!(fork.identity.dataset.len(), 34);
    assert_eq!(
        fork.identity.broker_write_policy,
        DebugBrokerPolicy::Forbidden
    );
    let view = d.inspect_debug(&fork.identity.run_id, None, None).unwrap();
    assert!(!view
        .nodes
        .iter()
        .any(|n| n.task.node.recipe_id.as_str() == "gate.execution"));
    assert!(view
        .nodes
        .iter()
        .filter(|n| n.task.node.recipe_id.as_str() == "research.analyst")
        .all(
            |n| n.task.node.objective.contains("read_range exactly once")
                && n.task.node.budget.max_input_tokens == 1_000_000
        ));
    assert!(d
        .inspect_debug(&parent.identity.run_id, None, None)
        .unwrap()
        .nodes
        .iter()
        .all(|n| !n.task.node.objective.contains("read_range exactly once")));
    d.store.verify_integrity().unwrap();
}

#[test]
fn paper_experiment_cannot_bypass_session_reservation_by_becoming_debug() {
    let d = daemon(true);
    let parent = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    assert!(d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
                experiment_id: RunId::new(),
                reason: "paper experiment".into(),
                read_range_probe: false,
            }
        )
        .is_err());
    assert!(d
        .inspect_debug(&parent.identity.run_id, None, None)
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.attempts.is_empty()));
    d.store.verify_integrity().unwrap();
}

#[tokio::test]
async fn configured_budget_is_frozen_through_attempt_runtime_and_inspect() {
    let mut d = daemon(true);
    let mut budget = akzio_domain::AgentBudgetConfig::default();
    budget.analyst.max_input_tokens = Some(1_000_000);
    budget.outcome_worker.max_input_tokens = Some(128_000);
    d.workflow = d.workflow.clone().with_agent_budgets(&budget).unwrap();
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
            fixture_controller: false,
        })
        .unwrap();
    let run = session.identity.run_id;
    let before = d.inspect_debug(&run, None, None).unwrap();
    let analyst = before
        .nodes
        .iter()
        .find(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
        .unwrap()
        .task
        .node
        .clone();
    let critic = before
        .nodes
        .iter()
        .find(|n| n.role == akzio_domain::RESEARCH_CRITIC_RECIPE_ID)
        .unwrap();
    assert_eq!(analyst.budget.max_input_tokens, 1_000_000);
    assert_eq!(critic.task.node.budget.max_input_tokens, 1_000_000);
    let gate = before
        .nodes
        .iter()
        .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap()
        .task
        .node
        .task_id
        .clone();
    step(&d, &run, &gate);
    assert!(d.run_one("budget-evidence").await.unwrap());

    // Simulate a reloaded config after Run creation, before its first Analyst Attempt.
    budget.analyst.max_input_tokens = Some(10);
    budget.analyst.max_tool_calls = Some(akzio_domain::budget::ToolCallLimit::Limited(0));
    d.workflow = d.workflow.clone().with_agent_budgets(&budget).unwrap();
    let recovered = d.workflow.recover(&run).unwrap();
    assert_eq!(
        recovered.revision.graph.agent_budgets["research.analyst"].max_input_tokens,
        1_000_000
    );
    assert_eq!(
        recovered.revision.graph.agent_budgets["learning.outcome_worker"].max_input_tokens,
        128_000
    );
    step(&d, &run, &analyst.task_id);
    assert!(d.run_one("budget-analyst").await.unwrap());
    budget.analyst.max_input_tokens = Some(256_000);
    d.workflow = d.workflow.clone().with_agent_budgets(&budget).unwrap();
    assert_eq!(
        d.workflow
            .recover(&run)
            .unwrap()
            .revision
            .graph
            .agent_budgets["research.analyst"]
            .max_input_tokens,
        1_000_000
    );
    let view = d.inspect_debug(&run, Some(&analyst.task_id), None).unwrap();
    let node = &view.nodes[0];
    assert_eq!(node.task.node.budget, analyst.budget);
    assert_eq!(node.budget["resolved"]["max_input_tokens"], 1_000_000);
    assert_eq!(node.budget["resolved"]["max_tool_calls"], "unlimited");
    let observation = &node.budget["last_runtime_observation"];
    assert_eq!(
        observation["resolved"]["max_input_tokens"], 1_000_000,
        "{}",
        node.budget
    );
    assert_eq!(observation["resolved"]["max_tool_calls"], "unlimited");
    assert!(observation["tool_calls_remaining"].is_null());
    assert!(observation["input_tokens_used"].as_u64().unwrap() > 0);
    assert_eq!(
        observation["input_tokens_used"].as_u64().unwrap()
            + observation["input_tokens_remaining"].as_u64().unwrap(),
        1_000_000
    );
    let attempt_id = node.budget["attempt"].as_str().unwrap();
    let attempt_view = d
        .inspect_debug(
            &run,
            Some(&analyst.task_id),
            Some(&akzio_domain::AttemptId(attempt_id.to_owned())),
        )
        .unwrap();
    assert_eq!(attempt_view.nodes[0].budget, node.budget);

    // Durable AgentTurn provenance carries the same resolved budget and rendered Prompt.
    let turns = &view.events;
    let mut saw_provenance = false;
    for event in turns {
        if event.task_id.as_ref() != Some(&analyst.task_id) {
            continue;
        }
        let Some(id) = &event.artifact_id else {
            continue;
        };
        let artifact = d.store.artifact(id).unwrap();
        if artifact.kind != ArtifactKind::AgentTurn {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_slice(&d.store.read_blob(&artifact.blob).unwrap()).unwrap();
        if value.get("request").is_some() {
            assert_eq!(value["resolved_budget"]["max_input_tokens"], 1_000_000);
            assert!(value["request"]["prompt"]
                .as_str()
                .unwrap()
                .contains("1000000"));
            saw_provenance = true;
        }
    }
    assert!(saw_provenance);
    d.store.verify_integrity().unwrap();
}
