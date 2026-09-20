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
            research_settings: Default::default(),
            debug_control: debug.then(|| DebugCoreConfig {
                code_revision: "test".into(),
                runtime_identity: ContentHash::of_bytes(b"test"),
                decision_policy_status: "unconfigured".into(),
                decision_policy_input_hash: None,
                decision_policy_artifact: None,
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

fn assert_structured_research_calls(d: &Daemon, run: &RunId) {
    let target = d
        .store
        .root()
        .parent()
        .unwrap()
        .join(format!("protocol-{}", RunId::new()));
    d.store.export_debug_bundle(run, &target).unwrap();
    let acceptance: serde_json::Value =
        serde_json::from_slice(&std::fs::read(target.join("stage_acceptance.json")).unwrap())
            .unwrap();
    assert_eq!(acceptance["counts"]["NOT_REACHED"], 10);
    assert_eq!(acceptance["workflow_status_counts"]["skipped"], 10);
    assert!(acceptance["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|task| task["workflow_status"] == "skipped")
        .all(|task| task["acceptance_status"] == "NOT_REACHED"));
    let audit: serde_json::Value =
        serde_json::from_slice(&std::fs::read(target.join("research_review.json")).unwrap())
            .unwrap();
    let records = audit["records"].as_array().unwrap();
    let proposal = records
        .iter()
        .find(|r| r["kind"] == "decision_proposal")
        .unwrap();
    let review = records
        .iter()
        .find(|r| r["kind"] == "proposal_review")
        .unwrap();
    assert_eq!(proposal["proposal_revision"], 0);
    assert_eq!(review["proposal_revision"], 0);
    assert_eq!(
        review["payload"]["proposal"]["artifact_id"],
        proposal["artifact_id"]
    );
    let records = std::fs::read_to_string(target.join("llm_calls.jsonl")).unwrap();
    let calls = records
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 8);
    for call in calls {
        assert_eq!(call["phase"], "submit");
        assert_eq!(call["turn_id"], 0);
        let request = &call["domain_request"];
        assert_eq!(request["tools"], serde_json::json!([]));
        assert!(request["continuation"].is_null());
        assert!(!request["prompt"].as_str().unwrap().contains("Draft"));
        assert!(request["terminal"].is_object());
    }
}

#[tokio::test]
async fn position_plan_fixture_completes_bounded_nodes_without_execution() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: Utc::now().format("%Y-%m-%d").to_string(),
            paper_allowed: false,
        })
        .unwrap();
    let run = &session.identity.run_id;
    for _ in 0..21 {
        let view = d.inspect_debug(run, None, None).unwrap();
        let ready = view
            .nodes
            .iter()
            .find(|node| node.step_eligible)
            .unwrap_or_else(|| {
                panic!(
                    "no ready node: {:?}",
                    view.nodes
                        .iter()
                        .map(|n| (&n.role, &n.task.status, &n.blocked_reason))
                        .collect::<Vec<_>>()
                )
            });
        step(&d, run, &ready.task.node.task_id);
        assert!(d.run_one("position-plan-fixture").await.unwrap());
    }
    let view = d.inspect_debug(run, None, None).unwrap();
    assert_eq!(view.nodes.len(), 21);
    assert!(view.nodes.iter().all(|node| matches!(
        node.task.status,
        TaskStatus::Succeeded | TaskStatus::Skipped
    )));
    assert_eq!(
        session.identity.broker_write_policy,
        DebugBrokerPolicy::Forbidden
    );
    assert!(view.nodes.iter().all(|node| !matches!(
        node.role.as_str(),
        "gate.execution" | "paper.commit" | "paper.reconcile" | "learning.evaluate"
    )));
    assert_structured_research_calls(&d, run);
    d.store.verify_integrity().unwrap();
}

#[tokio::test]
async fn uncalibrated_position_plan_prepares_real_research_but_rejects_paper_permission() {
    let mut d = daemon(true);
    d.fixture_mode = false;
    d.debug_control.as_mut().unwrap().decision_policy_status = "store_active_head_missing".into();
    let mut request = DebugPrepareRequest {
        purpose: RunPurpose::PositionPlan,
        session_key: "2026-09-09".into(),
        paper_allowed: false,
    };
    let session = d.prepare_debug(&request).unwrap();
    assert_eq!(session.identity.llm_mode, DebugLlmMode::Real);
    assert_eq!(
        session.identity.broker_write_policy,
        DebugBrokerPolicy::Forbidden
    );
    assert!(session.identity.decision_policy_input_hash.is_none());
    let view = d
        .inspect_debug(&session.identity.run_id, None, None)
        .unwrap();
    let decision = view
        .nodes
        .iter()
        .find(|n| n.role == "gate.decision")
        .unwrap();
    assert!(!view.allowed_actions.iter().any(|action| action == "resume"));
    assert!(!decision.business_ready);
    assert!(!decision.step_eligible);
    assert!(decision
        .blocked_reason
        .as_deref()
        .unwrap()
        .contains("DecisionPolicy missing"));
    assert!(d
        .control_debug(
            &session.identity.run_id,
            &DebugControlRequest {
                action: DebugAction::Step,
                expected_revision: session.revision,
                task_id: Some(decision.task.node.task_id.clone()),
            }
        )
        .is_err());

    request.paper_allowed = true;
    assert!(d.prepare_debug(&request).is_err());
    request.paper_allowed = false;
    d.debug_control.as_mut().unwrap().decision_policy_status = "contract_mismatch".into();
    assert!(d.prepare_debug(&request).is_err());
}

#[tokio::test]
async fn formal_debug_replay_accepts_control_budget_and_terminal_acceptance_records() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
        })
        .unwrap();
    let run = &session.identity.run_id;
    d.workflow
        .replay_run(run)
        .expect("prepared Debug identity is a valid run-level event");
    let view = d.inspect_debug(run, None, None).unwrap();
    let gate = view
        .nodes
        .iter()
        .find(|n| n.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap();
    step(&d, run, &gate.task.node.task_id);
    assert!(d.run_one("replay-evidence").await.unwrap());
    d.workflow
        .replay_run(run)
        .expect("step and completion controls are replayable");
    let view = d.inspect_debug(run, None, None).unwrap();
    let analyst = view
        .nodes
        .iter()
        .find(|n| n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID && n.step_eligible)
        .unwrap();
    step(&d, run, &analyst.task.node.task_id);
    let task = d
        .store
        .claim_next_task_for_workload_with_identity(
            "replay-budget",
            Utc::now(),
            Duration::seconds(30),
            akzio_store::TaskWorkload::Any,
            Some(&session.identity.runtime_identity),
        )
        .unwrap()
        .unwrap();
    d.store
        .observe_debug_budget(
            &task.permit,
            &serde_json::json!({"input_tokens": 10}),
            Utc::now(),
        )
        .unwrap();
    d.store
        .append_task_event(
            &task.permit,
            akzio_domain::LifecycleEventType::SupplementalRoundAbandoned,
            Utc::now(),
        )
        .unwrap();
    d.store
        .finish_task(&task.permit, TaskStatus::Failed, Utc::now())
        .unwrap();
    d.store
        .record_stage_acceptance(&akzio_domain::StageAcceptance {
            version: 1,
            run_id: run.clone(),
            task_id: task.permit.task_id.clone(),
            attempt_id: task.permit.attempt_id.clone(),
            stage: "offline_replay".into(),
            business_result: "failed".into(),
            test_result: akzio_domain::AcceptanceResult::Pass,
            checks: vec![],
            created_at: Utc::now(),
        })
        .unwrap();
    d.workflow
        .replay_run(run)
        .expect("budget and post-terminal acceptance do not impersonate model outputs");
}

#[test]
fn formal_prepare_reuses_approved_topology_and_forty_needs() {
    let d = daemon(true);
    let request = DebugPrepareRequest {
        purpose: RunPurpose::Paper,
        session_key: "2026-09-09".into(),
        paper_allowed: false,
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
        6
    );
    assert_eq!(
        view.nodes
            .iter()
            .filter(|n| n.role.contains("critic"))
            .count(),
        6
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
            4
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
        })
        .unwrap();
    let fork = d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
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
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-22".into(),
            paper_allowed: false,
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
        session_key: "2026-09-22".into(),
        paper_allowed: false,
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
        })
        .unwrap();
    let plan = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
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
            .find(|n| n.role == node.role && n.task.node.spec == node.task.node.spec)
            .unwrap();
        assert_eq!(node.task.node.contract_hash, other.task.node.contract_hash);
        assert_eq!(node.task.node.objective, other.task.node.objective);
        assert_eq!(node.task.node.budget, other.task.node.budget);
        assert_eq!(
            node.task.node.dependencies.len(),
            other.task.node.dependencies.len()
        );
    }
    assert_eq!(plan_view.nodes.len(), 21);
    assert!(!plan_view.nodes.iter().any(|n| {
        [
            "gate.execution",
            "gate.paper",
            "gate.reconcile",
            "gate.evaluate",
            "research.planner",
        ]
        .contains(&n.role.as_str())
    }));
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
async fn uncalibrated_paper_run_reaches_no_order_and_outcome_schedule() {
    let mut d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
        })
        .unwrap();
    let run = session.identity.run_id;
    assert!(d.store.paper_approval_for_run(&run).unwrap().is_none());
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    // Only the execution refresh uses this existing offline adapter. Research
    // runs through the normal fixture model and governed evidence pipeline.
    for _ in 0..25 {
        let view = d.inspect_debug(&run, None, None).unwrap();
        let node = view
            .nodes
            .iter()
            .find(|node| node.step_eligible)
            .expect("the formal Paper graph must reach Evaluate");
        let role = node.role.clone();
        if role == "gate.execution" {
            Arc::make_mut(&mut d.production_evidence).insert(
                EvidenceSource::Alpaca,
                Arc::new(ExecutionProbe {
                    calls: calls.clone(),
                    invalid_quote: false,
                    future_time: false,
                }),
            );
        }
        step(&d, &run, &node.task.node.task_id);
        let executor = d.clone();
        assert!(d
            .task_runtime
            .run_one("cold-start", move |task| async move {
                let completion = executor.execute_task_inner(&task, Utc::now()).await.expect(
                    "missing approval must reach a durable NoOrder, not fail pre-trade safety",
                );
                if matches!(role.as_str(), "gate.paper" | "gate.reconcile") {
                    assert!(matches!(completion, TaskCompletion::NoOutput));
                }
                completion
            })
            .await
            .unwrap());
        let updated = d.inspect_debug(&run, None, None).unwrap();
        assert!(matches!(
            updated
                .nodes
                .iter()
                .find(|current| current.task.node.task_id == node.task.node.task_id)
                .unwrap()
                .task
                .status,
            TaskStatus::Succeeded | TaskStatus::Skipped
        ));
        if node.role == "gate.evaluate" {
            break;
        }
    }
    assert_eq!(calls.lock().unwrap().len(), 6);
    let schedule_artifact = d.store.outcome_schedule_for_run(&run).unwrap().unwrap();
    let schedule: OutcomeSchedule =
        serde_json::from_slice(&d.store.read_blob(&schedule_artifact.blob).unwrap()).unwrap();
    schedule.validate().unwrap();
    let OutcomeExecutionLineage::NoOrder { execution_verdict } = &schedule.execution else {
        panic!("uncalibrated Paper cannot accept execution");
    };
    let verdict: ExecutionVerdict = d.read_artifact_payload(execution_verdict).unwrap();
    let ExecutionVerdict::NoOrder { no_order, .. } = verdict else {
        panic!("expected NoOrder")
    };
    assert!(no_order
        .blockers
        .contains(&akzio_domain::HardBlocker::UnqualifiedRuntime));
    let context: ExecutionContext = d
        .read_artifact_payload(&schedule.execution_context)
        .unwrap();
    assert!(context.account_snapshot.is_some());
    assert!(context.quote_snapshot.is_some());
    assert!(context.market_clock_snapshot.is_some());
    assert!(context.execution_plan.is_none());
    assert!(context.pretrade_safety.is_none());
    let decision: Decision = d.read_artifact_payload(&schedule.decision).unwrap();
    assert!(decision
        .targets
        .weights
        .values()
        .all(|weight| weight.0 == 0));
    assert!(d.store.assert_debug_broker_write(&run).is_err());
    assert!(d
        .store
        .run_artifacts_by_kind(&run, ArtifactKind::ExecutionCommitment)
        .unwrap()
        .is_empty());
    assert_structured_research_calls(&d, &run);
    d.store.verify_integrity().unwrap();
}

struct CanonicalColdStartClock;
impl crate::scheduler::BrokerSessionClock for CanonicalColdStartClock {
    fn open_session_key<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = crate::scheduler::SchedulerResult<Option<String>>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(Some("2026-09-09".into())) })
    }
    fn paper_account_id<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = crate::scheduler::SchedulerResult<String>> + Send + 'a,
        >,
    > {
        Box::pin(async { panic!("cold start must not bind trading approval") })
    }
}

#[tokio::test]
async fn canonical_cold_start_reaches_no_order_outcome_without_debug_control() {
    let mut d = daemon(false);
    d.outcome_scheduling_runtime = d
        .outcome_scheduling_runtime
        .clone()
        .with_worker_enabled(true);
    let source = d.paper_workflow_source();
    let reservation = d
        .paper
        .scheduler
        .tick(&CanonicalColdStartClock, &source, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let run = reservation.slot.workflow.run.run_id;
    assert!(d.store.debug_session(&run).unwrap().is_none());
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    for _ in 0..25 {
        // EvidenceGate has completed before the first Decision appears. Inject
        // the existing test adapter only for the normal execution refresh.
        if !d
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::Decision)
            .unwrap()
            .is_empty()
        {
            Arc::make_mut(&mut d.production_evidence).insert(
                EvidenceSource::Alpaca,
                Arc::new(ExecutionProbe {
                    calls: calls.clone(),
                    invalid_quote: false,
                    future_time: false,
                }),
            );
        }
        let executor = d.clone();
        assert!(d
            .task_runtime
            .run_one("canonical-cold-start", move |task| async move {
                let result = executor
                    .execute_task_inner(&task, Utc::now())
                    .await
                    .unwrap();
                if matches!(
                    task.node.recipe_id.as_str(),
                    "gate.paper" | "gate.reconcile"
                ) {
                    assert!(matches!(result, TaskCompletion::NoOutput));
                }
                result
            })
            .await
            .unwrap());
        if d.store.outcome_schedule_for_run(&run).unwrap().is_some() {
            break;
        }
    }
    assert_eq!(calls.lock().unwrap().len(), 6);
    assert!(d.store.paper_approval_for_run(&run).unwrap().is_none());
    let artifact = d.store.outcome_schedule_for_run(&run).unwrap().unwrap();
    assert_eq!(artifact.lifecycle, ArtifactLifecycle::Canonical);
    let schedule: OutcomeSchedule =
        serde_json::from_slice(&d.store.read_blob(&artifact.blob).unwrap()).unwrap();
    schedule.validate().unwrap();
    let OutcomeExecutionLineage::NoOrder { execution_verdict } = &schedule.execution else {
        panic!("cold start cannot execute")
    };
    let ExecutionVerdict::NoOrder { no_order } = d
        .read_artifact_payload::<ExecutionVerdict>(execution_verdict)
        .unwrap()
    else {
        panic!("expected NoOrder")
    };
    assert!(no_order
        .blockers
        .contains(&akzio_domain::HardBlocker::UnqualifiedRuntime));
    let decision: Decision = d.read_artifact_payload(&schedule.decision).unwrap();
    assert!(decision
        .targets
        .weights
        .values()
        .all(|weight| weight.0 == 0));
    let execution: ExecutionContext = d
        .read_artifact_payload(&schedule.execution_context)
        .unwrap();
    assert!(execution.execution_plan.is_none());
    assert!(execution.quote_snapshot.is_some() && execution.account_snapshot.is_some());
    // An overnight Paper baseline belongs to the following trading date. No
    // post-baseline daily session can exist on the preceding New York date.
    let before_baseline = DateTime::parse_from_rfc3339("2026-09-09T03:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let executor = d.clone();
    let schedule_ref = ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: ArtifactKind::OutcomeSchedule,
    };
    assert!(d
        .task_runtime
        .clone()
        .with_outcome_processing(true)
        .run_one("overnight-outcome", move |task| async move {
            assert_eq!(task.node.recipe_id.as_str(), "learning.outcome_worker");
            let lease = executor
                .store
                .acquire_daemon_lease(
                    "test.overnight.outcome",
                    "test",
                    Utc::now(),
                    Utc::now() + Duration::minutes(5),
                )
                .unwrap()
                .unwrap();
            for observed_at in [before_baseline, before_baseline + Duration::days(1)] {
                let collected = executor
                    .collect_outcome_materialization(
                        &lease,
                        &task,
                        &schedule_ref,
                        &schedule,
                        observed_at,
                    )
                    .await
                    .expect(
                        "an immature overnight outcome must wait, not fail its evidence request",
                    );
                assert!(collected.is_none());
            }
            executor
                .store
                .release_daemon_lease(&lease, Utc::now())
                .unwrap();
            TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(20))
        })
        .await
        .unwrap());
    assert_eq!(
        calls.lock().unwrap().len(),
        6,
        "immature outcomes must not request future bars"
    );
    assert!(d
        .store
        .run_artifacts_by_kind(&run, ArtifactKind::ExecutionCommitment)
        .unwrap()
        .is_empty());
    d.store.verify_integrity().unwrap();
}

struct AccountComponentAgeProbe {
    stale_resource: Option<&'static str>,
    fresh_at: DateTime<Utc>,
    stale_at: DateTime<Utc>,
}

impl AsyncEvidenceAdapter for AccountComponentAgeProbe {
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
        Box::pin(async move {
            let observed_at = if self.stale_resource == Some(request.resource.as_str()) {
                self.stale_at
            } else {
                self.fresh_at
            };
            Ok(debug_fixture_evidence(
                EvidenceSource::Alpaca,
                &request.resource,
                observed_at,
            ))
        })
    }
}

#[tokio::test]
async fn account_aggregate_freshness_cannot_hide_an_older_component() {
    let mut d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::Paper,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
        })
        .unwrap();
    let run = session.identity.run_id;
    let gate = d
        .inspect_debug(&run, None, None)
        .unwrap()
        .nodes
        .into_iter()
        .find(|node| node.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
        .unwrap();
    step(&d, &run, &gate.task.node.task_id);
    let task = d
        .store
        .claim_next_task_for_workload_with_identity(
            "aggregate-freshness-probe",
            Utc::now(),
            Duration::seconds(60),
            akzio_store::TaskWorkload::Session,
            Some(&session.identity.runtime_identity),
        )
        .unwrap()
        .unwrap();
    for stale_resource in [
        None,
        Some(PAPER_ACCOUNT_RESOURCE),
        Some(PAPER_POSITIONS_RESOURCE),
        Some(PAPER_OPEN_ORDERS_RESOURCE),
        Some("paper.fills:2026-09-09"),
    ] {
        let fresh_at = Utc::now();
        // Accepted by the source Need (300s), but too old for the execution
        // policy's account freshness (5s). Every component can be the bottleneck.
        let stale_at = fresh_at - Duration::seconds(6);
        Arc::make_mut(&mut d.production_evidence).insert(
            EvidenceSource::Alpaca,
            Arc::new(AccountComponentAgeProbe {
                stale_resource,
                fresh_at,
                stale_at,
            }),
        );
        let refreshed = d
            .refresh_execution_snapshots(&task, fresh_at)
            .await
            .unwrap();
        let account_ref = refreshed.account.unwrap();
        let artifact = d.store.artifact(&account_ref.artifact_id).unwrap();
        let account: AccountSnapshot =
            serde_json::from_slice(&d.store.read_blob(&artifact.blob).unwrap()).unwrap();
        let expected = if stale_resource.is_some() {
            stale_at
        } else {
            fresh_at
        };
        assert_eq!(
            account.observed_at, expected,
            "latest data must not refresh older {stale_resource:?}"
        );
        assert_eq!(artifact.provenance.observed_at, Some(expected));
        assert_eq!(
            artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
                .count(),
            4,
            "all account components remain in provenance"
        );
        assert_eq!(
            fresh_at
                .signed_duration_since(account.observed_at)
                .num_seconds()
                <= ExecutionPolicy::default().max_account_age_secs,
            stale_resource.is_none()
        );
    }
    assert!(d.store.assert_debug_broker_write(&run).is_err());
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

async fn assert_research_identity_failure_is_fatal(fault: &str) {
    for purpose in [RunPurpose::Paper, RunPurpose::PositionPlan] {
        let mut d = daemon(true);
        let need = akzio_domain::paper_session_evidence_needs("2026-09-09")
            .into_iter()
            .find(|need| need.resource.starts_with("news:"))
            .unwrap();
        let mut evidence =
            debug_fixture_evidence(EvidenceSource::NewsWeb, &need.resource, Utc::now());
        evidence.normalized["source_document"] = serde_json::json!({
            "acquisition_mode": evidence_acquisition_mode(purpose, &need).as_str(),
            "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
        });
        match fault {
            "mode" => {
                evidence.normalized["source_document"]["acquisition_mode"] =
                    serde_json::json!("discovery_only")
            }
            "policy" => {
                evidence.normalized["source_document"]["acquisition_policy_hash"] =
                    serde_json::json!(ContentHash::of_bytes(b"different-acquisition-policy"))
            }
            "provenance" => evidence.provenance.source_uri = "https://unrelated.example/".into(),
            "bundle_hash" | "bundle_range" => {}
            _ => unreachable!(),
        }
        evidence.raw = serde_json::to_vec(&evidence.normalized).unwrap();
        if matches!(fault, "bundle_hash" | "bundle_range") {
            evidence.raw = b"independently acquired source body".to_vec();
            evidence.normalized["source_document"]["sources"] = serde_json::json!([{
                "status": "snapshot", "bundle_start_byte": 0,
                "bundle_end_byte": evidence.raw.len() + usize::from(fault == "bundle_range"),
                "content_hash": ContentHash::of_bytes(if fault == "bundle_hash" { b"different bytes" } else { &evidence.raw }),
                "media_type": "text/plain", "claim_bindings": [],
            }]);
        }
        Arc::make_mut(&mut d.fixture_evidence)
            .entry(EvidenceSource::NewsWeb)
            .or_default()
            .insert(need.resource, evidence);
        let session = d
            .prepare_debug(&DebugPrepareRequest {
                purpose,
                session_key: "2026-09-09".into(),
                paper_allowed: false,
            })
            .unwrap();
        let run = session.identity.run_id;
        let gate = d
            .inspect_debug(&run, None, None)
            .unwrap()
            .nodes
            .into_iter()
            .find(|node| node.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .unwrap();
        step(&d, &run, &gate.task.node.task_id);
        assert!(d.run_one("evidence-identity-failure").await.unwrap());
        let view = d.inspect_debug(&run, None, None).unwrap();
        assert_eq!(
            view.nodes
                .iter()
                .find(|node| node.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
                .unwrap()
                .task
                .status,
            TaskStatus::Failed,
            "{purpose:?} must not convert {fault} identity mismatch to an ordinary missing-source success"
        );
        assert!(!view.nodes.iter().any(|node| node.role
            == akzio_domain::RESEARCH_ANALYST_RECIPE_ID
            && node.step_eligible));
    }
}

#[tokio::test]
async fn research_acquisition_mode_mismatch_is_fatal() {
    assert_research_identity_failure_is_fatal("mode").await;
}

#[tokio::test]
async fn research_acquisition_policy_mismatch_is_fatal() {
    assert_research_identity_failure_is_fatal("policy").await;
}

#[tokio::test]
async fn research_acquisition_provenance_mismatch_is_fatal() {
    assert_research_identity_failure_is_fatal("provenance").await;
}

#[tokio::test]
async fn research_source_bundle_hash_mismatch_is_fatal() {
    assert_research_identity_failure_is_fatal("bundle_hash").await;
}

#[tokio::test]
async fn research_source_bundle_range_mismatch_is_fatal() {
    assert_research_identity_failure_is_fatal("bundle_range").await;
}

#[tokio::test]
async fn research_missing_or_stale_sources_remain_gaps_and_execution_stays_deferred() {
    for purpose in [RunPurpose::Paper, RunPurpose::PositionPlan] {
        for source_state in ["stale", "missing_adapter", "empty_content", "valid_bundle"] {
            let mut d = daemon(true);
            let need = akzio_domain::paper_session_evidence_needs("2026-09-09")
                .into_iter()
                .find(|need| need.resource.starts_with("news:"))
                .unwrap();
            let stale_at =
                Utc::now() - Duration::seconds(i64::try_from(need.max_age_secs).unwrap() + 60);
            let mut evidence = debug_fixture_evidence(
                EvidenceSource::NewsWeb,
                &need.resource,
                if source_state == "stale" {
                    stale_at
                } else {
                    Utc::now()
                },
            );
            if source_state == "empty_content" {
                evidence.raw.clear();
            }
            if source_state == "valid_bundle" {
                evidence.raw = b"independently acquired source body".to_vec();
                evidence.normalized["source_document"] = serde_json::json!({
                    "acquisition_mode": evidence_acquisition_mode(purpose, &need).as_str(),
                    "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
                    "sources": [{"status": "snapshot", "bundle_start_byte": 0,
                        "bundle_end_byte": evidence.raw.len(), "content_hash": ContentHash::of_bytes(&evidence.raw),
                        "media_type": "text/plain", "claim_bindings": []}],
                });
            }
            Arc::make_mut(&mut d.fixture_evidence)
                .entry(EvidenceSource::NewsWeb)
                .or_default()
                .insert(need.resource.clone(), evidence);
            let session = d
                .prepare_debug(&DebugPrepareRequest {
                    purpose,
                    session_key: "2026-09-09".into(),
                    paper_allowed: false,
                })
                .unwrap();
            let run = session.identity.run_id;
            // The injected daemon has no production adapters. Exercise the
            // normal missing-adapter branch without any external I/O.
            if source_state == "missing_adapter" {
                assert!(d.production_evidence.is_empty());
                d.fixture_mode = false;
            }
            let gate = d
                .inspect_debug(&run, None, None)
                .unwrap()
                .nodes
                .into_iter()
                .find(|node| node.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
                .unwrap();
            step(&d, &run, &gate.task.node.task_id);
            assert!(d.run_one("ordinary-source-gap").await.unwrap());
            let view = d.inspect_debug(&run, None, None).unwrap();
            assert_eq!(
                view.nodes
                    .iter()
                    .find(|node| node.role == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
                    .unwrap()
                    .task
                    .status,
                TaskStatus::Succeeded
            );
            assert!(view
                .nodes
                .iter()
                .any(|node| node.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID
                    && node.step_eligible));
            let status = &view
                .artifacts
                .iter()
                .find(|artifact| artifact.artifact.producer == "evidence.collection_status")
                .unwrap()
                .payload;
            let requirements = status["requirements"].as_array().unwrap();
            let news = requirements
                .iter()
                .find(|entry| entry["resource"] == need.resource)
                .unwrap();
            assert_eq!(
                news["status"],
                if source_state == "valid_bundle" {
                    "available"
                } else {
                    "unavailable"
                }
            );
            assert_eq!(
                news["diagnostic"],
                match source_state {
                    "missing_adapter" => "adapter_unavailable",
                    "stale" => "stale_content",
                    "empty_content" => "data_quality",
                    "valid_bundle" => "none",
                    _ => unreachable!(),
                }
            );
            assert_eq!(
                requirements
                    .iter()
                    .filter(|entry| entry["status"] == "deferred_to_execution")
                    .count(),
                if purpose == RunPurpose::Paper { 6 } else { 0 }
            );
        }
    }
}

struct NeverCalledBroker;
impl akzio_execution::paper::CommittedPaperBroker for NeverCalledBroker {
    fn execute_commitment<'a>(
        &'a self,
        _: &'a akzio_domain::PaperCommitment,
        _: &'a akzio_execution::ExecutionPlan,
        _: &'a akzio_execution::paper::PaperSubmissionAuthorization,
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
        _: &'a akzio_execution::paper::PaperSubmissionAuthorization,
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
            session_key: "2026-09-22".into(),
            paper_allowed: false,
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
        .with_lease_duration(Duration::milliseconds(500))
        .unwrap();
    // The handler spans at least two lease periods, so renewals are required.
    // Leave enough scheduling margin for the formal graph and parallel tests.
    let executor = d.store_executor.clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runtime.run_one("contention", move |_| async move {
            for _ in 0..100 {
                executor
                    .execute(|_| std::thread::sleep(std::time::Duration::from_millis(10)))
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
fn position_plan_experiment_preserves_structured_protocol() {
    let d = daemon(true);
    let parent = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: "2026-09-09".into(),
            paper_allowed: false,
        })
        .unwrap();
    let fork = d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
                experiment_id: RunId::new(),
                reason: "range coverage".into(),
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
            |n| !n.task.node.objective.contains("read_range exactly once")
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
        })
        .unwrap();
    assert!(d
        .fork_debug(
            &parent.identity.run_id,
            &DebugForkRequest {
                task_id: None,
                experiment_id: RunId::new(),
                reason: "paper experiment".into(),
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
        })
        .unwrap();
    let run = session.identity.run_id;
    let before = d.inspect_debug(&run, None, None).unwrap();
    let analyst = before
        .nodes
        .iter()
        .find(|n| {
            n.role == akzio_domain::RESEARCH_ANALYST_RECIPE_ID
                && n.task.node.execution_spec().research_round == Some(0)
        })
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

#[test]
fn removed_debug_fields_and_purposes_are_rejected() {
    for field in ["fixture_controller", "read_range_probe"] {
        let request = serde_json::json!({"session_key":"2026-09-22","purpose":"position_plan","paper_allowed":false,field:false});
        assert!(serde_json::from_value::<DebugPrepareRequest>(request).is_err());
    }
    let d = daemon(true);
    for purpose in [RunPurpose::PaperDryRun, RunPurpose::Debug] {
        assert!(d
            .prepare_debug(&DebugPrepareRequest {
                session_key: "2026-09-22".into(),
                purpose,
                paper_allowed: false
            })
            .is_err());
    }
}

#[path = "research_loop_tests.rs"]
mod bounded_research_tests;

#[path = "runtime_refactor_tests.rs"]
mod runtime_refactor_tests;

#[tokio::test]
async fn app_launch_uses_formal_position_plan_and_rejects_unauthorized_modes() {
    let d = daemon(false);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/runs", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, d.router()).into_future());
    let client = reqwest::Client::new();
    let request = serde_json::json!({"purpose":"position_plan"});
    assert_eq!(
        client
            .post(&url)
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&url)
            .header("x-akzio-token", "test-native-token")
            .header("Origin", "http://example.test")
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    for purpose in ["debug", "paper_dry_run", "replay", "paper"] {
        assert_eq!(
            client
                .post(&url)
                .header("x-akzio-token", "test-native-token")
                .json(&serde_json::json!({"purpose":purpose}))
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
    }
    let response = client
        .post(&url)
        .header("x-akzio-token", "test-native-token")
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let run = RunId(response["run_id"].as_str().unwrap().into());
    let workflow = d.workflow.replay_run(&run).unwrap();
    assert_eq!(workflow.run.purpose, RunPurpose::PositionPlan);
    assert!(!workflow.tasks.is_empty());
    assert!(workflow
        .tasks
        .iter()
        .all(|task| !task.node.recipe_id.as_str().contains("execution")
            && !task.node.recipe_id.as_str().contains("paper_commit")
            && !task.node.recipe_id.as_str().contains("reconcile")));
    assert!(d.store.debug_session(&run).unwrap().is_none());
    server.abort();
}

#[tokio::test]
async fn app_launch_cannot_promote_an_isolated_debug_core() {
    let d = daemon(true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/runs", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, d.router()).into_future());
    for purpose in ["position_plan", "paper"] {
        assert_eq!(
            reqwest::Client::new()
                .post(&url)
                .header("x-akzio-token", "test-native-token")
                .json(&serde_json::json!({"purpose":purpose}))
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
    }
    server.abort();
}
