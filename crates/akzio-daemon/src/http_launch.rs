// 文件导读：Native App 的 `/runs` 入口只复用正式 PositionPlan graph 或 Paper scheduler。
// 它拒绝浏览器来源和隔离 Debug Core；PositionPlan 在 Decision 后结束，Paper 只返回
// scheduler reservation 的 RunId。返回 RunId/HTTP 成功不等于模型完成、Decision 通过、
// Paper submission、fill 或 Outcome 成熟。
// Rust 机制：Axum `State/Json/HeaderMap` extractor 取得请求；闭包 move 进 StoreExecutor
// 取得原子 commit；`match` 穷举 RunPurpose，`Option` 表示 scheduler 尚未创建 slot。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunLaunchRequest {
    purpose: RunPurpose,
}

type RunHttpResult =
    std::result::Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)>;

fn run_http_error(error: DaemonError) -> (StatusCode, Json<serde_json::Value>) {
    let status = match error {
        DaemonError::Store(StoreError::MissingRun(_) | StoreError::MissingTask(_)) => {
            StatusCode::NOT_FOUND
        }
        DaemonError::RunLogUnavailable(_)
        | DaemonError::Scheduler(SchedulerError::RunLogUnavailable(_)) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::CONFLICT,
    };
    (status, Json(serde_json::json!({"error":error.to_string()})))
}

async fn http_launch_run(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<RunLaunchRequest>,
) -> RunHttpResult {
    // 认证和浏览器来源检查先于业务动作；PositionPlan 用 StoreExecutor 提交新图，
    // Paper 只向 scheduler 请求真实 session slot，二者均不会在 handler 内直接跑 worker。
    authorize(&daemon, &headers).map_err(|status| {
        (status, Json(serde_json::json!({"error":"authenticated_native_client_required"})))
    })?;
    if !native_client_headers(&headers) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"native_client_required"})),
        ));
    }
    let submission = daemon.submit_formal_run(request.purpose, "http").await
    .map_err(|error| match error {
        crate::launch::FormalRunLaunchError::Rejected(error) => run_http_error(error),
        crate::launch::FormalRunLaunchError::NotSubmitted { run_id, .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error":"not_submitted",
                "submission_status":"not_submitted",
                "run_id":run_id,
            })),
        ),
        crate::launch::FormalRunLaunchError::Unknown { run_id, .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error":"unknown_after_post",
                "submission_status":"unknown_after_post",
                "run_id":run_id,
            })),
        ),
    })?;
    // 保留已有 REST wire shape。
    Ok(Json(serde_json::json!({"run_id":submission.run_id})))
}

#[cfg(test)]
mod formal_launch_tests {
    use super::*;
    use crate::scheduler::SchedulerResult;
    use std::sync::Arc;

    struct OpenPaperSession;

    impl crate::BrokerSessionClock for OpenPaperSession {
        fn open_session_key<'a>(
            &'a self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SchedulerResult<Option<String>>> + Send + 'a>> {
            Box::pin(async {
                Ok(Some(
                    Utc::now()
                        .with_timezone(&chrono_tz::America::New_York)
                        .date_naive()
                        .to_string(),
                ))
            })
        }

        fn paper_account_id<'a>(
            &'a self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SchedulerResult<String>> + Send + 'a>> {
            Box::pin(async { panic!("uncalibrated Paper must not request an approval identity") })
        }
    }

    fn fixture() -> Arc<Daemon> {
        fixture_with_log(|_| {})
    }

    fn fixture_with_log(prepare_log: impl FnOnce(&crate::RunLogger)) -> Arc<Daemon> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-run-http-tests")
            .join(RunId::new().0);
        let run_log = crate::RunLogger::for_test(root.join("log"));
        prepare_log(&run_log);
        Arc::new(
            Daemon::with_model(
                DaemonConfig {
                    agent_budget: Default::default(),
                    research_settings: Default::default(),
                    run_log: Some(run_log),
                    outcome_processing: false,
                    store_root: root,
                    http_token: "test-formal-token".into(),
                    worker_count: 1,
                    auto_paper: false,
                    market_data_feed: Some(akzio_ingest::AlpacaMarketDataFeed::Sip),
                    outcome_cost_model: akzio_domain::OutcomeCostModel {
                        transaction_cost_ppm: 0,
                        slippage_ppm: 0,
                    },
                    decision_policy: akzio_execution::DecisionPolicy::default(),
                    runtime_identity_hash: None,
                    historical_evaluation_condition: None,
                    model_knowledge_cutoff: None,
                },
                crate::fixture_model_client(),
            )
            .unwrap(),
        )
    }

    struct NoClockCalls;

    impl crate::BrokerSessionClock for NoClockCalls {
        fn open_session_key<'a>(
            &'a self,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = crate::scheduler::SchedulerResult<Option<String>>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async { panic!("blocked logging cannot request a new Paper session") })
        }

        fn paper_account_id<'a>(
            &'a self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::scheduler::SchedulerResult<String>> + Send + 'a>,
        > {
            Box::pin(async { panic!("blocked logging cannot inspect approval account") })
        }
    }

    #[tokio::test]
    async fn an_unavailable_run_log_blocks_scheduler_before_paper_session_io() {
        let daemon = fixture();
        daemon
            .run_log
            .as_ref()
            .unwrap()
            .mark_unavailable("injected archive verification failure")
            .unwrap();
        let result = daemon
            .paper
            .scheduler
            .tick(&NoClockCalls, &daemon.paper_workflow_source(), Utc::now())
            .await;
        assert!(matches!(
            result,
            Err(crate::SchedulerError::RunLogUnavailable(_))
        ));
    }

    /// WHY THIS TEST EXISTS (Task #5): A Store read failure before sealing must block new formal work.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn archive_preparation_failure_blocks_readiness_and_formal_launch() {
        let run_id = RunId::new();
        let daemon = fixture_with_log(|logger| {
            // Stage an inflight log for a Run missing from the Store at archive time.
            let context = akzio_model::ModelAuditContext {
                run_id: run_id.0.clone(),
                phase: "T0".into(),
                task_id: None,
                attempt_id: None,
                call_id: None,
                retry_index: None,
            };
            logger
                .record(&context, "fixture.segment", serde_json::Value::Null)
                .unwrap();
        });
        assert!(daemon.ready().unwrap().run_log_ready);

        assert!(matches!(
            daemon.archive_ready_run_stages(),
            Err(DaemonError::Store(StoreError::MissingRun(missing))) if missing == run_id
        ));
        let readiness = daemon.ready().unwrap();
        assert_eq!(readiness.status, "run_log_blocked");
        assert!(!readiness.run_log_ready);
        assert!(matches!(
            daemon.submit_formal_run(RunPurpose::PositionPlan, "test").await,
            Err(crate::launch::FormalRunLaunchError::Rejected(DaemonError::RunLogUnavailable(_)))
        ));
    }

    #[tokio::test]
    async fn normal_position_plan_launch_and_share_safe_bundle_need_no_policy() {
        let daemon = fixture();
        let store = daemon.store.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, receiver) = watch::channel(false);
        let service = daemon.clone();
        let server = tokio::spawn(async move { service.serve_http_listener(listener, receiver).await });
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let readiness = client
            .get(format!("http://{address}/ready"))
            .header("x-akzio-token", "test-formal-token")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(readiness["store_scope"], "canonical");
        assert_eq!(readiness["formal_run_bundle_supported"], true);
        let retired = client
            .post(format!("http://{address}/v1/debug/runs"))
            .header("x-akzio-token", "test-formal-token")
            .json(&serde_json::json!({"purpose":"position_plan"}))
            .send()
            .await
            .unwrap();
        assert_eq!(retired.status(), reqwest::StatusCode::NOT_FOUND);
        let unauthenticated = client
            .post(format!("http://{address}/runs"))
            .json(&serde_json::json!({"purpose":"position_plan"}))
            .send()
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);
        let browser = client
            .post(format!("http://{address}/runs"))
            .header("x-akzio-token", "test-formal-token")
            .header("origin", "http://127.0.0.1")
            .json(&serde_json::json!({"purpose":"position_plan"}))
            .send()
            .await
            .unwrap();
        assert_eq!(browser.status(), reqwest::StatusCode::FORBIDDEN);
        let unconfigured_paper = client
            .post(format!("http://{address}/runs"))
            .header("x-akzio-token", "test-formal-token")
            .json(&serde_json::json!({"purpose":"paper"}))
            .send()
            .await
            .unwrap();
        assert_eq!(unconfigured_paper.status(), reqwest::StatusCode::CONFLICT);
        assert!(store.recent_workflows(1).unwrap().is_empty());
        let launch = client
            .post(format!("http://{address}/runs"))
            .header("x-akzio-token", "test-formal-token")
            .json(&serde_json::json!({"purpose":"position_plan"}))
            .send()
            .await
            .unwrap();
        assert_eq!(launch.status(), reqwest::StatusCode::OK);
        let run_id = launch.json::<serde_json::Value>().await.unwrap()["run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let run = RunId(run_id.clone());
        assert_eq!(store.run_purpose(&run).unwrap(), RunPurpose::PositionPlan);
        assert!(store.debug_environment().unwrap().is_none());
        assert!(store.debug_session(&run).unwrap().is_none());
        assert!(store.active_decision_policy().unwrap().is_none());
        for _ in 0..32 {
            if !daemon.run_one("formal-position-plan-fixture").await.unwrap() {
                break;
            }
        }
        let decisions = store
            .run_artifacts_by_kind(&run, ArtifactKind::Decision)
            .unwrap();
        assert_eq!(decisions.len(), 1, "the ordinary worker must reach Decision");
        let decision: akzio_domain::Decision =
            serde_json::from_slice(&store.read_blob(&decisions[0].blob).unwrap()).unwrap();
        assert_eq!(decision.targets, akzio_domain::TargetPortfolio::zeroed());
        decision
            .research_plan
            .as_ref()
            .expect("Rust-reviewed research allocation survives a zero target")
            .validated
            .validate()
            .unwrap();
        assert!(store.outcome_schedule_for_run(&run).unwrap().is_none());
        daemon.archive_ready_run_stages().unwrap();
        let logs = store.run_log_status(&run).unwrap();
        assert!(logs.iter().any(|entry| {
            entry["phase"] == "T0"
                && entry["status"] == "archived"
                && entry["detail"]["stage_status"] == "completed"
        }));
        let path = logs.last().unwrap()["detail"]["path"].as_str().unwrap();
        let mut zip = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
        assert!(zip.by_name("rust_flow.jsonl").is_ok());
        let inspection = store.inspect_run(&run).unwrap();
        assert_eq!(inspection.run_log_status.len(), logs.len());
        let output = store.root().parent().unwrap().join(format!("report-{}", RunId::new().0));
        let response = client
            .post(format!("http://{address}/control/store/export-run-bundle"))
            .header("x-akzio-token", "test-formal-token")
            .json(&serde_json::json!({"run_id":run_id,"target":output}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let manifest = response.json::<serde_json::Value>().await.unwrap();
        assert_eq!(manifest["purpose"], "position_plan");
        assert_eq!(manifest["exporter_version"], "run-bundle-v1");
        assert_eq!(manifest["raw_model_access"]["allowed"], false);
        assert_eq!(manifest["raw_model_access"]["requested"], false);
        assert_eq!(manifest["raw_model_access"]["reason"], "formal_run_share_safe_redaction");
        let artifact_index: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("artifact_index.json")).unwrap())
                .unwrap();
        assert!(artifact_index.as_array().unwrap().iter().any(|artifact| {
            artifact["kind"] == "raw_evidence" && artifact["status"] == "omitted"
        }));
        assert!(output.join("checksums.sha256").is_file());
        stop.send(true).unwrap();
        server.await.unwrap().unwrap();
    }

    /// WHY THIS TEST EXISTS (Task #12): Decision lineage must identify the exact approved revision even when its research inputs include a rejected review.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn decision_lineage_binds_the_review_that_approved_the_final_revision() {
        use akzio_domain::{
            Decision, ProposalReview, RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
            RESEARCH_SYNTHESIZER_RECIPE_ID,
        };
        use akzio_model::ModelClient;
        use serde_json::{json, Value};

        let mut daemon = (*fixture()).clone();
        let ModelClient::FixtureByPurposePhase(templates) = crate::fixture_model_client() else {
            panic!("expected purpose-scoped research fixture responses");
        };
        let mut first = (*templates).clone();
        let reviewer = &mut first.get_mut(RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID).unwrap()[1];
        let mut arguments: Value =
            serde_json::from_str(reviewer["output"][0]["arguments"].as_str().unwrap()).unwrap();
        let cash = arguments["result"]["assessments"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|assessment| assessment["scope"] == "allocation.cash")
            .unwrap();
        cash["accepted"] = json!(false);
        cash["rationale"] = json!("The cash basis should explain how the residual is calculated.");
        cash["issues"] = json!([{
            "category": "estimate_basis_mismatch",
            "field_path": "numeric_basis.allocation.cash.method",
            "correction_criterion": "State that cash equals total weight minus the invested weights.",
            "evidence_refs": []
        }]);
        reviewer["output"][0]["arguments"] = json!(arguments.to_string());
        daemon.model = crate::ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(first));

        let run = daemon
            .launch_formal_run(RunPurpose::PositionPlan, "decision-lineage-fixture")
            .await
            .unwrap()
            .run_id;
        let store = &daemon.store;
        let artifacts = |kind| store.run_artifacts_by_kind(&run, kind).unwrap();
        for _ in 0..32 {
            assert!(daemon.run_one("decision-lineage-fixture").await.unwrap());
            if !artifacts(ArtifactKind::ProposalReview).is_empty() {
                break;
            }
        }
        let [rejected]: [_; 1] = artifacts(ArtifactKind::ProposalReview).try_into().unwrap();
        let first_review: ProposalReview =
            serde_json::from_slice(&store.read_blob(&rejected.blob).unwrap()).unwrap();
        assert!(!first_review.accepted());
        assert!(artifacts(ArtifactKind::Decision).is_empty());

        let mut revised = (*templates).clone();
        let synthesizer = &mut revised.get_mut(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap()[1];
        let mut arguments: Value =
            serde_json::from_str(synthesizer["output"][0]["arguments"].as_str().unwrap()).unwrap();
        let basis = arguments["result"]["numeric_basis"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|basis| basis["scope"] == "allocation.cash")
            .unwrap();
        basis["method"] = json!("Cash equals total portfolio weight minus the invested weights.");
        synthesizer["output"][0]["arguments"] = json!(arguments.to_string());
        daemon.model =
            crate::ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(revised));
        for _ in 0..32 {
            if !daemon.run_one("decision-lineage-fixture").await.unwrap() {
                break;
            }
        }

        assert_eq!(artifacts(ArtifactKind::DecisionProposal).len(), 2);
        let reviews = artifacts(ArtifactKind::ProposalReview);
        assert_eq!(reviews.len(), 2);
        let approved = reviews
            .iter()
            .find(|review| review.artifact_id != rejected.artifact_id)
            .unwrap();
        let [decision]: [_; 1] = artifacts(ArtifactKind::Decision).try_into().unwrap();
        let payload: Decision =
            serde_json::from_slice(&store.read_blob(&decision.blob).unwrap()).unwrap();
        assert!(decision.source_refs.contains(&payload.decision_context));
        let context = store
            .artifact(&payload.decision_context.artifact_id)
            .unwrap();
        let review_sources = context
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::ProposalReview)
            .collect::<Vec<_>>();
        assert_eq!(
            review_sources
                .iter()
                .map(|source| &source.artifact_id)
                .collect::<Vec<_>>(),
            vec![&approved.artifact_id],
            "only the authorizing review belongs on the DecisionContext"
        );

        let review: ProposalReview = daemon.read_artifact_payload(review_sources[0]).unwrap();
        assert!(review.accepted());
        assert!(decision.source_refs.contains(&review.proposal));
        let proposal = store.artifact(&review.proposal.artifact_id).unwrap();
        assert_eq!(review.proposal_hash, proposal.blob.hash);
        assert_ne!(review.proposal, first_review.proposal);
        assert_ne!(review.proposal_hash, first_review.proposal_hash);
        assert!(proposal
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::ContextManifest)
            .any(|source| {
                let manifest = store.artifact(&source.artifact_id).unwrap();
                manifest.source_refs.iter().any(|source| {
                    source.kind == ArtifactKind::ProposalReview
                        && source.artifact_id == rejected.artifact_id
                })
            }));
        assert!(approved.source_refs.contains(&review.proposal));
        assert!(approved.source_refs.contains(&review.manifest));
        assert_eq!(approved.producer, "agent.research.proposal_reviewer");
        let origin = approved.origin.as_ref().unwrap();
        assert_eq!(origin.run_id.as_ref(), Some(&run));
        let task_id = origin.task_id.as_ref().unwrap();
        let workflow = store.workflow_snapshot(&run).unwrap();
        let reviewer = workflow
            .tasks
            .iter()
            .find(|task| &task.node.task_id == task_id)
            .unwrap();
        assert_eq!(
            reviewer.node.recipe_id.as_str(),
            RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
        );
        assert_eq!(reviewer.node.execution_spec().proposal_revision, Some(1));
        let attempt = store.current_succeeded_attempt(&run, task_id).unwrap();
        assert_eq!(origin.attempt_id.as_ref(), Some(&attempt.attempt_id));
        assert_eq!(origin.contract_hash.as_ref(), Some(&review.contract_hash));
        assert_eq!(attempt.contract_hash, origin.contract_hash);
        assert_eq!(attempt.context_manifest.as_ref(), Some(&review.manifest));
        assert!(attempt.outputs.contains(approved));
    }

    #[tokio::test]
    async fn canonical_paper_cold_start_schedules_outcome_without_broker_writes() {
        let daemon = fixture();
        let store = daemon.store.clone();
        let first = daemon
            .paper
            .scheduler
            .tick(&OpenPaperSession, &daemon.paper_workflow_source(), Utc::now())
            .await
            .unwrap()
            .expect("the initial Paper session must be able to learn without a policy");
        let run = first.slot.workflow.run.run_id.clone();
        assert_eq!(store.run_purpose(&run).unwrap(), RunPurpose::Paper);
        assert!(store.paper_approval_for_run(&run).unwrap().is_none());
        assert!(store.debug_environment().unwrap().is_none());
        let repeated = daemon
            .paper
            .scheduler
            .tick(&OpenPaperSession, &daemon.paper_workflow_source(), Utc::now())
            .await
            .unwrap()
            .expect("an already reserved Paper session is reused");
        assert_eq!(repeated.slot.workflow.run.run_id, run);
        assert!(!repeated.newly_reserved);
        for _ in 0..40 {
            if !daemon.run_one("canonical-paper-cold-start-fixture").await.unwrap() {
                break;
            }
        }
        let decision = store.run_artifacts_by_kind(&run, ArtifactKind::Decision).unwrap();
        assert_eq!(decision.len(), 1);
        let payload: akzio_domain::Decision =
            serde_json::from_slice(&store.read_blob(&decision[0].blob).unwrap()).unwrap();
        assert_eq!(payload.targets, akzio_domain::TargetPortfolio::zeroed());
        let verdicts = store.run_artifacts_by_kind(&run, ArtifactKind::ExecutionVerdict).unwrap();
        assert_eq!(verdicts.len(), 1);
        let verdict: akzio_domain::ExecutionVerdict =
            serde_json::from_slice(&store.read_blob(&verdicts[0].blob).unwrap()).unwrap();
        assert!(matches!(verdict, akzio_domain::ExecutionVerdict::NoOrder { .. }));
        assert!(store
            .run_artifacts_by_kind(&run, ArtifactKind::ExecutionCommitment)
            .unwrap()
            .is_empty());
        assert!(store
            .run_artifacts_by_kind(&run, ArtifactKind::OrderReceipt)
            .unwrap()
            .is_empty());
        assert!(store.outcome_schedule_for_run(&run).unwrap().is_some());
        daemon.archive_ready_run_stages().unwrap();
        assert!(store.run_log_status(&run).unwrap().iter().any(|entry| {
            entry["phase"] == "T0" && entry["status"] == "archived"
        }));
        assert!(!store.run_lifecycle_health(&run).unwrap().numeric_outcome_sealed);
        let report = store
            .export_run_bundle(
                &run,
                store
                    .root()
                    .parent()
                    .unwrap()
                    .join(format!("paper-report-{}", RunId::new().0)),
            )
            .unwrap();
        assert_eq!(report.purpose, Some(RunPurpose::Paper));
        assert_eq!(report.exporter_version, "run-bundle-v1");
        assert!(!report.raw_model_access.allowed);
    }
}
