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
    if headers.contains_key("origin") || headers.get("sec-fetch-site").is_some_and(|v| v != "none")
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"native_client_required"})),
        ));
    }
    if let Some(logger) = &daemon.run_log {
        logger.assert_available().map_err(|error| (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"run_log_unavailable","detail":error.to_string()})),
        ))?;
    }
    let (run_id, newly_created) = match request.purpose {
        RunPurpose::PositionPlan => {
            let operation = daemon.clone();
            let run_id = run_daemon_store_operation(daemon.store_executor.clone(), move || {
                let now = Utc::now();
                let session = now
                    .with_timezone(&chrono_tz::America::New_York)
                    .date_naive()
                    .to_string();
                let (workflow, setup) = operation.prepare_position_plan(&session, now)?;
                operation.store.commit_position_plan(&workflow, &setup)?;
                Ok(workflow.run.run_id)
            })
            .await
            .map_err(run_http_error)?;
            (run_id, true)
        }
        RunPurpose::Paper => {
            let paper = daemon.paper.paper_observer.clone().ok_or_else(|| {
                run_http_error(DaemonError::InvalidInput(
                    "完整模式尚未启用：配置 daemon.manual_paper=true 后重启 Core".into(),
                ))
            })?;
            if daemon.paper.runtime_identity_hash.is_none() {
                return Err(run_http_error(DaemonError::InvalidInput(
                    "Paper runtime identity is unavailable".into(),
                )));
            }
            let clock = AlpacaPaperSessionClock::new(paper);
            let reservation = daemon
                .paper
                .scheduler
                .tick(&clock, &daemon.paper_workflow_source(), Utc::now())
                .await
                .map_err(|error| run_http_error(error.into()))?;
            let reservation = reservation
                .ok_or_else(|| {
                    run_http_error(DaemonError::InvalidInput(
                        "Paper 暂未启动：请检查交易时段、当前审批及校准状态；未创建订单".into(),
                    ))
                })?;
            (
                reservation.slot.workflow.run.run_id,
                reservation.newly_reserved,
            )
        }
        _ => {
            return Err(run_http_error(DaemonError::InvalidInput(
                "仅支持 PositionPlan 或 Paper".into(),
            )));
        }
    };
    if newly_created {
        if let Some(logger) = &daemon.run_log {
            let scope = akzio_model::ModelAuditContext {
                run_id: run_id.0.clone(),
                phase: "T0".into(),
                task_id: None,
                attempt_id: None,
                call_id: None,
                retry_index: None,
            };
            logger
                .record(
                    &scope,
                    "rust.run.submitted",
                    serde_json::json!({"purpose":request.purpose,"source":"http"}),
                )
                .map_err(|error| {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(serde_json::json!({
                            "error":"run_log_unavailable_after_commit",
                            "run_id":run_id,
                            "detail":error.to_string(),
                        })),
                    )
                })?;
        }
    }
    Ok(Json(serde_json::json!({"run_id":run_id})))
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
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-run-http-tests")
            .join(RunId::new().0);
        let run_log = crate::RunLogger::for_test(root.join("log"));
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
