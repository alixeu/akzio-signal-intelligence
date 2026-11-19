// 文件导读：Native App 的 `/runs` 入口只复用正式 PositionPlan graph 或 Paper scheduler。
// 它拒绝浏览器来源和隔离 Debug Core；PositionPlan 在 Decision 后结束，Paper 只返回
// scheduler reservation 的 RunId。返回 RunId/HTTP 成功不等于模型完成、Decision 通过、
// Paper submission、fill 或 Outcome 成熟。
// Rust 机制：Axum `State/Json` extractor 取得请求；闭包 move 进 StoreExecutor
// 取得原子 commit；`match` 穷举 RunPurpose，`Option` 表示 scheduler 尚未创建 slot。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunLaunchRequest {
    purpose: RunPurpose,
    #[serde(default, deserialize_with = "deserialize_launch_uuid")]
    requested_run_id: Option<uuid::Uuid>,
    #[serde(default, deserialize_with = "deserialize_launch_uuid")]
    paper_request_id: Option<uuid::Uuid>,
}

fn canonical_launch_uuid(raw: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(raw).ok().filter(|id| {
        id.get_variant() == uuid::Variant::RFC4122
            && id.get_version() == Some(uuid::Version::Random)
            && id.to_string() == raw
    })
}

fn deserialize_launch_uuid<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<uuid::Uuid>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    canonical_launch_uuid(&raw)
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("launch request ID must be a canonical UUID v4"))
}

type RunHttpResult =
    std::result::Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)>;

fn run_http_error(error: DaemonError) -> (StatusCode, Json<serde_json::Value>) {
    let status = match error {
        DaemonError::Store(StoreError::MissingRun(_) | StoreError::MissingTask(_)) => {
            StatusCode::NOT_FOUND
        }
        DaemonError::RunLogUnavailable(_)
        | DaemonError::Scheduler(
            SchedulerError::RunLogUnavailable(_) | SchedulerError::PaperLaunchRequestBinding(_),
        ) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::CONFLICT,
    };
    (status, Json(serde_json::json!({"error":error.to_string()})))
}

async fn http_launch_run(
    State(daemon): State<Arc<Daemon>>,
    Json(request): Json<RunLaunchRequest>,
) -> RunHttpResult {
    // PositionPlan 用 StoreExecutor 提交新图，Paper 向 scheduler 请求真实 session slot。
    // 两者均不在 handler 内直接跑 worker。
    if request.requested_run_id.is_some() && request.purpose != RunPurpose::PositionPlan {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"requested_run_id_requires_position_plan"})),
        ));
    }
    if request.paper_request_id.is_some() && request.purpose != RunPurpose::Paper {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"paper_request_id_requires_paper"})),
        ));
    }
    let requested_run_id = request.requested_run_id.map(|id| RunId(id.to_string()));
    let submission = daemon
        .submit_formal_run(
            request.purpose,
            requested_run_id,
            request.paper_request_id,
            "http",
        )
        .await
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

async fn http_paper_launch_lookup(
    State(daemon): State<Arc<Daemon>>,
    Path(request_id): Path<String>,
) -> RunHttpResult {
    if canonical_launch_uuid(&request_id).is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"invalid_paper_request_id"})),
        ));
    }
    let query_id = request_id.clone();
    let unavailable = || {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"lookup_status":"unavailable"})),
        )
    };
    let result = daemon
        .store_executor
        .execute(move |store| -> akzio_store::Result<_> {
            if store.debug_environment()?.is_some()
                || !store.paper_launch_request_lookup_supported()?
            {
                return Ok(None);
            }
            store.paper_launch_request_session(&query_id).map(Some)
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?
        .ok_or_else(unavailable)?;
    let (session_key, run_id) = match result {
        Some((session, run)) => (Some(session), run),
        None => (None, None),
    };
    let status = if run_id.is_some() {
        "session_run_found"
    } else {
        "unknown"
    };
    Ok(Json(serde_json::json!({
        "request_id": request_id,
        "lookup_status": status,
        "session_key": session_key,
        "run_id": run_id,
    })))
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
        let parent = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-run-http-tests");
        std::fs::create_dir_all(&parent).unwrap();
        let root = parent.join(RunId::new().0);
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
            daemon
                .submit_formal_run(RunPurpose::PositionPlan, None, None, "test")
                .await,
            Err(crate::launch::FormalRunLaunchError::Rejected(DaemonError::RunLogUnavailable(_)))
        ));
    }

    async fn drop_launch_response(
        request: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> axum::response::Response {
        let disconnect = request
            .headers()
            .contains_key("x-test-lose-launch-response");
        let mut response = next.run(request).await;
        if disconnect && response.status().is_success() {
            response
                .headers_mut()
                .remove(axum::http::header::CONTENT_LENGTH);
            *response.body_mut() =
                axum::body::Body::from_stream(futures::stream::once(std::future::ready(Err::<
                    axum::body::Bytes,
                    _,
                >(
                    std::io::Error::other("injected post-commit disconnect"),
                ))));
        }
        response
    }

    struct AdvancingPaperClock {
        day: std::sync::Mutex<String>,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl crate::BrokerSessionClock for AdvancingPaperClock {
        fn open_session_key(
            &self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = SchedulerResult<Option<String>>> + Send + '_>,
        > {
            Box::pin(async {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Some(self.day.lock().unwrap().clone()))
            })
        }

        fn paper_account_id(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SchedulerResult<String>> + Send + '_>>
        {
            Box::pin(async { panic!("cold-start Paper must not look up an approval account") })
        }
    }

    /// WHY THIS TEST EXISTS (Task #8): A lost Paper response must leave its originally bound Session and committed Run inspectable after the Broker session changes.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn task8_paper_locator_survives_post_commit_disconnect() {
        use std::sync::atomic::Ordering;
        let day = Utc::now()
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive();
        let next_day = day.succ_opt().unwrap().to_string();
        let original_day = day.to_string();
        let clock = Arc::new(AdvancingPaperClock {
            day: std::sync::Mutex::new(original_day.clone()),
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut daemon = fixture();
        let local = Arc::get_mut(&mut daemon).unwrap();
        local.paper.runtime_identity_hash = Some(ContentHash::of_bytes(b"test-paper-runtime"));
        local.paper.fixture_session_clock = Some(clock.clone());
        let store = daemon.store.clone();
        let router = daemon
            .router()
            .layer(axum::middleware::from_fn(drop_launch_response));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        let request_id = "1856d70a-8701-4bb9-8d07-d93a05c607c8";
        let launch = || {
            client
                .post(format!("{base}/runs"))
                .header("x-akzio-token", "test-formal-token")
                .json(&serde_json::json!({"purpose":"paper","paper_request_id":request_id}))
        };
        let lookup = |id: &str| client.get(format!("{base}/v1/runs/submissions/{id}"));
        let read = |id: &str| lookup(id).header("x-akzio-token", "test-formal-token");
        let ready = client
            .get(format!("{base}/ready"))
            .header("x-akzio-token", "test-formal-token")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(ready["paper_launch_request_lookup_supported"], true);
        assert_eq!(
            lookup(request_id).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            read(request_id)
                .header("origin", "http://127.0.0.1")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            read(&request_id.to_uppercase())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        let unknown = read(request_id)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(unknown["lookup_status"], "unknown");
        assert!(unknown["session_key"].is_null());
        assert!(unknown["run_id"].is_null());
        assert_eq!(clock.reads.load(Ordering::SeqCst), 0);
        assert!(store
            .daemon_lease(crate::SCHEDULER_LEASE_NAME)
            .unwrap()
            .is_none());
        let response = launch()
            .header("x-test-lose-launch-response", "1")
            .send()
            .await;
        if let Ok(response) = response {
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.bytes().await.is_err());
        }
        let slot = store
            .session_slot(&original_day)
            .unwrap()
            .expect("Paper must persist the original research slot first");
        let run_id = slot.workflow.run.run_id;
        assert_eq!(store.run_purpose(&run_id).unwrap(), RunPurpose::Paper);
        assert!(store.paper_approval_for_run(&run_id).unwrap().is_none());
        assert!(slot.commitment_artifact_id.is_none());
        assert_eq!(clock.reads.load(Ordering::SeqCst), 1);
        let first_lookup = read(request_id)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(first_lookup["lookup_status"], "session_run_found");
        assert_eq!(first_lookup["request_id"], request_id);
        assert_eq!(first_lookup["session_key"], original_day);
        assert_eq!(first_lookup["run_id"], run_id.0);
        assert!(first_lookup.get("submission_status").is_none());
        assert_eq!(
            launch().send().await.unwrap().status(),
            StatusCode::CONFLICT
        );
        *clock.day.lock().unwrap() = next_day.clone();
        let lease_before = store.daemon_lease(crate::SCHEDULER_LEASE_NAME).unwrap();
        assert_eq!(
            launch().send().await.unwrap().status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            read(request_id)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap(),
            first_lookup
        );
        assert_eq!(clock.reads.load(Ordering::SeqCst), 1);
        assert!(store.session_slot(&next_day).unwrap().is_none());
        assert_eq!(store.recent_workflows(2).unwrap().len(), 1);
        assert_eq!(
            store.daemon_lease(crate::SCHEDULER_LEASE_NAME).unwrap(),
            lease_before
        );
        let reopened = akzio_store::Store::open_existing(store.root()).unwrap();
        assert_eq!(
            reopened.paper_launch_request_session(request_id).unwrap(),
            Some((original_day, Some(run_id)))
        );
        let pending_id = "7f9630d8-a86b-4b42-908e-f9568dc85b05";
        assert!(store
            .bind_paper_launch_request(pending_id, &next_day, Utc::now())
            .unwrap());
        daemon
            .run_log
            .as_ref()
            .unwrap()
            .mark_unavailable("test lookup with blocked run log")
            .unwrap();
        let pending = read(pending_id)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(pending["lookup_status"], "unknown");
        assert_eq!(pending["session_key"], next_day);
        assert!(pending["run_id"].is_null());
        assert_eq!(clock.reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.daemon_lease(crate::SCHEDULER_LEASE_NAME).unwrap(),
            lease_before
        );
        server.abort();
    }

    /// WHY THIS TEST EXISTS (Task #8): A lost launch response must leave the requested RunId independently inspectable with its original committed graph.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn task8_position_plan_id_survives_post_commit_disconnect() {
        let daemon = fixture();
        let router = daemon.router().layer(axum::middleware::from_fn(drop_launch_response));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        let run_id = "b302f57d-53be-4a10-9b27-8c37de95d2f6";
        let inspect = || {
            client
                .get(format!("{base}/v1/observer/runs/{run_id}/inspection"))
                .header("x-akzio-token", "test-formal-token")
        };
        let launch = || {
            client
                .post(format!("{base}/runs"))
                .header("x-akzio-token", "test-formal-token")
                .json(&serde_json::json!({"purpose":"position_plan","requested_run_id":run_id}))
        };
        assert_eq!(
            inspect().send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        let response = launch()
            .header("x-test-lose-launch-response", "1")
            .send()
            .await;
        if let Ok(response) = response {
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.bytes().await.is_err());
        }
        let inspection = inspect()
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(inspection["workflow"]["run"]["run_id"], run_id);
        assert_eq!(inspection["workflow"]["run"]["purpose"], "position_plan");
        assert!(!inspection["workflow"]["tasks"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            daemon
                .store
                .run_artifacts_by_kind(&RunId(run_id.into()), ArtifactKind::EvidenceNeed)
                .unwrap()
                .len(),
            34
        );
        let duplicate = launch().send().await.unwrap();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        let original = inspect()
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(original["workflow"], inspection["workflow"]);
        assert_eq!(daemon.store.recent_workflows(2).unwrap().len(), 1);
        server.abort();
    }

    /// WHY THIS TEST EXISTS (Task #8): Invalid, noncanonical, or cross-purpose IDs must fail before formal Run admission or publication.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn task8_launch_rejects_invalid_requested_run_ids_before_effects() {
        let daemon = fixture();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, receiver) = watch::channel(false);
        let service = daemon.clone();
        let server =
            tokio::spawn(async move { service.serve_http_listener(listener, receiver).await });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let run_id = "b302f57d-53be-4a10-9b27-8c37de95d2f6";
        for (purpose, id) in [
            ("position_plan", serde_json::json!("not-a-uuid")),
            ("position_plan", serde_json::json!(run_id.to_uppercase())),
            ("position_plan", serde_json::json!(run_id.replace('-', ""))),
            (
                "position_plan",
                serde_json::json!(run_id.replace("-4a10-", "-5a10-")),
            ),
            (
                "position_plan",
                serde_json::json!(run_id.replace("-9b27-", "-1b27-")),
            ),
            ("position_plan", serde_json::Value::Null),
            ("position_plan", serde_json::json!(1)),
            ("paper", serde_json::json!(run_id)),
            ("shadow", serde_json::json!(run_id)),
        ] {
            let response = client
                .post(format!("http://{address}/runs"))
                .header("x-akzio-token", "test-formal-token")
                .json(&serde_json::json!({"purpose":purpose,"requested_run_id":id}))
                .send()
                .await
                .unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                ),
                "{purpose} {id}: {}",
                response.status()
            );
        }
        for body in [
            serde_json::json!({"purpose":"position_plan","paper_request_id":run_id}),
            serde_json::json!({"purpose":"paper","paper_request_id":null}),
            serde_json::json!({"purpose":"paper","paper_request_id":"bad"}),
            serde_json::json!({"purpose":"paper","paper_request_id":run_id.to_uppercase()}),
            serde_json::json!({"purpose":"paper","requested_run_id":run_id,"paper_request_id":run_id}),
        ] {
            let response = client
                .post(format!("http://{address}/runs"))
                .header("x-akzio-token", "test-formal-token")
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                ),
                "{body}: {}",
                response.status()
            );
        }
        assert!(daemon.store.recent_workflows(1).unwrap().is_empty());
        stop.send(true).unwrap();
        server.await.unwrap().unwrap();
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
        assert_eq!(readiness["position_plan_requested_run_id_supported"], true);
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
            .launch_formal_run(RunPurpose::PositionPlan, None, None, "decision-lineage-fixture")
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

    /// WHY THIS TEST EXISTS (Task #22): Workflow reads and controls must share
    /// the native client checks before handlers can read or modify the Store.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn workflow_routes_require_the_same_native_client() {
        let daemon = fixture();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, receiver) = watch::channel(false);
        let service = daemon.clone();
        let server =
            tokio::spawn(async move { service.serve_http_listener(listener, receiver).await });
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let export_target = daemon
            .store
            .root()
            .parent()
            .unwrap()
            .join(format!("denied-export-{}", RunId::new().0));
        let cases = [
            (
                "/v1/workflows/blueprint?purpose=position_plan",
                None,
                StatusCode::OK,
            ),
            (
                "/v1/observer/runs/missing/inspection",
                None,
                StatusCode::NOT_FOUND,
            ),
            (
                "/v1/observer/runs/missing/journal?limit=1",
                None,
                StatusCode::NOT_FOUND,
            ),
            (
                "/runs",
                Some(serde_json::json!({"purpose":"position_plan"})),
                StatusCode::OK,
            ),
            (
                "/control/freeze",
                Some(serde_json::json!({"reason":"fixture freeze"})),
                StatusCode::OK,
            ),
            (
                "/control/paper-approval",
                Some(serde_json::json!({})),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "/control/canary/resume",
                Some(serde_json::json!({})),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "/control/store/export-run-bundle",
                Some(serde_json::json!({"run_id":"missing","target":export_target})),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        let request = |path, body: &Option<Value>| {
            let url = format!("http://{address}{path}");
            match body {
                Some(value) => client.post(url).json(value),
                None => client.get(url),
            }
        };
        for (path, body, _) in &cases {
            let base = request(path, body);
            let rejected = [
                (base.try_clone().unwrap(), StatusCode::UNAUTHORIZED),
                (
                    base.try_clone()
                        .unwrap()
                        .header("x-akzio-token", "wrong-fixture-token"),
                    StatusCode::UNAUTHORIZED,
                ),
                (
                    base.try_clone().unwrap().header("origin", "null"),
                    StatusCode::UNAUTHORIZED,
                ),
                (
                    base.try_clone()
                        .unwrap()
                        .header("x-akzio-token", "test-formal-token")
                        .header("origin", "http://127.0.0.1"),
                    StatusCode::FORBIDDEN,
                ),
                (
                    base.header("x-akzio-token", "test-formal-token")
                        .header("sec-fetch-site", "same-origin"),
                    StatusCode::FORBIDDEN,
                ),
            ];
            for (candidate, expected) in rejected {
                assert_eq!(candidate.send().await.unwrap().status(), expected, "{path}");
            }
        }
        assert!(daemon.store.recent_workflows(1).unwrap().is_empty());
        assert!(!daemon.health().unwrap().frozen);
        assert!(!export_target.exists());

        for (path, body, expected) in &cases {
            let response = request(path, body)
                .header("x-akzio-token", "test-formal-token")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), *expected, "{path}");
        }
        assert_eq!(daemon.store.recent_workflows(2).unwrap().len(), 1);
        assert!(daemon.health().unwrap().frozen);
        assert!(!export_target.exists());
        let blueprint = client
            .get(format!(
                "http://{address}/v1/workflows/blueprint?purpose=position_plan"
            ))
            .header("x-akzio-token", "test-formal-token")
            .header("sec-fetch-site", "none")
            .send()
            .await
            .unwrap();
        assert_eq!(blueprint.status(), StatusCode::OK);

        let shadow_id = RunId::new();
        let definition = daemon
            .workflow
            .research_definition("approved-research")
            .unwrap();
        let shadow_graph = daemon
            .workflow
            .lower(RunPurpose::Shadow, &definition.proposal)
            .unwrap();
        let shadow = daemon
            .workflow
            .prepare_workflow_commit(
                shadow_id.clone(),
                RunPurpose::Shadow,
                shadow_graph,
                Utc::now(),
            )
            .unwrap();
        daemon.store.commit_workflow(&shadow).unwrap();
        let rejected_export = client
            .post(format!("http://{address}/control/store/export-run-bundle"))
            .header("x-akzio-token", "test-formal-token")
            .json(&serde_json::json!({"run_id": shadow_id, "target": export_target}))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected_export.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!export_target.exists());

        let malformed = client
            .post(format!("http://{address}/runs"))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            malformed.json::<Value>().await.unwrap()["error"],
            "authenticated_native_client_required"
        );
        let wrong_method = client
            .get(format!("http://{address}/runs"))
            .send()
            .await
            .unwrap();
        assert_eq!(wrong_method.status(), StatusCode::METHOD_NOT_ALLOWED);
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
