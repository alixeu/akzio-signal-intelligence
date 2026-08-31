#[tokio::test]
async fn cancellation_and_freeze_are_durable_store_owned_transitions() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    assert_eq!(
        daemon
            .request_cancel(&run_id, "fixture cancellation request")
            .await
            .unwrap(),
        1
    );
    assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

    assert!(
        daemon
            .set_freeze(true, "fixture freeze".to_owned())
            .unwrap()
            .frozen
    );
    let reopened = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    assert!(reopened.health().unwrap().frozen);
    assert!(
        !reopened
            .set_freeze(false, "fixture operator unfreeze".to_owned())
            .unwrap()
            .frozen
    );
    reopened.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn http_control_rejects_non_loopback_bind() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let (_shutdown, receiver) = watch::channel(false);
    assert!(matches!(
        daemon
            .serve_http("0.0.0.0:0".parse().unwrap(), receiver)
            .await,
        Err(DaemonError::InvalidInput(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_store_maintenance_keeps_http_responsive() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let started_at = std::time::Instant::now();
    let maintenance = tokio::spawn(http::run_store_maintenance(
        daemon.store_executor.clone(),
        StoreMaintenanceKind::Test,
        move |_| {
            started_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            Ok(())
        },
    ));

    started_rx.await.unwrap();
    let started_promptly = started_at.elapsed() < std::time::Duration::from_secs(2);
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        daemon.router().oneshot(
            Request::builder()
                .uri("/control/store/executor")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await;
    let _ = release_tx.send(());
    maintenance.await.unwrap().unwrap();

    assert!(started_promptly, "maintenance ran on the Tokio event loop");
    let response = response.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["maintenance"]["state"], "running");
}

#[tokio::test]
async fn store_maintenance_join_error_maps_to_internal_server_error() {
    let directory = tempdir().unwrap();
    let status = http::run_store_maintenance(
        StoreExecutor::new(V2Store::open(directory.path()).unwrap()),
        StoreMaintenanceKind::Test,
        |_| -> std::result::Result<(), StoreError> { panic!("fixture maintenance panic") },
    )
    .await
    .unwrap_err();

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn readiness_requires_auth_and_injected_paper_broker() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let not_ready = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

    let daemon = daemon.with_paper_broker(Arc::new(FakePaperBroker::default()));
    let ready = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_keeps_historical_failures_observable() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(
        daemon_config,
        ModelClient::fixture_sequence({
            let mut responses = two_phase_responses(serde_json::json!({}));
            responses.push(responses[1].clone());
            responses
        }),
    )
    .unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("failed-run-fixture").await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(daemon.run_one("failed-run-fixture").await.unwrap());
    let daemon = daemon.with_paper_broker(Arc::new(FakePaperBroker::default()));

    let health = daemon.health().unwrap();
    assert!(health
        .alerts
        .iter()
        .any(|alert| matches!(alert.severity, AlertSeverity::Critical)));
    assert!(daemon.ready().is_ok());
}

#[tokio::test]
async fn http_control_auth_cancel_retry_and_freeze_are_governed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);

    let submitted = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runs")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"debug"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(submitted.status(), StatusCode::OK);
    let run_id = serde_json::from_slice::<RunSubmissionResponse>(
        &to_bytes(submitted.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()
    .run_id;

    let cancelled = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/runs/{run_id}/cancel"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::OK);
    assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

    let retried = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/runs/{run_id}/retry"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retried.status(), StatusCode::OK);
    let retry = serde_json::from_slice::<RunRetryResponse>(
        &to_bytes(retried.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(retry.source_run_id, run_id);
    let retry_run_id = retry.run_id;
    assert_eq!(
        daemon.store().run_purpose(&retry_run_id).unwrap(),
        RunPurpose::Debug
    );

    let frozen = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/freeze")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"fixture freeze"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(frozen.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(frozen.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap()["frozen"]
            .as_bool(),
        Some(true)
    );

    let unfrozen = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/unfreeze")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"fixture unfreeze"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unfrozen.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(unfrozen.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap()["frozen"]
            .as_bool(),
        Some(false)
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn observer_snapshot_uses_a_separate_read_only_credential() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    let control_token = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/v1/observer/snapshot")
                .header("x-akzio-observer-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(control_token.status(), StatusCode::UNAUTHORIZED);

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/v1/observer/snapshot")
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(body["schema_version"].as_u64(), Some(2));
    assert!(body["core"]["readiness_ppm"].as_u64().is_some());
    assert_eq!(body["recent_runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["run_summaries"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["outcome"]["status"], "pending");
    assert_eq!(body["recent_runs"][0]["run"]["run_id"], run_id.0);
    assert_eq!(body["current_run"]["workflow"]["run"]["run_id"], run_id.0);
    assert_eq!(body["portfolio"]["status"], "unavailable");
    assert_eq!(body["core"]["approval"]["status"], "missing");
    assert!(body["event_cursor"].as_i64().unwrap() > 0);

    let run_detail = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/observer/runs/{run_id}"))
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(run_detail.status(), StatusCode::OK);

    let observer_cannot_use_control_api = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        observer_cannot_use_control_api.status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn http_sse_resumes_from_the_requested_cursor() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    daemon
        .request_cancel(&run_id, "fixture cancellation request")
        .await
        .unwrap();
    let events = daemon.store().events_after(&run_id, 0, 16).unwrap();
    assert!(events.len() >= 2);
    let after = events[0].cursor;
    let expected = &events[1];

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/events?after={after}"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains(&format!("id: {}", expected.cursor)));
    assert!(frame.contains(&expected.event_type));
}

#[tokio::test]
async fn http_sse_forwards_transient_reasoning_events() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let after = daemon.store().event_cursor().unwrap();
    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/events?after={after}"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    daemon
        .reasoning_events
        .send(AgentReasoningEvent::ReasoningDelta {
            run_id,
            task_id: TaskId::new(),
            attempt_id: akzio_domain::AttemptId::new(),
            purpose: "research.analyst".to_owned(),
            turn: 0,
            delta: "bounded summary".to_owned(),
        })
        .unwrap();

    let mut body = response.into_body().into_data_stream();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let frame = body.next().await.unwrap().unwrap();
            let frame = String::from_utf8(frame.to_vec()).unwrap();
            if frame.contains("event: reasoning-delta") {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert!(frame.contains("bounded summary"));
    assert!(frame.contains("research.analyst"));
}

#[tokio::test]
async fn http_trajectory_is_authenticated_and_read_only() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let before = daemon.store().events_after(&run_id, 0, 32).unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/trajectory"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/trajectory"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let entries: Vec<akzio_store::v2::TrajectoryEntry> =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(entries.is_empty());
    let after = daemon.store().events_after(&run_id, 0, 32).unwrap();
    assert_eq!(before, after);
}
