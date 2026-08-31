#[tokio::test]
async fn http_replay_reports_the_durable_snapshot() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/replay"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = serde_json::from_slice::<ReplayReport>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(report.run_id, run_id);
    assert_eq!(report.purpose, RunPurpose::Debug);
    assert!(report.task_count > 0);
    assert_eq!(report.revision, 0);
}

#[test]
fn paper_submit_and_direct_retry_fail_closed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    assert!(matches!(
        daemon.submit_default(RunPurpose::Paper),
        Err(DaemonError::InvalidInput(_))
    ));
    assert!(daemon.retry_run(&RunId::new()).is_err());
}

#[tokio::test]
async fn retry_starts_a_fresh_terminal_nonpaper_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let source_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    daemon
        .request_cancel(&source_run_id, "fixture cancellation request")
        .await
        .unwrap();

    let run_id = daemon.retry_run(&source_run_id).unwrap();
    assert_ne!(run_id, source_run_id);
    assert_eq!(
        daemon.store().run_purpose(&run_id).unwrap(),
        RunPurpose::Debug
    );
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn direct_submit_allows_only_operator_owned_noncanonical_runs() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    for purpose in [RunPurpose::Paper, RunPurpose::Replay, RunPurpose::Shadow] {
        assert!(matches!(
            daemon.submit_default(purpose),
            Err(DaemonError::InvalidInput(_))
        ));
    }

    assert!(daemon.submit_default(RunPurpose::Debug).is_ok());
    assert!(daemon.submit_default(RunPurpose::PositionPlan).is_ok());
    assert!(daemon.submit_default(RunPurpose::PaperDryRun).is_ok());
}

#[tokio::test]
async fn http_submit_accepts_position_plan_and_persists_purpose() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runs")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"position_plan"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let run_id = serde_json::from_slice::<RunSubmissionResponse>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()
    .run_id;
    assert_eq!(
        daemon.store().run_purpose(&run_id).unwrap(),
        RunPurpose::PositionPlan
    );
    let graph = daemon.store().workflow_snapshot(&run_id).unwrap().revision.graph;
    assert!(graph
        .nodes
        .iter()
        .all(|node| !matches!(
            node.recipe_id.as_str(),
            "gate.execution" | "gate.paper" | "gate.reconcile" | "gate.evaluate"
        )));
}

#[test]
fn operator_retry_rejects_paper_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let run_id = daemon
        .reserve_paper_session(&session_key, &paper_proposal(), now)
        .unwrap()
        .slot
        .workflow
        .run
        .run_id;

    assert!(matches!(
        daemon.retry_run(&run_id),
        Err(DaemonError::InvalidInput(message)) if message.contains("scheduler-owned")
    ));
}

#[tokio::test]
async fn http_submit_rejects_replay_before_workflow_creation() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runs")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"replay"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
