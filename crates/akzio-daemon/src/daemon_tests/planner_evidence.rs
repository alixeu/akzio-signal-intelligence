#[tokio::test]
async fn planner_task_runs_agent_runtime_and_commits_graph_patch() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    assert!(daemon.run_one("fixture").await.unwrap());

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert!(snapshot
        .revision
        .graph
        .nodes
        .iter()
        .any(|node| node.recipe_id.as_str() == "research.analyst"));
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "task.succeeded"));
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn planner_accepts_a_real_debug_shape_with_one_analyst_task() {
    let directory = tempdir().unwrap();
    let planner = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
    "topology_id": "active",
        "tasks": {
            "research_analyst": {
                "depends_on": [],
                "evidence_needs": [],
                "objective": "Identify bounded evidence needs.",
                "priority": 1,
                "recipe_id": "research.analyst",
                "research_intents": [],
            },
        },
        "stop_reason": "proposal_complete",
    });
    let model = ModelClient::fixture_by_purpose(BTreeMap::from([(
        "research.planner".to_owned(),
        two_phase_responses(planner),
    )]));
    let daemon = Daemon::with_model(config(directory.path().to_path_buf()), model).unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let task = daemon
        .store()
        .claim_next_task("fixture", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("planner task");
    let result = daemon.execute_task_inner(&task, Utc::now()).await;
    println!("planner result: {result:?}");
    assert!(result.is_ok(), "planner result: {result:?}");
    daemon.store().workflow_snapshot(&run_id).unwrap();
}

#[tokio::test]
async fn debug_evidence_gate_uses_controlled_fixture_when_planner_has_no_needs() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("fixture").await.unwrap());
    let task = daemon
        .store()
        .claim_next_task("fixture", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("evidence gate task");
    let artifacts = daemon.acquire_evidence(&task, Utc::now()).await.unwrap();
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence));
}

#[tokio::test]
async fn invalid_agent_output_requests_task_retry() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        ModelClient::fixture_sequence({
            let mut responses = two_phase_responses(serde_json::json!({}));
            responses.push(responses[1].clone());
            responses
        }),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let task = daemon
        .store()
        .claim_next_task("invalid-output", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("planner task");

    assert_eq!(task.node.recipe_id.as_str(), "research.planner");
    assert_eq!(
        daemon.execute_task(task).await,
        TaskCompletion::Retry(RetryCause::InvalidOutput)
    );
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "agent.turn_completed"));
    assert!(!daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "task.failed"));
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn evidence_gate_resolves_need_with_fixture_adapter_and_keeps_provenance() {
    let directory = tempdir().unwrap();
    let observed_at = Utc::now();
    let fixture_evidence = BTreeMap::from([(
        EvidenceSource::Alpaca,
        BTreeMap::from([(
            "bars:TQQQ:1d".to_owned(),
            AcquiredEvidence {
                raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                media_type: "application/json".to_owned(),
                source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                observed_at,
                normalized: serde_json::json!({"close": 100}),
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-bars".to_owned()),
                    published_at: None,
                    observed_at,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    dedupe_key: "fixture:alpaca:bars:TQQQ:1d".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )]),
    )]);
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        planner_with_alpaca_need(),
        fixture_evidence,
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    for _ in 0..16 {
        if !daemon.run_one("fixture").await.unwrap() {
            break;
        }
    }
    let artifacts = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .filter_map(|event| event.artifact_id)
        .filter_map(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .collect::<Vec<_>>();
    let normalized = artifacts
        .iter()
        .find(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
        .expect("evidence gate committed normalized fixture evidence");
    let payload: NormalizedEvidencePayload =
        serde_json::from_slice(&daemon.store().read_blob(&normalized.blob).unwrap()).unwrap();
    assert_eq!(payload.resource, "bars:TQQQ:1d");
    assert_eq!(payload.need.kind, ArtifactKind::EvidenceNeed);
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Claim));

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let task_status = |recipe_id: &str| {
        snapshot
            .tasks
            .iter()
            .find(|task| task.node.recipe_id.as_str() == recipe_id)
            .map(|task| task.status)
    };
    assert_eq!(task_status("research.analyst"), Some(TaskStatus::Succeeded));
    assert_eq!(task_status("gate.decision"), Some(TaskStatus::Failed));
    daemon.store().verify_integrity().unwrap();
}
