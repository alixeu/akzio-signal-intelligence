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
async fn paper_blocking_gap_collects_one_supplemental_round() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let supplemental_start = (now.date_naive() - ChronoDuration::days(2)).to_string();
    let first_claim = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topic": "fixture_gap",
        "statement": "The initial evidence lacks a focused news view.",
        "horizon": "t1",
        "stance": "neutral",
        "materiality_ppm": 500000,
        "confidence_ppm": 500000,
        "grounds": [{
            "evidence": {
                "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                "kind": "normalized_evidence"
            },
            "support": "The initial governed evidence is descriptive.",
            "role": "descriptive",
            "assets": [],
            "domain": null
        }],
        "evidence_gaps": [{
            "topic": "news",
            "rationale": "A focused asset news query is needed.",
            "impact": "blocks_directional_forecast",
            "supplemental_needs": [{
                "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                "source_family": "news_web",
                "resource": format!("news:QQQ:{supplemental_start}:{session_key}:market"),
                "query": "QQQ market news",
                "assets": ["QQQ"],
                "window_start": null,
                "window_end": null,
                "max_age_secs": 300,
                "max_results": 1
            }]
        }]
    });
    let second_claim = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topic": "refined",
        "statement": "The supplemental evidence was reviewed.",
        "horizon": "t1",
        "stance": "neutral",
        "materiality_ppm": 500000,
        "confidence_ppm": 500000,
        "grounds": [{
            "evidence": {
                "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                "kind": "normalized_evidence"
            },
            "support": "The refined review remains neutral.",
            "role": "descriptive",
            "assets": [],
            "domain": null
        }],
        "evidence_gaps": []
    });
    let mut responses = two_phase_responses(first_claim);
    responses.extend(two_phase_responses(second_claim));
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        ModelClient::FixtureSequence(Arc::new(Mutex::new(VecDeque::from(responses)))),
        BTreeMap::new(),
    )
    .unwrap();
    let paper_run_id = RunId::new();
    let setup_artifacts = paper_session_evidence_needs(&session_key)
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess fixture Paper evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: snapshot_refs,
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let slot = daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            now,
        )
        .unwrap();
    let run_id = slot.slot.workflow.run.run_id.clone();
    let evidence_task = daemon
        .store()
        .claim_next_task("refinement-evidence", now, ChronoDuration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_outputs = daemon.acquire_evidence(&evidence_task, now).await.unwrap();
    daemon
        .store()
        .commit_attempt(&evidence_task.permit, &evidence_outputs, TaskStatus::Succeeded, now)
        .unwrap();
    assert!(daemon.run_one("refinement-analyst").await.unwrap());

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let analyst = snapshot
        .tasks
        .iter()
        .find(|task| task.node.recipe_id.as_str() == "research.analyst")
        .unwrap();
    let turns = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .iter()
        .filter(|event| {
            event.task_id.as_ref() == Some(&analyst.node.task_id)
                && event.event_type == LifecycleEventType::ContextManifestCreated.as_str()
        })
        .count();
    assert_eq!(turns, 2);
    assert!(daemon
        .store()
        .recent_artifacts_by_kind(ArtifactKind::NormalizedEvidence, 256)
        .unwrap()
        .iter()
        .any(|artifact| artifact
            .provenance
            .source_family
            == EvidenceSource::NewsWeb.as_str()));
}

#[tokio::test]
async fn rejected_supplemental_request_leaves_a_durable_abandoned_event() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let supplemental_start = (now.date_naive() - ChronoDuration::days(2)).to_string();
    // The window ends after the broker session, so the Rust supplemental policy
    // rejects the request and no second analyst round can run.
    let future_end = (now.date_naive() + ChronoDuration::days(1)).to_string();
    let first_claim = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topic": "fixture_gap",
        "statement": "The initial evidence lacks a focused news view.",
        "horizon": "t1",
        "stance": "neutral",
        "materiality_ppm": 500000,
        "confidence_ppm": 500000,
        "grounds": [{
            "evidence": {
                "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                "kind": "normalized_evidence"
            },
            "support": "The initial governed evidence is descriptive.",
            "role": "descriptive",
            "assets": [],
            "domain": null
        }],
        "evidence_gaps": [{
            "topic": "news",
            "rationale": "A focused asset news query is needed.",
            "impact": "blocks_directional_forecast",
            "supplemental_needs": [{
                "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                "source_family": "news_web",
                "resource": format!("news:QQQ:{supplemental_start}:{future_end}:market"),
                "query": "QQQ market news",
                "assets": ["QQQ"],
                "window_start": null,
                "window_end": null,
                "max_age_secs": 300,
                "max_results": 1
            }]
        }]
    });
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        ModelClient::FixtureSequence(Arc::new(Mutex::new(VecDeque::from(two_phase_responses(
            first_claim,
        ))))),
        BTreeMap::new(),
    )
    .unwrap();
    let paper_run_id = RunId::new();
    let setup_artifacts = paper_session_evidence_needs(&session_key)
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess fixture Paper evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: snapshot_refs,
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let slot = daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            now,
        )
        .unwrap();
    let run_id = slot.slot.workflow.run.run_id.clone();
    let evidence_task = daemon
        .store()
        .claim_next_task("abandoned-evidence", now, ChronoDuration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_outputs = daemon.acquire_evidence(&evidence_task, now).await.unwrap();
    daemon
        .store()
        .commit_attempt(
            &evidence_task.permit,
            &evidence_outputs,
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    assert!(daemon.run_one("abandoned-analyst").await.unwrap());

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let analyst = snapshot
        .tasks
        .iter()
        .find(|task| task.node.recipe_id.as_str() == "research.analyst")
        .unwrap();
    let events = daemon.store().events_after(&run_id, 0, 256).unwrap();
    let analyst_events = |event_type: LifecycleEventType| {
        events
            .iter()
            .filter(|event| {
                event.task_id.as_ref() == Some(&analyst.node.task_id)
                    && event.event_type == event_type.as_str()
            })
            .count()
    };
    // One turn only: the refined round never ran, and the abandonment is the
    // sole durable trace that the coverage gap stayed open.
    assert_eq!(analyst_events(LifecycleEventType::ContextManifestCreated), 1);
    assert_eq!(
        analyst_events(LifecycleEventType::SupplementalRoundAbandoned),
        1
    );
    assert_eq!(analyst.status, TaskStatus::Succeeded);
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
