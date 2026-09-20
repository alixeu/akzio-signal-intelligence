use super::*;

#[test]
fn runtime_blueprint_has_stable_typed_control_and_preserves_frozen_history() {
    let d = daemon(true);
    let definition = d.workflow.research_definition("preview").unwrap();
    for (purpose, count) in [(RunPurpose::PositionPlan, 21), (RunPurpose::Paper, 25)] {
        let graph = d.workflow.lower(purpose, &definition.proposal).unwrap();
        let blueprint = graph.blueprint(purpose).unwrap();
        let again = d.workflow.lower(purpose, &definition.proposal).unwrap();
        assert_ne!(graph.nodes[0].task_id, again.nodes[0].task_id);
        assert_eq!(blueprint, again.blueprint(purpose).unwrap());
        assert_eq!(blueprint.nodes.len(), count);
        assert_eq!(blueprint.definition_version, Some(1));
        assert!(graph.nodes.iter().all(|n| n.spec.is_some()));
        assert!(!graph
            .nodes
            .iter()
            .any(|n| n.objective.contains("[research_horizon=")));
        let mut prose_changed = graph.clone();
        let analyst = prose_changed
            .nodes
            .iter_mut()
            .find(|n| n.execution_spec().key == "analyst_t1")
            .unwrap();
        analyst.objective = "Description changed [research_horizon=t5]".into();
        assert_eq!(
            analyst.execution_spec().horizon,
            Some(akzio_domain::DecisionHorizon::T1)
        );
        assert!(analyst
            .model_objective()
            .starts_with("[research_horizon=t1] [research_round=0]"));
        assert_eq!(
            blueprint.definition_hash,
            prose_changed.blueprint(purpose).unwrap().definition_hash
        );
        let mut missing_spec = graph.clone();
        missing_spec.nodes[0].spec = None;
        assert!(missing_spec.validate().is_err());
        let mut changed_priority = graph.clone();
        changed_priority.nodes[0].priority = changed_priority.nodes[0].priority.saturating_sub(1);
        assert_ne!(
            blueprint.definition_hash,
            changed_priority.blueprint(purpose).unwrap().definition_hash
        );
        if purpose == RunPurpose::PositionPlan {
            assert_eq!(blueprint.finish, ["gate.decision"]);
            assert!(!blueprint
                .nodes
                .iter()
                .any(|n| n.recipe_id == "gate.execution"));
        }
        let run = RunId::new();
        let commit = d
            .workflow
            .prepare_workflow_commit(run.clone(), purpose, graph.clone(), Utc::now())
            .unwrap();
        d.store.commit_workflow(&commit).unwrap();
        let reopened = Store::open_existing(d.store.root()).unwrap();
        let inspection = reopened.inspect_run(&run).unwrap();
        assert_eq!(inspection.blueprint, blueprint);
        assert_eq!(inspection.workflow.revision.graph, graph);
        assert_eq!(
            inspection.checkpoint.unwrap().graph.artifact_id,
            commit.graph.artifact_id
        );
        d.workflow.replay_run(&run).unwrap();
    }
    d.store.verify_integrity().unwrap();
}

#[tokio::test]
async fn runtime_http_preview_and_journal_are_authenticated_and_read_only() {
    let d = daemon(true);
    let session = d
        .prepare_debug(&DebugPrepareRequest {
            purpose: RunPurpose::PositionPlan,
            session_key: Utc::now().format("%Y-%m-%d").to_string(),
            paper_allowed: false,
        })
        .unwrap();
    let run = &session.identity.run_id;
    let before = d.store.workflow_snapshot(run).unwrap().event_cursor;
    let capture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/runtime-refactor-inspection");
    std::fs::create_dir_all(&capture).unwrap();
    std::fs::write(
        capture.join("inspection.json"),
        serde_json::to_vec_pretty(&d.store.inspect_run(run).unwrap()).unwrap(),
    )
    .unwrap();
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
    let preview = format!("{base}/v1/workflows/blueprint?purpose=position_plan");
    assert_eq!(
        client.get(&preview).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let value: serde_json::Value = client
        .get(&preview)
        .header("x-akzio-token", "test-native-token")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(value["nodes"].as_array().unwrap().len(), 21);
    for suffix in ["inspection", "journal?limit=1"] {
        let response = client
            .get(format!("{base}/v1/observer/runs/{run}/{suffix}"))
            .header("x-akzio-token", "test-native-token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let value: serde_json::Value = response.json().await.unwrap();
        if suffix == "inspection" {
            assert_eq!(value["control"]["status"], "paused");
        } else {
            assert_eq!(value["events"].as_array().unwrap().len(), 1);
            assert_eq!(value["has_more"], true);
        }
    }
    assert_eq!(d.store.workflow_snapshot(run).unwrap().event_cursor, before);
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
