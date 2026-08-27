#[tokio::test]
async fn model_client_adapter_debug_trace_retains_the_provider_request_and_result() {
    let artifact_hash = akzio_domain::ContentHash::of_bytes(b"fixture-artifact");
    let adapter = ModelClientAdapter::with_debug(
        akzio_model::ModelClient::Fixture(json!({
            "output_text": "",
            "tool_calls": [{
                "call_id": "fixture-tool",
                "name": "read_artifact",
                "arguments": {"artifact_id": artifact_hash.as_str()},
            }],
        })),
        true,
    );
    let response = adapter
        .turn(AgentModelRequest {
            contract_hash: akzio_domain::ContentHash::of_bytes(b"fixture-contract"),
            purpose: "research.analyst".to_owned(),
            phase: AgentTurnPhase::Draft,
            prompt: "fixture prompt".to_owned(),
            objective: "fixture objective".to_owned(),
            manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(
                b"fixture-manifest",
            )),
            context: vec![],
            continuation: None,
            tool_outputs: vec![],
            continuation_instruction: None,
            max_output_tokens: 32,
            tools: vec![AgentToolDefinition {
                name: "read_artifact".to_owned(),
                description: "fixture".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"artifact_id": {"type": "string"}},
                    "required": ["artifact_id"],
                    "additionalProperties": false,
                }),
                strict: true,
            }],
            terminal: None,
        })
        .await
        .unwrap();

    assert!(response.assistant_text.is_none());
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].name, "read_artifact");
    assert_eq!(
        response.tool_calls[0].arguments["artifact_id"],
        artifact_hash.to_string()
    );
    let trace = response.model_debug.expect("debug trace is retained");
    assert_eq!(trace.request["model"], "fixture");
    assert_eq!(trace.request["tool_choice"], "auto");
    assert_eq!(trace.request["tools"][0]["strict"], true);
    assert_eq!(trace.result["tool_calls"][0]["call_id"], "fixture-tool");
}

#[tokio::test]
async fn agent_runtime_rejects_a_grant_that_expires_during_a_model_turn() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::milliseconds(1));
    let model = DelayedToolModel {
        evidence_id: evidence.artifact_id.clone(),
    };

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::Context(ContextError::GrantDenied { .. }))
    ));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_rejects_a_stale_permit_before_model_call() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store, catalogue, Duration::minutes(5));
    let model = ToolThenOutputModel {
        evidence_id: evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let mut stale_permit = claimed.permit.clone();
    stale_permit.epoch = stale_permit.epoch.saturating_add(1);

    assert!(matches!(
        runtime
            .run(
                &stale_permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::Store(StoreError::StalePermit(_)))
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn agent_runtime_rejects_a_node_from_another_task() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store, catalogue, Duration::minutes(5));
    let model = FixedModel(submission_turn(json!({"summary": "should not run"})));
    let mut foreign_node = claimed.node.clone();
    foreign_node.task_id = akzio_domain::TaskId::new();

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &foreign_node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::TaskMismatch)
    ));
}

#[tokio::test]
async fn agent_runtime_enforces_tool_source_family_scope() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        ..
    } = fixture_with(|contract| {
        contract
            .context
            .permitted_source_families
            .insert("news".to_owned());
    });
    let news = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_bytes(br#"{"headline":"fixture"}"#, "application/json")
            .unwrap(),
        "fixture.news",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "news".to_owned(),
            ..provenance()
        },
        Some(claimed.permit.artifact_origin()),
        vec![],
        Utc::now(),
    )
    .unwrap();
    store
        .write_task_artifact(
            &claimed.permit,
            &news,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-news-denied".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"artifact_id": news.artifact_id.0.as_str()}),
    }));

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: news.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolSourceNotGranted { .. })
    ));
    let failure_id = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == "tool.failed")
        .and_then(|event| event.artifact_id)
        .expect("failed tool result is durable");
    let failure = store.artifact(&failure_id).unwrap();
    let trace: Value = serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
    assert_eq!(trace["ok"], false);
    assert_eq!(trace["error"]["code"], "tool_source_not_granted");
    assert!(failure
        .source_refs
        .iter()
        .any(|reference| reference.kind == ArtifactKind::ToolCall));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_records_invalid_tool_arguments_before_rejecting() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-invalid-arguments".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"unexpected": true}),
    }));

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::InvalidOutput(_))
    ));

    let failure_id = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == "tool.failed")
        .and_then(|event| event.artifact_id)
        .expect("invalid tool result is durable");
    let failure = store.artifact(&failure_id).unwrap();
    let trace: Value = serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
    assert_eq!(trace["ok"], false);
    assert_eq!(trace["error"]["code"], "invalid_tool_arguments");
    let call = failure
        .source_refs
        .iter()
        .find(|reference| reference.kind == ArtifactKind::ToolCall)
        .and_then(|reference| store.artifact(&reference.artifact_id).ok())
        .expect("invalid tool call is durable");
    let call_trace: Value = serde_json::from_slice(&store.read_blob(&call.blob).unwrap()).unwrap();
    assert_eq!(call_trace["call"]["arguments"], json!({"unexpected": true}));
}

#[tokio::test]
async fn agent_runtime_records_and_rejects_an_overdue_model_turn() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_wall_time_secs = 1);
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &SlowOutputModel,
                Utc::now(),
            )
            .await,
        Err(ResearchError::WallTimeExceeded { maximum_secs: 1 })
    ));
    store.verify_integrity().unwrap();
}
