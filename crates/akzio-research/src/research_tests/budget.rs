#[tokio::test]
async fn agent_runtime_records_complete_tool_trace_and_contract_validated_claim() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    contract.budget.max_output_tokens = 1_024;
    contract.contract_hash = contract.expected_hash().unwrap();
    let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
    let node = WorkflowNode {
        task_id: akzio_domain::TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: Some(contract.contract_hash.clone()),
        objective: "claim".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: contract.budget.clone(),
        retry: contract.retry.clone(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "test".to_owned(),
        nodes: vec![node.clone()],
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        None,
        vec![],
        Utc::now(),
    )
    .unwrap();
    let run = StoredRun {
        run_id: akzio_domain::RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
        .unwrap()
        .unwrap();
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_bytes(br#"{"price":100}"#, "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        Some(claimed.permit.artifact_origin()),
        vec![],
        Utc::now(),
    )
    .unwrap();
    store
        .write_task_artifact(
            &claimed.permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = ToolThenOutputModel {
        evidence_id: evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let output = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &model,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(output.kind, ArtifactKind::Claim);
    assert!(matches!(
        store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(id)) if id == output.artifact_id
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);
    assert!(output
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::ContextManifest));
    assert!(output
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::AgentTurn));
    assert!(output.source_refs.iter().any(|source| {
        source.kind == ArtifactKind::NormalizedEvidence
            && source.artifact_id == evidence.artifact_id
    }));
    let tool_result = output
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ToolResult)
        .expect("output retains the tool result trace");
    let tool_result = store.artifact(&tool_result.artifact_id).unwrap();
    assert!(tool_result
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::ToolCall));
    assert!(tool_result
        .source_refs
        .iter()
        .any(|source| source.artifact_id == evidence.artifact_id));
    let tool_trace: Value =
        serde_json::from_slice(&store.read_blob(&tool_result.blob).unwrap()).unwrap();
    assert!(tool_trace["request_hash"].as_str().is_some());
    let tool_call = tool_result
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ToolCall)
        .and_then(|source| store.artifact(&source.artifact_id).ok())
        .expect("tool call trace is durable");
    let tool_call_trace: Value =
        serde_json::from_slice(&store.read_blob(&tool_call.blob).unwrap()).unwrap();
    assert_eq!(
        tool_call_trace["call"]["arguments"]["artifact_id"],
        evidence.artifact_id.0.as_str()
    );
    let turn_trace = output
        .source_refs
        .iter()
        .filter(|source| source.kind == ArtifactKind::AgentTurn)
        .filter_map(|source| store.artifact(&source.artifact_id).ok())
        .map(|artifact| {
            serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .find(|trace| trace["response"]["model_debug"]["request"]["fixture"] == "provider-request")
        .expect("agent turn trace retains request and response");
    assert!(turn_trace["request_hash"].as_str().is_some());
    let capability_snapshot = turn_trace["capability_snapshot"].clone();
    assert_eq!(capability_snapshot["provider_id"], "fixture");
    assert_eq!(
        turn_trace["capability_snapshot_hash"],
        serde_json::to_value(capability_snapshot_hash(&fixture_capabilities()).unwrap()).unwrap()
    );
    assert!(turn_trace["tool_set_hash"].as_str().is_some());
    assert_eq!(turn_trace["request"]["tools"][0]["strict"], true);
    assert_eq!(
        turn_trace["response"]["model_debug"]["request"]["fixture"],
        "provider-request"
    );
    assert_eq!(
        turn_trace["response"]["model_debug"]["result"]["fixture"],
        "provider-result"
    );
    let failed_turn_trace = output
        .source_refs
        .iter()
        .filter(|source| source.kind == ArtifactKind::AgentTurn)
        .filter_map(|source| store.artifact(&source.artifact_id).ok())
        .map(|artifact| {
            serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .find(|trace| trace["error_class"] == "transport")
        .expect("failed agent turn trace is durable");
    assert_eq!(
        failed_turn_trace["model_debug"]["request"]["fixture"],
        "failed-provider-request"
    );
    assert_eq!(
        failed_turn_trace["model_debug"]["result"]["error"],
        "fixture-transport"
    );
    assert_eq!(
        failed_turn_trace["capability_snapshot"],
        capability_snapshot
    );
    assert_eq!(
        failed_turn_trace["capability_snapshot_hash"],
        turn_trace["capability_snapshot_hash"]
    );

    let malformed = FixedModel(submission_turn(json!({"summary": 42})));
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &malformed,
                Utc::now(),
            )
            .await,
        Err(ResearchError::InvalidOutput(_))
    ));

    let too_many_basis_ids = FixedModel(submission_turn(json!({
        "result": {
            "schema_version": V2_DOMAIN_SCHEMA_VERSION,
            "topic": "bounded deliberation",
            "statement": "The governed evidence supports a bounded neutral claim.",
            "horizon": "t1",
            "stance": "neutral",
            "materiality_ppm": 100_000,
            "confidence_ppm": 900_000,
            "grounds": [{
                "evidence": {
                    "artifact_id": evidence.artifact_id.0.as_str(),
                    "kind": "normalized_evidence"
                },
                "support": "The governed evidence is selected.",
                "role": "descriptive",
                "assets": [],
                "domain": null
            }],
            "evidence_gaps": []
        },
        "deliberation": {
            "selected_path": "Use governed evidence.",
            "alternatives": [],
            "alternative_match_ppm": [],
            "uncertainties": [],
            "uncertainty_weight_ppm": [],
            "basis_artifact_ids": vec![evidence.artifact_id.0.as_str(); 9],
            "confidence_ppm": 900_000
        }
    })));
    let error = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &too_many_basis_ids,
            Utc::now(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ResearchError::InvalidOutput(_)),
        "{error:?}"
    );

    let denied_tool = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-denied-raw".to_owned(),
        name: "read_raw_evidence".to_owned(),
        arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
    }));
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &denied_tool,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolNotGranted(_))
    ));

    let over_budget_tools = FixedModel(AgentModelTurn {
        assistant_text: None,
        tool_calls: (0..3)
            .map(|index| AgentToolCall {
                call_id: format!("fixture-over-budget-{index}"),
                name: "read_artifact".to_owned(),
                arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
            })
            .collect(),
        terminal_submission: None,
        continuation: fixture_continuation("over budget tools"),
        telemetry: None,
        model_debug: None,
    });
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &over_budget_tools,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolBudgetExceeded)
    ));

    let mut mismatched_node = claimed.node.clone();
    mismatched_node.budget.max_tool_calls = 1;
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &mismatched_node,
                std::iter::empty::<ArtifactRef>(),
                &malformed,
                Utc::now(),
            )
            .await,
        Err(ResearchError::NodePolicyMismatch)
    ));
    store
        .commit_attempt(
            &claimed.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    let persisted = store.artifact(&output.artifact_id).unwrap();
    assert_eq!(persisted.artifact_id, output.artifact_id);
    assert_eq!(persisted.kind, output.kind);
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_enforces_input_and_output_token_budgets() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_input_tokens = 1);
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let output = FixedModel(submission_turn(json!({"summary":"source-linked claim"})));
    let result = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &output,
            Utc::now(),
        )
        .await;
    assert!(
        matches!(&result, Err(ResearchError::InputBudgetExceeded { .. })),
        "{result:?}"
    );
    store.verify_integrity().unwrap();

    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_output_tokens = 1);
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
                &output,
                Utc::now(),
            )
            .await,
        Err(ResearchError::OutputBudgetExceeded { .. })
    ));
    store.verify_integrity().unwrap();
}
