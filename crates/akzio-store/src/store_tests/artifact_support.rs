fn artifact(
    store: &V2Store,
    kind: ArtifactKind,
    value: &str,
    origin: Option<ArtifactOrigin>,
) -> Artifact {
    artifact_with_refs(store, kind, value, origin, vec![])
}

fn artifact_with_refs(
    store: &V2Store,
    kind: ArtifactKind,
    value: &str,
    origin: Option<ArtifactOrigin>,
    source_refs: Vec<ArtifactRef>,
) -> Artifact {
    let producer_contract_hash = origin
        .as_ref()
        .and_then(|origin| origin.contract_hash.clone());
    Artifact::new(
        kind,
        store
            .put_bytes(value.as_bytes(), "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash,
        },
        origin,
        source_refs,
        Utc::now(),
    )
    .unwrap()
}

fn graph() -> WorkflowGraph {
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: None,
            objective: "analyze".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        }],
    }
}

fn permit_artifact<T: Serialize>(
    store: &V2Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    payload: &T,
    source_refs: Vec<ArtifactRef>,
    lifecycle: ArtifactLifecycle,
    now: DateTime<Utc>,
) -> Artifact {
    Artifact::new(
        kind,
        store.put_json(payload).unwrap(),
        "fixture.policy",
        lifecycle,
        ArtifactProvenance {
            source_family: "fixture.policy".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        source_refs,
        now,
    )
    .unwrap()
}

struct TaskArtifactFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    run: StoredRun,
    permit: TaskWritePermit,
    now: DateTime<Utc>,
}

fn task_artifact_fixture(purpose: RunPurpose) -> TaskArtifactFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    TaskArtifactFixture {
        _root: root,
        store,
        run,
        permit,
        now,
    }
}

fn lifecycle_test_artifact(
    fixture: &TaskArtifactFixture,
    lifecycle: ArtifactLifecycle,
    label: &str,
) -> Artifact {
    permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"label": label}),
        vec![],
        lifecycle,
        fixture.now,
    )
}
