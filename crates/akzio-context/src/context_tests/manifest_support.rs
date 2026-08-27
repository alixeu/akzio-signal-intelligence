fn permit(store: &V2Store) -> TaskWritePermit {
    permit_for_purpose(store, RunPurpose::Debug)
}

fn permit_for_purpose(store: &V2Store, purpose: RunPurpose) -> TaskWritePermit {
    permit_for_contract(store, purpose, None)
}

fn permit_for_contract(
    store: &V2Store,
    purpose: RunPurpose,
    contract_hash: Option<akzio_domain::ContentHash>,
) -> TaskWritePermit {
    let node = WorkflowNode {
        task_id: akzio_domain::TaskId::new(),
        recipe_id: akzio_domain::TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash,
        objective: "analyze".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: TaskBudget {
            max_input_tokens: 1024,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
        },
        retry: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
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
        provenance("fixture"),
        None,
        vec![],
        Utc::now(),
    )
    .unwrap();
    let run = StoredRun {
        run_id: akzio_domain::RunId::new(),
        purpose,
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
    store
        .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
        .unwrap()
        .unwrap()
        .permit
}

fn provenance(source_family: &str) -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: source_family.to_owned(),
        observed_at: None,
        retrieved_at: Utc::now(),
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    }
}

fn task_artifact(
    store: &V2Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    source_refs: Vec<ArtifactRef>,
    value: &str,
) -> Artifact {
    Artifact::new(
        kind,
        store
            .put_bytes(value.as_bytes(), "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance("market"),
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        source_refs,
        Utc::now(),
    )
    .unwrap()
}

fn manifest_fixture() -> (
    tempfile::TempDir,
    V2Store,
    TaskWritePermit,
    AgentContract,
    ContextManifest,
    ArtifactRef,
    DateTime<Utc>,
) {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();
    let raw = task_artifact(&store, &permit, ArtifactKind::RawEvidence, vec![], "raw");
    store
        .write_task_artifact(&permit, &raw, LifecycleEventType::EvidenceRaw, now)
        .unwrap();
    let raw_ref = ArtifactRef {
        artifact_id: raw.artifact_id,
        kind: raw.kind,
    };
    let normalized = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![raw_ref.clone()],
        "normalized",
    );
    store
        .write_task_artifact(
            &permit,
            &normalized,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    let manifest = ContextBroker::new(store.clone())
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: normalized.artifact_id,
                kind: normalized.kind,
            }],
            now,
            Duration::minutes(5),
        )
        .unwrap();
    (root, store, permit, contract, manifest, raw_ref, now)
}

fn persist_manifest_payload(
    store: &V2Store,
    permit: &TaskWritePermit,
    original: &ContextManifest,
    payload: ContextManifestPayload,
    now: DateTime<Utc>,
) -> ContextManifest {
    let artifact = Artifact::new(
        ArtifactKind::ContextManifest,
        store.put_json(&payload).unwrap(),
        original.artifact.producer.clone(),
        original.artifact.lifecycle,
        original.artifact.provenance.clone(),
        original.artifact.origin.clone(),
        original.artifact.source_refs.clone(),
        original.artifact.created_at,
    )
    .unwrap();
    store
        .write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ContextManifestCreated,
            now,
        )
        .unwrap();
    let mut grant = original.grant.clone();
    grant.manifest_artifact_id = artifact.artifact_id.clone();
    ContextManifest {
        artifact,
        payload,
        grant,
    }
}
