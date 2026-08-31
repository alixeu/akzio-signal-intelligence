fn contract(store: &V2Store) -> AgentContract {
    AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.analyst").unwrap(),
        "produce a claim",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"governance", "text/plain").unwrap(),
            role: store.put_bytes(b"prompt", "text/plain").unwrap(),
        },
        ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: false,
        },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["market".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read granted artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_json(&artifact_id_tool_input_schema()).unwrap(),
            strict: true,
        }],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store.put_json(&claim_output_schema()).unwrap(),
        },
        TaskBudget {
            max_input_tokens: 4096,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
        },
        RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        TerminationPolicy::leaf(),
        FailureDisposition::FailRun,
    )
    .unwrap()
}

fn provenance() -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: "market".to_owned(),
        observed_at: None,
        retrieved_at: Utc::now(),
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    store: V2Store,
    catalogue: ContractCatalogue,
    claimed: akzio_store::v2::ClaimedAttempt,
    evidence: Artifact,
}

fn fixture_with(configure: impl FnOnce(&mut AgentContract)) -> Fixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    configure(&mut contract);
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.candidate_capability_ceiling.tool_grants = contract.tool_grants.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
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
        purpose: akzio_domain::RunPurpose::Debug,
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
    Fixture {
        _root: root,
        store,
        catalogue,
        claimed,
        evidence,
    }
}
