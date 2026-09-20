use super::*;
use akzio_domain::WorkflowGraph;
use akzio_store::{StoredRun, WorkflowCommit};

#[test]
fn configured_output_is_frozen_in_workflow_and_survives_config_reload() {
    let run_id = RunId::new();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/llm-chain-20260920/budget-freeze-tests")
        .join(&run_id.0);
    let store = Store::open(root).unwrap();
    let now = Utc::now();
    let catalogue = ActiveResearchCatalogue::install(&store, now).unwrap();
    let mut configured = akzio_domain::AgentBudgetConfig::default();
    configured.default.max_output_tokens = Some(250_000);
    configured.analyst.max_output_tokens = Some(75_000);
    configured.critic.max_output_tokens = Some(16_000);
    let runtime = akzio_runtime::WorkflowRuntime::new(store.clone(), catalogue.recipes.clone())
        .with_agent_budgets(&configured)
        .unwrap();
    let graph = runtime
        .lower(
            RunPurpose::PositionPlan,
            &runtime
                .approved_research_proposal("frozen-output-config")
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        graph.agent_budgets[RESEARCH_ANALYST_RECIPE_ID].max_output_tokens,
        75_000
    );
    assert_eq!(
        graph.agent_budgets[RESEARCH_CRITIC_RECIPE_ID].max_output_tokens,
        16_000
    );
    assert_eq!(
        graph.agent_budgets[RESEARCH_SYNTHESIZER_RECIPE_ID].max_output_tokens,
        250_000
    );
    for node in &graph.nodes {
        if let Some(budget) = graph.agent_budgets.get(node.recipe_id.as_str()) {
            assert_eq!(&node.budget, budget);
        }
    }
    let artifact = runtime
        .submit(run_id.clone(), RunPurpose::PositionPlan, graph.clone(), now)
        .unwrap();
    let frozen_bytes = store.read_blob(&artifact.blob).unwrap();
    configured.default.max_output_tokens = Some(1_000_000);
    configured.analyst.max_output_tokens = Some(500_000);
    let changed = akzio_runtime::WorkflowRuntime::new(store.clone(), catalogue.recipes)
        .with_agent_budgets(&configured)
        .unwrap()
        .lower(
            RunPurpose::PositionPlan,
            &runtime
                .approved_research_proposal("changed-output-config")
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        changed.agent_budgets[RESEARCH_ANALYST_RECIPE_ID].max_output_tokens,
        500_000
    );
    let restored = store.workflow_snapshot(&run_id).unwrap();
    let persisted: WorkflowGraph =
        serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
    assert_eq!(persisted, graph);
    for task in restored.tasks {
        assert_eq!(
            task.node.budget,
            graph
                .nodes
                .iter()
                .find(|node| node.task_id == task.node.task_id)
                .unwrap()
                .budget
        );
    }
    assert_eq!(store.read_blob(&artifact.blob).unwrap(), frozen_bytes);
}

#[test]
fn canonical_upgrade_from_47_rejects_new_old_tasks_and_preserves_cas() {
    assert_canonical_upgrade_preserves_frozen_contract(
        47,
        28,
        akzio_domain::budget::legacy_contract_budget(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap(),
    );
}

#[test]
fn canonical_upgrade_from_49_rejects_new_old_tasks_and_preserves_cas() {
    assert_canonical_upgrade_preserves_frozen_contract(
        49,
        29,
        akzio_domain::budget::versioned_contract_budget(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap(),
    );
}

fn assert_canonical_upgrade_preserves_frozen_contract(
    previous_version: u32,
    previous_prompt_version: u32,
    previous_budget: akzio_domain::TaskBudget,
) {
    let run_id = RunId::new();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/llm-chain-20260920/contract-migration-tests")
        .join(&run_id.0);
    let store = Store::open(root).unwrap();
    let now = Utc::now();
    // Synthetic frozen old-version contract tests migration mechanics. It does
    // not purport to recreate any historical contract's prompt or hash.
    let mut previous = canonical_active_contracts(&store)
        .unwrap()
        .into_iter()
        .find(|contract| contract.purpose.as_str() == RESEARCH_SYNTHESIZER_RECIPE_ID)
        .unwrap();
    previous.version = previous_version;
    previous.prompt.version = previous_prompt_version;
    previous.budget = previous_budget;
    previous.contract_hash = previous.expected_hash().unwrap();
    let frozen = store.install_active_contract(&previous, now).unwrap();
    let original_blob = store.read_blob(&frozen.artifact.blob).unwrap();
    let task_id = TaskId::new();
    let node = WorkflowNode {
        spec: None,
        task_id,
        recipe_id: TaskRecipeId::new(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap(),
        contract_hash: Some(previous.contract_hash.clone()),
        objective: "migration boundary fixture".into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: previous.budget.clone(),
        retry: previous.retry.clone(),
        on_failure: previous.on_failure,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "contract-migration-fixture".into(),
        nodes: vec![node.clone()],
        agent_budgets: Default::default(),
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.stage_json(&graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(akzio_domain::ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let rejected = store
        .commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id: run_id.clone(),
                purpose: RunPurpose::PositionPlan,
                topology_id: graph.topology_id,
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: vec![node],
        })
        .unwrap_err();
    assert!(rejected.to_string().contains("legacy_workflow_retired"));
    ActiveResearchCatalogue::install(&store, now).unwrap();
    assert_eq!(
        store
            .active_contract(&previous.purpose)
            .unwrap()
            .unwrap()
            .contract
            .version,
        ACTIVE_CONTRACT_VERSION
    );
    let old = store
        .contract_installation(&previous.contract_hash)
        .unwrap()
        .unwrap();
    assert_eq!(old.contract, previous);
    assert_eq!(old.artifact.artifact_id, frozen.artifact.artifact_id);
    assert_eq!(store.read_blob(&old.artifact.blob).unwrap(), original_blob);
}

#[test]
fn outcome_release_identity_remains_exactly_frozen() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/outcome-identity")
        .join(RunId::new().0);
    let store = Store::open(root).unwrap();
    let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
    let outcome = active
        .contracts
        .contracts()
        .find(|c| c.contract.purpose.as_str() == LEARNING_OUTCOME_WORKER_RECIPE_ID)
        .unwrap();
    assert_eq!(outcome.contract.version, 63);
    assert_eq!(outcome.contract.prompt.version, 35);
    assert_eq!(
        outcome.contract.contract_hash.as_str(),
        "c9556a7ca9000cd06b96e385876013e3ce06a330fb4d01a473a43ef2db067ddf"
    );
    for research in active
        .contracts
        .contracts()
        .filter(|c| c.contract.purpose.as_str() != LEARNING_OUTCOME_WORKER_RECIPE_ID)
    {
        assert_eq!(research.contract.version, 69);
        assert_eq!(research.contract.prompt.version, 38);
        assert!(research.contract.tool_specs.is_empty());
        assert!(research.contract.tool_grants.is_empty());
    }
}
