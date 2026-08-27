use akzio_domain::{
    ArtifactLifecycle, ArtifactProvenance, Asset, ContextPolicy, ExecutionPlan, FactorExposure,
    FailureDisposition, HardBlocker, MoneyMicros, NoOrder, OrderIntent, OrderSide, OutputContract,
    PaperApprovalScope, PaperCommitment, PaperCommitmentId, PromptBundle, RetryPolicy,
    RuntimeManifest, TargetPortfolio, TaskBudget, TaskRecipeId, TerminationPolicy, ToolGrant,
    ToolKind, ToolSpec, WeightPpm, WorkflowProposalTask,
};
use chrono::NaiveDate;
use tempfile::tempdir;

use super::*;

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 32,
        max_output_tokens: 16,
        max_wall_time_secs: 10,
        max_tool_calls: 1,
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 1,
        initial_backoff_ms: 1,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: false,
    }
}

#[test]
fn poisoned_connection_returns_integrity_error() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let connection = store.connection.clone();
    assert!(std::thread::spawn(move || {
        let _guard = connection.lock().unwrap();
        panic!("poison fixture connection");
    })
    .join()
    .is_err());

    assert!(matches!(
        store.metrics(Utc::now()),
        Err(StoreError::Integrity(message)) if message == "store connection poisoned"
    ));
}

fn contract(store: &V2Store, version: u32) -> AgentContract {
    AgentContract::new(
            ContractId::new(),
        version,
        ContractPurpose::new("research.fixture").unwrap(),
        "fixture contract",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"fixture governance", "text/plain").unwrap(),
            role: store.put_bytes(b"fixture prompt", "text/plain").unwrap(),
        },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["fixture".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["fixture".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read fixture artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_bytes(b"fixture tool schema", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store
                    .put_bytes(
                        br#"{"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"],"additionalProperties":false}"#,
                        "application/json",
                    )
                    .unwrap(),
            },
            budget(),
            retry(),
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap()
}

#[test]
fn contract_catalogue_rejects_duplicate_or_expanded_installations_and_doctor_corruption() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut duplicate = active.clone();
    duplicate.responsibility = "same identity, different contract".to_owned();
    duplicate.contract_hash = duplicate.expected_hash().unwrap();
    duplicate.validate().unwrap();
    assert!(matches!(
        store.install_active_contract(&duplicate, now),
        Err(StoreError::DuplicateContractVersion { .. })
    ));

    let mut expanded = active.clone();
    expanded.version = 2;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        store.install_candidate_contract(&active.contract_hash, &expanded, now),
        Err(StoreError::ContractCapabilityExpansion { .. })
    ));

    let mut candidate = active.clone();
    candidate.version = 2;
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();
    let stored_candidate = store
        .install_candidate_contract(&active.contract_hash, &candidate, now)
        .unwrap();
    assert_eq!(stored_candidate.contract, candidate);
    assert_eq!(
        store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        active.contract_hash
    );
    store.verify_integrity().unwrap();

    store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_contract_installations \
                 SET contract_id = ?1 WHERE contract_hash = ?2",
            params!["forged-contract-id", active.contract_hash.as_str()],
        )
        .unwrap();
    assert!(matches!(
        store.verify_integrity(),
        Err(StoreError::Integrity(_))
    ));
}

#[test]
fn observatory_configuration_round_trips_and_clears() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let configuration = serde_json::json!({
        "llm_api_key": "fixture-llm-key",
        "alpaca_api_secret": "fixture-alpaca-secret",
        "model": "fixture-model"
    });

    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        None
    );
    store.set_observatory_configuration(&configuration).unwrap();
    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        Some(configuration)
    );
    assert!(store.clear_observatory_configuration().unwrap());
    assert!(!store.clear_observatory_configuration().unwrap());
    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        None
    );
}

#[test]
fn canonical_contract_upgrade_is_monotonic_bounded_and_preserves_history() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut upgraded = active.clone();
    upgraded.version = 2;
    upgraded.responsibility = "bounded canonical runtime upgrade".to_owned();
    upgraded.contract_hash = upgraded.expected_hash().unwrap();
    upgraded.validate().unwrap();
    let stored = store
        .install_canonical_contract_upgrade(
            &active.contract_hash,
            &upgraded,
            now + Duration::seconds(1),
        )
        .unwrap();

    assert_eq!(stored.contract, upgraded);
    assert_eq!(
        stored.baseline_contract_hash,
        Some(active.contract_hash.clone())
    );
    assert!(stored.activated_at.is_some());
    assert_eq!(
        store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        upgraded.contract_hash
    );
    assert!(store
        .contract_installation(&active.contract_hash)
        .unwrap()
        .is_some());

    let mut expanded = upgraded.clone();
    expanded.version = 3;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        store.install_canonical_contract_upgrade(
            &upgraded.contract_hash,
            &expanded,
            now + Duration::seconds(2),
        ),
        Err(StoreError::ContractCapabilityExpansion { .. })
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn canonical_contract_upgrade_rejects_nonterminal_tasks_and_lists_blockers() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut graph = graph();
    graph.nodes[0].contract_hash = Some(active.contract_hash.clone());
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes.clone(),
        })
        .unwrap();

    let mut upgraded = active.clone();
    upgraded.version = 2;
    upgraded.responsibility = "blocked upgrade".to_owned();
    upgraded.contract_hash = upgraded.expected_hash().unwrap();
    let error = store
        .install_canonical_contract_upgrade(
            &active.contract_hash,
            &upgraded,
            now + Duration::seconds(1),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::ContractUpgradeBlocked { active: hash, blockers }
            if hash == active.contract_hash
                && blockers.contains(run.run_id.0.as_str())
                && blockers.contains(graph.nodes[0].task_id.0.as_str())
    ));
}
