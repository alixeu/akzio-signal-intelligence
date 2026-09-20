use akzio_context::{ContextBroker, ContextError};
use akzio_domain::*;
use akzio_store::{Store, StoreError, StoredRun, WorkflowCommit};
use chrono::{Duration, Utc};

#[test]
fn recovered_attempt_cannot_reuse_a_still_unexpired_context_grant() {
    let run_id = RunId::new();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/audit-20260919/tests/context-permit")
        .join(&run_id.0);
    let store = Store::open(root).unwrap();
    let now = Utc::now();
    let document = store
        .stage_json(&serde_json::json!({"type": "object"}))
        .unwrap();
    let budget = TaskBudget {
        max_input_tokens: 1000,
        max_output_tokens: 100,
        max_wall_time_secs: 30,
        max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(1),
    };
    let retry = RetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 1,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: true,
    };
    let contract = AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("test.context").unwrap(),
        "Test context fencing",
        PromptBundle {
            version: 1,
            governance: document.clone(),
            role: document.clone(),
        },
        ContextPolicy {
            permitted_kinds: [
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
            ]
            .into_iter()
            .collect(),
            permitted_source_families: Default::default(),
            min_artifacts: 0,
            max_artifacts: 2,
            max_bytes: 4096,
            max_source_bytes: None,
            max_tokens: 1000,
            allow_raw_reread: false,
        },
        vec![],
        vec![],
        OutputContract {
            artifact_kind: ArtifactKind::EvidenceNeed,
            schema: document,
        },
        budget.clone(),
        retry.clone(),
        TerminationPolicy::leaf(),
        FailureDisposition::FailTask,
    )
    .unwrap();
    store.install_active_contract(&contract, now).unwrap();
    let node = WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("test.context").unwrap(),
        contract_hash: Some(contract.contract_hash.clone()),
        objective: "context fencing".into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget,
        retry,
        on_failure: FailureDisposition::FailTask,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "context-permit-test".into(),
        agent_budgets: Default::default(),
        nodes: vec![
            node.clone(),
            WorkflowNode {
                spec: None,
                task_id: TaskId::new(),
                dependencies: vec![node.task_id.clone()],
                parent_task_id: Some(node.task_id.clone()),
                ..node.clone()
            },
        ],
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
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    store
        .commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id: run_id.clone(),
                purpose: RunPurpose::Debug,
                topology_id: graph.topology_id,
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let attempt = store
        .claim_next_task_for_workload(
            "context-worker",
            now,
            Duration::seconds(30),
            akzio_store::TaskWorkload::Any,
        )
        .unwrap()
        .unwrap();
    let broker = ContextBroker::new(store.clone());
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .stage_json(
                &serde_json::json!({"resource":"option_chain:QQQ:2026-09-22:2026-10-22",
            "source":"alpaca","value":{"snapshots":{},"pagination_complete":true}}),
            )
            .unwrap(),
        "evidence.normalize",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "alpaca".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: Some(contract.contract_hash.clone()),
        },
        Some(attempt.permit.artifact_origin()),
        vec![],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &attempt.permit,
            &evidence,
            LifecycleEventType::ArtifactCommitted,
            now,
        )
        .unwrap();
    let evidence_ref = ArtifactRef {
        artifact_id: evidence.artifact_id.clone(),
        kind: evidence.kind,
    };
    let manifest = broker
        .assemble(
            &attempt.permit,
            &contract,
            &ContextQueryScope::for_node(&node),
            vec![evidence_ref.clone()],
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert!(broker
        .search_context(&attempt.permit, &contract, &manifest.grant, "QQQ", 1, now)
        .is_ok());
    let original_materialization = broker
        .materialize_for_agent_with_budget(&attempt.permit, &contract, &manifest, &node.budget, now)
        .unwrap();
    let mut forged_manifest = manifest.clone();
    forged_manifest.grant.readable.clear();
    assert!(matches!(
        broker.materialize_for_agent_with_budget(
            &attempt.permit,
            &contract,
            &forged_manifest,
            &node.budget,
            now,
        ),
        Err(ContextError::InvalidManifestClosure)
    ));
    // Store recovery revokes the old attempt while its independent read-grant TTL is still live.
    let recovered_at = now + Duration::seconds(31);
    assert_eq!(store.recover_expired_tasks(recovered_at).unwrap(), 1);
    let replacement = store
        .claim_next_task_for_workload(
            "replacement",
            recovered_at,
            Duration::seconds(30),
            akzio_store::TaskWorkload::Any,
        )
        .unwrap()
        .unwrap();
    assert!(replacement.permit.epoch > attempt.permit.epoch);
    assert!(matches!(
        broker.materialize_for_agent_with_budget(
            &attempt.permit,
            &contract,
            &manifest,
            &node.budget,
            recovered_at,
        ),
        Err(ContextError::Store(StoreError::StalePermit(_)))
    ));
    assert!(matches!(
        broker.search_context(
            &attempt.permit,
            &contract,
            &manifest.grant,
            "QQQ",
            1,
            recovered_at
        ),
        Err(ContextError::Store(StoreError::StalePermit(_)))
    ));
    let replacement_manifest = broker
        .assemble(
            &replacement.permit,
            &contract,
            &ContextQueryScope::for_node(&node),
            vec![evidence_ref],
            recovered_at,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(
        manifest.payload, replacement_manifest.payload,
        "identical option evidence must retain its projection ID across recovery"
    );
    assert_ne!(
        manifest.grant.attempt_id,
        replacement_manifest.grant.attempt_id
    );
    let before = broker
        .materialize_for_agent_with_budget(
            &replacement.permit,
            &contract,
            &replacement_manifest,
            &node.budget,
            recovered_at,
        )
        .unwrap();
    assert_eq!(
        before.ledger[0].document_id,
        manifest.payload.selections[0].artifact.artifact_id
    );
    assert_eq!(
        original_materialization.read_grant_identity,
        before.read_grant_identity
    );
    assert_eq!(
        original_materialization.materialization_identity,
        before.materialization_identity
    );
    assert!(broker
        .search_context(
            &replacement.permit,
            &contract,
            &replacement_manifest.grant,
            "QQQ",
            1,
            recovered_at
        )
        .is_ok());
    let output = Artifact::new(
        ArtifactKind::EvidenceNeed,
        store
            .stage_json(&EvidenceNeed {
                schema_version: DOMAIN_SCHEMA_VERSION,
                source_family: "alpaca".into(),
                resource: "paper.account".into(),
                max_age_secs: 30,
            })
            .unwrap(),
        "test.context",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".into(),
            observed_at: None,
            retrieved_at: recovered_at,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: replacement.permit.contract_hash.clone(),
        },
        Some(replacement.permit.artifact_origin()),
        vec![ArtifactRef {
            artifact_id: replacement_manifest.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }],
        recovered_at,
    )
    .unwrap();
    store
        .commit_attempt(
            &replacement.permit,
            &[output],
            TaskStatus::Succeeded,
            recovered_at,
        )
        .unwrap();
    let child = store
        .claim_next_task_for_workload(
            "child",
            recovered_at,
            Duration::seconds(30),
            akzio_store::TaskWorkload::Any,
        )
        .unwrap()
        .unwrap();
    let child_manifest = broker
        .assemble_child(
            &replacement.permit,
            &contract,
            &replacement_manifest,
            &ContextProjection {
                parent_manifest: ArtifactRef {
                    artifact_id: replacement_manifest.artifact.artifact_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                },
                allowed: vec![],
                reason: "successful parent proof remains usable".into(),
            },
            &child.permit,
            &contract,
            recovered_at,
            Duration::minutes(5),
        )
        .unwrap();
    assert!(broker
        .search_context(
            &child.permit,
            &contract,
            &child_manifest.grant,
            "QQQ",
            1,
            recovered_at
        )
        .is_ok());
}
