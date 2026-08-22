use std::collections::BTreeSet;

use akzio_domain::{
    ArtifactKind, CandidatePolicyState, ContractId, ContractPurpose, FailureDisposition,
    MemoryLifecycle, OutputContract, PromptBundle, RetryPolicy, RunPurpose, TaskBudget,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowGraph, WorkflowNode,
    V2_SCHEMA_VERSION,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use tempfile::tempdir;

use super::*;

fn contract(store: &V2Store) -> AgentContract {
    AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.analyst").unwrap(),
        "analyze",
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
            allow_raw_reread: true,
        },
        vec![ToolGrant {
            kind: ToolKind::ReadRawEvidence,
            allowed_sources: vec!["market".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_raw_evidence".to_owned(),
            description: "read granted raw evidence".to_owned(),
            kind: ToolKind::ReadRawEvidence,
            input_schema: store.put_bytes(b"tool schema", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store.put_bytes(b"schema", "application/json").unwrap(),
        },
        TaskBudget {
            max_input_tokens: 1024,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
        },
        RetryPolicy {
            max_attempts: 1,
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

#[test]
fn assemble_injects_active_lessons_when_contract_allows_operator_knowledge() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    contract
        .context
        .permitted_kinds
        .insert(ArtifactKind::Lesson);
    contract
        .context
        .permitted_source_families
        .insert("akzio.operator".to_owned());
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        store
            .put_json(&serde_json::json!({"operator": "source"}))
            .unwrap(),
        "operator.lesson.source",
        ArtifactLifecycle::Canonical,
        provenance("akzio.operator"),
        None,
        vec![],
        now,
    )
    .unwrap();
    let lesson = akzio_domain::Lesson {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        lesson_id: akzio_domain::LessonId::new(),
        origin: akzio_domain::LessonOrigin::Operator,
        lifecycle: akzio_domain::LessonLifecycle::Draft,
        title: "Opening volatility".to_owned(),
        statement: "Require stronger evidence after a noisy open.".to_owned(),
        rationale: "The initial quote window is unstable.".to_owned(),
        recommended_behavior: "Wait for confirmation.".to_owned(),
        exclusions: vec![],
        scope: akzio_domain::LessonScope::default(),
        source_refs: vec![ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }],
        supersedes: vec![],
        conflicts_with: vec![],
        confidence_ppm: 700_000,
        authored_by: Some("operator:test".to_owned()),
        approved_by: None,
        created_at: now,
        updated_at: now,
    };
    store.write_lesson(&lesson, &source, now).unwrap();
    store
        .transition_lesson(
            &lesson.lesson_id,
            akzio_domain::LessonLifecycle::Active,
            "operator:reviewer",
            "approved",
            now + Duration::seconds(1),
        )
        .unwrap();

    let manifest = ContextBroker::new(store)
        .assemble(
            &permit,
            &contract,
            Vec::<ArtifactRef>::new(),
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert!(manifest
        .payload
        .selections
        .iter()
        .any(|selection| selection.artifact.kind == ArtifactKind::Lesson));
}

#[test]
fn assemble_bounds_active_lessons_to_four_candidates() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    contract
        .context
        .permitted_kinds
        .insert(ArtifactKind::Lesson);
    contract
        .context
        .permitted_source_families
        .insert("akzio.operator".to_owned());
    contract.context.max_artifacts = 24;
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();

    for index in 0..6 {
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            store
                .put_json(&serde_json::json!({"operator": index}))
                .unwrap(),
            "operator.lesson.source",
            ArtifactLifecycle::Canonical,
            provenance("akzio.operator"),
            None,
            vec![],
            now,
        )
        .unwrap();
        let lesson = akzio_domain::Lesson {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            lesson_id: akzio_domain::LessonId::new(),
            origin: akzio_domain::LessonOrigin::Operator,
            lifecycle: akzio_domain::LessonLifecycle::Draft,
            title: format!("Opening volatility {index}"),
            statement: "Require stronger evidence at the open.".to_owned(),
            rationale: "The first quote window is noisy.".to_owned(),
            recommended_behavior: "Wait for confirmation.".to_owned(),
            exclusions: vec![],
            scope: akzio_domain::LessonScope::default(),
            source_refs: vec![ArtifactRef {
                artifact_id: source.artifact_id.clone(),
                kind: source.kind,
            }],
            supersedes: vec![],
            conflicts_with: vec![],
            confidence_ppm: 700_000,
            authored_by: Some("operator:test".to_owned()),
            approved_by: None,
            created_at: now,
            updated_at: now,
        };
        store.write_lesson(&lesson, &source, now).unwrap();
        store
            .transition_lesson(
                &lesson.lesson_id,
                akzio_domain::LessonLifecycle::Active,
                "operator:reviewer",
                "approved",
                now + Duration::seconds(i64::from(index)),
            )
            .unwrap();
    }

    let manifest = ContextBroker::new(store)
        .assemble(
            &permit,
            &contract,
            Vec::<ArtifactRef>::new(),
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(
        manifest
            .payload
            .selections
            .iter()
            .filter(|selection| selection.artifact.kind == ArtifactKind::Lesson)
            .count(),
        4
    );
}

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
        schema_version: V2_SCHEMA_VERSION,
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

#[test]
fn restore_manifest_for_proof_accepts_parent_manifest_source_ref() {
    let (_root, store, permit, contract, parent, _raw, now) = manifest_fixture();
    let parent_ref = ArtifactRef {
        artifact_id: parent.artifact.artifact_id.clone(),
        kind: ArtifactKind::ContextManifest,
    };
    let nested = Artifact::new(
        ArtifactKind::ContextManifest,
        store.put_json(&parent.payload).unwrap(),
        parent.artifact.producer.clone(),
        ArtifactLifecycle::RunScoped,
        parent.artifact.provenance.clone(),
        parent.artifact.origin.clone(),
        parent
            .payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .chain(std::iter::once(parent_ref.clone()))
            .collect(),
        now,
    )
    .unwrap();
    let proof = SucceededAttemptProof {
        run_id: permit.run_id.clone(),
        task_id: permit.task_id.clone(),
        attempt_id: permit.attempt_id.clone(),
        lease_id: permit.lease_id.clone(),
        epoch: permit.epoch,
        contract_hash: permit.contract_hash.clone(),
        context_manifest: Some(ArtifactRef {
            artifact_id: nested.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }),
        outputs: Vec::new(),
    };

    let restored = ContextBroker::new(store)
        .restore_manifest_for_proof(&proof, &contract, nested, parent.payload, now)
        .unwrap();

    assert_eq!(restored.grant.readable.len(), 1);
    assert!(!restored.grant.readable.contains(&parent_ref.artifact_id));
}

#[test]
fn context_is_explicit_and_raw_is_only_granted_by_closure() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let raw = task_artifact(&store, &permit, ArtifactKind::RawEvidence, vec![], "raw");
    store
        .write_task_artifact(&permit, &raw, LifecycleEventType::EvidenceRaw, Utc::now())
        .unwrap();
    let normalized = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        }],
        "normalized",
    );
    store
        .write_task_artifact(
            &permit,
            &normalized,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();

    let broker = ContextBroker::new(store.clone());
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            Utc::now(),
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(manifest.payload.selections.len(), 1);
    assert_eq!(
        broker
            .read_raw(
                &permit,
                &contract,
                &manifest.grant,
                &raw.artifact_id,
                Utc::now()
            )
            .unwrap()
            .kind,
        ArtifactKind::RawEvidence
    );
    assert!(matches!(
        broker.read(
            &permit,
            &contract,
            &manifest.grant,
            &raw.artifact_id,
            Utc::now()
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn read_grant_expiry_is_exclusive_for_context_reads() {
    let (_root, store, permit, contract, manifest, raw, _now) = manifest_fixture();
    let broker = ContextBroker::new(store);
    let selected = manifest.payload.selections[0].artifact.artifact_id.clone();
    let just_before = manifest.grant.expires_at - Duration::nanoseconds(1);

    assert!(broker
        .read(&permit, &contract, &manifest.grant, &selected, just_before)
        .is_ok());
    assert!(broker
        .read_raw(
            &permit,
            &contract,
            &manifest.grant,
            &raw.artifact_id,
            just_before
        )
        .is_ok());
    assert!(matches!(
        broker.read(
            &permit,
            &contract,
            &manifest.grant,
            &selected,
            manifest.grant.expires_at
        ),
        Err(ContextError::GrantDenied { .. })
    ));
    assert!(matches!(
        broker.read_raw(
            &permit,
            &contract,
            &manifest.grant,
            &raw.artifact_id,
            manifest.grant.expires_at,
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn unrelated_artifact_is_not_visible_to_the_grant() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let first = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "first",
    );
    let second = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "second",
    );
    store
        .write_task_artifact(&permit, &first, LifecycleEventType::Evidence, Utc::now())
        .unwrap();
    store
        .write_task_artifact(&permit, &second, LifecycleEventType::Evidence, Utc::now())
        .unwrap();
    let broker = ContextBroker::new(store.clone());
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: first.artifact_id.clone(),
                kind: first.kind,
            }],
            Utc::now(),
            Duration::minutes(5),
        )
        .unwrap();
    assert!(matches!(
        broker.read(
            &permit,
            &contract,
            &manifest.grant,
            &second.artifact_id,
            Utc::now()
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn read_rejects_a_forged_readable_set() {
    let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store);
    let mut forged_grant = manifest.grant.clone();
    forged_grant.readable.insert(raw.artifact_id.clone());

    assert!(matches!(
        broker.read(&permit, &contract, &forged_grant, &raw.artifact_id, now),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn read_raw_rejects_a_forged_raw_source_closure() {
    let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store);
    let selected = manifest.payload.selections[0].artifact.artifact_id.clone();
    let mut forged_grant = manifest.grant.clone();
    forged_grant.raw_source_closure.insert(selected);

    assert!(matches!(
        broker.read_raw(&permit, &contract, &forged_grant, &raw.artifact_id, now),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn reads_reject_stale_attempt_identity_and_contract() {
    let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store.clone());
    let selected = &manifest.payload.selections[0].artifact.artifact_id;

    let mut wrong_epoch = permit.clone();
    wrong_epoch.epoch = wrong_epoch.epoch.saturating_add(1);
    assert!(matches!(
        broker.read(
            &wrong_epoch,
            &manifest_contract,
            &manifest.grant,
            selected,
            now
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut wrong_attempt = permit.clone();
    wrong_attempt.attempt_id = akzio_domain::AttemptId::new();
    assert!(matches!(
        broker.read(
            &wrong_attempt,
            &manifest_contract,
            &manifest.grant,
            selected,
            now
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut wrong_lease = permit.clone();
    wrong_lease.lease_id = akzio_domain::LeaseId::new();
    assert!(matches!(
        broker.read(
            &wrong_lease,
            &manifest_contract,
            &manifest.grant,
            selected,
            now
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let wrong_contract = contract(&store);
    assert!(matches!(
        broker.read(&permit, &wrong_contract, &manifest.grant, selected, now),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn bootstrap_policy_can_mint_an_explicit_empty_manifest_only_when_allowed() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let permit = permit(&store);
    let broker = ContextBroker::new(store.clone());

    assert!(matches!(
        broker.assemble(
            &permit,
            &contract(&store),
            std::iter::empty(),
            Utc::now(),
            Duration::minutes(5),
        ),
        Err(ContextError::BudgetExceeded)
    ));

    let mut bootstrap = contract(&store);
    bootstrap.context.min_artifacts = 0;
    bootstrap.candidate_capability_ceiling.context.min_artifacts = 0;
    bootstrap.termination.require_evidence = false;
    bootstrap.contract_hash = bootstrap.expected_hash().unwrap();
    bootstrap.validate().unwrap();

    let manifest = broker
        .assemble(
            &permit,
            &bootstrap,
            std::iter::empty(),
            Utc::now(),
            Duration::minutes(5),
        )
        .unwrap();
    assert!(manifest.payload.selections.is_empty());
    assert!(manifest.grant.readable.is_empty());
    assert!(manifest.grant.raw_source_closure.is_empty());
}

#[test]
fn repair_is_explicit_and_cannot_expand_a_grant() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let normalized = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "normalized",
    );
    let unrelated = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "unrelated",
    );
    store
        .write_task_artifact(
            &permit,
            &normalized,
            LifecycleEventType::Evidence,
            Utc::now(),
        )
        .unwrap();
    store
        .write_task_artifact(
            &permit,
            &unrelated,
            LifecycleEventType::Evidence,
            Utc::now(),
        )
        .unwrap();
    let broker = ContextBroker::new(store.clone());
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            Utc::now(),
            Duration::minutes(5),
        )
        .unwrap();
    let repair = broker
        .record_repair(
            &permit,
            &contract,
            &manifest.grant,
            vec![ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &serde_json::json!({"repair": "fixture"}),
            Utc::now(),
        )
        .unwrap();
    assert_eq!(repair.kind, ArtifactKind::ContextRepair);
    assert_eq!(repair.source_refs[0].artifact_id, normalized.artifact_id);

    let mut stale_grant = manifest.grant.clone();
    stale_grant.epoch = stale_grant.epoch.saturating_add(1);
    assert!(matches!(
        broker.record_repair(
            &permit,
            &contract,
            &stale_grant,
            vec![ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &serde_json::json!({"repair": "stale-grant"}),
            Utc::now(),
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut wrong_contract = contract.clone();
    wrong_contract.context.max_tokens = wrong_contract.context.max_tokens.saturating_sub(1);
    wrong_contract.contract_hash = wrong_contract.expected_hash().unwrap();
    assert!(matches!(
        broker.record_repair(
            &permit,
            &wrong_contract,
            &manifest.grant,
            vec![ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &serde_json::json!({"repair": "wrong-contract"}),
            Utc::now(),
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut forged_grant = manifest.grant.clone();
    forged_grant.readable.insert(unrelated.artifact_id.clone());
    assert!(matches!(
        broker.record_repair(
            &permit,
            &contract,
            &forged_grant,
            vec![ArtifactRef {
                artifact_id: unrelated.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &serde_json::json!({"repair": "forged-closure"}),
            Utc::now(),
        ),
        Err(ContextError::InvalidManifestClosure)
    ));
    assert!(matches!(
        broker.record_repair(
            &permit,
            &contract,
            &manifest.grant,
            vec![ArtifactRef {
                artifact_id: unrelated.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &serde_json::json!({"repair": "forbidden"}),
            Utc::now(),
        ),
        Err(ContextError::GrantDenied { .. })
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn policy_influences_accepts_only_the_persisted_manifest() {
    let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store);
    assert!(broker
        .policy_influences(&permit, &manifest_contract, &manifest, now)
        .unwrap()
        .is_empty());
}

#[test]
fn policy_influences_rejects_a_coherent_in_memory_forgery() {
    let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
    let second = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![raw],
        "second normalized",
    );
    store
        .write_task_artifact(
            &permit,
            &second,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();

    let second_ref = ArtifactRef {
        artifact_id: second.artifact_id,
        kind: second.kind,
    };
    let mut forged = manifest;
    forged.payload.selections[0].artifact = second_ref.clone();
    forged.payload.selections[0].estimated_tokens = estimate_tokens(second.blob.bytes);
    forged.payload.total_bytes = second.blob.bytes;
    forged.payload.estimated_tokens = estimate_tokens(second.blob.bytes);
    forged.payload.input_hash = manifest_input_hash(&forged.payload.selections).unwrap();
    forged.artifact.source_refs = vec![second_ref.clone()];
    forged.grant.readable = BTreeSet::from([second_ref.artifact_id]);

    assert!(matches!(
        ContextBroker::new(store).policy_influences(&permit, &contract, &forged, now,),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn policy_influences_rejects_wrong_permit_contract_and_expiry() {
    let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store.clone());

    let mut wrong_permit = permit.clone();
    wrong_permit.epoch = wrong_permit.epoch.saturating_add(1);
    assert!(matches!(
        broker.policy_influences(&wrong_permit, &manifest_contract, &manifest, now),
        Err(ContextError::InvalidManifestClosure)
    ));

    let wrong_contract = contract(&store);
    assert!(matches!(
        broker.policy_influences(&permit, &wrong_contract, &manifest, now),
        Err(ContextError::InvalidManifestClosure)
    ));

    assert!(matches!(
        broker.policy_influences(
            &permit,
            &manifest_contract,
            &manifest,
            manifest.grant.expires_at,
        ),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn policy_influences_rejects_payload_artifact_and_raw_closure_mismatch() {
    let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store);

    let mut payload_mismatch = manifest.clone();
    payload_mismatch.payload.total_bytes = payload_mismatch.payload.total_bytes.saturating_add(1);
    assert!(matches!(
        broker.policy_influences(&permit, &contract, &payload_mismatch, now),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut artifact_mismatch = manifest.clone();
    artifact_mismatch.artifact.source_refs.clear();
    assert!(matches!(
        broker.policy_influences(&permit, &contract, &artifact_mismatch, now),
        Err(ContextError::InvalidManifestClosure)
    ));

    let mut closure_mismatch = manifest;
    assert!(!closure_mismatch.grant.raw_source_closure.is_empty());
    closure_mismatch.grant.raw_source_closure.clear();
    assert!(matches!(
        broker.policy_influences(&permit, &contract, &closure_mismatch, now),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn policy_influences_recomputes_persisted_input_hash() {
    let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
    let mut payload = manifest.payload.clone();
    payload.input_hash = akzio_domain::ContentHash::of_bytes(b"forged input hash");
    let forged = persist_manifest_payload(&store, &permit, &manifest, payload, now);

    assert!(matches!(
        ContextBroker::new(store).policy_influences(&permit, &contract, &forged, now,),
        Err(ContextError::InvalidManifestClosure)
    ));
}

#[test]
fn overlay_states_only_allow_active_proven_memory_and_active_policies() {
    assert!(overlay_state_is_eligible(
        ArtifactKind::Experience,
        PolicyState::Memory(MemoryLifecycle::Active),
    ));
    assert!(overlay_state_is_eligible(
        ArtifactKind::Experience,
        PolicyState::Memory(MemoryLifecycle::Proven),
    ));
    for state in [
        MemoryLifecycle::Candidate,
        MemoryLifecycle::Contested,
        MemoryLifecycle::Retired,
    ] {
        assert!(!overlay_state_is_eligible(
            ArtifactKind::Experience,
            PolicyState::Memory(state),
        ));
    }

    assert!(overlay_state_is_eligible(
        ArtifactKind::CandidatePolicy,
        PolicyState::Contract(CandidatePolicyState::Active),
    ));
    assert!(overlay_state_is_eligible(
        ArtifactKind::CandidatePolicy,
        PolicyState::Topology(CandidatePolicyState::Active),
    ));
    for state in [
        CandidatePolicyState::Candidate,
        CandidatePolicyState::Canary10,
        CandidatePolicyState::Canary25,
        CandidatePolicyState::Canary50,
    ] {
        assert!(!overlay_state_is_eligible(
            ArtifactKind::CandidatePolicy,
            PolicyState::Contract(state),
        ));
    }
    assert!(!overlay_state_is_eligible(
        ArtifactKind::CandidatePolicy,
        PolicyState::Memory(MemoryLifecycle::Active),
    ));
}

#[test]
fn noncanonical_overlay_is_filtered_before_manifest_write() {
    for kind in [ArtifactKind::Experience, ArtifactKind::CandidatePolicy] {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::Replay,
            RunPurpose::Shadow,
            RunPurpose::PaperDryRun,
        ] {
            let root = tempdir().unwrap();
            let store = V2Store::open(root.path()).unwrap();
            let permit = permit_for_purpose(&store, purpose);
            let now = Utc::now();
            let overlay = Artifact::new(
                kind,
                store
                    .put_json(&serde_json::json!({"noncanonical": true}))
                    .unwrap(),
                "fixture",
                ArtifactLifecycle::Canonical,
                provenance("learning"),
                Some(ArtifactOrigin {
                    run_id: Some(permit.run_id.clone()),
                    task_id: Some(permit.task_id.clone()),
                    attempt_id: Some(permit.attempt_id.clone()),
                    contract_hash: permit.contract_hash.clone(),
                }),
                vec![],
                now,
            )
            .unwrap();
            assert!(matches!(
                store.write_task_artifact(
                    &permit,
                    &overlay,
                    LifecycleEventType::LearningOverlay,
                    now
                ),
                Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.atomic_commit_required"
                ))
            ));
        }
    }
}

#[test]
fn child_projection_from_proof_rejects_stale_parent_and_trace_outputs() {
    let (_root, store, parent_permit, parent_contract, parent, raw, now) = manifest_fixture();
    let child_contract = contract(&store);
    let mut child_permit = parent_permit.clone();
    child_permit.task_id = akzio_domain::TaskId::new();
    child_permit.attempt_id = akzio_domain::AttemptId::new();
    child_permit.lease_id = akzio_domain::LeaseId::new();
    child_permit.contract_hash = Some(child_contract.contract_hash.clone());
    let broker = ContextBroker::new(store.clone());

    let call = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::ToolCall,
        vec![],
        "trace-call",
    );
    store
        .write_task_artifact(&parent_permit, &call, LifecycleEventType::ToolCalled, now)
        .unwrap();
    let trace = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::ToolResult,
        vec![ArtifactRef {
            artifact_id: call.artifact_id.clone(),
            kind: ArtifactKind::ToolCall,
        }],
        "trace",
    );
    store
        .write_task_artifact(
            &parent_permit,
            &trace,
            LifecycleEventType::ToolCompleted,
            now,
        )
        .unwrap();
    store
        .commit_attempt(
            &parent_permit,
            &[parent.artifact.clone(), trace.clone()],
            akzio_domain::TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let current = store
        .current_succeeded_attempt(&parent_permit.run_id, &parent_permit.task_id)
        .unwrap();

    let mut stale = current.clone();
    stale.epoch = stale.epoch.saturating_add(1);
    assert!(matches!(
        broker.assemble_child_from_proof(
            &stale,
            &parent_contract,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let trace_projection = ContextProjection {
        parent_manifest: ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        },
        allowed: vec![ArtifactRef {
            artifact_id: trace.artifact_id.clone(),
            kind: trace.kind,
        }],
        reason: "trace-output".to_owned(),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &trace_projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::GrantDenied { .. })
    ));

    let forged = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::NormalizedEvidence,
        vec![ArtifactRef {
            artifact_id: raw.artifact_id,
            kind: ArtifactKind::RawEvidence,
        }],
        "foreign output",
    );
    let projection = ContextProjection {
        parent_manifest: ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        },
        allowed: vec![ArtifactRef {
            artifact_id: forged.artifact_id,
            kind: forged.kind,
        }],
        reason: "foreign-output".to_owned(),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn deliberation_note_is_projectable_but_agent_turn_is_not() {
    let (_root, store, parent_permit, parent_contract, parent, _raw, now) = manifest_fixture();
    let broker = ContextBroker::new(store.clone());
    let agent_artifact = |kind: ArtifactKind, source_refs: Vec<ArtifactRef>, value: &str| {
        Artifact::new(
            kind,
            store
                .put_bytes(value.as_bytes(), "application/json")
                .unwrap(),
            "agent.research.analyst",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(parent_contract.contract_hash.clone()),
            },
            Some(ArtifactOrigin {
                run_id: Some(parent_permit.run_id.clone()),
                task_id: Some(parent_permit.task_id.clone()),
                attempt_id: Some(parent_permit.attempt_id.clone()),
                contract_hash: parent_permit.contract_hash.clone(),
            }),
            source_refs,
            now,
        )
        .unwrap()
    };
    let note = agent_artifact(
            ArtifactKind::DeliberationNote,
            vec![
                ArtifactRef {
                    artifact_id: parent.artifact.artifact_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                },
                parent.payload.selections[0].artifact.clone(),
            ],
            "{\"selected_path\":\"use evidence\",\"alternatives\":[],\"uncertainties\":[],\"basis_artifact_ids\":[],\"confidence_ppm\":750000}",
        );
    store
        .write_task_artifact(
            &parent_permit,
            &note,
            LifecycleEventType::DeliberationNoteCreated,
            now,
        )
        .unwrap();
    let output = agent_artifact(
        ArtifactKind::DecisionProposal,
        vec![
            ArtifactRef {
                artifact_id: parent.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            },
            ArtifactRef {
                artifact_id: note.artifact_id.clone(),
                kind: ArtifactKind::DeliberationNote,
            },
        ],
        "decision proposal",
    );
    store
        .commit_attempt(
            &parent_permit,
            std::slice::from_ref(&output),
            akzio_domain::TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let mut child_contract = contract(&store);
    child_contract
        .context
        .permitted_kinds
        .insert(ArtifactKind::DeliberationNote);
    child_contract
        .context
        .permitted_source_families
        .insert("akzio.agent".to_owned());
    child_contract.candidate_capability_ceiling.context = child_contract.context.clone();
    child_contract.contract_hash = child_contract.expected_hash().unwrap();
    let mut child_permit = parent_permit.clone();
    child_permit.task_id = akzio_domain::TaskId::new();
    child_permit.attempt_id = akzio_domain::AttemptId::new();
    child_permit.lease_id = akzio_domain::LeaseId::new();
    child_permit.contract_hash = Some(child_contract.contract_hash.clone());

    let proof = store
        .current_succeeded_attempt(&parent_permit.run_id, &parent_permit.task_id)
        .unwrap();
    let projection = derive_child_projection(
        &proof,
        proof.context_manifest.clone().unwrap(),
        &child_contract,
    );
    assert_eq!(projection.allowed.len(), 1);
    assert_eq!(projection.allowed[0].artifact_id, note.artifact_id);
    broker
        .validate_parent_output_provenance(
            &output,
            &projection.parent_manifest,
            &BTreeSet::from([parent.payload.selections[0].artifact.clone()]),
            &BTreeSet::new(),
            &parent_permit,
            &parent_contract,
        )
        .unwrap();

    let agent_turn = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::AgentTurn,
        vec![],
        "raw agent turn",
    );
    let trace_projection = ContextProjection {
        parent_manifest: ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        },
        allowed: vec![ArtifactRef {
            artifact_id: agent_turn.artifact_id,
            kind: ArtifactKind::AgentTurn,
        }],
        reason: "agent-turn-trace".to_owned(),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &trace_projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn deliberation_note_can_be_read_but_agent_turn_cannot() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut read_contract = contract(&store);
    read_contract
        .context
        .permitted_kinds
        .extend([ArtifactKind::DeliberationNote, ArtifactKind::AgentTurn]);
    read_contract
        .context
        .permitted_source_families
        .insert("akzio.agent".to_owned());
    read_contract.candidate_capability_ceiling.context = read_contract.context.clone();
    read_contract.contract_hash = read_contract.expected_hash().unwrap();
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(read_contract.contract_hash.clone()),
    );
    let now = Utc::now();
    let make_agent_artifact = |kind: ArtifactKind, value: &str| {
        Artifact::new(
            kind,
            store
                .put_bytes(value.as_bytes(), "application/json")
                .unwrap(),
            "agent.research.analyst",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(read_contract.contract_hash.clone()),
            },
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            vec![],
            now,
        )
        .unwrap()
    };
    let note = make_agent_artifact(
            ArtifactKind::DeliberationNote,
            "{\"selected_path\":\"readable summary\",\"alternatives\":[],\"uncertainties\":[],\"basis_artifact_ids\":[],\"confidence_ppm\":900000}",
        );
    store
        .write_task_artifact(
            &permit,
            &note,
            LifecycleEventType::DeliberationNoteCreated,
            now,
        )
        .unwrap();
    let turn = make_agent_artifact(ArtifactKind::AgentTurn, "agent turn");
    store
        .append_task_event(&permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    store
        .write_task_artifact(&permit, &turn, LifecycleEventType::AgentTurnCompleted, now)
        .unwrap();
    let broker = ContextBroker::new(store);
    let manifest = broker
        .assemble(
            &permit,
            &read_contract,
            [
                ArtifactRef {
                    artifact_id: note.artifact_id.clone(),
                    kind: ArtifactKind::DeliberationNote,
                },
                ArtifactRef {
                    artifact_id: turn.artifact_id.clone(),
                    kind: ArtifactKind::AgentTurn,
                },
            ],
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(
        broker
            .read(
                &permit,
                &read_contract,
                &manifest.grant,
                &note.artifact_id,
                now,
            )
            .unwrap()
            .artifact_id,
        note.artifact_id
    );
    assert!(matches!(
        broker.read(
            &permit,
            &read_contract,
            &manifest.grant,
            &turn.artifact_id,
            now,
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn child_projection_filters_by_child_policy_and_is_stable() {
    let (_root, store, parent_permit, parent_contract, parent, _raw, now) = manifest_fixture();
    let child_contract = contract(&store);
    let normalized = store
        .artifact(&parent.payload.selections[0].artifact.artifact_id)
        .unwrap();
    let mut wrong_source = normalized.clone();
    wrong_source.provenance.source_family = "other".to_owned();
    let trace = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::ToolResult,
        vec![],
        "trace",
    );
    let semantic = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::SemanticDetail,
        vec![parent.payload.selections[0].artifact.clone()],
        "semantic detail",
    );
    let raw = store.artifact(&_raw.artifact_id).unwrap();
    let proof = SucceededAttemptProof {
        run_id: parent_permit.run_id.clone(),
        task_id: parent_permit.task_id.clone(),
        attempt_id: parent_permit.attempt_id.clone(),
        lease_id: parent_permit.lease_id.clone(),
        epoch: parent_permit.epoch,
        contract_hash: parent_contract.contract_hash.clone().into(),
        context_manifest: Some(ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }),
        outputs: vec![semantic, trace, raw, wrong_source, normalized.clone()],
    };
    let projection = derive_child_projection(
        &proof,
        proof.context_manifest.clone().unwrap(),
        &child_contract,
    );
    assert_eq!(projection.allowed.len(), 1);
    assert_eq!(projection.allowed[0], parent.payload.selections[0].artifact);

    let mut reversed = proof.clone();
    reversed.outputs.reverse();
    let reversed_projection = derive_child_projection(
        &reversed,
        reversed.context_manifest.clone().unwrap(),
        &child_contract,
    );
    assert_eq!(
        projection_artifact_ids(&projection),
        projection_artifact_ids(&reversed_projection)
    );

    let empty_projection = ContextProjection {
        parent_manifest: proof.context_manifest.clone().unwrap(),
        allowed: Vec::new(),
        reason: "parent_attempt_projection".to_owned(),
    };
    store
        .commit_attempt(
            &parent_permit,
            std::slice::from_ref(&parent.artifact),
            akzio_domain::TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    let broker = ContextBroker::new(store);
    let child_permit = TaskWritePermit {
        run_id: parent_permit.run_id.clone(),
        task_id: akzio_domain::TaskId::new(),
        attempt_id: akzio_domain::AttemptId::new(),
        lease_id: akzio_domain::LeaseId::new(),
        epoch: parent_permit.epoch,
        contract_hash: Some(child_contract.contract_hash.clone()),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &empty_projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::BudgetExceeded)
    ));
}
