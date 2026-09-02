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
                artifact_id: normalized.artifact_id,
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
