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
    forged.payload.selections[0].estimated_tokens = estimate_tokens_from_bytes(second.blob.bytes);
    forged.payload.total_bytes = second.blob.bytes;
    forged.payload.estimated_tokens = estimate_tokens_from_bytes(second.blob.bytes);
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
