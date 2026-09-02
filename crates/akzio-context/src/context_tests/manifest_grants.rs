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
        contract_hash: permit.contract_hash,
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

    let broker = ContextBroker::new(store);
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: normalized.artifact_id,
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
    let broker = ContextBroker::new(store);
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
    let mut forged_grant = manifest.grant;
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
    let mut forged_grant = manifest.grant;
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
