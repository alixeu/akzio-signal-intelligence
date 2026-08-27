#[test]
fn context_child_and_repair_lifecycle_validator_enforces_lineage_and_sources() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let parent = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"parent": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &parent,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        )
        .unwrap();
    let parent_ref = ArtifactRef {
        artifact_id: parent.artifact_id.clone(),
        kind: ArtifactKind::ContextManifest,
    };

    let missing_parent = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"missing_parent": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &missing_parent,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&missing_parent.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let foreign_run = RunId::new();
    let foreign_parent = Artifact::new(
        ArtifactKind::ContextManifest,
        fixture
            .store
            .put_json(&serde_json::json!({"foreign": true}))
            .unwrap(),
        "fixture.foreign",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: fixture.now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: fixture.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(foreign_run),
            task_id: Some(fixture.permit.task_id.clone()),
            attempt_id: Some(fixture.permit.attempt_id.clone()),
            contract_hash: fixture.permit.contract_hash.clone(),
        }),
        vec![],
        fixture.now,
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &foreign_parent).unwrap();
        transaction.commit().unwrap();
    }
    let foreign_child = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"foreign_parent": true}),
        vec![ArtifactRef {
            artifact_id: foreign_parent.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &foreign_child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&foreign_child.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let child = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"child": true}),
        vec![parent_ref],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        )
        .unwrap();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &parent,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    let empty_repair = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextRepair,
        &serde_json::json!({"empty": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &empty_repair,
            LifecycleEventType::ContextRepaired,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&empty_repair.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let source = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::NormalizedEvidence,
        &serde_json::json!({"source": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &source,
            LifecycleEventType::Evidence,
            fixture.now,
        )
        .unwrap();
    let repair = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextRepair,
        &serde_json::json!({"repair": true}),
        vec![ArtifactRef {
            artifact_id: source.artifact_id,
            kind: ArtifactKind::NormalizedEvidence,
        }],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &repair,
            LifecycleEventType::ContextRepaired,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}
