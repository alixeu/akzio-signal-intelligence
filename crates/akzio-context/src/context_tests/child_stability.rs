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
        outputs: vec![semantic, trace, raw, wrong_source, normalized],
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
        parent_manifest: proof.context_manifest.unwrap(),
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
