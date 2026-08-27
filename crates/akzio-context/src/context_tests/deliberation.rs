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
