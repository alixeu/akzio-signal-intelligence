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
