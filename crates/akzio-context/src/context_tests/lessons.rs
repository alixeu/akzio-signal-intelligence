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
