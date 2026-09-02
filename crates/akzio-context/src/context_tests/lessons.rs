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

/// Setup for the paired Lesson experiment: one evidence artifact and `lessons`
/// Active Lessons under a cap that admits only some of them. The Lessons that
/// miss the cap form the refill pool — eligible, ranked, and available to move
/// into the slot an ablation frees.
fn ablation_fixture(
    lessons: u32,
    max_artifacts: u16,
) -> (
    tempfile::TempDir,
    V2Store,
    AgentContract,
    TaskWritePermit,
    Vec<ArtifactRef>,
    DateTime<Utc>,
) {
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
    contract.context.max_artifacts = max_artifacts;
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();

    let evidence = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "normalized",
    );
    store
        .write_task_artifact(
            &permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();

    for index in 0..lessons {
        active_lesson(&store, index, now);
    }

    let candidates = vec![ArtifactRef {
        artifact_id: evidence.artifact_id,
        kind: evidence.kind,
    }];
    (root, store, contract, permit, candidates, now)
}

/// Distinct confidences keep the selection order deterministic, so the Lessons
/// that miss the cap are a stable set rather than an artifact-id coin flip.
fn active_lesson(store: &V2Store, index: u32, now: DateTime<Utc>) {
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
        confidence_ppm: 900_000 - index * 100_000,
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

/// The two arms of the P3 experiment must differ by the Lesson alone. Two
/// Lessons are eligible but only one fits the cap, so a re-assembly of the
/// off-arm would promote the runner-up into the freed slot and the comparison
/// would measure that swap instead of the Lesson. The ablation copies the
/// baseline's non-Lesson selections verbatim, so nothing can move in.
#[test]
fn lesson_ablation_drops_lessons_without_refilling_the_freed_slot() {
    let (_root, store, contract, permit, candidates, now) = ablation_fixture(2, 2);
    let broker = ContextBroker::new(store);
    let baseline = broker
        .assemble(
            &permit,
            &contract,
            candidates,
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(
        baseline
            .payload
            .selections
            .iter()
            .filter(|selection| selection.artifact.kind == ArtifactKind::Lesson)
            .count(),
        1,
        "the cap must admit exactly one of the two eligible Lessons"
    );

    let ablated = broker
        .assemble_lesson_ablation(&permit, &contract, &baseline, now, Duration::minutes(5))
        .unwrap();

    assert!(ablated
        .payload
        .selections
        .iter()
        .all(|selection| selection.artifact.kind != ArtifactKind::Lesson));
    let expected = baseline
        .payload
        .selections
        .iter()
        .filter(|selection| selection.artifact.kind != ArtifactKind::Lesson)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        ablated.payload.selections, expected,
        "the off-arm must be the on-arm minus its Lessons, in the same order"
    );
    // The general no-refill invariant: never an artifact the baseline did not
    // already carry, whatever kind the leftover candidates happen to be.
    assert!(ablated
        .payload
        .selections
        .iter()
        .all(|selection| baseline.payload.selections.contains(selection)));

    // The arms are paired durably by the off-arm naming its baseline, and the
    // derived manifest still satisfies the ordinary closure check.
    assert!(ablated.artifact.source_refs.contains(&ArtifactRef {
        artifact_id: baseline.artifact.artifact_id.clone(),
        kind: ArtifactKind::ContextManifest,
    }));
    assert_ne!(ablated.payload.input_hash, baseline.payload.input_hash);
    broker
        .policy_influences(&permit, &contract, &ablated, now)
        .unwrap();
}

/// A baseline with no Lesson would make the two arms byte-identical: a null
/// result from a treatment that never varied. That has to fail loudly rather
/// than mint a second manifest that looks like a control.
#[test]
fn lesson_ablation_requires_a_baseline_that_carried_a_lesson() {
    let (_root, store, contract, permit, candidates, now) = ablation_fixture(0, 2);
    let broker = ContextBroker::new(store);
    let baseline = broker
        .assemble(
            &permit,
            &contract,
            candidates,
            now,
            Duration::minutes(5),
        )
        .unwrap();

    assert!(matches!(
        broker.assemble_lesson_ablation(&permit, &contract, &baseline, now, Duration::minutes(5)),
        Err(ContextError::NoLessonToAblate)
    ));
}

/// When the Lesson is the only thing that met `min_artifacts`, there is no
/// comparable off-arm to build.
#[test]
fn lesson_ablation_refuses_to_fall_below_the_minimum_artifact_floor() {
    let (_root, store, mut contract, _permit, _candidates, now) = ablation_fixture(1, 2);
    contract.context.min_artifacts = 2;
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let evidence = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "normalized-floor",
    );
    store
        .write_task_artifact(
            &permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    let broker = ContextBroker::new(store);
    let baseline = broker
        .assemble(
            &permit,
            &contract,
            [ArtifactRef {
                artifact_id: evidence.artifact_id,
                kind: evidence.kind,
            }],
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(baseline.payload.selections.len(), 2);

    assert!(matches!(
        broker.assemble_lesson_ablation(&permit, &contract, &baseline, now, Duration::minutes(5)),
        Err(ContextError::BudgetExceeded)
    ));
}
