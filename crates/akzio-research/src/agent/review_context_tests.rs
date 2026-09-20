use super::*;

#[test]
fn lesson_recall_uses_explicit_scope_despite_evidence_tags() {
    use akzio_domain::{
        ContextQueryScope, DecisionHorizon, Lesson, LessonGovernance, LessonId, LessonLifecycle,
        LessonOrigin, LessonScope,
    };
    let now = Utc::now();
    let (store, runtime, attempt) = late_model_tests::isolated_agent_attempt(
        RESEARCH_SYNTHESIZER_RECIPE_ID,
        RunPurpose::PositionPlan,
        120,
        now,
    );
    let contract = &runtime
        .contract(attempt.node.contract_hash.as_ref().unwrap())
        .unwrap()
        .contract;
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        store
            .stage_json(&json!({"operator":"scope regression"}))
            .unwrap(),
        "test.operator",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )
    .unwrap();
    let mut lessons = BTreeMap::new();
    for (name, horizon, regime, stage) in [
        ("t1", Some(DecisionHorizon::T1), None, None),
        ("t5", Some(DecisionHorizon::T5), None, None),
        ("generic", None, None, None),
        ("fake-regime", None, Some("fake"), None),
        ("fake-stage", None, None, Some("fake")),
        ("wrong-asset", None, None, None),
    ] {
        let lesson = Lesson {
            schema_version: DOMAIN_SCHEMA_VERSION,
            lesson_id: LessonId(name.into()),
            origin: LessonOrigin::Operator,
            lifecycle: LessonLifecycle::Active,
            title: name.into(),
            statement: name.into(),
            rationale: "scope regression".into(),
            recommended_behavior: "Keep uncertainty".into(),
            exclusions: vec![],
            scope: LessonScope {
                assets: if name == "wrong-asset" {
                    BTreeSet::from([Asset::Soxx])
                } else {
                    BTreeSet::new()
                },
                horizons: horizon.into_iter().collect(),
                regimes: regime.map(str::to_owned).into_iter().collect(),
                decision_stages: stage.map(str::to_owned).into_iter().collect(),
            },
            source_refs: vec![ArtifactRef {
                artifact_id: source.artifact_id.clone(),
                kind: source.kind,
            }],
            supersedes: vec![],
            conflicts_with: vec![],
            confidence_ppm: 500_000,
            authored_by: Some("test".into()),
            approved_by: Some("test".into()),
            created_at: now,
            updated_at: now,
            governance: Some(LessonGovernance::operator_reviewed(now)),
        };
        lessons.insert(
            name,
            store
                .write_lesson(&lesson, &source, now)
                .unwrap()
                .lesson
                .artifact
                .artifact_id,
        );
    }
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .stage_json(&json!({"resource":"scope-test", "value":{
            "labels":["SOXX", "t5", "regime:fake", "stage:fake"]}}))
            .unwrap(),
        "evidence.normalize",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "alpaca".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(attempt.permit.artifact_origin()),
        vec![],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &attempt.permit,
            &evidence,
            LifecycleEventType::ArtifactCommitted,
            now,
        )
        .unwrap();
    let broker = ContextBroker::new(store.clone());
    for (horizons, expected) in [
        (vec![DecisionHorizon::T1], vec!["generic", "t1"]),
        (vec![], vec!["generic"]),
        (
            vec![
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ],
            vec!["generic", "t1", "t5"],
        ),
    ] {
        let query = ContextQueryScope {
            assets: BTreeSet::from([Asset::Qqq]),
            horizons: horizons.into_iter().collect(),
            ..Default::default()
        };
        let manifest = broker
            .assemble(
                &attempt.permit,
                contract,
                &query,
                attempt
                    .node
                    .input_artifacts
                    .iter()
                    .cloned()
                    .chain([ArtifactRef {
                        artifact_id: evidence.artifact_id.clone(),
                        kind: evidence.kind,
                    }]),
                now,
                Duration::minutes(5),
            )
            .unwrap();
        let selected = manifest
            .payload
            .selections
            .iter()
            .filter(|s| s.artifact.kind == ArtifactKind::Lesson)
            .map(|s| s.artifact.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            selected,
            expected
                .into_iter()
                .map(|name| lessons[name].clone())
                .collect()
        );
    }
}

#[test]
fn critical_twenty_fifth_document_is_selected_and_counterevidence_over_budget_blocks() {
    let now = Utc::now();
    let (store, runtime, attempt) = late_model_tests::isolated_agent_attempt(
        RESEARCH_CRITIC_RECIPE_ID,
        RunPurpose::PositionPlan,
        120,
        now,
    );
    let contract = &runtime
        .contract(attempt.node.contract_hash.as_ref().unwrap())
        .unwrap()
        .contract;
    let mut evidence = Vec::new();
    for index in 0..25 {
        let a=Artifact::new(ArtifactKind::NormalizedEvidence,store.stage_json(&json!({"source":"alpaca","resource":format!("context-test-{index}"),"value":{"observation":index}})).unwrap(),
            "evidence.normalize",ArtifactLifecycle::RunScoped,ArtifactProvenance{source_family:"alpaca".into(),observed_at:Some(now),retrieved_at:now,
                source_uri:None,confidence_ppm:1_000_000,producer_contract_hash:None},Some(attempt.permit.artifact_origin()),vec![],now).unwrap();
        store
            .write_task_artifact(
                &attempt.permit,
                &a,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        evidence.push(ArtifactRef {
            artifact_id: a.artifact_id,
            kind: a.kind,
        });
    }
    let mut claim = crate::fixture_claim_output();
    claim["grounds"][0]["evidence"] = json!(evidence[24]);
    let a = Artifact::new(
        ArtifactKind::Claim,
        store.stage_json(&claim).unwrap(),
        "agent.research.analyst",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: attempt.permit.contract_hash.clone(),
        },
        Some(attempt.permit.artifact_origin()),
        vec![evidence[24].clone()],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &attempt.permit,
            &a,
            LifecycleEventType::ArtifactCommitted,
            now,
        )
        .unwrap();
    let claim_ref = ArtifactRef {
        artifact_id: a.artifact_id,
        kind: a.kind,
    };
    let broker = ContextBroker::new(store.clone());
    let manifest = broker
        .assemble(
            &attempt.permit,
            contract,
            &akzio_domain::ContextQueryScope::for_node(&attempt.node),
            evidence.iter().cloned().chain([claim_ref.clone()]),
            now,
            Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(manifest.payload.selections.len(), 24);
    assert!(manifest
        .payload
        .selections
        .iter()
        .any(|s| s.artifact == evidence[24]));
    assert!(manifest
        .payload
        .selections
        .iter()
        .any(|s| s.artifact == claim_ref));
    let mut critique_refs = Vec::new();
    for chunk in evidence.chunks(9) {
        let mut critique = crate::fixture_critique_output();
        critique["target"] = json!(claim_ref);
        critique["grounds"] = json!(chunk
            .iter()
            .map(
                |r| json!({"evidence":r,"support":"Critical counterevidence",
            "role":"descriptive","assets":[],"domain":null})
            )
            .collect::<Vec<_>>());
        critique["conflicting_refs"] = json!(chunk.iter().map(|r| json!({"evidence":r,"authority":"official","temporal_validity":"valid_at_decision_cutoff"})).collect::<Vec<_>>());
        let typed: ResearchCritique = serde_json::from_value(critique.clone()).unwrap();
        typed.validate().unwrap();
        validate_schema_value(
            &critique,
            &reviewed_research_schema(critique_output_schema()),
            "$",
        )
        .unwrap();
        let a = Artifact::new(
            ArtifactKind::Critique,
            store.stage_json(&critique).unwrap(),
            "agent.research.critic",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".into(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: attempt.permit.contract_hash.clone(),
            },
            Some(attempt.permit.artifact_origin()),
            chunk.iter().cloned().chain([claim_ref.clone()]).collect(),
            now,
        )
        .unwrap();
        store
            .write_task_artifact(
                &attempt.permit,
                &a,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        critique_refs.push(ArtifactRef {
            artifact_id: a.artifact_id,
            kind: a.kind,
        });
    }
    // Use the installed Synthesizer contract: it must carry Critique counterevidence,
    // not merely the previously selected Analyst ground.
    let synth = ActiveResearchCatalogue::install(&store, now)
        .unwrap()
        .contracts
        .contracts()
        .find(|c| c.contract.purpose.as_str() == RESEARCH_SYNTHESIZER_RECIPE_ID)
        .unwrap()
        .contract
        .clone();
    let result = broker.assemble(
        &attempt.permit,
        &synth,
        &akzio_domain::ContextQueryScope::default(),
        evidence.into_iter().chain([claim_ref]).chain(critique_refs),
        now,
        Duration::minutes(5),
    );
    assert!(
        matches!(result,Err(akzio_context::ContextError::MissingRequiredInput{requirement,..}) if requirement.contains("exceeds context budget"))
    );
}
