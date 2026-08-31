#[test]
fn metadata_first_materialization_does_not_load_optional_document_bodies() {
    let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
    let materialization = ContextBroker::new(store)
        .materialize_for_agent(&permit, &contract, &manifest, now)
        .unwrap();

    assert_eq!(materialization.ledger.len(), 1);
    assert!(materialization.must_read.is_empty());
    let document = &materialization.ledger[0];
    assert_eq!(document.document_id, manifest.payload.selections[0].artifact.artifact_id);
    assert_eq!(document.kind, ArtifactKind::NormalizedEvidence);
    assert_eq!(document.source, "market");
    assert_eq!(document.estimated_tokens, manifest.payload.selections[0].estimated_tokens);
    assert_eq!(document.read_grant_identity, materialization.read_grant_identity);
    assert!(!document.must_read);

    let model_context = materialization.model_context();
    assert_eq!(model_context.len(), 2);
    assert_eq!(model_context[0]["type"], "context_metadata_ledger");
    assert_eq!(model_context[1]["class"], "task_contract");
    assert!(!serde_json::to_string(&model_context)
        .unwrap()
        .contains("\"value\":\"normalized\""));
}

#[test]
fn explicitly_mandatory_observation_is_injected_deterministically() {
    let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
    let mut payload = manifest.payload.clone();
    payload.selections[0].reason = "mandatory_observation:market_open".to_owned();
    let manifest = persist_manifest_payload(&store, &permit, &manifest, payload, now);
    let broker = ContextBroker::new(store);

    let first = broker
        .materialize_for_agent(&permit, &contract, &manifest, now)
        .unwrap();
    let second = broker
        .materialize_for_agent(&permit, &contract, &manifest, now)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.must_read.len(), 1);
    assert_eq!(first.must_read[0].class, "mandatory_observation");
    assert_eq!(first.must_read[0].value, Value::String("normalized".to_owned()));
    assert_eq!(first.model_context().len(), 3);
}

#[test]
fn bounded_reads_search_and_compare_stay_inside_manifest() {
    let (_root, store, permit, contract, original, _raw, now) = manifest_fixture();
    let first = original.payload.selections[0].artifact.clone();
    let second_artifact = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "second authorized document",
    );
    store
        .write_task_artifact(
            &permit,
            &second_artifact,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    let outside = task_artifact(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        vec![],
        "outside document",
    );
    store
        .write_task_artifact(
            &permit,
            &outside,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    let broker = ContextBroker::new(store);
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [
                first.clone(),
                ArtifactRef {
                    artifact_id: second_artifact.artifact_id.clone(),
                    kind: second_artifact.kind,
                },
            ],
            now,
            Duration::minutes(5),
        )
        .unwrap();

    let document = broker
        .read_document_result(
            &permit,
            &contract,
            &manifest.grant,
            &first.artifact_id,
            now,
        )
        .unwrap();
    assert_eq!(document.value, Value::String("normalized".to_owned()));

    let range = broker
        .read_range(
            &permit,
            &contract,
            &manifest.grant,
            &second_artifact.artifact_id,
            0,
            6,
            now,
        )
        .unwrap();
    assert_eq!(range.value["text"], "second");
    assert!(matches!(
        broker.read_range(
            &permit,
            &contract,
            &manifest.grant,
            &second_artifact.artifact_id,
            0,
            usize::MAX,
            now,
        ),
        Err(ContextError::InvalidRange)
    ));

    let search = broker
        .search_context(
            &permit,
            &contract,
            &manifest.grant,
            "authorized",
            4,
            now,
        )
        .unwrap();
    assert_eq!(search.artifacts.len(), 1);
    assert_eq!(search.artifacts[0].artifact_id, second_artifact.artifact_id);
    let outside_search = broker
        .search_context(
            &permit,
            &contract,
            &manifest.grant,
            "outside",
            4,
            now,
        )
        .unwrap();
    assert!(outside_search.artifacts.is_empty());

    let comparison = broker
        .compare_sources(
            &permit,
            &contract,
            &manifest.grant,
            &[first.artifact_id, second_artifact.artifact_id],
            now,
        )
        .unwrap();
    assert_eq!(comparison.artifacts.len(), 2);
    assert!(matches!(
        broker.read_document_result(
            &permit,
            &contract,
            &manifest.grant,
            &outside.artifact_id,
            now,
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}

#[test]
fn claim_evidence_read_closes_over_granted_evidence_only() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    contract.context.permitted_kinds.insert(ArtifactKind::Claim);
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
        "claim evidence",
    );
    store
        .write_task_artifact(
            &permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    let evidence_ref = ArtifactRef {
        artifact_id: evidence.artifact_id.clone(),
        kind: evidence.kind,
    };
    let claim = akzio_domain::ResearchClaim {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topic: "market_regime".to_owned(),
        statement: "The observation supports a neutral market claim.".to_owned(),
        horizon: DecisionHorizon::T1,
        stance: akzio_domain::ClaimStance::Neutral,
        materiality_ppm: 500_000,
        confidence_ppm: 500_000,
        grounds: vec![akzio_domain::EvidenceGround {
            evidence: evidence_ref.clone(),
            support: "direct fixture support".to_owned(),
            role: akzio_domain::EvidenceGroundRole::Descriptive,
            assets: BTreeSet::new(),
            domain: None,
        }],
        evidence_gaps: vec![],
    };
    let claim_artifact = Artifact::new(
        ArtifactKind::Claim,
        store.put_json(&claim).unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance("market"),
        Some(permit.artifact_origin()),
        vec![evidence_ref.clone()],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &claim_artifact,
            LifecycleEventType::ClaimCreated,
            now,
        )
        .unwrap();
    let broker = ContextBroker::new(store);
    let manifest = broker
        .assemble(
            &permit,
            &contract,
            [
                evidence_ref,
                ArtifactRef {
                    artifact_id: claim_artifact.artifact_id.clone(),
                    kind: claim_artifact.kind,
                },
            ],
            now,
            Duration::minutes(5),
        )
        .unwrap();

    let result = broker
        .read_claim_evidence(
            &permit,
            &contract,
            &manifest.grant,
            &claim_artifact.artifact_id,
            now,
        )
        .unwrap();
    assert_eq!(result.artifacts.len(), 2);
    assert_eq!(result.value["evidence"].as_array().unwrap().len(), 1);
}
