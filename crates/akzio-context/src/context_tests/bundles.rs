fn bundle_contract(
    store: &V2Store,
    max_artifacts: u16,
    max_bytes: u64,
    max_tokens: u32,
) -> AgentContract {
    let mut contract = contract(store);
    contract.context.max_artifacts = max_artifacts;
    contract.context.max_bytes = max_bytes;
    contract.context.max_tokens = max_tokens;
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    contract
}

fn normalized_bundle_artifact(
    store: &V2Store,
    permit: &TaskWritePermit,
    resource: &str,
    confidence_ppm: u32,
    value_bytes: usize,
    now: DateTime<Utc>,
) -> Artifact {
    let payload = serde_json::json!({
        "resource": resource,
        "value": "x".repeat(value_bytes),
    });
    let artifact = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_json(&payload).unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "market".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(permit.artifact_origin()),
        Vec::new(),
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::EvidenceNormalized,
            now,
        )
        .unwrap();
    artifact
}

fn selected_resources(store: &V2Store, manifest: &ContextManifest) -> Vec<String> {
    manifest
        .payload
        .selections
        .iter()
        .map(|selection| {
            let artifact = store.artifact(&selection.artifact.artifact_id).unwrap();
            let payload: serde_json::Value =
                serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
            payload["resource"].as_str().unwrap().to_owned()
        })
        .collect()
}

#[test]
fn analyst_bundle_without_one_domain_keeps_best_effort_selection() {
    let root = tempfile::tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = bundle_contract(&store, 3, 4096, 1024);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();
    let bars = normalized_bundle_artifact(
        &store,
        &permit,
        "bars:TQQQ:1d:2026-01-01:252",
        900_000,
        8,
        now,
    );
    let macro_series = normalized_bundle_artifact(
        &store,
        &permit,
        "series:DFF:2026-01-01:2026-08-28",
        900_000,
        8,
        now,
    );

    let manifest = ContextBroker::new(store.clone())
        .assemble(
            &permit,
            &contract,
            [
                ArtifactRef {
                    artifact_id: bars.artifact_id,
                    kind: bars.kind,
                },
                ArtifactRef {
                    artifact_id: macro_series.artifact_id,
                    kind: macro_series.kind,
                },
            ],
            now,
            Duration::minutes(5),
        )
        .unwrap();

    assert_eq!(manifest.payload.selections.len(), 2);
    assert!(selected_resources(&store, &manifest)
        .iter()
        .all(|resource| !resource.starts_with("news:")));
}

#[test]
fn analyst_complete_bundle_that_cannot_fit_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = bundle_contract(&store, 4, 1_000, 1_024);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();
    let artifacts = [
        normalized_bundle_artifact(
            &store,
            &permit,
            "bars:TQQQ:1d:2026-01-01:252",
            900_000,
            512,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "series:DFF:2026-01-01:2026-08-28",
            900_000,
            512,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "news:TQQQ:2026-08-20:2026-08-28:market",
            900_000,
            512,
            now,
        ),
    ];

    let result = ContextBroker::new(store)
        .assemble(
            &permit,
            &contract,
            artifacts
                .into_iter()
                .map(|artifact| ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                }),
            now,
            Duration::minutes(5),
        );

    assert!(matches!(result, Err(ContextError::BudgetExceeded)));
}

#[test]
fn analyst_selects_the_highest_confidence_same_asset_bundle() {
    let root = tempfile::tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = bundle_contract(&store, 3, 4096, 1024);
    let permit = permit_for_contract(
        &store,
        RunPurpose::Debug,
        Some(contract.contract_hash.clone()),
    );
    let now = Utc::now();

    let artifacts = [
        normalized_bundle_artifact(
            &store,
            &permit,
            "bars:TQQQ:1d:2026-01-01:252",
            900_000,
            8,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "series:DFF:2026-01-01:2026-08-28",
            905_000,
            8,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "news:TQQQ:2026-08-20:2026-08-28:market",
            904_000,
            8,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "bars:QQQ:1d:2026-01-01:252",
            950_000,
            8,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "series:DFII10:2026-01-01:2026-08-28",
            910_000,
            8,
            now,
        ),
        normalized_bundle_artifact(
            &store,
            &permit,
            "news:QQQ:2026-08-20:2026-08-28:market",
            909_000,
            8,
            now,
        ),
    ];

    let manifest = ContextBroker::new(store.clone())
        .assemble(
            &permit,
            &contract,
            artifacts
                .into_iter()
                .map(|artifact| ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                }),
            now,
            Duration::minutes(5),
        )
        .unwrap();

    let resources = selected_resources(&store, &manifest);
    assert_eq!(resources.len(), 3);
    assert!(resources.iter().all(|resource| {
        resource.starts_with("bars:QQQ:")
            || resource.starts_with("series:")
            || resource.starts_with("news:QQQ:")
    }));
    assert!(resources.iter().any(|resource| resource.starts_with("bars:QQQ:")));
    assert!(resources
        .iter()
        .any(|resource| resource.starts_with("series:DFII10:")));
    assert!(resources.iter().any(|resource| resource.starts_with("news:QQQ:")));
}
