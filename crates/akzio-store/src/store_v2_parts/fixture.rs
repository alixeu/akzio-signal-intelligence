#[cfg(test)]
#[allow(clippy::items_after_test_module)]
fn retrospective_artifact(
    store: &V2Store,
    permit: &TaskWritePermit,
    outcome: &Artifact,
    now: DateTime<Utc>,
) -> Artifact {
    let outcome_ref = ArtifactRef {
        artifact_id: outcome.artifact_id.clone(),
        kind: ArtifactKind::Outcome,
    };
    let payload = akzio_domain::Retrospective {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: akzio_domain::OutcomeId::new(),
        horizon: OutcomeHorizon::T5,
        status: akzio_domain::RetrospectiveStatus::Complete,
        summary: "fixture retrospective".to_owned(),
        findings: Vec::new(),
        counterfactuals: Vec::new(),
        lesson_candidates: Vec::new(),
        diagnostic_gaps: Vec::new(),
        source_refs: vec![outcome_ref.clone()],
        outcome: outcome_ref.clone(),
        created_at: now,
        sealed_at: Some(now),
    };
    Artifact::new(
        ArtifactKind::Retrospective,
        store
            .put_json(&payload)
            .expect("fixture retrospective payload"),
        "fixture.policy",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "fixture.policy".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        vec![outcome_ref],
        now,
    )
    .expect("fixture retrospective artifact")
}
