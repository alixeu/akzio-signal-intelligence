#[test]
fn evidence_quality_failure_stays_terminal() {
    let error = DaemonError::Evidence(EvidenceRuntimeError::InvalidAcquisition);

    assert_eq!(retry_cause_for_daemon_error(&error), None);
}

#[test]
fn paper_provider_payloads_are_mapped_to_domain_snapshots() {
    let now = Utc::now();
    let session = "2026-08-14".to_owned();
    let account = serde_json::json!({
        "equity": "100000",
        "buying_power": "400000",
        "status": "ACTIVE",
        "trading_blocked": false,
    });
    let account = decode_paper_account(&account, session.clone(), now).unwrap();
    assert_eq!(
        account.schema_version,
        akzio_domain::V2_DOMAIN_SCHEMA_VERSION
    );
    assert!(account.validate().is_ok());

    let quotes = serde_json::json!({
        "quotes": {
            "TQQQ": { "bp": 76.28, "ap": 76.29, "t": "2026-08-14T18:02:07Z" },
            "QQQ": { "bp": 729.38, "ap": 729.41, "t": "2026-08-14T18:02:07Z" },
            "SOXX": { "bp": 544.54, "ap": 544.78, "t": "2026-08-14T18:02:08Z" },
            "SOXL": { "bp": 140.14, "ap": 140.22, "t": "2026-08-14T18:02:07Z" },
        },
    });
    let quotes = decode_paper_quotes(&quotes, session.clone(), now).unwrap();
    assert_eq!(quotes.quotes.len(), 4);
    assert!(quotes.validate().is_ok());

    let clock = serde_json::json!({
        "is_open": true,
        "timestamp": "2026-08-14T18:02:08Z",
        "next_close": "2026-08-14T20:00:00Z",
    });
    let clock = decode_paper_clock(&clock, session, now).unwrap();
    assert!(clock.is_open);
    assert!(clock.validate().is_ok());
}

#[test]
fn paper_session_inputs_include_bounded_directional_bars() {
    let resources = paper_snapshot_resources("2026-08-17");
    assert_eq!(resources.len(), 10);
    for asset in Asset::EXECUTABLE {
        assert!(resources.contains(&format!("bars:{}:1d:2026-07-20:32", asset.symbol())));
    }
}

fn two_phase_responses(output: serde_json::Value) -> Vec<serde_json::Value> {
    let output = serde_json::json!({
        "result": output,
        "deliberation": {
            "selected_path": "fixture path",
            "alternatives": [],
            "alternative_match_ppm": [],
            "uncertainties": [],
            "uncertainty_weight_ppm": [],
            "basis_artifact_ids": [],
            "confidence_ppm": 1000000
        }
    });
    vec![
        serde_json::json!({
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "fixture research memo"}]
            }]
        }),
        serde_json::json!({
            "output": [{
                "type": "function_call",
                "call_id": "fixture-submit",
                "name": "submit_result",
                "arguments": serde_json::to_string(&output).unwrap()
            }]
        }),
    ]
}

fn planner_with_alpaca_need() -> ModelClient {
    let draft = WorkflowProposalDraft {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "Assess TQQQ fixture evidence".to_owned(),
                depends_on: vec![],
                priority: 80,
                evidence_needs: vec![EvidenceNeed {
                    schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                    source_family: "alpaca".to_owned(),
                    resource: "bars:TQQQ:1d".to_owned(),
                    max_age_secs: 86_400,
                }],
                research_intents: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    ModelClient::fixture_by_purpose(BTreeMap::from([
        (
            "research.planner".to_owned(),
            two_phase_responses(serde_json::to_value(draft).unwrap()),
        ),
        (
            "research.analyst".to_owned(),
            two_phase_responses(fixture_claim_output()),
        ),
        (
            "research.critic".to_owned(),
            two_phase_responses(fixture_critique_output()),
        ),
    ]))
}

fn paper_proposal() -> WorkflowProposal {
    WorkflowProposal {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "paper-fixture".to_owned(),
        tasks: BTreeMap::from([(
            "synthesizer".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.synthesizer").unwrap(),
                objective: "Create a fixture Paper decision proposal".to_owned(),
                depends_on: vec![],
                priority: 100,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture Paper workflow".to_owned()),
    }
}

fn accepted_paper_decision(
    claim: ArtifactRef,
    evidence: Vec<ArtifactRef>,
) -> Vec<serde_json::Value> {
    let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                    serde_json::json!({
                        "asset": asset.symbol(),
                        "horizon": horizon,
                        "positive_return_probability_ppm": if asset == Asset::Qqq { 900000 } else { 500000 },
                        "expected_return_ppm": if asset == Asset::Qqq { 100000 } else { 0 },
                    })
                })
            })
            .collect::<Vec<_>>();
    two_phase_responses(serde_json::json!({
        "summary": "fixture accepted Paper decision",
        "confidence_ppm": 900000,
        "forecasts": forecasts,
        "claims": [claim],
        "critiques": [],
            "evidence": evidence,
        "material_conflicts": [],
        "hard_blockers": [],
        "soft_warnings": []
    }))
}

fn blocked_paper_decision() -> Vec<serde_json::Value> {
    let forecasts = Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                serde_json::json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    two_phase_responses(serde_json::json!({
        "summary": "fixture blocked Paper decision",
        "confidence_ppm": 0,
        "forecasts": forecasts,
        "claims": [],
        "critiques": [],
        "evidence": [],
        "material_conflicts": [],
        "hard_blockers": ["missing_evidence"],
        "soft_warnings": []
    }))
}

fn scheduler_fixture_model_client() -> ModelClient {
    ModelClient::fixture_by_purpose(BTreeMap::from([(
        "research.synthesizer".to_owned(),
        blocked_paper_decision(),
    )]))
}

fn scheduler_snapshot_need(
    store: &V2Store,
    run_id: &RunId,
    resource: &str,
    now: DateTime<Utc>,
) -> Artifact {
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: resource.to_owned(),
        max_age_secs: 5,
    };
    Artifact::new(
        ArtifactKind::EvidenceNeed,
        store.put_json(&need).unwrap(),
        "scheduler.paper_snapshot",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.scheduler".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap()
}
