use super::*;

async fn submit(
    client: &ModelClient,
    purpose: &str,
    objective: &str,
    result_schema: Value,
    refs: &[Value],
) -> Value {
    let canonical_schema = deliberation_output_schema(&result_schema);
    let mut schema = canonical_schema.clone();
    bind_reference_schema(&mut schema, refs);
    let mut request = ModelRequest {
        instructions: "Explicit offline neutral research fixture".into(),
        input: ModelInput::Fresh {
            text: json!({"objective": objective, "context": refs}).to_string(),
        },
        max_output_tokens: 5000,
        reasoning_effort: None,
        tools: vec![],
        tool_choice: ModelToolChoice::Auto,
        fixture_key: Some(purpose.into()),
    };
    request.tool_choice = ModelToolChoice::RequiredFunction("submit_result".into());
    request.tools = vec![ModelToolDefinition {
        name: "submit_result".into(),
        description: "Submit fixture".into(),
        input_schema: schema.clone(),
        strict: true,
    }];
    let response = client.respond(request).await.unwrap();
    let mut output = response.tool_calls[0].arguments.clone();
    // Real bound wire schema, then the unchanged Rust kind resolver and
    // installed schema: this is not a fixture-only acceptance validator.
    validate_schema_value(&output, &schema, "$").unwrap();
    resolve_reference_kinds(&mut output, refs).unwrap();
    validate_schema_value(&output, &canonical_schema, "$").unwrap();
    output["result"].clone()
}

fn reference(label: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(label.as_bytes())),
        kind,
    }
}

#[tokio::test]
async fn formal_fixture_submit_matches_canonical_schemas_and_retains_complete_reference_closure() {
    let client = crate::fixture_model_client();
    let semantic = reference("semantic-only-evidence", ArtifactKind::SemanticDetail);
    let mut all_refs = vec![json!(semantic)];
    let mut claims = Vec::new();
    let mut critiques = Vec::new();
    for horizon in ["t1", "t3", "t5"] {
        let claim = submit(
            &client,
            RESEARCH_ANALYST_RECIPE_ID,
            &format!("[research_horizon={horizon}] Offline fixture"),
            reviewed_research_schema(claim_output_schema()),
            &[json!(semantic)],
        )
        .await;
        let claim: ResearchClaim = serde_json::from_value(claim).unwrap();
        claim.validate().unwrap();
        let mut duplicate = claim.clone();
        duplicate.grounds.push(duplicate.grounds[0].clone());
        assert!(matches!(
            validate_claim_submission(&duplicate),
            Err(ResearchError::InvalidOutput(message)) if message.contains("duplicate")
        ));
        assert_eq!(claim.stance, akzio_domain::ClaimStance::Neutral);
        assert_eq!(claim.grounds[0].evidence, semantic);
        let claim_ref = reference(&format!("claim-{horizon}"), ArtifactKind::Claim);
        let critique = submit(
            &client,
            RESEARCH_CRITIC_RECIPE_ID,
            "Review bounded claim",
            reviewed_research_schema(critique_output_schema()),
            &[json!(semantic), json!(claim_ref)],
        )
        .await;
        let critique: ResearchCritique = serde_json::from_value(critique).unwrap();
        critique.validate().unwrap();
        let mut mixed = critique.clone();
        mixed.grounds = claim.grounds.clone();
        mixed.verification_status = ClaimVerificationStatus::Supported;
        let verified = akzio_domain::ClaimVerificationEvidence {
            evidence: semantic.clone(),
            authority: akzio_domain::SourceAuthority::Official,
            temporal_validity: akzio_domain::TemporalValidity::ValidAtDecisionCutoff,
        };
        mixed.supporting_refs = vec![verified.clone()];
        mixed.conflicting_refs = vec![verified];
        assert!(matches!(
            validate_critique_submission(&mixed),
            Err(ResearchError::InvalidOutput(message)) if message.contains("conflicting_refs")
        ));
        mixed.verification_status = ClaimVerificationStatus::NotEnoughInformation;
        validate_critique_submission(&mixed).unwrap();
        assert_eq!(critique.target, claim_ref);
        assert_eq!(
            critique.verification_status,
            ClaimVerificationStatus::NotEnoughInformation
        );
        let critique_ref = reference(&format!("critique-{horizon}"), ArtifactKind::Critique);
        all_refs.extend([json!(claim_ref), json!(critique_ref)]);
        claims.push((claim_ref, claim));
        critiques.push(critique);
    }
    let proposal = submit(
        &client,
        RESEARCH_SYNTHESIZER_RECIPE_ID,
        "Synthesize all three horizons",
        reviewed_proposal_schema(),
        &all_refs,
    )
    .await;
    let proposal: DecisionDraft = serde_json::from_value(proposal).unwrap();
    proposal.validate().unwrap();
    // The real failure used the context cutoff as expiry, which was already
    // eight minutes old at DecisionGate. Reject it while Submit can still repair.
    let submitted_at: DateTime<Utc> = "2026-09-20T09:56:14Z".parse().unwrap();
    validate_proposal_at(&proposal, submitted_at).unwrap();
    for expiry in ["2026-09-20T09:47:42Z", "2026-09-20T09:56:14Z"] {
        let mut expired = proposal.clone();
        expired.forecasts[0]
            .thesis
            .as_mut()
            .unwrap()
            .thesis_valid_until = expiry.parse().unwrap();
        assert!(matches!(
            validate_proposal_at(&expired, submitted_at),
            Err(ResearchError::InvalidOutput(message)) if message.contains("thesis_valid_until")
        ));
    }
    assert_eq!(proposal.claims.len(), 3);
    assert_eq!(proposal.critiques.len(), 3);
    assert_eq!(proposal.evidence, vec![semantic]);
    assert!(proposal
        .forecasts
        .iter()
        .all(akzio_domain::Forecast::is_neutral));
    assert!(!proposal
        .research_allocation
        .as_ref()
        .unwrap()
        .has_non_zero_target());
    akzio_domain::validate_verified_forecast_slots(&proposal, &claims, &critiques).unwrap();
    validate_research_allocation_sufficiency(&proposal, &claims, &critiques).unwrap();
}
