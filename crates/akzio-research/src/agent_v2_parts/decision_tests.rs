#[cfg(test)]
mod decision_proposal_tests {
    use super::*;

    #[test]
    fn directional_evidence_requires_complete_citations() {
        assert!(evidence_has_complete_citations(&serde_json::json!({
            "quality": { "citations_complete": true }
        })));
        assert!(!evidence_has_complete_citations(&serde_json::json!({
            "quality": { "citations_complete": false }
        })));
    }

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    fn proposal(
        claims: Vec<ArtifactRef>,
        evidence: Vec<ArtifactRef>,
        hard_blockers: Vec<akzio_domain::HardBlocker>,
    ) -> DecisionDraft {
        let forecasts = akzio_domain::Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    akzio_domain::DecisionHorizon::T1,
                    akzio_domain::DecisionHorizon::T3,
                    akzio_domain::DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| akzio_domain::Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                })
            })
            .collect();
        DecisionDraft {
            summary: "bounded decision".to_owned(),
            confidence_ppm: 700_000,
            forecasts,
            claims,
            critiques: Vec::new(),
            evidence,
            applied_learning_refs: Vec::new(),
            rejected_learning_refs: Vec::new(),
            material_conflicts: Vec::new(),
            hard_blockers,
            soft_warnings: Vec::new(),
        }
    }

    #[test]
    fn decision_proposal_requires_claim_and_evidence_closure() {
        let root = tempfile::tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let evidence = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store.put_json(&serde_json::json!({"price": 100})).unwrap(),
            "fixture.evidence",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            Vec::new(),
            now,
        )
        .unwrap();
        let evidence_ref = ArtifactRef {
            artifact_id: evidence.artifact_id.clone(),
            kind: ArtifactKind::NormalizedEvidence,
        };
        let claim_payload = ResearchClaim {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topic: "market".to_owned(),
            statement: "Evidence is neutral.".to_owned(),
            horizon: akzio_domain::DecisionHorizon::T1,
            stance: akzio_domain::ClaimStance::Neutral,
            materiality_ppm: 700_000,
            confidence_ppm: 700_000,
            grounds: vec![akzio_domain::EvidenceGround {
                evidence: evidence_ref.clone(),
                support: "Observed fixture evidence.".to_owned(),
                role: akzio_domain::EvidenceGroundRole::Descriptive,
                assets: std::collections::BTreeSet::new(),
                domain: None,
            }],
            evidence_gaps: Vec::new(),
        };
        let claim = Artifact::new(
            ArtifactKind::Claim,
            store.put_json(&claim_payload).unwrap(),
            "fixture.claim",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            claim_payload.source_refs(),
            now,
        )
        .unwrap();
        let claim_ref = ArtifactRef {
            artifact_id: claim.artifact_id.clone(),
            kind: ArtifactKind::Claim,
        };
        let contract_hash = akzio_domain::ContentHash::of_bytes(b"contract");
        let manifest_payload = akzio_domain::ContextManifestPayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            contract_hash: contract_hash.clone(),
            selections: vec![
                akzio_domain::ContextSelection {
                    artifact: claim_ref,
                    reason: "approved claim".to_owned(),
                    estimated_tokens: 1,
                },
                akzio_domain::ContextSelection {
                    artifact: evidence_ref.clone(),
                    reason: "claim ground".to_owned(),
                    estimated_tokens: 1,
                },
            ],
            total_bytes: 2,
            estimated_tokens: 2,
            input_hash: akzio_domain::ContentHash::of_bytes(b"input"),
        };
        let manifest_artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            store.put_json(&manifest_payload).unwrap(),
            "fixture.manifest",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            Vec::new(),
            now,
        )
        .unwrap();
        let manifest = ContextManifest {
            artifact: manifest_artifact.clone(),
            payload: manifest_payload,
            grant: akzio_domain::ReadGrant {
                manifest_artifact_id: manifest_artifact.artifact_id,
                run_id: RunId::new(),
                task_id: TaskId::new(),
                attempt_id: akzio_domain::AttemptId::new(),
                lease_id: akzio_domain::LeaseId::new(),
                epoch: 1,
                contract_hash,
                readable: BTreeSet::from([claim.artifact_id, evidence.artifact_id]),
                raw_source_closure: BTreeSet::new(),
                expires_at: now + Duration::hours(1),
            },
        };

        let dropped = proposal(
            Vec::new(),
            vec![evidence_ref],
            vec![akzio_domain::HardBlocker::MissingEvidence],
        );
        let dropped_error = research_output_source_refs(
            &store,
            ArtifactKind::DecisionProposal,
            &serde_json::to_value(dropped).unwrap(),
            &manifest,
        )
        .unwrap_err();
        assert!(dropped_error.to_string().contains("dropped all claims"));
    }
}
