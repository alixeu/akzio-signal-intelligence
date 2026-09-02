impl DecisionRuntime {
    pub fn new(store: Store, policy: DecisionPolicy) -> DecisionGateResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &DecisionPolicy {
        &self.policy
    }

    /// Validate, bind, and atomically complete the DecisionGate attempt.
    pub fn decide(&self, input: &DecisionGateInput) -> DecisionGateResult<DecisionGateOutput> {
        let decision_gate_started = std::time::Instant::now();
        self.store.validate_task_permit(&input.permit)?;

        let proposal = self.load_expected(&input.proposal, ArtifactKind::DecisionProposal)?;
        let proposal_contract = self.validate_proposal(&proposal, &input.permit)?;
        let manifest_ref = unique_manifest_ref(&proposal)?;
        let manifest = self.load_expected(manifest_ref, ArtifactKind::ContextManifest)?;
        let selected =
            self.validate_manifest(&manifest, &proposal, &proposal_contract, &input.permit)?;

        let draft: DecisionDraft = serde_json::from_slice(&self.store.read_blob(&proposal.blob)?)?;
        draft.validate()?;
        self.validate_draft_closure(&draft, &selected)?;
        // Semantic evidence sufficiency is unconditional. A producer contract
        // that is not installed, or that predates the rule, cannot vouch for
        // claim semantics, so the gate rejects the proposal rather than
        // silently skipping the check.
        let installed = self
            .store
            .contract_installation(&proposal_contract)?
            .ok_or(DecisionGateError::UnsupportedProposalContract)?;
        if installed.contract.version < 16 {
            return Err(DecisionGateError::UnsupportedProposalContract);
        }
        let claim_records = draft
            .claims
            .iter()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::Claim)?;
                let claim: ResearchClaim =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                claim.validate()?;
                Ok((artifact, claim))
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;
        let claims = claim_records
            .iter()
            .map(|(_, claim)| claim.clone())
            .collect::<Vec<_>>();
        validate_decision_evidence_sufficiency(&draft, &claims)
            .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;

        let critique_records = draft
            .critiques
            .iter()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::Critique)?;
                let critique: ResearchCritique =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                critique.validate()?;
                if !draft.claims.contains(&critique.target) {
                    return Err(DecisionGateError::InvalidClaimVerification(
                        reference.artifact_id.clone(),
                    ));
                }
                Ok((artifact, critique))
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;
        let critiques = critique_records
            .iter()
            .map(|(_, critique)| critique.clone())
            .collect::<Vec<_>>();
        akzio_domain::validate_verified_forecast_slots(
            &draft,
            &claim_records
                .iter()
                .map(|(artifact, claim)| {
                    (
                        ArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            kind: ArtifactKind::Claim,
                        },
                        claim.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            &critiques,
        )
        .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;
        let active_claims = draft
            .claims
            .iter()
            .zip(&claims)
            .filter(|(_, claim)| {
                draft.forecasts.iter().any(|forecast| {
                    !forecast.is_neutral()
                        && forecast.horizon == claim.horizon
                        && claim.grounds.iter().any(|g| {
                            g.role == akzio_domain::EvidenceGroundRole::Directional
                                && g.assets.contains(&forecast.asset)
                        })
                })
            })
            .collect::<Vec<_>>();
        let active_refs = active_claims
            .iter()
            .map(|(reference, _)| (*reference).clone())
            .collect::<Vec<_>>();
        let active_payloads = active_claims
            .iter()
            .map(|(_, claim)| (*claim).clone())
            .collect::<Vec<_>>();
        let critical_claim_unverified =
            has_unverified_critical_claim(&active_refs, &active_payloads, &critiques);

        let policy_influences = draft
            .applied_learning_refs
            .iter()
            .filter(|reference| {
                matches!(
                    reference.kind,
                    ArtifactKind::Experience | ArtifactKind::CandidatePolicy
                )
            })
            .map(|reference| {
                self.validate_policy_influence(reference)?;
                Ok(reference.clone())
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;

        let mut hard_blockers = draft.hard_blockers.iter().copied().collect::<BTreeSet<_>>();
        if critical_claim_unverified {
            hard_blockers.insert(HardBlocker::UnverifiedClaim);
        }
        if draft
            .material_conflicts
            .iter()
            .any(|conflict| active_refs.contains(&conflict.claim))
        {
            hard_blockers.insert(HardBlocker::MaterialConflict);
        }

        let mut consensus_diversity = (!claim_records.is_empty())
            .then(|| self.consensus_diversity_assessment(&claim_records, draft.confidence_ppm))
            .transpose()?;
        let correlated_consensus = consensus_diversity
            .as_ref()
            .is_some_and(|assessment| assessment.participant_count > 1 && !assessment.independent);
        let effective_confidence_ppm = consensus_diversity
            .as_ref()
            .map_or(draft.confidence_ppm, |assessment| {
                self.effective_consensus_confidence_ppm(assessment, draft.confidence_ppm)
            });
        if let Some(assessment) = &mut consensus_diversity {
            assessment.record_confidence(draft.confidence_ppm, effective_confidence_ppm)?;
        }
        let mut soft_warnings = draft.soft_warnings.iter().copied().collect::<BTreeSet<_>>();
        if correlated_consensus {
            soft_warnings.insert(SoftWarning::CorrelatedConsensus);
        }

        let policy_hash = self.policy.policy_hash()?;
        let horizon_trace = self.policy.horizon_trace(input.now, &draft.forecasts)?;
        if !horizon_trace.conflicts.is_empty() {
            hard_blockers.insert(HardBlocker::HorizonConflict);
        }
        let (target, portfolio_risk) =
            self.policy
                .target_with_risk(input.now, effective_confidence_ppm, &draft.forecasts)?;
        let evidence_cutoff = selected
            .iter()
            .filter_map(|reference| self.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| artifact.provenance.observed_at)
            .filter(|observed_at| *observed_at <= input.now)
            .max()
            .unwrap_or(input.now);
        let policy_valid_until = input.now
            + chrono::Duration::milliseconds(
                i64::try_from(self.policy.maximum_execution_delay_ms).map_err(|_| {
                    DomainError::InvalidBudget {
                        field: "decision_policy.maximum_execution_delay_ms",
                    }
                })?,
            );
        let thesis_valid_until = draft
            .forecasts
            .iter()
            .filter_map(|forecast| forecast.thesis.as_ref())
            .map(|thesis| thesis.thesis_valid_until)
            .min()
            .ok_or(DomainError::EmptyField {
                field: "decision_draft.forecast_thesis",
            })?;
        let validity = DecisionValidity {
            evidence_cutoff,
            generated_at: input.now,
            valid_until: policy_valid_until.min(thesis_valid_until),
            maximum_execution_delay_ms: self.policy.maximum_execution_delay_ms,
            market_state_hash: manifest.artifact_id.0.clone(),
        };
        let score_ratio = |passing: usize, total: usize| {
            (total > 0).then(|| {
                u32::try_from(
                    u64::try_from(passing)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(u64::from(WeightPpm::SCALE))
                        / u64::try_from(total).unwrap_or(u64::MAX),
                )
                .unwrap_or(WeightPpm::SCALE)
            })
        };
        let observed_events = claim_records
            .iter()
            .flat_map(|(_, claim)| claim.grounds.iter().map(|ground| ground.evidence.clone()))
            .chain(critique_records.iter().flat_map(|(_, critique)| {
                critique
                    .grounds
                    .iter()
                    .map(|ground| ground.evidence.clone())
                    .chain(
                        critique
                            .supporting_refs
                            .iter()
                            .map(|reference| reference.evidence.clone()),
                    )
                    .chain(
                        critique
                            .conflicting_refs
                            .iter()
                            .map(|reference| reference.evidence.clone()),
                    )
            }))
            .chain(draft.evidence.iter().cloned())
            .collect::<BTreeSet<_>>();
        let grounded_event_count = claim_records
            .iter()
            .flat_map(|(artifact, claim)| {
                claim
                    .grounds
                    .iter()
                    .map(move |ground| artifact.source_refs.contains(&ground.evidence))
            })
            .chain(critique_records.iter().flat_map(|(artifact, critique)| {
                critique
                    .source_refs()
                    .into_iter()
                    .filter(|reference| reference.kind != ArtifactKind::Claim)
                    .map(move |reference| artifact.source_refs.contains(&reference))
            }))
            .filter(|grounded| *grounded)
            .count();
        let total_event_ground_count = claim_records
            .iter()
            .map(|(_, claim)| claim.grounds.len())
            .chain(critique_records.iter().map(|(_, critique)| {
                critique
                    .source_refs()
                    .into_iter()
                    .filter(|reference| reference.kind != ArtifactKind::Claim)
                    .count()
            }))
            .sum();
        let event_grounding_ppm = score_ratio(grounded_event_count, total_event_ground_count);
        let premise_support_ppm = score_ratio(
            claim_records
                .iter()
                .filter(|(artifact, claim)| {
                    !claim.grounds.is_empty()
                        && claim
                            .grounds
                            .iter()
                            .all(|ground| artifact.source_refs.contains(&ground.evidence))
                })
                .count(),
            claim_records.len(),
        );
        let verification_refs = critiques
            .iter()
            .flat_map(|critique| {
                critique
                    .supporting_refs
                    .iter()
                    .chain(critique.conflicting_refs.iter())
            })
            .collect::<Vec<_>>();
        let temporal_validity_ppm = score_ratio(
            verification_refs
                .iter()
                .filter(|reference| reference.is_current_authoritative())
                .count(),
            verification_refs.len(),
        );
        let logic_validity_ppm = score_ratio(
            active_refs
                .iter()
                .filter(|claim| {
                    let statuses = critiques
                        .iter()
                        .filter(|critique| critique.target == **claim)
                        .map(|critique| critique.verification_status)
                        .collect::<Vec<_>>();
                    statuses.contains(&ClaimVerificationStatus::Supported)
                        && !statuses.contains(&ClaimVerificationStatus::Contradicted)
                })
                .count(),
            active_refs.len(),
        );
        let stage_latencies = DecisionStageLatencies {
            retrieval_latency_millis: None,
            model_latency_millis: self.model_stage_latency_millis(&proposal, &claim_records)?,
            critic_latency_millis: self.critic_stage_latency_millis(&critique_records)?,
            decision_gate_latency_millis: Some(
                u64::try_from(decision_gate_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ),
        };
        let investment_logic = InvestmentLogicTrace {
            observed_events: observed_events.into_iter().collect(),
            accepted_premises: draft.claims.clone(),
            rejected_premises: critiques
                .iter()
                .filter(|critique| {
                    critique.verification_status == ClaimVerificationStatus::Contradicted
                })
                .map(|critique| critique.target.clone())
                .collect(),
            alternatives_considered: draft.critiques.clone(),
            selected_action_hash: content_hash_json(&serde_json::to_value(&target)?)?,
            invalidation_conditions: draft
                .forecasts
                .iter()
                .flat_map(|forecast| {
                    forecast
                        .thesis
                        .iter()
                        .flat_map(|thesis| thesis.invalidation_conditions.iter().cloned())
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            quality: ProcessQualityAssessment {
                event_grounding_ppm,
                premise_support_ppm,
                temporal_validity_ppm,
                logic_validity_ppm,
                mandate_consistency_ppm: None,
                portfolio_consistency_ppm: None,
                action_feasibility_ppm: None,
            },
            consensus_diversity,
            stage_latencies,
        };
        if self.policy.minimum_process_quality_ppm > 0
            && investment_logic
                .quality
                .research_measured_floor()
                .is_none_or(|floor| floor < self.policy.minimum_process_quality_ppm)
        {
            hard_blockers.insert(HardBlocker::UnverifiedClaim);
        }
        let context_payload = DecisionContext {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_id: akzio_domain::DecisionId::new(),
            run_id: input.permit.run_id.clone(),
            claims: draft.claims.clone(),
            critiques: draft.critiques.clone(),
            evidence: draft.evidence.clone(),
            policy_influences,
            applied_learning_refs: draft.applied_learning_refs.clone(),
            rejected_learning_refs: draft.rejected_learning_refs.clone(),
            material_conflicts: draft.material_conflicts.clone(),
            hard_blockers: hard_blockers.into_iter().collect(),
            soft_warnings: soft_warnings.into_iter().collect(),
            decision_policy_hash: policy_hash,
            behavior_bundle_hash: None,
            portfolio_risk,
            target: target.clone(),
            created_at: input.now,
            validity: Some(validity),
            horizon_trace: Some(horizon_trace),
            investment_logic: Some(investment_logic),
        };
        context_payload.validate()?;

        let lifecycle = match self.store.run_purpose(&input.permit.run_id)? {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            _ => ArtifactLifecycle::RunScoped,
        };
        let mut context_sources = Vec::with_capacity(selected.len() + 2);
        context_sources.push(input.proposal.clone());
        context_sources.push(manifest_ref.clone());
        context_sources.extend(selected.iter().cloned());
        let decision_context = self.artifact(
            ArtifactKind::DecisionContext,
            "decision.context",
            &context_payload,
            lifecycle,
            context_sources,
            input,
        )?;
        let context_ref = ArtifactRef {
            artifact_id: decision_context.artifact_id.clone(),
            kind: ArtifactKind::DecisionContext,
        };
        let decision_payload = Decision {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_context: context_ref.clone(),
            summary: draft.summary,
            targets: target,
            confidence_ppm: effective_confidence_ppm,
            forecasts: draft.forecasts,
            created_at: input.now,
        };
        decision_payload.validate()?;
        let decision = self.artifact(
            ArtifactKind::Decision,
            "decision.bound",
            &decision_payload,
            lifecycle,
            vec![context_ref, input.proposal.clone()],
            input,
        )?;

        self.store.commit_attempt(
            &input.permit,
            &[decision_context.clone(), decision.clone()],
            TaskStatus::Succeeded,
            input.now,
        )?;
        Ok(DecisionGateOutput {
            decision_context,
            decision,
        })
    }

    fn consensus_diversity_assessment(
        &self,
        claim_records: &[(Artifact, ResearchClaim)],
        raw_confidence_ppm: u32,
    ) -> DecisionGateResult<ConsensusDiversityAssessment> {
        let mut grouped = BTreeMap::<String, (BTreeSet<ContentHash>, BTreeSet<ContentHash>)>::new();
        for (claim_artifact, claim) in claim_records {
            let agent_id = claim_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.task_id.as_ref())
                .map(|task_id| task_id.0.clone())
                .unwrap_or_else(|| format!("claim:{}", claim_artifact.artifact_id));
            let entry = grouped.entry(agent_id).or_default();
            for turn_ref in claim_artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            {
                let turn = self.load_expected(turn_ref, ArtifactKind::AgentTurn)?;
                let payload: serde_json::Value =
                    serde_json::from_slice(&self.store.read_blob(&turn.blob)?)?;
                if let Some(value) = payload.get("capability_snapshot_hash") {
                    if let Ok(hash) = serde_json::from_value::<ContentHash>(value.clone()) {
                        entry.0.insert(hash);
                    }
                }
            }
            for ground in &claim.grounds {
                let evidence = self.load_expected(&ground.evidence, ground.evidence.kind)?;
                let payload: serde_json::Value =
                    serde_json::from_slice(&self.store.read_blob(&evidence.blob)?)?;
                let cluster = payload
                    .get("financial_content")
                    .and_then(|value| value.get("content_similarity_cluster"))
                    .and_then(|value| serde_json::from_value::<ContentHash>(value.clone()).ok())
                    .unwrap_or_else(|| evidence.artifact_id.0.clone());
                entry.1.insert(cluster);
            }
        }

        let contributions = grouped
            .into_iter()
            .map(
                |(agent_id, (capability_hashes, evidence_clusters))| AgentEvidenceContribution {
                    agent_id,
                    capability_snapshot_hash: (capability_hashes.len() == 1).then(|| {
                        capability_hashes
                            .into_iter()
                            .next()
                            .expect("one capability hash")
                    }),
                    evidence_clusters,
                },
            )
            .collect::<Vec<_>>();
        let mut assessment = ConsensusDiversityAssessment::from_contributions(
            &contributions,
            MAXIMUM_CONSENSUS_SOURCE_OVERLAP_PPM,
        )?;
        assessment.record_confidence(raw_confidence_ppm, raw_confidence_ppm)?;
        Ok(assessment)
    }

    fn effective_consensus_confidence_ppm(
        &self,
        assessment: &ConsensusDiversityAssessment,
        raw_confidence_ppm: u32,
    ) -> u32 {
        if assessment.participant_count > 1 && !assessment.independent {
            raw_confidence_ppm.min(self.policy.min_confidence_ppm)
        } else if assessment.participant_count > 1
            && assessment.unique_evidence_clusters < assessment.participant_count
        {
            let cluster_ratio_ppm = (assessment.unique_evidence_clusters as u64 * 1_000_000)
                / (assessment.participant_count as u64);
            let scaled_confidence =
                ((raw_confidence_ppm as u64) * cluster_ratio_ppm / 1_000_000) as u32;
            scaled_confidence
                .max(self.policy.min_confidence_ppm)
                .min(raw_confidence_ppm)
        } else {
            raw_confidence_ppm
        }
    }

    fn model_stage_latency_millis(
        &self,
        proposal: &Artifact,
        claim_records: &[(Artifact, ResearchClaim)],
    ) -> DecisionGateResult<Option<u64>> {
        let mut turns = proposal
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            .cloned()
            .collect::<BTreeSet<_>>();
        for (claim, _) in claim_records {
            turns.extend(
                claim
                    .source_refs
                    .iter()
                    .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
                    .cloned(),
            );
        }
        self.complete_turn_latency_millis(&turns)
    }

    fn critic_stage_latency_millis(
        &self,
        critique_records: &[(Artifact, ResearchCritique)],
    ) -> DecisionGateResult<Option<u64>> {
        let turns = critique_records
            .iter()
            .flat_map(|(critique, _)| critique.source_refs.iter())
            .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.complete_turn_latency_millis(&turns)
    }

    fn complete_turn_latency_millis(
        &self,
        turns: &BTreeSet<ArtifactRef>,
    ) -> DecisionGateResult<Option<u64>> {
        if turns.is_empty() {
            return Ok(None);
        }
        let mut total = 0_u64;
        for turn_ref in turns {
            let turn = self.load_expected(turn_ref, ArtifactKind::AgentTurn)?;
            let payload: serde_json::Value =
                serde_json::from_slice(&self.store.read_blob(&turn.blob)?)?;
            let Some(latency) = payload
                .get("response")
                .and_then(|value| value.get("telemetry"))
                .and_then(|value| value.get("latency_millis"))
                .and_then(serde_json::Value::as_u64)
            else {
                return Ok(None);
            };
            total = total.saturating_add(latency);
        }
        Ok(Some(total))
    }

    fn validate_proposal(
        &self,
        proposal: &Artifact,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<akzio_domain::ContentHash> {
        let Some(origin) = proposal.origin.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        let Some(contract_hash) = origin.contract_hash.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        if proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal.producer != "agent.research.synthesizer"
            || proposal.provenance.source_family != "akzio.agent"
            || proposal.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id.is_none()
            || origin.attempt_id.is_none()
        {
            return Err(DecisionGateError::InvalidProposalProvenance);
        }
        Ok(contract_hash.clone())
    }
}

fn has_unverified_critical_claim(
    claim_refs: &[ArtifactRef],
    claims: &[ResearchClaim],
    critiques: &[ResearchCritique],
) -> bool {
    claim_refs
        .iter()
        .zip(claims.iter())
        .any(|(claim_ref, claim)| {
            if claim.materiality_ppm < CRITICAL_CLAIM_MATERIALITY_PPM {
                return false;
            }
            let mut matching = critiques
                .iter()
                .filter(|critique| critique.target == *claim_ref);
            let Some(verification) = matching.next() else {
                return true;
            };
            matching.next().is_some()
                || verification.verification_status != ClaimVerificationStatus::Supported
                || verification.blocker
        })
}
