impl V2DecisionRuntime {
    pub fn new(store: V2Store, policy: DecisionPolicy) -> DecisionGateResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &DecisionPolicy {
        &self.policy
    }

    /// Validate, bind, and atomically complete the DecisionGate attempt.
    pub fn decide(&self, input: &DecisionGateInput) -> DecisionGateResult<DecisionGateOutput> {
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
        if installed.contract.version < 5 {
            return Err(DecisionGateError::UnsupportedProposalContract);
        }
        let claims = draft
            .claims
            .iter()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::Claim)?;
                let claim: ResearchClaim =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                claim.validate()?;
                Ok(claim)
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;
        validate_decision_evidence_sufficiency(&draft, &claims)
            .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;

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
        if !draft.material_conflicts.is_empty() {
            hard_blockers.insert(HardBlocker::MaterialConflict);
        }

        let policy_hash = self.policy.policy_hash()?;
        let target = self
            .policy
            .target_for(draft.confidence_ppm, &draft.forecasts)?;
        let context_payload = DecisionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
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
            soft_warnings: draft.soft_warnings.clone(),
            decision_policy_hash: policy_hash,
            target: target.clone(),
            created_at: input.now,
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
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            decision_context: context_ref.clone(),
            summary: draft.summary,
            targets: target,
            confidence_ppm: draft.confidence_ppm,
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
