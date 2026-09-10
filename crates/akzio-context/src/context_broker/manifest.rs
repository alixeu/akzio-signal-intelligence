impl ContextBroker {
    fn required_role_inputs(
        &self,
        contract: &AgentContract,
        artifacts: &[Artifact],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        let purpose = contract.purpose.as_str();
        let missing = |requirement: String| ContextError::MissingRequiredInput {
            purpose: purpose.to_owned(),
            requirement,
        };
        let mut required = BTreeSet::new();
        if purpose == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
            for kind in [
                ArtifactKind::Decision,
                ArtifactKind::DecisionContext,
                ArtifactKind::ExecutionContext,
                ArtifactKind::OutcomeSchedule,
            ] {
                let artifact = artifacts
                    .iter()
                    .find(|a| a.kind == kind)
                    .ok_or_else(|| missing(format!("{kind:?}")))?;
                required.insert(artifact.artifact_id.clone());
            }
            let stage = artifacts
                .iter()
                .find(|a| a.producer == "learning.outcome_stage" || a.kind == ArtifactKind::Outcome)
                .ok_or_else(|| missing("Rust stage packet or sealed Outcome".to_owned()))?;
            required.insert(stage.artifact_id.clone());
            let context = artifacts
                .iter()
                .find(|a| a.kind == ArtifactKind::DecisionContext)
                .expect("required above");
            let value: Value = self.read_payload(context)?;
            for key in ["claims", "critiques"] {
                let refs: Vec<ArtifactRef> = serde_json::from_value(
                    value
                        .get(key)
                        .cloned()
                        .ok_or_else(|| missing(format!("DecisionContext.{key}")))?,
                )?;
                for reference in refs {
                    if !artifacts
                        .iter()
                        .any(|a| a.artifact_id == reference.artifact_id && a.kind == reference.kind)
                    {
                        return Err(missing(format!(
                            "DecisionContext.{key}: {}",
                            reference.artifact_id
                        )));
                    }
                    required.insert(reference.artifact_id);
                }
            }
            required.extend(
                artifacts
                    .iter()
                    .filter(|a| a.kind == ArtifactKind::Retrospective)
                    .map(|a| a.artifact_id.clone()),
            );
        } else if matches!(
            purpose,
            RESEARCH_CRITIC_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID
        ) {
            for artifact in artifacts
                .iter()
                .filter(|a| matches!(a.kind, ArtifactKind::Claim | ArtifactKind::Critique))
            {
                required.insert(artifact.artifact_id.clone());
                let refs = if artifact.kind == ArtifactKind::Claim {
                    self.read_payload::<ResearchClaim>(artifact)?.source_refs()
                } else {
                    self.read_payload::<ResearchCritique>(artifact)?
                        .source_refs()
                };
                for reference in refs {
                    if !artifacts
                        .iter()
                        .any(|a| a.artifact_id == reference.artifact_id && a.kind == reference.kind)
                    {
                        return Err(missing(format!(
                            "research source {}",
                            reference.artifact_id
                        )));
                    }
                    required.insert(reference.artifact_id);
                }
            }
        }
        Ok(required)
    }
    fn extend_synthesizer_critique_targets(
        &self,
        permit: &TaskWritePermit,
        candidates: &mut Vec<ArtifactRef>,
    ) -> ContextResult<()> {
        let critiques = candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::Critique)
            .cloned()
            .collect::<Vec<_>>();
        for reference in critiques {
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != ArtifactKind::Critique {
                return Err(ContextError::InvalidManifestClosure);
            }
            if !artifact
                .source_refs
                .iter()
                .any(|source| source.kind == ArtifactKind::ContextManifest)
            {
                continue;
            }
            let critique: ResearchCritique = self.read_payload(&artifact)?;
            critique.validate()?;
            if !artifact.source_refs.contains(&critique.target) {
                return Err(ContextError::InvalidManifestClosure);
            }
            let origin = artifact
                .origin
                .as_ref()
                .ok_or(ContextError::InvalidManifestClosure)?;
            if origin.run_id.as_ref() != Some(&permit.run_id)
                || origin.contract_hash.as_ref()
                    != artifact.provenance.producer_contract_hash.as_ref()
            {
                return Err(ContextError::InvalidManifestClosure);
            }
            let target = self.store.artifact(&critique.target.artifact_id)?;
            if target.kind != ArtifactKind::Claim
                || target
                    .origin
                    .as_ref()
                    .and_then(|target_origin| target_origin.run_id.as_ref())
                    != Some(&permit.run_id)
            {
                return Err(ContextError::InvalidManifestClosure);
            }
            let readable_from_source_manifest = artifact
                .source_refs
                .iter()
                .filter(|source| source.kind == ArtifactKind::ContextManifest)
                .any(|source| {
                    let Ok(manifest) = self.store.artifact(&source.artifact_id) else {
                        return false;
                    };
                    let Some(manifest_origin) = manifest.origin.as_ref() else {
                        return false;
                    };
                    if manifest.kind != ArtifactKind::ContextManifest
                        || manifest_origin.run_id != origin.run_id
                        || manifest_origin.task_id != origin.task_id
                        || manifest_origin.attempt_id != origin.attempt_id
                        || manifest_origin.contract_hash != origin.contract_hash
                        || manifest.provenance.producer_contract_hash
                            != artifact.provenance.producer_contract_hash
                    {
                        return false;
                    }
                    self.read_payload::<ContextManifestPayload>(&manifest)
                        .is_ok_and(|payload| {
                            payload
                                .selections
                                .iter()
                                .any(|selection| selection.artifact == critique.target)
                        })
                });
            if !readable_from_source_manifest {
                return Err(ContextError::InvalidManifestClosure);
            }
            candidates.push(critique.target);
        }
        Ok(())
    }

    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Reconstructs durable learning influences only from the exact persisted
    /// manifest closure. Current policy heads are rechecked at use time.
    pub fn policy_influences(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ContextResult<Vec<ArtifactRef>> {
        self.policy_influences_internal(permit, contract, manifest, now, true)
    }

    fn validate_manifest_closure(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
        require_live_grant: bool,
    ) -> ContextResult<Vec<ArtifactRef>> {
        contract.validate()?;
        if !manifest.grant.matches_permit(permit)
            || manifest.grant.contract_hash != contract.contract_hash
            || manifest.payload.contract_hash != contract.contract_hash
            || (require_live_grant && manifest.grant.expires_at <= now)
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        let persisted = self.store.artifact(&manifest.grant.manifest_artifact_id)?;
        persisted.validate()?;
        let expected_producer = format!("context.{}", contract.purpose.as_str());
        let Some(origin) = persisted.origin.as_ref() else {
            return Err(ContextError::InvalidManifestClosure);
        };
        if persisted != manifest.artifact
            || persisted.kind != ArtifactKind::ContextManifest
            || persisted.lifecycle != ArtifactLifecycle::RunScoped
            || persisted.producer != expected_producer
            || persisted.provenance.source_family != "akzio.context"
            || persisted.provenance.producer_contract_hash.as_ref() != Some(&contract.contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id.as_ref() != Some(&permit.task_id)
            || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
            || origin.contract_hash.as_ref() != Some(&contract.contract_hash)
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        let persisted_payload: ContextManifestPayload = self.read_payload(&persisted)?;
        if persisted_payload != manifest.payload
            || persisted_payload.validate(&contract.context).is_err()
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        let mut selected = Vec::with_capacity(persisted_payload.selections.len());
        let mut readable = BTreeSet::new();
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for selection in &persisted_payload.selections {
            if !readable.insert(selection.artifact.artifact_id.clone()) {
                return Err(ContextError::InvalidManifestClosure);
            }
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            artifact.validate()?;
            if artifact.kind != selection.artifact.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            self.assert_context_permitted(&contract.context, &artifact)?;
            self.assert_context_run(permit, &artifact)?;
            let legacy_tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            let (_, projected_tokens) = self.projection_budget(&artifact)?;
            let expected_tokens = if selection.projected_bytes.is_some() {
                projected_tokens
            } else {
                legacy_tokens
            };
            if selection.estimated_tokens != expected_tokens {
                return Err(ContextError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            projected_bytes = projected_bytes.saturating_add(
                selection
                    .projected_bytes
                    .unwrap_or(artifact.blob.bytes),
            );
            estimated_tokens = estimated_tokens.saturating_add(selection.estimated_tokens);
            selected.push(selection.artifact.clone());
        }
        let required_inputs = selected
            .iter()
            .map(|r| self.store.artifact(&r.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        self.required_role_inputs(contract, &required_inputs)?;
        selected.sort();
        let mut expected_source_refs = selected.clone();
        expected_source_refs.extend(
            persisted_payload
                .quarantined
                .iter()
                .map(|quarantine| quarantine.artifact.clone()),
        );
        expected_source_refs.extend(
            persisted
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .cloned(),
        );
        expected_source_refs.sort();
        expected_source_refs.dedup();
        if expected_source_refs != persisted.source_refs
            || manifest.grant.readable != readable
            || manifest.grant.raw_source_closure
                != self.raw_closure(&contract.context, &persisted_payload.selections)?
            || persisted_payload.total_bytes != total_bytes
            || persisted_payload.projected_bytes.unwrap_or(total_bytes) != projected_bytes
            || persisted_payload.estimated_tokens != estimated_tokens
            || persisted_payload.input_hash != manifest_input_hash(&persisted_payload.selections)?
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        Ok(selected)
    }

    fn policy_influences_internal(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
        require_live_grant: bool,
    ) -> ContextResult<Vec<ArtifactRef>> {
        let selected =
            self.validate_manifest_closure(permit, contract, manifest, now, require_live_grant)?;
        let mut influences = Vec::new();
        for reference in selected {
            if !matches!(
                reference.kind,
                ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != reference.kind || !self.overlay_is_eligible(&artifact)? {
                return Err(ContextError::ForbiddenArtifact {
                    artifact_id: reference.artifact_id,
                });
            }
            influences.push(reference);
        }
        Ok(influences)
    }

    /// Build context from an explicit candidate set only. There is intentionally no
    /// `documents_for_run` fallback: a task's data surface is reproducible from the
    /// manifest and source closure alone.
    pub fn assemble(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        contract.validate()?;
        let policy = &contract.context;
        let mut seen = BTreeSet::new();
        let mut candidate_refs = candidates.into_iter().collect::<Vec<_>>();
        if contract.purpose.as_str() == RESEARCH_SYNTHESIZER_RECIPE_ID {
            self.extend_synthesizer_critique_targets(permit, &mut candidate_refs)?;
        }
        let learning_scope = self.infer_learning_scope(&candidate_refs)?;
        candidate_refs.extend(self.learning_candidates(policy, &learning_scope, now)?);
        let artifacts = candidate_refs
            .into_iter()
            .filter(|reference| seen.insert(reference.artifact_id.clone()))
            .map(|reference| self.store.artifact(&reference.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut eligible = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            if !governed_internal_source(&artifact)
                || artifact.kind == ArtifactKind::RawEvidence
                || !policy.permitted_kinds.contains(&artifact.kind)
                || (!policy.permitted_source_families.is_empty()
                    && !policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family))
            {
                continue;
            }
            if self.overlay_is_eligible(&artifact)? {
                self.assert_context_run(permit, &artifact)?;
                let artifact = if artifact.kind == ArtifactKind::NormalizedEvidence
                    && policy.permitted_kinds.contains(&ArtifactKind::SemanticDetail)
                    && (policy.permitted_source_families.is_empty()
                        || policy
                            .permitted_source_families
                            .contains("akzio.ingest"))
                    && self
                        .document_value(&artifact)?
                        .get("resource")
                        .and_then(Value::as_str)
                        .is_some_and(|resource| resource.starts_with("option_chain:"))
                {
                    self.option_projection_artifact(permit, contract, &artifact, now)?
                } else {
                    artifact
                };
                self.assert_context_run(permit, &artifact)?;
                eligible.push(artifact);
            }
        }
        let (mut artifacts, quarantined) = self.partition_untrusted_context(eligible)?;
        let analyst_bundle = if contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID {
            self.select_analyst_bundle(&artifacts, policy)?
        } else {
            None
        };
        let mut directly_referenced = BTreeSet::new();
        if matches!(
            contract.purpose.as_str(),
            RESEARCH_CRITIC_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID
        ) {
            for artifact in &artifacts {
                match artifact.kind {
                    ArtifactKind::Claim => {
                        if let Ok(claim) = self.read_payload::<ResearchClaim>(artifact) {
                            if claim.validate().is_ok() {
                                directly_referenced.extend(claim.source_refs());
                            }
                        }
                    }
                    ArtifactKind::Critique => {
                        if let Ok(critique) = self.read_payload::<ResearchCritique>(artifact) {
                            if critique.validate().is_ok() {
                                directly_referenced.extend(critique.source_refs());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        artifacts.sort_by(|left, right| {
            purpose_rank(contract.purpose.as_str(), left)
                .cmp(&purpose_rank(contract.purpose.as_str(), right))
                .then_with(|| {
                    let left_reference = ArtifactRef {
                        artifact_id: left.artifact_id.clone(),
                        kind: left.kind,
                    };
                    let right_reference = ArtifactRef {
                        artifact_id: right.artifact_id.clone(),
                        kind: right.kind,
                    };
                    (!directly_referenced.contains(&left_reference))
                        .cmp(&(!directly_referenced.contains(&right_reference)))
                })
                .then_with(|| {
                    right
                        .provenance
                        .confidence_ppm
                        .cmp(&left.provenance.confidence_ppm)
                })
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });

        if let Some(bundle) = analyst_bundle {
            let bundle_ids = bundle
                .iter()
                .map(|artifact| artifact.artifact_id.clone())
                .collect::<BTreeSet<_>>();
            let mut prioritized = bundle;
            prioritized.extend(
                artifacts
                    .into_iter()
                    .filter(|artifact| !bundle_ids.contains(&artifact.artifact_id)),
            );
            artifacts = prioritized;
        }

        let required = self.required_role_inputs(contract, &artifacts)?;
        artifacts.sort_by_key(|artifact| !required.contains(&artifact.artifact_id));
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        let mut selections = Vec::new();
        for artifact in artifacts {
            let (projected, tokens) = self.projection_budget(&artifact)?;
            let next_source_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            let next_projected_bytes = projected_bytes.saturating_add(projected);
            let next_tokens = estimated_tokens.saturating_add(tokens);
            if selections.len() >= usize::from(policy.max_artifacts)
                || next_source_bytes > Self::source_budget(policy)
                || next_projected_bytes > policy.max_bytes
                || next_tokens > policy.max_tokens
            {
                if required.contains(&artifact.artifact_id) {
                    return Err(ContextError::MissingRequiredInput {
                        purpose: contract.purpose.as_str().to_owned(),
                        requirement: format!(
                            "{:?} {} exceeds context budget",
                            artifact.kind, artifact.artifact_id
                        ),
                    });
                }
                continue;
            }
            total_bytes = next_source_bytes;
            projected_bytes = next_projected_bytes;
            estimated_tokens = next_tokens;
            selections.push(ContextSelection {
                artifact: ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
                reason: if artifact.producer == "evidence.option_projection" {
                    "option_chain_projection".to_owned()
                } else {
                    selection_reason(artifact.kind).to_owned()
                },
                estimated_tokens: tokens,
                projected_bytes: Some(projected),
                trust: context_trust(artifact.kind),
            });
        }
        if selections.len() < usize::from(policy.min_artifacts)
            || (contract.purpose.as_str() == RESEARCH_CRITIC_RECIPE_ID
                && (!selections
                    .iter()
                    .any(|selection| selection.artifact.kind == ArtifactKind::Claim)
                    || !selections.iter().any(|selection| {
                        selection.artifact.kind == ArtifactKind::NormalizedEvidence
                    })))
        {
            return Err(ContextError::BudgetExceeded);
        }
        // Artifact bytes are immutable, but overlay eligibility reads the mutable
        // policy head. Re-check selected artifacts immediately before minting the grant.
        let mut revalidated = Vec::with_capacity(selections.len());
        total_bytes = 0;
        estimated_tokens = 0;
        for mut selection in selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            self.assert_context_permitted(policy, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                continue;
            }
            let (projected, tokens) = self.projection_budget(&artifact)?;
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
            selection.estimated_tokens = tokens;
            selection.projected_bytes = Some(projected);
            revalidated.push(selection);
        }
        let selections = revalidated;
        if selections.len() < usize::from(policy.min_artifacts) {
            return Err(ContextError::BudgetExceeded);
        }

        self.mint_manifest(
            permit,
            contract,
            selections,
            total_bytes,
            estimated_tokens,
            quarantined,
            Vec::new(),
            now,
            grant_ttl,
        )
    }

    /// Persist a manifest over an already-budgeted selection list and mint its
    /// grant.
    ///
    /// `extra_source_refs` records lineage that is not itself a selection — the
    /// baseline manifest an ablation arm was derived from. Only
    /// `ArtifactKind::ContextManifest` refs belong there, because
    /// `validate_manifest_closure` reconstructs the expected `source_refs` as the
    /// selections plus exactly those.
    #[allow(clippy::too_many_arguments)]
    fn mint_manifest(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        selections: Vec<ContextSelection>,
        total_bytes: u64,
        estimated_tokens: u32,
        quarantined: Vec<ContextQuarantine>,
        extra_source_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        let policy = &contract.context;
        let role_inputs = selections
            .iter()
            .map(|s| self.store.artifact(&s.artifact.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        self.required_role_inputs(contract, &role_inputs)?;
        let input_hash = manifest_input_hash(&selections)?;
        let projected_bytes = selections
            .iter()
            .map(|selection| selection.projected_bytes.unwrap_or(0))
            .sum::<u64>();
        let payload = ContextManifestPayload {
            schema_version: DOMAIN_SCHEMA_VERSION,
            contract_hash: contract.contract_hash.clone(),
            selections: selections.clone(),
            quarantined: quarantined.clone(),
            total_bytes,
            projected_bytes: Some(projected_bytes),
            estimated_tokens,
            input_hash,
        };
        payload.validate(policy)?;
        let blob = self.store.stage_json(&payload)?;
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            blob,
            format!("context.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            selections
                .iter()
                .map(|selection| selection.artifact.clone())
                .chain(
                    quarantined
                        .iter()
                        .map(|quarantine| quarantine.artifact.clone()),
                )
                .chain(extra_source_refs)
                .collect(),
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ContextManifestCreated,
            now,
        )?;
        let grant = ReadGrant {
            manifest_artifact_id: artifact.artifact_id.clone(),
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
            lease_id: permit.lease_id.clone(),
            epoch: permit.epoch,
            contract_hash: contract.contract_hash.clone(),
            readable: selections
                .iter()
                .map(|selection| selection.artifact.artifact_id.clone())
                .collect(),
            raw_source_closure: self.raw_closure(policy, &selections)?,
            expires_at: now + grant_ttl,
        };
        Ok(ContextManifest {
            artifact,
            payload,
            grant,
        })
    }

    /// Derive the Lesson-off arm of a paired experiment from the Lesson-on
    /// manifest.
    ///
    /// Deliberately **not** a re-run of `assemble` with Lessons suppressed. The
    /// budget filler there skips an oversized candidate and keeps going, so
    /// removing the Lessons frees artifact, byte and token capacity that the next
    /// candidates immediately refill — and the two arms would then differ by
    /// whatever moved in rather than by the Lessons. Copying the baseline's
    /// non-Lesson selections verbatim makes that refill structurally impossible:
    /// the filler never runs.
    ///
    /// The result names the baseline manifest in its `source_refs`, which is what
    /// pairs the two arms durably and content-addressably.
    pub fn assemble_lesson_ablation(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        baseline: &ContextManifest,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        // The ablation copies the selection list only and mints its own grant, so
        // an expired baseline grant is not a read-authority leak. The baseline's
        // closure must still be intact, or the arm would be derived from an
        // unverified selection list.
        self.validate_manifest_closure(permit, contract, baseline, now, false)?;
        let policy = &contract.context;
        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        let mut selections = Vec::with_capacity(baseline.payload.selections.len());
        let mut ablated = 0_usize;
        for selection in &baseline.payload.selections {
            if selection.artifact.kind == ArtifactKind::Lesson {
                ablated += 1;
                continue;
            }
            // Overlay eligibility reads the mutable policy head. If a kept
            // artifact has since become ineligible the arms are no longer
            // comparable, so fail instead of silently dropping it and
            // reintroducing the very asymmetry this method exists to prevent.
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            self.assert_context_permitted(policy, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                return Err(ContextError::ForbiddenArtifact {
                    artifact_id: selection.artifact.artifact_id.clone(),
                });
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(selection.estimated_tokens);
            selections.push(selection.clone());
        }
        if ablated == 0 {
            return Err(ContextError::NoLessonToAblate);
        }
        if selections.len() < usize::from(policy.min_artifacts) {
            return Err(ContextError::BudgetExceeded);
        }

        self.mint_manifest(
            permit,
            contract,
            selections,
            total_bytes,
            estimated_tokens,
            baseline.payload.quarantined.clone(),
            vec![ArtifactRef {
                artifact_id: baseline.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            now,
            grant_ttl,
        )
    }

    fn learning_candidates(
        &self,
        policy: &ContextPolicy,
        scope: &LessonScope,
        now: DateTime<Utc>,
    ) -> ContextResult<Vec<ArtifactRef>> {
        let mut candidates = Vec::new();
        if policy.permitted_kinds.contains(&ArtifactKind::Lesson) {
            let mut lesson_count = 0;
            for stored in self.store.lessons(Some(LessonLifecycle::Active), 50)? {
                let artifact = stored.artifact;
                if !stored.lesson.scope.matches(
                    &scope.assets,
                    &scope.horizons,
                    &scope.regimes,
                    &scope.decision_stages,
                ) {
                    continue;
                }
                let usage = self.store.lesson_usage(&stored.lesson.lesson_id)?;
                let usage_count = usage
                    .context_manifests
                    .saturating_add(usage.decision_contexts);
                if !stored
                    .lesson
                    .is_retrievable(now, usage_count, &scope.regimes)
                {
                    continue;
                }
                if policy.permitted_source_families.is_empty()
                    || policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family)
                {
                    self.assert_context_permitted(policy, &artifact)?;
                    if self.overlay_is_eligible(&artifact)? {
                        candidates.push(ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        });
                        lesson_count += 1;
                        if lesson_count >= 4 {
                            break;
                        }
                    }
                }
            }
        }
        if policy.permitted_kinds.contains(&ArtifactKind::Experience) {
            let mut experience_count = 0;
            for artifact in self
                .store
                .recent_artifacts_by_kind(ArtifactKind::Experience, 100)?
            {
                if policy.permitted_source_families.is_empty()
                    || policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family)
                {
                    self.assert_context_permitted(policy, &artifact)?;
                    if self.overlay_is_eligible(&artifact)? {
                        candidates.push(ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        });
                        for source in &artifact.source_refs {
                            if source.kind == ArtifactKind::Retrospective
                                && policy.permitted_kinds.contains(&source.kind)
                            {
                                candidates.push(source.clone());
                            }
                        }
                        experience_count += 1;
                    }
                }
                if experience_count >= 4 {
                    break;
                }
            }
        }
        Ok(candidates)
    }
}
