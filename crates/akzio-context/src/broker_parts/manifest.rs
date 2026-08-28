impl ContextBroker {
    pub fn new(store: V2Store) -> Self {
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
            let tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            if selection.estimated_tokens != tokens {
                return Err(ContextError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
            selected.push(selection.artifact.clone());
        }
        selected.sort();
        let mut expected_source_refs = selected.clone();
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
        let learning_scope = self.infer_learning_scope(&candidate_refs)?;
        candidate_refs.extend(self.learning_candidates(policy, &learning_scope)?);
        let artifacts = candidate_refs
            .into_iter()
            .filter(|reference| seen.insert(reference.artifact_id.clone()))
            .map(|reference| self.store.artifact(&reference.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
    let mut eligible = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        if artifact.kind == ArtifactKind::RawEvidence
            || !policy.permitted_kinds.contains(&artifact.kind)
            || (!policy.permitted_source_families.is_empty()
                && !policy
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family))
        {
            continue;
        }
        if self.overlay_is_eligible(&artifact)? {
            eligible.push(artifact);
        }
    }
        let mut artifacts = eligible;
        let analyst_bundle = if contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID {
            self.select_analyst_bundle(&artifacts, policy)?
        } else {
            None
        };
        artifacts.sort_by(|left, right| {
            context_rank(left)
                .cmp(&context_rank(right))
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

        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        let mut selections = Vec::new();
        for artifact in artifacts {
            let tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            let next_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            let next_tokens = estimated_tokens.saturating_add(tokens);
            if selections.len() >= usize::from(policy.max_artifacts)
                || next_bytes > policy.max_bytes
                || next_tokens > policy.max_tokens
            {
                continue;
            }
            total_bytes = next_bytes;
            estimated_tokens = next_tokens;
            selections.push(ContextSelection {
                artifact: ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
                reason: selection_reason(artifact.kind).to_owned(),
                estimated_tokens: tokens,
            });
        }
        if selections.len() < usize::from(policy.min_artifacts) {
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
            let tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
            selection.estimated_tokens = tokens;
            revalidated.push(selection);
        }
        let selections = revalidated;
        if selections.len() < usize::from(policy.min_artifacts) {
            return Err(ContextError::BudgetExceeded);
        }

        let input_hash = manifest_input_hash(&selections)?;
        let payload = ContextManifestPayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            contract_hash: contract.contract_hash.clone(),
            selections: selections.clone(),
            total_bytes,
            estimated_tokens,
            input_hash,
        };
        payload.validate(policy)?;
        let blob = self.store.put_json(&payload)?;
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
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            selections
                .iter()
                .map(|selection| selection.artifact.clone())
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

    fn learning_candidates(
        &self,
        policy: &ContextPolicy,
        scope: &LessonScope,
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
