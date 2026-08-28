impl ContextBroker {
    fn infer_learning_scope(&self, references: &[ArtifactRef]) -> ContextResult<LessonScope> {
        let mut scope = LessonScope::default();
        for reference in references {
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if !matches!(
                artifact.kind,
                ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
            ) {
                continue;
            }
            let bytes = self.store.read_blob(&artifact.blob)?;
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let mut strings = Vec::new();
            collect_strings(&value, &mut strings);
            for value in strings {
                if let Ok(asset) = Asset::try_from(value.as_str()) {
                    scope.assets.insert(asset);
                }
                match value.to_ascii_lowercase().as_str() {
                    "t1" => {
                        scope.horizons.insert(DecisionHorizon::T1);
                    }
                    "t3" => {
                        scope.horizons.insert(DecisionHorizon::T3);
                    }
                    "t5" => {
                        scope.horizons.insert(DecisionHorizon::T5);
                    }
                    value if value.starts_with("regime:") => {
                        scope.regimes.insert(value[7..].to_owned());
                    }
                    value if value.starts_with("stage:") => {
                        scope.decision_stages.insert(value[6..].to_owned());
                    }
                    _ => {}
                }
            }
        }
        Ok(scope)
    }

    /// Attenuate a persisted parent manifest into a child attempt grant.
    /// Projection may include parent outputs, but only from the current
    /// succeeded attempt and only when their provenance closes to the parent.
    #[allow(clippy::too_many_arguments)]
    pub fn assemble_child(
        &self,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
        parent: &ContextManifest,
        projection: &ContextProjection,
        child_permit: &TaskWritePermit,
        child_contract: &AgentContract,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        projection.validate()?;
        child_contract.validate()?;
        if child_permit.contract_hash.as_ref() != Some(&child_contract.contract_hash) {
            return Err(ContextError::InvalidManifestClosure);
        }
        if child_permit.run_id != parent_permit.run_id {
            return Err(ContextError::InvalidManifestClosure);
        }
        let succeeded = self
            .store
            .current_succeeded_attempt(&parent_permit.run_id, &parent_permit.task_id)?;
        if succeeded.attempt_id != parent_permit.attempt_id
            || succeeded.lease_id != parent_permit.lease_id
            || succeeded.epoch != parent_permit.epoch
            || succeeded.contract_hash != parent_permit.contract_hash
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        if projection.parent_manifest.artifact_id != parent.artifact.artifact_id
            || projection.parent_manifest.kind != ArtifactKind::ContextManifest
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        // Reuse the canonical persisted-manifest validation before projecting.
        self.policy_influences_internal(parent_permit, parent_contract, parent, now, false)?;

        let parent_readable = parent
            .payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let parent_readable_ids = parent_readable
            .iter()
            .map(|reference| reference.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        if parent.grant.readable != parent_readable_ids {
            return Err(ContextError::InvalidManifestClosure);
        }
        let parent_raw_closure =
            self.raw_closure(&parent_contract.context, &parent.payload.selections)?;
        let needs_parent_outputs = projection
            .allowed
            .iter()
            .any(|reference| !parent_readable.contains(reference));
        let parent_outputs = if needs_parent_outputs {
            let mut outputs = succeeded.outputs.clone();
            let deliberation_sources = succeeded
                .outputs
                .iter()
                .flat_map(|output| output.source_refs.iter())
                .filter(|source| is_safe_deliberation_summary(source.kind))
                .cloned()
                .collect::<BTreeSet<_>>();
            for source in deliberation_sources {
                let artifact = self.store.artifact(&source.artifact_id)?;
                if artifact.kind != source.kind {
                    return Err(ContextError::InvalidManifestClosure);
                }
                outputs.push(artifact);
            }
            outputs
        } else {
            Vec::new()
        };
        let mut allowed = Vec::with_capacity(projection.allowed.len());
        for reference in &projection.allowed {
            if is_trace_kind(reference.kind) {
                return Err(ContextError::GrantDenied {
                    manifest_id: parent.artifact.artifact_id.clone(),
                    artifact_id: reference.artifact_id.clone(),
                });
            }
            if parent_readable.contains(reference) {
                allowed.push(self.store.artifact(&reference.artifact_id)?);
                continue;
            }
            let Some(output) = parent_outputs.iter().find(|artifact| {
                artifact.artifact_id == reference.artifact_id && artifact.kind == reference.kind
            }) else {
                return Err(ContextError::GrantDenied {
                    manifest_id: parent.artifact.artifact_id.clone(),
                    artifact_id: reference.artifact_id.clone(),
                });
            };
            self.validate_parent_output_provenance(
                output,
                &projection.parent_manifest,
                &parent_readable,
                &parent_raw_closure,
                parent_permit,
                parent_contract,
            )?;
            allowed.push(output.clone());
        }
        let policy = &child_contract.context;
        let mut selections = Vec::with_capacity(allowed.len());
        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for artifact in allowed {
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            };
            self.assert_context_permitted(policy, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                continue;
            }
        let tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
            selections.push(ContextSelection {
                artifact: reference,
                reason: projection.reason.clone(),
                estimated_tokens: tokens,
            });
        }
        if selections.len() < usize::from(policy.min_artifacts)
            || selections.len() > usize::from(policy.max_artifacts)
            || total_bytes > policy.max_bytes
            || estimated_tokens > policy.max_tokens
        {
            return Err(ContextError::BudgetExceeded);
        }

        let raw_source_closure = self.raw_closure(policy, &selections)?;
        if !raw_source_closure.is_subset(&parent_raw_closure) {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload = ContextManifestPayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            contract_hash: child_contract.contract_hash.clone(),
            input_hash: manifest_input_hash(&selections)?,
            selections: selections.clone(),
            total_bytes,
            estimated_tokens,
        };
        payload.validate(policy)?;
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            self.store.put_json(&payload)?,
            format!("context.{}", child_contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(child_contract.contract_hash.clone()),
            },
            Some(ArtifactOrigin {
                run_id: Some(child_permit.run_id.clone()),
                task_id: Some(child_permit.task_id.clone()),
                attempt_id: Some(child_permit.attempt_id.clone()),
                contract_hash: child_permit.contract_hash.clone(),
            }),
            std::iter::once(projection.parent_manifest.clone())
                .chain(
                    selections
                        .iter()
                        .map(|selection| selection.artifact.clone()),
                )
                .collect(),
            now,
        )?;
        self.store.write_task_artifact(
            child_permit,
            &artifact,
            LifecycleEventType::ContextChildManifestCreated,
            now,
        )?;
        let grant = ReadGrant {
            manifest_artifact_id: artifact.artifact_id.clone(),
            run_id: child_permit.run_id.clone(),
            task_id: child_permit.task_id.clone(),
            attempt_id: child_permit.attempt_id.clone(),
            lease_id: child_permit.lease_id.clone(),
            epoch: child_permit.epoch,
            contract_hash: child_contract.contract_hash.clone(),
            readable: selections
                .iter()
                .map(|selection| selection.artifact.artifact_id.clone())
                .collect(),
            raw_source_closure,
            expires_at: now + grant_ttl,
        };
        Ok(ContextManifest {
            artifact,
            payload,
            grant,
        })
    }

    /// Project the current succeeded parent attempt without reviving its
    /// write permit. The proof is read-only Store state; the synthetic permit
    /// exists only inside this validation path.
    pub fn assemble_child_from_proof(
        &self,
        proof: &SucceededAttemptProof,
        parent_contract: &AgentContract,
        child_permit: &TaskWritePermit,
        child_contract: &AgentContract,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        let current = self
            .store
            .current_succeeded_attempt(&proof.run_id, &proof.task_id)?;
        if &current != proof {
            return Err(ContextError::InvalidManifestClosure);
        }
        let manifest_ref = proof
            .context_manifest
            .clone()
            .ok_or(ContextError::InvalidManifestClosure)?;
        let artifact = self.store.artifact(&manifest_ref.artifact_id)?;
        if artifact.kind != ArtifactKind::ContextManifest {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload: ContextManifestPayload = self.read_payload(&artifact)?;
        // Parent manifest proves provenance; committed outputs are the child data surface
        // only after Rust applies the child's policy-owned projection.
        let mut projection = derive_child_projection(proof, manifest_ref, child_contract);
        for selection in &payload.selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            let kind_allowed = child_contract
                .context
                .permitted_kinds
                .contains(&artifact.kind);
            let source_allowed = child_contract.context.permitted_source_families.is_empty()
                || child_contract
                    .context
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family);
            if kind_allowed && source_allowed {
                projection.allowed.push(selection.artifact.clone());
            }
        }
        projection.allowed.sort();
        projection.allowed.dedup();
        let parent_permit = TaskWritePermit {
            run_id: proof.run_id.clone(),
            task_id: proof.task_id.clone(),
            attempt_id: proof.attempt_id.clone(),
            lease_id: proof.lease_id.clone(),
            epoch: proof.epoch,
            contract_hash: proof.contract_hash.clone(),
        };
        let parent =
            self.restore_manifest_for_proof(proof, parent_contract, artifact, payload, now)?;
        self.assemble_child(
            &parent_permit,
            parent_contract,
            &parent,
            &projection,
            child_permit,
            child_contract,
            now,
            grant_ttl,
        )
    }
}

impl ContextBroker {
    fn select_analyst_bundle(
        &self,
        artifacts: &[Artifact],
        policy: &ContextPolicy,
    ) -> ContextResult<Option<Vec<Artifact>>> {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum EvidenceDomain {
            Bars,
            Macro,
            News,
        }

        #[derive(Clone)]
        struct Bundle {
            asset: Asset,
            artifacts: [Artifact; 3],
            minimum_confidence_ppm: u32,
            total_confidence_ppm: u64,
            total_bytes: u64,
            estimated_tokens: u32,
        }

        let mut by_asset =
            std::collections::BTreeMap::<Asset, std::collections::BTreeMap<_, Vec<Artifact>>>::new();

        for artifact in artifacts {
            if artifact.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let Some(bytes) = self.store.read_blob(&artifact.blob).ok() else {
                continue;
            };
            let Ok(payload) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let Some(resource) = payload.get("resource").and_then(Value::as_str) else {
                continue;
            };
            let mut parts = resource.split(':');
            let Some(kind) = parts.next() else {
                continue;
            };

            match kind {
                "bars" | "news" => {
                    let Some(asset) = parts.next().and_then(|symbol| Asset::try_from(symbol).ok())
                    else {
                        continue;
                    };
                    let domain = if kind == "bars" {
                        EvidenceDomain::Bars
                    } else {
                        EvidenceDomain::News
                    };
                    by_asset
                        .entry(asset)
                        .or_default()
                        .entry(domain)
                        .or_default()
                        .push(artifact.clone());
                }
                "series"
                    if matches!(
                        parts.next(),
                        Some("DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                    ) =>
                {
                    for asset in Asset::EXECUTABLE {
                        by_asset
                            .entry(asset)
                            .or_default()
                            .entry(EvidenceDomain::Macro)
                            .or_default()
                            .push(artifact.clone());
                    }
                }
                _ => {}
            }
        }

        let mut complete_bundles = Vec::new();
        for (asset, domains) in by_asset {
            let Some(bars_candidates) = domains.get(&EvidenceDomain::Bars) else {
                continue;
            };
            let Some(macro_candidates) = domains.get(&EvidenceDomain::Macro) else {
                continue;
            };
            let Some(news_candidates) = domains.get(&EvidenceDomain::News) else {
                continue;
            };

            for bars in bars_candidates {
                for macro_series in macro_candidates {
                    for news in news_candidates {
                        let artifacts = [bars.clone(), macro_series.clone(), news.clone()];
                        let minimum_confidence_ppm = artifacts
                            .iter()
                            .map(|artifact| artifact.provenance.confidence_ppm)
                            .min()
                            .unwrap_or_default();
                        let total_confidence_ppm = artifacts
                            .iter()
                            .map(|artifact| u64::from(artifact.provenance.confidence_ppm))
                            .sum();
                        let total_bytes = artifacts
                            .iter()
                            .map(|artifact| artifact.blob.bytes)
                            .fold(0_u64, u64::saturating_add);
                        let estimated_tokens = artifacts
                            .iter()
                            .map(|artifact| estimate_tokens_from_bytes(artifact.blob.bytes))
                            .fold(0_u32, u32::saturating_add);
                        complete_bundles.push(Bundle {
                            asset,
                            artifacts,
                            minimum_confidence_ppm,
                            total_confidence_ppm,
                            total_bytes,
                            estimated_tokens,
                        });
                    }
                }
            }
        }

        if complete_bundles.is_empty() {
            return Ok(None);
        }

        complete_bundles.retain(|bundle| {
            bundle.artifacts.len() <= usize::from(policy.max_artifacts)
                && bundle.total_bytes <= policy.max_bytes
                && bundle.estimated_tokens <= policy.max_tokens
        });

        let Some(bundle) = complete_bundles.into_iter().max_by(|left, right| {
            left.minimum_confidence_ppm
                .cmp(&right.minimum_confidence_ppm)
                .then_with(|| left.total_confidence_ppm.cmp(&right.total_confidence_ppm))
                .then_with(|| right.total_bytes.cmp(&left.total_bytes))
                .then_with(|| right.estimated_tokens.cmp(&left.estimated_tokens))
                .then_with(|| right.asset.cmp(&left.asset))
                .then_with(|| {
                    right
                        .artifacts
                        .iter()
                        .map(|artifact| &artifact.artifact_id)
                        .cmp(left.artifacts.iter().map(|artifact| &artifact.artifact_id))
                })
        }) else {
            return Err(ContextError::BudgetExceeded);
        };

        Ok(Some(bundle.artifacts.into_iter().collect()))
    }
}
