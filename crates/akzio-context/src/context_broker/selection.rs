fn financial_content_indicators(value: &Value) -> Option<Vec<String>> {
    let assessment = financial_content_assessment(value)?;
    if !assessment.blocks_trading(&FinancialContentPolicy::default()) {
        return None;
    }
    let mut indicators = assessment
        .indicators
        .iter()
        .map(|indicator| format!("financial_{indicator:?}").to_ascii_lowercase())
        .collect::<Vec<_>>();
    if indicators.is_empty() {
        indicators.push(
            format!(
                "financial_information_{:?}",
                assessment.information_classification
            )
            .to_ascii_lowercase(),
        );
    }
    indicators.sort();
    indicators.dedup();
    Some(indicators)
}

fn financial_content_assessment(value: &Value) -> Option<FinancialContentAssessment> {
    let content = value.get("financial_content")?;
    serde_json::from_value(content.clone()).ok()
}

impl ContextBroker {
    /// Measure what the model receives separately from the original CAS
    /// document. Full source bytes remain bounded by max_source_bytes and
    /// range-read authorization; compact projection bytes consume max_bytes.
    fn projection_budget(&self, artifact: &Artifact) -> ContextResult<(u64, u32)> {
        let value = self.document_value(artifact)?;
        let projection = compact_governed_projection(artifact.kind, value);
        let bytes = u64::try_from(serde_json::to_vec(&projection)?.len())
            .map_err(|_| ContextError::BudgetExceeded)?;
        Ok((bytes, estimate_tokens_from_bytes(bytes)))
    }

    fn source_budget(policy: &ContextPolicy) -> u64 {
        policy.max_source_bytes.unwrap_or(policy.max_bytes)
    }

    fn partition_untrusted_context(
        &self,
        artifacts: Vec<Artifact>,
    ) -> ContextResult<(Vec<Artifact>, Vec<ContextQuarantine>)> {
        let mut allowed = Vec::with_capacity(artifacts.len());
        let mut quarantined = Vec::new();
        for artifact in artifacts {
            if context_trust(artifact.kind) == ContextTrust::UntrustedEvidence {
                let bytes = self.store.read_blob(&artifact.blob)?;
                let (reason, indicators) = match serde_json::from_slice::<Value>(&bytes) {
                    Ok(value) => {
                        let instruction = instruction_indicators(&value);
                        if !instruction.is_empty() {
                            (ContextQuarantineReason::InstructionLikeContent, instruction)
                        } else if let Some(indicators) = financial_content_indicators(&value) {
                            (ContextQuarantineReason::FinancialContentRisk, indicators)
                        } else {
                            (ContextQuarantineReason::InstructionLikeContent, Vec::new())
                        }
                    }
                    Err(_) => (
                        ContextQuarantineReason::InstructionLikeContent,
                        instruction_indicators(&Value::String(
                            String::from_utf8_lossy(&bytes).into_owned(),
                        )),
                    ),
                };
                if !indicators.is_empty() {
                    quarantined.push(ContextQuarantine {
                        artifact: ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        },
                        reason,
                        indicators,
                    });
                    continue;
                }
            }
            allowed.push(artifact);
        }
        Ok((allowed, quarantined))
    }

    fn learning_query_scope(
        &self,
        permit: &TaskWritePermit,
        policy: &ContextPolicy,
        query: &ContextQueryScope,
        references: &[ArtifactRef],
    ) -> ContextResult<ContextQueryScope> {
        let mut scope = query.clone();
        // Regime labels are derived only from authorized typed snapshots. No
        // evidence text, arbitrary tag, or caller-supplied label is authoritative.
        scope.regimes.clear();
        for reference in references {
            if reference.kind != ArtifactKind::RegimeSnapshot
                || !policy.permitted_kinds.contains(&ArtifactKind::RegimeSnapshot)
            {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != ArtifactKind::RegimeSnapshot
                || !governed_internal_source(&artifact)
                || (!policy.permitted_source_families.is_empty()
                    && !policy.permitted_source_families.contains(&artifact.provenance.source_family))
            {
                continue;
            }
            self.assert_context_permitted(policy, &artifact)?;
            self.assert_context_run(permit, &artifact)?;
            let snapshot: RegimeSnapshot = self.read_payload(&artifact)?;
            snapshot.validate()?;
            if snapshot.classification_kind == RegimeClassificationKind::DecisionTime {
                scope.regimes.extend(snapshot.canonical_regime_labels());
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
        let (mut allowed, quarantined) = self.partition_untrusted_context(allowed)?;
        let mut directly_referenced = BTreeSet::new();
        for artifact in &allowed {
            match artifact.kind {
                ArtifactKind::Claim => {
                    let claim: ResearchClaim = self.read_payload(artifact)?;
                    claim.validate()?;
                    directly_referenced.extend(claim.source_refs());
                }
                ArtifactKind::Critique => {
                    let critique: ResearchCritique = self.read_payload(artifact)?;
                    critique.validate()?;
                    directly_referenced.extend(critique.source_refs());
                }
                _ => {}
            }
        }
        allowed.sort_by(|left, right| {
            purpose_rank(child_contract.purpose.as_str(), left)
                .cmp(&purpose_rank(child_contract.purpose.as_str(), right))
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
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });
        let policy = &child_contract.context;
        let mut selections = Vec::with_capacity(allowed.len());
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for artifact in allowed {
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            };
            self.assert_context_permitted(policy, &artifact)?;
            self.assert_context_run(child_permit, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                continue;
            }
            let (projected, tokens) = self.projection_budget(&artifact)?;
            let next_source_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            let next_projected_bytes = projected_bytes.saturating_add(projected);
            let next_tokens = estimated_tokens.saturating_add(tokens);
            if selections.len() >= usize::from(policy.max_artifacts)
                || next_source_bytes > Self::source_budget(policy)
                || next_projected_bytes > policy.max_bytes
                || next_tokens > policy.max_tokens
            {
                continue;
            }
            total_bytes = next_source_bytes;
            projected_bytes = next_projected_bytes;
            estimated_tokens = next_tokens;
            selections.push(ContextSelection {
                artifact: reference,
                reason: if artifact.producer == "evidence.option_projection" {
                    "option_chain_projection".to_owned()
                } else {
                    projection.reason.clone()
                },
                estimated_tokens: tokens,
                projected_bytes: Some(projected),
                trust: context_trust(artifact.kind),
            });
        }
        if selections.len() < usize::from(policy.min_artifacts)
            || (child_contract.purpose.as_str() == RESEARCH_CRITIC_RECIPE_ID
                && (!selections
                    .iter()
                    .any(|selection| selection.artifact.kind == ArtifactKind::Claim)
                    || !selections.iter().any(|selection| {
                        selection.artifact.kind == ArtifactKind::NormalizedEvidence
                    })))
        {
            return Err(ContextError::BudgetExceeded);
        }

        let raw_source_closure = self.raw_closure(policy, &selections)?;
        if !raw_source_closure.is_subset(&parent_raw_closure) {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload = ContextManifestPayload {
            schema_version: DOMAIN_SCHEMA_VERSION,
            contract_hash: child_contract.contract_hash.clone(),
            input_hash: manifest_input_hash(&selections)?,
            selections: selections.clone(),
            quarantined: quarantined.clone(),
            total_bytes,
            projected_bytes: Some(projected_bytes),
            estimated_tokens,
        };
        payload.validate(policy)?;
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            self.store.stage_json(&payload)?,
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
                .chain(
                    quarantined
                        .iter()
                        .map(|quarantine| quarantine.artifact.clone()),
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
        // Reserve a balanced core before ranking optional documents. A missing
        // domain remains a scoped coverage gap; it does not erase other assets.
        let mut by_key = std::collections::BTreeMap::<String, Vec<Artifact>>::new();
        for artifact in artifacts {
            if artifact.kind == ArtifactKind::SemanticDetail
                && matches!(artifact.producer.as_str(), "evidence.collection_status" | "canary.evidence_snapshot")
            {
                by_key
                    .entry("0:coverage".to_owned())
                    .or_default()
                    .push(artifact.clone());
            }
            let option_projection = artifact.kind == ArtifactKind::SemanticDetail
                && artifact.producer == "evidence.option_projection";
            if artifact.kind != ArtifactKind::NormalizedEvidence && !option_projection {
                continue;
            }
            let payload: Value = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let Some(resource) = payload.get("resource").and_then(Value::as_str) else {
                continue;
            };
            let mut parts = resource.split(':');
            let domain = parts.next().unwrap_or_default();
            let scope = parts.next().unwrap_or_default();
            let key = match domain {
                "bars" | "news" if Asset::try_from(scope).is_ok() => {
                    format!("1:{scope}:{domain}")
                }
                "series" if matches!(scope, "DFF" | "DFII10" | "VIXCLS") => format!("2:{scope}"),
                "research" if scope == "earnings_event_calendar" => format!("3:{}", parts.next().unwrap_or_default()),
                "option_chain" if Asset::try_from(scope).is_ok() => format!("4:{scope}"),
                _ => continue,
            };
            by_key.entry(key).or_default().push(artifact.clone());
        }
        let mut selected = Vec::new();
        let mut source_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut tokens = 0_u32;
        for candidates in by_key.values_mut() {
            candidates.sort_by_key(|a| {
                (
                    std::cmp::Reverse(a.provenance.confidence_ppm),
                    a.blob.bytes,
                    a.artifact_id.clone(),
                )
            });
            if let Some(artifact) = candidates.iter().find(|a| {
                let (projected, estimated) = self.projection_budget(a).unwrap_or((u64::MAX, u32::MAX));
                source_bytes.saturating_add(a.blob.bytes) <= Self::source_budget(policy)
                    && projected_bytes.saturating_add(projected) <= policy.max_bytes
                    && tokens.saturating_add(estimated) <= policy.max_tokens
                    && selected.len() < usize::from(policy.max_artifacts)
            }) {
                let (projected, estimated) = self.projection_budget(artifact)?;
                source_bytes += artifact.blob.bytes;
                projected_bytes += projected;
                tokens += estimated;
                selected.push(artifact.clone());
            }
        }
        Ok((!selected.is_empty()).then_some(selected))
    }
}

#[cfg(test)]
mod event_selection_tests {
    use super::*;
    #[test]
    fn tight_budget_reserves_events_before_options_after_price_and_macro() {
        let root=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/event-selection-tests").join(akzio_domain::RunId::new().0);
        let store=Store::open(root).unwrap();
        let now=Utc::now();
        let resources=["option_chain:QQQ:2026-09-22:2026-10-22",
            "research:earnings_event_calendar:QQQ:2026-09-22","series:DFF",
            "bars:QQQ:2026-01-01:2026-09-22:1Day"];
        let artifacts=resources.iter().map(|resource| {
            let option=resource.starts_with("option_chain:");
            Artifact::new(if option {ArtifactKind::SemanticDetail} else {ArtifactKind::NormalizedEvidence},
                store.stage_json(&serde_json::json!({"resource":resource,"value":{}})).unwrap(),
                if option {"evidence.option_projection"} else {"evidence.normalize"},
                akzio_domain::ArtifactLifecycle::RunScoped,
                akzio_domain::ArtifactProvenance {source_family:"test".into(), observed_at:Some(now),retrieved_at:now,
                    source_uri:None,confidence_ppm:1_000_000,producer_contract_hash:None},None,vec![],now).unwrap()
        }).collect::<Vec<_>>();
        let broker=ContextBroker::new(store);
        let policy=ContextPolicy {permitted_kinds:Default::default(),permitted_source_families:Default::default(),
            min_artifacts:0,max_artifacts:3,max_bytes:131072,max_source_bytes:None,max_tokens:32000,allow_raw_reread:false};
        let selected=broker.select_analyst_bundle(&artifacts,&policy).unwrap().unwrap();
        assert_eq!(selected.len(),3);
        assert!(!selected.iter().any(|a|a.artifact_id==artifacts[0].artifact_id));
        assert!(selected.iter().any(|a|a.artifact_id==artifacts[1].artifact_id));
    }
}
