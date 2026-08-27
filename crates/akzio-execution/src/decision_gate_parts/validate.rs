impl V2DecisionRuntime {
    fn validate_manifest(
        &self,
        manifest: &Artifact,
        proposal: &Artifact,
        contract_hash: &akzio_domain::ContentHash,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<BTreeSet<ArtifactRef>> {
        let proposal_origin = proposal
            .origin
            .as_ref()
            .ok_or(DecisionGateError::InvalidProposalProvenance)?;
        let Some(origin) = manifest.origin.as_ref() else {
            return Err(DecisionGateError::InvalidManifestClosure);
        };
        if manifest.lifecycle != ArtifactLifecycle::RunScoped
            || manifest.producer != "context.research.synthesizer"
            || manifest.provenance.source_family != "akzio.context"
            || manifest.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id != proposal_origin.task_id
            || origin.attempt_id != proposal_origin.attempt_id
            || origin.contract_hash.as_ref() != Some(contract_hash)
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let payload: ContextManifestPayload =
            serde_json::from_slice(&self.store.read_blob(&manifest.blob)?)?;
        if payload.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || payload.contract_hash != *contract_hash
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        self.validate_manifest_source_closure(manifest, &payload, permit, &mut BTreeSet::new())?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        Ok(selected)
    }

    fn validate_manifest_source_closure(
        &self,
        manifest: &Artifact,
        payload: &ContextManifestPayload,
        permit: &TaskWritePermit,
        visiting: &mut BTreeSet<ArtifactId>,
    ) -> DecisionGateResult<()> {
        if !visiting.insert(manifest.artifact_id.clone()) {
            return Err(DecisionGateError::InvalidManifestClosure);
        }
        if payload.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let ancestors = manifest
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .cloned()
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut expected = selected.clone();
        expected.extend(ancestors.iter().cloned());
        if selected.len() != payload.selections.len()
            || declared.len() != manifest.source_refs.len()
            || declared != expected
            || payload.input_hash != manifest_input_hash(&payload.selections)?
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for selection in &payload.selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            if artifact.kind != selection.artifact.kind
                || matches!(
                    artifact.kind,
                    ArtifactKind::RawEvidence
                        | ArtifactKind::AgentTurn
                        | ArtifactKind::ToolCall
                        | ArtifactKind::ToolResult
                )
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let tokens = estimate_tokens(artifact.blob.bytes);
            if tokens != selection.estimated_tokens {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
        }
        if total_bytes != payload.total_bytes || estimated_tokens != payload.estimated_tokens {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        for parent_ref in ancestors {
            if selected.contains(&parent_ref) {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent = self.load_expected(&parent_ref, ArtifactKind::ContextManifest)?;
            let Some(origin) = parent.origin.as_ref() else {
                return Err(DecisionGateError::InvalidManifestClosure);
            };
            if parent.lifecycle != ArtifactLifecycle::RunScoped
                || !parent.producer.starts_with("context.")
                || parent.provenance.source_family != "akzio.context"
                || origin.run_id.as_ref() != Some(&permit.run_id)
                || origin.task_id.is_none()
                || origin.attempt_id.is_none()
                || origin.contract_hash.is_none()
                || parent.provenance.producer_contract_hash != origin.contract_hash
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent_payload: ContextManifestPayload =
                serde_json::from_slice(&self.store.read_blob(&parent.blob)?)?;
            if parent_payload.contract_hash != origin.contract_hash.clone().unwrap() {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            self.validate_manifest_source_closure(&parent, &parent_payload, permit, visiting)?;
        }
        visiting.remove(&manifest.artifact_id);
        Ok(())
    }

    fn validate_draft_closure(
        &self,
        draft: &DecisionDraft,
        selected: &BTreeSet<ArtifactRef>,
    ) -> DecisionGateResult<()> {
        for reference in draft
            .claims
            .iter()
            .chain(draft.critiques.iter())
            .chain(draft.evidence.iter())
            .chain(draft.applied_learning_refs.iter())
            .chain(draft.rejected_learning_refs.iter())
            .chain(
                draft
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            if !selected.contains(reference) {
                return Err(DecisionGateError::ReferenceOutsideManifest(
                    reference.artifact_id.clone(),
                ));
            }
        }
        for reference in selected.iter().filter(|reference| {
            matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience
            )
        }) {
            if !draft.applied_learning_refs.contains(reference)
                && !draft.rejected_learning_refs.contains(reference)
            {
                return Err(DecisionGateError::MissingLearningAttribution(
                    reference.artifact_id.clone(),
                ));
            }
        }
        Ok(())
    }
}
