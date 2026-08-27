impl V2ExecutionRuntime {
    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ExecutionGateResult<T> {
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn validate_decision_provenance(
        &self,
        artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("decision context origin run"));
        }
        for reference in decision
            .claims
            .iter()
            .chain(decision.critiques.iter())
            .chain(decision.evidence.iter())
            .chain(decision.policy_influences.iter())
            .chain(decision.applied_learning_refs.iter())
            .chain(decision.rejected_learning_refs.iter())
            .chain(
                decision
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            let source = self.store.artifact(&reference.artifact_id)?;
            if source.kind != reference.kind
                || !artifact
                    .source_refs
                    .iter()
                    .any(|declared| declared == reference)
            {
                return Err(ExecutionGateError::Integrity(
                    "decision context source refs",
                ));
            }
        }
        self.validate_policy_influences(artifact, decision)
    }

    fn validate_policy_influences(
        &self,
        decision_artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        if decision.policy_influences.is_empty() {
            return Ok(());
        }
        let manifest_refs = decision_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .collect::<Vec<_>>();
        if manifest_refs.len() != 1 {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let manifest = self.store.artifact(&manifest_refs[0].artifact_id)?;
        if manifest.kind != ArtifactKind::ContextManifest
            || manifest
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let payload: ContextManifestPayload = self.read_payload(&manifest)?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if selected != declared
            || decision
                .policy_influences
                .iter()
                .any(|reference| !selected.contains(reference))
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        for reference in &decision.policy_influences {
            let influence = self.store.artifact(&reference.artifact_id)?;
            if influence.kind != reference.kind || !self.is_canonical_paper(&influence)? {
                return Err(ExecutionGateError::Integrity("policy influence authority"));
            }
            let subject: PolicySubject = match reference.kind {
                ArtifactKind::Experience => {
                    let experience: Experience = self.read_payload(&influence)?;
                    experience.validate()?;
                    experience.subject
                }
                ArtifactKind::CandidatePolicy => {
                    let policy: CandidatePolicy = self.read_payload(&influence)?;
                    policy.validate()?;
                    let evaluation = self.store.artifact(&policy.source_evaluation.artifact_id)?;
                    if evaluation.kind != ArtifactKind::Evaluation
                        || !self.is_canonical_paper(&evaluation)?
                    {
                        return Err(ExecutionGateError::Integrity("candidate policy evaluation"));
                    }
                    policy.subject
                }
                _ => {
                    return Err(ExecutionGateError::Integrity(
                        "policy influence artifact kind",
                    ));
                }
            };
            if self
                .store
                .recorded_policy_influence_subject(&reference.artifact_id)?
                .is_some_and(|recorded| recorded != subject)
            {
                return Err(ExecutionGateError::Integrity("policy influence subject"));
            }
            let head = self
                .store
                .policy_head(&subject)?
                .ok_or(ExecutionGateError::Integrity("policy head"))?;
            if !head.state.permits_influence_kind(reference.kind) {
                return Err(ExecutionGateError::Integrity("policy head state"));
            }
        }
        Ok(())
    }

    fn is_canonical_paper(&self, artifact: &Artifact) -> ExecutionGateResult<bool> {
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Ok(false);
        }
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            return Ok(false);
        };
        Ok(self.store.run_purpose(run_id)? == RunPurpose::Paper)
    }

    fn frozen(&self) -> ExecutionGateResult<bool> {
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(false);
        };
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Err(ExecutionGateError::Integrity("freeze state lifecycle"));
        }
        let state: FreezeState = self.read_payload(&artifact)?;
        state.validate()?;
        Ok(state.frozen)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        input: &ExecutionGateInput,
    ) -> ExecutionGateResult<Artifact> {
        Ok(Artifact::new(
            kind,
            self.store.put_json(payload)?,
            producer,
            ArtifactLifecycle::RunScoped,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}
