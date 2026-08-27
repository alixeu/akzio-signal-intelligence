impl V2DecisionRuntime {
    fn validate_policy_influence(&self, reference: &ArtifactRef) -> DecisionGateResult<()> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        let canonical_paper = artifact.lifecycle == ArtifactLifecycle::Canonical
            && artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                .is_some_and(|run_id| {
                    self.store
                        .run_purpose(run_id)
                        .is_ok_and(|purpose| purpose == RunPurpose::Paper)
                });
        if artifact.kind != reference.kind || !canonical_paper {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }

        let subject = match artifact.kind {
            ArtifactKind::Experience => {
                let payload: Experience =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            ArtifactKind::CandidatePolicy => {
                let payload: CandidatePolicy =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            _ => {
                return Err(DecisionGateError::InvalidPolicyInfluence(
                    reference.artifact_id.clone(),
                ));
            }
        };
        self.require_active_policy_influence(reference, &subject)
    }

    fn require_active_policy_influence(
        &self,
        reference: &ArtifactRef,
        subject: &PolicySubject,
    ) -> DecisionGateResult<()> {
        if self
            .store
            .recorded_policy_influence_subject(&reference.artifact_id)?
            .is_some_and(|recorded| recorded != *subject)
            || !self
                .store
                .policy_head(subject)?
                .is_some_and(|head| head.state.permits_influence_kind(reference.kind))
        {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> DecisionGateResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(DecisionGateError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        lifecycle: ArtifactLifecycle,
        source_refs: Vec<ArtifactRef>,
        input: &DecisionGateInput,
    ) -> DecisionGateResult<Artifact> {
        Ok(Artifact::new(
            kind,
            self.store.put_json(payload)?,
            producer,
            lifecycle,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}
