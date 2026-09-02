impl ContextBroker {
    fn validate_parent_output_provenance(
        &self,
        output: &Artifact,
        parent_manifest: &ArtifactRef,
        parent_readable: &BTreeSet<ArtifactRef>,
        parent_raw_closure: &BTreeSet<ArtifactId>,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
    ) -> ContextResult<()> {
        if output.kind == ArtifactKind::RawEvidence || is_trace_kind(output.kind) {
            return Err(ContextError::InvalidManifestClosure);
        }
        let proof = ParentContextProof {
            manifest: parent_manifest,
            readable: parent_readable,
            raw_closure: parent_raw_closure,
            permit: parent_permit,
            contract: parent_contract,
        };
        self.validate_parent_attempt_artifact(output, proof.permit, proof.contract)?;
        self.validate_parent_output_sources(output, &proof, &mut BTreeSet::new())
    }

    fn validate_parent_output_sources(
        &self,
        artifact: &Artifact,
        proof: &ParentContextProof<'_>,
        visiting: &mut BTreeSet<ArtifactId>,
    ) -> ContextResult<()> {
        if !visiting.insert(artifact.artifact_id.clone()) {
            return Err(ContextError::InvalidManifestClosure);
        }
        for source in &artifact.source_refs {
            let source_artifact = self.store.artifact(&source.artifact_id)?;
            if source_artifact.kind != source.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            if source == proof.manifest {
                if source_artifact.kind != ArtifactKind::ContextManifest {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if source.kind == ArtifactKind::RawEvidence {
                if !proof.raw_closure.contains(&source.artifact_id) {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if !is_trace_kind(source.kind) && proof.readable.contains(source) {
                continue;
            }
            if is_safe_deliberation_summary(source.kind) {
                self.validate_parent_attempt_artifact(
                    &source_artifact,
                    proof.permit,
                    proof.contract,
                )?;
                self.validate_parent_output_sources(&source_artifact, proof, visiting)?;
                continue;
            }
            if !is_trace_kind(source.kind) {
                return Err(ContextError::InvalidManifestClosure);
            }
            self.validate_parent_attempt_artifact(&source_artifact, proof.permit, proof.contract)?;
            self.validate_parent_output_sources(&source_artifact, proof, visiting)?;
        }
        visiting.remove(&artifact.artifact_id);
        Ok(())
    }

    fn validate_parent_attempt_artifact(
        &self,
        artifact: &Artifact,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
    ) -> ContextResult<()> {
        artifact.validate()?;
        if self.store.artifact(&artifact.artifact_id)? != *artifact {
            return Err(ContextError::InvalidManifestClosure);
        }
        let Some(origin) = artifact.origin.as_ref() else {
            return Err(ContextError::InvalidManifestClosure);
        };
        if origin.run_id.as_ref() != Some(&parent_permit.run_id)
            || origin.task_id.as_ref() != Some(&parent_permit.task_id)
            || origin.attempt_id.as_ref() != Some(&parent_permit.attempt_id)
            || origin.contract_hash.as_ref() != Some(&parent_contract.contract_hash)
            || artifact.provenance.producer_contract_hash.as_ref()
                != Some(&parent_contract.contract_hash)
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        Ok(())
    }

    fn restore_manifest_for_proof(
        &self,
        proof: &SucceededAttemptProof,
        contract: &AgentContract,
        artifact: Artifact,
        payload: ContextManifestPayload,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextManifest> {
        contract.validate()?;
        payload.validate(&contract.context)?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let mut expected_source_refs = selected;
        expected_source_refs.extend(
            artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .cloned(),
        );
        if artifact.kind != ArtifactKind::ContextManifest
            || expected_source_refs != artifact.source_refs.iter().cloned().collect()
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        let readable = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        let raw_source_closure = self.raw_closure(&contract.context, &payload.selections)?;
        Ok(ContextManifest {
            artifact,
            payload,
            grant: ReadGrant {
                manifest_artifact_id: proof
                    .context_manifest
                    .as_ref()
                    .ok_or(ContextError::InvalidManifestClosure)?
                    .artifact_id
                    .clone(),
                run_id: proof.run_id.clone(),
                task_id: proof.task_id.clone(),
                attempt_id: proof.attempt_id.clone(),
                lease_id: proof.lease_id.clone(),
                epoch: proof.epoch,
                contract_hash: contract.contract_hash.clone(),
                readable,
                raw_source_closure,
                expires_at: now,
            },
        })
    }

    pub fn read(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        if !grant.permits(artifact_id, false, now) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        let artifact = self.store.artifact(artifact_id)?;
        if is_trace_kind(artifact.kind) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact.artifact_id,
            });
        }
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(ContextError::RawEvidenceRequiresExplicitRead);
        }
        Ok(artifact)
    }

    pub fn read_raw(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        if !grant.permits(artifact_id, true, now) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        let artifact = self.store.artifact(artifact_id)?;
        if is_trace_kind(artifact.kind) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact.artifact_id,
            });
        }
        if artifact.kind != ArtifactKind::RawEvidence {
            return Err(ContextError::ExpectedRawEvidence);
        }
        Ok(artifact)
    }

    pub fn read_document(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<(Artifact, Value)> {
        let artifact = self.read(permit, contract, grant, artifact_id, now)?;
        let value = self.document_value(&artifact)?;
        Ok((artifact, value))
    }

pub fn read_raw_document(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<(Artifact, Value)> {
        let artifact = self.read_raw(permit, contract, grant, artifact_id, now)?;
        let value = self.document_value(&artifact)?;
    Ok((artifact, value))
}

pub fn read_authority_document(
    &self,
    contract: &AgentContract,
    blob_ref: &BlobRef,
) -> ContextResult<Vec<u8>> {
    contract.validate()?;
    let declared = contract.prompt.governance == *blob_ref
        || contract.prompt.role == *blob_ref
        || contract.output.schema == *blob_ref
        || contract
            .tool_specs
            .iter()
            .any(|spec| spec.input_schema == *blob_ref);
    if !declared {
        return Err(ContextError::AuthorityBlobNotDeclared);
    }
    Ok(self.store.read_blob(blob_ref)?)
}

fn document_value(&self, artifact: &Artifact) -> ContextResult<Value> {
        let bytes = self.store.read_blob(&artifact.blob)?;
        Ok(serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())))
    }

    fn validate_persisted_grant(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        now: DateTime<Utc>,
    ) -> ContextResult<()> {
        let artifact = self.store.artifact(&grant.manifest_artifact_id)?;
        let payload: ContextManifestPayload = self.read_payload(&artifact)?;
        let manifest = ContextManifest {
            artifact,
            payload,
            grant: grant.clone(),
        };
        self.validate_manifest_closure(permit, contract, &manifest, now, true)
            .map(|_| ())
    }
}
