impl ContextBroker {
    pub fn materialize_for_agent(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextMaterialization> {
        contract.validate()?;
        if !manifest.grant.matches_permit(permit)
            || manifest.grant.contract_hash != contract.contract_hash
            || manifest.artifact.artifact_id != manifest.grant.manifest_artifact_id
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.validate_persisted_grant(permit, contract, &manifest.grant, now)?;

        let read_grant_identity =
            stable_read_grant_identity(&manifest.grant, &manifest.payload.input_hash)?;
        let mut ledger = Vec::with_capacity(manifest.payload.selections.len());
        let mut must_read = Vec::new();
        for selection in &manifest.payload.selections {
            let artifact = self.read(
                permit,
                contract,
                &manifest.grant,
                &selection.artifact.artifact_id,
                now,
            )?;
            if artifact.kind != selection.artifact.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            let must_read_class = must_read_class(selection, &artifact);
            let metadata = ContextDocumentMetadata {
                document_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
                source: artifact.provenance.source_family.clone(),
                observed_at: artifact.provenance.observed_at,
                published_at: None,
                estimated_tokens: selection.estimated_tokens,
                relevance: context_relevance(artifact.kind),
                reason: selection.reason.clone(),
                must_read: must_read_class.is_some(),
                read_grant_identity: read_grant_identity.clone(),
            };
            if let Some(class) = must_read_class {
                must_read.push(ContextMustReadDocument {
                    class: class.to_owned(),
                    metadata: metadata.clone(),
                    value: self.document_value(&artifact)?,
                });
            }
            ledger.push(metadata);
        }

        let task_contract = serde_json::json!({
            "contract_hash": contract.contract_hash,
            "purpose": contract.purpose,
            "responsibility": contract.responsibility,
            "permitted_context_kinds": contract.context.permitted_kinds,
            "permitted_source_families": contract.context.permitted_source_families,
            "context_limits": {
                "max_artifacts": contract.context.max_artifacts,
                "max_bytes": contract.context.max_bytes,
                "max_tokens": contract.context.max_tokens,
            },
            "read_tools": contract.tool_specs.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>(),
            "output_artifact_kind": contract.output.artifact_kind,
            "budget": contract.budget,
        });
        let materialization_identity = content_hash_json(&serde_json::json!({
            "context_manifest_input_hash": manifest.payload.input_hash,
            "read_grant_identity": read_grant_identity,
            "task_contract": task_contract,
            "ledger": ledger,
            "must_read": must_read,
        }))?;
        Ok(ContextMaterialization {
            manifest_artifact_id: manifest.artifact.artifact_id.clone(),
            read_grant_identity,
            materialization_identity,
            task_contract,
            ledger,
            must_read,
        })
    }
}

impl ContextMaterialization {
    pub fn model_context(&self) -> Vec<Value> {
        let mut context = Vec::with_capacity(self.must_read.len() + 2);
        context.push(serde_json::json!({
            "type": "context_metadata_ledger",
            "manifest_artifact_id": self.manifest_artifact_id,
            "read_grant_identity": self.read_grant_identity,
            "materialization_identity": self.materialization_identity,
            "documents": self.ledger,
        }));
        context.push(serde_json::json!({
            "type": "must_read",
            "class": "task_contract",
            "value": self.task_contract,
        }));
        context.extend(self.must_read.iter().map(|document| {
            serde_json::json!({
                "type": "must_read",
                "class": document.class,
                "metadata": document.metadata,
                "value": document.value,
            })
        }));
        context
    }
}

fn stable_read_grant_identity(
    grant: &ReadGrant,
    manifest_input_hash: &ContentHash,
) -> ContextResult<ContentHash> {
    Ok(content_hash_json(&serde_json::json!({
        "manifest_input_hash": manifest_input_hash,
        "run_id": grant.run_id,
        "task_id": grant.task_id,
        "contract_hash": grant.contract_hash,
        "readable": grant.readable,
        "raw_source_closure": grant.raw_source_closure,
    }))?)
}

fn must_read_class(selection: &ContextSelection, artifact: &Artifact) -> Option<&'static str> {
    let reason = selection.reason.trim().to_ascii_lowercase();
    if reason == "must_read"
        || reason == "mandatory_observation"
        || reason.starts_with("must_read:")
        || reason.starts_with("mandatory_observation:")
    {
        return Some("mandatory_observation");
    }
    match artifact.kind {
        ArtifactKind::DecisionContext => Some("portfolio"),
        ArtifactKind::ExecutionContext
        | ArtifactKind::ExecutionVerdict
        | ArtifactKind::ExecutionPlan
        | ArtifactKind::ExecutionCommitment
        | ArtifactKind::ExecutionReprice
        | ArtifactKind::PaperLaunchApproval
        | ArtifactKind::FreezeState => Some("risk_execution_constraint"),
        _ => None,
    }
}

const fn context_relevance(kind: ArtifactKind) -> u32 {
    match kind {
        ArtifactKind::DecisionContext
        | ArtifactKind::ExecutionContext
        | ArtifactKind::ExecutionVerdict
        | ArtifactKind::ExecutionPlan
        | ArtifactKind::ExecutionCommitment
        | ArtifactKind::ExecutionReprice
        | ArtifactKind::PaperLaunchApproval
        | ArtifactKind::FreezeState => 1_000_000,
        ArtifactKind::NormalizedEvidence => 950_000,
        ArtifactKind::SemanticDetail => 900_000,
        ArtifactKind::Claim | ArtifactKind::Critique | ArtifactKind::Resolution => 850_000,
        ArtifactKind::Lesson
        | ArtifactKind::Retrospective
        | ArtifactKind::Experience
        | ArtifactKind::CandidatePolicy
        | ArtifactKind::Evaluation => 700_000,
        _ => 500_000,
    }
}
