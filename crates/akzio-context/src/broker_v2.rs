//! Manifest-and-grant context broker for the v2 runtime.

use std::collections::{BTreeSet, VecDeque};

use akzio_domain::{
    content_hash_json, AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle,
    ArtifactOrigin, ArtifactProvenance, ArtifactRef, CandidatePolicy, ContextManifestPayload,
    ContextPolicy, ContextProjection, ContextSelection, DomainError, Experience,
    LifecycleEventType, PolicyState, ReadGrant, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, SucceededAttemptProof, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContextError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("artifact {artifact_id} is not permitted by the contract context policy")]
    ForbiddenArtifact { artifact_id: ArtifactId },
    #[error("raw evidence cannot appear directly in a manifest")]
    RawEvidenceInManifest,
    #[error("artifact {artifact_id} is not granted by manifest {manifest_id}")]
    GrantDenied {
        manifest_id: ArtifactId,
        artifact_id: ArtifactId,
    },
    #[error("raw read requested for a non-raw artifact")]
    ExpectedRawEvidence,
    #[error("non-raw read requested for raw evidence")]
    RawEvidenceRequiresExplicitRead,
    #[error("context budget is exhausted")]
    BudgetExceeded,
    #[error("context manifest closure is invalid")]
    InvalidManifestClosure,
}

pub type ContextResult<T> = Result<T, ContextError>;

#[derive(Debug, Clone)]
pub struct ContextBroker {
    store: V2Store,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    pub artifact: Artifact,
    pub payload: ContextManifestPayload,
    pub grant: ReadGrant,
}

impl ContextBroker {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
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
            let tokens = estimate_tokens(artifact.blob.bytes);
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
        let artifacts = candidates
            .into_iter()
            .filter(|reference| seen.insert(reference.artifact_id.clone()))
            .map(|reference| self.store.artifact(&reference.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut eligible = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            self.assert_context_permitted(policy, &artifact)?;
            if self.overlay_is_eligible(&artifact)? {
                eligible.push(artifact);
            }
        }
        let mut artifacts = eligible;
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

        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        let mut selections = Vec::new();
        for artifact in artifacts {
            let tokens = estimate_tokens(artifact.blob.bytes);
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

        let mut revalidated = Vec::with_capacity(selections.len());
        total_bytes = 0;
        estimated_tokens = 0;
        for mut selection in selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            self.assert_context_permitted(policy, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                continue;
            }
            let tokens = estimate_tokens(artifact.blob.bytes);
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

    /// Attenuate a persisted parent manifest into a child attempt grant.
    /// Projection may include parent outputs, but only from the current
    /// succeeded attempt and only when their provenance closes to the parent.
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
            succeeded.outputs.clone()
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
            let tokens = estimate_tokens(artifact.blob.bytes);
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
        // Parent manifest proves provenance; committed outputs are the child data surface.
        let readable = proof
            .outputs
            .iter()
            .map(|output| ArtifactRef {
                artifact_id: output.artifact_id.clone(),
                kind: output.kind,
            })
            .collect::<BTreeSet<_>>();
        let projection = ContextProjection {
            parent_manifest: manifest_ref,
            allowed: readable.into_iter().collect(),
            reason: "parent_attempt_projection".to_owned(),
        };
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
        self.validate_parent_attempt_artifact(output, parent_permit, parent_contract)?;
        self.validate_parent_output_sources(
            output,
            parent_manifest,
            parent_readable,
            parent_raw_closure,
            parent_permit,
            parent_contract,
            &mut BTreeSet::new(),
        )
    }

    fn validate_parent_output_sources(
        &self,
        artifact: &Artifact,
        parent_manifest: &ArtifactRef,
        parent_readable: &BTreeSet<ArtifactRef>,
        parent_raw_closure: &BTreeSet<ArtifactId>,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
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
            if source == parent_manifest {
                if source_artifact.kind != ArtifactKind::ContextManifest {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if source.kind == ArtifactKind::RawEvidence {
                if !parent_raw_closure.contains(&source.artifact_id) {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if !is_trace_kind(source.kind) && parent_readable.contains(source) {
                continue;
            }
            if !is_trace_kind(source.kind) {
                return Err(ContextError::InvalidManifestClosure);
            }
            self.validate_parent_attempt_artifact(
                &source_artifact,
                parent_permit,
                parent_contract,
            )?;
            self.validate_parent_output_sources(
                &source_artifact,
                parent_manifest,
                parent_readable,
                parent_raw_closure,
                parent_permit,
                parent_contract,
                visiting,
            )?;
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
        let mut expected_source_refs = selected.clone();
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

    /// Record an explicitly source-linked Context repair. This is intentionally a
    /// normal artifact write, so repair is observable and may itself be cited.
    pub fn record_repair<T: Serialize>(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        source_refs: Vec<ArtifactRef>,
        value: &T,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        for source in &source_refs {
            if !grant.permits(
                &source.artifact_id,
                source.kind == ArtifactKind::RawEvidence,
                now,
            ) {
                return Err(ContextError::GrantDenied {
                    manifest_id: grant.manifest_artifact_id.clone(),
                    artifact_id: source.artifact_id.clone(),
                });
            }
        }
        let artifact = Artifact::new(
            ArtifactKind::ContextRepair,
            self.store.put_json(value)?,
            format!("context.repair.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context_repair".to_owned(),
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
            source_refs,
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ContextRepaired,
            now,
        )?;
        Ok(artifact)
    }

    fn assert_context_permitted(
        &self,
        policy: &ContextPolicy,
        artifact: &Artifact,
    ) -> ContextResult<()> {
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(ContextError::RawEvidenceInManifest);
        }
        if !policy.permitted_kinds.contains(&artifact.kind)
            || (!policy.permitted_source_families.is_empty()
                && !policy
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family))
        {
            return Err(ContextError::ForbiddenArtifact {
                artifact_id: artifact.artifact_id.clone(),
            });
        }
        Ok(())
    }

    fn overlay_is_eligible(&self, artifact: &Artifact) -> ContextResult<bool> {
        match artifact.kind {
            ArtifactKind::Experience => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let experience: Experience = self.read_payload(artifact)?;
                experience.validate()?;
                if self
                    .store
                    .recorded_policy_influence_subject(&artifact.artifact_id)?
                    .as_ref()
                    != Some(&experience.subject)
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&experience.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            ArtifactKind::CandidatePolicy => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let candidate: CandidatePolicy = self.read_payload(artifact)?;
                candidate.validate()?;
                if self
                    .store
                    .recorded_policy_influence_subject(&artifact.artifact_id)?
                    .as_ref()
                    != Some(&candidate.subject)
                {
                    return Ok(false);
                }
                let evaluation = self
                    .store
                    .artifact(&candidate.source_evaluation.artifact_id)?;
                if evaluation.kind != ArtifactKind::Evaluation
                    || !self.is_canonical_paper_artifact(&evaluation)?
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&candidate.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            _ => Ok(true),
        }
    }

    fn is_canonical_paper_artifact(&self, artifact: &Artifact) -> ContextResult<bool> {
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
        Ok(self.store.run_purpose(run_id)?.is_canonical_learning())
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ContextResult<T> {
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn raw_closure(
        &self,
        policy: &ContextPolicy,
        selections: &[ContextSelection],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        if !policy.allow_raw_reread {
            return Ok(BTreeSet::new());
        }
        let mut closure = BTreeSet::new();
        let mut queue = selections
            .iter()
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<VecDeque<_>>();
        let mut seen = BTreeSet::new();
        while let Some(artifact_id) = queue.pop_front() {
            if !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = self.store.artifact(&artifact_id)?;
            for source in artifact.source_refs {
                let source_artifact = self.store.artifact(&source.artifact_id)?;
                if source_artifact.kind == ArtifactKind::RawEvidence {
                    if policy.permitted_source_families.is_empty()
                        || policy
                            .permitted_source_families
                            .contains(&source_artifact.provenance.source_family)
                    {
                        closure.insert(source_artifact.artifact_id);
                    }
                } else {
                    queue.push_back(source_artifact.artifact_id);
                }
            }
        }
        Ok(closure)
    }
}

fn context_rank(artifact: &Artifact) -> u8 {
    match artifact.kind {
        ArtifactKind::NormalizedEvidence => 0,
        ArtifactKind::SemanticDetail => 1,
        ArtifactKind::Claim | ArtifactKind::Critique => 2,
        ArtifactKind::Experience | ArtifactKind::CandidatePolicy | ArtifactKind::Evaluation => 3,
        _ => 4,
    }
}

fn is_trace_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

fn overlay_state_is_eligible(kind: ArtifactKind, state: PolicyState) -> bool {
    state.permits_influence_kind(kind)
}

fn selection_reason(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::NormalizedEvidence => "normalized_evidence",
        ArtifactKind::SemanticDetail => "semantic_detail",
        ArtifactKind::Claim => "claim",
        ArtifactKind::Critique => "critique",
        ArtifactKind::Experience => "experience",
        ArtifactKind::CandidatePolicy => "candidate_policy",
        ArtifactKind::Evaluation => "evaluation",
        _ => "contract_permitted",
    }
}

fn manifest_input_hash(
    selections: &[ContextSelection],
) -> Result<akzio_domain::ContentHash, serde_json::Error> {
    content_hash_json(&serde_json::to_value(
        selections
            .iter()
            .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
            .collect::<Vec<_>>(),
    )?)
}

fn estimate_tokens(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use akzio_domain::{
        ArtifactKind, CandidatePolicyState, ContractId, ContractPurpose, FailureDisposition,
        MemoryLifecycle, OutputContract, PromptBundle, RetryPolicy, RunPurpose, TaskBudget,
        TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowGraph, WorkflowNode,
        V2_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    fn contract(store: &V2Store) -> AgentContract {
        AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "analyze",
            PromptBundle {
                version: 1,
                governance: store.put_bytes(b"governance", "text/plain").unwrap(),
                role: store.put_bytes(b"prompt", "text/plain").unwrap(),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: true,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadRawEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_raw_evidence".to_owned(),
                description: "read granted raw evidence".to_owned(),
                kind: ToolKind::ReadRawEvidence,
                input_schema: store.put_bytes(b"tool schema", "application/json").unwrap(),
                strict: true,
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store.put_bytes(b"schema", "application/json").unwrap(),
            },
            TaskBudget {
                max_input_tokens: 1024,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap()
    }

    fn permit(store: &V2Store) -> TaskWritePermit {
        permit_for_purpose(store, RunPurpose::Debug)
    }

    fn permit_for_purpose(store: &V2Store, purpose: RunPurpose) -> TaskWritePermit {
        permit_for_contract(store, purpose, None)
    }

    fn permit_for_contract(
        store: &V2Store,
        purpose: RunPurpose,
        contract_hash: Option<akzio_domain::ContentHash>,
    ) -> TaskWritePermit {
        let node = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: akzio_domain::TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash,
            objective: "analyze".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: TaskBudget {
                max_input_tokens: 1024,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            retry: RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "test".to_owned(),
            nodes: vec![node.clone()],
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance("fixture"),
            None,
            vec![],
            Utc::now(),
        )
        .unwrap();
        let run = StoredRun {
            run_id: akzio_domain::RunId::new(),
            purpose,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        store
            .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
            .unwrap()
            .unwrap()
            .permit
    }

    fn provenance(source_family: &str) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: source_family.to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    fn task_artifact(
        store: &V2Store,
        permit: &TaskWritePermit,
        kind: ArtifactKind,
        source_refs: Vec<ArtifactRef>,
        value: &str,
    ) -> Artifact {
        Artifact::new(
            kind,
            store
                .put_bytes(value.as_bytes(), "application/json")
                .unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance("market"),
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            source_refs,
            Utc::now(),
        )
        .unwrap()
    }

    fn manifest_fixture() -> (
        tempfile::TempDir,
        V2Store,
        TaskWritePermit,
        AgentContract,
        ContextManifest,
        ArtifactRef,
        DateTime<Utc>,
    ) {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let permit = permit_for_contract(
            &store,
            RunPurpose::Debug,
            Some(contract.contract_hash.clone()),
        );
        let now = Utc::now();
        let raw = task_artifact(&store, &permit, ArtifactKind::RawEvidence, vec![], "raw");
        store
            .write_task_artifact(&permit, &raw, LifecycleEventType::EvidenceRaw, now)
            .unwrap();
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id,
            kind: raw.kind,
        };
        let normalized = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![raw_ref.clone()],
            "normalized",
        );
        store
            .write_task_artifact(
                &permit,
                &normalized,
                LifecycleEventType::EvidenceNormalized,
                now,
            )
            .unwrap();
        let manifest = ContextBroker::new(store.clone())
            .assemble(
                &permit,
                &contract,
                [ArtifactRef {
                    artifact_id: normalized.artifact_id,
                    kind: normalized.kind,
                }],
                now,
                Duration::minutes(5),
            )
            .unwrap();
        (root, store, permit, contract, manifest, raw_ref, now)
    }

    fn persist_manifest_payload(
        store: &V2Store,
        permit: &TaskWritePermit,
        original: &ContextManifest,
        payload: ContextManifestPayload,
        now: DateTime<Utc>,
    ) -> ContextManifest {
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            store.put_json(&payload).unwrap(),
            original.artifact.producer.clone(),
            original.artifact.lifecycle,
            original.artifact.provenance.clone(),
            original.artifact.origin.clone(),
            original.artifact.source_refs.clone(),
            original.artifact.created_at,
        )
        .unwrap();
        store
            .write_task_artifact(
                permit,
                &artifact,
                LifecycleEventType::ContextManifestCreated,
                now,
            )
            .unwrap();
        let mut grant = original.grant.clone();
        grant.manifest_artifact_id = artifact.artifact_id.clone();
        ContextManifest {
            artifact,
            payload,
            grant,
        }
    }

    #[test]
    fn restore_manifest_for_proof_accepts_parent_manifest_source_ref() {
        let (_root, store, permit, contract, parent, _raw, now) = manifest_fixture();
        let parent_ref = ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        };
        let nested = Artifact::new(
            ArtifactKind::ContextManifest,
            store.put_json(&parent.payload).unwrap(),
            parent.artifact.producer.clone(),
            ArtifactLifecycle::RunScoped,
            parent.artifact.provenance.clone(),
            parent.artifact.origin.clone(),
            parent
                .payload
                .selections
                .iter()
                .map(|selection| selection.artifact.clone())
                .chain(std::iter::once(parent_ref.clone()))
                .collect(),
            now,
        )
        .unwrap();
        let proof = SucceededAttemptProof {
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
            lease_id: permit.lease_id.clone(),
            epoch: permit.epoch,
            contract_hash: permit.contract_hash.clone(),
            context_manifest: Some(ArtifactRef {
                artifact_id: nested.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }),
            outputs: Vec::new(),
        };

        let restored = ContextBroker::new(store)
            .restore_manifest_for_proof(&proof, &contract, nested, parent.payload, now)
            .unwrap();

        assert_eq!(restored.grant.readable.len(), 1);
        assert!(!restored.grant.readable.contains(&parent_ref.artifact_id));
    }

    #[test]
    fn context_is_explicit_and_raw_is_only_granted_by_closure() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let permit = permit_for_contract(
            &store,
            RunPurpose::Debug,
            Some(contract.contract_hash.clone()),
        );
        let raw = task_artifact(&store, &permit, ArtifactKind::RawEvidence, vec![], "raw");
        store
            .write_task_artifact(&permit, &raw, LifecycleEventType::EvidenceRaw, Utc::now())
            .unwrap();
        let normalized = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![ArtifactRef {
                artifact_id: raw.artifact_id.clone(),
                kind: ArtifactKind::RawEvidence,
            }],
            "normalized",
        );
        store
            .write_task_artifact(
                &permit,
                &normalized,
                LifecycleEventType::EvidenceNormalized,
                Utc::now(),
            )
            .unwrap();

        let broker = ContextBroker::new(store.clone());
        let manifest = broker
            .assemble(
                &permit,
                &contract,
                [ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                Utc::now(),
                Duration::minutes(5),
            )
            .unwrap();
        assert_eq!(manifest.payload.selections.len(), 1);
        assert_eq!(
            broker
                .read_raw(
                    &permit,
                    &contract,
                    &manifest.grant,
                    &raw.artifact_id,
                    Utc::now()
                )
                .unwrap()
                .kind,
            ArtifactKind::RawEvidence
        );
        assert!(matches!(
            broker.read(
                &permit,
                &contract,
                &manifest.grant,
                &raw.artifact_id,
                Utc::now()
            ),
            Err(ContextError::GrantDenied { .. })
        ));
    }

    #[test]
    fn read_grant_expiry_is_exclusive_for_context_reads() {
        let (_root, store, permit, contract, manifest, raw, _now) = manifest_fixture();
        let broker = ContextBroker::new(store);
        let selected = manifest.payload.selections[0].artifact.artifact_id.clone();
        let just_before = manifest.grant.expires_at - Duration::nanoseconds(1);

        assert!(broker
            .read(&permit, &contract, &manifest.grant, &selected, just_before)
            .is_ok());
        assert!(broker
            .read_raw(
                &permit,
                &contract,
                &manifest.grant,
                &raw.artifact_id,
                just_before
            )
            .is_ok());
        assert!(matches!(
            broker.read(
                &permit,
                &contract,
                &manifest.grant,
                &selected,
                manifest.grant.expires_at
            ),
            Err(ContextError::GrantDenied { .. })
        ));
        assert!(matches!(
            broker.read_raw(
                &permit,
                &contract,
                &manifest.grant,
                &raw.artifact_id,
                manifest.grant.expires_at,
            ),
            Err(ContextError::GrantDenied { .. })
        ));
    }

    #[test]
    fn unrelated_artifact_is_not_visible_to_the_grant() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let permit = permit_for_contract(
            &store,
            RunPurpose::Debug,
            Some(contract.contract_hash.clone()),
        );
        let first = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![],
            "first",
        );
        let second = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![],
            "second",
        );
        store
            .write_task_artifact(&permit, &first, LifecycleEventType::Evidence, Utc::now())
            .unwrap();
        store
            .write_task_artifact(&permit, &second, LifecycleEventType::Evidence, Utc::now())
            .unwrap();
        let broker = ContextBroker::new(store.clone());
        let manifest = broker
            .assemble(
                &permit,
                &contract,
                [ArtifactRef {
                    artifact_id: first.artifact_id.clone(),
                    kind: first.kind,
                }],
                Utc::now(),
                Duration::minutes(5),
            )
            .unwrap();
        assert!(matches!(
            broker.read(
                &permit,
                &contract,
                &manifest.grant,
                &second.artifact_id,
                Utc::now()
            ),
            Err(ContextError::GrantDenied { .. })
        ));
    }

    #[test]
    fn read_rejects_a_forged_readable_set() {
        let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store);
        let mut forged_grant = manifest.grant.clone();
        forged_grant.readable.insert(raw.artifact_id.clone());

        assert!(matches!(
            broker.read(&permit, &contract, &forged_grant, &raw.artifact_id, now),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn read_raw_rejects_a_forged_raw_source_closure() {
        let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store);
        let selected = manifest.payload.selections[0].artifact.artifact_id.clone();
        let mut forged_grant = manifest.grant.clone();
        forged_grant.raw_source_closure.insert(selected);

        assert!(matches!(
            broker.read_raw(&permit, &contract, &forged_grant, &raw.artifact_id, now),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn reads_reject_stale_attempt_identity_and_contract() {
        let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store.clone());
        let selected = &manifest.payload.selections[0].artifact.artifact_id;

        let mut wrong_epoch = permit.clone();
        wrong_epoch.epoch = wrong_epoch.epoch.saturating_add(1);
        assert!(matches!(
            broker.read(
                &wrong_epoch,
                &manifest_contract,
                &manifest.grant,
                selected,
                now
            ),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut wrong_attempt = permit.clone();
        wrong_attempt.attempt_id = akzio_domain::AttemptId::new();
        assert!(matches!(
            broker.read(
                &wrong_attempt,
                &manifest_contract,
                &manifest.grant,
                selected,
                now
            ),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut wrong_lease = permit.clone();
        wrong_lease.lease_id = akzio_domain::LeaseId::new();
        assert!(matches!(
            broker.read(
                &wrong_lease,
                &manifest_contract,
                &manifest.grant,
                selected,
                now
            ),
            Err(ContextError::InvalidManifestClosure)
        ));

        let wrong_contract = contract(&store);
        assert!(matches!(
            broker.read(&permit, &wrong_contract, &manifest.grant, selected, now),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn bootstrap_policy_can_mint_an_explicit_empty_manifest_only_when_allowed() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let permit = permit(&store);
        let broker = ContextBroker::new(store.clone());

        assert!(matches!(
            broker.assemble(
                &permit,
                &contract(&store),
                std::iter::empty(),
                Utc::now(),
                Duration::minutes(5),
            ),
            Err(ContextError::BudgetExceeded)
        ));

        let mut bootstrap = contract(&store);
        bootstrap.context.min_artifacts = 0;
        bootstrap.candidate_capability_ceiling.context.min_artifacts = 0;
        bootstrap.termination.require_evidence = false;
        bootstrap.contract_hash = bootstrap.expected_hash().unwrap();
        bootstrap.validate().unwrap();

        let manifest = broker
            .assemble(
                &permit,
                &bootstrap,
                std::iter::empty(),
                Utc::now(),
                Duration::minutes(5),
            )
            .unwrap();
        assert!(manifest.payload.selections.is_empty());
        assert!(manifest.grant.readable.is_empty());
        assert!(manifest.grant.raw_source_closure.is_empty());
    }

    #[test]
    fn repair_is_explicit_and_cannot_expand_a_grant() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let permit = permit_for_contract(
            &store,
            RunPurpose::Debug,
            Some(contract.contract_hash.clone()),
        );
        let normalized = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![],
            "normalized",
        );
        let unrelated = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![],
            "unrelated",
        );
        store
            .write_task_artifact(
                &permit,
                &normalized,
                LifecycleEventType::Evidence,
                Utc::now(),
            )
            .unwrap();
        store
            .write_task_artifact(
                &permit,
                &unrelated,
                LifecycleEventType::Evidence,
                Utc::now(),
            )
            .unwrap();
        let broker = ContextBroker::new(store.clone());
        let manifest = broker
            .assemble(
                &permit,
                &contract,
                [ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                Utc::now(),
                Duration::minutes(5),
            )
            .unwrap();
        let repair = broker
            .record_repair(
                &permit,
                &contract,
                &manifest.grant,
                vec![ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &serde_json::json!({"repair": "fixture"}),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(repair.kind, ArtifactKind::ContextRepair);
        assert_eq!(repair.source_refs[0].artifact_id, normalized.artifact_id);

        let mut stale_grant = manifest.grant.clone();
        stale_grant.epoch = stale_grant.epoch.saturating_add(1);
        assert!(matches!(
            broker.record_repair(
                &permit,
                &contract,
                &stale_grant,
                vec![ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &serde_json::json!({"repair": "stale-grant"}),
                Utc::now(),
            ),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut wrong_contract = contract.clone();
        wrong_contract.context.max_tokens = wrong_contract.context.max_tokens.saturating_sub(1);
        wrong_contract.contract_hash = wrong_contract.expected_hash().unwrap();
        assert!(matches!(
            broker.record_repair(
                &permit,
                &wrong_contract,
                &manifest.grant,
                vec![ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &serde_json::json!({"repair": "wrong-contract"}),
                Utc::now(),
            ),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut forged_grant = manifest.grant.clone();
        forged_grant.readable.insert(unrelated.artifact_id.clone());
        assert!(matches!(
            broker.record_repair(
                &permit,
                &contract,
                &forged_grant,
                vec![ArtifactRef {
                    artifact_id: unrelated.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &serde_json::json!({"repair": "forged-closure"}),
                Utc::now(),
            ),
            Err(ContextError::InvalidManifestClosure)
        ));
        assert!(matches!(
            broker.record_repair(
                &permit,
                &contract,
                &manifest.grant,
                vec![ArtifactRef {
                    artifact_id: unrelated.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &serde_json::json!({"repair": "forbidden"}),
                Utc::now(),
            ),
            Err(ContextError::GrantDenied { .. })
        ));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn policy_influences_accepts_only_the_persisted_manifest() {
        let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store);
        assert!(broker
            .policy_influences(&permit, &manifest_contract, &manifest, now)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn policy_influences_rejects_a_coherent_in_memory_forgery() {
        let (_root, store, permit, contract, manifest, raw, now) = manifest_fixture();
        let second = task_artifact(
            &store,
            &permit,
            ArtifactKind::NormalizedEvidence,
            vec![raw],
            "second normalized",
        );
        store
            .write_task_artifact(
                &permit,
                &second,
                LifecycleEventType::EvidenceNormalized,
                now,
            )
            .unwrap();

        let second_ref = ArtifactRef {
            artifact_id: second.artifact_id,
            kind: second.kind,
        };
        let mut forged = manifest;
        forged.payload.selections[0].artifact = second_ref.clone();
        forged.payload.selections[0].estimated_tokens = estimate_tokens(second.blob.bytes);
        forged.payload.total_bytes = second.blob.bytes;
        forged.payload.estimated_tokens = estimate_tokens(second.blob.bytes);
        forged.payload.input_hash = manifest_input_hash(&forged.payload.selections).unwrap();
        forged.artifact.source_refs = vec![second_ref.clone()];
        forged.grant.readable = BTreeSet::from([second_ref.artifact_id]);

        assert!(matches!(
            ContextBroker::new(store).policy_influences(&permit, &contract, &forged, now,),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn policy_influences_rejects_wrong_permit_contract_and_expiry() {
        let (_root, store, permit, manifest_contract, manifest, _raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store.clone());

        let mut wrong_permit = permit.clone();
        wrong_permit.epoch = wrong_permit.epoch.saturating_add(1);
        assert!(matches!(
            broker.policy_influences(&wrong_permit, &manifest_contract, &manifest, now),
            Err(ContextError::InvalidManifestClosure)
        ));

        let wrong_contract = contract(&store);
        assert!(matches!(
            broker.policy_influences(&permit, &wrong_contract, &manifest, now),
            Err(ContextError::InvalidManifestClosure)
        ));

        assert!(matches!(
            broker.policy_influences(
                &permit,
                &manifest_contract,
                &manifest,
                manifest.grant.expires_at,
            ),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn policy_influences_rejects_payload_artifact_and_raw_closure_mismatch() {
        let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
        let broker = ContextBroker::new(store);

        let mut payload_mismatch = manifest.clone();
        payload_mismatch.payload.total_bytes =
            payload_mismatch.payload.total_bytes.saturating_add(1);
        assert!(matches!(
            broker.policy_influences(&permit, &contract, &payload_mismatch, now),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut artifact_mismatch = manifest.clone();
        artifact_mismatch.artifact.source_refs.clear();
        assert!(matches!(
            broker.policy_influences(&permit, &contract, &artifact_mismatch, now),
            Err(ContextError::InvalidManifestClosure)
        ));

        let mut closure_mismatch = manifest;
        assert!(!closure_mismatch.grant.raw_source_closure.is_empty());
        closure_mismatch.grant.raw_source_closure.clear();
        assert!(matches!(
            broker.policy_influences(&permit, &contract, &closure_mismatch, now),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn policy_influences_recomputes_persisted_input_hash() {
        let (_root, store, permit, contract, manifest, _raw, now) = manifest_fixture();
        let mut payload = manifest.payload.clone();
        payload.input_hash = akzio_domain::ContentHash::of_bytes(b"forged input hash");
        let forged = persist_manifest_payload(&store, &permit, &manifest, payload, now);

        assert!(matches!(
            ContextBroker::new(store).policy_influences(&permit, &contract, &forged, now,),
            Err(ContextError::InvalidManifestClosure)
        ));
    }

    #[test]
    fn overlay_states_only_allow_active_proven_memory_and_active_policies() {
        assert!(overlay_state_is_eligible(
            ArtifactKind::Experience,
            PolicyState::Memory(MemoryLifecycle::Active),
        ));
        assert!(overlay_state_is_eligible(
            ArtifactKind::Experience,
            PolicyState::Memory(MemoryLifecycle::Proven),
        ));
        for state in [
            MemoryLifecycle::Candidate,
            MemoryLifecycle::Contested,
            MemoryLifecycle::Retired,
        ] {
            assert!(!overlay_state_is_eligible(
                ArtifactKind::Experience,
                PolicyState::Memory(state),
            ));
        }

        assert!(overlay_state_is_eligible(
            ArtifactKind::CandidatePolicy,
            PolicyState::Contract(CandidatePolicyState::Active),
        ));
        assert!(overlay_state_is_eligible(
            ArtifactKind::CandidatePolicy,
            PolicyState::Topology(CandidatePolicyState::Active),
        ));
        for state in [
            CandidatePolicyState::Candidate,
            CandidatePolicyState::Canary10,
            CandidatePolicyState::Canary25,
            CandidatePolicyState::Canary50,
        ] {
            assert!(!overlay_state_is_eligible(
                ArtifactKind::CandidatePolicy,
                PolicyState::Contract(state),
            ));
        }
        assert!(!overlay_state_is_eligible(
            ArtifactKind::CandidatePolicy,
            PolicyState::Memory(MemoryLifecycle::Active),
        ));
    }

    #[test]
    fn noncanonical_overlay_is_filtered_before_manifest_write() {
        for kind in [ArtifactKind::Experience, ArtifactKind::CandidatePolicy] {
            for purpose in [
                RunPurpose::Debug,
                RunPurpose::Replay,
                RunPurpose::Shadow,
                RunPurpose::PaperDryRun,
            ] {
                let root = tempdir().unwrap();
                let store = V2Store::open(root.path()).unwrap();
                let permit = permit_for_purpose(&store, purpose);
                let now = Utc::now();
                let overlay = Artifact::new(
                    kind,
                    store
                        .put_json(&serde_json::json!({"noncanonical": true}))
                        .unwrap(),
                    "fixture",
                    ArtifactLifecycle::Canonical,
                    provenance("learning"),
                    Some(ArtifactOrigin {
                        run_id: Some(permit.run_id.clone()),
                        task_id: Some(permit.task_id.clone()),
                        attempt_id: Some(permit.attempt_id.clone()),
                        contract_hash: permit.contract_hash.clone(),
                    }),
                    vec![],
                    now,
                )
                .unwrap();
                assert!(matches!(
                    store.write_task_artifact(
                        &permit,
                        &overlay,
                        LifecycleEventType::LearningOverlay,
                        now
                    ),
                    Err(StoreError::InvalidLearningCommit(
                        "learning_artifact.atomic_commit_required"
                    ))
                ));
            }
        }
    }
}
