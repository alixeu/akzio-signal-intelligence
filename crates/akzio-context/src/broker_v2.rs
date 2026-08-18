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

struct ParentContextProof<'a> {
    manifest: &'a ArtifactRef,
    readable: &'a BTreeSet<ArtifactRef>,
    raw_closure: &'a BTreeSet<ArtifactId>,
    permit: &'a TaskWritePermit,
    contract: &'a AgentContract,
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
        // Parent manifest proves provenance; committed outputs are the child data surface
        // only after Rust applies the child's policy-owned projection.
        let projection = derive_child_projection(proof, manifest_ref, child_contract);
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

#[path = "selection.rs"]
mod selection;
#[cfg(test)]
use selection::projection_artifact_ids;
use selection::{
    context_rank, derive_child_projection, estimate_tokens, is_safe_deliberation_summary,
    is_trace_kind, manifest_input_hash, overlay_state_is_eligible, selection_reason,
};
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
