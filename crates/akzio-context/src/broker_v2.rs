//! Manifest-and-grant context broker for the v2 runtime.

use std::collections::{BTreeSet, VecDeque};

use akzio_domain::{
    content_hash_json, AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle,
    ArtifactOrigin, ArtifactProvenance, ArtifactRef, ContextManifestPayload, ContextPolicy,
    ContextSelection, DomainError, ReadGrant, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebuildContextError {
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
}

pub type RebuildContextResult<T> = Result<T, RebuildContextError>;

#[derive(Debug, Clone)]
pub struct RebuildContextBroker {
    store: V2Store,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildContextManifest {
    pub artifact: Artifact,
    pub payload: ContextManifestPayload,
    pub grant: ReadGrant,
}

impl RebuildContextBroker {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
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
    ) -> RebuildContextResult<RebuildContextManifest> {
        contract.validate()?;
        let policy = &contract.context;
        let mut seen = BTreeSet::new();
        let mut artifacts = candidates
            .into_iter()
            .filter(|reference| seen.insert(reference.artifact_id.clone()))
            .map(|reference| self.store.artifact(&reference.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        for artifact in &artifacts {
            self.assert_context_permitted(policy, artifact)?;
        }
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
            return Err(RebuildContextError::BudgetExceeded);
        }

        let input_hash = content_hash_json(&serde_json::to_value(
            selections
                .iter()
                .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
                .collect::<Vec<_>>(),
        )?)?;
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
        self.store
            .write_task_artifact(permit, &artifact, "context.manifest_created", now)?;
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
        Ok(RebuildContextManifest {
            artifact,
            payload,
            grant,
        })
    }

    pub fn read(
        &self,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> RebuildContextResult<Artifact> {
        if !grant.permits(artifact_id, false, now) {
            return Err(RebuildContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        let artifact = self.store.artifact(artifact_id)?;
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(RebuildContextError::RawEvidenceRequiresExplicitRead);
        }
        Ok(artifact)
    }

    pub fn read_raw(
        &self,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> RebuildContextResult<Artifact> {
        if !grant.permits(artifact_id, true, now) {
            return Err(RebuildContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        let artifact = self.store.artifact(artifact_id)?;
        if artifact.kind != ArtifactKind::RawEvidence {
            return Err(RebuildContextError::ExpectedRawEvidence);
        }
        Ok(artifact)
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
    ) -> RebuildContextResult<Artifact> {
        for source in &source_refs {
            if !grant.permits(
                &source.artifact_id,
                source.kind == ArtifactKind::RawEvidence,
                now,
            ) {
                return Err(RebuildContextError::GrantDenied {
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
        self.store
            .write_task_artifact(permit, &artifact, "context.repaired", now)?;
        Ok(artifact)
    }

    fn assert_context_permitted(
        &self,
        policy: &ContextPolicy,
        artifact: &Artifact,
    ) -> RebuildContextResult<()> {
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(RebuildContextError::RawEvidenceInManifest);
        }
        if !policy.permitted_kinds.contains(&artifact.kind)
            || (!policy.permitted_source_families.is_empty()
                && !policy
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family))
        {
            return Err(RebuildContextError::ForbiddenArtifact {
                artifact_id: artifact.artifact_id.clone(),
            });
        }
        Ok(())
    }

    fn raw_closure(
        &self,
        policy: &ContextPolicy,
        selections: &[ContextSelection],
    ) -> RebuildContextResult<BTreeSet<ArtifactId>> {
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
        ArtifactKind::Experience | ArtifactKind::Evaluation => 3,
        _ => 4,
    }
}

fn selection_reason(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::NormalizedEvidence => "normalized_evidence",
        ArtifactKind::SemanticDetail => "semantic_detail",
        ArtifactKind::Claim => "claim",
        ArtifactKind::Critique => "critique",
        ArtifactKind::Experience => "experience",
        ArtifactKind::Evaluation => "evaluation",
        _ => "contract_permitted",
    }
}

fn estimate_tokens(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use akzio_domain::{
        ArtifactKind, ContractId, ContractPurpose, FailureDisposition, OutputContract, RetryPolicy,
        TaskBudget, TerminationPolicy, ToolGrant, ToolKind, WorkflowGraph, WorkflowNode,
        REBUILD_SCHEMA_VERSION,
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
            store.put_bytes(b"prompt", "text/plain").unwrap(),
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
        let node = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: akzio_domain::TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: None,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            purpose: akzio_domain::RunPurpose::Debug,
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
                contract_hash: None,
            }),
            source_refs,
            Utc::now(),
        )
        .unwrap()
    }

    #[test]
    fn context_is_explicit_and_raw_is_only_granted_by_closure() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let permit = permit(&store);
        let raw = task_artifact(&store, &permit, ArtifactKind::RawEvidence, vec![], "raw");
        store
            .write_task_artifact(&permit, &raw, "evidence.raw", Utc::now())
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
            .write_task_artifact(&permit, &normalized, "evidence.normalized", Utc::now())
            .unwrap();

        let broker = RebuildContextBroker::new(store.clone());
        let contract = contract(&store);
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
                .read_raw(&manifest.grant, &raw.artifact_id, Utc::now())
                .unwrap()
                .kind,
            ArtifactKind::RawEvidence
        );
        assert!(matches!(
            broker.read(&manifest.grant, &raw.artifact_id, Utc::now()),
            Err(RebuildContextError::GrantDenied { .. })
        ));
    }

    #[test]
    fn unrelated_artifact_is_not_visible_to_the_grant() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let permit = permit(&store);
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
            .write_task_artifact(&permit, &first, "evidence", Utc::now())
            .unwrap();
        store
            .write_task_artifact(&permit, &second, "evidence", Utc::now())
            .unwrap();
        let broker = RebuildContextBroker::new(store.clone());
        let contract = contract(&store);
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
            broker.read(&manifest.grant, &second.artifact_id, Utc::now()),
            Err(RebuildContextError::GrantDenied { .. })
        ));
    }

    #[test]
    fn bootstrap_policy_can_mint_an_explicit_empty_manifest_only_when_allowed() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let permit = permit(&store);
        let broker = RebuildContextBroker::new(store.clone());

        assert!(matches!(
            broker.assemble(
                &permit,
                &contract(&store),
                std::iter::empty(),
                Utc::now(),
                Duration::minutes(5),
            ),
            Err(RebuildContextError::BudgetExceeded)
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
        let permit = permit(&store);
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
            .write_task_artifact(&permit, &normalized, "evidence", Utc::now())
            .unwrap();
        store
            .write_task_artifact(&permit, &unrelated, "evidence", Utc::now())
            .unwrap();
        let broker = RebuildContextBroker::new(store.clone());
        let contract = contract(&store);
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
            Err(RebuildContextError::GrantDenied { .. })
        ));
        store.verify_integrity().unwrap();
    }
}
