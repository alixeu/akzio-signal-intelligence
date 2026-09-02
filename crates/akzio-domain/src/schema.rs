//! Canonical v2 artifact, contract, workflow, and authority schema.
//!
//! This module is intentionally introduced beside the former vocabulary while the
//! workspace is migrated. Its types are the only types new runtime code may use;
//! the old document/role/task types are removed once every crate crosses this seam.

use serde::{Deserialize, Serialize};

use crate::DomainError;

#[cfg(test)]
use crate::{
    AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance,
    ArtifactRef, BlobRef, CandidateCapabilityCeiling, ContentHash, ContextPolicy, ContractId,
    ContractPurpose, EvidenceNeed, FailureDisposition, LeaseId, OutputContract, PromptBundle,
    ReadGrant, RetryPolicy, RunId, RuntimeTaskClass, TaskBudget, TaskId, TaskRecipe, TaskRecipeId,
    TaskWritePermit, TerminationPolicy, ToolGrant, ToolSpec, WorkflowProposal,
    WorkflowProposalDraft, WorkflowProposalDraftTask, WorkflowProposalTask,
};
#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};

/// A Store Root with this schema is intentionally incompatible with the previous
/// v2 database. It is a fresh schema, not a migration layer.
pub const V2_SCHEMA_VERSION: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLimits {
    pub global_leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub paired_index_ppm: u32,
}

impl FactorLimits {
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.global_leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.paired_index_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_limits",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{AttemptId, ToolKind};

    fn blob(value: &[u8]) -> BlobRef {
        BlobRef {
            hash: ContentHash::of_bytes(value),
            media_type: "application/json".to_owned(),
            bytes: value.len() as u64,
        }
    }

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.market".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    #[test]
    fn artifact_identity_commits_metadata_and_payload() {
        let artifact = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            blob(b"payload"),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![],
            Utc::now(),
        )
        .unwrap();
        artifact.validate().unwrap();

        let mut substituted = artifact;
        substituted.producer = "different".to_owned();
        assert_eq!(substituted.validate(), Err(DomainError::InvalidContentHash));
    }

    #[test]
    fn artifact_identity_canonicalizes_source_reference_order() {
        let first = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"z-source")),
            kind: ArtifactKind::ToolCall,
        };
        let second = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"a-source")),
            kind: ArtifactKind::NormalizedEvidence,
        };
        let artifact = Artifact::new(
            ArtifactKind::Claim,
            blob(b"claim"),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![first.clone(), second.clone()],
            Utc::now(),
        )
        .unwrap();

        assert_eq!(artifact.source_refs, vec![second, first]);

        let mut reordered = artifact.clone();
        reordered.source_refs.reverse();
        assert_eq!(
            reordered.validate(),
            Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            })
        );
        assert_eq!(reordered.expected_hash().unwrap(), artifact.artifact_id.0);
    }

    #[test]
    fn planner_artifacts_cannot_be_canonical() {
        for kind in [
            ArtifactKind::EvidenceNeed,
            ArtifactKind::WorkflowProposalDraft,
            ArtifactKind::WorkflowProposal,
            ArtifactKind::WorkflowGraph,
            ArtifactKind::DecisionProposal,
        ] {
            assert!(!kind.can_be_canonical());
        }
    }

    #[test]
    fn normalized_evidence_rejects_a_non_raw_source() {
        let source = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"detail")),
            kind: ArtifactKind::SemanticDetail,
        };

        assert!(matches!(
            Artifact::new(
                ArtifactKind::NormalizedEvidence,
                blob(b"normalized"),
                "fixture",
                ArtifactLifecycle::RunScoped,
                provenance(),
                None,
                vec![source],
                Utc::now(),
            ),
            Err(DomainError::EmptyField {
                field: "artifact.normalized_source_refs"
            })
        ));
    }

    #[test]
    fn contract_hash_rejects_prompt_or_grant_substitution() {
        let contract = AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "derive claims",
            PromptBundle {
                version: 1,
                governance: blob(b"governance"),
                role: blob(b"prompt"),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 1024,
                max_tokens: 256,
                allow_raw_reread: true,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_artifact".to_owned(),
                description: "read granted artifact".to_owned(),
                kind: ToolKind::ReadEvidence,
                input_schema: blob(b"tool schema"),
                strict: true,
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: blob(b"schema"),
            },
            TaskBudget {
                max_input_tokens: 256,
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
            FailureDisposition::FailTask,
        )
        .unwrap();
        contract.validate().unwrap();

        let mut substituted = contract.clone();
        substituted.prompt.role = blob(b"different prompt");
        assert_eq!(substituted.validate(), Err(DomainError::InvalidContentHash));

        let mut substituted_tool = contract.clone();
        substituted_tool.tool_specs[0].description = "different tool description".to_owned();
        assert_eq!(
            substituted_tool.validate(),
            Err(DomainError::InvalidContentHash)
        );

        let mut expanded = contract.clone();
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::FetchWebEvidence,
            allowed_sources: vec!["news".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());

        expanded = contract.clone();
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["news".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());

        let mut candidate = contract.clone();
        candidate
            .context
            .permitted_source_families
            .insert("news".to_owned());
        candidate.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["market".to_owned(), "news".to_owned()],
        }];
        candidate.candidate_capability_ceiling = CandidateCapabilityCeiling {
            context: candidate.context.clone(),
            tool_grants: candidate.tool_grants.clone(),
        };
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();
        assert!(!contract.permits_candidate(&candidate));

        let active = contract
            .clone()
            .with_candidate_capability_ceiling(candidate.candidate_capability_ceiling.clone())
            .unwrap();
        assert!(active.permits_candidate(&candidate));

        expanded = contract;
        expanded.context.allow_raw_reread = false;
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadRawEvidence,
            allowed_sources: vec!["market".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());
    }

    #[test]
    fn context_minimum_is_validated_and_candidates_cannot_lower_it() {
        let policy = ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 1024,
            max_tokens: 256,
            allow_raw_reread: false,
        };
        policy.validate().unwrap();
        let ceiling = CandidateCapabilityCeiling {
            context: policy.clone(),
            tool_grants: vec![],
        };

        let mut lower_minimum = policy.clone();
        lower_minimum.min_artifacts = 0;
        assert!(!ceiling.permits(&lower_minimum, &[]));

        let mut stricter_minimum = policy.clone();
        stricter_minimum.min_artifacts = 2;
        assert!(ceiling.permits(&stricter_minimum, &[]));

        let mut invalid = policy;
        invalid.min_artifacts = invalid.max_artifacts + 1;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn workflow_proposal_rejects_unknown_recipes_and_cycles() {
        let recipe_id = TaskRecipeId::new("analyst").unwrap();
        let recipe = TaskRecipe {
            recipe_id: recipe_id.clone(),
            purpose: ContractPurpose::new("research.analyst").unwrap(),
            contract_hash: Some(ContentHash::of_bytes(b"fixture-contract")),
            task_class: RuntimeTaskClass::Agent,
            allowed_evidence_sources: BTreeSet::new(),
            max_children: 4,
            max_depth: 4,
            priority_ceiling: 80,
            budget: TaskBudget {
                max_input_tokens: 256,
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
            on_failure: FailureDisposition::FailTask,
        };
        let recipes = BTreeMap::from([(recipe_id.clone(), recipe)]);
        let mut proposal = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    WorkflowProposalTask {
                        recipe_id: recipe_id.clone(),
                        objective: "analyze evidence".to_owned(),
                        depends_on: vec![],
                        priority: 50,
                        evidence_needs: vec![],
                    },
                ),
                (
                    "critic".to_owned(),
                    WorkflowProposalTask {
                        recipe_id,
                        objective: "challenge the analysis".to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: 50,
                        evidence_needs: vec![],
                    },
                ),
            ]),
            stop_reason: None,
        };
        proposal.validate(&recipes).unwrap();

        proposal
            .tasks
            .get_mut("analyst")
            .unwrap()
            .depends_on
            .push("critic".to_owned());
        assert_eq!(proposal.validate(&recipes), Err(DomainError::CyclicPlan));

        proposal
            .tasks
            .get_mut("analyst")
            .unwrap()
            .depends_on
            .clear();
        proposal.tasks.get_mut("critic").unwrap().recipe_id =
            TaskRecipeId::new("uninstalled").unwrap();
        assert!(matches!(
            proposal.validate(&recipes),
            Err(DomainError::EmptyField {
                field: "workflow_proposal.recipe"
            })
        ));
    }

    #[test]
    fn workflow_proposal_draft_limits_evidence_to_recipe_sources() {
        let recipe_id = TaskRecipeId::new("research.analyst").unwrap();
        let recipe = TaskRecipe {
            recipe_id: recipe_id.clone(),
            purpose: ContractPurpose::new("research.analyst").unwrap(),
            contract_hash: Some(ContentHash::of_bytes(b"fixture-contract")),
            task_class: RuntimeTaskClass::Agent,
            allowed_evidence_sources: BTreeSet::from(["alpaca".to_owned()]),
            max_children: 4,
            max_depth: 4,
            priority_ceiling: 80,
            budget: TaskBudget {
                max_input_tokens: 256,
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
            on_failure: FailureDisposition::FailTask,
        };
        let recipes = BTreeMap::from([(recipe_id.clone(), recipe)]);
        let mut draft = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id,
                    objective: "analyze governed market evidence".to_owned(),
                    depends_on: vec![],
                    priority: 50,
                    evidence_needs: vec![EvidenceNeed {
                        schema_version: V2_SCHEMA_VERSION,
                        source_family: "alpaca".to_owned(),
                        resource: "bars:TQQQ:1d".to_owned(),
                        max_age_secs: 86_400,
                    }],
                    research_intents: vec![],
                },
            )]),
            stop_reason: None,
        };
        draft.validate(&recipes).unwrap();

        draft.tasks.get_mut("analyst").unwrap().evidence_needs[0].source_family =
            "uninstalled-web".to_owned();
        assert_eq!(
            draft.validate(&recipes),
            Err(DomainError::EvidenceSourceNotAllowed(
                "uninstalled-web".to_owned()
            ))
        );
    }

    #[test]
    fn write_permit_is_attempt_specific() {
        let permit = TaskWritePermit {
            run_id: RunId::new(),
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            lease_id: LeaseId::new(),
            epoch: 1,
            contract_hash: None,
        };
        assert_ne!(permit.attempt_id.0, AttemptId::new().0);
    }

    #[test]
    fn read_grant_is_bound_to_the_minting_permit() {
        let permit = TaskWritePermit {
            run_id: RunId::new(),
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            lease_id: LeaseId::new(),
            epoch: 7,
            contract_hash: Some(ContentHash::of_bytes(b"contract")),
        };
        let mut grant = ReadGrant {
            manifest_artifact_id: ArtifactId(ContentHash::of_bytes(b"manifest")),
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
            lease_id: permit.lease_id.clone(),
            epoch: permit.epoch,
            contract_hash: permit.contract_hash.clone().unwrap(),
            readable: BTreeSet::new(),
            raw_source_closure: BTreeSet::new(),
            expires_at: Utc::now(),
        };

        assert!(grant.matches_permit(&permit));
        grant.epoch += 1;
        assert!(!grant.matches_permit(&permit));
    }
}
