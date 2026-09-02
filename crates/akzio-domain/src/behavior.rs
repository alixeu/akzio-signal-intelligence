//! Unified Candidate Identity and Behavior Bundle Manifest.
//!
//! A behavioral change in an agent trading system encompasses prompts, model
//! snapshots, provider routes, decision/execution/evaluation policies, regime
//! detectors, and retrieval rules—not merely Contract and Topology graphs.
//! Every candidate undergoes verification bound strictly to this immutable identity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, ContentHash, DomainError, DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BehaviorChangeReason {
    InitialBaseline,
    PromptRefinement,
    ModelSnapshotUpgrade,
    PolicyAdjustment,
    RegimeDetectorUpdate,
    ContractEvolution,
    TopologyRefactor,
    CanaryPromotion,
    RollbackRemediation,
    ManualIntervention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorBundleManifest {
    pub schema_version: u32,
    pub bundle_hash: ContentHash,
    pub parent_bundle: Option<ContentHash>,
    pub contract: ArtifactRef,
    pub topology: ArtifactRef,
    pub model_routes_hash: ContentHash,
    pub model_capability_bundle_hash: ContentHash,
    pub prompt_bundle_hash: ContentHash,
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    pub governance_bundle_hash: ContentHash,
    pub regime_taxonomy_hash: ContentHash,
    pub regime_detector_hash: ContentHash,
    pub retrieval_policy_hash: ContentHash,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub change_reason: BehaviorChangeReason,
}

impl BehaviorBundleManifest {
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.bundle_hash = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "parent_bundle": self.parent_bundle,
            "contract": self.contract,
            "topology": self.topology,
            "model_routes_hash": self.model_routes_hash,
            "model_capability_bundle_hash": self.model_capability_bundle_hash,
            "prompt_bundle_hash": self.prompt_bundle_hash,
            "decision_policy_hash": self.decision_policy_hash,
            "execution_policy_hash": self.execution_policy_hash,
            "evaluation_policy_hash": self.evaluation_policy_hash,
            "governance_bundle_hash": self.governance_bundle_hash,
            "regime_taxonomy_hash": self.regime_taxonomy_hash,
            "regime_detector_hash": self.regime_detector_hash,
            "retrieval_policy_hash": self.retrieval_policy_hash,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "change_reason": self.change_reason,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.bundle_hash != self.identity_hash()?
            || self.contract.kind != ArtifactKind::Contract
            || self.topology.kind != ArtifactKind::WorkflowGraph
            || self.created_by.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "behavior_bundle_manifest",
            });
        }
        Ok(())
    }
}

/// Unified candidate lifecycle across the complete verification pipeline:
/// Draft -> Candidate -> ExperimentVerified -> Qualified -> CanaryVerified ->
/// PaperVerified -> PostOutcomeApproved -> Canonical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateLifecycleState {
    Draft,
    Candidate,
    ExperimentVerified,
    Qualified,
    CanaryVerified,
    PaperVerified,
    PostOutcomeApproved,
    Canonical,
    // Failure / terminal states
    Rejected,
    RegressionFailed,
    Failed,
    Quarantined,
    Stale,
    Demoted,
    Revoked,
}

impl CandidateLifecycleState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Rejected
                | Self::RegressionFailed
                | Self::Failed
                | Self::Quarantined
                | Self::Stale
                | Self::Demoted
                | Self::Revoked
        )
    }

    pub const fn is_canonical(self) -> bool {
        matches!(self, Self::Canonical)
    }

    pub fn permits_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Draft, Self::Candidate)
                | (Self::Candidate, Self::ExperimentVerified | Self::Rejected)
                | (
                    Self::ExperimentVerified,
                    Self::Qualified | Self::RegressionFailed | Self::Rejected,
                )
                | (
                    Self::Qualified,
                    Self::CanaryVerified | Self::RegressionFailed | Self::Failed
                )
                | (
                    Self::CanaryVerified,
                    Self::PaperVerified | Self::Failed | Self::Quarantined
                )
                | (
                    Self::PaperVerified,
                    Self::PostOutcomeApproved | Self::Quarantined | Self::Failed,
                )
                | (Self::PostOutcomeApproved, Self::Canonical | Self::Demoted)
                | (Self::Canonical, Self::Stale | Self::Demoted | Self::Revoked)
        )
    }
}
