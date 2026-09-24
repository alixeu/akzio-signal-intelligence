// 文件导读：定义把 Contract、Topology、模型路由、Prompt、Policy 和检索规则
// 绑定为一个不可变行为候选身份的清单，并负责生成/校验其内容哈希。
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
    // 先用当前字段重算 bundle_hash，再执行完整校验，返回带正确身份哈希的副本。
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.bundle_hash = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    // 按身份字段构造稳定 JSON；元数据之外的 bundle_hash 本身不参与哈希，避免自引用。
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

    // 检查 schema、身份哈希、Artifact kind 和创建者，失败时拒绝整个行为清单。
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
