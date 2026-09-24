// 文件导读：记录行为候选在各个资格阶段的冻结输入、评估输出和确定性 verdict。
// receipt_id 由身份字段计算，避免用后续可变字段替换已验收的上下文。
//! Immutable qualification stage receipts and deterministic evaluation contracts.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, ContentHash, DomainError, DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationStage {
    Replay,
    Adversarial,
    ExecutionSimulation,
    Shadow,
    Canary,
}

impl QualificationStage {
    pub const ORDERED: [Self; 5] = [
        Self::Replay,
        Self::Adversarial,
        Self::ExecutionSimulation,
        Self::Shadow,
        Self::Canary,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Adversarial => "adversarial",
            Self::ExecutionSimulation => "execution_simulation",
            Self::Shadow => "shadow",
            Self::Canary => "canary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationVerdict {
    Pass,
    Fail,
    Inconclusive,
    Regressed,
}

impl QualificationVerdict {
    // 只有 Pass 是可晋级结果，其余 verdict 都保留为非通过状态。
    pub const fn is_pass(self) -> bool {
        matches!(self, Self::Pass)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationStageReceipt {
    pub schema_version: u32,
    pub receipt_id: ContentHash,
    pub behavior_bundle_hash: ContentHash,
    pub qualification_key_hash: ContentHash,
    pub stage: QualificationStage,
    pub scenario_bank_hash: ContentHash,
    pub scenario_id: String,
    pub critical: bool,
    pub frozen_context_manifest: ArtifactRef,
    pub champion_snapshot: ContentHash,
    pub challenger_snapshot: ContentHash,
    pub champion_output: ArtifactRef,
    pub challenger_output: ArtifactRef,
    pub evaluator_commitment: ContentHash,
    pub evaluator_reveal: ContentHash,
    pub evaluator_version_hash: ContentHash,
    #[serde(default)]
    pub measured_metrics: BTreeMap<String, i64>,
    pub deterministic_verdict: QualificationVerdict,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl QualificationStageReceipt {
    // 先封存身份哈希，再验证当前 receipt，返回可持久化的不可变副本。
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.receipt_id = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    // 哈希覆盖资格输入、评估承诺/reveal、指标、结果和来源引用。
    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "behavior_bundle_hash": self.behavior_bundle_hash,
            "qualification_key_hash": self.qualification_key_hash,
            "stage": self.stage,
            "scenario_bank_hash": self.scenario_bank_hash,
            "scenario_id": self.scenario_id,
            "critical": self.critical,
            "frozen_context_manifest": self.frozen_context_manifest,
            "champion_snapshot": self.champion_snapshot,
            "challenger_snapshot": self.challenger_snapshot,
            "champion_output": self.champion_output,
            "challenger_output": self.challenger_output,
            "evaluator_commitment": self.evaluator_commitment,
            "evaluator_reveal": self.evaluator_reveal,
            "evaluator_version_hash": self.evaluator_version_hash,
            "measured_metrics": self.measured_metrics,
            "deterministic_verdict": self.deterministic_verdict,
            "source_refs": self.source_refs,
            "created_at": self.created_at,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    // 校验 schema、场景、ContextManifest kind 以及两侧输出允许的 Artifact kind。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.receipt_id != self.identity_hash()?
            || self.scenario_id.trim().is_empty()
            || self.frozen_context_manifest.kind != ArtifactKind::ContextManifest
            || !matches!(
                self.champion_output.kind,
                ArtifactKind::Evaluation
                    | ArtifactKind::DecisionProposal
                    | ArtifactKind::Decision
                    | ArtifactKind::AgentTurn
            )
            || !matches!(
                self.challenger_output.kind,
                ArtifactKind::Evaluation
                    | ArtifactKind::DecisionProposal
                    | ArtifactKind::Decision
                    | ArtifactKind::AgentTurn
            )
        {
            return Err(DomainError::InvalidBudget {
                field: "qualification_stage_receipt",
            });
        }
        Ok(())
    }
}
