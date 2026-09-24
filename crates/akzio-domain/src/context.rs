// 文件导读：定义 ContextManifest 的选择、隔离、投影和一次性读取授权。
// 这里的类型只表达 Rust 已经决定的边界，实际材料读取仍由 akzio-context 执行。
//! Context-manifest and read-grant domain vocabulary.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactId, ArtifactKind, ArtifactOrigin, ArtifactRef};
use crate::contract::ContextPolicy;
use crate::schema::SCHEMA_VERSION;
use crate::{content_hash_json, ContentHash, DomainError, LeaseId, RunId, TaskId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextTrust {
    GovernedArtifact,
    #[default]
    UntrustedEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextQuarantineReason {
    InstructionLikeContent,
    FinancialContentRisk,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextQuarantine {
    pub artifact: ArtifactRef,
    pub reason: ContextQuarantineReason,
    pub indicators: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelection {
    pub artifact: ArtifactRef,
    pub reason: String,
    pub estimated_tokens: u32,
    /// Size of the compact model projection, when this manifest uses the
    /// separated source/projection budget. Legacy manifests leave it absent.
    #[serde(default)]
    pub projected_bytes: Option<u64>,
    #[serde(default)]
    pub trust: ContextTrust,
}

/// Hashes the ordered (artifact ID, kind) tuples. Selection metadata is excluded;
/// order and repeated references are part of the existing identity contract.
// 仅取有序选择中的 ID/kind 元组计算身份哈希；reason 和预算字段不会改变该身份。
pub fn manifest_input_hash(
    selections: &[ContextSelection],
) -> Result<ContentHash, serde_json::Error> {
    content_hash_json(&serde_json::to_value(
        selections
            .iter()
            .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
            .collect::<Vec<_>>(),
    )?)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextManifestPayload {
    pub schema_version: u32,
    pub contract_hash: ContentHash,
    pub selections: Vec<ContextSelection>,
    #[serde(default)]
    pub quarantined: Vec<ContextQuarantine>,
    pub total_bytes: u64,
    /// Sum of compact projection bytes. Legacy manifests use `total_bytes`
    /// for both source and model-visible content.
    #[serde(default)]
    pub projected_bytes: Option<u64>,
    pub estimated_tokens: u32,
    pub input_hash: ContentHash,
}

impl ContextManifestPayload {
    // 校验 schema、数量、源/投影字节预算、token 估算、kind/trust 及 quarantine 唯一性。
    pub fn validate(&self, policy: &ContextPolicy) -> Result<(), DomainError> {
        if self.schema_version != SCHEMA_VERSION
            || self.selections.len() < usize::from(policy.min_artifacts)
            || self.selections.len() > usize::from(policy.max_artifacts)
            || self.total_bytes > policy.max_source_bytes.unwrap_or(policy.max_bytes)
            || self.projected_bytes.unwrap_or(self.total_bytes) > policy.max_bytes
            || self.estimated_tokens > policy.max_tokens
        {
            return Err(DomainError::InvalidBudget {
                field: "context_manifest",
            });
        }
        // any 闭包把每个选择项的局部字段和信任边界组合成单一拒绝条件。
        if self.selections.iter().any(|selection| {
            selection.reason.trim().is_empty()
                || selection.estimated_tokens == 0
                || !policy.permitted_kinds.contains(&selection.artifact.kind)
                || (matches!(
                    selection.artifact.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                ) && selection.trust != ContextTrust::UntrustedEvidence)
        }) {
            return Err(DomainError::EmptyField {
                field: "context_manifest.selection",
            });
        }
        let selected = self
            .selections
            .iter()
            .map(|selection| &selection.artifact)
            .collect::<BTreeSet<_>>();
        let mut quarantined = BTreeSet::new();
        // BTreeSet 同时检查 quarantine 不重复，并让后续验证保持确定性。
        if self.quarantined.iter().any(|quarantine| {
            !matches!(
                quarantine.artifact.kind,
                ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
            ) || selected.contains(&quarantine.artifact)
                || !quarantined.insert(quarantine.artifact.clone())
                || quarantine.indicators.is_empty()
                || quarantine.indicators.len() > 8
                || quarantine
                    .indicators
                    .iter()
                    .any(|indicator| indicator.trim().is_empty() || indicator.len() > 128)
        }) {
            return Err(DomainError::EmptyField {
                field: "context_manifest.quarantine",
            });
        }
        Ok(())
    }
}

/// Rust-owned attenuation contract for projecting one persisted context
/// manifest into a child task. It carries artifact references only; raw
/// evidence and model transcripts are never delegation inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextProjection {
    pub parent_manifest: ArtifactRef,
    pub allowed: Vec<ArtifactRef>,
    pub reason: String,
}

impl ContextProjection {
    // 验证父清单类型、原因和允许集合；RawEvidence 以及重复 ID 都被拒绝。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.parent_manifest.kind != ArtifactKind::ContextManifest
            || self.reason.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "context_projection",
            });
        }

        let mut seen = BTreeSet::new();
        if self.allowed.iter().any(|reference| {
            reference.kind == ArtifactKind::RawEvidence
                || !seen.insert(reference.artifact_id.clone())
        }) {
            return Err(DomainError::EmptyField {
                field: "context_projection.allowed",
            });
        }

        Ok(())
    }
}

/// Ephemeral, task-scoped authorization derived from a persisted manifest. It is
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadGrant {
    pub manifest_artifact_id: ArtifactId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: ContentHash,
    pub readable: BTreeSet<ArtifactId>,
    pub raw_source_closure: BTreeSet<ArtifactId>,
    pub expires_at: DateTime<Utc>,
}

impl ReadGrant {
    // 比较一次运行尝试的全部身份字段，防止旧 permit 被另一任务或 lease 借用。
    pub fn matches_permit(&self, permit: &TaskWritePermit) -> bool {
        self.run_id == permit.run_id
            && self.task_id == permit.task_id
            && self.attempt_id == permit.attempt_id
            && self.lease_id == permit.lease_id
            && self.epoch == permit.epoch
            && permit.contract_hash.as_ref() == Some(&self.contract_hash)
    }

    // 在有效期内按 raw 标志选择普通 readable 集合或原始来源闭包进行授权判断。
    pub fn permits(&self, artifact_id: &ArtifactId, raw: bool, now: DateTime<Utc>) -> bool {
        now < self.expires_at
            && if raw {
                self.raw_source_closure.contains(artifact_id)
            } else {
                self.readable.contains(artifact_id)
            }
    }
}

/// Authorizes exactly one running attempt to create an artifact or commit a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWritePermit {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: Option<ContentHash>,
}

impl TaskWritePermit {
    // 把 permit 的运行/任务/尝试身份转换为新 Artifact 的 provenance 来源。
    pub fn artifact_origin(&self) -> ArtifactOrigin {
        ArtifactOrigin {
            run_id: Some(self.run_id.clone()),
            task_id: Some(self.task_id.clone()),
            attempt_id: Some(self.attempt_id.clone()),
            contract_hash: self.contract_hash.clone(),
        }
    }
}
