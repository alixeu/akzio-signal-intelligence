// 文件导读：定义 RuntimeIdentity、运行期授权清单、Paper 启动审批和 T+5 后研究审批。
// 这些类型把代码/配置/模型/Policy/证据身份绑定到可验证的时间与范围窗口。
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, ContentHash, DomainError, ExperimentCondition,
    MandateSnapshot, ModelQualificationKey, ModelQualificationReport, MoneyMicros,
    DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperApprovalScope {
    Canary,
    Scheduled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub code_revision: String,
    pub cargo_lock_hash: ContentHash,
    pub config_hash: ContentHash,
    pub provider_id: String,
    pub model_id: String,
    pub prompt_hash: ContentHash,
    pub contract_hash: ContentHash,
    pub topology_hash: ContentHash,
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    #[serde(default = "legacy_experiment_profile")]
    pub experiment_profile: String,
    #[serde(default = "legacy_rust_toolchain")]
    pub rust_toolchain: String,
    #[serde(default)]
    pub model_release_date: Option<NaiveDate>,
    #[serde(default)]
    pub model_knowledge_cutoff: Option<NaiveDate>,
    #[serde(default)]
    pub historical_evaluation_condition: Option<ExperimentCondition>,
    #[serde(default)]
    pub model_routes_hash: Option<ContentHash>,
    #[serde(default)]
    pub model_capability_hashes: BTreeMap<String, ContentHash>,
    #[serde(default = "empty_component_bundle_hash")]
    pub model_capability_bundle_hash: ContentHash,
    #[serde(default)]
    pub governance_component_hashes: BTreeMap<String, ContentHash>,
    #[serde(default = "empty_component_bundle_hash")]
    pub governance_bundle_hash: ContentHash,
    pub market_data_feed: String,
}

impl RuntimeIdentity {
    /// Qualification identity deliberately covers every externally mutable
    /// behavior surface currently represented by RuntimeIdentity. A changed
    /// provider/model route, prompt/contract/tool capability, config or
    /// retrieval/governance bundle therefore cannot reuse an old report.
    // 先验证身份，再把 provider/model/prompt/tool/config/context 的哈希投影成资格键。
    pub fn qualification_key(&self) -> Result<ModelQualificationKey, DomainError> {
        self.validate()?;
        Ok(ModelQualificationKey {
            provider_id: self.provider_id.clone(),
            model_id: self.model_id.clone(),
            model_snapshot_hash: self
                .model_routes_hash
                .clone()
                .unwrap_or_else(|| self.model_capability_bundle_hash.clone()),
            system_prompt_hash: self.prompt_hash.clone(),
            role_prompt_hash: self.contract_hash.clone(),
            tool_schema_hash: self.model_capability_bundle_hash.clone(),
            runtime_config_hash: self.config_hash.clone(),
            context_strategy_hash: self.governance_bundle_hash.clone(),
        })
    }

    // 按 profile 校验必填文本、feed、模型日期、legacy 兼容字段和组件 bundle 完整性。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.code_revision.trim().is_empty()
            || self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || self.rust_toolchain.trim().is_empty()
            || !matches!(
                self.experiment_profile.as_str(),
                "fixture"
                    | "paper-engineering"
                    | "paper-research"
                    | "historical-eval"
                    | "legacy-v2"
            )
            || !matches!(self.market_data_feed.as_str(), "iex" | "sip")
        {
            return Err(DomainError::InvalidContentHash);
        }
        let research_profile = matches!(
            self.experiment_profile.as_str(),
            "paper-research" | "historical-eval"
        );
        if research_profile
            && (self.model_release_date.is_none()
                || self.model_knowledge_cutoff.is_none()
                || self.model_routes_hash.is_none())
        {
            return Err(DomainError::InvalidContentHash);
        }
        let legacy_identity = self.experiment_profile == "legacy-v2";
        if legacy_identity {
            if self.rust_toolchain != "unrecorded-legacy-v2"
                || !self.model_capability_hashes.is_empty()
                || !self.governance_component_hashes.is_empty()
                || self.model_capability_bundle_hash != empty_component_bundle_hash()
                || self.governance_bundle_hash != empty_component_bundle_hash()
                || self.model_release_date.is_some()
                || self.model_knowledge_cutoff.is_some()
                || self.historical_evaluation_condition.is_some()
                || self.model_routes_hash.is_some()
            {
                return Err(DomainError::InvalidContentHash);
            }
        } else {
            validate_hash_bundle(
                &self.model_capability_hashes,
                &self.model_capability_bundle_hash,
            )?;
            validate_hash_bundle(
                &self.governance_component_hashes,
                &self.governance_bundle_hash,
            )?;
            if !self.model_capability_hashes.contains_key("default") {
                return Err(DomainError::InvalidContentHash);
            }
        }
        // 研究 profile 必须声明所有影响治理行为的组件哈希，缺任何一项即拒绝。
        if research_profile {
            for component in [
                "instrument_evidence_registry",
                "market_clock_observed_session_policy",
                "forecast_calibrator_risk_model",
                "source_trust_injection_policy",
                "cost_model",
                "benchmark_definition",
                "claim_verifier",
                "execution_policy_bundle",
                "decision_validity_horizon_policy",
                "lesson_mandate_policy",
                "promotion_integrity_capability_retention",
                "model_qualification",
            ] {
                if !self.governance_component_hashes.contains_key(component) {
                    return Err(DomainError::InvalidContentHash);
                }
            }
        }
        match self.experiment_profile.as_str() {
            "historical-eval"
                if !matches!(
                    self.historical_evaluation_condition,
                    Some(
                        ExperimentCondition::Bright
                            | ExperimentCondition::IdentifierMasked
                            | ExperimentCondition::CalendarMasked
                            | ExperimentCondition::FullyMasked
                    )
                ) || self.market_data_feed != "sip" =>
            {
                return Err(DomainError::InvalidContentHash);
            }
            "paper-research"
                if self.historical_evaluation_condition
                    != Some(ExperimentCondition::PostCutoffForward)
                    || self.market_data_feed != "sip" =>
            {
                return Err(DomainError::InvalidContentHash);
            }
            "fixture" | "paper-engineering" | "legacy-v2"
                if self.historical_evaluation_condition.is_some() =>
            {
                return Err(DomainError::InvalidContentHash);
            }
            _ => {}
        }
        Ok(())
    }

    // legacy-v2 只哈希历史允许字段；新 profile 哈希完整序列化身份。
    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        if self.experiment_profile == "legacy-v2" {
            return content_hash_json(&serde_json::json!({
                "code_revision": self.code_revision,
                "cargo_lock_hash": self.cargo_lock_hash,
                "config_hash": self.config_hash,
                "provider_id": self.provider_id,
                "model_id": self.model_id,
                "prompt_hash": self.prompt_hash,
                "contract_hash": self.contract_hash,
                "topology_hash": self.topology_hash,
                "decision_policy_hash": self.decision_policy_hash,
                "execution_policy_hash": self.execution_policy_hash,
                "evaluation_policy_hash": self.evaluation_policy_hash,
                "market_data_feed": self.market_data_feed,
            }))
            .map_err(|_| DomainError::InvalidContentHash);
        }
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }
}

// serde 缺少旧字段时恢复历史 profile 名称。
fn legacy_experiment_profile() -> String {
    "legacy-v2".to_owned()
}

// serde 缺少旧字段时恢复历史 toolchain 标记。
fn legacy_rust_toolchain() -> String {
    "unrecorded-legacy-v2".to_owned()
}

// 计算空组件表的稳定 bundle 哈希，用作 legacy 默认值。
fn empty_component_bundle_hash() -> ContentHash {
    runtime_component_bundle_hash(&BTreeMap::new())
        .expect("serializing an empty component map is infallible")
}

fn validate_hash_bundle(
    components: &BTreeMap<String, ContentHash>,
    bundle_hash: &ContentHash,
) -> Result<(), DomainError> {
    // 组件表必须非空、名称非空，并且其规范化 JSON 哈希要等于 bundle_hash。
    if components.is_empty()
        || components.keys().any(|name| name.trim().is_empty())
        || content_hash_json(
            &serde_json::to_value(components).map_err(|_| DomainError::InvalidContentHash)?,
        )
        .map_err(|_| DomainError::InvalidContentHash)?
            != *bundle_hash
    {
        return Err(DomainError::InvalidContentHash);
    }
    Ok(())
}

pub fn runtime_component_bundle_hash(
    components: &BTreeMap<String, ContentHash>,
) -> Result<ContentHash, DomainError> {
    // 对按键有序的 BTreeMap 直接序列化并计算 bundle 身份哈希。
    content_hash_json(
        &serde_json::to_value(components).map_err(|_| DomainError::InvalidContentHash)?,
    )
    .map_err(|_| DomainError::InvalidContentHash)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeManifest {
    pub schema_version: u32,
    pub code_revision: String,
    pub cargo_lock_hash: ContentHash,
    pub config_hash: ContentHash,
    pub provider_id: String,
    pub model_id: String,
    pub prompt_hash: ContentHash,
    pub contract_hash: ContentHash,
    pub topology_hash: ContentHash,
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    #[serde(default = "legacy_experiment_profile")]
    pub experiment_profile: String,
    #[serde(default = "legacy_rust_toolchain")]
    pub rust_toolchain: String,
    #[serde(default)]
    pub model_release_date: Option<NaiveDate>,
    #[serde(default)]
    pub model_knowledge_cutoff: Option<NaiveDate>,
    #[serde(default)]
    pub historical_evaluation_condition: Option<ExperimentCondition>,
    #[serde(default)]
    pub model_routes_hash: Option<ContentHash>,
    #[serde(default)]
    pub model_capability_hashes: BTreeMap<String, ContentHash>,
    #[serde(default = "empty_component_bundle_hash")]
    pub model_capability_bundle_hash: ContentHash,
    #[serde(default)]
    pub governance_component_hashes: BTreeMap<String, ContentHash>,
    #[serde(default = "empty_component_bundle_hash")]
    pub governance_bundle_hash: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    pub market_data_feed: String,
    pub broker_account_id: String,
    pub maximum_notional: MoneyMicros,
    pub allowed_session_start: NaiveDate,
    pub allowed_session_end: NaiveDate,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl RuntimeManifest {
    // 复用 RuntimeIdentity 校验，再检查 broker、名义金额、日期窗口和模型 cutoff。
    pub fn validate(&self) -> Result<(), DomainError> {
        self.runtime_identity().validate()?;
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.broker_account_id.trim().is_empty()
            || self.maximum_notional.0 <= 0
            || self.allowed_session_end < self.allowed_session_start
            || self.expires_at <= self.created_at
            || self
                .model_release_date
                .is_some_and(|release| release > self.created_at.date_naive())
            || self
                .model_knowledge_cutoff
                .is_some_and(|cutoff| cutoff > self.created_at.date_naive())
            || (self.experiment_profile == "paper-research"
                && self
                    .model_knowledge_cutoff
                    .is_none_or(|cutoff| self.allowed_session_start <= cutoff))
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    // 校验后按 profile 选择 legacy 字段投影或完整 manifest 序列化来计算哈希。
    pub fn manifest_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        if self.experiment_profile == "legacy-v2" {
            return content_hash_json(&serde_json::json!({
                "schema_version": self.schema_version,
                "code_revision": self.code_revision,
                "cargo_lock_hash": self.cargo_lock_hash,
                "config_hash": self.config_hash,
                "provider_id": self.provider_id,
                "model_id": self.model_id,
                "prompt_hash": self.prompt_hash,
                "contract_hash": self.contract_hash,
                "topology_hash": self.topology_hash,
                "decision_policy_hash": self.decision_policy_hash,
                "execution_policy_hash": self.execution_policy_hash,
                "evaluation_policy_hash": self.evaluation_policy_hash,
                "market_data_feed": self.market_data_feed,
                "broker_account_id": self.broker_account_id,
                "maximum_notional": self.maximum_notional,
                "allowed_session_start": self.allowed_session_start,
                "allowed_session_end": self.allowed_session_end,
                "expires_at": self.expires_at,
                "created_at": self.created_at,
            }))
            .map_err(|_| DomainError::InvalidContentHash);
        }
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    // 返回经过 manifest 校验的 RuntimeIdentity 哈希，供审批绑定。
    pub fn runtime_identity_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        self.runtime_identity().identity_hash()
    }

    // 从 manifest 字段复制出不含授权窗口/账户信息的运行身份快照。
    pub fn runtime_identity(&self) -> RuntimeIdentity {
        RuntimeIdentity {
            code_revision: self.code_revision.clone(),
            cargo_lock_hash: self.cargo_lock_hash.clone(),
            config_hash: self.config_hash.clone(),
            provider_id: self.provider_id.clone(),
            model_id: self.model_id.clone(),
            prompt_hash: self.prompt_hash.clone(),
            contract_hash: self.contract_hash.clone(),
            topology_hash: self.topology_hash.clone(),
            decision_policy_hash: self.decision_policy_hash.clone(),
            execution_policy_hash: self.execution_policy_hash.clone(),
            evaluation_policy_hash: self.evaluation_policy_hash.clone(),
            experiment_profile: self.experiment_profile.clone(),
            rust_toolchain: self.rust_toolchain.clone(),
            model_release_date: self.model_release_date,
            model_knowledge_cutoff: self.model_knowledge_cutoff,
            historical_evaluation_condition: self.historical_evaluation_condition,
            model_routes_hash: self.model_routes_hash.clone(),
            model_capability_hashes: self.model_capability_hashes.clone(),
            model_capability_bundle_hash: self.model_capability_bundle_hash.clone(),
            governance_component_hashes: self.governance_component_hashes.clone(),
            governance_bundle_hash: self.governance_bundle_hash.clone(),
            market_data_feed: self.market_data_feed.clone(),
        }
    }

    // 判断 session 在允许日期内且当前时刻尚未超过 expires_at。
    pub fn permits(&self, session: NaiveDate, now: DateTime<Utc>) -> bool {
        self.validate().is_ok()
            && session >= self.allowed_session_start
            && session <= self.allowed_session_end
            && now <= self.expires_at
    }

    // legacy-v2 只能历史读取；新 session 授权还必须通过普通 permits 检查。
    pub fn authorizes_new_session(&self, session: NaiveDate, now: DateTime<Utc>) -> bool {
        self.experiment_profile != "legacy-v2" && self.permits(session, now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperLaunchApproval {
    pub schema_version: u32,
    pub operator_identity: String,
    pub runtime_manifest: ArtifactRef,
    pub runtime_manifest_hash: ContentHash,
    pub scope: PaperApprovalScope,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    pub approved_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub qualification: ModelQualificationReport,
    pub mandate: MandateSnapshot,
    pub approval_hash: ContentHash,
}

impl PaperLaunchApproval {
    // 计算不含 approval_hash 自引用的审批内容哈希。
    pub fn unsigned_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "operator_identity": self.operator_identity,
            "runtime_manifest": self.runtime_manifest,
            "runtime_manifest_hash": self.runtime_manifest_hash,
            "scope": self.scope,
            "reason": self.reason,
            "behavior_bundle_hash": self.behavior_bundle_hash,
            "approved_at": self.approved_at,
            "expires_at": self.expires_at,
            "qualification": self.qualification,
            "mandate": self.mandate,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    // 校验审批身份、引用 kind、时间顺序、资格/mandate 和最终审批哈希。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.operator_identity.trim().is_empty()
            || self.reason.trim().is_empty()
            || self.runtime_manifest.kind != ArtifactKind::RuntimeManifest
            || self.expires_at <= self.approved_at
            || self.qualification.validate().is_err()
            || self.mandate.validate().is_err()
            || self.unsigned_hash()? != self.approval_hash
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchApprovalDecision {
    Approved,
    Rejected,
    NeedsMoreEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchApprovalReason {
    OutcomesMeetTarget,
    RiskWithinBounds,
    CanaryCompleted,
    ReconciliationClean,
    NonInferiorityPreserved,
    EvidenceComplete,
    DrawdownViolation,
    TailLossExceeded,
    RegressionDetected,
    IncompleteEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOutcomeResearchApproval {
    pub schema_version: u32,
    pub approval_id: ContentHash,
    pub behavior_bundle_hash: ContentHash,
    pub runtime_identity_hash: ContentHash,
    pub release_evidence_bundle: ArtifactRef,
    pub outcome_t1: ArtifactRef,
    pub outcome_t3: ArtifactRef,
    pub outcome_t5: ArtifactRef,
    pub learning_evaluation: ArtifactRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learning_transition: Option<ArtifactRef>,
    pub completed_canary: ArtifactRef,
    pub decision: ResearchApprovalDecision,
    pub reason_codes: BTreeSet<ResearchApprovalReason>,
    pub operator: String,
    pub approved_at: DateTime<Utc>,
}

impl PostOutcomeResearchApproval {
    // 先封存 approval_id，再验证完整的 T1/T3/T5、评估和 operator 信息。
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.approval_id = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    // 对审批涉及的行为、运行、Outcome、评估、学习和理由字段计算身份哈希。
    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "behavior_bundle_hash": self.behavior_bundle_hash,
            "runtime_identity_hash": self.runtime_identity_hash,
            "release_evidence_bundle": self.release_evidence_bundle,
            "outcome_t1": self.outcome_t1,
            "outcome_t3": self.outcome_t3,
            "outcome_t5": self.outcome_t5,
            "learning_evaluation": self.learning_evaluation,
            "learning_transition": self.learning_transition,
            "completed_canary": self.completed_canary,
            "decision": self.decision,
            "reason_codes": self.reason_codes,
            "operator": self.operator,
            "approved_at": self.approved_at,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    // 检查 schema、哈希、operator、三个 Outcome、Evaluation 和非空理由集合。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.approval_id != self.identity_hash()?
            || self.operator.trim().is_empty()
            || self.outcome_t1.kind != ArtifactKind::Outcome
            || self.outcome_t3.kind != ArtifactKind::Outcome
            || self.outcome_t5.kind != ArtifactKind::Outcome
            || self.learning_evaluation.kind != ArtifactKind::Evaluation
            || self.reason_codes.is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "post_outcome_research_approval",
            });
        }
        Ok(())
    }
}
