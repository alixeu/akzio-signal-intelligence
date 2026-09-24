//! Pure, deterministic model-qualification assembly for sealed offline results.
//!
//! This module does not run a model, acquire evidence, contact a broker, or
//! persist state. Callers must first materialize each stage evaluation as a
//! governed artifact, then pass only its immutable reference and fixed result.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    content_hash_json, ArtifactKind, ArtifactRef, CapabilityRetentionMatrix,
    CapabilityScenarioResult, ContentHash, MetricVector, ModelQualificationEvidence,
    ModelQualificationKey, ModelQualificationReport,
};
pub use akzio_domain::{QualificationStage, QualificationStageReceipt};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// 文件导读：qualification 是纯离线、确定性的 sealed 结果组装；它不运行模型、不采集
// 证据、不访问 Broker、不写 Store，结果也不能替代真实 Paper 或 T+1/T+3/T+5 验收。

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationScenarioObservation {
    pub scenario_id: String,
    pub stage: QualificationStage,
    pub critical: bool,
    pub champion_passed: bool,
    pub challenger_passed: bool,
    pub output_fingerprint: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflineModelQualificationInput {
    pub key: ModelQualificationKey,
    pub frozen_context_manifest: ArtifactRef,
    pub champion_snapshot: ContentHash,
    pub challenger_snapshot: ContentHash,
    pub rollback_snapshot: ContentHash,
    pub observed_rollback_snapshot: ContentHash,
    pub required_scenarios: BTreeSet<String>,
    pub scenarios: Vec<QualificationScenarioObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<QualificationStageReceipt>,
    pub stage_evaluations: BTreeMap<QualificationStage, ArtifactRef>,
    pub approved_by: String,
    pub qualified_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflineModelQualificationResult {
    pub report: ModelQualificationReport,
    pub capability_retention: CapabilityRetentionMatrix,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QualificationRunError {
    #[error("invalid model qualification input: {0}")]
    InvalidInput(&'static str),
    #[error("qualification stage {0:?} is missing")]
    MissingStage(QualificationStage),
    #[error("qualification scenario id is duplicated: {0}")]
    DuplicateScenario(String),
    #[error("required qualification scenario is missing: {0}")]
    MissingRequiredScenario(String),
    #[error("qualification evidence closure is invalid")]
    InvalidEvidenceClosure,
    #[error("assembled qualification report is invalid")]
    InvalidReport,
}

pub fn run_offline_model_qualification(
    input: &OfflineModelQualificationInput,
) -> Result<OfflineModelQualificationResult, QualificationRunError> {
    // 先完成输入/证据闭包校验，再按 stage/scenario 排序，保证 fingerprint 和 report 的
    // 内容不受调用者传入顺序影响。
    validate_input(input)?;

    let mut scenarios = input.scenarios.clone();
    scenarios.sort_by(|left, right| {
        left.stage
            .cmp(&right.stage)
            .then_with(|| left.scenario_id.cmp(&right.scenario_id))
    });
    let capability_retention = CapabilityRetentionMatrix {
        champion_snapshot: input.champion_snapshot.clone(),
        challenger_snapshot: input.challenger_snapshot.clone(),
        rollback_snapshot: input.rollback_snapshot.clone(),
        rollback_verified: input.rollback_snapshot == input.observed_rollback_snapshot,
        required_scenarios: input.required_scenarios.clone(),
        scenarios: scenarios
            .iter()
            .map(|scenario| {
                let receipt = input
                    .receipts
                    .iter()
                    .find(|r| r.scenario_id == scenario.scenario_id)
                    .map(|r| ArtifactRef {
                        artifact_id: akzio_domain::ArtifactId(r.receipt_id.clone()),
                        kind: akzio_domain::ArtifactKind::QualificationStageReceipt,
                    });
                let measured = input
                    .receipts
                    .iter()
                    .find(|r| r.scenario_id == scenario.scenario_id)
                    .map(|r| r.measured_metrics.clone())
                    .unwrap_or_default();
                let challenger_metrics = MetricVector::from_map(measured);
                CapabilityScenarioResult {
                    scenario_id: scenario.scenario_id.clone(),
                    critical: scenario.critical,
                    champion_metrics: MetricVector::default(),
                    challenger_metrics: challenger_metrics.clone(),
                    delta_metrics: challenger_metrics,
                    champion_passed: scenario.champion_passed,
                    challenger_passed: scenario.challenger_passed,
                    noninferiority_passed: scenario.challenger_passed,
                    evidence_receipt: receipt,
                }
            })
            .collect(),
    };
    // receipt 只把已提交的 measured_metrics 绑定到 challenger；没有 receipt 的场景保留
    // 默认空指标，但其 deterministic challenger verdict 仍由场景观察和完整闭包校验约束。
    capability_retention
        .validate()
        .map_err(|_| QualificationRunError::InvalidInput("capability_retention"))?;

    let evidence = stage_evidence(input)?;
    let behavior_fingerprint = content_hash_json(&serde_json::json!({
        "key": input.key,
        "frozen_context_manifest": input.frozen_context_manifest,
        "champion_snapshot": input.champion_snapshot,
        "challenger_snapshot": input.challenger_snapshot,
        "rollback_snapshot": input.rollback_snapshot,
        "observed_rollback_snapshot": input.observed_rollback_snapshot,
        "required_scenarios": input.required_scenarios,
        "scenarios": scenarios,
        "stage_evaluations": input.stage_evaluations,
    }))
    .map_err(|_| QualificationRunError::InvalidInput("behavior_fingerprint"))?;
    let critical_regressions = scenarios
        .iter()
        .filter(|scenario| {
            scenario.critical && scenario.champion_passed && !scenario.challenger_passed
        })
        .count()
        .try_into()
        .map_err(|_| QualificationRunError::InvalidInput("critical_regressions"))?;
    let stage_passed = |stage| {
        // 每个阶段要求该阶段所有场景的 challenger_passed；Canary 还必须通过 capability
        // retention，避免单一成功场景掩盖关键回归或 rollback 失败。
        scenarios
            .iter()
            .filter(|scenario| scenario.stage == stage)
            .all(|scenario| scenario.challenger_passed)
    };

    let mut report = ModelQualificationReport {
        key: input.key.clone(),
        behavior_fingerprint,
        evidence,
        replay_passed: stage_passed(QualificationStage::Replay),
        adversarial_passed: stage_passed(QualificationStage::Adversarial),
        execution_simulation_passed: stage_passed(QualificationStage::ExecutionSimulation),
        shadow_passed: stage_passed(QualificationStage::Shadow),
        canary_passed: stage_passed(QualificationStage::Canary)
            && capability_retention.permits_promotion(),
        critical_regressions,
        approved_by: input.approved_by.clone(),
        qualified_at: input.qualified_at,
        expires_at: input.expires_at,
        report_hash: ContentHash::of_bytes(b"pending-offline-qualification"),
    };
    report.report_hash = report.unsigned_hash();
    report
        .validate()
        .map_err(|_| QualificationRunError::InvalidReport)?;

    Ok(OfflineModelQualificationResult {
        report,
        capability_retention,
    })
}

fn validate_input(input: &OfflineModelQualificationInput) -> Result<(), QualificationRunError> {
    // 必需 scenario、五个有序 stage evaluation、receipt identity 和观察 verdict 必须闭合；
    // 缺一个就返回明确错误，不能用“阶段没有场景所以 all=true”伪造通过。
    input
        .key
        .validate()
        .map_err(|_| QualificationRunError::InvalidInput("qualification_key"))?;
    if input.approved_by.trim().is_empty() || input.expires_at <= input.qualified_at {
        return Err(QualificationRunError::InvalidInput("approval_window"));
    }
    if input.scenarios.is_empty() || input.scenarios.len() > 256 {
        return Err(QualificationRunError::InvalidInput("scenarios"));
    }
    if input.required_scenarios.is_empty()
        || input
            .required_scenarios
            .iter()
            .any(|scenario| scenario.trim().is_empty())
    {
        return Err(QualificationRunError::InvalidInput("required_scenarios"));
    }

    let mut scenario_ids = BTreeSet::new();
    for scenario in &input.scenarios {
        if scenario.scenario_id.trim().is_empty() {
            return Err(QualificationRunError::InvalidInput("scenario_id"));
        }
        if !scenario_ids.insert(scenario.scenario_id.clone()) {
            return Err(QualificationRunError::DuplicateScenario(
                scenario.scenario_id.clone(),
            ));
        }
    }
    if let Some(missing) = input
        .required_scenarios
        .difference(&scenario_ids)
        .next()
        .cloned()
    {
        return Err(QualificationRunError::MissingRequiredScenario(missing));
    }
    for stage in QualificationStage::ORDERED {
        if !input
            .scenarios
            .iter()
            .any(|scenario| scenario.stage == stage)
            || !input.stage_evaluations.contains_key(&stage)
        {
            return Err(QualificationRunError::MissingStage(stage));
        }
    }
    if input.stage_evaluations.len() != QualificationStage::ORDERED.len() {
        return Err(QualificationRunError::InvalidEvidenceClosure);
    }
    for receipt in &input.receipts {
        // receipt 的 qualification key、双 snapshot 和 frozen manifest 必须与本次输入相同；
        // deterministic verdict 若与 scenario 不同，也属于证据闭包错误。
        receipt
            .validate()
            .map_err(|_| QualificationRunError::InvalidEvidenceClosure)?;
        if receipt.qualification_key_hash != input.key.identity_hash()
            || receipt.challenger_snapshot != input.challenger_snapshot
            || receipt.champion_snapshot != input.champion_snapshot
            || receipt.frozen_context_manifest != input.frozen_context_manifest
        {
            return Err(QualificationRunError::InvalidEvidenceClosure);
        }
        if let Some(matching_obs) = input
            .scenarios
            .iter()
            .find(|sc| sc.scenario_id == receipt.scenario_id && sc.stage == receipt.stage)
        {
            if matching_obs.challenger_passed != receipt.deterministic_verdict.is_pass() {
                return Err(QualificationRunError::InvalidEvidenceClosure);
            }
        }
    }
    stage_evidence(input)?;
    Ok(())
}

fn stage_evidence(
    input: &OfflineModelQualificationInput,
) -> Result<ModelQualificationEvidence, QualificationRunError> {
    // 五个 ArtifactRef 都从同一 frozen_context_manifest 资格链取得，并要求引用 kind 正确
    // 且无重复；这里仍只组装引用，不读取 BLOB 或改变任何 Store 状态。
    let get = |stage| {
        input
            .stage_evaluations
            .get(&stage)
            .cloned()
            .ok_or(QualificationRunError::MissingStage(stage))
    };
    let evidence = ModelQualificationEvidence {
        frozen_context_manifest: input.frozen_context_manifest.clone(),
        replay_evaluation: get(QualificationStage::Replay)?,
        adversarial_evaluation: get(QualificationStage::Adversarial)?,
        execution_simulation_evaluation: get(QualificationStage::ExecutionSimulation)?,
        shadow_evaluation: get(QualificationStage::Shadow)?,
        canary_evaluation: get(QualificationStage::Canary)?,
    };
    let references = evidence.references();
    if evidence.validate().is_err()
        || references
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len()
            != references.len()
        || evidence.frozen_context_manifest.kind != ArtifactKind::ContextManifest
    {
        return Err(QualificationRunError::InvalidEvidenceClosure);
    }
    Ok(evidence)
}
