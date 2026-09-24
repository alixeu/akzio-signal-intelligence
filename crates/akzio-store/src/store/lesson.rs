// 文件导读：Lesson 采用独立的惰性表和 canonical CAS Artifact；证据账本只记录
// 已封存 Outcome 对 Lesson 的观察关联，和 Policy 激活、Paper 执行不是同一状态。
// 数据写入/生命周期迁移由 write.rs 执行，queries_verify.rs 负责读取与 Doctor 校验，
// helpers.rs 只探测表形状；先读 LessonEvidenceMetrics 转换，再看 Store API。
use super::*;

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, LessonAttribution, LessonEvidence,
    LessonGovernance, LessonOrigin,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LessonEvidenceMetrics {
    pub utility_ppm_by_horizon: [i64; 3],
    pub calibration_ppm_by_horizon: [Option<u32>; 3],
}

impl From<&LessonEvidence> for LessonEvidenceMetrics {
    // `From<&LessonEvidence>` 实现标准转换 trait：调用方可用 `LessonEvidenceMetrics::from(&evidence)`；
    // 输入只借用完整 LessonEvidence，复制固定长度 metrics 数组到 SQL 列投影；
    // lesson/context/outcome identity 仍存于单独索引列，不在 metrics_json 中重复编码。
    fn from(evidence: &LessonEvidence) -> Self {
        Self {
            utility_ppm_by_horizon: evidence.utility_ppm_by_horizon,
            calibration_ppm_by_horizon: evidence.calibration_ppm_by_horizon,
        }
    }
}

// 将 SQL identity/attribution/metrics/time 列重建成领域 LessonEvidence；未知 attribution、
// 非法 hash、JSON、时间或 validate 错误都会阻止该行返回，不套用默认值。
#[allow(clippy::too_many_arguments)]
pub(super) fn lesson_evidence_from_columns(
    lesson_id: String,
    lesson_artifact_id: String,
    decision_context_artifact_id: String,
    outcome_artifact_id: String,
    attribution: String,
    metrics_json: String,
    recorded_at: String,
) -> StoreResult<LessonEvidence> {
    let attribution = match attribution.as_str() {
        "applied" => LessonAttribution::Applied,
        "rejected" => LessonAttribution::Rejected,
        _ => {
            return Err(StoreError::Integrity(format!(
                "invalid lesson evidence attribution {attribution}"
            )));
        }
    };
    let metrics: LessonEvidenceMetrics = serde_json::from_str(&metrics_json)?;
    let evidence = LessonEvidence {
        schema_version: DOMAIN_SCHEMA_VERSION,
        lesson_id: LessonId(lesson_id),
        lesson_artifact: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::new(lesson_artifact_id)?),
            kind: ArtifactKind::Lesson,
        },
        decision_context: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::new(decision_context_artifact_id)?),
            kind: ArtifactKind::DecisionContext,
        },
        outcome: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::new(outcome_artifact_id)?),
            kind: ArtifactKind::Outcome,
        },
        attribution,
        utility_ppm_by_horizon: metrics.utility_ppm_by_horizon,
        calibration_ppm_by_horizon: metrics.calibration_ppm_by_horizon,
        recorded_at: parse_time(&recorded_at)?,
    };
    evidence.validate()?;
    Ok(evidence)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLesson {
    pub artifact: Artifact,
    pub lesson: Lesson,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LessonWriteResult {
    pub lesson: StoredLesson,
    pub newly_created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LessonUsage {
    pub context_manifests: u64,
    pub decision_contexts: u64,
    pub latest_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LessonRevalidationScan {
    pub scanned_active: u64,
    pub contested: u64,
}

include!("lesson/write.rs");
include!("lesson/queries_verify.rs");
include!("lesson/helpers.rs");
