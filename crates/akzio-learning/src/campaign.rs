//! Canary campaign comparison and fenced state transition.

use std::collections::BTreeSet;

use thiserror::Error;

use akzio_domain::{
    content_hash_json, CanaryCalibrationReport, CanaryCampaignStatus, CanaryCohortEvaluation,
    CanaryCohortManifest, CanaryPairedObservation, CanaryPairedSubjectMetrics,
    CanaryPromotionPolicy, CanarySubjectKind, CanaryVerdict, CandidatePolicyState, ContentHash,
    ForecastScore, OutcomeHorizon, PolicyState, PolicySubject, DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{CanaryCampaignHead, DaemonLease, Store, StoreError};
use chrono::{DateTime, Utc};

use crate::evaluation::{aggregate_calibration_report, AKZIO_MIN_CALIBRATION_SAMPLES};

const PPM_ONE: u32 = 1_000_000;

// 文件导读：这里把同一 Canary cohort 的三类 subject 与 T+1/T+3/T+5 观察汇总成
// 一个有版本和哈希的评估；它只决定 canary verdict，真正的 Policy head 变更仍由 Store
// 的带 lease 事务负责。daemon/outcome/canary 先记录配对观察，再用 verdict 评估各 subject，
// 最后调用 apply_cohort_evaluation 完成 campaign 状态迁移。

#[derive(Debug, Error)]
pub enum CanaryError {
    #[error(transparent)]
    Domain(#[from] akzio_domain::DomainError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("canary cohort policy differs from the immutable manifest")]
    PolicyDrift,
    #[error("canary cohort observation does not match {0}")]
    CohortMismatch(&'static str),
    #[error("canary cohort contains a duplicate session/horizon observation")]
    DuplicateObservation,
}

pub fn evaluate_canary_cohort(
    manifest: &CanaryCohortManifest,
    policy: &CanaryPromotionPolicy,
    observations: &[CanaryPairedObservation],
    evaluated_at: DateTime<Utc>,
) -> Result<CanaryCohortEvaluation, CanaryError> {
    // 输入均为借用：manifest/policy 冻结 cohort 和阈值；observations 应由上层从
    // Store 获取，本纯函数不验证其持久化来源或提交 Policy head，错误经 ? 返回。
    manifest.validate()?;
    policy.validate()?;
    let policy_hash = policy.identity_hash();
    if manifest.promotion_policy_hash != policy_hash {
        return Err(CanaryError::PolicyDrift);
    }

    let mut identities = BTreeSet::new();
    let mut market_days = BTreeSet::new();
    let mut covered_regimes = BTreeSet::new();
    let mut paired_sessions_by_horizon = [0_u64; 3];
    let mut rollback = false;
    let mut confidence_insufficient = false;
    let mut required_metric_unmeasured = false;
    let mut utility_sums = [[0_i128; 3]; 3];
    let mut utility_counts = [[0_u64; 3]; 3];
    let mut calibration_scores: [[[Vec<ForecastScore>; 2]; 3]; 3] =
        std::array::from_fn(|_| std::array::from_fn(|_| std::array::from_fn(|_| Vec::new())));
    let mut observation_hashes = Vec::with_capacity(observations.len());

    // session_key 与 horizon 共同构成唯一观察身份；同一交易 Session 的三个窗口分别
    // 计数，market_days/regime 则用于检查跨日和跨市场状态的覆盖，而不是把缺失窗口当成 0。
    for observation in observations {
        observation.validate()?;
        validate_observation_manifest(manifest, observation)?;
        if !identities.insert((observation.session_key.clone(), observation.horizon)) {
            return Err(CanaryError::DuplicateObservation);
        }
        market_days.insert(observation.market_day);
        covered_regimes.insert(observation.regime.clone());
        let horizon_index = horizon_index(observation.horizon);
        paired_sessions_by_horizon[horizon_index] =
            paired_sessions_by_horizon[horizon_index].saturating_add(1);
        observation_hashes.push(observation.identity_hash());

        for (subject_index, subject) in [
            &observation.contract,
            &observation.topology,
            &observation.bundle,
        ]
        .into_iter()
        .enumerate()
        {
            // utility 以 ppm 累加后再与样本数比较；forecast score 保留 parent/candidate
            // 的逐资产分数，下面只有达到最小样本数才会形成 calibration report。
            rollback |= subject_requires_rollback(subject, policy);
            required_metric_unmeasured |= subject_required_metric_unmeasured(subject);
            if let Some(score) = subject.parent.forecast_score {
                calibration_scores[subject_index][horizon_index][0].push(score);
            }
            if let Some(score) = subject.candidate.forecast_score {
                calibration_scores[subject_index][horizon_index][1].push(score);
            }
            utility_sums[subject_index][horizon_index] += i128::from(
                subject
                    .candidate
                    .cost_adjusted_utility_ppm
                    .saturating_sub(subject.parent.cost_adjusted_utility_ppm),
            );
            utility_counts[subject_index][horizon_index] =
                utility_counts[subject_index][horizon_index].saturating_add(1);
        }
    }

    observation_hashes.sort();
    // 排序后再哈希，使输入观察顺序不影响 observation_set_hash；这里的 expect 依赖
    // ContentHash 列表可 JSON 序列化，若该固定数据类型序列化失败会 panic，而非返回 CanaryError。
    let observation_set_hash = content_hash_json(&serde_json::json!(observation_hashes))
        .expect("canary observation hashes serialize");
    // 覆盖度分别检查三个 horizon、不同交易日和必需 regime；任一不足即 Defer 条件，
    // 不会把没有观察的 session 当成零收益样本。
    let coverage_insufficient = paired_sessions_by_horizon
        .iter()
        .zip(policy.required_paired_sessions_per_horizon)
        .any(|(actual, required)| *actual < required)
        || (market_days.len() as u64) < policy.minimum_distinct_market_days
        || !policy.required_regimes.is_subset(&covered_regimes);
    let utility_insufficient = utility_sums
        .iter()
        .zip(utility_counts.iter())
        .flat_map(|(sums, counts)| sums.iter().zip(counts.iter()))
        .any(|(sum, count)| {
            *count == 0
                || *sum
                    < i128::from(policy.minimum_cost_adjusted_utility_delta_ppm)
                        * i128::from(*count)
        });
    let mut calibration_reports = Vec::with_capacity(9);
    // 9 个 report = 3 个 subject × 3 个 Outcome horizon；样本不足时 report 为 None，
    // confidence_insufficient 会让 verdict 保持 Defer，不能用单条 T+1/T+3/T+5 观察晋级。
    for (subject_index, subject) in [
        CanarySubjectKind::Contract,
        CanarySubjectKind::Topology,
        CanarySubjectKind::Bundle,
    ]
    .into_iter()
    .enumerate()
    {
        for (horizon_index, horizon) in OutcomeHorizon::ALL.into_iter().enumerate() {
            let parent = aggregate_calibration_report(
                calibration_scores[subject_index][horizon_index][0]
                    .iter()
                    .copied(),
                AKZIO_MIN_CALIBRATION_SAMPLES,
            );
            let candidate = aggregate_calibration_report(
                calibration_scores[subject_index][horizon_index][1]
                    .iter()
                    .copied(),
                AKZIO_MIN_CALIBRATION_SAMPLES,
            );
            confidence_insufficient |= [parent, candidate].into_iter().any(|report| {
                report.is_none_or(|report| {
                    PPM_ONE.saturating_sub(report.expected_calibration_error_ppm)
                        < policy.minimum_confidence_ppm
                })
            });
            calibration_reports.push(CanaryCalibrationReport {
                subject,
                horizon,
                parent,
                candidate,
            });
        }
    }
    let integrity_failed = manifest
        .promotion_integrity
        .as_ref()
        .is_some_and(|evidence| !evidence.permits_promotion());
    let retention_failed = manifest
        .capability_retention
        .as_ref()
        .is_some_and(|matrix| !matrix.permits_promotion());
    let governance_missing =
        manifest.promotion_integrity.is_none() || manifest.capability_retention.is_none();
    // 已测出的退化才允许 Rollback；coverage、置信度、必需指标或治理材料缺失属于
    // Defer；指标完整但 utility 未达到阈值才是 Hold。未知状态不会被解释成通过。
    let verdict = if rollback || integrity_failed || retention_failed {
        CanaryVerdict::Rollback
    } else if coverage_insufficient
        || confidence_insufficient
        || required_metric_unmeasured
        || manifest.search_bias_certificate.is_none()
        || governance_missing
    {
        CanaryVerdict::Defer
    } else if utility_insufficient {
        CanaryVerdict::Hold
    } else {
        CanaryVerdict::Advance
    };

    Ok(CanaryCohortEvaluation {
        schema_version: DOMAIN_SCHEMA_VERSION,
        evaluation_id: ContentHash::of_bytes(b"pending-canary-evaluation"),
        cohort_id: manifest.cohort_id.clone(),
        promotion_policy_hash: policy_hash,
        observation_set_hash,
        verdict,
        paired_sessions_by_horizon,
        distinct_market_days: market_days.len() as u64,
        covered_regimes,
        calibration_reports,
        search_bias_certificate: manifest.search_bias_certificate.clone(),
        evaluated_at,
    }
    .seal())
}

fn validate_observation_manifest(
    manifest: &CanaryCohortManifest,
    observation: &CanaryPairedObservation,
) -> Result<(), CanaryError> {
    // 逐一绑定 manifest 中的 cohort、日期/market regime、资产范围、成本模型、交易日历和数据集；
    // 首个不匹配就返回具体 CohortMismatch，调用者不能将其他 cohort 的观测混入本次评估。
    if observation.cohort_id != manifest.cohort_id {
        return Err(CanaryError::CohortMismatch("cohort identity"));
    }
    if observation.market_day < manifest.observation_start
        || observation.market_day > manifest.observation_end
        || manifest.regime_for(observation.market_day) != Some(observation.regime.as_str())
    {
        return Err(CanaryError::CohortMismatch("observation window or regime"));
    }
    if observation.asset_universe != manifest.asset_universe {
        return Err(CanaryError::CohortMismatch("asset universe"));
    }
    if observation.cost_model != manifest.cost_model {
        return Err(CanaryError::CohortMismatch("cost model"));
    }
    if observation.market_calendar_id != manifest.market_calendar_id {
        return Err(CanaryError::CohortMismatch("market calendar"));
    }
    if observation.generation_dataset_id != manifest.generation_dataset_id
        || observation.promotion_dataset_id != manifest.promotion_dataset_id
    {
        return Err(CanaryError::CohortMismatch("dataset identity"));
    }
    Ok(())
}

/// Rollback fires only on measured degradation. Unmeasured risk recall is
/// routed to `Defer` by `subject_risk_recall_unmeasured`, never to `Rollback`.
fn subject_requires_rollback(
    subject: &CanaryPairedSubjectMetrics,
    policy: &CanaryPromotionPolicy,
) -> bool {
    // 只有已测量的候选退化会触发回滚；Option::is_some_and 和 Some/Some 比较避免把 None
    // 误当成 0。与父版本对比的各类风险/过程/回撤阈值共同形成 fail-closed 风险判断。
    subject
        .candidate
        .evidence_completeness_ppm
        .is_some_and(|value| value < policy.minimum_evidence_completeness_ppm)
        || match (
            subject.candidate.evidence_completeness_ppm,
            subject.parent.evidence_completeness_ppm,
        ) {
            (Some(candidate), Some(parent)) => candidate < parent,
            _ => false,
        }
        || subject.candidate.cost_adjusted_utility_ppm < subject.parent.cost_adjusted_utility_ppm
        || subject
            .candidate
            .risk_recall_ppm
            .is_some_and(|value| value < policy.minimum_risk_recall_ppm)
        || match (
            subject.candidate.risk_recall_ppm,
            subject.parent.risk_recall_ppm,
        ) {
            (Some(candidate), Some(parent)) => candidate < parent,
            _ => false,
        }
        || subject
            .candidate
            .process_quality_ppm
            .is_some_and(|value| value < policy.minimum_process_quality_ppm)
        || match (
            subject.candidate.process_quality_ppm,
            subject.parent.process_quality_ppm,
        ) {
            (Some(candidate), Some(parent)) => candidate < parent,
            _ => false,
        }
        || match (subject.candidate.drawdown_ppm, subject.parent.drawdown_ppm) {
            (Some(candidate), Some(parent)) => {
                candidate > parent.saturating_add(policy.maximum_drawdown_delta_ppm)
            }
            _ => false,
        }
        || match (
            subject.candidate.tail_loss_ppm,
            subject.parent.tail_loss_ppm,
        ) {
            (Some(candidate), Some(parent)) => {
                candidate > parent.saturating_add(policy.maximum_tail_loss_delta_ppm)
            }
            _ => false,
        }
}

const fn subject_required_metric_unmeasured(subject: &CanaryPairedSubjectMetrics) -> bool {
    // 必需指标任一缺失即 true，由上层映射到 Defer；未知测量不会被视作合格或退化。
    !subject.risk_recall_is_measured()
        || subject.parent.evidence_completeness_ppm.is_none()
        || subject.candidate.evidence_completeness_ppm.is_none()
        || subject.parent.process_quality_ppm.is_none()
        || subject.candidate.process_quality_ppm.is_none()
        || subject.parent.drawdown_ppm.is_none()
        || subject.candidate.drawdown_ppm.is_none()
        || subject.parent.tail_loss_ppm.is_none()
        || subject.candidate.tail_loss_ppm.is_none()
}

const fn horizon_index(horizon: OutcomeHorizon) -> usize {
    // 将有限枚举映射到固定数组下标；match 穷尽所有 horizon，新增变体会要求显式补齐映射。
    // 固定数组顺序与 OutcomeHorizon::ALL 一致，避免把 T+1/T+3/T+5 的计数错位。
    match horizon {
        OutcomeHorizon::T1 => 0,
        OutcomeHorizon::T3 => 1,
        OutcomeHorizon::T5 => 2,
    }
}

#[derive(Debug, Clone)]
pub struct CanaryCampaignRuntime {
    store: Store,
}

impl CanaryCampaignRuntime {
    // Store 按值移入运行时并由字段持有；minimum_ppm 只校验范围，不在构造时创建 campaign。
    pub fn new(store: Store, minimum_ppm: u32) -> Result<Self, CanaryError> {
        // minimum_ppm 只是本运行时接收的边界校验；campaign 状态不会在构造时写入 Store。
        if minimum_ppm > PPM_ONE {
            return Err(CanaryError::Domain(
                akzio_domain::DomainError::InvalidBudget {
                    field: "canary.minimum_ppm",
                },
            ));
        }
        Ok(Self { store })
    }

    // 按 verdict 与 campaign status 推导 subject 对应的目标 PolicyState；Advance/ Rollback
    // 只返回建议状态，实际 head 迁移由后续 Store 事务完成，Memory subject 保持当前状态。
    pub fn target_policy_state(
        &self,
        subject: &PolicySubject,
        current: PolicyState,
        status: CanaryCampaignStatus,
        verdict: CanaryVerdict,
    ) -> PolicyState {
        match verdict {
            CanaryVerdict::Advance => status
                .policy_state()
                .map(|state| match subject {
                    PolicySubject::Contract(_) => PolicyState::Contract(state),
                    PolicySubject::Topology(_) => PolicyState::Topology(state),
                    PolicySubject::Memory(_) => current,
                })
                .unwrap_or(current),
            CanaryVerdict::Rollback => match subject {
                PolicySubject::Contract(_) => {
                    PolicyState::Contract(CandidatePolicyState::Candidate)
                }
                PolicySubject::Topology(_) => {
                    PolicyState::Topology(CandidatePolicyState::Candidate)
                }
                PolicySubject::Memory(_) => current,
            },
            CanaryVerdict::Hold | CanaryVerdict::Defer => current,
        }
    }

    // 此处只借用 lease、campaign ID 和已密封 evaluation，将一次状态迁移交给 Store；
    // Ok 仅说明 campaign transition 返回了 head，不代表整个 Outcome worker 后续工作都完成。
    pub fn apply_cohort_evaluation(
        &self,
        lease: &DaemonLease,
        campaign_id: &akzio_domain::ContentHash,
        status: CanaryCampaignStatus,
        evaluation: &CanaryCohortEvaluation,
        now: DateTime<Utc>,
    ) -> Result<CanaryCampaignHead, CanaryError> {
        // 评估结果已经密封；这里不直接改内存，而是把 lease、campaign、status 和
        // evaluation 一并交给 Store 的受保护状态迁移，失败时不应留下半个 transition。
        Ok(self.store.transition_canary_campaign_with_evaluation(
            lease,
            campaign_id,
            status,
            evaluation,
            now,
        )?)
    }
}
