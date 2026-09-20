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
    let observation_set_hash = content_hash_json(&serde_json::json!(observation_hashes))
        .expect("canary observation hashes serialize");
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
    pub fn new(store: Store, minimum_ppm: u32) -> Result<Self, CanaryError> {
        if minimum_ppm > PPM_ONE {
            return Err(CanaryError::Domain(
                akzio_domain::DomainError::InvalidBudget {
                    field: "canary.minimum_ppm",
                },
            ));
        }
        Ok(Self { store })
    }

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

    pub fn apply_cohort_evaluation(
        &self,
        lease: &DaemonLease,
        campaign_id: &akzio_domain::ContentHash,
        status: CanaryCampaignStatus,
        evaluation: &CanaryCohortEvaluation,
        now: DateTime<Utc>,
    ) -> Result<CanaryCampaignHead, CanaryError> {
        Ok(self.store.transition_canary_campaign_with_evaluation(
            lease,
            campaign_id,
            status,
            evaluation,
            now,
        )?)
    }
}
