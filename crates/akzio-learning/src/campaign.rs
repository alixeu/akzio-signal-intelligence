//! Canary campaign comparison and fenced state transition.

use std::collections::BTreeSet;

use thiserror::Error;

use akzio_domain::{
    content_hash_json, CanaryCampaignStatus, CanaryCohortEvaluation, CanaryCohortManifest,
    CanaryPairedObservation, CanaryPairedSubjectMetrics, CanaryPromotionPolicy, CanaryVerdict,
    CandidatePolicyState, ContentHash, Outcome, OutcomeHorizon, PolicyState, PolicySubject,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{CanaryCampaignHead, DaemonLease, StoreError, V2Store};
use chrono::{DateTime, Utc};

const PPM_ONE: u32 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanaryHorizonMetrics {
    pub evidence_completeness_ppm: u32,
    pub risk_recall_ppm: u32,
    pub utility_ppm: i64,
}

impl CanaryHorizonMetrics {
    pub fn from_outcome_window(window: &akzio_domain::OutcomeWindow) -> Self {
        Self {
            evidence_completeness_ppm: window.evidence_completeness_ppm,
            risk_recall_ppm: window.risk_recall_ppm.unwrap_or_default(),
            utility_ppm: window.utility_ppm,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanarySubjectComparison {
    pub parent: [CanaryHorizonMetrics; 3],
    pub candidate: [CanaryHorizonMetrics; 3],
}

impl CanarySubjectComparison {
    pub fn from_outcomes(parent: &Outcome, candidate: &Outcome) -> Result<Self, CanaryError> {
        parent.validate()?;
        candidate.validate()?;
        let parent = metrics_by_horizon(parent)?;
        let candidate = metrics_by_horizon(candidate)?;
        Ok(Self { parent, candidate })
    }

    pub fn verdict(&self, minimum_ppm: u32) -> CanaryVerdict {
        if self
            .candidate
            .iter()
            .zip(self.parent.iter())
            .any(|(candidate, parent)| {
                candidate.evidence_completeness_ppm < minimum_ppm
                    || candidate.risk_recall_ppm < minimum_ppm
                    || candidate.evidence_completeness_ppm < parent.evidence_completeness_ppm
                    || candidate.risk_recall_ppm < parent.risk_recall_ppm
                    || candidate.utility_ppm < parent.utility_ppm
            })
        {
            return CanaryVerdict::Rollback;
        }

        let parent_utility = self
            .parent
            .iter()
            .map(|metrics| i128::from(metrics.utility_ppm))
            .sum::<i128>();
        let candidate_utility = self
            .candidate
            .iter()
            .map(|metrics| i128::from(metrics.utility_ppm))
            .sum::<i128>();
        if candidate_utility > parent_utility {
            CanaryVerdict::Advance
        } else {
            CanaryVerdict::Hold
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryBundleComparison {
    pub contract: CanarySubjectComparison,
    pub topology: CanarySubjectComparison,
    pub bundle: CanarySubjectComparison,
}

impl CanaryBundleComparison {
    pub fn verdict(&self, minimum_ppm: u32) -> CanaryVerdict {
        let verdicts = [
            self.contract.verdict(minimum_ppm),
            self.topology.verdict(minimum_ppm),
            self.bundle.verdict(minimum_ppm),
        ];
        if verdicts.contains(&CanaryVerdict::Rollback) {
            CanaryVerdict::Rollback
        } else if verdicts.contains(&CanaryVerdict::Defer) {
            CanaryVerdict::Defer
        } else if verdicts
            .iter()
            .all(|verdict| *verdict == CanaryVerdict::Advance)
        {
            CanaryVerdict::Advance
        } else {
            CanaryVerdict::Hold
        }
    }
}

#[derive(Debug, Error)]
pub enum CanaryError {
    #[error(transparent)]
    Domain(#[from] akzio_domain::DomainError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("outcome is missing one of T+1/T+3/T+5 windows")]
    MissingHorizon,
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
    let mut utility_sums = [[0_i128; 3]; 3];
    let mut utility_counts = [[0_u64; 3]; 3];
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
            confidence_insufficient |= subject.parent.confidence_ppm
                < policy.minimum_confidence_ppm
                || subject.candidate.confidence_ppm < policy.minimum_confidence_ppm;
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
    let verdict = if rollback {
        CanaryVerdict::Rollback
    } else if coverage_insufficient || confidence_insufficient {
        CanaryVerdict::Defer
    } else if utility_insufficient {
        CanaryVerdict::Hold
    } else {
        CanaryVerdict::Advance
    };

    Ok(CanaryCohortEvaluation {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        evaluation_id: ContentHash::of_bytes(b"pending-canary-evaluation"),
        cohort_id: manifest.cohort_id.clone(),
        promotion_policy_hash: policy_hash,
        observation_set_hash,
        verdict,
        paired_sessions_by_horizon,
        distinct_market_days: market_days.len() as u64,
        covered_regimes,
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

fn subject_requires_rollback(
    subject: &CanaryPairedSubjectMetrics,
    policy: &CanaryPromotionPolicy,
) -> bool {
    subject.candidate.evidence_completeness_ppm < policy.minimum_evidence_completeness_ppm
        || subject.candidate.risk_recall_ppm < policy.minimum_risk_recall_ppm
        || subject.candidate.evidence_completeness_ppm < subject.parent.evidence_completeness_ppm
        || subject.candidate.risk_recall_ppm < subject.parent.risk_recall_ppm
        || subject.candidate.cost_adjusted_utility_ppm < subject.parent.cost_adjusted_utility_ppm
        || subject.candidate.drawdown_ppm
            > subject
                .parent
                .drawdown_ppm
                .saturating_add(policy.maximum_drawdown_delta_ppm)
        || subject.candidate.tail_loss_ppm
            > subject
                .parent
                .tail_loss_ppm
                .saturating_add(policy.maximum_tail_loss_delta_ppm)
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
    store: V2Store,
    minimum_ppm: u32,
}

impl CanaryCampaignRuntime {
    pub fn new(store: V2Store, minimum_ppm: u32) -> Result<Self, CanaryError> {
        if minimum_ppm > PPM_ONE {
            return Err(CanaryError::Domain(
                akzio_domain::DomainError::InvalidBudget {
                    field: "canary.minimum_ppm",
                },
            ));
        }
        Ok(Self { store, minimum_ppm })
    }

    pub fn minimum_ppm(&self) -> u32 {
        self.minimum_ppm
    }

    pub fn compare(&self, comparison: &CanaryBundleComparison) -> CanaryVerdict {
        comparison.verdict(self.minimum_ppm)
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

    pub fn apply_verdict(
        &self,
        lease: &DaemonLease,
        campaign_id: &akzio_domain::ContentHash,
        status: CanaryCampaignStatus,
        comparison: &CanaryBundleComparison,
        now: DateTime<Utc>,
    ) -> Result<CanaryCampaignHead, CanaryError> {
        let verdict = self.compare(comparison);
        Ok(self
            .store
            .transition_canary_campaign(lease, campaign_id, status, verdict, now)?)
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

fn metrics_by_horizon(outcome: &Outcome) -> Result<[CanaryHorizonMetrics; 3], CanaryError> {
    OutcomeHorizon::ALL
        .map(|horizon| {
            outcome
                .windows
                .iter()
                .find(|window| window.horizon == horizon)
                .map(CanaryHorizonMetrics::from_outcome_window)
                .ok_or(CanaryError::MissingHorizon)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| CanaryError::MissingHorizon)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use akzio_domain::{
        Asset, CanaryCampaignStatus, CanaryCohortManifest, CanaryPairedObservation,
        CanaryPairedOutcomeMetrics, CanaryPairedSubjectMetrics, CanaryPromotionPolicy,
        CanaryVerdict, ContentHash, OutcomeCostModel, OutcomeHorizon, TopologyId,
        V2_DOMAIN_SCHEMA_VERSION,
    };
    use chrono::{NaiveDate, Utc};

    use super::{
        evaluate_canary_cohort, CanaryBundleComparison, CanaryHorizonMetrics,
        CanarySubjectComparison,
    };

    fn promotion_policy() -> CanaryPromotionPolicy {
        CanaryPromotionPolicy {
            minimum_evidence_completeness_ppm: 900_000,
            minimum_risk_recall_ppm: 900_000,
            required_paired_sessions_per_horizon: [2, 2, 2],
            minimum_distinct_market_days: 2,
            required_regimes: ["risk_on".to_owned(), "risk_off".to_owned()]
                .into_iter()
                .collect(),
            minimum_cost_adjusted_utility_delta_ppm: 100,
            maximum_drawdown_delta_ppm: 500,
            maximum_tail_loss_delta_ppm: 500,
            minimum_confidence_ppm: 800_000,
        }
    }

    fn cohort(policy: &CanaryPromotionPolicy) -> CanaryCohortManifest {
        CanaryCohortManifest {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            cohort_id: ContentHash::of_bytes(b"pending"),
            campaign_id: ContentHash::of_bytes(b"campaign"),
            parent_contract_hash: ContentHash::of_bytes(b"parent-contract"),
            candidate_contract_hash: ContentHash::of_bytes(b"candidate-contract"),
            parent_topology_id: TopologyId("active".to_owned()),
            candidate_topology_id: TopologyId("candidate".to_owned()),
            validation_stage: CanaryCampaignStatus::ValidationStage1,
            observation_start: NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
            observation_end: NaiveDate::from_ymd_opt(2026, 8, 28).unwrap(),
            asset_universe: Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>(),
            cost_model: OutcomeCostModel {
                transaction_cost_ppm: 100,
                slippage_ppm: 200,
            },
            market_calendar_id: ContentHash::of_bytes(b"calendar"),
            market_regimes: BTreeMap::from([
                (
                    NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
                    "risk_on".to_owned(),
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 8, 25).unwrap(),
                    "risk_off".to_owned(),
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 8, 26).unwrap(),
                    "risk_on".to_owned(),
                ),
            ]),
            generation_dataset_id: ContentHash::of_bytes(b"generation"),
            promotion_dataset_id: ContentHash::of_bytes(b"promotion"),
            promotion_policy_hash: policy.identity_hash(),
        }
        .seal()
    }

    fn paired_metrics(
        observed_trading_day: NaiveDate,
        parent_utility: i64,
        candidate_utility: i64,
    ) -> CanaryPairedSubjectMetrics {
        CanaryPairedSubjectMetrics {
            parent: CanaryPairedOutcomeMetrics {
                observed_trading_day,
                evidence_completeness_ppm: 950_000,
                risk_recall_ppm: 950_000,
                cost_adjusted_utility_ppm: parent_utility,
                drawdown_ppm: 1_000,
                tail_loss_ppm: 1_000,
                confidence_ppm: 900_000,
            },
            candidate: CanaryPairedOutcomeMetrics {
                observed_trading_day,
                evidence_completeness_ppm: 950_000,
                risk_recall_ppm: 950_000,
                cost_adjusted_utility_ppm: candidate_utility,
                drawdown_ppm: 1_100,
                tail_loss_ppm: 1_100,
                confidence_ppm: 900_000,
            },
        }
    }

    fn observation(
        cohort: &CanaryCohortManifest,
        market_day: NaiveDate,
        regime: &str,
        horizon: OutcomeHorizon,
    ) -> CanaryPairedObservation {
        let observed_trading_day = market_day.succ_opt().unwrap();
        let comparison = paired_metrics(observed_trading_day, 1_000, 1_200);
        CanaryPairedObservation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            cohort_id: cohort.cohort_id.clone(),
            session_key: market_day.to_string(),
            market_day,
            regime: regime.to_owned(),
            horizon,
            asset_universe: cohort.asset_universe.clone(),
            cost_model: cohort.cost_model,
            market_calendar_id: cohort.market_calendar_id.clone(),
            generation_dataset_id: cohort.generation_dataset_id.clone(),
            promotion_dataset_id: cohort.promotion_dataset_id.clone(),
            contract: comparison,
            topology: comparison,
            bundle: comparison,
        }
    }

    fn complete_observations(cohort: &CanaryCohortManifest) -> Vec<CanaryPairedObservation> {
        [
            (NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(), "risk_on"),
            (NaiveDate::from_ymd_opt(2026, 8, 25).unwrap(), "risk_off"),
        ]
        .into_iter()
        .flat_map(|(market_day, regime)| {
            OutcomeHorizon::ALL
                .into_iter()
                .map(move |horizon| observation(cohort, market_day, regime, horizon))
        })
        .collect()
    }

    fn subject(parent_utility: i64, candidate_utility: i64) -> CanarySubjectComparison {
        CanarySubjectComparison {
            parent: [CanaryHorizonMetrics {
                evidence_completeness_ppm: 950_000,
                risk_recall_ppm: 950_000,
                utility_ppm: parent_utility,
            }; 3],
            candidate: [CanaryHorizonMetrics {
                evidence_completeness_ppm: 950_000,
                risk_recall_ppm: 950_000,
                utility_ppm: candidate_utility,
            }; 3],
        }
    }

    #[test]
    fn all_three_ablations_must_advance() {
        let comparison = CanaryBundleComparison {
            contract: subject(1, 2),
            topology: subject(1, 2),
            bundle: subject(1, 2),
        };
        assert_eq!(comparison.verdict(900_000), CanaryVerdict::Advance);
    }

    #[test]
    fn utility_equality_holds_and_quality_regression_rolls_back() {
        let hold = CanaryBundleComparison {
            contract: subject(1, 1),
            topology: subject(1, 2),
            bundle: subject(1, 2),
        };
        assert_eq!(hold.verdict(900_000), CanaryVerdict::Hold);

        let mut degraded = subject(1, 2);
        degraded.candidate[1].risk_recall_ppm = 899_999;
        let rollback = CanaryBundleComparison {
            contract: degraded,
            topology: subject(1, 2),
            bundle: subject(1, 2),
        };
        assert_eq!(rollback.verdict(900_000), CanaryVerdict::Rollback);
    }

    #[test]
    fn cohort_defers_until_required_paired_sessions_exist_for_every_horizon() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let observations = OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                observation(
                    &cohort,
                    NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
                    "risk_on",
                    horizon,
                )
            })
            .collect::<Vec<_>>();

        let evaluation = evaluate_canary_cohort(&cohort, &policy, &observations, Utc::now())
            .expect("valid cohort evaluation");

        assert_eq!(evaluation.verdict, CanaryVerdict::Defer);
        assert_eq!(evaluation.paired_sessions_by_horizon, [1, 1, 1]);
    }

    #[test]
    fn cohort_defers_when_only_one_horizon_is_below_threshold() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let mut observations = complete_observations(&cohort);
        observations.retain(|observation| {
            observation.horizon != OutcomeHorizon::T5
                || observation.market_day == NaiveDate::from_ymd_opt(2026, 8, 24).unwrap()
        });

        let evaluation = evaluate_canary_cohort(&cohort, &policy, &observations, Utc::now())
            .expect("valid cohort evaluation");

        assert_eq!(evaluation.verdict, CanaryVerdict::Defer);
        assert_eq!(evaluation.paired_sessions_by_horizon, [2, 2, 1]);
    }

    #[test]
    fn tail_loss_regression_rolls_back_even_when_utility_improves() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let mut observations = complete_observations(&cohort);
        let degraded = observations.first_mut().unwrap();
        for subject in [
            &mut degraded.contract,
            &mut degraded.topology,
            &mut degraded.bundle,
        ] {
            subject.candidate.cost_adjusted_utility_ppm = 2_000;
            subject.candidate.tail_loss_ppm = 1_501;
        }

        let evaluation = evaluate_canary_cohort(&cohort, &policy, &observations, Utc::now())
            .expect("valid cohort evaluation");

        assert_eq!(evaluation.verdict, CanaryVerdict::Rollback);
    }

    #[test]
    fn rollback_has_priority_over_insufficient_sample_coverage() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let mut observations = vec![observation(
            &cohort,
            NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
            "risk_on",
            OutcomeHorizon::T1,
        )];
        observations[0].bundle.candidate.risk_recall_ppm = 899_999;

        let evaluation = evaluate_canary_cohort(&cohort, &policy, &observations, Utc::now())
            .expect("valid cohort evaluation");

        assert_eq!(evaluation.verdict, CanaryVerdict::Rollback);
    }

    #[test]
    fn cohort_requires_distinct_market_days_and_regime_coverage() {
        let mut days_policy = promotion_policy();
        days_policy.minimum_distinct_market_days = 3;
        let days_cohort = cohort(&days_policy);
        let days = complete_observations(&days_cohort);
        assert_eq!(
            evaluate_canary_cohort(&days_cohort, &days_policy, &days, Utc::now())
                .unwrap()
                .verdict,
            CanaryVerdict::Defer
        );

        let regime_policy = promotion_policy();
        let regime_cohort = cohort(&regime_policy);
        let regime_cohort_ref = &regime_cohort;
        let risk_on_only = [
            NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
            NaiveDate::from_ymd_opt(2026, 8, 26).unwrap(),
        ]
        .into_iter()
        .flat_map(move |market_day| {
            OutcomeHorizon::ALL
                .into_iter()
                .map(move |horizon| observation(regime_cohort_ref, market_day, "risk_on", horizon))
        })
        .collect::<Vec<_>>();
        assert_eq!(
            evaluate_canary_cohort(&regime_cohort, &regime_policy, &risk_on_only, Utc::now(),)
                .unwrap()
                .verdict,
            CanaryVerdict::Defer
        );
    }

    #[test]
    fn cohort_rejects_condition_mismatch_and_policy_drift() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let mut observation = observation(
            &cohort,
            NaiveDate::from_ymd_opt(2026, 8, 24).unwrap(),
            "risk_on",
            OutcomeHorizon::T1,
        );
        observation.bundle.candidate.observed_trading_day = observation
            .bundle
            .candidate
            .observed_trading_day
            .succ_opt()
            .unwrap();
        assert!(evaluate_canary_cohort(&cohort, &policy, &[observation], Utc::now()).is_err());

        let mut asset_mismatch = complete_observations(&cohort);
        asset_mismatch[0].asset_universe.remove(&Asset::Soxl);
        assert!(matches!(
            evaluate_canary_cohort(&cohort, &policy, &asset_mismatch, Utc::now()),
            Err(super::CanaryError::CohortMismatch("asset universe"))
        ));

        let mut cost_mismatch = complete_observations(&cohort);
        cost_mismatch[0].cost_model.slippage_ppm += 1;
        assert!(matches!(
            evaluate_canary_cohort(&cohort, &policy, &cost_mismatch, Utc::now()),
            Err(super::CanaryError::CohortMismatch("cost model"))
        ));

        let mut dataset_mismatch = complete_observations(&cohort);
        dataset_mismatch[0].promotion_dataset_id = ContentHash::of_bytes(b"other-promotion");
        assert!(matches!(
            evaluate_canary_cohort(&cohort, &policy, &dataset_mismatch, Utc::now()),
            Err(super::CanaryError::CohortMismatch("dataset identity"))
        ));

        let mut drifted_policy = policy;
        drifted_policy.minimum_confidence_ppm += 1;
        assert!(matches!(
            evaluate_canary_cohort(
                &cohort,
                &drifted_policy,
                &complete_observations(&cohort),
                Utc::now(),
            ),
            Err(super::CanaryError::PolicyDrift)
        ));
    }

    #[test]
    fn confidence_defers_and_complete_matched_cohort_advances() {
        let policy = promotion_policy();
        let cohort = cohort(&policy);
        let mut low_confidence = complete_observations(&cohort);
        low_confidence[0].contract.candidate.confidence_ppm = 799_999;
        assert_eq!(
            evaluate_canary_cohort(&cohort, &policy, &low_confidence, Utc::now())
                .unwrap()
                .verdict,
            CanaryVerdict::Defer
        );

        let evaluation = evaluate_canary_cohort(
            &cohort,
            &policy,
            &complete_observations(&cohort),
            Utc::now(),
        )
        .unwrap();
        assert_eq!(evaluation.verdict, CanaryVerdict::Advance);
        assert_eq!(evaluation.paired_sessions_by_horizon, [2, 2, 2]);
        assert_eq!(evaluation.distinct_market_days, 2);
    }
}
