//! Canary campaign comparison and fenced state transition.

use thiserror::Error;

use akzio_domain::{CanaryCampaignStatus, CanaryVerdict, Outcome, OutcomeHorizon};
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

    pub fn store(&self) -> &V2Store {
        &self.store
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
    use super::{CanaryBundleComparison, CanaryHorizonMetrics, CanarySubjectComparison};
    use akzio_domain::CanaryVerdict;

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
}
