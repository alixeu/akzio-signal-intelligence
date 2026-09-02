//! Immutable experiment-trial and search-bias evidence contracts.
//!
//! These records separate point-in-time leakage controls from search-bias
//! controls. A masked historical replay can still be overfit, and a complete
//! trial ledger does not make a bright/ticker-visible replay contamination-safe.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, ContentHash, DomainError, PolicySubject,
    DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentCondition {
    /// Real identifiers and calendar dates are visible to the model.
    Bright,
    /// Asset identifiers are replaced by stable anonymous identifiers.
    IdentifierMasked,
    /// Calendar dates are represented relative to the decision cutoff.
    CalendarMasked,
    /// Both identifiers and calendar dates are masked.
    FullyMasked,
    /// Forward evidence starts strictly after the registered knowledge cutoff.
    PostCutoffForward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentTrialStatus {
    Failed,
    Rejected,
    Evaluated,
    Selected,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentTrialMetrics {
    /// Ordered, common evaluation slices used by every candidate in a search.
    pub slice_returns_ppm: Vec<i64>,
    pub mean_return_ppm: i64,
    pub sharpe_ratio_ppm: Option<i64>,
}

impl ExperimentTrialMetrics {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.slice_returns_ppm.len() < 2
            || self.slice_returns_ppm.len() > 10_000
            || self
                .slice_returns_ppm
                .iter()
                .any(|value| value.unsigned_abs() > 10_000_000)
            || self.mean_return_ppm.unsigned_abs() > 10_000_000
            || self
                .sharpe_ratio_ppm
                .is_some_and(|value| value.unsigned_abs() > 100_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "experiment_trial.metrics",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentTrial {
    pub schema_version: u32,
    pub trial_id: ContentHash,
    pub subject: PolicySubject,
    pub parent_trial_id: Option<ContentHash>,
    pub candidate_hash: ContentHash,
    pub parameters_hash: ContentHash,
    pub prompt_hash: ContentHash,
    pub model_snapshot_hash: ContentHash,
    pub provider_id: String,
    pub model_id: String,
    pub model_release_date: NaiveDate,
    pub model_knowledge_cutoff: NaiveDate,
    pub condition: ExperimentCondition,
    pub generation_dataset_id: ContentHash,
    pub validation_dataset_id: ContentHash,
    pub holdout_dataset_id: ContentHash,
    pub generation_start: NaiveDate,
    pub generation_end: NaiveDate,
    pub validation_start: NaiveDate,
    pub validation_end: NaiveDate,
    pub holdout_start: NaiveDate,
    pub holdout_end: NaiveDate,
    pub candidate_frozen_at: DateTime<Utc>,
    pub holdout_first_opened_at: Option<DateTime<Utc>>,
    pub holdout_access_count: u32,
    pub status: ExperimentTrialStatus,
    pub metrics: Option<ExperimentTrialMetrics>,
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trial_index: Option<u64>,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl ExperimentTrial {
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.trial_id = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "subject": self.subject,
            "parent_trial_id": self.parent_trial_id,
            "candidate_hash": self.candidate_hash,
            "parameters_hash": self.parameters_hash,
            "prompt_hash": self.prompt_hash,
            "model_snapshot_hash": self.model_snapshot_hash,
            "provider_id": self.provider_id,
            "model_id": self.model_id,
            "model_release_date": self.model_release_date,
            "model_knowledge_cutoff": self.model_knowledge_cutoff,
            "condition": self.condition,
            "generation_dataset_id": self.generation_dataset_id,
            "validation_dataset_id": self.validation_dataset_id,
            "holdout_dataset_id": self.holdout_dataset_id,
            "generation_start": self.generation_start,
            "generation_end": self.generation_end,
            "validation_start": self.validation_start,
            "validation_end": self.validation_end,
            "holdout_start": self.holdout_start,
            "holdout_end": self.holdout_end,
            "candidate_frozen_at": self.candidate_frozen_at,
            "holdout_first_opened_at": self.holdout_first_opened_at,
            "holdout_access_count": self.holdout_access_count,
            "status": self.status,
            "metrics": self.metrics,
            "failure_reason": self.failure_reason,
            "behavior_bundle_hash": self.behavior_bundle_hash,
            "campaign_id": self.campaign_id,
            "trial_index": self.trial_index,
            "source_refs": self.source_refs,
            "created_at": self.created_at,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        self.subject.validate()?;
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.trial_id != self.identity_hash()?
            || self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || self.model_release_date > self.created_at.date_naive()
            || self.model_knowledge_cutoff > self.created_at.date_naive()
            || self.generation_start > self.generation_end
            || self.validation_start > self.validation_end
            || self.holdout_start > self.holdout_end
            || self.generation_end >= self.validation_start
            || self.validation_end >= self.holdout_start
            || self.generation_dataset_id == self.validation_dataset_id
            || self.generation_dataset_id == self.holdout_dataset_id
            || self.validation_dataset_id == self.holdout_dataset_id
            || self.candidate_frozen_at > self.created_at
            || self.holdout_first_opened_at.is_some_and(|opened| {
                opened > self.created_at
                    || (opened < self.candidate_frozen_at
                        && self.status != ExperimentTrialStatus::Invalidated)
            })
            || self.holdout_first_opened_at.is_some() != (self.holdout_access_count > 0)
            || self.source_refs.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DomainError::InvalidBudget {
                field: "experiment_trial",
            });
        }
        if self.condition == ExperimentCondition::PostCutoffForward
            && self.holdout_start <= self.model_knowledge_cutoff
        {
            return Err(DomainError::InvalidBudget {
                field: "experiment_trial.post_cutoff",
            });
        }
        match self.status {
            ExperimentTrialStatus::Failed => {
                if self.metrics.is_some()
                    || self
                        .failure_reason
                        .as_deref()
                        .is_none_or(|reason| reason.trim().is_empty())
                {
                    return Err(DomainError::InvalidBudget {
                        field: "experiment_trial.failure",
                    });
                }
            }
            ExperimentTrialStatus::Invalidated => {
                if self.holdout_access_count <= 1
                    || self
                        .failure_reason
                        .as_deref()
                        .is_none_or(|reason| reason.trim().is_empty())
                {
                    return Err(DomainError::InvalidBudget {
                        field: "experiment_trial.invalidation",
                    });
                }
            }
            ExperimentTrialStatus::Rejected
            | ExperimentTrialStatus::Evaluated
            | ExperimentTrialStatus::Selected => {
                if self.metrics.is_none() || self.failure_reason.is_some() {
                    return Err(DomainError::InvalidBudget {
                        field: "experiment_trial.result",
                    });
                }
                if self.holdout_access_count != 1 {
                    return Err(DomainError::InvalidBudget {
                        field: "experiment_trial.holdout_access",
                    });
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.validate()?;
        }
        Ok(())
    }

    pub const fn is_contamination_controlled(&self) -> bool {
        matches!(
            self.condition,
            ExperimentCondition::FullyMasked | ExperimentCondition::PostCutoffForward
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchBiasAcceptancePolicy {
    pub min_deflated_sharpe_ratio_ppm: i64,
    pub max_probability_of_backtest_overfitting_ppm: u32,
    pub min_bootstrap_lower_bound_ppm: i64,
    pub max_family_wise_error_rate_ppm: u32,
    pub min_global_trial_count: u64,
    pub min_comparable_trial_count: u64,
    pub selection_metric_hash: ContentHash,
    pub search_space_hash: ContentHash,
    pub slice_schema_hash: ContentHash,
    pub metric_implementation_hash: ContentHash,
}

impl SearchBiasAcceptancePolicy {
    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.min_global_trial_count < 2
            || self.max_probability_of_backtest_overfitting_ppm > 1_000_000
            || self.max_family_wise_error_rate_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "search_bias_acceptance_policy",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricIdentity {
    pub metric_name: String,
    pub metric_version: String,
    pub implementation_hash: ContentHash,
    pub assumptions_hash: ContentHash,
}

impl MetricIdentity {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.metric_name.trim().is_empty() || self.metric_version.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "metric_identity",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchBiasCertificate {
    pub schema_version: u32,
    pub certificate_id: ContentHash,
    pub selected_trial: ArtifactRef,
    pub trial_refs: Vec<ArtifactRef>,
    pub global_trial_count: u64,
    pub deflated_sharpe_ratio_ppm: Option<i64>,
    pub probability_of_backtest_overfitting_ppm: Option<u32>,
    pub stationary_bootstrap_lower_bound_ppm: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moving_block_bootstrap_lower_bound_ppm: Option<i64>,
    pub family_wise_error_rate_ppm: Option<u32>,
    pub selected_condition: ExperimentCondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bright_trial: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bright_minus_masked_bootstrap_lower_bound_ppm: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contamination_risk: Option<bool>,
    pub holdout_dataset_id: ContentHash,
    pub holdout_access_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_policy_hash: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metric_identities: BTreeMap<String, MetricIdentity>,
    pub created_at: DateTime<Utc>,
}

impl SearchBiasCertificate {
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.certificate_id = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "selected_trial": self.selected_trial,
            "trial_refs": self.trial_refs,
            "global_trial_count": self.global_trial_count,
            "deflated_sharpe_ratio_ppm": self.deflated_sharpe_ratio_ppm,
            "probability_of_backtest_overfitting_ppm": self.probability_of_backtest_overfitting_ppm,
            "stationary_bootstrap_lower_bound_ppm": self.stationary_bootstrap_lower_bound_ppm,
            "moving_block_bootstrap_lower_bound_ppm": self.moving_block_bootstrap_lower_bound_ppm,
            "family_wise_error_rate_ppm": self.family_wise_error_rate_ppm,
            "selected_condition": self.selected_condition,
            "bright_trial": self.bright_trial,
            "bright_minus_masked_bootstrap_lower_bound_ppm": self.bright_minus_masked_bootstrap_lower_bound_ppm,
            "contamination_risk": self.contamination_risk,
            "holdout_dataset_id": self.holdout_dataset_id,
            "holdout_access_count": self.holdout_access_count,
            "behavior_bundle_hash": self.behavior_bundle_hash,
            "acceptance_policy_hash": self.acceptance_policy_hash,
            "metric_identities": self.metric_identities,
            "created_at": self.created_at,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.certificate_id != self.identity_hash()?
            || self.selected_trial.kind != ArtifactKind::ExperimentTrial
            || self.trial_refs.len() < 2
            || self.global_trial_count != self.trial_refs.len() as u64
            || self.trial_refs.windows(2).any(|pair| pair[0] >= pair[1])
            || !self.trial_refs.contains(&self.selected_trial)
            || self
                .trial_refs
                .iter()
                .any(|reference| reference.kind != ArtifactKind::ExperimentTrial)
            || self.holdout_access_count != 1
            || self
                .probability_of_backtest_overfitting_ppm
                .is_some_and(|value| value > 1_000_000)
            || self
                .family_wise_error_rate_ppm
                .is_some_and(|value| value > 1_000_000)
            || self.bright_trial.as_ref().is_some_and(|reference| {
                reference.kind != ArtifactKind::ExperimentTrial
                    || !self.trial_refs.contains(reference)
            })
        {
            return Err(DomainError::InvalidBudget {
                field: "search_bias_certificate",
            });
        }
        for identity in self.metric_identities.values() {
            identity.validate()?;
        }
        match self.selected_condition {
            ExperimentCondition::FullyMasked => {
                if self.bright_trial.is_none()
                    || self.bright_minus_masked_bootstrap_lower_bound_ppm.is_none()
                    || self.contamination_risk.is_none()
                {
                    return Err(DomainError::InvalidBudget {
                        field: "search_bias_certificate.contamination",
                    });
                }
            }
            ExperimentCondition::PostCutoffForward => {
                if self.bright_trial.is_some()
                    || self.bright_minus_masked_bootstrap_lower_bound_ppm.is_some()
                    || self.contamination_risk.is_some()
                {
                    return Err(DomainError::InvalidBudget {
                        field: "search_bias_certificate.contamination",
                    });
                }
            }
            ExperimentCondition::Bright
            | ExperimentCondition::IdentifierMasked
            | ExperimentCondition::CalendarMasked => {
                return Err(DomainError::InvalidBudget {
                    field: "search_bias_certificate.selected_condition",
                });
            }
        }
        Ok(())
    }

    pub fn is_promotion_ready(&self) -> bool {
        self.deflated_sharpe_ratio_ppm.is_some()
            && self.probability_of_backtest_overfitting_ppm.is_some()
            && self.stationary_bootstrap_lower_bound_ppm.is_some()
            && self.family_wise_error_rate_ppm.is_some()
            && self.holdout_access_count == 1
            && self.contamination_risk != Some(true)
    }

    pub fn permits_promotion(&self, policy: &SearchBiasAcceptancePolicy) -> bool {
        self.validate().is_ok()
            && policy.validate().is_ok()
            && self.holdout_access_count == 1
            && self.contamination_risk != Some(true)
            && self.global_trial_count >= policy.min_global_trial_count
            && self
                .deflated_sharpe_ratio_ppm
                .is_some_and(|v| v >= policy.min_deflated_sharpe_ratio_ppm)
            && self
                .probability_of_backtest_overfitting_ppm
                .is_some_and(|v| v <= policy.max_probability_of_backtest_overfitting_ppm)
            && self
                .stationary_bootstrap_lower_bound_ppm
                .is_some_and(|v| v >= policy.min_bootstrap_lower_bound_ppm)
            && self
                .family_wise_error_rate_ppm
                .is_some_and(|v| v <= policy.max_family_wise_error_rate_ppm)
            && self
                .acceptance_policy_hash
                .as_ref()
                .is_none_or(|hash| policy.identity_hash().map(|h| &h == hash).unwrap_or(false))
    }
}
