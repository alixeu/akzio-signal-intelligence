//! Domain vocabulary for a Paper-only canary campaign.
//!
//! The campaign state machine is deliberately kept in the domain crate so
//! Store, scheduler and learning code share one serialized contract.  It does
//! not perform I/O or decide whether a candidate is good; those decisions are
//! owned by `akzio-learning`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, Asset, CalibrationReport,
    CapabilityRetentionMatrix, ContentHash, DomainError, ForecastScore, MoneyMicros,
    OutcomeCostModel, OutcomeHorizon, OutcomeWindow, PromotionIntegrityEvidence, RunId,
    SearchBiasAcceptancePolicy, TopologyId, DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryCampaignStatus {
    Staged,
    #[serde(rename = "validation_stage1", alias = "canary10")]
    ValidationStage1,
    #[serde(rename = "validation_stage2", alias = "canary25")]
    ValidationStage2,
    #[serde(rename = "validation_stage3", alias = "canary50")]
    ValidationStage3,
    ActiveValidation,
    Completed,
    Frozen,
}

impl CanaryCampaignStatus {
    pub const LEVELS: [Self; 4] = [
        Self::ValidationStage1,
        Self::ValidationStage2,
        Self::ValidationStage3,
        Self::ActiveValidation,
    ];

    #[allow(non_upper_case_globals)]
    pub const Canary10: Self = Self::ValidationStage1;
    #[allow(non_upper_case_globals)]
    pub const Canary25: Self = Self::ValidationStage2;
    #[allow(non_upper_case_globals)]
    pub const Canary50: Self = Self::ValidationStage3;

    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Staged => Some(Self::ValidationStage1),
            Self::ValidationStage1 => Some(Self::ValidationStage2),
            Self::ValidationStage2 => Some(Self::ValidationStage3),
            Self::ValidationStage3 => Some(Self::ActiveValidation),
            Self::ActiveValidation => Some(Self::Completed),
            Self::Completed | Self::Frozen => None,
        }
    }

    pub const fn is_level(self) -> bool {
        matches!(
            self,
            Self::ValidationStage1
                | Self::ValidationStage2
                | Self::ValidationStage3
                | Self::ActiveValidation
        )
    }

    pub const fn policy_state(self) -> Option<crate::CandidatePolicyState> {
        match self {
            Self::ValidationStage1 => Some(crate::CandidatePolicyState::Canary10),
            Self::ValidationStage2 => Some(crate::CandidatePolicyState::Canary25),
            Self::ValidationStage3 => Some(crate::CandidatePolicyState::Canary50),
            Self::ActiveValidation | Self::Completed => Some(crate::CandidatePolicyState::Active),
            Self::Staged | Self::Frozen => None,
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Staged => "Staged",
            Self::ValidationStage1 => "ValidationStage1",
            Self::ValidationStage2 => "ValidationStage2",
            Self::ValidationStage3 => "ValidationStage3",
            Self::ActiveValidation => "ActiveValidation",
            Self::Completed => "Completed",
            Self::Frozen => "Frozen",
        }
    }

    pub const fn legacy_storage_name(self) -> Option<&'static str> {
        match self {
            Self::ValidationStage1 => Some("canary10"),
            Self::ValidationStage2 => Some("canary25"),
            Self::ValidationStage3 => Some("canary50"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryPromotionPolicy {
    pub minimum_evidence_completeness_ppm: u32,
    pub minimum_risk_recall_ppm: u32,
    pub minimum_process_quality_ppm: u32,
    pub required_paired_sessions_per_horizon: [u64; 3],
    pub minimum_distinct_market_days: u64,
    pub required_regimes: BTreeSet<String>,
    pub minimum_cost_adjusted_utility_delta_ppm: i64,
    pub maximum_drawdown_delta_ppm: u32,
    pub maximum_tail_loss_delta_ppm: u32,
    pub minimum_confidence_ppm: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_bias_acceptance: Option<SearchBiasAcceptancePolicy>,
}

impl CanaryPromotionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.minimum_evidence_completeness_ppm > 1_000_000
            || self.minimum_risk_recall_ppm > 1_000_000
            || self.minimum_process_quality_ppm > 1_000_000
            || self.maximum_drawdown_delta_ppm > 1_000_000
            || self.maximum_tail_loss_delta_ppm > 1_000_000
            || self.minimum_confidence_ppm > 1_000_000
            || self.minimum_cost_adjusted_utility_delta_ppm < 0
            || self.required_paired_sessions_per_horizon.contains(&0)
            || self.minimum_distinct_market_days == 0
            || self.required_regimes.is_empty()
            || self
                .required_regimes
                .iter()
                .any(|regime| regime.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.promotion_policy",
            });
        }
        if let Some(acceptance) = &self.search_bias_acceptance {
            acceptance.validate()?;
        }
        Ok(())
    }

    pub fn identity_hash(&self) -> ContentHash {
        let value = serde_json::to_value(self).expect("CanaryPromotionPolicy serializes");
        content_hash_json(&value).expect("CanaryPromotionPolicy canonical JSON serializes")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCohortManifest {
    pub schema_version: u32,
    pub cohort_id: ContentHash,
    pub campaign_id: ContentHash,
    pub parent_contract_hash: ContentHash,
    pub candidate_contract_hash: ContentHash,
    pub parent_topology_id: TopologyId,
    pub candidate_topology_id: TopologyId,
    pub validation_stage: CanaryCampaignStatus,
    pub observation_start: NaiveDate,
    pub observation_end: NaiveDate,
    pub asset_universe: BTreeSet<Asset>,
    pub cost_model: OutcomeCostModel,
    pub market_calendar_id: ContentHash,
    pub market_regimes: BTreeMap<NaiveDate, String>,
    pub generation_dataset_id: ContentHash,
    pub promotion_dataset_id: ContentHash,
    pub promotion_policy_hash: ContentHash,
    /// Immutable pre-canary search-bias evidence. Missing evidence may collect
    /// observations, but it cannot justify an Advance verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_bias_certificate: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_integrity: Option<PromotionIntegrityEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_retention: Option<CapabilityRetentionMatrix>,
}

impl CanaryCohortManifest {
    pub fn seal(mut self) -> Self {
        self.cohort_id = self.identity_hash();
        self
    }

    pub fn identity_hash(&self) -> ContentHash {
        let value = serde_json::json!({
            "schema_version": self.schema_version,
            "campaign_id": self.campaign_id,
            "parent_contract_hash": self.parent_contract_hash,
            "candidate_contract_hash": self.candidate_contract_hash,
            "parent_topology_id": self.parent_topology_id,
            "candidate_topology_id": self.candidate_topology_id,
            "validation_stage": self.validation_stage,
            "observation_start": self.observation_start,
            "observation_end": self.observation_end,
            "asset_universe": self.asset_universe,
            "cost_model": self.cost_model,
            "market_calendar_id": self.market_calendar_id,
            "market_regimes": self.market_regimes,
            "generation_dataset_id": self.generation_dataset_id,
            "promotion_dataset_id": self.promotion_dataset_id,
            "promotion_policy_hash": self.promotion_policy_hash,
            "search_bias_certificate": self.search_bias_certificate,
            "promotion_integrity": self.promotion_integrity,
            "capability_retention": self.capability_retention,
        });
        content_hash_json(&value).expect("CanaryCohortManifest canonical JSON serializes")
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.cohort_id != self.identity_hash()
            || !self.validation_stage.is_level()
            || self.parent_topology_id.0.trim().is_empty()
            || self.candidate_topology_id.0.trim().is_empty()
            || self.observation_start > self.observation_end
            || self.asset_universe.is_empty()
            || self.market_regimes.is_empty()
            || self.market_regimes.iter().any(|(day, regime)| {
                *day < self.observation_start
                    || *day > self.observation_end
                    || regime.trim().is_empty()
            })
            || self.generation_dataset_id == self.promotion_dataset_id
            || self
                .search_bias_certificate
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::SearchBiasCertificate)
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.cohort_manifest",
            });
        }
        self.cost_model.validate()?;
        if let Some(retention) = &self.capability_retention {
            retention.validate()?;
        }
        Ok(())
    }

    pub fn regime_for(&self, market_day: NaiveDate) -> Option<&str> {
        self.market_regimes.get(&market_day).map(String::as_str)
    }
}

/// Paired canary metrics for one horizon.
///
/// `risk_recall_ppm` stays `Option` all the way to the verdict so that
/// "measured zero recall" and "not measured yet" never collapse into the same
/// value. Collapsing them with `unwrap_or_default` would make an unmeasured
/// window indistinguishable from total risk-detection failure and force a
/// rollback on absent evidence rather than on observed degradation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryPairedOutcomeMetrics {
    pub observed_trading_day: NaiveDate,
    pub evidence_completeness_ppm: Option<u32>,
    pub risk_recall_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_quality_ppm: Option<u32>,
    pub cost_adjusted_utility_ppm: i64,
    pub drawdown_ppm: Option<u32>,
    pub tail_loss_ppm: Option<u32>,
    /// Legacy single-window field retained for old serialized observations.
    /// New promotion decisions use the aggregated `forecast_score` instead.
    pub confidence_ppm: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forecast_score: Option<ForecastScore>,
}

impl CanaryPairedOutcomeMetrics {
    pub fn from_outcome_window(window: &OutcomeWindow) -> Self {
        Self {
            observed_trading_day: window.observed_trading_day,
            evidence_completeness_ppm: window.evidence_completeness_ppm,
            risk_recall_ppm: window.risk_recall_ppm,
            process_quality_ppm: None,
            cost_adjusted_utility_ppm: window.utility_ppm,
            drawdown_ppm: window.maximum_drawdown_ppm,
            tail_loss_ppm: window.expected_shortfall_ppm,
            confidence_ppm: window.calibration_ppm.unwrap_or_default(),
            forecast_score: window.forecast_score,
        }
    }

    /// False when risk recall was never measured for this window. Callers must
    /// defer instead of promoting or rolling back on an unmeasured metric.
    pub const fn risk_recall_is_measured(&self) -> bool {
        self.risk_recall_ppm.is_some()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.evidence_completeness_ppm,
            self.risk_recall_ppm,
            self.process_quality_ppm,
            self.drawdown_ppm,
            self.tail_loss_ppm,
            Some(self.confidence_ppm),
        ]
        .into_iter()
        .flatten()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.paired_outcome_metrics",
            });
        }
        if let Some(score) = self.forecast_score {
            score.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryPairedSubjectMetrics {
    pub parent: CanaryPairedOutcomeMetrics,
    pub candidate: CanaryPairedOutcomeMetrics,
}

impl CanaryPairedSubjectMetrics {
    /// False when either side of the pair lacks a measured risk recall. An
    /// unmeasured pair carries no risk evidence at all, so it can neither
    /// justify promotion nor prove degradation.
    pub const fn risk_recall_is_measured(&self) -> bool {
        self.parent.risk_recall_is_measured() && self.candidate.risk_recall_is_measured()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        self.parent.validate()?;
        self.candidate.validate()?;
        if self.parent.observed_trading_day != self.candidate.observed_trading_day {
            return Err(DomainError::InvalidBudget {
                field: "canary.paired_observation_window",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryPairedObservation {
    pub schema_version: u32,
    pub cohort_id: ContentHash,
    pub session_key: String,
    pub market_day: NaiveDate,
    pub regime: String,
    pub horizon: OutcomeHorizon,
    pub asset_universe: BTreeSet<Asset>,
    pub cost_model: OutcomeCostModel,
    pub market_calendar_id: ContentHash,
    pub generation_dataset_id: ContentHash,
    pub promotion_dataset_id: ContentHash,
    pub contract: CanaryPairedSubjectMetrics,
    pub topology: CanaryPairedSubjectMetrics,
    pub bundle: CanaryPairedSubjectMetrics,
}

impl CanaryPairedObservation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.session_key != self.market_day.to_string()
            || self.regime.trim().is_empty()
            || self.asset_universe.is_empty()
            || self.generation_dataset_id == self.promotion_dataset_id
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.paired_observation",
            });
        }
        self.cost_model.validate()?;
        self.contract.validate()?;
        self.topology.validate()?;
        self.bundle.validate()
    }

    pub fn identity_hash(&self) -> ContentHash {
        let value = serde_json::to_value(self).expect("CanaryPairedObservation serializes");
        content_hash_json(&value).expect("CanaryPairedObservation canonical JSON serializes")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCohortEvaluation {
    pub schema_version: u32,
    pub evaluation_id: ContentHash,
    pub cohort_id: ContentHash,
    pub promotion_policy_hash: ContentHash,
    pub observation_set_hash: ContentHash,
    pub verdict: CanaryVerdict,
    pub paired_sessions_by_horizon: [u64; 3],
    pub distinct_market_days: u64,
    pub covered_regimes: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calibration_reports: Vec<CanaryCalibrationReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_bias_certificate: Option<ArtifactRef>,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanarySubjectKind {
    Contract,
    Topology,
    Bundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCalibrationReport {
    pub subject: CanarySubjectKind,
    pub horizon: OutcomeHorizon,
    pub parent: Option<CalibrationReport>,
    pub candidate: Option<CalibrationReport>,
}

impl CanaryCalibrationReport {
    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(report) = self.parent {
            report.validate()?;
        }
        if let Some(report) = self.candidate {
            report.validate()?;
        }
        Ok(())
    }
}

impl CanaryCohortEvaluation {
    pub fn seal(mut self) -> Self {
        self.evaluation_id = self.identity_hash();
        self
    }

    pub fn identity_hash(&self) -> ContentHash {
        let value = serde_json::json!({
            "schema_version": self.schema_version,
            "cohort_id": self.cohort_id,
            "promotion_policy_hash": self.promotion_policy_hash,
            "observation_set_hash": self.observation_set_hash,
            "verdict": self.verdict,
            "paired_sessions_by_horizon": self.paired_sessions_by_horizon,
            "distinct_market_days": self.distinct_market_days,
            "covered_regimes": self.covered_regimes,
            "calibration_reports": self.calibration_reports,
            "search_bias_certificate": self.search_bias_certificate,
        });
        content_hash_json(&value).expect("CanaryCohortEvaluation canonical JSON serializes")
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.evaluation_id != self.identity_hash()
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.cohort_evaluation",
            });
        }
        self.calibration_reports
            .iter()
            .try_for_each(CanaryCalibrationReport::validate)?;
        if self
            .search_bias_certificate
            .as_ref()
            .is_some_and(|reference| reference.kind != ArtifactKind::SearchBiasCertificate)
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.cohort_evaluation.search_bias_certificate",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryVerdict {
    Advance,
    Hold,
    Rollback,
    Defer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCampaignSpec {
    pub schema_version: u32,
    pub campaign_id: ContentHash,
    pub active_contract_hash: ContentHash,
    pub candidate_contract: ArtifactRef,
    pub active_topology_id: TopologyId,
    pub candidate_topology: ArtifactRef,
    pub runtime_manifest: ArtifactRef,
    pub paper_approval: ArtifactRef,
    pub source_revision: String,
    pub maximum_total_notional: MoneyMicros,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    #[serde(default)]
    pub promotion_policy: Option<CanaryPromotionPolicy>,
    #[serde(default)]
    pub cohorts: Vec<CanaryCohortManifest>,
    pub created_at: DateTime<Utc>,
}

impl CanaryCampaignSpec {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.source_revision.trim().is_empty()
            || self.active_topology_id.0.trim().is_empty()
            || self.maximum_total_notional.0 <= 0
            || self.maximum_total_notional.0 > MoneyMicros::from_usd_cents(100_000).0
        {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.spec",
            });
        }

        if self.candidate_contract.kind != ArtifactKind::Contract {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.candidate_contract",
            });
        }
        if self.candidate_topology.kind != ArtifactKind::WorkflowGraph {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.candidate_topology",
            });
        }
        if self.runtime_manifest.kind != ArtifactKind::RuntimeManifest {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.runtime_manifest",
            });
        }
        if self.paper_approval.kind != ArtifactKind::PaperLaunchApproval {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.paper_approval",
            });
        }
        match (&self.promotion_policy, self.cohorts.as_slice()) {
            (None, []) => {}
            (Some(policy), cohorts) => {
                policy.validate()?;
                if cohorts.len() != CanaryCampaignStatus::LEVELS.len() {
                    return Err(DomainError::InvalidBudget {
                        field: "canary_campaign.cohorts",
                    });
                }
                let policy_hash = policy.identity_hash();
                let cohort_ids = cohorts
                    .iter()
                    .map(|cohort| &cohort.cohort_id)
                    .collect::<BTreeSet<_>>();
                if cohort_ids.len() != cohorts.len() {
                    return Err(DomainError::InvalidBudget {
                        field: "canary_campaign.cohorts",
                    });
                }
                for stage in CanaryCampaignStatus::LEVELS {
                    let cohort = cohorts
                        .iter()
                        .find(|cohort| cohort.validation_stage == stage)
                        .ok_or(DomainError::InvalidBudget {
                            field: "canary_campaign.cohorts",
                        })?;
                    cohort.validate()?;
                    let available_regimes = cohort
                        .market_regimes
                        .values()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    if cohort.campaign_id != self.campaign_id
                        || cohort.parent_contract_hash != self.active_contract_hash
                        || cohort.parent_topology_id != self.active_topology_id
                        || cohort.promotion_policy_hash != policy_hash
                        || !policy.required_regimes.is_subset(&available_regimes)
                        || policy.minimum_distinct_market_days > cohort.market_regimes.len() as u64
                        || policy
                            .required_paired_sessions_per_horizon
                            .iter()
                            .any(|required| *required > cohort.market_regimes.len() as u64)
                    {
                        return Err(DomainError::InvalidBudget {
                            field: "canary_campaign.cohorts",
                        });
                    }
                }
            }
            _ => {
                return Err(DomainError::InvalidBudget {
                    field: "canary_campaign.cohorts",
                });
            }
        }
        Ok(())
    }

    pub fn cohort(&self, stage: CanaryCampaignStatus) -> Option<&CanaryCohortManifest> {
        self.cohorts
            .iter()
            .find(|cohort| cohort.validation_stage == stage)
    }

    pub fn has_paired_cohorts(&self) -> bool {
        self.promotion_policy.is_some() && !self.cohorts.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanarySessionReservation {
    pub schema_version: u32,
    pub campaign_id: ContentHash,
    pub level: CanaryCampaignStatus,
    pub session_key: String,
    #[serde(default)]
    pub cohort_id: Option<ContentHash>,
    #[serde(default)]
    pub market_day: Option<NaiveDate>,
    #[serde(default)]
    pub regime: Option<String>,
    pub parent_run_id: RunId,
    pub contract_shadow_run_id: RunId,
    pub topology_shadow_run_id: RunId,
    pub bundle_shadow_run_id: RunId,
    pub scheduler_epoch: u64,
    pub reserved_at: DateTime<Utc>,
}

impl CanarySessionReservation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || !self.level.is_level()
            || self.session_key.trim().is_empty()
            || self.scheduler_epoch == 0
            || self.parent_run_id.0.trim().is_empty()
            || self.contract_shadow_run_id.0.trim().is_empty()
            || self.topology_shadow_run_id.0.trim().is_empty()
            || self.bundle_shadow_run_id.0.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.session",
            });
        }

        match (&self.cohort_id, self.market_day, self.regime.as_deref()) {
            (None, None, None) => {}
            (Some(_), Some(market_day), Some(regime))
                if !regime.trim().is_empty() && self.session_key == market_day.to_string() => {}
            _ => {
                return Err(DomainError::EmptyField {
                    field: "canary_campaign.session.cohort",
                });
            }
        }

        let run_ids = [
            &self.parent_run_id,
            &self.contract_shadow_run_id,
            &self.topology_shadow_run_id,
            &self.bundle_shadow_run_id,
        ];
        if run_ids.iter().copied().collect::<BTreeSet<_>>().len() != run_ids.len() {
            return Err(DomainError::EmptyField {
                field: "canary_campaign.session.run_ids",
            });
        }
        Ok(())
    }
}
