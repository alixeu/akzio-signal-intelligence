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
    content_hash_json, ArtifactKind, ArtifactRef, Asset, ContentHash, DomainError, MoneyMicros,
    OutcomeCostModel, OutcomeHorizon, OutcomeWindow, RunId, TopologyId, V2_DOMAIN_SCHEMA_VERSION,
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
    pub required_paired_sessions_per_horizon: [u64; 3],
    pub minimum_distinct_market_days: u64,
    pub required_regimes: BTreeSet<String>,
    pub minimum_cost_adjusted_utility_delta_ppm: i64,
    pub maximum_drawdown_delta_ppm: u32,
    pub maximum_tail_loss_delta_ppm: u32,
    pub minimum_confidence_ppm: u32,
}

impl CanaryPromotionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.minimum_evidence_completeness_ppm > 1_000_000
            || self.minimum_risk_recall_ppm > 1_000_000
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
        });
        content_hash_json(&value).expect("CanaryCohortManifest canonical JSON serializes")
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
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
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.cohort_manifest",
            });
        }
        self.cost_model.validate()
    }

    pub fn regime_for(&self, market_day: NaiveDate) -> Option<&str> {
        self.market_regimes.get(&market_day).map(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryPairedOutcomeMetrics {
    pub observed_trading_day: NaiveDate,
    pub evidence_completeness_ppm: u32,
    pub risk_recall_ppm: u32,
    pub cost_adjusted_utility_ppm: i64,
    pub drawdown_ppm: u32,
    pub tail_loss_ppm: u32,
    pub confidence_ppm: u32,
}

impl CanaryPairedOutcomeMetrics {
    pub fn from_outcome_window(window: &OutcomeWindow) -> Self {
        Self {
            observed_trading_day: window.observed_trading_day,
            evidence_completeness_ppm: window.evidence_completeness_ppm,
            risk_recall_ppm: window.risk_recall_ppm.unwrap_or_default(),
            cost_adjusted_utility_ppm: window.utility_ppm,
            drawdown_ppm: negative_ppm(window.portfolio_return_ppm),
            tail_loss_ppm: negative_ppm(window.utility_ppm),
            confidence_ppm: window.calibration_ppm.unwrap_or_default(),
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.evidence_completeness_ppm,
            self.risk_recall_ppm,
            self.drawdown_ppm,
            self.tail_loss_ppm,
            self.confidence_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.paired_outcome_metrics",
            });
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
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
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
    pub evaluated_at: DateTime<Utc>,
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
        });
        content_hash_json(&value).expect("CanaryCohortEvaluation canonical JSON serializes")
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.evaluation_id != self.identity_hash()
        {
            return Err(DomainError::InvalidBudget {
                field: "canary.cohort_evaluation",
            });
        }
        Ok(())
    }
}

fn negative_ppm(value: i64) -> u32 {
    if value >= 0 {
        0
    } else {
        u32::try_from(value.unsigned_abs()).unwrap_or(u32::MAX)
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
    #[serde(default)]
    pub promotion_policy: Option<CanaryPromotionPolicy>,
    #[serde(default)]
    pub cohorts: Vec<CanaryCohortManifest>,
    pub created_at: DateTime<Utc>,
}

impl CanaryCampaignSpec {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
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
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::NaiveDate;

    use super::{
        CanaryCampaignStatus, CanaryCohortManifest, CanaryPromotionPolicy,
        CanarySessionReservation, CanaryVerdict,
    };
    use crate::{
        Asset, ContentHash, OutcomeCostModel, RunId, TopologyId, V2_DOMAIN_SCHEMA_VERSION,
    };

    fn policy() -> CanaryPromotionPolicy {
        CanaryPromotionPolicy {
            minimum_evidence_completeness_ppm: 900_000,
            minimum_risk_recall_ppm: 900_000,
            required_paired_sessions_per_horizon: [2, 2, 2],
            minimum_distinct_market_days: 2,
            required_regimes: ["risk_on".to_owned(), "risk_off".to_owned()]
                .into_iter()
                .collect(),
            minimum_cost_adjusted_utility_delta_ppm: 100,
            maximum_drawdown_delta_ppm: 1_000,
            maximum_tail_loss_delta_ppm: 1_000,
            minimum_confidence_ppm: 800_000,
        }
    }

    fn manifest() -> CanaryCohortManifest {
        let campaign_id = ContentHash::of_bytes(b"campaign");
        let policy = policy();
        CanaryCohortManifest {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            cohort_id: ContentHash::of_bytes(b"pending"),
            campaign_id,
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
            ]),
            generation_dataset_id: ContentHash::of_bytes(b"generation"),
            promotion_dataset_id: ContentHash::of_bytes(b"promotion"),
            promotion_policy_hash: policy.identity_hash(),
        }
        .seal()
    }

    #[test]
    fn campaign_levels_are_ordered_and_terminal_states_have_no_successor() {
        assert_eq!(
            CanaryCampaignStatus::Staged.next(),
            Some(CanaryCampaignStatus::ValidationStage1)
        );
        assert_eq!(
            CanaryCampaignStatus::ValidationStage3.next(),
            Some(CanaryCampaignStatus::ActiveValidation)
        );
        assert_eq!(CanaryCampaignStatus::Completed.next(), None);
        assert!(CanaryCampaignStatus::ValidationStage2.is_level());
        assert!(!CanaryCampaignStatus::Frozen.is_level());
    }

    #[test]
    fn validation_stage_names_replace_percentage_semantics_and_read_legacy_values() {
        for (legacy, stage) in [
            ("canary10", CanaryCampaignStatus::ValidationStage1),
            ("canary25", CanaryCampaignStatus::ValidationStage2),
            ("canary50", CanaryCampaignStatus::ValidationStage3),
        ] {
            assert_eq!(
                serde_json::from_str::<CanaryCampaignStatus>(&format!("\"{legacy}\"")).unwrap(),
                stage
            );
        }
        assert_eq!(
            serde_json::to_string(&CanaryCampaignStatus::ValidationStage1).unwrap(),
            "\"validation_stage1\""
        );
        assert_eq!(
            CanaryCampaignStatus::ValidationStage2.display_name(),
            "ValidationStage2"
        );
    }

    #[test]
    fn sealed_cohort_binds_policy_conditions_and_separates_datasets() {
        let manifest = manifest();
        manifest.validate().unwrap();
        assert_eq!(manifest, manifest.clone().seal());

        let mut reused = manifest;
        reused.promotion_dataset_id = reused.generation_dataset_id.clone();
        assert!(reused.seal().validate().is_err());
    }

    #[test]
    fn session_reservation_rejects_duplicate_run_ids() {
        let run = RunId::new();
        let reservation = CanarySessionReservation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: ContentHash::of_bytes(b"campaign"),
            level: CanaryCampaignStatus::ValidationStage1,
            session_key: "2026-08-25".to_owned(),
            cohort_id: None,
            market_day: None,
            regime: None,
            parent_run_id: run.clone(),
            contract_shadow_run_id: run,
            topology_shadow_run_id: RunId::new(),
            bundle_shadow_run_id: RunId::new(),
            scheduler_epoch: 1,
            reserved_at: chrono::Utc::now(),
        };
        assert!(reservation.validate().is_err());

        let duplicate = RunId::new();
        let reservation = CanarySessionReservation {
            parent_run_id: duplicate.clone(),
            contract_shadow_run_id: RunId::new(),
            topology_shadow_run_id: duplicate,
            bundle_shadow_run_id: RunId::new(),
            ..reservation
        };
        assert!(reservation.validate().is_err());
        let _ = CanaryVerdict::Defer;
    }
}
