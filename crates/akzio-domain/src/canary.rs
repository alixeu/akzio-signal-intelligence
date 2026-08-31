//! Domain vocabulary for a Paper-only canary campaign.
//!
//! The campaign state machine is deliberately kept in the domain crate so
//! Store, scheduler and learning code share one serialized contract.  It does
//! not perform I/O or decide whether a candidate is good; those decisions are
//! owned by `akzio-learning`.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, ContentHash, DomainError, MoneyMicros, RunId, TopologyId,
    V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryCampaignStatus {
    Staged,
    Canary10,
    Canary25,
    Canary50,
    ActiveValidation,
    Completed,
    Frozen,
}

impl CanaryCampaignStatus {
    pub const LEVELS: [Self; 4] = [
        Self::Canary10,
        Self::Canary25,
        Self::Canary50,
        Self::ActiveValidation,
    ];

    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Staged => Some(Self::Canary10),
            Self::Canary10 => Some(Self::Canary25),
            Self::Canary25 => Some(Self::Canary50),
            Self::Canary50 => Some(Self::ActiveValidation),
            Self::ActiveValidation => Some(Self::Completed),
            Self::Completed | Self::Frozen => None,
        }
    }

    pub const fn is_level(self) -> bool {
        matches!(
            self,
            Self::Canary10 | Self::Canary25 | Self::Canary50 | Self::ActiveValidation
        )
    }

    pub const fn policy_state(self) -> Option<crate::CandidatePolicyState> {
        match self {
            Self::Canary10 => Some(crate::CandidatePolicyState::Canary10),
            Self::Canary25 => Some(crate::CandidatePolicyState::Canary25),
            Self::Canary50 => Some(crate::CandidatePolicyState::Canary50),
            Self::ActiveValidation | Self::Completed => Some(crate::CandidatePolicyState::Active),
            Self::Staged | Self::Frozen => None,
        }
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
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanarySessionReservation {
    pub schema_version: u32,
    pub campaign_id: ContentHash,
    pub level: CanaryCampaignStatus,
    pub session_key: String,
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
    use super::{CanaryCampaignStatus, CanarySessionReservation, CanaryVerdict};
    use crate::{ContentHash, RunId, V2_DOMAIN_SCHEMA_VERSION};

    #[test]
    fn campaign_levels_are_ordered_and_terminal_states_have_no_successor() {
        assert_eq!(
            CanaryCampaignStatus::Staged.next(),
            Some(CanaryCampaignStatus::Canary10)
        );
        assert_eq!(
            CanaryCampaignStatus::Canary50.next(),
            Some(CanaryCampaignStatus::ActiveValidation)
        );
        assert_eq!(CanaryCampaignStatus::Completed.next(), None);
        assert!(CanaryCampaignStatus::Canary25.is_level());
        assert!(!CanaryCampaignStatus::Frozen.is_level());
    }

    #[test]
    fn session_reservation_rejects_duplicate_run_ids() {
        let run = RunId::new();
        let reservation = CanarySessionReservation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: ContentHash::of_bytes(b"campaign"),
            level: CanaryCampaignStatus::Canary10,
            session_key: "2026-08-25".to_owned(),
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
