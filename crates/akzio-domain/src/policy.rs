//! R0's stable index of v2 architectural invariants.
//!
//! It records implementation ownership and the first phase that must enforce
//! each rule. The rules themselves remain enforced by their owning crate.

pub use crate::decision::{HardBlocker, MaterialConflict, SoftWarning};
pub use crate::execution::FactorExposure;
pub use crate::schema::{BlockerSeverity, DecisionBlocker, FactorLimits};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefactorPhase {
    R0,
    R1,
    R2,
    R3,
    R4,
    R5,
    R6,
    R7,
    R8,
    R9,
    R10,
}

impl RefactorPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R0 => "R0",
            Self::R1 => "R1",
            Self::R2 => "R2",
            Self::R3 => "R3",
            Self::R4 => "R4",
            Self::R5 => "R5",
            Self::R6 => "R6",
            Self::R7 => "R7",
            Self::R8 => "R8",
            Self::R9 => "R9",
            Self::R10 => "R10",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum V2Invariant {
    AssetUniverse,
    PaperOnlyEndpoint,
    RustAuthority,
    StoreAuthority,
    PermitAtomicity,
    DurableEvents,
    ContextGrants,
    EvidenceProvenance,
    ContractAuthority,
    WorkflowGates,
    CanonicalLearning,
    SchedulerFencing,
    FreezeControl,
    HttpControlPlane,
}

impl V2Invariant {
    pub const ALL: [Self; 14] = [
        Self::AssetUniverse,
        Self::PaperOnlyEndpoint,
        Self::RustAuthority,
        Self::StoreAuthority,
        Self::PermitAtomicity,
        Self::DurableEvents,
        Self::ContextGrants,
        Self::EvidenceProvenance,
        Self::ContractAuthority,
        Self::WorkflowGates,
        Self::CanonicalLearning,
        Self::SchedulerFencing,
        Self::FreezeControl,
        Self::HttpControlPlane,
    ];

    pub const fn owner(self) -> &'static str {
        match self {
            Self::AssetUniverse | Self::RustAuthority => "akzio-domain",
            Self::PaperOnlyEndpoint => "akzio-execution",
            Self::StoreAuthority | Self::PermitAtomicity | Self::DurableEvents => "akzio-store",
            Self::ContextGrants => "akzio-context",
            Self::EvidenceProvenance => "akzio-store + akzio-context",
            Self::ContractAuthority => "akzio-research",
            Self::WorkflowGates => "akzio-runtime",
            Self::CanonicalLearning => "akzio-learning",
            Self::SchedulerFencing => "akzio-store + akzio-daemon",
            Self::FreezeControl => "akzio-daemon",
            Self::HttpControlPlane => "akzio-daemon + akzio-cli",
        }
    }

    pub const fn first_enforced_in(self) -> RefactorPhase {
        match self {
            Self::AssetUniverse | Self::RustAuthority => RefactorPhase::R1,
            Self::StoreAuthority | Self::PermitAtomicity | Self::DurableEvents => RefactorPhase::R2,
            Self::ContextGrants | Self::EvidenceProvenance => RefactorPhase::R3,
            Self::ContractAuthority => RefactorPhase::R4,
            Self::WorkflowGates => RefactorPhase::R5,
            Self::CanonicalLearning => RefactorPhase::R6,
            Self::PaperOnlyEndpoint => RefactorPhase::R7,
            Self::SchedulerFencing | Self::FreezeControl => RefactorPhase::R8,
            Self::HttpControlPlane => RefactorPhase::R9,
        }
    }

    pub const fn test_category(self) -> &'static str {
        match self {
            Self::AssetUniverse => "schema/config/execution",
            Self::PaperOnlyEndpoint => "adapter URL",
            Self::RustAuthority | Self::ContractAuthority => "authority negative",
            Self::StoreAuthority | Self::PermitAtomicity => "transaction/failure injection",
            Self::DurableEvents => "event closure/replay",
            Self::ContextGrants => "grant scope/expiry",
            Self::EvidenceProvenance => "reference integrity/Doctor",
            Self::WorkflowGates => "graph lowering/gate bypass",
            Self::CanonicalLearning => "canonicality/promotion",
            Self::SchedulerFencing => "singleton/epoch/recovery",
            Self::FreezeControl | Self::HttpControlPlane => "daemon transport/control",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::V2Invariant;

    #[test]
    fn invariant_registry_has_unique_complete_metadata() {
        let mut unique = BTreeSet::new();

        for invariant in V2Invariant::ALL {
            assert!(unique.insert(invariant));
            assert!(!invariant.owner().is_empty());
            assert!(!invariant.test_category().is_empty());
            assert!(invariant.first_enforced_in().as_str().starts_with('R'));
        }

        assert_eq!(unique.len(), V2Invariant::ALL.len());
    }
}
