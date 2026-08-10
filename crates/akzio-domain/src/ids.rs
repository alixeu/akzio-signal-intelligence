//! Stable identifiers that are not content-addressed artifacts.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::{
    AttemptId, ContractId, DecisionId, DocumentId, ExecutionPlanId, LeaseId, MemoryId, RunId,
    TaskId, TopologyId,
};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

id_type!(EventId);
id_type!(ExperienceId);
id_type!(OutcomeId);
id_type!(EvaluationId);
id_type!(PolicyTransitionId);
id_type!(PaperCommitmentId);
id_type!(PaperRepriceId);
id_type!(ReconciliationId);
