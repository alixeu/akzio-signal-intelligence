//! Stable identifiers that are not content-addressed artifacts.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::{AttemptId, ContractId, DecisionId, LeaseId, MemoryId, RunId, TaskId, TopologyId};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                let value = Uuid::new_v4().simple().to_string();
                Self(value[..16].to_owned())
            }

            pub fn short_id(&self) -> String {
                self.0
                    .chars()
                    .filter(|character| *character != '-')
                    .take(16)
                    .collect()
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

id_type!(ExperienceId);
id_type!(OutcomeId);
id_type!(EvaluationId);
id_type!(PolicyTransitionId);
id_type!(LessonId);
id_type!(PaperCommitmentId);
id_type!(PaperRepriceId);
id_type!(ReconciliationId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_domain_ids_are_sixteen_lowercase_hex_characters() {
        for value in [OutcomeId::new().0, PolicyTransitionId::new().0] {
            assert_eq!(value.len(), 16);
            assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(value, value.to_ascii_lowercase());
        }
    }
}
