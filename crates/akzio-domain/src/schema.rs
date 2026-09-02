//! Canonical artifact, contract, workflow, and authority schema.
//!
//! This module is intentionally introduced beside the former vocabulary while the
//! workspace is migrated. Its types are the only types new runtime code may use;
//! the old document/role/task types are removed once every crate crosses this seam.

use serde::{Deserialize, Serialize};

use crate::DomainError;

pub const SCHEMA_VERSION: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLimits {
    pub global_leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub paired_index_ppm: u32,
}

impl FactorLimits {
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.global_leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.paired_index_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_limits",
            });
        }
        Ok(())
    }
}
