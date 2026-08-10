//! Rust-owned execution gate policy.
//!
//! The model never supplies these limits. They are evaluated against the
//! typed `ExecutionContext` before a Paper commitment can be created.

use akzio_domain::{DomainError, FactorExposure, FactorLimits, HardBlocker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGatePolicy {
    pub factor_limits: FactorLimits,
    pub max_turnover_ppm: u32,
}

impl ExecutionGatePolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.factor_limits.validate()?;
        if self.max_turnover_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "execution_gate_policy.max_turnover_ppm",
            });
        }
        Ok(())
    }

    pub fn blockers_for(&self, exposure: &FactorExposure, turnover_ppm: u32) -> Vec<HardBlocker> {
        let mut blockers = Vec::new();
        if exposure.leveraged_equity_ppm > self.factor_limits.global_leveraged_equity_ppm
            || exposure.nasdaq_ppm > self.factor_limits.nasdaq_ppm
            || exposure.semiconductor_ppm > self.factor_limits.semiconductor_ppm
        {
            blockers.push(HardBlocker::FactorLimit);
        }
        if exposure.tqqq_qqq_pair_ppm > self.factor_limits.paired_index_ppm
            || exposure.soxl_soxx_pair_ppm > self.factor_limits.paired_index_ppm
        {
            blockers.push(HardBlocker::PairExposureLimit);
        }
        if turnover_ppm > self.max_turnover_ppm {
            blockers.push(HardBlocker::TurnoverLimit);
        }
        blockers
    }
}

#[cfg(test)]
mod tests {
    use akzio_domain::{FactorExposure, FactorLimits, HardBlocker};

    use super::ExecutionGatePolicy;

    fn policy() -> ExecutionGatePolicy {
        ExecutionGatePolicy {
            factor_limits: FactorLimits {
                global_leveraged_equity_ppm: 100,
                nasdaq_ppm: 100,
                semiconductor_ppm: 100,
                paired_index_ppm: 100,
            },
            max_turnover_ppm: 100,
        }
    }

    #[test]
    fn factor_pair_and_turnover_limits_have_distinct_typed_blockers() {
        let blockers = policy().blockers_for(
            &FactorExposure {
                leveraged_equity_ppm: 101,
                nasdaq_ppm: 0,
                semiconductor_ppm: 0,
                tqqq_qqq_pair_ppm: 101,
                soxl_soxx_pair_ppm: 0,
            },
            101,
        );

        assert_eq!(
            blockers,
            vec![
                HardBlocker::FactorLimit,
                HardBlocker::PairExposureLimit,
                HardBlocker::TurnoverLimit,
            ]
        );
    }
}
