//! Rust-owned execution gate policy.
//!
//! The model never supplies these limits. They are evaluated against the
//! typed `ExecutionContext` before a Paper commitment can be created.

// 文件导读：ExecutionGatePolicy 是模型不能覆盖的二次闸门，负责因子暴露、配对暴露、
// 换手、mandate、capacity 和 compliance 的组合限制。它只从已构造的账户/目标及外部
// 证据计算 blocker 或 MandateAssessment，不创建订单、不写 Commitment。

use akzio_domain::{
    AccountSnapshot, Asset, CapacityPolicy, ComplianceActionPolicy, DomainError, FactorExposure,
    FactorLimits, HardBlocker, MandateAssessment, MandateSnapshot, TargetPortfolio, WeightPpm,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGatePolicy {
    pub factor_limits: FactorLimits,
    pub max_turnover_ppm: u32,
    pub mandate: MandateSnapshot,
    pub capacity: CapacityPolicy,
    pub compliance: ComplianceActionPolicy,
}

impl ExecutionGatePolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        // 先委托领域策略检查，再验证 ppm 上限；非法配置在运行时构造阶段失败，而不是
        // 等到某个订单路径才表现为偶发 blocker。
        self.factor_limits.validate()?;
        self.mandate.validate()?;
        self.capacity.validate()?;
        self.compliance.validate()?;
        if self.max_turnover_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "execution_gate_policy.max_turnover_ppm",
            });
        }
        Ok(())
    }

    pub fn blockers_for(&self, exposure: &FactorExposure, turnover_ppm: u32) -> Vec<HardBlocker> {
        // 因子/配对/换手是可并列存在的硬阻断，使用 Vec 保留每个独立原因，便于
        // ExecutionContext 记录完整失败面而不是只留下第一个错误。
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

    pub fn assess_mandate(
        &self,
        account: &AccountSnapshot,
        target: &TargetPortfolio,
        projected_drawdown_ppm: u32,
        turnover_ppm: u32,
    ) -> MandateAssessment {
        // 从当前账户持仓重建 before portfolio，再交给 mandate 比较 before→target；负
        // 持仓被截为零只影响评估输入，不会替账户修正或产生平仓订单。
        let mut before = TargetPortfolio::zeroed();
        if account.equity.0 > 0 {
            for asset in Asset::EXECUTABLE {
                let market_value = account
                    .positions
                    .get(&asset)
                    .map_or(0_i64, |position| position.market_value.0.max(0));
                let weight = (i128::from(market_value) * i128::from(WeightPpm::SCALE)
                    / i128::from(account.equity.0))
                .clamp(0, i128::from(WeightPpm::SCALE)) as u32;
                before.weights.insert(asset, WeightPpm(weight));
            }
        }
        self.mandate.assess(
            &before,
            target,
            WeightPpm(projected_drawdown_ppm.min(WeightPpm::SCALE)),
            WeightPpm(turnover_ppm.min(WeightPpm::SCALE)),
        )
    }
}

impl Default for ExecutionGatePolicy {
    fn default() -> Self {
        // 默认 Gate 限制仍是 fail-closed 的业务上限，不能被理解为已经获得 Paper approval。
        Self {
            factor_limits: FactorLimits {
                global_leveraged_equity_ppm: 500_000,
                nasdaq_ppm: 500_000,
                semiconductor_ppm: 500_000,
                paired_index_ppm: 500_000,
            },
            max_turnover_ppm: 500_000,
            mandate: MandateSnapshot::default(),
            capacity: CapacityPolicy::default(),
            compliance: ComplianceActionPolicy::default(),
        }
    }
}
