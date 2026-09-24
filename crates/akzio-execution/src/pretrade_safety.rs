//! Deterministic pre-trade safety composition.
//!
//! This module combines domain-owned capacity, compliance, and dependency
//! contracts into one fail-closed assessment. Missing market or compliance
//! data never grants execution authorization.

// 文件导读：本模块把容量、信息分类、合规活动和依赖闭包合成一个安全快照。输入由
// ExecutionGate 以外部采集结果提供，Rust 再依据目标是否增加风险选择“新风险”或
// “风险降低”许可；缺少成交量、分类、依赖许可或任何合规结果时，assessment 保留
// 缺口并拒绝执行，而不是把缺失解释成安全。

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    assess_capacity, Asset, CapacityPolicy, ComplianceActionPolicy, ComplianceActivitySnapshot,
    DependencyClosure, DomainError, InformationClassification, MoneyMicros, PreTradeSafetySnapshot,
    TargetPortfolio,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreTradeSafetyPolicy {
    pub capacity: CapacityPolicy,
    pub compliance: ComplianceActionPolicy,
}

impl PreTradeSafetyPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        // 先验证容量和合规策略本身，确保后面每个资产的 assessment 都使用已知边界。
        self.capacity.validate()?;
        self.compliance.validate()?;
        Ok(())
    }

    pub fn assess(
        &self,
        input: &PreTradeSafetyInput,
    ) -> Result<PreTradeSafetyAssessment, DomainError> {
        // 先检查账户、资产集合和参与者数量，再以 BTreeSet/BTreeMap 累积缺失项与违规项；
        // 最终 permits_execution 同时要求数据完整、容量可行、合规无违规和依赖许可。
        self.validate()?;
        input.target.validate_universe()?;
        if input.account_equity.0 <= 0
            || input.homogeneous_agent_count == 0
            || input.ordered_assets.is_empty()
            || input
                .ordered_assets
                .iter()
                .any(|asset| !Asset::EXECUTABLE.contains(asset))
        {
            return Err(DomainError::InvalidBudget {
                field: "pretrade_safety_input",
            });
        }

        let active_target_assets = Asset::EXECUTABLE
            .into_iter()
            .filter(|asset| input.target.weights[asset].0 > 0)
            .collect::<BTreeSet<_>>();
        let missing_average_daily_dollar_volume = if input.risk_increasing {
            active_target_assets
                .iter()
                .filter(|asset| {
                    input
                        .average_daily_dollar_volume
                        .get(asset)
                        .is_none_or(|value| value.0 <= 0)
                })
                .copied()
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        let capacity_scenario =
            if input.risk_increasing && missing_average_daily_dollar_volume.is_empty() {
                Some(assess_capacity(
                    &self.capacity,
                    &input.target,
                    input.account_equity,
                    input.homogeneous_agent_count,
                    &input.average_daily_dollar_volume,
                )?)
            } else {
                None
            };

        let mut missing_information_classifications = BTreeSet::new();
        let mut compliance_violations_by_asset = BTreeMap::new();
        for asset in input.ordered_assets.iter().copied() {
            let information = input
                .information_classifications
                .get(&asset)
                .copied()
                .unwrap_or_else(|| {
                    missing_information_classifications.insert(asset);
                    InformationClassification::Unknown
                });
            let violations =
                self.compliance
                    .assess(asset, information, &input.recent_compliance_activity)?;
            compliance_violations_by_asset.insert(asset, violations);
        }

        let dependency_permits_new_risk = input.dependency_closure.permits_new_risk(input.now);
        let dependency_permits_risk_reduction =
            input.dependency_closure.permits_risk_reduction(input.now);
        let capacity_permits_execution = !input.risk_increasing
            || capacity_scenario
                .as_ref()
                .is_some_and(|scenario| scenario.within_capacity);
        let compliance_permits_execution = !compliance_violations_by_asset.is_empty()
            && compliance_violations_by_asset
                .values()
                .all(BTreeSet::is_empty);
        let permits_execution = (!input.risk_increasing
            || missing_average_daily_dollar_volume.is_empty())
            && missing_information_classifications.is_empty()
            && capacity_permits_execution
            && compliance_permits_execution
            && if input.risk_increasing {
                dependency_permits_new_risk
            } else {
                dependency_permits_risk_reduction
            };
        let permits_risk_increasing = input.risk_increasing && permits_execution;

        let assessment = PreTradeSafetySnapshot {
            ordered_assets: input.ordered_assets.clone(),
            information_classifications: input
                .information_classifications
                .iter()
                .filter(|(asset, _)| input.ordered_assets.contains(asset))
                .map(|(asset, classification)| (*asset, *classification))
                .collect(),
            recent_compliance_activity: input.recent_compliance_activity.clone(),
            dependency_closure: input.dependency_closure.clone(),
            capacity_scenario,
            missing_average_daily_dollar_volume,
            compliance_violations_by_asset,
            missing_information_classifications,
            risk_increasing: input.risk_increasing,
            dependency_permits_new_risk,
            dependency_permits_risk_reduction,
            permits_execution,
            permits_risk_increasing,
        };
        assessment.validate()?;
        Ok(assessment)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreTradeSafetyInput {
    pub target: TargetPortfolio,
    pub ordered_assets: BTreeSet<Asset>,
    pub risk_increasing: bool,
    pub account_equity: MoneyMicros,
    pub homogeneous_agent_count: u32,
    pub average_daily_dollar_volume: BTreeMap<Asset, MoneyMicros>,
    pub information_classifications: BTreeMap<Asset, InformationClassification>,
    pub recent_compliance_activity: ComplianceActivitySnapshot,
    pub dependency_closure: DependencyClosure,
    pub now: DateTime<Utc>,
}

/// Inputs acquired outside the allocation engine. The execution gate combines
/// them with the Rust-derived target, order sides, and current account equity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreTradeSafetyEvidence {
    // 这些字段是 Gate 刷新得到的外部事实投影；它们不携带订单权限，权限仍由下方
    // assess 根据 target 与 now 重新组合并验证。
    pub homogeneous_agent_count: u32,
    pub average_daily_dollar_volume: BTreeMap<Asset, MoneyMicros>,
    pub information_classifications: BTreeMap<Asset, InformationClassification>,
    pub recent_compliance_activity: ComplianceActivitySnapshot,
    pub dependency_closure: DependencyClosure,
}

pub type PreTradeSafetyAssessment = PreTradeSafetySnapshot;
