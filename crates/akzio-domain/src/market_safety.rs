// 文件导读：定义金融内容分类、共识独立性、容量、合规、依赖健康和 PreTradeSafety
// 的纯领域约束；它们只产出可审计的允许/阻断事实，不推断法律结论或市场真相。
//! Cross-cycle financial safety contracts.
//!
//! These types keep content integrity, consensus independence, capacity,
//! compliance, and third-party degradation on Rust-owned deterministic
//! surfaces. They do not infer legal conclusions or market truth.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Asset, ContentHash, DomainError, MoneyMicros, TargetPortfolio, WeightPpm};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationClassification {
    Public,
    Licensed,
    Confidential,
    SuspectedMnpi,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthorityClass {
    Regulator,
    Exchange,
    Issuer,
    FundSponsor,
    EstablishedNews,
    SocialMedia,
    Unknown,
}

impl SourceAuthorityClass {
    // 监管方、交易所、发行人和基金发起方才算高影响内容的一手来源。
    pub const fn is_primary_for_high_impact(self) -> bool {
        matches!(
            self,
            Self::Regulator | Self::Exchange | Self::Issuer | Self::FundSponsor
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinancialContentIndicator {
    InstructionLikeContent,
    UnicodeAnomaly,
    HiddenContent,
    EntityIdentifierMismatch,
    UnverifiedHighImpactClaim,
    SyndicatedDuplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinancialContentAssessment {
    pub source_origin: String,
    #[serde(default)]
    pub syndication_parent: Option<String>,
    pub content_similarity_cluster: ContentHash,
    pub first_seen_at: DateTime<Utc>,
    #[serde(default)]
    pub canonical_entity_ids: BTreeSet<String>,
    #[serde(default)]
    pub indicators: BTreeSet<FinancialContentIndicator>,
    pub authority: SourceAuthorityClass,
    pub information_classification: InformationClassification,
    pub independent_confirmation_clusters: u16,
    pub high_impact: bool,
}

impl FinancialContentAssessment {
    // 校验来源、联动实体和独立确认簇的基本字段/数量边界。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.source_origin.trim().is_empty()
            || self
                .syndication_parent
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self
                .canonical_entity_ids
                .iter()
                .any(|value| value.trim().is_empty())
            || self.independent_confirmation_clusters > 1_000
        {
            return Err(DomainError::EmptyField {
                field: "financial_content_assessment",
            });
        }
        Ok(())
    }

    // 依次拒绝未知/敏感信息、危险内容指示器，以及未达到一手确认阈值的高影响内容。
    pub fn blocks_trading(&self, policy: &FinancialContentPolicy) -> bool {
        if self.validate().is_err() || policy.validate().is_err() {
            return true;
        }
        if matches!(
            self.information_classification,
            InformationClassification::Confidential
                | InformationClassification::SuspectedMnpi
                | InformationClassification::Unknown
        ) || (!policy.allow_licensed_information
            && self.information_classification == InformationClassification::Licensed)
        {
            return true;
        }
        // any 闭包把四类不可直接用于交易的内容指示器收敛为一个阻断条件。
        if self.indicators.iter().any(|indicator| {
            matches!(
                indicator,
                FinancialContentIndicator::InstructionLikeContent
                    | FinancialContentIndicator::UnicodeAnomaly
                    | FinancialContentIndicator::HiddenContent
                    | FinancialContentIndicator::EntityIdentifierMismatch
            )
        }) {
            return true;
        }
        self.high_impact
            && (!self.authority.is_primary_for_high_impact()
                || self.independent_confirmation_clusters
                    < policy.minimum_high_impact_confirmation_clusters)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinancialContentPolicy {
    pub minimum_high_impact_confirmation_clusters: u16,
    pub allow_licensed_information: bool,
}

impl FinancialContentPolicy {
    // 高影响内容至少需要一个且不超过 16 个独立确认簇。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.minimum_high_impact_confirmation_clusters == 0
            || self.minimum_high_impact_confirmation_clusters > 16
        {
            return Err(DomainError::InvalidBudget {
                field: "financial_content_policy.confirmation_clusters",
            });
        }
        Ok(())
    }
}

impl Default for FinancialContentPolicy {
    // 默认允许 Licensed，但要求两个高影响确认簇。
    fn default() -> Self {
        Self {
            minimum_high_impact_confirmation_clusters: 2,
            allow_licensed_information: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvidenceContribution {
    pub agent_id: String,
    #[serde(default)]
    pub capability_snapshot_hash: Option<ContentHash>,
    pub evidence_clusters: BTreeSet<ContentHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusDiversityAssessment {
    pub contributions: Vec<AgentEvidenceContribution>,
    pub participant_count: u16,
    pub capability_observation_complete: bool,
    pub unique_capability_snapshots: u16,
    pub unique_evidence_clusters: u16,
    pub maximum_allowed_overlap_ppm: u32,
    pub maximum_source_overlap_ppm: u32,
    pub independent: bool,
    pub raw_confidence_ppm: u32,
    pub effective_confidence_ppm: u32,
    pub confidence_capped: bool,
}

impl ConsensusDiversityAssessment {
    // 从 Agent 贡献计算参与者、能力快照、证据簇、最大重叠和独立性结论。
    pub fn from_contributions(
        contributions: &[AgentEvidenceContribution],
        maximum_allowed_overlap_ppm: u32,
    ) -> Result<Self, DomainError> {
        if contributions.is_empty() || maximum_allowed_overlap_ppm > WeightPpm::SCALE {
            return Err(DomainError::InvalidBudget {
                field: "consensus_diversity",
            });
        }
        let mut agent_ids = BTreeSet::new();
        let mut capability_snapshots = BTreeSet::new();
        let mut all_clusters = BTreeSet::new();
        for contribution in contributions {
            if contribution.agent_id.trim().is_empty()
                || contribution.evidence_clusters.is_empty()
                || !agent_ids.insert(contribution.agent_id.clone())
            {
                return Err(DomainError::EmptyField {
                    field: "consensus_diversity.contribution",
                });
            }
            capability_snapshots.extend(contribution.capability_snapshot_hash.iter().cloned());
            all_clusters.extend(contribution.evidence_clusters.iter().cloned());
        }

        // 两两计算 intersection/union 的 ppm 重叠；没有可除的 union 时保守取满额。
        let mut maximum_overlap = 0_u32;
        for left_index in 0..contributions.len() {
            for right_index in left_index + 1..contributions.len() {
                let left = &contributions[left_index].evidence_clusters;
                let right = &contributions[right_index].evidence_clusters;
                let union = left.union(right).count();
                let intersection = left.intersection(right).count();
                let overlap = intersection
                    .saturating_mul(WeightPpm::SCALE as usize)
                    .checked_div(union)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(WeightPpm::SCALE);
                maximum_overlap = maximum_overlap.max(overlap);
            }
        }

        let participant_count =
            u16::try_from(contributions.len()).map_err(|_| DomainError::InvalidBudget {
                field: "consensus_diversity.participant_count",
            })?;
        let unique_capability_snapshots =
            u16::try_from(capability_snapshots.len()).map_err(|_| DomainError::InvalidBudget {
                field: "consensus_diversity.capability_snapshots",
            })?;
        let unique_evidence_clusters =
            u16::try_from(all_clusters.len()).map_err(|_| DomainError::InvalidBudget {
                field: "consensus_diversity.evidence_clusters",
            })?;
        let capability_observation_complete = contributions
            .iter()
            .all(|contribution| contribution.capability_snapshot_hash.is_some());
        let independent = participant_count == 1
            || (capability_observation_complete
                && unique_capability_snapshots > 1
                && unique_evidence_clusters > 1
                && unique_evidence_clusters >= participant_count.min(2)
                && maximum_overlap <= maximum_allowed_overlap_ppm);
        Ok(Self {
            contributions: contributions.to_vec(),
            participant_count,
            capability_observation_complete,
            unique_capability_snapshots,
            unique_evidence_clusters,
            maximum_allowed_overlap_ppm,
            maximum_source_overlap_ppm: maximum_overlap,
            independent,
            raw_confidence_ppm: 0,
            effective_confidence_ppm: 0,
            confidence_capped: false,
        })
    }

    // 记录原始/有效 confidence，并标记 Rust 是否进行了 cap。
    pub fn record_confidence(
        &mut self,
        raw_confidence_ppm: u32,
        effective_confidence_ppm: u32,
    ) -> Result<(), DomainError> {
        if raw_confidence_ppm > WeightPpm::SCALE || effective_confidence_ppm > raw_confidence_ppm {
            return Err(DomainError::InvalidBudget {
                field: "consensus_diversity.confidence",
            });
        }
        self.raw_confidence_ppm = raw_confidence_ppm;
        self.effective_confidence_ppm = effective_confidence_ppm;
        self.confidence_capped = effective_confidence_ppm < raw_confidence_ppm;
        Ok(())
    }

    // 重新从 contributions 推导期望值，拒绝调用方伪造派生字段。
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut expected =
            Self::from_contributions(&self.contributions, self.maximum_allowed_overlap_ppm)?;
        expected.record_confidence(self.raw_confidence_ppm, self.effective_confidence_ppm)?;
        if &expected != self {
            return Err(DomainError::InvalidBudget {
                field: "consensus_diversity.assessment",
            });
        }
        Ok(())
    }

    // 只有多个参与者且满足 independent 条件时，才允许共识 confidence boost。
    pub const fn permits_consensus_confidence_boost(&self) -> bool {
        self.participant_count > 1 && self.independent
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityPolicy {
    pub maximum_market_participation_ppm: u32,
    pub maximum_estimated_slippage_ppm: u32,
    pub impact_slope_ppm: u32,
}

impl CapacityPolicy {
    // 校验参与率、滑点和 impact slope 的 ppm 上限。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.maximum_market_participation_ppm == 0
            || self.maximum_market_participation_ppm > WeightPpm::SCALE
            || self.maximum_estimated_slippage_ppm > WeightPpm::SCALE
            || self.impact_slope_ppm > 10 * WeightPpm::SCALE
        {
            return Err(DomainError::InvalidBudget {
                field: "capacity_policy",
            });
        }
        Ok(())
    }
}

impl Default for CapacityPolicy {
    // 提供 5% 参与率、1% 滑点和 20% impact slope 的保守默认值。
    fn default() -> Self {
        Self {
            maximum_market_participation_ppm: 50_000,
            maximum_estimated_slippage_ppm: 10_000,
            impact_slope_ppm: 200_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetCapacityAssessment {
    pub asset: Asset,
    pub aggregate_order_notional: MoneyMicros,
    pub average_daily_dollar_volume: MoneyMicros,
    pub expected_market_participation_ppm: u32,
    pub estimated_slippage_ppm: u32,
    pub estimated_fill_probability_ppm: u32,
    pub within_capacity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityScenario {
    pub account_notional: MoneyMicros,
    pub homogeneous_agent_count: u32,
    pub assets: Vec<AssetCapacityAssessment>,
    pub within_capacity: bool,
}

pub fn assess_capacity(
    policy: &CapacityPolicy,
    target: &TargetPortfolio,
    account_notional: MoneyMicros,
    homogeneous_agent_count: u32,
    average_daily_dollar_volume: &BTreeMap<Asset, MoneyMicros>,
) -> Result<CapacityScenario, DomainError> {
    // 按目标权重和同质 Agent 数计算每项 aggregate notional、ADV 参与率、滑点和填充概率。
    policy.validate()?;
    target.validate_universe()?;
    if account_notional.0 <= 0 || homogeneous_agent_count == 0 {
        return Err(DomainError::InvalidBudget {
            field: "capacity_scenario",
        });
    }
    let mut assets = Vec::new();
    // 只为非零资产生成容量行；缺 ADV 的资产直接返回错误而不猜测流动性。
    for asset in Asset::EXECUTABLE {
        let weight = i128::from(target.weights[&asset].0);
        if weight == 0 {
            continue;
        }
        let aggregate = i128::from(account_notional.0)
            .checked_mul(weight)
            .and_then(|value| value.checked_mul(i128::from(homogeneous_agent_count)))
            .and_then(|value| value.checked_div(i128::from(WeightPpm::SCALE)))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(DomainError::InvalidBudget {
                field: "capacity_scenario.notional",
            })?;
        let adv = average_daily_dollar_volume
            .get(&asset)
            .filter(|value| value.0 > 0)
            .ok_or(DomainError::InvalidBudget {
                field: "capacity_scenario.average_daily_dollar_volume",
            })?;
        let participation = i128::from(aggregate)
            .checked_mul(i128::from(WeightPpm::SCALE))
            .and_then(|value| value.checked_div(i128::from(adv.0)))
            .and_then(|value| u32::try_from(value.max(0)).ok())
            .unwrap_or(u32::MAX);
        let slippage = u64::from(participation).saturating_mul(u64::from(policy.impact_slope_ppm))
            / u64::from(WeightPpm::SCALE);
        let slippage = u32::try_from(slippage).unwrap_or(u32::MAX);
        let fill_probability = WeightPpm::SCALE.saturating_sub(participation.min(WeightPpm::SCALE));
        let within_capacity = participation <= policy.maximum_market_participation_ppm
            && slippage <= policy.maximum_estimated_slippage_ppm;
        assets.push(AssetCapacityAssessment {
            asset,
            aggregate_order_notional: MoneyMicros(aggregate),
            average_daily_dollar_volume: *adv,
            expected_market_participation_ppm: participation,
            estimated_slippage_ppm: slippage,
            estimated_fill_probability_ppm: fill_probability,
            within_capacity,
        });
    }
    let within_capacity = !assets.is_empty() && assets.iter().all(|asset| asset.within_capacity);
    Ok(CapacityScenario {
        account_notional,
        homogeneous_agent_count,
        assets,
        within_capacity,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceViolation {
    RestrictedAsset,
    WatchListRequiresReview,
    SuspectedMnpi,
    UnknownInformationClass,
    SelfTrade,
    ExcessiveCancellation,
    SpoofingOrLayeringPattern,
    MarkingTheCloseRisk,
    PrearrangedTradeRisk,
    FrontRunningRisk,
    SurveillanceCoverageIncomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceControl {
    InformationClassification,
    RestrictedAndWatchLists,
    SelfTrade,
    CancellationRatio,
    SpoofingAndLayering,
    MarkingTheClose,
    PrearrangedTrading,
    FrontRunning,
}

impl ComplianceControl {
    pub const ALL: [Self; 8] = [
        Self::InformationClassification,
        Self::RestrictedAndWatchLists,
        Self::SelfTrade,
        Self::CancellationRatio,
        Self::SpoofingAndLayering,
        Self::MarkingTheClose,
        Self::PrearrangedTrading,
        Self::FrontRunning,
    ];

    pub const PAPER_BASELINE: [Self; 5] = [
        Self::InformationClassification,
        Self::RestrictedAndWatchLists,
        Self::SelfTrade,
        Self::CancellationRatio,
        Self::SpoofingAndLayering,
    ];
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplianceActivitySnapshot {
    pub recent_submissions: u32,
    pub recent_cancellations: u32,
    pub self_trade_detected: bool,
    pub spoofing_or_layering_pattern: bool,
    pub marking_the_close_risk: bool,
    pub prearranged_trade_risk: bool,
    pub front_running_risk: bool,
    #[serde(default)]
    pub available_controls: BTreeSet<ComplianceControl>,
}

impl ComplianceActivitySnapshot {
    // 生成 Paper 默认可用的五项基础合规控制集合。
    pub fn paper_baseline() -> Self {
        Self {
            available_controls: ComplianceControl::PAPER_BASELINE.into_iter().collect(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplianceActionPolicy {
    #[serde(default)]
    pub restricted_assets: BTreeSet<Asset>,
    #[serde(default)]
    pub watch_list_assets: BTreeSet<Asset>,
    pub maximum_cancel_ratio_ppm: u32,
    pub permit_licensed_information: bool,
    #[serde(default = "paper_required_compliance_controls")]
    pub required_controls: BTreeSet<ComplianceControl>,
}

impl ComplianceActionPolicy {
    // 校验撤单率上限、restricted/watch list 不重叠及必需控制集合。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.maximum_cancel_ratio_ppm > WeightPpm::SCALE
            || !self.restricted_assets.is_disjoint(&self.watch_list_assets)
            || self.required_controls.is_empty()
        {
            return Err(DomainError::InvalidBudget {
                field: "compliance_action_policy",
            });
        }
        Ok(())
    }

    // 按资产、信息分类、可用控制和活动快照收集所有合规违规项。
    pub fn assess(
        &self,
        asset: Asset,
        information: InformationClassification,
        activity: &ComplianceActivitySnapshot,
    ) -> Result<BTreeSet<ComplianceViolation>, DomainError> {
        self.validate()?;
        let mut violations = BTreeSet::new();
        if !self
            .required_controls
            .is_subset(&activity.available_controls)
        {
            violations.insert(ComplianceViolation::SurveillanceCoverageIncomplete);
        }
        if self.restricted_assets.contains(&asset) {
            violations.insert(ComplianceViolation::RestrictedAsset);
        }
        if self.watch_list_assets.contains(&asset) {
            violations.insert(ComplianceViolation::WatchListRequiresReview);
        }
        match information {
            InformationClassification::SuspectedMnpi | InformationClassification::Confidential => {
                violations.insert(ComplianceViolation::SuspectedMnpi);
            }
            InformationClassification::Unknown => {
                violations.insert(ComplianceViolation::UnknownInformationClass);
            }
            InformationClassification::Licensed if !self.permit_licensed_information => {
                violations.insert(ComplianceViolation::UnknownInformationClass);
            }
            InformationClassification::Public | InformationClassification::Licensed => {}
        }
        if activity.self_trade_detected {
            violations.insert(ComplianceViolation::SelfTrade);
        }
        // submissions 为零时把非零撤单视为满额撤单率；否则用 u64 计算 ppm 后收窄为 u32。
        let cancel_ratio = if activity.recent_submissions == 0 {
            if activity.recent_cancellations == 0 {
                0
            } else {
                WeightPpm::SCALE
            }
        } else {
            u32::try_from(
                u64::from(activity.recent_cancellations)
                    .saturating_mul(u64::from(WeightPpm::SCALE))
                    / u64::from(activity.recent_submissions),
            )
            .unwrap_or(u32::MAX)
        };
        if cancel_ratio > self.maximum_cancel_ratio_ppm {
            violations.insert(ComplianceViolation::ExcessiveCancellation);
        }
        if activity.spoofing_or_layering_pattern {
            violations.insert(ComplianceViolation::SpoofingOrLayeringPattern);
        }
        if activity.marking_the_close_risk {
            violations.insert(ComplianceViolation::MarkingTheCloseRisk);
        }
        if activity.prearranged_trade_risk {
            violations.insert(ComplianceViolation::PrearrangedTradeRisk);
        }
        if activity.front_running_risk {
            violations.insert(ComplianceViolation::FrontRunningRisk);
        }
        Ok(violations)
    }

    // 把合规要求提升为完整八项市场完整性控制。
    pub fn require_full_market_integrity(&mut self) {
        self.required_controls = ComplianceControl::ALL.into_iter().collect();
    }
}

impl Default for ComplianceActionPolicy {
    // 默认使用 Paper baseline、无 restricted/watch list，允许 Licensed 信息。
    fn default() -> Self {
        Self {
            restricted_assets: BTreeSet::new(),
            watch_list_assets: BTreeSet::new(),
            maximum_cancel_ratio_ppm: 500_000,
            permit_licensed_information: true,
            required_controls: paper_required_compliance_controls(),
        }
    }
}

fn paper_required_compliance_controls() -> BTreeSet<ComplianceControl> {
    // 返回固定的 Paper baseline 控制集合，供 serde 默认值和 Default 共用。
    ComplianceControl::PAPER_BASELINE.into_iter().collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    ModelProvider,
    NewsProvider,
    MarketData,
    MacroData,
    Broker,
    Dns,
    MarketClock,
    Storage,
    Identity,
    ComplianceData,
}

impl DependencyKind {
    pub const ALL: [Self; 10] = [
        Self::ModelProvider,
        Self::NewsProvider,
        Self::MarketData,
        Self::MacroData,
        Self::Broker,
        Self::Dns,
        Self::MarketClock,
        Self::Storage,
        Self::Identity,
        Self::ComplianceData,
    ];

    // News/Macro/DNS/Identity 可被声明为 NotRequired/NotApplicable，其余依赖不可静默失活。
    const fn may_be_inactive(self) -> bool {
        matches!(
            self,
            Self::NewsProvider | Self::MacroData | Self::Dns | Self::Identity
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyHealthStatus {
    Healthy,
    Degraded,
    Partial,
    Unavailable,
    Stale,
    NotRequired,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyDegradationClass {
    Read,
    Decision,
    Execution,
    Reconciliation,
    Compliance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyIncident {
    pub kind: DependencyKind,
    pub class: DependencyDegradationClass,
    pub status: DependencyHealthStatus,
    pub configured_service: String,
    pub actual_service: String,
    pub observed_at: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    FailClosed,
    RiskReductionOnly,
    EquivalentServiceOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencySnapshot {
    pub kind: DependencyKind,
    pub provider: String,
    pub service: String,
    pub version: String,
    pub data_classification: String,
    pub expected_freshness_secs: u64,
    pub observed_at: DateTime<Utc>,
    pub status: DependencyHealthStatus,
    pub fallback_policy: FallbackPolicy,
    pub configured_service: String,
    pub actual_service: String,
    #[serde(default)]
    pub fourth_party_dependencies: BTreeSet<String>,
}

impl DependencySnapshot {
    // 校验 provider/service/version、freshness、实际/配置服务和失活状态合法性。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.provider.trim().is_empty()
            || self.service.trim().is_empty()
            || self.version.trim().is_empty()
            || self.data_classification.trim().is_empty()
            || self.expected_freshness_secs == 0
            || self.configured_service.trim().is_empty()
            || self.actual_service.trim().is_empty()
            || self
                .fourth_party_dependencies
                .iter()
                .any(|value| value.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "dependency_snapshot",
            });
        }
        if matches!(
            self.status,
            DependencyHealthStatus::NotRequired | DependencyHealthStatus::NotApplicable
        ) && (!self.kind.may_be_inactive() || self.configured_service != self.actual_service)
        {
            return Err(DomainError::InvalidBudget {
                field: "dependency_snapshot.inactive_status",
            });
        }
        Ok(())
    }

    // 只有配置未切换且依赖健康/未过期（或允许失活）时才允许新增风险。
    pub fn permits_new_risk(&self, now: DateTime<Utc>) -> bool {
        if self.validate().is_err() || self.configured_service != self.actual_service {
            return false;
        }
        match self.status {
            DependencyHealthStatus::Healthy => {
                now.signed_duration_since(self.observed_at)
                    .num_seconds()
                    .unsigned_abs()
                    <= self.expected_freshness_secs
            }
            DependencyHealthStatus::NotRequired | DependencyHealthStatus::NotApplicable => {
                self.kind.may_be_inactive()
            }
            DependencyHealthStatus::Degraded
            | DependencyHealthStatus::Partial
            | DependencyHealthStatus::Unavailable
            | DependencyHealthStatus::Stale => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyClosure {
    pub captured_at: DateTime<Utc>,
    pub dependencies: BTreeMap<DependencyKind, DependencySnapshot>,
}

impl DependencyClosure {
    // 要求十类依赖全部存在，并确认 key/kind 一致且观测不晚于 closure capture。
    pub fn validate(&self) -> Result<(), DomainError> {
        if DependencyKind::ALL
            .iter()
            .any(|kind| !self.dependencies.contains_key(kind))
        {
            return Err(DomainError::EmptyField {
                field: "dependency_closure.required",
            });
        }
        for (kind, snapshot) in &self.dependencies {
            snapshot.validate()?;
            if kind != &snapshot.kind || snapshot.observed_at > self.captured_at {
                return Err(DomainError::InvalidBudget {
                    field: "dependency_closure.snapshot",
                });
            }
        }
        Ok(())
    }

    // 所有依赖都必须各自允许新增风险。
    pub fn permits_new_risk(&self, now: DateTime<Utc>) -> bool {
        self.validate().is_ok()
            && self
                .dependencies
                .values()
                .all(|dependency| dependency.permits_new_risk(now))
    }

    /// Risk reduction may proceed without a currently healthy model provider,
    /// but never without current market, broker, clock, storage, and compliance
    /// dependencies.
    // 风险减少只依赖市场、Broker、时钟、存储和合规这五类当前有效依赖。
    pub fn permits_risk_reduction(&self, now: DateTime<Utc>) -> bool {
        self.validate().is_ok()
            && [
                DependencyKind::MarketData,
                DependencyKind::Broker,
                DependencyKind::MarketClock,
                DependencyKind::Storage,
                DependencyKind::ComplianceData,
            ]
            .into_iter()
            .all(|kind| {
                self.dependencies
                    .get(&kind)
                    .is_some_and(|dependency| dependency.permits_new_risk(now))
            })
    }

    /// Materialize every unavailable, stale, or silently switched dependency
    /// as an explicit incident.  `PreTradeSafetySnapshot` persists the source
    /// closure, so callers can reproduce this list without a parallel state
    /// store.
    // 为每个 stale/switched/非健康依赖生成显式 incident，保留 configured/actual service 差异。
    pub fn incidents(&self, now: DateTime<Utc>) -> Vec<DependencyIncident> {
        let mut incidents = Vec::new();
        for (kind, snapshot) in &self.dependencies {
            let stale = now
                .signed_duration_since(snapshot.observed_at)
                .num_seconds()
                .unsigned_abs()
                > snapshot.expected_freshness_secs;
            let switched = snapshot.configured_service != snapshot.actual_service;
            let inactive = matches!(
                snapshot.status,
                DependencyHealthStatus::NotRequired | DependencyHealthStatus::NotApplicable
            );
            if (snapshot.status == DependencyHealthStatus::Healthy && !stale && !switched)
                || (inactive && !switched)
            {
                continue;
            }
            // 优先报告服务切换，其次是 freshness 超时，最后保留 provider 状态的具体解释。
            let reason = if switched {
                "configured service differs from actual service"
            } else if stale {
                "dependency observation exceeded freshness budget"
            } else {
                match snapshot.status {
                    DependencyHealthStatus::Healthy => "dependency healthy",
                    DependencyHealthStatus::Degraded => "dependency degraded",
                    DependencyHealthStatus::Partial => "dependency response partial",
                    DependencyHealthStatus::Unavailable => "dependency unavailable",
                    DependencyHealthStatus::Stale => "dependency reported stale",
                    DependencyHealthStatus::NotRequired => "dependency not required",
                    DependencyHealthStatus::NotApplicable => "dependency not applicable",
                }
            };
            incidents.push(DependencyIncident {
                kind: *kind,
                class: dependency_degradation_class(*kind),
                status: snapshot.status,
                configured_service: snapshot.configured_service.clone(),
                actual_service: snapshot.actual_service.clone(),
                observed_at: snapshot.observed_at,
                reason: reason.to_owned(),
            });
        }
        incidents
    }
}

const fn dependency_degradation_class(kind: DependencyKind) -> DependencyDegradationClass {
    // 将依赖类别映射到读、决策、执行、对账或合规影响面。
    match kind {
        DependencyKind::NewsProvider | DependencyKind::MacroData | DependencyKind::Dns => {
            DependencyDegradationClass::Read
        }
        DependencyKind::ModelProvider => DependencyDegradationClass::Decision,
        DependencyKind::MarketData
        | DependencyKind::Broker
        | DependencyKind::MarketClock
        | DependencyKind::Identity => DependencyDegradationClass::Execution,
        DependencyKind::Storage => DependencyDegradationClass::Reconciliation,
        DependencyKind::ComplianceData => DependencyDegradationClass::Compliance,
    }
}

/// Durable result of deterministic pre-trade capacity, compliance, and
/// dependency checks. Missing evidence is explicit and never grants approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreTradeSafetySnapshot {
    pub ordered_assets: BTreeSet<Asset>,
    pub information_classifications: BTreeMap<Asset, InformationClassification>,
    pub recent_compliance_activity: ComplianceActivitySnapshot,
    pub dependency_closure: DependencyClosure,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_scenario: Option<CapacityScenario>,
    #[serde(default)]
    pub missing_average_daily_dollar_volume: BTreeSet<Asset>,
    #[serde(default)]
    pub compliance_violations_by_asset: BTreeMap<Asset, BTreeSet<ComplianceViolation>>,
    #[serde(default)]
    pub missing_information_classifications: BTreeSet<Asset>,
    pub risk_increasing: bool,
    pub dependency_permits_new_risk: bool,
    pub dependency_permits_risk_reduction: bool,
    pub permits_execution: bool,
    pub permits_risk_increasing: bool,
}

impl PreTradeSafetySnapshot {
    // 校验订单资产集合、逐资产信息/合规覆盖，以及 capacity/dependency 派生许可布尔值。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.ordered_assets.is_empty()
            || self
                .ordered_assets
                .iter()
                .any(|asset| !Asset::EXECUTABLE.contains(asset))
            || self
                .information_classifications
                .keys()
                .any(|asset| !self.ordered_assets.contains(asset))
            || self
                .compliance_violations_by_asset
                .keys()
                .collect::<BTreeSet<_>>()
                != self.ordered_assets.iter().collect::<BTreeSet<_>>()
        {
            return Err(DomainError::InvalidBudget {
                field: "pretrade_safety.assets",
            });
        }
        // 先分别计算合规和容量许可，再按 risk_increasing 选择新增风险或风险减少依赖。
        let compliance_permits = self.missing_information_classifications.is_empty()
            && !self.compliance_violations_by_asset.is_empty()
            && self
                .compliance_violations_by_asset
                .values()
                .all(BTreeSet::is_empty);
        let capacity_permits = !self.risk_increasing
            || (self
                .capacity_scenario
                .as_ref()
                .is_some_and(|scenario| scenario.within_capacity)
                && self.missing_average_daily_dollar_volume.is_empty());
        let expected_execution = compliance_permits
            && capacity_permits
            && if self.risk_increasing {
                self.dependency_permits_new_risk
            } else {
                self.dependency_permits_risk_reduction
            };
        let expected_risk_increasing = self.risk_increasing && expected_execution;
        if self.permits_execution != expected_execution
            || self.permits_risk_increasing != expected_risk_increasing
        {
            return Err(DomainError::InvalidBudget {
                field: "pretrade_safety.permission",
            });
        }
        Ok(())
    }
}
