// 文件导读：定义带版本、分类时点和多维概率分布的市场 Regime 快照。
// DecisionTime 快照可用于决策/检索；ExPost 快照只能留作事后研究，避免前视偏差。
//! Typed, Versioned, Decision-Time Aware Market Regime Snapshots.
//!
//! Enforces strict separation between DecisionTime (prior to decision_at, valid
//! for retrieval and risk scaling) and ExPost (subsequent market path, strictly
//! quarantined to research/attribution with lookahead bias prevention).

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{content_hash_json, ArtifactRef, ContentHash, DomainError, DOMAIN_SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegimeClassificationKind {
    DecisionTime,
    ExPost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegimeDistribution<T: Ord + Serialize> {
    pub probabilities_ppm: BTreeMap<T, u32>,
    pub dominant: T,
}

impl<T: Ord + Serialize> RegimeDistribution<T> {
    // 保存概率表和主导状态；不在构造阶段隐式修正概率，统一由 validate 检查。
    pub fn new(probabilities_ppm: BTreeMap<T, u32>, dominant: T) -> Self {
        Self {
            probabilities_ppm,
            dominant,
        }
    }

    // 检查概率非空、主导值存在且总和未超过允许的近似 ppm 上限。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.probabilities_ppm.is_empty() {
            return Err(DomainError::EmptyField {
                field: "regime_distribution.probabilities_ppm",
            });
        }
        if !self.probabilities_ppm.contains_key(&self.dominant) {
            return Err(DomainError::InvalidDistribution);
        }
        // 迭代器闭包把 u32 概率提升到 u64 后求和，避免累计溢出。
        let total: u64 = self.probabilities_ppm.values().map(|&v| v as u64).sum();
        if total > 1_000_005 {
            return Err(DomainError::InvalidDistribution);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendRegime {
    BullStrong,
    BullWeak,
    Neutral,
    BearWeak,
    BearStrong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolatilityRegime {
    Low,
    Normal,
    High,
    Extreme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRegime {
    Abundant,
    Normal,
    Stressed,
    Illiquid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreadthRegime {
    BroadParticipation,
    Selective,
    Divergent,
    NarrowConcentration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskRegime {
    RiskOn,
    Neutral,
    RiskOff,
    Crisis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegimeSnapshot {
    pub schema_version: u32,
    pub snapshot_hash: ContentHash,
    pub classification_kind: RegimeClassificationKind,
    pub decision_at: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub taxonomy_id: ContentHash,
    pub detector_id: String,
    pub detector_version: String,
    pub detector_hash: ContentHash,
    pub input_snapshot_refs: Vec<ArtifactRef>,
    pub input_snapshot_hash: ContentHash,
    pub trend: RegimeDistribution<TrendRegime>,
    pub volatility: RegimeDistribution<VolatilityRegime>,
    pub liquidity: RegimeDistribution<LiquidityRegime>,
    pub breadth: RegimeDistribution<BreadthRegime>,
    pub risk_state: RegimeDistribution<RiskRegime>,
    pub confidence_ppm: u32,
    pub entropy_ppm: u32,
}

impl RegimeSnapshot {
    // 用当前字段重算 snapshot_hash，再验证时间边界和所有维度分布。
    pub fn seal(mut self) -> Result<Self, DomainError> {
        self.snapshot_hash = self.identity_hash()?;
        self.validate()?;
        Ok(self)
    }

    // 计算不含 snapshot_hash 自引用的稳定身份哈希。
    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "classification_kind": self.classification_kind,
            "decision_at": self.decision_at,
            "as_of": self.as_of,
            "taxonomy_id": self.taxonomy_id,
            "detector_id": self.detector_id,
            "detector_version": self.detector_version,
            "detector_hash": self.detector_hash,
            "input_snapshot_refs": self.input_snapshot_refs,
            "input_snapshot_hash": self.input_snapshot_hash,
            "trend": self.trend,
            "volatility": self.volatility,
            "liquidity": self.liquidity,
            "breadth": self.breadth,
            "risk_state": self.risk_state,
            "confidence_ppm": self.confidence_ppm,
            "entropy_ppm": self.entropy_ppm,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    // 验证 schema、DecisionTime 的 as_of 截止、身份哈希和五个分布。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "regime_snapshot.schema_version",
            });
        }
        // Strict Look-ahead Bias Prevention:
        // DecisionTime regime snapshots can never use information as_of after decision_at.
        if self.classification_kind == RegimeClassificationKind::DecisionTime
            && self.as_of > self.decision_at
        {
            return Err(DomainError::DecisionTimeLookaheadViolation {
                decision_at: self.decision_at,
                as_of: self.as_of,
            });
        }
        if self.snapshot_hash != self.identity_hash()? {
            return Err(DomainError::InvalidContentHash);
        }

        self.trend.validate()?;
        self.volatility.validate()?;
        self.liquidity.validate()?;
        self.breadth.validate()?;
        self.risk_state.validate()?;

        Ok(())
    }

    /// Verifies whether this snapshot can participate in DecisionContext / retrieval.
    // 只有 DecisionTime 且信息不晚于调用方决策时点的快照可进入决策上下文。
    pub fn permits_decision_context(&self, decision_at: DateTime<Utc>) -> bool {
        self.classification_kind == RegimeClassificationKind::DecisionTime
            && self.as_of <= decision_at
            && self.decision_at <= decision_at
    }

    /// Set of canonical labels for compatibility checking across retrieval and lesson scopes.
    // 将五个主导枚举转换为基础标签和带维度前缀的标签，放入有序集合去重。
    pub fn canonical_regime_labels(&self) -> BTreeSet<String> {
        let mut labels = BTreeSet::new();

        let trend_str = match self.trend.dominant {
            TrendRegime::BullStrong => "bull_strong",
            TrendRegime::BullWeak => "bull_weak",
            TrendRegime::Neutral => "trend_neutral",
            TrendRegime::BearWeak => "bear_weak",
            TrendRegime::BearStrong => "bear_strong",
        };
        labels.insert(trend_str.to_owned());
        labels.insert(format!("trend:{}", trend_str));

        let vol_str = match self.volatility.dominant {
            VolatilityRegime::Low => "low_volatility",
            VolatilityRegime::Normal => "normal_volatility",
            VolatilityRegime::High => "high_volatility",
            VolatilityRegime::Extreme => "extreme_volatility",
        };
        labels.insert(vol_str.to_owned());
        labels.insert(format!("volatility:{}", vol_str));

        let liq_str = match self.liquidity.dominant {
            LiquidityRegime::Abundant => "abundant_liquidity",
            LiquidityRegime::Normal => "normal_liquidity",
            LiquidityRegime::Stressed => "stressed_liquidity",
            LiquidityRegime::Illiquid => "illiquid",
        };
        labels.insert(liq_str.to_owned());
        labels.insert(format!("liquidity:{}", liq_str));

        let breadth_str = match self.breadth.dominant {
            BreadthRegime::BroadParticipation => "broad_breadth",
            BreadthRegime::Selective => "selective_breadth",
            BreadthRegime::Divergent => "divergent_breadth",
            BreadthRegime::NarrowConcentration => "narrow_breadth",
        };
        labels.insert(breadth_str.to_owned());
        labels.insert(format!("breadth:{}", breadth_str));

        let risk_str = match self.risk_state.dominant {
            RiskRegime::RiskOn => "risk_on",
            RiskRegime::Neutral => "risk_neutral",
            RiskRegime::RiskOff => "risk_off",
            RiskRegime::Crisis => "crisis",
        };
        labels.insert(risk_str.to_owned());
        labels.insert(format!("risk:{}", risk_str));

        labels
    }
}
