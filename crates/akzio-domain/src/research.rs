// 文件导读：定义研究意图、EvidenceGround/Gap、Claim、Critique 和 Resolution。
// 每个公开校验都把模型提交限制在 Rust 已授权的证据 kind、资产/期限范围和预算内。
//! Typed, evidence-bound research artifacts.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, Asset, DecisionHorizon, DomainError, EvidenceNeed,
    DOMAIN_SCHEMA_VERSION,
};

pub const MAX_EVIDENCE_GAPS: usize = 2;
pub const STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID: &str = "candidate.structured_critique";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStance {
    Bullish,
    Bearish,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CritiqueSeverity {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClaimVerificationStatus {
    Supported,
    Contradicted,
    #[default]
    NotEnoughInformation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthority {
    Primary,
    Official,
    EstablishedSecondary,
    #[default]
    Unrated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TemporalValidity {
    ValidAtDecisionCutoff,
    Stale,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimVerificationEvidence {
    pub evidence: ArtifactRef,
    #[serde(default)]
    pub authority: SourceAuthority,
    #[serde(default)]
    pub temporal_validity: TemporalValidity,
}

impl ClaimVerificationEvidence {
    // 只接受标准化证据或语义细节引用，拒绝 RawEvidence 直接进入复核闭包。
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(
            self.evidence.kind,
            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
        ) {
            return Err(DomainError::EmptyField {
                field: "research.claim_verification.evidence",
            });
        }
        Ok(())
    }

    // authority 非 Unrated 且时间有效于决策 cutoff 时，才是当前权威验证引用。
    pub fn is_current_authoritative(&self) -> bool {
        self.authority != SourceAuthority::Unrated
            && self.temporal_validity == TemporalValidity::ValidAtDecisionCutoff
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGroundRole {
    #[default]
    Descriptive,
    Directional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGapImpact {
    #[default]
    Warning,
    BlocksDirectionalForecast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionDisposition {
    Accepted,
    Rebutted,
    Unresolved,
}

/// Historical proposal research lanes. Archived proposals may select a bounded subset
/// of these lanes, but it cannot invent a new source family or lane at run
/// time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchShard {
    PriceMarketStructure,
    Macro,
    FundamentalsSemiconductor,
    NewsEvent,
}

/// Rust-owned research request. It is lowered to an `EvidenceNeed` before a
/// workflow is installed; models may propose values but cannot widen them at
/// execution time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchIntent {
    pub schema_version: u32,
    pub source_family: String,
    pub resource: String,
    pub query: String,
    pub assets: BTreeSet<Asset>,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub max_age_secs: u64,
    pub max_results: u16,
}

impl ResearchIntent {
    // 依据 source_family 把意图归入固定研究 shard，不从 query 文本猜测类别。
    pub fn shard(&self) -> ResearchShard {
        match self.source_family.as_str() {
            "alpaca" => ResearchShard::PriceMarketStructure,
            "fred" => ResearchShard::Macro,
            "sec_edgar" => ResearchShard::FundamentalsSemiconductor,
            _ => ResearchShard::NewsEvent,
        }
    }

    // 校验 schema、文本/窗口/数量/来源白名单，并阻止用 Alpaca bars 请求新闻。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.source_family.trim().is_empty()
            || self.resource.trim().is_empty()
            || self.query.trim().is_empty()
            || self.resource.chars().count() > 2_048
            || self.query.chars().count() > 2_000
            || !(1..=86_400 * 7).contains(&self.max_age_secs)
            || !(1..=32).contains(&self.max_results)
        {
            return Err(DomainError::EmptyField {
                field: "research.intent",
            });
        }
        if !matches!(
            self.source_family.as_str(),
            "alpaca" | "sec_edgar" | "fred" | "news_web"
        ) {
            return Err(DomainError::EvidenceSourceNotAllowed(
                self.source_family.clone(),
            ));
        }
        // A market-price adapter cannot satisfy an explicit news acquisition.
        // This rejects the observed cross-domain request; it is not a general
        // natural-language classifier and does not manufacture a replacement.
        // lowercase 后用字符切分识别英文 news；这只是明确的跨域保护，不是语义分类器。
        let query = self.query.to_lowercase();
        if self.source_family == "alpaca"
            && (self.resource == "bars" || self.resource.starts_with("bars:"))
            && (query.contains("新闻")
                || query
                    .split(|c: char| !c.is_alphanumeric())
                    .any(|w| w == "news"))
        {
            return Err(DomainError::EmptyField {
                field: "research.intent.news_requires_news_source",
            });
        }
        if let (Some(start), Some(end)) = (self.window_start, self.window_end) {
            if end < start || end.signed_duration_since(start) > Duration::days(366) {
                // Report the rejected window. "budget ... must be positive" named
                // neither the dates nor the 366-day bound that actually failed.
                return Err(DomainError::InvalidEvidenceWindow {
                    field: "research.intent.window",
                    start: start.to_string(),
                    end: end.to_string(),
                });
            }
        }
        Ok(())
    }

    // 校验意图后降级为不含 query/资产窗口的 EvidenceNeed，供工作流授权使用。
    pub fn evidence_need(&self) -> Result<EvidenceNeed, DomainError> {
        self.validate()?;
        Ok(EvidenceNeed {
            schema_version: crate::DOMAIN_SCHEMA_VERSION,
            source_family: self.source_family.clone(),
            resource: self.resource.clone(),
            max_age_secs: self.max_age_secs,
        })
    }
}

/// A concrete statement of support attached to one governed evidence artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceGround {
    pub evidence: ArtifactRef,
    pub support: String,
    #[serde(default)]
    pub role: EvidenceGroundRole,
    #[serde(default)]
    pub assets: BTreeSet<Asset>,
    #[serde(default)]
    pub domain: Option<ResearchShard>,
}

impl EvidenceGround {
    // 证据必须是标准化/语义 kind，文本非空；Directional ground 必须声明资产范围。
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(
            self.evidence.kind,
            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
        ) || self.support.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.ground",
            });
        }
        if self.role == EvidenceGroundRole::Directional && self.assets.is_empty() {
            return Err(DomainError::InvalidEvidenceGroundScope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceGap {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supplemental_requests: Vec<crate::SupplementalIntent>,
    pub topic: String,
    pub rationale: String,
    #[serde(default)]
    pub impact: EvidenceGapImpact,
    /// Empty assets means all assets; empty horizons inherits the Claim horizon.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub assets: BTreeSet<Asset>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub horizons: BTreeSet<DecisionHorizon>,
    #[serde(default)]
    pub supplemental_needs: Vec<ResearchIntent>,
    /// Missing on historical payloads; new Contracts require an explicit classification.
    #[serde(default)]
    pub retriable: bool,
}

impl EvidenceGap {
    // 判断一个 gap 是否阻断指定资产/期限；空集合继承 Claim horizon 或覆盖全部资产。
    pub fn blocks_slot(
        &self,
        asset: Asset,
        horizon: DecisionHorizon,
        claim_horizon: DecisionHorizon,
    ) -> bool {
        self.impact == EvidenceGapImpact::BlocksDirectionalForecast
            && (self.assets.is_empty() || self.assets.contains(&asset))
            && if self.horizons.is_empty() {
                horizon == claim_horizon
            } else {
                self.horizons.contains(&horizon)
            }
    }
    // 校验主题/理由、可重试阻断缺口必须有补采请求，以及补采总量上限。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.topic.trim().is_empty() || self.rationale.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "research.evidence_gap",
            });
        }
        if self.retriable
            && self.impact == EvidenceGapImpact::BlocksDirectionalForecast
            && self.supplemental_needs.is_empty()
            && self.supplemental_requests.is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.evidence_gap.retriable_requires_supplemental_needs",
            });
        }
        if self.supplemental_needs.len() + self.supplemental_requests.len() > 8 {
            return Err(DomainError::InvalidBudget {
                field: "research.evidence_gap.supplemental_needs",
            });
        }
        for request in &self.supplemental_requests {
            request.validate()?;
        }
        for need in &self.supplemental_needs {
            need.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchClaim {
    pub schema_version: u32,
    pub topic: String,
    pub statement: String,
    pub horizon: DecisionHorizon,
    pub stance: ClaimStance,
    pub materiality_ppm: u32,
    pub confidence_ppm: u32,
    pub grounds: Vec<EvidenceGround>,
    pub evidence_gaps: Vec<EvidenceGap>,
}

impl ResearchClaim {
    // 组合通用身份、ppm、grounds 和 gaps 校验，保证 Claim 具备完整证据闭包。
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_research_identity(self.schema_version, &self.topic, &self.statement)?;
        validate_ppm(self.materiality_ppm, "research.claim.materiality_ppm")?;
        validate_ppm(self.confidence_ppm, "research.claim.confidence_ppm")?;
        validate_grounds(&self.grounds)?;
        validate_gaps(&self.evidence_gaps)
    }

    // 仅返回 Claim grounds 的去重 Artifact 引用，供 Artifact provenance 绑定。
    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        ground_refs(&self.grounds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchCritique {
    pub schema_version: u32,
    pub target: ArtifactRef,
    pub topic: String,
    pub severity: CritiqueSeverity,
    pub blocker: bool,
    pub rationale: String,
    pub grounds: Vec<EvidenceGround>,
    pub evidence_gaps: Vec<EvidenceGap>,
    #[serde(default)]
    pub verification_status: ClaimVerificationStatus,
    #[serde(default)]
    pub supporting_refs: Vec<ClaimVerificationEvidence>,
    #[serde(default)]
    pub conflicting_refs: Vec<ClaimVerificationEvidence>,
}

impl ResearchCritique {
    /// Return whether this critique's safety blocker applies to one concrete
    /// asset/horizon slot. A blocker with no directional gap is a genuine
    /// claim-wide blocker. When blocking gaps carry scope, the scope is the
    /// authority: an unrelated asset/horizon must not be rejected merely
    /// because the enclosing Claim has `blocker = true`.
    pub fn blocks_slot(
        &self,
        asset: Asset,
        horizon: DecisionHorizon,
        claim_horizon: DecisionHorizon,
    ) -> bool {
        // 非 blocker 直接放行；有范围的阻断 gap 按 gap 范围判断，无范围则是 Claim-wide。
        if !self.blocker {
            return false;
        }
        let blocking_gaps = self
            .evidence_gaps
            .iter()
            .filter(|gap| gap.impact == EvidenceGapImpact::BlocksDirectionalForecast)
            .collect::<Vec<_>>();
        if blocking_gaps.is_empty() {
            return true;
        }
        blocking_gaps
            .into_iter()
            .any(|gap| gap.blocks_slot(asset, horizon, claim_horizon))
    }

    // 校验目标 kind、grounds/gaps、验证引用闭包、blocker 一致性及 Supported/Contradicted 语义。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.target.kind != ArtifactKind::Claim
            || self.topic.trim().is_empty()
            || self.rationale.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.critique",
            });
        }
        if self.grounds.is_empty() && self.evidence_gaps.is_empty() {
            return Err(DomainError::EmptyField {
                field: "research.critique.grounds_or_gaps",
            });
        }
        if !self.grounds.is_empty() {
            validate_grounds(&self.grounds)?;
        }
        validate_gaps(&self.evidence_gaps)?;
        validate_verification_refs(&self.supporting_refs)?;
        validate_verification_refs(&self.conflicting_refs)?;
        // 用引用集合检查 supporting/conflicting refs 是否都来自本 Critique grounds。
        let ground_refs = self
            .grounds
            .iter()
            .map(|ground| &ground.evidence)
            .collect::<BTreeSet<_>>();
        // Name the offending reference. "ground_closure must not be empty" sent
        // repair rounds looking for a missing field while grounds were present
        // and the real defect was a verification ref citing evidence outside them.
        if let Some(reference) = self
            .supporting_refs
            .iter()
            .chain(self.conflicting_refs.iter())
            .find(|reference| !ground_refs.contains(&reference.evidence))
        {
            return Err(DomainError::VerificationRefOutsideGrounds {
                field: "research.claim_verification.ground_closure",
                evidence: format!(
                    "{} (kind {:?})",
                    reference.evidence.artifact_id.0, reference.evidence.kind
                ),
            });
        }
        if !self.blocker
            && self
                .evidence_gaps
                .iter()
                .any(|gap| gap.impact == EvidenceGapImpact::BlocksDirectionalForecast)
        {
            return Err(DomainError::EmptyField {
                field: "research.critique.blocking_gap_requires_blocker_true",
            });
        }
        match self.verification_status {
            ClaimVerificationStatus::Supported
                if self.supporting_refs.is_empty()
                    || !self.conflicting_refs.is_empty()
                    || self
                        .supporting_refs
                        .iter()
                        .any(|reference| !reference.is_current_authoritative()) =>
            {
                Err(DomainError::EmptyField {
                    field: "research.claim_verification.supported",
                })
            }
            ClaimVerificationStatus::Contradicted if self.conflicting_refs.is_empty() => {
                Err(DomainError::EmptyField {
                    field: "research.claim_verification.contradicted",
                })
            }
            _ => Ok(()),
        }
    }

    // 汇总 target、grounds 和验证引用，排序去重后形成稳定 source closure。
    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        let mut refs = BTreeSet::from([self.target.clone()]);
        refs.extend(ground_refs(&self.grounds));
        refs.extend(
            self.supporting_refs
                .iter()
                .chain(self.conflicting_refs.iter())
                .map(|reference| reference.evidence.clone()),
        );
        refs.into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchResolution {
    pub schema_version: u32,
    pub claim: ArtifactRef,
    pub critique: ArtifactRef,
    pub disposition: ResolutionDisposition,
    pub rationale: String,
    pub grounds: Vec<EvidenceGround>,
    pub remaining_gaps: Vec<EvidenceGap>,
}

impl ResearchResolution {
    // 校验 Claim/Critique kind、理由和剩余证据缺口，保留未解决状态而不强行清空。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.claim.kind != ArtifactKind::Claim
            || self.critique.kind != ArtifactKind::Critique
            || self.rationale.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.resolution",
            });
        }
        validate_grounds(&self.grounds)?;
        validate_gaps(&self.remaining_gaps)
    }

    // 返回 Claim、Critique 及 Resolution grounds 的去重引用集合。
    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        let mut refs = BTreeSet::from([self.claim.clone(), self.critique.clone()]);
        refs.extend(ground_refs(&self.grounds));
        refs.into_iter().collect()
    }
}

fn validate_research_identity(
    schema_version: u32,
    topic: &str,
    statement: &str,
) -> Result<(), DomainError> {
    // 研究对象的 schema、topic、statement 都必须非空。
    if schema_version != DOMAIN_SCHEMA_VERSION
        || topic.trim().is_empty()
        || statement.trim().is_empty()
    {
        return Err(DomainError::EmptyField {
            field: "research.claim",
        });
    }
    Ok(())
}

fn validate_ppm(value: u32, field: &'static str) -> Result<(), DomainError> {
    // 统一限制 ppm 不超过 100%，并把具体字段带回错误。
    if value > 1_000_000 {
        return Err(DomainError::InvalidBudget { field });
    }
    Ok(())
}

fn validate_grounds(grounds: &[EvidenceGround]) -> Result<(), DomainError> {
    // 至少一个 ground；逐个校验并用集合拒绝同一 Artifact 的重复引用。
    if grounds.is_empty() {
        return Err(DomainError::EmptyField {
            field: "research.grounds",
        });
    }
    let mut evidence = BTreeSet::new();
    for ground in grounds {
        ground.validate()?;
        if !evidence.insert(ground.evidence.clone()) {
            return Err(DomainError::EmptyField {
                field: "research.grounds",
            });
        }
    }
    Ok(())
}

fn validate_gaps(gaps: &[EvidenceGap]) -> Result<(), DomainError> {
    // gaps 上限为 MAX_EVIDENCE_GAPS，并逐项复用 EvidenceGap 的补采约束。
    if gaps.len() > MAX_EVIDENCE_GAPS {
        return Err(DomainError::InvalidBudget {
            field: "research.evidence_gaps",
        });
    }
    for gap in gaps {
        gap.validate()?;
    }
    Ok(())
}

fn validate_verification_refs(references: &[ClaimVerificationEvidence]) -> Result<(), DomainError> {
    // 校验每个验证引用并拒绝同一 evidence 重复出现。
    let mut seen = BTreeSet::new();
    for reference in references {
        reference.validate()?;
        if !seen.insert(reference.evidence.clone()) {
            return Err(DomainError::EmptyField {
                field: "research.claim_verification.references",
            });
        }
    }
    Ok(())
}

fn ground_refs(grounds: &[EvidenceGround]) -> Vec<ArtifactRef> {
    // 提取 grounds 的 ArtifactRef，排序去重后转回 Vec 供序列化/血缘使用。
    grounds
        .iter()
        .map(|ground| ground.evidence.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod acquisition_semantics_tests {
    use super::*;
    #[test]
    // 覆盖跨域采集保护、日期窗口错误和不可重试缺口的语义。
    fn news_cannot_be_acquired_as_price_bars_but_gap_may_remain_unfilled() {
        let intent = ResearchIntent {
            schema_version: DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".into(),
            resource: "bars".into(),
            query: "获取资产专属新闻".into(),
            assets: BTreeSet::new(),
            window_start: None,
            window_end: None,
            max_age_secs: 86400,
            max_results: 1,
        };
        assert!(intent.validate().is_err());

        let start = "2025-09-21T00:00:00Z".parse().unwrap();
        let end = "2026-09-22T07:02:34Z".parse().unwrap();
        let invalid_window = ResearchIntent {
            source_family: "fred".into(),
            resource: "series:VIXCLS".into(),
            query: "refresh VIX".into(),
            window_start: Some(start),
            window_end: Some(end),
            ..intent.clone()
        };
        assert!(matches!(
            invalid_window.validate(),
            Err(DomainError::InvalidEvidenceWindow {
                field: "research.intent.window",
                start: actual_start,
                end: actual_end,
            }) if actual_start == start.to_string() && actual_end == end.to_string()
        ));

        let gap = EvidenceGap {
            supplemental_requests: Vec::new(),
            topic: "news unavailable".into(),
            rationale: "No news adapter available; directional support remains insufficient".into(),
            impact: EvidenceGapImpact::BlocksDirectionalForecast,
            assets: BTreeSet::new(),
            horizons: BTreeSet::new(),
            supplemental_needs: vec![],
            retriable: false,
        };
        assert!(gap.validate().is_ok());
        let mut retriable = gap;
        retriable.retriable = true;
        assert!(retriable.validate().is_err());
        retriable.impact = EvidenceGapImpact::Warning;
        assert!(retriable.validate().is_ok());
    }
}

#[cfg(test)]
mod critique_blocking_gap_tests {
    use super::*;
    #[test]
    // 已支持的价格 ground 不能消除同资产同期限的方向阻断 gap。
    fn supported_price_does_not_clear_direction_blocking_gap() {
        let mut critique: ResearchCritique = serde_json::from_value(serde_json::json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "target": {"artifact_id":"a".repeat(64),"kind":"claim"},
            "topic":"QQQ t1 price", "severity":"low", "rationale":"Price supports the scoped claim; news remains missing",
            "verification_status":"supported", "blocker":false,
            "grounds":[{"evidence":{"artifact_id":"b".repeat(64),"kind":"normalized_evidence"},"role":"directional","assets":["QQQ"],"domain":"price_market_structure","support":"Price-only support"}],
            "supporting_refs":[{"evidence":{"artifact_id":"b".repeat(64),"kind":"normalized_evidence"},"authority":"official","temporal_validity":"valid_at_decision_cutoff"}],
            "conflicting_refs":[],
            "evidence_gaps":[{"topic":"news","rationale":"Unavailable news","assets":["QQQ"],"horizons":["t1"],"impact":"blocks_directional_forecast","supplemental_needs":[]}]
        })).unwrap();
        assert!(matches!(
            critique.validate(),
            Err(DomainError::EmptyField {
                field: "research.critique.blocking_gap_requires_blocker_true"
            })
        ));
        critique.blocker = true;
        assert!(critique.validate().is_ok());
    }

    #[test]
    // 验证引用越出 grounds 时，错误必须指出具体外部 Artifact。
    fn verification_ref_outside_grounds_names_the_offending_artifact() {
        let ground_id = "b".repeat(64);
        let outside_id = "c".repeat(64);
        let critique: ResearchCritique = serde_json::from_value(serde_json::json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "target": {"artifact_id":"a".repeat(64),"kind":"claim"},
            "topic":"QQQ t1 review", "severity":"low", "rationale":"supported",
            "verification_status":"supported", "blocker":false,
            "grounds":[{"evidence":{"artifact_id":ground_id,"kind":"normalized_evidence"},"role":"directional","assets":["QQQ"],"domain":"price_market_structure","support":"Price support"}],
            "supporting_refs":[{"evidence":{"artifact_id":outside_id,"kind":"normalized_evidence"},"authority":"official","temporal_validity":"valid_at_decision_cutoff"}],
            "conflicting_refs":[], "evidence_gaps":[]
        })).unwrap();
        assert!(matches!(
            critique.validate(),
            Err(DomainError::VerificationRefOutsideGrounds {
                field: "research.claim_verification.ground_closure",
                evidence,
            }) if evidence.contains(&"c".repeat(64)) && evidence.contains("NormalizedEvidence")
        ));
    }
}
