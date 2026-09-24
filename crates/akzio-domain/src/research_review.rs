//! Bounded research refinement and immutable final-proposal review.
// 文件职责：定义研究提案审查、有限修订、补采意图和不可变 provenance 绑定所需的领域值类型与校验。
// 这些函数只做确定性值校验和哈希/集合计算；错误统一通过 Result fail closed，不执行 Store、模型或 Paper I/O。
use crate::{ArtifactKind, ArtifactRef, Asset, ContentHash, DecisionHorizon, DomainError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const REVIEWED_RESEARCH_CONTRACT_VERSION: u32 = 67;
pub const STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION: u32 = 69;
pub const RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID: &str = "research.proposal_reviewer";
pub const RESEARCH_SUPPLEMENT_RECIPE_ID: &str = "research.supplement";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchSettings {
    /// Revisions after the initial proposal; each revision gets its own review.
    pub max_proposal_revisions: u8,
}
impl Default for ResearchSettings {
    // 无输入，输出冻结的默认修订额度；默认值只描述新配置，不改写历史 Workflow。
    fn default() -> Self {
        Self {
            max_proposal_revisions: 2,
        }
    }
}
impl ResearchSettings {
    // 输入当前设置的借用，输出 Ok 或 InvalidBudget；节点上限是 Contract/Workflow 的硬边界。
    pub fn validate(&self) -> Result<(), DomainError> {
        // Full Paper graph: evidence + 12 horizon tasks + supplement +
        // proposal/review pairs + five terminal gates = 21 + 2 * revisions.
        if 21usize + 2 * usize::from(self.max_proposal_revisions) > 32 {
            return Err(DomainError::InvalidBudget {
                field: "agent.research.max_proposal_revisions exceeds 32 workflow nodes",
            });
        }
        Ok(())
    }
}
// 无输入，输出稳定排序的 17 个 proposal review scope key；集合值可重复计算且不依赖数组顺序。
pub fn proposal_review_keys() -> BTreeSet<String> {
    let mut keys = BTreeSet::from(["allocation.cash".to_owned()]);
    for asset in Asset::EXECUTABLE {
        keys.insert(format!("allocation.{}", asset.symbol()));
        for horizon in ["t1", "t3", "t5"] {
            keys.insert(format!("forecast.{}.{horizon}", asset.symbol()));
        }
    }
    keys
}

/// An explanation of a raw estimate, never an empirical calibration claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericEstimateBasis {
    pub scope: String,
    pub inputs: Vec<ArtifactRef>,
    pub units: String,
    pub method: String,
    pub assumptions: String,
    pub uncertainty: String,
}
// 输入 numeric basis 切片借用，输出 Result；迭代器同时检查 scope 完整性、重复项、文本和允许的引用 kind。
pub fn validate_numeric_bases(bases: &[NumericEstimateBasis]) -> Result<(), DomainError> {
    let scopes = bases
        .iter()
        .map(|b| b.scope.clone())
        .collect::<BTreeSet<_>>();
    if scopes != proposal_review_keys()
        || scopes.len() != bases.len()
        || bases.iter().any(|b| {
            // any 闭包只接受每个 basis 的借用；任一空字段、重复/缺失 scope 或未授权 kind 都使整体校验失败。
            b.inputs.is_empty()
                || [&b.units, &b.method, &b.assumptions, &b.uncertainty]
                    .into_iter()
                    .any(|s| s.trim().is_empty())
                || b.inputs.iter().any(|r| {
                    !matches!(
                        r.kind,
                        ArtifactKind::Claim
                            | ArtifactKind::Critique
                            | ArtifactKind::NormalizedEvidence
                            | ArtifactKind::SemanticDetail
                    )
                })
        })
    {
        return Err(DomainError::EmptyField { field: "proposal.numeric_basis requires all 12 forecasts and 5 allocations with input refs, units, method, assumptions and uncertainty" });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalAssessment {
    pub scope: String,
    pub accepted: bool,
    pub rationale: String,
    pub evidence_refs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<ProposalIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposalIssueCategory {
    SupportMissing,
    SourceQualification,
    ConflictingSupport,
    TemporalMismatch,
    UnitError,
    EstimateBasisMismatch,
    AllocationInconsistency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalIssue {
    pub category: ProposalIssueCategory,
    /// Semantic field path, independent of array ordering.
    pub field_path: String,
    pub correction_criterion: String,
    pub evidence_refs: Vec<ArtifactRef>,
}

impl ProposalIssue {
    // 输入 issue 借用和所属 scope，输出只由 typed category/field identity 决定的稳定 String；prose 改动不会改变 ID。
    pub fn stable_id(&self, scope: &str) -> String {
        // Typed category + field identity; prose changes never reset progress.
        ContentHash::of_bytes(
            serde_json::to_string(&(scope, &self.category, &self.field_path))
                .expect("serializable issue identity")
                .as_bytes(),
        )
        .to_string()
    }
}
/// Identity fields are filled by Rust after validating the model's assessments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReview {
    pub proposal: ArtifactRef,
    pub proposal_hash: ContentHash,
    pub manifest: ArtifactRef,
    pub contract_hash: ContentHash,
    pub assessments: Vec<ProposalAssessment>,
}
impl ProposalReview {
    // 输入 Contract version，输出结构化问题与基础 review 校验的 Result；旧版本保留历史解码兼容但不强加新 issue 规则。
    pub fn validate_for_contract(&self, version: u32) -> Result<(), DomainError> {
        self.validate()?;
        if version < STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION {
            return Ok(());
        }
        for assessment in &self.assessments {
            // 每个 assessment 借用其 issues；BTreeSet 只用于本 scope 内去重 stable_id，不改变输入顺序或内容。
            let mut ids = BTreeSet::new();
            if assessment.issues.len() > 3
                || assessment.accepted != assessment.issues.is_empty()
                || assessment.issues.iter().any(|issue| {
                    // issue 闭包同时检查 field path、correction criterion、唯一 ID 和 evidence kind，任一失败即拒绝。
                    let path_matches = issue.field_path == assessment.scope
                        || issue
                            .field_path
                            .starts_with(&format!("{}.", assessment.scope))
                        || issue
                            .field_path
                            .starts_with(&format!("numeric_basis.{}.", assessment.scope));
                    !path_matches
                        || issue.correction_criterion.trim().is_empty()
                        || !ids.insert(issue.stable_id(&assessment.scope))
                        || issue.evidence_refs.iter().any(|r| {
                            !matches!(
                                r.kind,
                                ArtifactKind::Claim
                                    | ArtifactKind::Critique
                                    | ArtifactKind::NormalizedEvidence
                                    | ArtifactKind::SemanticDetail
                            )
                        })
                })
            {
                return Err(DomainError::EmptyField {field:"proposal_review structured issues require a rejected scope, matching field, unique ID and correction criterion"});
            }
        }
        Ok(())
    }
    // 输入 review 借用，输出是否覆盖完整 scope、kind 和 rationale 的 Result；不判断模型方向是否正确。
    pub fn validate(&self) -> Result<(), DomainError> {
        let keys = self
            .assessments
            .iter()
            .map(|a| a.scope.clone())
            .collect::<BTreeSet<_>>();
        if self.proposal.kind != ArtifactKind::DecisionProposal
            || self.manifest.kind != ArtifactKind::ContextManifest
            || keys != proposal_review_keys()
            || keys.len() != self.assessments.len()
            || self
                .assessments
                .iter()
                .any(|a| a.rationale.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "proposal_review complete scope and binding",
            });
        }
        Ok(())
    }
    // 无额外输入，输出 bool；只有基础结构有效且所有 assessment 都 accepted 才返回 true。
    pub fn accepted(&self) -> bool {
        self.validate().is_ok() && self.assessments.iter().all(|a| a.accepted)
    }
    // 输入候选 proposal ref/hash 的借用，输出是否被这份已通过 review 精确授权；不扩大 Manifest/Contract 范围。
    pub fn authorizes(&self, proposal: &ArtifactRef, hash: &ContentHash) -> bool {
        self.accepted() && &self.proposal == proposal && &self.proposal_hash == hash
    }
}

/// Scope equality compares typed values, never fuzzy prose or array position.
// 输入 DecisionDraft 借用和 scope 字符串，输出带 value/basis 的 JSON Value；找不到的可选路径明确保留为 Null。
pub fn proposal_scope_value(proposal: &crate::DecisionDraft, scope: &str) -> serde_json::Value {
    let parts = scope.split('.').collect::<Vec<_>>();
    let value = match parts.as_slice() {
        ["forecast", asset, horizon] => proposal
            .forecasts
            .iter()
            .find(|f| {
                // find 闭包按 asset symbol 与序列化 horizon 精确匹配，不用模糊文本或数组位置。
                f.asset.symbol() == *asset
                    && serde_json::to_value(f.horizon)
                        .ok()
                        .as_ref()
                        .and_then(serde_json::Value::as_str)
                        == Some(*horizon)
            })
            .map(|f| serde_json::to_value(f).expect("forecast serialization")),
        ["allocation", "cash"] => proposal
            .research_allocation
            .as_ref()
            .map(|a| serde_json::json!(a.cash_weight_ppm)),
        ["allocation", asset] => proposal
            .research_allocation
            .as_ref()
            .and_then(|a| a.allocations.iter().find(|a| a.asset.symbol() == *asset))
            .map(|a| serde_json::to_value(a).expect("allocation serialization")),
        _ => None,
    };
    serde_json::json!({"value":value,"basis":proposal.numeric_basis.iter().find(|b| b.scope == scope)})
}

pub fn validate_proposal_revision(
    previous: &crate::DecisionDraft,
    next: &crate::DecisionDraft,
    review: &ProposalReview,
) -> Result<(), DomainError> {
    // 输入前后两个 draft 与当前 review 的借用，输出 revision 是否保持已接受 forecast/basis 不变的 Result。
    // Portfolio weights share a sum and may depend on any repaired forecast.
    // Unrelated accepted forecasts (including their basis) remain frozen.
    let model_owned = |proposal: &crate::DecisionDraft, scope: &str| {
        // 闭包复制 scope 的 typed projection 后移除 Rust 重绑的 calendar 字段，只比较模型可变部分。
        let mut value = proposal_scope_value(proposal, scope);
        if let Some(thesis) = value
            .pointer_mut("/value/thesis")
            .and_then(serde_json::Value::as_object_mut)
        {
            // These two fields are rebound from the governed calendar by Rust.
            thesis.remove("thesis_valid_until");
            thesis.remove("expected_holding_period_days");
        }
        value
    };
    if review.assessments.iter().any(|a| {
        // any 闭包只检查 accepted forecast；其余 scope 可修订，但已接受 forecast 的 model_owned 值必须相等。
        a.accepted
            && a.scope.starts_with("forecast.")
            && model_owned(previous, &a.scope) != model_owned(next, &a.scope)
    }) {
        return Err(DomainError::EmptyField {
            field: "proposal revision changed an accepted forecast or its numeric basis",
        });
    }
    Ok(())
}

pub fn proposal_revision_stagnated(
    previous: &crate::DecisionDraft,
    previous_review: &ProposalReview,
    current: &crate::DecisionDraft,
    current_review: &ProposalReview,
) -> bool {
    // 输入前后 draft/review 借用，输出是否在两个拒绝 review 间保持相同问题、引用和 scope 内容。
    if previous_review
        .validate_for_contract(STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION)
        .is_err()
        || current_review
            .validate_for_contract(STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION)
            .is_err()
        || previous_review.accepted()
        || current_review.accepted()
    {
        return false;
    }
    let fingerprint = |review: &ProposalReview, proposal: &crate::DecisionDraft| {
        // fingerprint 闭包只收集未接受 scope，并对 issue/ref 做排序去重，消除数组顺序带来的假变化。
        let mut entries = review.assessments.iter().filter(|a| !a.accepted).map(|a| {
            // 每个 assessment 闭包保留 scope 与 typed content；嵌套 map 闭包将 issue 引用规范化为值语义集合。
            let mut issues = a.issues.iter().map(|i| {
                let mut refs = i.evidence_refs.clone(); refs.sort(); refs.dedup();
                (i.stable_id(&a.scope),refs)
            }).collect::<Vec<_>>(); issues.sort();
            let mut refs = a.evidence_refs.clone(); refs.sort(); refs.dedup();
            (a.scope.clone(),serde_json::json!({"issues":issues,"refs":refs,"content":proposal_scope_value(proposal,&a.scope)}).to_string())
        }).collect::<Vec<_>>();
        entries.sort();
        entries
    };
    fingerprint(previous_review, previous) == fingerprint(current_review, current)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupplementalKind {
    News,
    Price,
    Macro,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupplementalIntent {
    pub kind: SupplementalKind,
    pub assets: Vec<Asset>,
    pub series: Vec<String>,
    pub query: String,
}
impl SupplementalIntent {
    // 输入补采意图借用，输出 Result；校验 query、唯一资产/序列、kind 与受支持宏观序列的组合边界。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.query.trim().is_empty()
            || self.assets.is_empty()
            || self.assets.iter().collect::<BTreeSet<_>>().len() != self.assets.len()
            || self.series.iter().collect::<BTreeSet<_>>().len() != self.series.len()
            || match self.kind {
                // Macro 必须带受支持的 FRED 序列；News/Price 不接受 series，避免把类型边界藏进字符串。
                SupplementalKind::Macro => {
                    self.series.is_empty()
                        || self.series.iter().any(|s| {
                            !matches!(s.as_str(), "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                        })
                }
                _ => !self.series.is_empty(),
            }
        {
            return Err(DomainError::EmptyField {
                field:
                    "supplemental intent: unique assets, supported kind/series and query required",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplementalDisposition {
    pub requester: ArtifactRef,
    pub horizon: DecisionHorizon,
    pub gap_index: usize,
    pub request_index: usize,
    pub resource: Option<String>,
    pub status: String,
    pub reason: String,
    pub evidence: Vec<ArtifactRef>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplementalRound {
    pub dispositions: Vec<SupplementalDisposition>,
    pub affected_horizons: BTreeSet<DecisionHorizon>,
    pub evidence: Vec<ArtifactRef>,
}

#[cfg(test)]
mod tests {
    // 测试职责：验证修订额度、review scope 数量和补采意图的 fail-closed 规则；不创建 Store/模型状态。
    use super::*;
    // 无输入测试，以断言验证默认额度、0 修订合法及 32 节点上限。
    #[test]
    fn revision_budget_is_bounded_and_zero_is_legal() {
        assert_eq!(ResearchSettings::default().max_proposal_revisions, 2);
        for n in 0..=5 {
            assert!(ResearchSettings {
                max_proposal_revisions: n
            }
            .validate()
            .is_ok());
        }
        assert!(ResearchSettings {
            max_proposal_revisions: 6
        }
        .validate()
        .is_err());
        assert_eq!(proposal_review_keys().len(), 17);
    }
    // 无输入测试，以断言验证重复资产和不支持的宏观序列被拒绝。
    #[test]
    fn supplemental_intent_rejects_duplicate_assets_and_unsupported_series() {
        let mut intent = SupplementalIntent {
            kind: SupplementalKind::News,
            assets: Asset::EXECUTABLE.to_vec(),
            series: vec![],
            query: "news".into(),
        };
        assert!(intent.validate().is_ok());
        intent.assets.push(Asset::Qqq);
        assert!(intent.validate().is_err());
        intent.assets = vec![Asset::Qqq];
        intent.kind = SupplementalKind::Macro;
        intent.series = vec!["unknown".into()];
        assert!(intent.validate().is_err());
    }
}
