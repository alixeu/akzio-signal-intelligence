// 文件导读：定义 Context 选择排序、信任分类、提示注入指标和子任务投影过滤。
// 这些辅助函数只缩小候选集合，不创建授权；最终读取仍由 ContextBroker Grant 校验。
use super::*;

pub(super) fn context_rank(artifact: &Artifact) -> u8 {
    // 给证据、研究产物、学习产物和其他类型固定优先级，数值越小越先选。
    match artifact.kind {
        ArtifactKind::NormalizedEvidence => 0,
        ArtifactKind::SemanticDetail => 1,
        ArtifactKind::Claim | ArtifactKind::Critique => 2,
        ArtifactKind::Lesson
        | ArtifactKind::Retrospective
        | ArtifactKind::Experience
        | ArtifactKind::CandidatePolicy
        | ArtifactKind::Evaluation => 3,
        _ => 4,
    }
}

pub(super) fn purpose_rank(purpose: &str, artifact: &Artifact) -> u8 {
    // Critic/Synthesizer 按其必需输入重排；其他 purpose 复用通用 context_rank。
    match purpose {
        RESEARCH_CRITIC_RECIPE_ID => match artifact.kind {
            ArtifactKind::Claim => 0,
            ArtifactKind::NormalizedEvidence => 1,
            ArtifactKind::SemanticDetail => 2,
            ArtifactKind::DeliberationNote => 3,
            _ => 4,
        },
        RESEARCH_SYNTHESIZER_RECIPE_ID => match artifact.kind {
            ArtifactKind::Critique => 0,
            ArtifactKind::Claim => 1,
            ArtifactKind::NormalizedEvidence => 2,
            ArtifactKind::SemanticDetail => 3,
            ArtifactKind::DeliberationNote => 4,
            _ => 5,
        },
        _ => context_rank(artifact),
    }
}

pub(super) const fn context_trust(kind: ArtifactKind) -> ContextTrust {
    // 原始/标准化证据保持不可信数据标记，治理产物才可标为 GovernedArtifact。
    match kind {
        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail => {
            ContextTrust::UntrustedEvidence
        }
        _ => ContextTrust::GovernedArtifact,
    }
}

pub(super) fn instruction_indicators(value: &Value) -> Vec<String> {
    // 递归收集字符串后匹配固定危险短语，并以 BTreeSet 去重且最多保留八项。
    let mut strings = Vec::new();
    collect_strings(value, &mut strings);
    let mut indicators = BTreeSet::new();
    for text in strings {
        let normalized = text.to_ascii_lowercase();
        for indicator in [
            "ignore previous",
            "ignore all previous",
            "ignore prior instructions",
            "system prompt",
            "developer message",
            "reveal credentials",
            "reveal secrets",
            "call the tool",
            "execute tool",
            "run command",
            "override risk",
            "bypass risk",
            "disable risk",
            "ignore risk",
            "place an order",
            "buy soxl",
            "sell soxl",
        ] {
            if normalized.contains(indicator) {
                indicators.insert(indicator.to_owned());
            }
        }
    }
    indicators.into_iter().take(8).collect()
}

pub(super) fn is_trace_kind(kind: ArtifactKind) -> bool {
    // AgentTurn/ToolCall/ToolResult 是执行轨迹，不作为普通研究材料直接选择。
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

pub(super) fn is_safe_deliberation_summary(kind: ArtifactKind) -> bool {
    // 只有 DeliberationNote/Claim 可作为子任务安全摘要来源。
    matches!(kind, ArtifactKind::DeliberationNote | ArtifactKind::Claim)
}

pub(super) fn overlay_state_is_eligible(kind: ArtifactKind, state: PolicyState) -> bool {
    // 将 policy state 的影响权限直接委托给领域状态谓词。
    state.permits_influence_kind(kind)
}

pub(super) fn derive_child_projection(
    proof: &SucceededAttemptProof,
    parent_manifest: ArtifactRef,
    child_contract: &AgentContract,
) -> ContextProjection {
    // 从成功 Attempt 输出中只保留子 Contract 允许且非 Raw/Manifest/轨迹的引用，形成收缩投影。
    let policy = &child_contract.context;
    let mut allowed = BTreeSet::new();
    for output in &proof.outputs {
        if output.kind != ArtifactKind::RawEvidence
            && output.kind != ArtifactKind::ContextManifest
            && !is_trace_kind(output.kind)
            && policy.permitted_kinds.contains(&output.kind)
            && (policy.permitted_source_families.is_empty()
                || policy
                    .permitted_source_families
                    .contains(&output.provenance.source_family))
        {
            allowed.insert(ArtifactRef {
                artifact_id: output.artifact_id.clone(),
                kind: output.kind,
            });
        }
        // 同时保留安全的 deliberation/Claim source refs，避免把执行轨迹或原始证据向下委派。
        allowed.extend(
            output
                .source_refs
                .iter()
                .filter(|source| {
                    is_safe_deliberation_summary(source.kind)
                        && policy.permitted_kinds.contains(&source.kind)
                })
                .cloned(),
        );
    }

    ContextProjection {
        parent_manifest,
        allowed: allowed.into_iter().collect(),
        reason: "parent_attempt_projection".to_owned(),
    }
}

pub(super) fn selection_reason(kind: ArtifactKind) -> &'static str {
    // 把 ArtifactKind 转为清单审计使用的稳定选择理由。
    match kind {
        ArtifactKind::NormalizedEvidence => "normalized_evidence",
        ArtifactKind::SemanticDetail => "semantic_detail",
        ArtifactKind::DecisionProposal => "final_proposal",
        ArtifactKind::ProposalReview => "proposal_review",
        ArtifactKind::Claim => "claim",
        ArtifactKind::Critique => "critique",
        ArtifactKind::Experience => "experience",
        ArtifactKind::Lesson => "lesson",
        ArtifactKind::Retrospective => "retrospective",
        ArtifactKind::CandidatePolicy => "candidate_policy",
        ArtifactKind::Evaluation => "evaluation",
        _ => "contract_permitted",
    }
}
