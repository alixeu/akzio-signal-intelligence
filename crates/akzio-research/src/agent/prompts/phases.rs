//! Phase-specific wording and typed rendering; policy decisions stay in AgentRuntime.
// 文件职责：把 Draft/Submit 阶段的提示片段按角色、语言、ledger、Artifact kind 和预算
// 渲染成最终请求。format! 只替换明确的占位符；Prompt 正文是运行时输入，除注释外不得改写。
// Rust 机制：返回 Result 的函数用 ? 传播 JSON budget 序列化错误；match 按 ArtifactKind
// 选择角色约束；测试使用内存字符串检查协议边界，不代表真实 provider/Paper 验证。
use super::*;

pub(in crate::agent) fn outcome_request_prompt(
    governance: &str,
    role: &str,
    response_language: &str,
    reference_ledger: &str,
    budget: &TaskBudget,
) -> ResearchResult<String> {
    // Outcome 保留 Draft→受控读取→Submit 的两阶段提示；reference_ledger 只携带已授权引用。
    let prompt = format!(
        "{governance}\n\n{role}\n\n在 Draft 阶段，按需使用已授权的读取工具，然后以 {response_language} 返回简洁且可审计的研究备忘录。陈述结论、证据、反证和不确定性，但不要暴露隐藏的 chain-of-thought。在 Submit 阶段，恰好调用一次 submit_result；JSON property names、enum literals、identifiers、symbols 和引用的原文必须保持不变。\n\n顶层 ContextManifest 引用（将精确的 artifact_id 和 kind 复制到结果引用中；即使决策被阻断，也要保留已选 claims 及其 grounds）：\n{reference_ledger}\n\nWire Submit 引用只包含 artifact_id，不包含 kind。Rust 会从这个不可变 ledger 解析 kind。只能选择特定字段 schema 允许的 ID；Claim 不是 evidence ground。\n",
        governance = governance,
        role = role,
        response_language = response_language,
        reference_ledger = reference_ledger
    );
    // 第二次 format! 追加本 Attempt 的累计预算说明；serde_json 错误在这里直接返回调用层。
    let prompt = format!(
        "{prompt}\n\n已解析的 Attempt 资源预算：{}。max_input_tokens 是本 Attempt 中所有 LLM 请求的累计输入预算，不是 provider context window。Draft 和 Submit 共享该预算。\n",
        serde_json::to_string(budget)?,
        prompt = prompt
    );
    Ok(prompt)
}

pub(in crate::agent) fn continuation_instruction(
    phase: AgentTurnPhase,
    _contract_version: u32,
    repairing: bool,
) -> Option<String> {
    // 只有 Submit 阶段生成 continuation；repairing 分支决定是重用结果修复还是首次提交。
    (phase == AgentTurnPhase::Submit).then(|| {
        let text = if repairing {
            "上一次 submit_result 未通过 Rust 校验。这是一次有界的修复轮次：恰好调用一次 submit_result，复用上一次提交中的每个有效字段和精确来源引用，只修改结构化拒绝所要求的内容。不要重述 Critique 备忘录、grounds、证据文本或完整上下文；不要调用读取工具，也不要添加新证据。不要输出助手文本或使用其他工具。\n"
        } else {
            "Draft 备忘录已完成。恰好调用一次 submit_result。提交前：使用 schema 中的精确 ID 及其原始 kind、精确的任务 horizon、有范围的证据缺口，以及匹配的 source/resource 对。对于当前 Contracts，retriable 的阻断性缺口必须设置 retriable=true，并提供非空且受治理的 supplemental_needs。空请求必须有永久性或不可查询的理由。不要编造替代请求。保留不确定性和缺失的支持依据。不要使用其他工具或输出助手文本。\n"
        };
        text.to_owned()
    })
}

pub(in crate::agent) const COMPACT_JSON_GUIDANCE: &str = " 将结果编码为不带缩进的紧凑 JSON。保持 prose 字段简短且与决策相关：每个解释通常使用一句短句，不要在必需引用字段之外重复 Draft、来源内容或 evidence-ID ledger。保留每个必需字段、所有实质性反证和有范围的缺口、精确引用、数值结论及不确定性。简洁绝不允许删除必需事实或用某个值替代未知项。Deliberation 只需陈述一次关键决策和限制，不要重新讲述整个研究过程。\n";
// 下列常量是结构化提交的共享约束；它们由调用方拼入 Prompt，不是 Rust Gate 的替代品。
pub(in crate::agent) const ANALYST_GROUNDS_GUIDANCE: &str = "\n\n每个 grounds.evidence Artifact 只能出现一次。当一个宏观来源或其他共享来源支持多个资产时，使用一个 ground，并在其 assets 数组中列出这些资产；不要在每个资产的不同 grounds 中重复同一个 evidence ID。保留其真实 domain、role 和支持关系。不同的来源 Artifact 仍然必须区分。\n";
pub(in crate::agent) const CRITIC_VERDICT_GUIDANCE: &str = "\n\nSUPPORTED 至少需要一个当前且权威的 supporting_ref，且不得有 conflicting_refs。如果存在真实反证，保留反证，并根据情况选择 CONTRADICTED 或 NOT_ENOUGH_INFORMATION；绝不能为了得到 SUPPORTED 而删除反证。不与 Claim 矛盾的风险说明应放在 rationale 或有范围的 evidence_gap 中，而不是 conflicting_refs。保留 grounds 中的每一个核验引用。\n";

pub(in crate::agent) fn reads_exhausted_prompt(prompt: &str) -> String {
    // 读取额度耗尽时只扩展现有 prompt，明确要求保留缺口而不是虚构证据。
    format!("{prompt}\n读取工具预算已耗尽。使用现有事实完成简洁的 Draft 备忘录；明确保留证据不足。随后 Rust 会请求 Submit。\n", prompt = prompt)
}

const CURRENT_STRUCTURED_PROTOCOL: &str = "单次结构化研究协议：根据已授权 projections 完成研究，恰好调用一次 submit_result，完整提交 result 与简洁 deliberation。本次没有读取或搜索工具，不输出独立备忘录。研究期限 t1、t3、t5 分别表示基准 Session 之后第 1、3、5 个四资产共同完成的交易 Session，不是自然日、周或月；本轮同时研究这些期限，不等待到期。标题、解释和数值估计必须使用同一期限；Critic 必须指出 Claim 正文与正式 horizon 的冲突，不能把月度判断标为已核验的 t3。缺少的细节如实保留为缺口。Rust 拒绝提交时，只按字段级反馈修复，保留所有有效结论和精确引用。";

/// Role-scoped submission rules. Each role receives only the constraints that
/// bind its own output, so Analyst and Critic prompts no longer carry the
/// Synthesizer's forecast and allocation arithmetic.
fn structured_role_rules(output_kind: ArtifactKind) -> &'static str {
    // 角色约束按输出种类静态选择，防止 Analyst/Critic 携带 Synthesizer 的分配职责。
    match output_kind {
        ArtifactKind::Claim => {
            "Claim.result.grounds 是唯一正式依据；deliberation 不能补齐 ground。bars/news 的 directional ground.assets 只能是该 resource 命名的单一资产；即使两只 ETF 跟踪同一指数，也不能共享单资产新闻 ground。共享宏观可列多个资产。\n"
        }
        ArtifactKind::Critique => {
            "只审查目标 Claim.grounds 的资产和 Claim.horizon，不要求其他资产覆盖。精确核验该 Claim 的 price 和 macro grounds；额外反证可引用，但不得替 Analyst 补齐缺失 grounds。\n"
        }
        ArtifactKind::DecisionProposal => {
            "严格遵守 Rust coverage_verification_matrix 的 directional_eligible 和 eligible_claims，禁止跨 Claim/资产/horizon 拼接。提交 12 个唯一 forecasts，四行唯一 allocation 加 cash 合计 1000000。逐行检查：target_weight_ppm=0 必须有非空 abstention_reason；非零行 abstention_reason=null，且提供正向 supporting_horizons 和精确 evidence_refs。引用闭包保留所有已选 Claim/Critique 的正式 grounds。thesis_valid_until 和 expected_holding_period_days 由 Rust 按已获取交易日历填写，wire 中不要提交这些字段。缺乏资格的 slot 保持中性，现金允许。\n"
        }
        _ => "",
    }
}

pub(in crate::agent) fn structured_request_prompt(
    governance: &str,
    role: &str,
    language: &str,
    ledger: &str,
    output_kind: ArtifactKind,
    budget: &TaskBudget,
    _contract_version: u32,
) -> ResearchResult<String> {
    // 结构化研究一次性渲染 governance、role、协议、角色规则、ledger 和预算；不创建读取工具。
    let protocol = CURRENT_STRUCTURED_PROTOCOL;
    Ok(format!(
        "{governance}\n\n{role}\n\n{protocol}用 {language} 撰写说明。\n{rules}精确引用 ledger：{ledger}\nAttempt 预算：{budget}\n",
        rules = structured_role_rules(output_kind),
        budget = serde_json::to_string(budget)?
    ))
}

#[cfg(test)]
mod structured_prompt_tests {
    use super::*;

    fn rendered(output_kind: ArtifactKind) -> String {
        // 测试 helper 使用固定预算和空 ledger，只验证渲染文本的角色边界。
        structured_request_prompt(
            "governance",
            "角色文本：根据授权证据提交研究结果。",
            "简体中文",
            "[]",
            output_kind,
            &TaskBudget {
                max_input_tokens: 48_000,
                max_output_tokens: 8_000,
                max_wall_time_secs: 120,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(4),
            },
            65,
        )
        .unwrap()
    }

    /// Analyst and Critic must not receive the Synthesizer's forecast and
    /// allocation arithmetic; carrying it invites cross-role drift in a single
    /// structured submission.
    #[test]
    fn structured_rules_stay_scoped_to_the_submitting_role() {
        // 每个角色都只能看到自身约束；这里的字符串断言不调用模型。
        let analyst = rendered(ArtifactKind::Claim);
        assert!(analyst.contains("Claim.result.grounds 是唯一正式依据"));
        for foreign in ["12 个唯一 forecasts", "合计 1000000", "thesis_valid_until"] {
            assert!(
                !analyst.contains(foreign),
                "Analyst prompt leaked: {foreign}"
            );
        }

        let critic = rendered(ArtifactKind::Critique);
        assert!(critic.contains("只审查目标 Claim.grounds"));
        assert!(!critic.contains("12 个唯一 forecasts"));

        let synthesizer = rendered(ArtifactKind::DecisionProposal);
        assert!(synthesizer.contains("12 个唯一 forecasts"));
        assert!(synthesizer.contains("合计 1000000"));
        // The Synthesizer owns allocation, not the per-Claim grounds rule.
        assert!(!synthesizer.contains("Claim.result.grounds 是唯一正式依据"));
    }

    /// State the active protocol exactly once, without conflicting phase instructions.
    #[test]
    fn active_protocol_is_stated_once() {
        // 协议只出现一次，避免同一请求同时携带互相冲突的阶段说明。
        let prompt = rendered(ArtifactKind::Claim);
        assert_eq!(prompt.matches("单次结构化研究协议").count(), 1);
        assert!(prompt.contains("本次没有读取或搜索工具"));
        assert!(prompt.contains("恰好调用一次 submit_result"));
        assert!(prompt.contains("第 1、3、5 个四资产共同完成的交易 Session"));
        assert!(prompt.contains("Critic 必须指出 Claim 正文与正式 horizon 的冲突"));
    }

    #[test]
    fn current_role_documents_render_only_the_active_protocol() {
        // 三个研究角色共享一次结构化协议，但按 ArtifactKind 获得不同的局部规则。
        for (purpose, kind) in [
            (RESEARCH_ANALYST_RECIPE_ID, ArtifactKind::Claim),
            (RESEARCH_CRITIC_RECIPE_ID, ArtifactKind::Critique),
            (
                RESEARCH_SYNTHESIZER_RECIPE_ID,
                ArtifactKind::DecisionProposal,
            ),
        ] {
            let prompt = structured_request_prompt(
                SHARED_GOVERNANCE_PROMPT,
                &role_prompt(purpose).unwrap(),
                "简体中文",
                "[]",
                kind,
                &akzio_domain::budget::default_agent_budget(purpose).unwrap(),
                65,
            )
            .unwrap();
            for obsolete in [
                "Draft",
                "分两个阶段",
                "在 Submit 阶段",
                "forecast 都必须包含 thesis_valid_until",
            ] {
                assert!(!prompt.contains(obsolete), "{purpose}: {obsolete}");
            }
            assert!(prompt.contains("本次没有读取或搜索工具"));
            assert_eq!(prompt.matches("单次结构化研究协议").count(), 1);
        }
    }
}
