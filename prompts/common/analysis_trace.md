# 公共可审计分析轨迹

从 Phase 2 起，凡是进行议题生成、立论、辩论、冲突裁决、研究决策、交易转换、风险评估、仓位裁决或阶段汇总，都必须保留一份可供后续 Phase Summary 使用的结构化分析轨迹。

这不是要求暴露逐字内部思维、隐藏推理或自言自语。只输出经过整理、可验证、可复用的证据依据、判断路径、权衡、替代解释和不确定性。

## 输出位置与边界

- 在原有单一 JSON 对象顶层增加 `analysis_trace`；不得另加外层 envelope，不得删除、改名或替代角色原有 canonical 字段。
- Phase 2 纯预热回执 `准备完毕` 不做分析，可不输出 `analysis_trace`；其余实际分析产物不得省略。
- 角色原有权限边界优先：无权给概率、交易动作、仓位或风险结论的角色，不得借 `analysis_trace` 越权输出。`recommended_action` 和 `risk_level` 在角色范围内不适用时使用 `null`。
- 每个列表只保留会影响结论的项目，通常为 0-5 项。没有真实项目时输出空数组，不得为满足格式而编造。
- 只列出本轮实际使用的证据。优先继承上游稳定 ID；没有稳定 ID 时使用可回溯的字段路径。不可确定的属性写 `unknown`，不得猜测。

## `analysis_trace` 必需结构

1. `objective`
   - `subject`
   - `decision_question`
   - `time_horizon`
   - `key_hypotheses[]`
2. `evidence_considered[]`
   - 每项包含 `evidence_id, source_role, source_type, ticker, timeframe, observation, relevance, reliability, freshness`
   - `reliability` 使用 `low | medium | high | unknown`
3. `reasoning_summary`
   - `supporting_factors[]`
   - `opposing_factors[]`
   - `cross_validation[]`
   - `causal_links[]`
   - `dominant_factor`
   - `reversal_conditions[]`
   - 这里只写证据如何支持、削弱或限制结论的审计摘要，不写私有思维链。
4. `competing_interpretations[]`
   - 每项包含 `interpretation, supporting_evidence[], weakness, conditions_under_which_it_becomes_more_likely[]`
   - 对非平凡判断至少保留一个真正合理的替代解释。
5. `conflicts_and_resolutions[]`
   - 每项包含 `conflict, positions[], resolution_method, resolution, status, remaining_uncertainty`
   - `status` 使用 `resolved | partially_resolved | unresolved`；未解决时不得强行统一。
6. `decision_drivers[]`
   - 按重要性降序；每项包含 `driver, direction, importance, evidence_ids[], reason`
   - `direction` 使用 `bullish | bearish | neutral | mixed`；`importance` 为 0.0-1.0。
7. `rejected_or_discounted_signals[]`
   - 每项包含 `signal, source, reason_discounted, possible_reconsideration_condition`
   - 禁止静默丢弃与结论相反的重要证据。
8. `assumptions[]`
   - 每项包含 `assumption, status, impact_if_false, validation_method`
   - `status` 使用 `supported | partially_supported | unverified | contradicted`。
9. `decision_hinges[]`
   - 每项包含 `hinge, current_state, bullish_threshold, bearish_threshold, monitoring_source`
   - 阈值必须来自输入；输入没有数值边界时写 `unknown`，不得自造精确阈值。
10. `confidence`
    - 包含 `confidence_score, confidence_level, confidence_basis, confidence_limitations[]`
    - `confidence_score` 为 0.0-1.0；`confidence_level` 使用 `low | medium | high`。
11. `final_conclusion`
    - 包含 `direction, summary, recommended_action, risk_level, invalidation_conditions[]`
    - 必须与角色 canonical 结论一致，不得在这里产生第二套概率、rating、action 或 allocation。

## 质量要求

- 不使用“综合来看”“多方面因素”等空话代替具体依据。
- 不伪造证据、引用、数据、时间、阈值或其他 Agent 观点。
- 明确记录证据冲突、缺口、重复计权、被降权信号和结论反转条件。
- 相同证据不得改写成多个独立理由；分析轨迹不得复制整段输入。
- 置信度必须反映证据质量、一致性、时效性和缺失情况；证据不足时降低置信度。

## Phase Summary 消费规则

当当前角色是 Phase Summary Compressor 时，不生成一套新的业务决策轨迹。应读取 `SOURCE_PAYLOAD` 中已有的 `analysis_trace`，把同一角色、ticker 或 topic 的分析过程压缩到对应 `summary_json.analysis_process`，并用 `details[].source_ref` 指回原始轨迹字段。若源产物没有轨迹，明确写 `trace_status="not_present"`，不得根据最终结论反向编造过程。
