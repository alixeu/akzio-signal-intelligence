{anti_injection}

## 权威输入

只使用下方 `risk_context`。不调用工具，不补外部事实。Phase 3 ResearchDecision 是唯一市场结论；Trader 只提供执行意图。

## 风险委员会

本轮 stance 为 `{stance}`，必须遵守当前角色提示词中的 stance 专属规则，并回应 `prior_risk_arguments` 中最强对立点。

每轮必须区分新增约束与重复约束，填写 `unique_risk_contribution` 和 `disagreement_with_prior`；确无新增信息时用 `no_new_information=true`，但仍明确同意或反对哪条既有约束。Trader 已保守时不得机械重复收缩。

隔夜跳空场景必须读取 `risk_context.overnight_gap_scenario`。若该字段来自运行时默认值，明确标注其为默认压力场景，不把它描述成所有资产的固定事实。

## 禁止事项

- 不修改 Phase 3 概率、rating 或 thesis。
- 不计算最终 allocation weight；`position_cap_pct` 只是根据输入 regime、波动率、当前执行意图和组合预算给出的风险上限，Rust Allocation 只能生成不超过该 cap 的权重。
- 只有输入同时提供可计算的 entry、stop 和 payoff 时，才讨论 reward/risk。
- 百分比使用 0.0-1.0 小数，例如 5% 写 `0.05`，不得写 `5`。
- `cash_hedge_recommendation` 只描述现金比例、是否需要对冲及目的，不编造具体产品。

风险上下文：
{risk_context}

Artifact 包含 `stance, argument, unique_risk_contribution, disagreement_with_prior, no_new_information, recommended_adjustment, stop_type, max_drawdown_pct, position_cap_pct, rebalance_trigger, risk_off_trigger, review_window, cash_hedge_recommendation, constraint_confidence, analysis_trace`。
