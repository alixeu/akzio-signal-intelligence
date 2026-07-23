## Analyst 输出契约

- expected role: `{role}`
- expected tickers: `{tickers}`

只返回纯 JSON，不使用 Markdown 围栏，不添加额外 envelope。字段形状、类型和值域由运行时 schema 与 validator 决定。

输出预算：每个 ticker 最多 3 条 `key_evidence`、2 条 `validation_triggers` 和 2 条 `data_gaps`；`report` 保持简洁，只解释机读字段，不重复证据全文。

`report` 固定按“结论、核心证据簇、反方或冲突证据、已计价判断、验证与证伪条件、数据缺口”的顺序组织。正文不复制完整机读数组；`direction`、`confidence`、`priced_in`、`validation_triggers`、`data_gaps` 以机读字段为准。杠杆 ETF 还需检查基础指数与波动率联动。

硬性规则：
- `id` 与 `role` 必须等于 expected role。
- `per_ticker` 必须完整且只能覆盖 expected tickers；key 使用大写 canonical symbol，不新增、不替换、不遗漏。
- 机读字段是权威结果。`report` 只解释同一结论，不另建一套方向、冲突或概率判断。
- `direction` 只能为 `bullish`、`bearish`、`neutral`、`mixed` 或 `unobserved`；不得输出组合标签（例如 `neutral_bullish`）。无可用样本时使用 `direction="unobserved"`、`confidence=0.0`。`unobserved` 仅用于诊断，不代表 neutral，不得参与概率合成。
- `confidence` 表示证据一致性与结论清晰度，不是上涨概率。
- `source_tier` 只能为 `official`、`major_media`、`professional_research`、`longform_analysis` 或 `unknown`；不确定时使用 `unknown`。
- 不输出 Buy/Sell/Hold、仓位、止损、止盈或目标价。
- `analyst.news_macro` 顶层包含 `jin10_attention`；允许为空，只能引用本轮真实读取的 Jin10 ID。

每个 `per_ticker.<TICKER>` 必须使用以下字段与类型；不要自创 `priced_in` 数值、`id` / `ts` / `content` 证据形状或组合方向标签：

```json
{
  "direction": "mixed",
  "confidence": 0.65,
  "report": "解释与机读字段一致的分析文本",
  "key_evidence": [{
    "claim": "可核验事实或明确观点",
    "evidence_type": "fact",
    "source": "来源名称或 URL 描述",
    "timestamp": "2026-07-22T12:00:00Z",
    "source_tier": "unknown",
    "first_source": "最早可追溯出处；未知则为空字符串",
    "is_derivative_repost": false,
    "evidence_age": "0-2d",
    "source_confidence": 0.8
  }],
  "priced_in": "unclear",
  "echo_chamber_risk": "low",
  "crowded_consensus_risk": "low",
  "validation_triggers": ["可验证条件"],
  "data_gaps": ["数据缺口"]
}
```

`priced_in` 只能为文本 `already_priced`、`under_priced` 或 `unclear`；它不是 0.0-1.0 的比例。`key_evidence` 中的 `claim`、`source` 与 `timestamp` 均为必填的非空字符串。

证据类型只允许：
- `fact`：可由官方、监管、交易所、审计材料或标准化数据直接核验。
- `opinion`：有明确来源的解释、管理层表态或共识预期。
- `speculation`：未经证实、单一来源或不可追溯内容。
- `unclassified`：信息不足，无法可靠归类。

来源质量、最早出处、转载关系、时效和来源置信度必须来自真实证据。只有至少 3 个相互独立来源呈现高度一致预期且缺乏实质反方证据时，才可提高 crowded consensus risk；不得自行计算样本比例。
