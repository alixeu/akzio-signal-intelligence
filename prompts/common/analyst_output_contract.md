## 输出契约（必须严格遵守）

你的最终回复必须是**一个 JSON object**，不要任何 Markdown 代码块围栏（不要 ```json）、不要在 JSON 前后加解释文字。你的全部散文分析写进 `per_ticker.<ticker>.report` 字段，不要单独输出散文正文。

顶层结构：

```
{
  "id": "<role>",
  "role": "<role>",
  "status": "completed" | "unobserved",
  "per_ticker": {
    "<TICKER>": {
      "direction": "bullish" | "bearish" | "neutral" | "mixed" | "unobserved",
      "confidence": 0.0-1.0,
      "report": "本 ticker 的完整分析正文（可含分节标题、Markdown 表格），把原来所有分节写在这里",
      "key_evidence": [
        {
          "claim": "证据正文，1-2 句话",
          "evidence_type": "fact" | "opinion" | "speculation",
          "source": "证据来源（工具名/数据源/URL 描述）",
          "timestamp": "ISO 日期"
        }
      ],
      "priced_in": "already_priced" | "under_priced" | "unclear",
      "validation_triggers": ["会强化或推翻当前判断的 1-3 个可观察触发点"],
      "data_gaps": ["数据缺口与不确定性；无缺口时给空数组"]
    }
  }
}
```

硬性规则：
- `per_ticker` 的 key 必须与本次输入 ticker 完全一致，不新增、不替换、不遗漏。
- `direction` 与 `confidence` 是机器读取字段，必须存在且合法；`confidence` 是 0.0-1.0 的小数（不是 0-100）。
- 无可用样本时输出 `direction="unobserved"`、`confidence=0.0`，并把原因写进 `data_gaps`；不要臆造读数。
- `report` 承载你原本要写的一句话结论、价格结构/市场反应、已计价 vs 未充分计价、验证/证伪触发器、数据缺口等所有正文；结构化字段是这份正文的机读摘要，两者必须一致。
- 不要输出 BUY/HOLD/SELL、仓位、止损、止盈或目标价。

证据类型定义：
- `fact`：官方数据、监管文件、交易所数据、审计后财务报表。可从一手来源验证。
  - 示例：BLS CPI 发布、SEC 文件、交易所成交量、审计后财报
  - 权重：1.0x（下游角色给予完整权重）
- `opinion`：分析师解读、管理层评论、共识预期。基于数据但非数据本身。
  - 示例：分析师笔记 "Fed 可能在 9 月降息"、Fed Funds Futures 定价、财报电话会管理层指引
  - 权重：0.7x（下游角色给予 70% 权重）
- `speculation`：未经证实的传闻、社交媒体帖子、单一来源报告。无法从一手来源确认。
  - 示例："TQQQ 期权有大买家传闻"（Reddit）、"巨鲸在积累"（单一 X 帖子）、未经证实的泄露
  - 权重：0.3x（下游角色给予 30% 权重）

每条 `key_evidence` 必须包含 `evidence_type` 字段。若证据类型不明确，使用 `speculation` 并在 `source` 中说明不确定性。

每个 `per_ticker.<ticker>` 值的字段形状必须符合以下 JSON Schema（权威定义，与运行时校验同源）：

```json
{analyst_artifact_schema}
```

