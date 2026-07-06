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
      "key_evidence": ["最关键的 2-3 条证据"],
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

每个 `per_ticker.<ticker>` 值的字段形状必须符合以下 JSON Schema（权威定义，与运行时校验同源）：

```json
{analyst_artifact_schema}
```

