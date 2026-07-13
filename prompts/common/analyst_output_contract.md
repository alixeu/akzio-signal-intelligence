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
          "timestamp": "ISO 日期",
          "source_tier": "official" | "major_media" | "professional_research" | "longform_analysis" | "social_verified" | "social_unverified" | "unknown",
          "first_source": "信息最早可溯源出处（归因）",
          "is_derivative_repost": false,
          "evidence_age": "0-2d" | "3-5d" | "6-10d" | "10d+" | "unknown",
          "source_confidence": 0.0-1.0
        }
      ],
      "priced_in": "already_priced" | "under_priced" | "unclear",
      "echo_chamber_risk": "low" | "medium" | "high",
      "crowded_consensus_risk": "low" | "medium" | "high",
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

每条 `key_evidence` 还需尽量填写来源质量字段（这些字段会自动出现在下方 `{analyst_artifact_schema}` 注入的 JSON Schema 中，无需手动对齐）：
- `source_tier`：来源质量分层。`official`（官方/监管/交易所/审计财报）、`major_media`（主流媒体）、`professional_research`（券商/研究机构报告）、`longform_analysis`（长文深度分析/Substack/博客）、`social_verified`（已验证社媒账号）、`social_unverified`（未验证社媒/匿名）、`unknown`（未知/无法判断）。无把握时给 `unknown`，不要臆造。
- `first_source`：信息最早可溯源的出处（谁先提出、哪个原始帖子/文件）。用于跨平台去重与归因。
- `is_derivative_repost`：本条是否为二手转载/重复搬运（原始信息来自别处）。是则 `true` 并在 `first_source` 填最早出处。
- `evidence_age`：证据的人读时效，取值 `"0-2d" | "3-5d" | "6-10d" | "10d+" | "unknown"`。
- `source_confidence`：0.0-1.0 对本条来源质量的置信度。

每个 `per_ticker.<ticker>` 还需填写两条共识风险字段（自动出现在注入的 JSON Schema 中）：
- `echo_chamber_risk`：本 ticker 的讨论是否陷入同温层/回声室（信息高度同质、缺乏异见）。取值 `low | medium | high`。
- `crowded_consensus_risk`：当前是否呈现极端一致共识（可能成为逆向拥挤信号）。取值 `low | medium | high`。无把握时给 `low` 或空字符串，不要臆造。

无数据时的纪律：当某 ticker 没有可用样本，仍输出 `direction="unobserved"`、`confidence=0.0` 并填写 `data_gaps`（说明缺口、为何缺口重要、什么会扭转观点）；**严禁臆造"暗流/undercurrents"** 等未经数据支撑的叙事。

每个 `per_ticker.<ticker>` 值的字段形状必须符合以下 JSON Schema（权威定义，与运行时校验同源）：

```json
{analyst_artifact_schema}
```

