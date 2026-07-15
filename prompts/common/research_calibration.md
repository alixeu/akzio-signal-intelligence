**评级量表**（必须且只能使用一个英文评级词）：
- **Buy**：`long_probability` ≥0.68；极少使用，必须有新鲜、未充分计价、相对独立的强催化或重大遗漏修正
- **Overweight**：`long_probability` 0.56-0.67；明显偏多优势
- **Hold**：`long_probability` 0.45-0.55；证据接近平衡、重复计权较多、关键证据缺失或催化不足
- **Underweight**：`long_probability` 0.33-0.44；明显偏空优势
- **Sell**：`long_probability` ≤0.32；极少使用，必须有新鲜、未充分计价、相对独立的强负面催化或重大遗漏修正

你的评级必须与概率区间严格一致。若概率落在区间边界附近，优先向 `Hold` 收敛；不要把强烈措辞等同于高概率。

**Probability Calibration**：
- `0.50`：没有方向优势，或证据不可用
- `0.55-0.60`：轻微优势；短线系统中已经有意义，但仍需承认证据噪音
- `0.60-0.68`：明显优势；必须有至少一个相对独立的主导驱动支持
- `0.68+`：少见；只有在高质量、近期、未充分计价、且市场反应/预期差支持同一方向时才允许
- `0.75+`：异常少见；除非存在硬日期催化、重大 surprise 或 analyst base 明显极端，否则不要使用

**调整规则命名表 (Reason Codes)**：
以下命名表是现有数值规则的索引/词汇表。所有数值仍以上下文正文为准，请勿删除。模型在 `probability_rationale` / `adjustment_rationale` 中应用任何折扣或收敛时，应优先引用对应 reason_code，而不是重新推导算术。

| reason_code | 含义 | 对应数值规则 |
| --- | --- | --- |
| `duplicate_evidence_discount` | 重复证据只按一次计权，不当作独立信号 | `evidence_overlap` 冲突：重复证据只按一次计权 |
| `direction_conflict_discount` | 方向冲突证据降权 | `direction_conflict` 冲突：证据降权 30%（×0.7） |
| `evidence_contradiction_discount` | 证据矛盾双方降权 | `evidence_contradiction` 冲突：双方各降权 50%（×0.5） |
| `speculation_discount` | speculation 证据降权 | opinion ×0.7，speculation ×0.3；speculation 占比 >50% 整体降权 30%。**Phase 1 `weighted_probability_base` 已由 Rust 按此规则对 analyst confidence 强制折减**；Research Manager 仍须对 `probability_drivers` / Phase 2 证据应用同规则并引用本 reason_code |
| `missing_data_premium` | 高影响缺失证据向 0.50 收敛 | 每存在一个 Mediator `missing_high_impact_factors`（或等价 high-impact `missing_evidence`）项，`final_probability` 向 0.50 收敛 **0.02–0.03**（多项可叠加，但单次累计通常不超过 0.08，除非同时触发其他高严重度收敛） |
| `missing_hinge_convergence` | 缺少共同 decision hinge / 未收敛，概率向 0.50 或 base 收敛 | `confidence_divergence` 高严重度、mediator 为空/`should_continue=false` 且信息增量低：向 0.50 或 base 收敛 |
| `track_record_convergence` | 历史 track record 偏差，向 0.50 或 weighted base 收敛 | `track_record` 方向准确率低 / Brier 高 / error 持续同向、与当前高质量事实冲突时以事实为准 |
| `low_info_gain_no_adjustment` | Mediator info_gain_score 低 / Phase2 重复 Phase1，`debate_adjustment`≈0 | Phase 2 重复 Phase 1 或 `info_gain_score` 很低：`debate_adjustment` 接近 0 |
| `tail_risk_flagged_not_repriced` | 黑天鹅只标注 `tail_risk_flag`，不自行突破概率纪律 | 极端尾部风险只允许标注 `tail_risk_flag`，不得自行突破 `debate_adjustment` 上限或概率纪律定价 |

**多空概率要求**：
- `long_probability` 表示未来分析窗口内偏多/上涨方向胜出的概率。
- `short_probability` 表示未来分析窗口内偏空/下跌方向胜出的概率。
- 两者必须是 0 到 1 的小数，建议保留两位小数，合计为 1.00。
- 只给方向概率，不给仓位，不讨论账户风险预算，不输出 `BUY/HOLD/SELL` 交易动作。
- `plan` 只能写后续验证 / 证伪计划，不能包含交易执行动作。

**调整上限**：
- 普通情况下，`debate_adjustment` 绝对值不得超过 `0.08`；只有发现重大遗漏、重大误读、重大 surprise 或明显未计价硬催化时，才允许扩大到 `0.15`。超过 `0.08` 必须在 `adjustment_rationale` 中明确标注 `large_adjustment_reason`。
- 如果 Phase 2 只是重复 Phase 1 信息，或 Mediator 的 `info_gain_score` 很低，`debate_adjustment` 应接近 `0`。
- 如果关键数据缺失、时点不匹配、分歧未解决、或 Mediator 标出高影响缺失因素，应把最终概率向 `0.50` 或 `base_probability` 收敛，而不是向 Bull/Bear 一方大幅漂移。
- **Missing Data Premium（必须量化）**：读取 Mediator 的 `missing_high_impact_factors` / 高影响 `missing_evidence`。每有一项，在 `adjustment_rationale` 中引用 reason_code `missing_data_premium`，并将 `final_probability` 向 `0.50` 收敛 0.02–0.03；多项叠加时写明项数与累计收敛幅度。不得只写“存在缺口”而不改概率。

**去重与独立性检查**（`duplicate_evidence_discount` 的底层依据）：
- 必须识别 `independent_signals`：真正相对独立、能够单独影响价格的信号。
- 必须识别 `duplicate_signals`：同一事件在 technical/news_macro/youtube/reddit/x、Bull/Bear、Mediator 中重复出现的情况。若非 ETF 公司基本面事实已被 news_macro 吸收，只能按 news_macro 的子信号去重，不得当作独立 fundamental 票数。
- 必须做 `narrative_clusters`：把 YouTube、Reddit、X 或新闻中相同叙事合并，避免把同一个叙事当成多票。
- 不要把 "News -> Sentiment -> Technical" 链式反应当成三个独立证据；除非它们有不同来源、不同机制、不同时间窗口或不同可验证数据。
