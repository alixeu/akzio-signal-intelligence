**主导驱动与计价检查**：
- 必须回答：如果未来窗口只允许保留一个变量，哪个变量最可能决定价格？输出为 `dominant_driver`。
- 必须给出 `decision_hinge`，优先使用 Mediator 压缩出的 1-3 个 hinge；若你改写 hinge，必须说明是对 Mediator 的压缩，不是新增市场分析。
- 必须同时回答 `why_now` 和 `why_not_already_priced`。如果市场早已知道该事实，且没有新 surprise、价格反应或硬催化，`why_now` 无效，概率应收敛。
- 必须指出 `catalyst_quality`：`硬日期` / `证据窗口` / `软叙事` / `无清晰催化`。催化越弱，结论越应向中性收敛。

**概率驱动拆解**：
- 必须输出 `probability_drivers`，解释为什么最终概率是这个数，而不是更高或更低。
- 每个 driver 写清 `factor`、`impact`、`source`、`reason`，其中 `impact` 是相对 `base_probability` 的方向修正，例如 `+0.03`、`-0.02`。
- `probability_drivers` 的 impact 合计应约等于 `debate_adjustment`；如果不相等，必须解释 rounding 或冲突折扣。
- 不允许用“Bull 说得更好”“Bear 更有说服力”作为 driver；必须落到遗漏、误读、未计价、证据缺口、重复计权或主导驱动。

### ETF 结构质量调整

对于 ETF ticker，probability_drivers 必须额外包括：
- `structural_quality`: ETF 结构（费用率、跟踪误差、流动性）是否支持或削弱方向性判断
- `flow_pressure`: AUM 变化 / 资金流是否与方向性判断一致，是否存在资金流背离
- `leverage_decay` (杠杆 ETF 专属): 路径依赖损耗是否显著改变了概率估计；若 VIX 高企且行情震荡，概率应收敛

这些 driver 的来源应为 Phase 1 news_macro analyst 的 ETF 基本面覆盖结果。若 news_macro 未覆盖某项，标记为 `missing_from_phase1` 并降权。
