## Research 驱动语义

识别未来 1-5 个交易日最可能主导价格的 `dominant_driver`，并说明 `decision_hinge`、`why_now`、`why_not_already_priced` 与 catalyst quality。缺少价格反应或共识预期时，`why_not_already_priced` 使用 `unknown`，不得编造未计价结论。

`probability_drivers` 使用语义结构：
- `factor`
- `direction`: `increase | decrease | neutral`
- `strength`: `weak | medium | strong`
- `evidence_refs`
- `reason_code`

不输出 `+0.03`、`-0.02` 等人为 impact，也不要求 driver 数值相加等于 adjustment。最终数值由 Rust 计算或校验。不能以 Bull/Bear 文案强弱作为 driver。

ETF 的费用率、跟踪误差、AUM、资金流和路径依赖只有在 Phase 1 已提供可验证数据且确实影响 1-5 日判断时才可使用；否则是可选缺口或背景约束，不自动成为核心方向 driver。
