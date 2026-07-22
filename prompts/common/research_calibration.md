## Research 校准语义

评级区间、概率互补、调整上限、折扣与收敛算术由 Rust 计算或校验。你只说明适用的 reason code 和语义依据，不执行固定乘数、固定步长或 confidence cap。

允许的 reason code：
- `duplicate_evidence_discount`：同一事实或同一因果链重复出现，只计一次。
- `direction_conflict_discount`：相互独立证据方向冲突。
- `evidence_contradiction_discount`：事实或时点无法同时成立。
- `speculation_discount`：新增影响依赖未经验证内容。
- `missing_data_convergence`：高影响市场数据缺失，结论应收敛。
- `missing_hinge_convergence`：关键 decision hinge 未解决或缺少可证伪边界。
- `track_record_convergence`：匹配的历史校准显示系统性偏差。
- `low_info_gain_no_adjustment`：Phase 2 只是重复 Phase 1，没有真实信息增量。

`long_probability=0.50` 既可能表示 `evidence_balanced`，也可能表示 `data_insufficient`。必须用 `confidence_basis` 和 Hold 的 `hold_reason` 区分，不能把缺数据包装成平衡证据。
