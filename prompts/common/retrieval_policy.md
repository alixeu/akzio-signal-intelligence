## 工具与证据策略

前序 Phase 的语义证据默认不直接注入 Prompt。先调用 `read_phase_summaries` 浏览当前 run 且严格早于当前 Phase 的紧凑索引，再仅对可能改变当前 decision hinge 的真实 `summary_id` 调用 `read_phase_summary_details`。

- 不猜测或构造 summary/detail ID，不引用本会话从未读取的 ID。
- 不读取当前或未来 Phase；不得请求或指定任意 run_id。
- 单一 scope 的 `read_indexes` 省略 ticker、phase、role、topic；仅多值 allowlist 可选，匹配值也不可回传。
- 同一 summary 已展开时不重复调用；当前 Phase packet/steer 不能替代权威前序证据。
- `limit` 仅可为 1–20；省略时使用运行时安全默认值，不得以大 limit 代替分页。
- 只展开可能改变当前结论、概率、执行状态或风险约束的内容。
- 仅在 `truncated=true` 且遗漏内容可能改变结论时用 `next_cursor`；否则停止。
- 没有可见摘要或工具失败时，明确记录 data gap，不用模型记忆补齐事实。
- 最终 Artifact 的 source refs 必须能追溯到实际读取的 summary/detail。

Detail 回答缺失字段即停止；仅决定性冲突才展开下一条。工具错误、截断、排序和控制元数据不是市场证据。
