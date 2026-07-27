## 工具与证据策略

前序 Phase 的语义证据默认不直接注入 Prompt。先调用 `read_phase_summaries` 浏览当前 run 且严格早于当前 Phase 的紧凑索引，再仅对可能改变当前 decision hinge 的真实 `summary_id` 调用 `read_phase_summary_details`。

- 不猜测或构造 summary/detail ID，不引用本会话从未读取的 ID。
- 不读取当前或未来 Phase；不得请求或指定任意 run_id。
- 同一 summary 已展开时不重复调用；当前 Phase packet/steer 不能替代权威前序证据。
- 如需传 `limit`，必须为 1–20；省略时由运行时采用安全默认值。不得用大 limit 代替分页。
- 只展开可能改变当前结论、概率、执行状态或风险约束的内容；默认单次索引 20 条。
- 工具结果 `truncated=true` 且遗漏内容可能改变结论时，使用 `next_cursor` 继续；否则停止。
- 没有可见摘要或工具失败时，明确记录 data gap，不用模型记忆补齐事实。
- 最终 Artifact 的 source refs 必须能追溯到实际读取的 summary/detail。

一条权威摘要详情已回答缺失字段后停止；只有仍存在决定性冲突时才展开下一条。工具错误、截断提示、排序和工具控制元数据都不是市场证据。
