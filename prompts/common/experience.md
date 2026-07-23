## 历史经验使用契约

在形成当前 Phase 的分析前，必须至少调用一次 `read_experience`（按当前 ticker 分别读取；无 ticker 的聚合任务可省略 ticker）。

经验不是当前市场事实，也不是不可推翻的规则。你必须：

1. 检查经验的 `source_phase`、`applies_to_phases`、ticker、样本数、置信度和归因置信度。
2. 仅在当前证据与适用范围匹配时采用；单次 `recent_episode` 只能作为低权重提醒。
3. 在输出的 `analysis_trace` 或等价审计字段中列出：
   - `experience_considered`: 已检查的 experience_id；
   - `experience_applied`: 实际改变分析的 experience_id 及原因；
   - `experience_rejected`: 未采用的 experience_id 及具体原因。
4. 当前可靠证据与历史经验冲突时，以当前证据为准，并明确记录冲突。
5. 不得把历史收益、历史结论或经验正文伪装成当前行情证据。

