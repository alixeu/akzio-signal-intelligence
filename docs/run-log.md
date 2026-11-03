# 正式运行的私有阶段日志

正式 Core（Shell、CLI 与自动调度共用）在当前用户的 `~/.akzio/log/YYYYMMDD/` 生成私有 ZIP：

```text
run-<run_id>-T0-<segment_id>.zip
run-<run_id>-T1-<segment_id>.zip
run-<run_id>-T3-<segment_id>.zip
run-<run_id>-T5-<segment_id>.zip
core-startup-<segment_id>.zip
```

T0 在该 Run 的业务终态后单独归档，PositionPlan 不产生后续 Outcome ZIP；Paper 的 T1/T3/T5 在对应阶段确实提交后分别归档，失败、取消与正常停机可产生带状态的片段。日期在片段开始时取本机本地日期并冻结，manifest 同时保存 UTC 起止时间和当时的本地时区偏移；它不是美股交易 Session。重复恢复产生新的片段，通过 manifest 的 `previous_segment` 引用同阶段的前一份不可变 ZIP，不覆盖旧包。原 `~/.akzio/logs/` 进程启动日志保留。

每个 ZIP 保存 `manifest.json`、`events.jsonl`、`llm_calls.jsonl`、`rust_flow.jsonl`、`summary.md`、`checksums.sha256`。模型实际请求/响应、SSE 事件与错误保留在同一个 Run/Task/Attempt/调用关联中；Rust Task 操作和冻结窗口内的 Store 事件记录流程与来源引用，不逐行追踪 Rust 指令。提供方没有返回的内部 reasoning 不会补造；崩溃或取消期间没有已确认终态的调用标记为 `unknown_after_crash` / `unknown_after_cancel`。未闭合的 Rust task 和已记录响应却尚未由 Store 确认的模型调用分别列入完整性状态；普通阶段归档会在 Store 调用核对失败时保留明文并阻断，而不是发布一个虚假的完整包。流与归档按片段分块处理，不以固定长度截断大响应。

ZIP 是私有派生记录，不是 Store、订单成交、Outcome 或审批的权威，也**不是可分享 bundle**。认证 header、已知模型/Broker key、token 和本地配置不写入包；正文经过凭据字段、已登记密钥及可识别 token 文本脱敏。文件和目录分别要求 `0600`、`0700`，拒绝符号链接及越界路径。任意模型正文仍可能包含无法自动识别的敏感信息，切勿向外分享此包；需要分享时使用原有脱敏导出。

写入、同步或归档失败时，Core 停止新模型调用、新 Run、订单及改价；普通 Worker 不继续领取任务，仅已记录 Broker effect 的 Reconcile 可走只读观察和原已持久化撤单意图。Run 取消后也保留这一已记录 effect 的安全对账任务，取消不再允许新的订单和改价。正式 Alpaca adapter 在每次 POST/PATCH/DELETE 前再次执行日志守卫；保护性撤单仍须此前已持久化的意图。业务状态和日志状态分别查看 `/ready`、Run inspection 与 Observer 投影；故障不是模型错误，不能靠重试额度或 Rust-only Outcome 降级绕过。修复文件系统/Store 故障并重启后，先校验和补归档未完成片段或未确认 ZIP，才启动新的模型 I/O；无法判定完整性的残留使启动失败。

若某个 Run 的提供方调用在取消或崩溃时结果无法确认，其状态通过 Store 事件保留为 `unknown_provider_result`；已记录响应但无法证明 Store Attempt/用量已持久化的调用另标为 `unreconciled_provider_result`。重启后该 Run 不自动发起下一次模型调用或盲目重试；已成功结算的 Attempt 或匹配的 AgentTurn 可作为恢复证明。必须先检查其 Store Attempt/用量与私有 ZIP，按原恢复和人工处置边界处理；这不把未知响应伪装为成功，也不阻断其他 Run 的只读检查或已提交订单的安全对账。

正式 Run 的 T0 日志需求与新 Run 图在同一个 Store 事务标记；如果在第一条明文日志写入前崩溃，重启只为带此明确标记的 Run 重建待归档片段，不为旧版本缺失的模型正文造假。后续阶段以各自的开始水位收录 Store 事件，避免 T0 ZIP 发布较晚时遗漏已开始的 T1 事件。同一用户的不同正式 Store 通过不暴露路径的身份指纹隔离未归档片段；一个 Core 不会替另一个 Store 封包。归档顺序：冻结 Store 事件水位，逐段写私有临时 ZIP，关闭并同步，重读所有条目及哈希，创建不可覆盖的目标文件，持久化 Store 归档确认，然后只删除**该段**临时明文。失败文件、历史日志、Store 和旧 ZIP 不自动清理。Shell 与 HTTP 客户端都只触发一次 Run，不负责等待多日 Outcome 或创建 ZIP。
