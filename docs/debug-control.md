# 分流程 Debug 控制

Debug 复用正式 Rust 研究图，通过 V2Store 的共享 RunControl head、DebugSession 身份、revision CAS 和 claim 事务控制节点领取。CLI 与 App 使用同一认证 Observer API；readiness、allowed_actions 和 blocked_reason 由 Core 返回，客户端不计算业务依赖。

Core/CLI/App 的 Blueprint、checkpoint 和分页 journal 入口见 [Workflow Runtime](workflow-runtime.md)。

## 入口与隔离

真实 PositionPlan、隔离配置、打包和验证命令统一见 [开发 Workflow](development-workflow.md)。PositionPlan 在 Decision 后结束；缺 policy 时只有显式 research-only 才能执行研究，不能放行 Decision。Paper 调试与执行授权见 [运行时契约](agent-runtime-contract.md) 第 12 节。

Debug Core 必须使用 `.akzio/` 下的新隔离 Store，设置 `debug_control=true`、`auto_paper=false`；不得打开 `~/.akzio/store`。Store 隔离标记不能通过重启为普通 Core 移除。真实模型使用对应配置启动 `daemon serve`；`debug serve-fixture` 只服务离线 fixture，不构成真实模型验证。

自动离线验收使用 `debug verify-fixture`，在新隔离 Store 上完成正式 PositionPlan 图及 Store Doctor。真实模型与数据的原生 Paper 执行使用启动器 `--paper`，经正式 Paper 图提交到 Alpaca Paper 并等待成交；原审批与全部 Gate 仍生效，缺 Policy 时保留零执行目标与 NoOrder。旧 `-fakerOnline` / `--faker-online` 参数被拒绝，历史 `simulated_only` 身份不再允许 Broker 执行。命令、归档与配置细节统一见开发 Workflow。

Planner / PaperDryRun 的旧创建命令和 fixture 图已删除，旧运行只保留读取与审计；执行控制返回 `legacy_workflow_retired`。完整删除与兼容边界见 [研究协议退役清单](research-protocol-retirement.md)。

以下命令连接已有的隔离 Core。`<config>`、Run、Task 和 Attempt ID 必须来自本次实际配置与查询，不复用历史报告中的 ID。完整参数以 [CLI 定义](../crates/akzio-cli/src/cli/debug_commands.rs) 为准。

```text
akzio --config <config> debug prepare --session YYYY-MM-DD --purpose <paper|position-plan>
akzio --config <config> debug nodes <run_id>
akzio --config <config> debug inspect <run_id> [--task <task_id>] [--attempt <attempt_id>]
akzio --config <config> debug step <run_id> --task <task_id> --wait-seconds 60
akzio --config <config> debug pause <run_id>
akzio --config <config> debug resume <run_id>
akzio --config <config> debug retry-node <run_id> --task <task_id>
akzio --config <config> debug fork <run_id> --task <successful_task_id> --reason <reason>
akzio --config <config> debug experiment <run_id> --reason <reason>
akzio --config <config> debug acceptance <run_id> --input <stage_acceptance.json>
akzio --config <config> debug export-bundle <run_id> --out <new_output_directory>
```

`--wait-seconds` 只等待服务端状态，超时不撤回或重新发放 permit。控制请求携带 `expected_revision`；重复或过期请求通过 CAS 拒绝。inspect 展示节点、Acceptance、事件、模型调用与预算，不重跑任务。

## 调度与恢复

| 起点 / 操作 | 条件和终点 |
|---|---|
| prepare | graph + 身份 + paused head 在同一事务发布 → Paused |
| Running → pause | 立刻阻止新 claim；已有 attempt 保持原 lease 和 future → PauseRequested |
| PauseRequested | 最后一个在途 attempt 在成功、失败、Deferred、Retry 或恢复边界关闭 → Paused；没有在途则直接 Paused |
| Paused → step TaskId | revision、Run、依赖、due time、runtime identity 合法；仅保存指定 permit → Stepping |
| Stepping → claim | 唯一 task、唯一 active attempt；与 Attempt insert 原子消费，不放行 sibling |
| Stepping → attempt 关闭 | 自动 Paused；成功、失败、Deferred 均是一调度单位，不自动重试 |
| Paused → resume | 同一身份 → Running / continuous，沿正常 DAG claim |
| Running → 无 queued/running task | Completed；若后续合法 Outcome worker 入队，恢复 continuous 或 manual 对应状态 |
| Abort | Core 内部控制动作，阻止继续放行；已有业务取消/恢复规则不被替代 |

Manual 模式最后一个 T0 节点结束仍显示 Paused，业务 workflow completed 单独展示。Outcome 的未来 due time 不是依赖伪造；未到期显示 not_due_until。修改关键代码、Contract 或模型身份后，Inspect 仍可用，执行返回 runtime_identity_changed。

## Resume / Retry / Fork / New experiment

- **Resume**：同 Run、同 Task 历史和原恢复 guard；不清空预算或成功输出。crash 后过期 lease 走原 abandoned/recovery 关系，手动 step 被消费过时先停住，不自动另领 sibling。
- **Retry failed attempt**：只放行原 retry policy 已重排且 due 的失败/abandoned Task。新 epoch / Attempt 保留旧 retried/failed 历史；耗尽次数、不可重试或成功节点明确拒绝。不得借手动重试扩充业务预算。
- **Successful fork**：要求指定 Task 真正 succeeded 且有输出；新 RunId、TaskId 和 CAS 身份引用父 Run/Task/Artifact/reason。新研究图重新采集证据，不覆盖旧产物、不复制 Paper 执行权。使用正式 proposal/lowering 的非 Paper purpose 分支，因此不是把成功产物“就地重跑”。
- **New experiment**：不要求伪造成功父节点，引用父 WorkflowGraph 建立新研究身份，使用当前模型/Contract/代码，重新创建 EvidenceNeeds。CLI 和 App 有独立入口，API 复用 fork 构造器（无 task_id）。同 experiment_id 重复请求一致性校验，冲突不创建第二份。

取舍：成功 fork 与新实验只创建非 canonical 研究图；要进行正式 Paper/Outcome 调试应创建隔离的正式 Paper session。没有更改 `purpose == Paper` 来让 Debug purpose 获得 Paper/学习权限。

## Broker 与学习边界

`broker_write_policy=forbidden` 是创建时的默认值，属于不可变实验身份，在 Reconcile、Dispatch 和 Store effect intent 边界阻断写入。显式 `prepare --paper-allowed` 仅解除 Debug 限制，仍需原审批、身份、账户、资金、报价、Session、风险与幂等 Gate；禁写 session 不能由 UI 原地改成允许。

Outcome processing 独立于新 T0 的 auto_paper 开关，仍只处理合法 Paper purpose，依据四资产共同完成的真实交易 Session 推进 T1/T3/T5。隔离实验不能激活 canonical policy 或 Active Lesson。业务成功、Acceptance 测试结果和真实模型/Paper/跨交易日验证是分别记录的事实；未运行的检查不能标为通过。

`--paper` 启动器启用 Outcome worker，关闭自动创建 Paper run，并保留隔离 Store。观察窗口结束会停止本次 Core；待成交订单和未成熟 Outcome 需要使用保留配置重新启动 Core、恢复同一 Run 后继续处理。隔离 Store 的 readiness 明确报告 `isolated_debug_store` 和 `use_canonical_store`；其中的运行不能计入正式校准样本，等待或完成隔离 Outcome 都不会解除此限制。

导出遵循 Core 脱敏与读取授权；配置、Store、API key、认证 header、Broker secret 和 token 不应进入分享产物。运行是否完成与导出是否完整分别核验，具体以开发 Workflow 的 FINAL_STATUS / EXPORT_STATUS 说明为准。
