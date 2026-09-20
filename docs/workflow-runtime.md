# Workflow Runtime 与运行检查

本页描述固定业务图、共享运行控制和恢复记录。运行权限、Paper 审批和 Contract 以 [运行时契约](agent-runtime-contract.md) 为准。

## 定义与执行

`WorkflowRuntime::research_definition` 生成版本化 `WorkflowDefinition`，原有编译器为 PositionPlan、Paper、Shadow 添加各自的 Rust Gate。默认 PositionPlan 21 个节点、Paper 25 个节点；数量包含共享补充、refined Analyst/Critic 和三组 Synthesizer/ProposalReviewer，配置的修订上限会改变数量。Outcome worker 仍由到期协议另行安排，不代表 T0 已完成后续评估。

每个新节点携带 `NodeSpec`：稳定逻辑 key、horizon、research round、proposal revision。研究版本选择、输出 horizon 校验和展示读取结构化字段。`objective` 保存说明文字；模型请求中的既有 scope 标记由 NodeSpec 渲染，保持当前研究协议。历史缺少 spec 的节点通过集中只读适配器解析，不回写原 CAS。Agent 入口核对持久化节点的执行规格与当前 permit；额外 Evidence candidates 继续由 Context 授权。

`WorkflowGraph::blueprint` 从同一个编译结果产生只读节点、依赖、entry、finish、Contract、budget、retry、priority 和 parent 信息。definition hash 排除随机 Task ID、描述文字和具体 Evidence Artifact，标识控制结构。它不能单独保证模型重跑逐字一致；完整追溯还需要 graph CAS、RuntimeManifest、Contract/Prompt、模型记录和证据快照。

`NodeExecutor + NodeContext → NodeOutcome` 是统一执行接口。WorkerPool 和 Daemon 单步入口进入 TaskRuntime 的同一个 lease、heartbeat、deadline、重试及提交协议。领域 handler 继续负责 Evidence、Agent、Decision、Execution、Paper、Reconcile 和 Outcome 的业务判断。没有允许模型自由创建交易拓扑的 API。

## RunControl 与 checkpoint

Store 18 使用唯一的 `rebuild_run_controls` SQL head。普通运行默认为 continuous；原 Debug 的 pause/step/resume/retry/abort 作用于这个 head，保留 revision CAS、精确 task permit、runtime identity 和原有权限。`RunControlStatus` 与旧 `DebugStatus` 保持 wire 兼容。控制状态与业务 WorkflowStatus 分开：控制结束不代表研究成功、Paper 成交或 Outcome 封存。

`RuntimeCheckpoint` 是独立、仅 RunScoped 的 CAS 类型。图创建、任务边界、暂停、重试/延期、取消及 Outcome 入队等事件在同一 SQLite 事务内追加 checkpoint。记录包含 graph 引用、被覆盖的 event cursor、control revision、可用的 runtime identity、task/attempt、触发事件及来源 Artifact。checkpoint 自身通过 `runtime.checkpoint_saved` 进入同一个 journal，不另建状态文件或第二份权威 state。

恢复在同一个 SQLite snapshot 读取图 head 与最新 checkpoint，并核对来源链；Replay 和 Doctor 校验 checkpoint 事件。它不替代 AgentTurn/Tool、lease fencing、确定性 Commitment、Paper effect intent 和 reconcile 恢复。checkpoint 是可验证的恢复边界，不是可脱离 Store 独立恢复的序列化进程，也不提供外部副作用 exactly-once 保证。

Provider retry、预算失败、交易时段延期仍按既有语义执行，没有强改为人工 Paused。查看 inspection、checkpoint 或 journal 不会创建事件或释放任务。普通运行的 `allowed_actions` 为空，本次没有新增 canonical Store 的调试控制权限。

## 只读检查入口

所有 HTTP 入口复用 loopback 认证与现有 Origin 检查：

| 接口 | 内容 |
| --- | --- |
| `GET /v1/workflows/blueprint?purpose=position_plan` | 当前已安装定义的预览，不创建 Run、不调用模型 |
| `GET /v1/observer/runs/{run_id}/inspection` | 持久化图、Blueprint、控制 head、checkpoint、恢复状态 |
| `GET /v1/observer/runs/{run_id}/journal?after=0&limit=100` | journal 游标分页；支持 `task_id`、`attempt_id`，上限 500 |

Journal 返回 Artifact 和 source refs，不返回原始模型/工具载荷。原始数据仍通过现有授权导出读取。Observer Run Detail 与 Debug Inspect 携带同一 inspection 投影，SwiftUI Workflow 页可预览定义、检查恢复记录和加载事件来源。

```sh
akzio --config <隔离配置> workflow blueprint --purpose position_plan --format json
akzio --config <隔离配置> workflow blueprint --purpose paper --format mermaid
akzio --config <隔离配置> run inspect <run-id>
akzio --config <隔离配置> run checkpoint <run-id>
akzio --config <隔离配置> run journal <run-id> --after 0 --limit 100
```

`run events` 保留原 SSE 语义，`run journal` 是有限分页查询。Blueprint CLI 读取目标 Core 的已安装定义，需该 Core 已启动及已有 token。

## Store 17 → 18

迁移在事务中新增 task `node_spec_json`，把旧 Debug 控制行迁入共享 RunControl，再为普通历史 Run 补控制 head，最后更新 Store schema。原 graph、Contract、Commitment、Artifact hash 和 journal 不改写；历史 Run 不回填虚构 checkpoint。

升级沿用原阻断条件：queued/leased/running 任务或有效 daemon lease 都会阻断；仅停止进程不代表可迁移。应先用旧版本处理既有工作与 lease，再升级。`Store::open_existing` 保持只读，不隐式迁移；旧 Store 需要明确的写模式初始化路径，不能通过只读检查命令升级。

Paper 的 `PaperSubmissionAuthorization`、审批消费、先持久化 Commitment、client order ID 与恢复协议保持原边界。PositionPlan 在 Decision 后结束，Blueprint 或 checkpoint 不授予执行权限。
