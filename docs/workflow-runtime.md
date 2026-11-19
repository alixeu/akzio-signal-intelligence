# Workflow Runtime 与运行检查

本页描述固定业务图、共享运行控制和恢复记录。运行权限、Paper 审批和 Contract 以 [运行时契约](agent-runtime-contract.md) 为准。

## 定义与执行

`WorkflowRuntime::research_definition` 生成版本化 `WorkflowDefinition`，原有编译器为 PositionPlan、Paper、Shadow 添加各自的 Rust Gate。默认 PositionPlan 21 个节点、Paper 25 个节点；数量包含共享补充、refined Analyst/Critic 和三组 Synthesizer/ProposalReviewer，配置的修订上限会改变数量。Outcome worker 仍由到期协议另行安排，不代表 T0 已完成后续评估。

每个新节点携带 `NodeSpec`：稳定逻辑 key、horizon、research round、proposal revision。研究版本选择、输出 horizon 校验和展示读取结构化字段。`objective` 保存说明文字；模型请求中的既有 scope 标记由 NodeSpec 渲染，保持当前研究协议。历史缺少 spec 的节点通过集中只读适配器解析，不回写原 CAS。Agent 入口核对持久化节点的执行规格与当前 permit；额外 Evidence candidates 继续由 Context 授权。

`WorkflowGraph::blueprint` 从同一个编译结果产生只读节点、依赖、entry、finish、Contract、budget、retry、priority 和 parent 信息。definition hash 排除随机 Task ID、描述文字和具体 Evidence Artifact，标识控制结构。它不能单独保证模型重跑逐字一致；完整追溯还需要 graph CAS、RuntimeManifest、Contract/Prompt、模型记录和证据快照。

`NodeExecutor + NodeContext → NodeOutcome` 是统一执行接口。WorkerPool 和 Daemon 单步入口进入 TaskRuntime 的同一个 lease、heartbeat、deadline、重试及提交协议。领域 handler 继续负责 Evidence、Agent、Decision、Execution、Paper、Reconcile 和 Outcome 的业务判断。没有允许模型自由创建交易拓扑的 API。

## RunControl 与 checkpoint

Store 18 引入统一的 `rebuild_run_controls` SQL head。普通运行默认为 continuous；旧 Debug 的 pause/step/resume/retry/abort 历史 head 保留 revision CAS、精确 task permit 与身份以供只读审计，但不再提供创建或控制入口。`RunControlStatus` 与旧 `DebugStatus` 保持 wire 解码兼容。控制状态与业务 WorkflowStatus 分开：控制结束不代表研究成功、Paper 成交或 Outcome 封存。

`RuntimeCheckpoint` 是独立、仅 RunScoped 的 CAS 类型。图创建、任务边界、暂停、重试/延期、取消及 Outcome 入队等事件在同一 SQLite 事务内追加 checkpoint。记录包含 graph 引用、被覆盖的 event cursor、control revision、可用的 runtime identity、task/attempt、触发事件及来源 Artifact。checkpoint 自身通过 `runtime.checkpoint_saved` 进入同一个 journal，不另建状态文件或第二份权威 state。

恢复在同一个 SQLite snapshot 读取图 head 与最新 checkpoint，并核对来源链；Replay 和 Doctor 校验 checkpoint 事件。它不替代 AgentTurn/Tool、lease fencing、确定性 Commitment、Paper effect intent 和 reconcile 恢复。checkpoint 是可验证的恢复边界，不是可脱离 Store 独立恢复的序列化进程，也不提供外部副作用 exactly-once 保证。

Provider retry、预算失败、交易时段延期仍按既有语义执行，没有强改为人工 Paused。查看 inspection、checkpoint 或 journal 不会创建事件或释放任务。普通运行的 `allowed_actions` 为空，本次没有新增 canonical Store 的调试控制权限。

## 回环 HTTP 权限

Core 只监听回环地址。已注册的 HTTP 路由共用同一入口检查，主体是持有当前 Core token 的本地原生客户端。请求须提供精确 token，缺失或不匹配时返回 `401`。有 `Origin` 的请求，以及 `Sec-Fetch-Site` 存在且值有别于 `none` 的请求，均返回 `403`。原生 CLI 可以省略这两个请求头。

| 路由用途 | 请求与审计边界 |
| --- | --- |
| 只读查询 | Health、Ready、Workflow Blueprint、Run inspection、journal、events 及 Store 查询沿用各自的只读范围。 |
| 正式启动与控制 | `/runs`、取消、重试、叙事修复、审批、冻结及 Canary 控制继续由各自的运行时和 Store 检查授权。 |
| 维护与导出 | `doctor`、备份、恢复和导出继续走现有 Store 操作。分享用 Run Bundle 保留范围限制与脱敏。 |

通过入口检查的启动、控制、维护与导出会记录主体类别、操作类别、HTTP 方法、路由模板和返回状态。该 HTTP 记录使用注册模板，避免写入请求头、请求体、实际路径参数和私有资料。审批、冻结等持久证据仍由各自的业务流程写入。

## 只读检查入口


| 接口 | 内容 |
| --- | --- |
| `GET /v1/workflows/blueprint?purpose=position_plan` | 当前已安装定义的预览，不创建 Run、不调用模型 |
| `GET /v1/observer/runs/{run_id}/inspection` | 持久化图、Blueprint、控制 head、checkpoint、恢复状态 |
| `GET /v1/observer/runs/{run_id}/journal?after=0&limit=100` | journal 游标分页；支持 `task_id`、`attempt_id`，上限 500 |

Journal 返回 Artifact 和 source refs，不返回原始模型/工具载荷。正式运行的私有日志在[阶段 ZIP](run-log.md)。`akzio run detail RUN_ID` 返回该 Run 的 inspection、事件、轨迹、白名单 Artifact 投影和模型用量。需要分享时使用授权导出。

退役的 Observatory 仪表盘已移除整盘快照、组合历史和全局刷新流。订阅单个 Run 的生命周期使用 `akzio run events RUN_ID`，查询复盘使用 `akzio run retrospectives RUN_ID`。

```sh
akzio --config <正式配置> workflow blueprint --purpose position_plan --format json
akzio --config <正式配置> workflow blueprint --purpose paper --format mermaid
akzio --config <正式配置> run inspect <run-id>
akzio --config <正式配置> run checkpoint <run-id>
akzio --config <正式配置> run journal <run-id> --after 0 --limit 100
```

`run events` 保留原 SSE 语义，`run journal` 是有限分页查询。Blueprint CLI 读取目标 Core 的已安装定义，需该 Core 已启动及已有 token。

## Store 17 → 18

迁移在事务中新增 task `node_spec_json`，把旧 Debug 控制行迁入共享 RunControl，再为普通历史 Run 补控制 head，最后更新 Store schema。原 graph、Contract、Commitment、Artifact hash 和 journal 不改写；历史 Run 不回填虚构 checkpoint。

升级沿用原阻断条件：queued/leased/running 任务或有效 daemon lease 都会阻断；仅停止进程不代表可迁移。应先用旧版本处理既有工作与 lease，再升级。`Store::open_existing` 保持只读，不隐式迁移；旧 Store 需要明确的写模式初始化路径，不能通过只读检查命令升级。

Paper 的 `PaperSubmissionAuthorization`、审批消费、先持久化 Commitment、client order ID 与恢复协议保持原边界。PositionPlan 在 Decision 后结束，Blueprint 或 checkpoint 不授予执行权限。

## Store 18 → 19

迁移在同一 SQLite 事务创建空的逐单提交账本并发布 Store 19。升级前已有 effect intent 的批次保留 `LegacyUnknown` 状态，并按原账户及原始或已授权的替换 ID 查询。新版在原 Run 和 Commitment 下逐单追加核验通过的实际回执，供后续 Attempt 恢复。旧批次仍缺逐单提交证明。原单查询返回 404 时状态仍未知。即使已查到替换单终态，也须有原单独立终态或 `Replaced` 的可核验证据才能结算。旧 CAS、Commitment 和 effect intent 保留原记录。

新版只读打开、备份和恢复均支持 Store 18 和 19，备份 manifest 记录实际快照版本。进入 19 需要明确的可写打开。旧版二进制重新打开 Store 19 会拒绝，升级应先按[开发 Workflow](development-workflow.md#store-18--19-升级)停止所有旧写入者。
