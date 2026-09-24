# 主线：系统入口之后，Workflow、调度、状态与存储如何共同推进

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

## 阅读边界

- 这是 2026-09-29 对当前工作树的源码核查，不是某个实际 Run 的运行验收。
- 核查起点 HEAD 为 `e4292f09acf5b3798bf16de26718ceb85046a190`，分支为 `main`；工作树原本存在未提交修改，因此以下描述不等同于该 commit 的原始内容，也不等同于已启动 Core 的二进制。
- 未修改源码、未启动 Core、未运行模型、未访问 Paper 账户、未读取 canonical 数据库，未执行 Rust 测试矩阵。
- 其他章节分别细化证据、研究、Decision、执行、Outcome 和界面。本章负责这些阶段之间的控制流和持久化语义。

## 1. 系统不是一个让模型自由执行的聊天循环

业务判断分成三类不同权限：

1. Rust 编译图、保存状态、授予资料、限制预算、检验结果、决定目标、批准执行、保存副作用意图。
2. 模型在一个已确定的角色、期限、Contract 和 Context 里提交研究产物或复盘叙事。
3. Broker 只接受 Rust 在合法审批、Gate、Commitment 和时段授权之后发出的 Paper 请求。

源码的统一分发口把节点穷举为 `Agent / ResearchControl / Evidence / DecisionGate / ExecutionGate / PaperCommit / Reconcile / Evaluate`。Outcome worker 另走专门入口；没有“未识别就交给 Planner”或让模型随意创建执行链的 fallback。

证据：[dispatch.rs](../../crates/akzio-daemon/src/dispatch.rs)、[application/mod.rs](../../crates/akzio-daemon/src/application/mod.rs)。

## 2. 三种活动 purpose 与三个独立维度

### 2.1 活动图

- `PositionPlan`：研究图 + EvidenceGate + DecisionGate；到 Decision 为止。
- `Paper`：同一研究图 + EvidenceGate + DecisionGate + ExecutionGate + PaperCommit + Reconcile + Evaluate。
- `Shadow`：受控对照图；研究后仍有 ExecutionGate、Reconcile、Evaluate，但没有 PaperCommit。候选 Contract 不因为被安装或放进 Shadow 就成为 canonical active Contract。
- `Debug / Replay / PaperDryRun` 等旧枚举仍可出现在历史数据里；不能仅从 enum 存在推断今天还有相同的创建或执行入口。

证据：[lowering.rs](../../crates/akzio-runtime/src/runtime/compilation/lowering.rs)、[evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)。

### 2.2 必须分别看待的维度

1. `purpose`：业务上运行什么图。
2. RunControl / Debug execution mode：现在连续推进、暂停还是只允许精确的一个 Task。
3. Broker write policy、Paper 审批和 Store learning eligibility：是否允许外部写入、是否有执行权限、能否参与正式校准/学习。

例如，隔离 Debug 可以承载 Paper purpose 的图，但这不自动赋予 Broker 写权限，也不赋予 canonical 校准资格。普通 PositionPlan 也不是“Paper 暂时还没走到下单”：它根本没有那些后续执行节点。

证据：[workflow-runtime.md](../workflow-runtime.md)、[agent-runtime-contract.md](../agent-runtime-contract.md)。

## 3. Core 启动时做什么

生产 CLI 的 `serve` 顺序是：

1. 读取配置和 SQL active DecisionPolicy，计算 policy 状态和输入身份。
2. 检查隔离 Debug 的 policy 前置；缺 policy 的受限研究模式与 decision-capable 模式分开。
3. 取得本地认证 token。
4. 对配置的模型 routes 做 capability probe；这不是正式研究，但启动生产 Core 可能在这里发生模型 provider 请求。
5. 在需要的模式里生成 RuntimeIdentity hash，绑定当前配置、模型能力和 policy 等身份。
6. 构建 Daemon：校验预算；打开 Store；检查/登记 Debug 环境；安装当前研究 Contract catalogue；构造 WorkflowRuntime、AgentRuntime、Decision/Execution runtimes、scheduler 和 StoreExecutor。
7. 配置生产 Evidence adapters。没有可用来源不会偷偷退回虚构数据；`auto_paper` 明确要求 Alpaca adapter 和 FRED。
8. 如果 Outcome processing 开启且 Alpaca adapter 可用，恢复已有待评估工作；即使暂时关闭处理，未来 Outcome 工作仍可被持久化，之后启用无需新 T0 再发现旧任务。
9. 需要 Paper 观察/执行能力的模式注入已经通过 endpoint 校验的 Paper client。
10. 并行服务 HTTP 和 WorkerPool；只有显式 `auto_paper=true` 才启动自动 scheduler。手动 Paper launch 使用 scheduler 的受控 tick，不等于自动调度已开启。

容易混淆的默认值：

- CLI 生产入口在未设置 `worker_count` 时用 **4**。
- `WorkerPoolConfig::default()` 自身用 **2**；fixture 构造也有独立默认值。
- 自动 scheduler 生产轮询间隔是 **30 秒**。
- TaskRuntime / PaperScheduler 默认 lease 时长是 **30 秒**；Task heartbeat / recovery 间隔由 lease 的三分之一计算，即默认约 **10 秒**。

这些值来自不同层，不能混写成一个“系统默认”。

证据：[run_commands.rs](../../crates/akzio-cli/src/cli/run_commands.rs)、[bootstrap.rs](../../crates/akzio-daemon/src/orchestration/bootstrap.rs)、[worker.rs](../../crates/akzio-daemon/src/worker.rs)、[task.rs](../../crates/akzio-runtime/src/runtime/task.rs)。

## 4. Scheduler 如何决定是否创建一个 Paper Run

### 4.1 真实 session，不是电脑日期

AlpacaPaperSessionClock 获取真实 broker clock，读取 Rust 解释出的 `TradingSession`。当前源码使用 `session.kind != Closed` 判断可取得 session key，不仅仅检查 Regular market 的 `clock.is_open`。因此文档里笼统的“每天开市时一次”不能替代当前扩展时段实现。

证据：[scheduler.rs](../../crates/akzio-daemon/src/scheduler.rs)。

### 4.2 tick 的分支顺序

1. 没有开放 session：返回等待，不创建 Run。
2. 本 session 已有 slot：返回已有 Run / graph / task 身份，不因按钮重复点击、进程重启或前次失败再开一张图。
3. 检查当前 Canary campaign；Staged 时不自行推进，进入 level 时走专门 canary 分支。
4. 检查 SQL active DecisionPolicy：
   - 缺失：cold start，不绑定交易 approval，即使 Store 里有旧 approval 也不借用。
   - 已有：必须取得当前 approval binding；缺失就等待。
5. 有 approval 时继续核对 RuntimeManifest 的 runtime identity、真实 broker account ID、market-data feed。
6. 选择 Rust 已批准的 WorkflowProposal；不可用就等待。
7. 取得/续约 scheduler leader lease。
8. 为本次 Run 分配 Run ID；过滤旧 Run 的 RunScoped EvidenceNeed。旧 scheduler snapshot 需求必须重新铸造，不能把前一天账户/报价带进今天。
9. 生成本 session 的 40 项 EvidenceNeed，并绑定到本 Run 的 provenance。
10. 在同一 Store 写路径中原子预约 session slot 和完整图；如果有 approval，使用批准绑定的专门事务。

“创建了 slot”只表示本次工作被持久化，不表示证据齐全、模型成功、Decision 通过或订单已提交。

证据：[scheduler_tick.rs](../../crates/akzio-daemon/src/scheduler/scheduler_tick.rs)、[workflow.rs](../../crates/akzio-runtime/src/runtime/workflow.rs)。

### 4.3 防重复不是只有一个内存锁

- scheduler leader lease 带 owner、epoch、expiry。
- SQL session slot 以 session key 唯一。
- Store 还验证 Run 必须存在，且同一 Run 不能占有另一 session slot。
- 每次最终写事务都重新核对 lease，不相信旧进程手里的缓存。
- scheduler 重启不重新 lowering 旧 proposal，也不为同一 session 重新分配 Task IDs。

证据：[lease.rs](../../crates/akzio-daemon/src/scheduler/lease.rs)、[free_lifecycle.rs](../../crates/akzio-store/src/store/free_lifecycle.rs)、[workflow.rs](../../crates/akzio-runtime/src/runtime/workflow.rs)。

## 5. 图是怎样编译出来的

### 5.1 研究骨架在创建时完整冻结

依次预建：

1. `analyst_t1`、`analyst_t3`、`analyst_t5`。
2. 每个 Analyst 对应一个 `critic_t*`，只依赖相应 Claim 路径。
3. 一个 `supplement`，等待首轮全部六个节点。
4. 三个 `analyst_t*_refined` 和三个 `critic_t*_refined`；没有合格新增事实时由业务 handler 明确跳过。
5. `synthesizer_0 → proposal_review_0`。
6. 若允许修订，预建 `synthesizer_1 → proposal_review_1`，一直到配置上限 N。
7. 各 Synthesizer 依赖首轮/重跑的有效研究闭包，修订版还依赖上一轮 Review。Rust 选择有效 revision，而不是把两个互相矛盾的 Claim 随意拼在一起。

这是预冻结的有界工作图，不是模型出错后临时生成无限 worker。

研究节点数：首轮 6 + supplement 1 + refined 6 + `(N+1)×2` = `15+2N`。

- PositionPlan 再加 Evidence + Decision，合计 `17+2N`，默认 N=2 为 **21**。
- Paper 再加六个 Rust 节点，合计 `21+2N`，默认 **25**。
- 当前 lowering 的 Shadow 加五个 Rust 节点，合计 `20+2N`，默认 **24**。
- 图上存在节点不代表每个节点都会发模型请求；跳过也是可审计结果。

证据：[workflow.rs](../../crates/akzio-runtime/src/runtime/workflow.rs)、[evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)、[agent-runtime-contract.md](../agent-runtime-contract.md)。

### 5.2 每个节点都带什么

`NodeSpec` 保存稳定逻辑 key、horizon、research round、proposal revision；`WorkflowNode` 还冻结 recipe、Contract hash、依赖、输入 Artifact refs、priority、TaskBudget、RetryPolicy、on_failure。

`objective` 是解释文字，不是授权字段。模型可见的 `[research_horizon=...]` 等标记由 Rust 从 NodeSpec 生成。旧图缺少 spec 时，只做集中只读历史适配，不回写旧 CAS。

证据：[workflow_definition.rs](../../crates/akzio-domain/src/workflow_definition.rs)、[evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)。

### 5.3 编译器主动阻止的东西

- 在研究 proposal 中塞入 Rust terminal gate。
- 非法 recipe、Contract、预算、重试或 failure policy。
- 重复/循环/缺失依赖、超出 fanout/depth/node 上限。
- 研究绕过唯一 EvidenceGate。
- Decision 不等待完整研究叶节点。
- PositionPlan 混入 Execution/Paper/Reconcile/Evaluate。
- 非 Paper 图包含 PaperCommit。
- Shadow candidate 未按安装记录与能力不扩张约束匹配。

证据：[evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)、[validation.rs](../../crates/akzio-runtime/src/runtime/compilation/validation.rs)。

## 6. Worker 怎样领取和运行任务

### 6.1 并行受容量与依赖共同约束

图允许三个期限在依赖满足后并行，但不承诺它们在现实中同一瞬间开始。WorkerPool 受实际 worker_count、Session/Outcome 预留容量、ready_at 和前置任务状态约束。

多 worker 时，index 0 保留给 Session，index 1 保留给 Outcome，其余为共享 worker；单 worker / 共享 worker 交替优先两类队列，并在无任务时尝试另一类。这样旧 T+5 不会按设计把新 T0 串行阻塞，反之亦然。空闲轮询默认 250ms。

证据：[worker.rs](../../crates/akzio-daemon/src/worker.rs)。

### 6.2 claim 是一个 SQL 事务

Store 筛选：

- queued 且 `ready_at <= now`；
- RunControl 允许连续运行，或 stepping 精确许可当前 Task 且没有 active attempt；
- 隔离身份相符；
- workload 匹配；
- Run 未取消，状态允许运行；post-terminal Outcome 是特例；
- 每个依赖都已 `succeeded` 或 `skipped`。

排序实际为 `ready_at ASC, priority DESC, task_id ASC`，不是只看 priority。

命中后生成新 Attempt ID、Lease ID，并使 epoch+1；条件更新 Task 为 running，插入 Attempt，保存 TaskStarted、AttemptRelation 和控制许可消费；事务成功后 permit 才交给 Worker。

证据：[commits.rs](../../crates/akzio-store/src/store/workflow/commits.rs)。

### 6.3 执行期间同时发生三件事

TaskRuntime 并行 poll：

1. 真正的业务 handler。
2. heartbeat + durable cancellation 检查。
3. 当前 Task 的 wall-clock timeout。

lease 默认 30s，heartbeat 默认约 10s；失去 permit、租约过期或被更高 epoch 接管的旧 worker 不能再写。timeout 返回可受冻结 policy 限制的 Retry，不自动放宽额度。

退出 select 后先 drop handler/monitor 的 futures，再执行终态写入，避免排队中的异步许可占用造成完成路径死锁。取消某个 async waiter 不等于已经开始的同步 SQLite 操作被撤回。

证据：[task.rs](../../crates/akzio-runtime/src/runtime/task.rs)、[free_lifecycle.rs](../../crates/akzio-store/src/store/free_lifecycle.rs)、[store_executor.rs](../../crates/akzio-runtime/src/runtime/store_executor.rs)。

### 6.4 handler 的返回值不是同一种“成功”

| 返回值 | 实际含义 |
|---|---|
| `Succeeded(artifacts)` | 交由 Store 原子提交本阶段产物与成功终态 |
| `NoOutput` | 本阶段可以成功，但不制造重复 Artifact |
| `Committed` | handler 已完成专门事务；Runtime 再核对精确 Attempt 真正成功 |
| `Skipped` | 明确不执行该节点；后继可沿其依赖闭包读取有效来源 |
| `DeferredUntil` | 业务等待；关闭当前 Attempt，稍后重新排队，不消耗失败预算 |
| `Retry / RetryAfter` | 根据冻结 RetryPolicy 与已持久化失败次数决定重试或终止 |
| `Failed` | 当前节点失败；按冻结 on_failure 影响图 |
| `Cancelled` | 取消当前 Attempt；不等于已提交的 Broker 副作用被撤销 |

证据：[runtime.rs](../../crates/akzio-runtime/src/runtime.rs)、[task.rs](../../crates/akzio-runtime/src/runtime/task.rs)。

### 6.5 retry / recovery / defer 的差别

- Retry 受 `max_attempts`、transport/rate-limit/invalid-output 开关限制；次数从 SQL 读取。
- Deferred 是条件未成熟，例如市场未开放或 Outcome session 未结束，不拿失败次数惩罚正常等待；Runtime 至少推迟 1 秒防止空转。
- lease 过期由单独 recovery loop 扫描；有额度则原 Attempt 记 abandoned，Task 重新排队；无额度按 failure policy 收束。
- 新 Attempt 与旧 Attempt 保留 `Retry` 或 `Recovery` 的关系，恢复不改原 Run/Task/Contract。
- Outcome 的失败退避从至少 30 秒指数增加，上限 5 分钟；各成熟阶段的已提交事件形成独立计数边界。

证据：[commits.rs](../../crates/akzio-store/src/store/workflow/commits.rs)、[outputs.rs](../../crates/akzio-store/src/store/workflow/outputs.rs)、[task.rs](../../crates/akzio-runtime/src/runtime/task.rs)。

### 6.6 失败怎样传播

- `FailRun`：当前普通节点失败后，取消该 Run 尚未领取的 queued 任务；不是在该事务里强杀所有已在运行的 Future。
- `FailTask`：沿依赖递归取消受失败/取消父节点阻塞的 queued 子任务。
- `SkipTask`：把请求的 Failed 映射为 Skipped，保留明确的跳过终态。
- post-terminal Outcome 失败不反向把已结束的 T0 Run 改成失败。
- Reconcile 默认 `RetryPolicy::none()`、30 秒 wall。正常未成交的 Deferred 不消耗失败额度；但一次崩溃留下 running Attempt 或外层 timeout，并不因此保证有第二次自动执行机会。必须把“可用相同 ID 查询的恢复机制”与“调度一定再次运行”分开。

证据：[free_policy_helpers.rs](../../crates/akzio-store/src/store/free_policy_helpers.rs)、[catalogue.rs](../../crates/akzio-runtime/src/runtime/catalogue.rs)、[workflow/helpers.rs](../../crates/akzio-store/src/store/workflow/helpers.rs)。

## 7. 后继到底能读取哪些“已完成结果”

下游输入不是“扫描这个 Run 的所有 Artifact，把看起来有用的都拿来”。

- 先沿声明的依赖 DAG 找祖先。
- Succeeded 节点只能读取 `rebuild_attempt_outputs` 中的正式成功输出。
- Skipped 节点没有新结果，继续沿其祖先保留来源。
- Running / Failed 等状态不能被包装成合法的上游成功输入。
- 中间 AgentTurn、ToolCall、失败尝试留下的材料、未完成 Attempt 的阶段数据，不会只因为已经落盘就成为正式输出。
- 成功 Artifact 的索引与对应 `artifact.committed` journal cursor 必须匹配。

因此三个概念不能混淆：**staged payload、已持久化阶段 Artifact、成功 Attempt 的下游输出**。

证据：[dispatch.rs](../../crates/akzio-daemon/src/dispatch.rs)、[outputs.rs](../../crates/akzio-store/src/store/workflow/outputs.rs)、[free_reads.rs](../../crates/akzio-store/src/store/free_reads.rs)。

## 8. CAS 中保存的是什么

### 8.1 两层内容身份

1. `BlobRef.hash`：未压缩逻辑字节的 SHA-256；压缩不改变逻辑内容身份。
2. `ArtifactId`：包含 kind、BlobRef、producer、lifecycle、provenance、origin、source_refs 和 created_at 等元数据的内容哈希，去掉 artifact_id 自引用后计算。

同一文本可以复用 BLOB，但来源、时间或生产者不同的 Artifact 不因此等价。`source_refs` 不只是 UI 超链接，而是后续精确权限、复盘与执行证明的一部分。

Artifact 的来源字段包括 source_family、observed_at、retrieved_at、source_uri、confidence_ppm、producer_contract_hash；origin 则保存 Run/Task/Attempt/Contract 身份。

证据：[core.rs](../../crates/akzio-domain/src/core.rs)、[artifact.rs](../../crates/akzio-domain/src/artifact.rs)。

### 8.2 staged 不等于 durable

`stage_json / stage_bytes` 先写当前 SQLite connection 私有 TEMP staging 表。只有引用它的 Artifact 写事务执行 promotion，逻辑内容才进入 durable `rebuild_blobs`。输出仍须业务校验、permit 检查和最终提交。

这既允许 Rust 在正式提交前读回验证模型结果，也避免“只写了大块 BLOB、没有合法 Artifact/事件”的半成品被下游使用。

证据：[blob.rs](../../crates/akzio-store/src/store/blob.rs)。

### 8.3 物理存储和校验

- 支持 identity、zstd、精确父字节切片和 zstd dictionary 表示。
- 普通负载达到 1024 bytes 才尝试 zstd level 3；压缩结果加 64 bytes 仍小于原文才采用。
- 切片/字典保留父 BLOB 依赖，不把省略数据视为不存在。
- 读取核对物理长度、逻辑长度、编码、依赖、深度/环和最终内容 hash；坏内容报错，不静默给默认值。
- BLOB 依赖深度上限 32。

证据：[prelude.rs](../../crates/akzio-store/src/store/prelude.rs)、[blob.rs](../../crates/akzio-store/src/store/blob.rs)。

### 8.4 核心表家族

| 表家族 | 用途 |
|---|---|
| `rebuild_blobs` / dependencies / embedded refs | 统一载荷和派生内容依赖 |
| `rebuild_artifacts` / `rebuild_artifact_refs` | 类型化不可变对象、元数据、血缘 |
| `rebuild_runs` / workflow revisions | Run 身份与冻结图 |
| `rebuild_tasks` / task dependencies | 队列、节点规格、冻结预算、lease、ready_at |
| `rebuild_attempts` / attempt outputs | 尝试与正式输出 |
| `rebuild_events` | 单调递增 journal cursor |
| `rebuild_daemon_leases` / session slots | 进程领导权与 Paper session 排他预约 |
| `rebuild_run_controls` | 唯一 RunControl head |
| paper approval consumptions / execution cancels / reprices | 审批消费与执行恢复索引 |
| contract installations / activations / catalogue heads | Contract 冻结历史与当前头 |
| decision-policy installations / activations / head | DecisionPolicy 的 SQL 权威 |
| policy evaluations / transitions / consumption heads | 学习政策的评估与迁移 |
| lesson heads / events / evidence | 有界 Lesson 生命周期 |
| shadow pairs / canary tables | 对照、分级试验及其进度 |

业务对象本体仍以类型化 JSON/CAS 保存，不是另造 claims/orders 等业务平铺表，更不是日志文件、导出 JSON 和数据库各当一份权威。

证据：[free_validation.rs](../../crates/akzio-store/src/store/free_validation.rs)、[agent-runtime-contract.md](../agent-runtime-contract.md)。

## 9. 原子提交、RunControl 与 checkpoint

### 9.1 最终提交边界

成功 Attempt 的 Artifact、血缘、journal、attempt_outputs 和 Task/Attempt 终态在同一 SQLite 事务提交。单独 `write_task_artifact` 可以保存阶段证据，但不自动让 Task 成功，也不自动进入下游输出索引。

涉及 broker-visible 输出时，还要同时验证 daemon lease 和 Task permit。write permit 的有效性使用实际写入时刻判断，不能拿 Artifact 的历史 observed_at 续命。

证据：[tasks.rs](../../crates/akzio-store/src/store/workflow/tasks.rs)、[free_lifecycle.rs](../../crates/akzio-store/src/store/free_lifecycle.rs)。

### 9.2 唯一控制 head

新 Run 有连续运行的初始 RunControl。Debug 的 pause / step / resume 等仍在同一 head 上做 revision CAS 和精确任务许可，不存在第二个 JSON 控制文件。

WorkflowStatus、RunControlStatus、ExecutionVerdict、Outcome 状态是不同视角。控制结束不是研究通过；T0 执行生命周期完成也不是跨交易日复盘完成。Outcome 入队可以唤醒普通控制 head，而不假造一张新的 T0 研究图。

证据：[run_control.rs](../../crates/akzio-store/src/store/run_control.rs)、[workflow-runtime.md](../workflow-runtime.md)。

### 9.3 checkpoint 保存什么、不能做什么

图创建、节点开始/结束、重试、延期、恢复、取消、控制变更、Outcome 入队等边界，会在同一事务保存 RunScoped RuntimeCheckpoint。

内容包括 Run、graph ref、覆盖的 event cursor、control revision、runtime identity（如果有）、Task/Attempt、边界类型、来源 Artifact 和时间。checkpoint 自身也写入同一 journal。

读取时核验它确实指向该 Run 的合法图 revision、原始边界事件和 source refs；不允许把任意 JSON 当恢复快照。

checkpoint 不是序列化的进程，不保存正在 await 的 Future；也不替代 AgentTurn/Tool ledger、预算累计、lease fencing、Commitment 和 reconcile。更不能由 checkpoint 的存在推出外部副作用 exactly-once。

证据：[run_control.rs](../../crates/akzio-store/src/store/run_control.rs)、[workflow-runtime.md](../workflow-runtime.md)。

## 10. 重启、Replay、Doctor、迁移分别意味着什么

- **Recovery**：从同一个 SQL snapshot 读取已有 graph/head/checkpoint；复用 Run/Task IDs，核对历史冻结 Contract，不按今天配置重建旧图。
- **Replay**：按 event cursor 重放事件，重建 revisions 和 Task 状态，再与 Store snapshot 比较；不调用模型，不重新交易。
- **Doctor**：校验结构、CAS、血缘和各类 lifecycle；完整性通过不是研究正确或策略盈利的证明。
- **打开已有 Store**：`open_existing` 是只读，要求 schema 精确匹配，不隐式迁移。
- **初始化/升级**：只有显式可写 `Store::open`；迁移前检查 queued/leased/running task 与有效 daemon lease，不能仅停止进程就认为已经可升级。
- **Store 18**：新增/迁入共享 RunControl 与 node_spec 列；历史 CAS、graph、Contract、Commitment 和事件不回写；历史 Run 不虚构 checkpoint。

证据：[replay.rs](../../crates/akzio-runtime/src/runtime/replay.rs)、[schema.rs](../../crates/akzio-store/src/store/schema.rs)、[free_validation.rs](../../crates/akzio-store/src/store/free_validation.rs)。

## 11. 源码核查发现的文档口径差异

1. 当前源码研究 Contract **69** / PromptBundle **38** / freshness candidate **70**；`CONTEXT.md` 仍出现 Contract 67 / Prompt 37 / candidate 68。
2. Store 常量为 **18**；`CONTEXT.md` 仍出现 17。
3. 当前默认 PositionPlan 图为 **21** 节点；README 某些 CLI 说明仍写“九节点”，不能据此描述当前完整拓扑。
4. README 的某些历史说明仍提 `--fixture-controller`；当前开发 Workflow 已明确删除旧入口。
5. README/历史词汇表的“开市时一次”应结合源码的非 Closed session 解释，不能误读为只支持 Regular。
6. “默认 worker 2”是 WorkerPool 类型默认，不是 CLI 生产入口默认 4。
7. 文档里某些旧 input budget 数字不能覆盖新 Run 冻结预算；实际 role budget / Contract / provider cap 在研究章节独立说明。

这里没有修改这些文档。以上差异的意义是：讲解以当前消费者、类型和校验为准；历史 Run 则以其原冻结身份为准，不能用今天的版本重写历史。

证据：[catalogue.rs](../../crates/akzio-research/src/agent/catalogue.rs)、[prelude.rs](../../crates/akzio-store/src/store/prelude.rs)、[CONTEXT.md](../../CONTEXT.md)、[development-workflow.md](../development-workflow.md)、[README.md](../../README.md)。
