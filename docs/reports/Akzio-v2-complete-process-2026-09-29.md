# Akzio v2 全流程源码详解（2026-09-29）

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

**阅读范围：**截至 2026-09-29 当前未提交工作树，起点 HEAD `e4292f09acf5b3798bf16de26718ceb85046a190`；本文合并主助手控制流/持久化核查、六个独立模块核查和跨模块纠偏。它是源码级说明，**不是当前运行 Core、真实模型、Paper 成交或 T+5 学习的验收报告**。原工作区已有未提交改动，未触碰其源码或配置；当前合并版位于 `docs/reports/`；最初的运行证据快照与本报告文件应区分。

**配套图：**[全流程与 DecisionPolicy 合并版 HTML](../../.archify/akzio-process/akzio-complete-process.html)固定到上述 HEAD 的已提交源码；本文还阅读了未提交工作树，二者证据范围不同，不能用图替代当前工作树或运行验收。

**怎么读：**按从启动到到期 Outcome 的顺序阅读第 0–6 部分，再读第 7 部分的跨模块语义、静态限制与文档漂移。每部分保留仓库相对来源文件、关键职责、默认值、异常分支和未验证边界。本文不是行动许可；任何会访问账户/调用模型/提交 Paper 订单/激活 Policy 的操作仍走原身份、审批和 Gate。

## 目录

| 部分 | 内容 | 独立文件 |
|---|---|---|
| 0 | 控制流、固定拓扑与统一 CAS Store | [00-runtime-store.md](00-runtime-store.md) |
| 1 | 40/34 项证据、来源资格与 Context 授权 | [01-evidence-context.md](01-evidence-context.md) |
| 2 | Analyst/Critic/Synthesizer/Reviewer、模型与补采 | [02-research-model.md](02-research-model.md) |
| 3 | DecisionGate、风险计算与 SQL 校准 | [03-decision-calibration.md](03-decision-calibration.md) |
| 4 | Paper 执行、Commitment、Dispatch 与 Reconcile | [04-paper-execution.md](04-paper-execution.md) |
| 5 | 跨 Session Outcome、复盘、学习、Lesson/Canary | [05-outcome-learning.md](05-outcome-learning.md) |
| 6 | App/Core/CLI、Debug、Observer 与安全导出 | [06-entry-observatory-debug.md](06-entry-observatory-debug.md) |
| 7 | 跨模块状态语义、代码实际限制和纠偏 | [07-integration-boundaries.md](07-integration-boundaries.md) |

---

<!-- 分报告 0: 00-runtime-store.md -->

# 主线：系统入口之后，Workflow、调度、状态与存储如何共同推进

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


---

<!-- 分报告 1: 01-evidence-context.md -->

# 数据采集与 Context 全流程：当前工作树只读核查

- 核查日期：2026-09-29（Asia/Singapore）。对象：当前工作树文件，不是 Git HEAD、已部署二进制或某次 Run 的实际结果。
- 唯一写入：本报告。没有修改源码、配置、Store；没有执行 cargo、测试、真实模型、Broker 或外部 API；没有读取用户凭据或数据库内容。
- 证据标签：**source-inspected（当前源码已查）**。不是 offline-verified、real-Paper-verified 或 outcome/learning-verified。
- 启动和结束的 `git status --short` 均显示他人既有改动：CI、根 AGENTS、README、daemon HTTP/launch/lib/health_canary、Store debug_bundle、运行时/Debug/开发/退役文档、run_core.py，另有未跟踪 test_run_core.py。均按当前内容读取且未处理。
- 已先读根 AGENTS、运行时契约 3/5/6/11/12 节及 news-evidence 文档。本报告只覆盖采集和 Context；调度/Runtime/Store 仅列跨模块接口，不复述其状态机；研究执行、修订及 Gate 业务裁决交其他 worker。
- 下文以仓库相对文件路径及职责/符号说明定位来源；源码移动后须重核行为，不能把静态阅读当作联网或测试通过。

## 1. 给主助手的核心结论

1. **40 项是 Need 数，不是 40 次 HTTP，更不是每个模型能看 40 件材料。** Paper 固定 40 个去重 EvidenceNeed：6 ExecutionSafety + 34 DirectionalResearch；PositionPlan 去掉六项执行安全 Need，保留 34 项。当前固定表中 Enhancement=0。六项前置只标记 deferred_to_execution，不采集；执行阶段才重新获取。依据：`crates/akzio-domain/src/workflow.rs`；`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-daemon/src/evidence.rs`。
2. **EvidenceGate 是受治理输入采集/封存边界，不是事实真值或交易许可。** 已知来源失败可保留缺口，成功项继续；哪怕所有研究来源不可用，collection-status Artifact 仍可能使门面返回 Succeeded。时间污染阻断整个 Gate；身份、引用、策略、Store 错误不得降级成普通覆盖缺口。依据：`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/evidence_acquisition.rs`。
3. **新闻当前主路径为 discovery + 独立模型 source review，而非 Rust 独立抓网页。** `model_reviewed`、`citations_complete=true`、URL 绑定都不等于 `source_verified=true`。可用事实和失败项允许共存；空事实、审阅失败也可能形成合法 NormalizedEvidence 并在 Gate 被记 available。依据：`crates/akzio-ingest/src/news.rs`；`crates/akzio-daemon/src/evidence.rs`。
4. **时间边界并非“下载新鲜即内容新鲜”。** 日线严格按美东交易所 close+20m、宏观按前一自然日 vintage、期权保留各字段时间和缺失。但通用 `StaleEvidence` 实际比较 `now - acquired.observed_at`，生产适配器通常把 observed_at 设为抓取完成时间，未统一用 `available_at` 做 max_age。须报告此处文档/实现差异。依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`docs/agent-runtime-contract.md`。
5. **Context 是精确引用授权，不是一个目录/来源族的泛化读取权。** Manifest 固化选择；ReadGrant 再绑定 Run/Task/Attempt/Lease/epoch/Contract、精确 readable 和过期时间；读取重验持久化闭包和当前 permit。RawEvidence 不进 Manifest，当前所有活动 Contract 均 `allow_raw_reread=false`。依据：`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
6. **预算要讲清完整来源与投影分离。** 24 件上限；普通角色投影128 KiB、来源512 KiB；Synthesizer投影192 KiB、来源768 KiB。必需闭包先于可选背景；过大必需输入直接拒绝。原始期权 NormalizedEvidence 在条件满足时被替换为可引用 SemanticDetail 投影，保留原 source_ref，但不自动授予原对象读取权。依据：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`。
7. **召回、选中、内联正文、工具读取是四件不同事。** 当前 Lesson 召回只向允许 Lesson 的 Contract 开放，默认研究链只有 Synthesizer；但通用 selection reason 为 lesson，must_read_class 无 Lesson 分支，因此默认研究 materialization 只列 metadata，没有 Lesson 正文，且研究无读工具。这是静态路径推导，不是运行复现。依据：`crates/akzio-context/src/selection.rs`；`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。

## 2. 全链路图与数据对象

```text
冻结 EvidenceNeed（Rust 的 resource/source/max_age）
  -> daemon 核对完整固定集合、producer、Run、去重
  -> ExecutionSafety 六项：仅 collection-status deferred_to_execution
  -> 其余 Need：EvidenceRequest + 指定 adapter（可并发，逐项有界）
       Alpaca direct / FRED direct / SEC direct（未进固定40项）
       news_web router：issuer direct 或 native discovery + source review
  -> AcquiredEvidence { raw, normalized JSON, provenance, quality }
  -> EvidenceRuntime：请求授权 + URI/citation/quality + 时间污染检查
  -> RawEvidence + NormalizedEvidence（Normalized source_refs = Raw + Need）
  -> evidence.collection_status（各 Need 的状态与诊断）
  -> EvidenceGate task result（不等于研究/方向/执行通过）
  -> 调用方显式 candidate refs
  -> ContextBroker：Contract/来源/Run/overlay/隔离过滤
       + 允许时 Lesson/Experience 召回
       + 期权 source -> 受治理 SemanticDetail 投影
       + 必需闭包 -> 平衡背景优先级 -> 件数/来源/投影/token预算
  -> ContextManifest（CAS）+ 当前 Attempt 的 ReadGrant（内存授权）
  -> materialize_for_agent_with_budget
       研究：已授权 projections + metadata；无资料读取工具
       Outcome：独立 projection + 五个受控读工具
```

对象证据：`crates/akzio-ingest/src/runtime.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。

注意 RawEvidence 并非统一“文章原始HTML”：直接 API 是响应 bytes；市场捕获是多请求 JSON 账本；ModelReviewed 新闻是 discovery provider envelope 加 review 审计的 NDJSON；历史 VerifiedSource 模式可拼接来源正文。其含义是适配器保留的原始获取材料，不是统一媒体类型。依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-ingest/src/adapters.rs`。

## 3. 40 / 34 项完整清单

### 3.1 表中符号和计数

- `S` = Run 已冻结的 session 日期，不是本机今天；`B=S-400自然日`、`N=S-14日`、`M=S-366日`、`V=S-1日`、`O=S+120日`、`C=S+45日`。
- E=ExecutionSafety；D=DirectionalResearch。**下表 1–6 仅 Paper；7–40 同时属于 Paper 和 PositionPlan**。每行一个唯一 Need；不按 registry requirement 出现次数重复计数。
- 基础17项 = 6执行 + 4日线 + 4新闻 + 3宏观。Instrument registry 另外贡献24个唯一Need，其中 paper.quotes 与基础表重复1个：17+24-1=40。它的4个 LiquiditySpread requirement 共用同一 paper.quotes，不能算成4次需求。
- 源码按 BTreeSet 排序，本表为了说明按业务分组，**不是生产顺序或实际请求顺序**。

清单依据：`crates/akzio-domain/src/workflow.rs`（基础）；`crates/akzio-domain/src/instrument_evidence.rs`（扩展）；分类依据 `crates/akzio-domain/src/workflow.rs`；过滤依据 `crates/akzio-daemon/src/evidence.rs`。

| # | source_family | 完整 resource 模板 | max_age_secs | 分类/用途 | 清单来源 |
|---:|---|---|---:|---|---|
|1|alpaca|`paper.account`|300|E 账户|基础|
|2|alpaca|`paper.positions`|300|E 持仓|基础|
|3|alpaca|`paper.open_orders`|300|E 挂单|基础|
|4|alpaca|`paper.fills:S`|300|E 当session成交活动|基础|
|5|alpaca|`paper.quotes`|300|E 四资产报价/流动性共享|基础；扩展|
|6|alpaca|`paper.clock`|300|E 市场时钟|基础|
|7|alpaca|`bars:TQQQ:1d:B:252`|604800|D 价格|基础|
|8|alpaca|`bars:QQQ:1d:B:252`|604800|D 价格|基础|
|9|alpaca|`bars:SOXX:1d:B:252`|604800|D 价格|基础|
|10|alpaca|`bars:SOXL:1d:B:252`|604800|D 价格|基础|
|11|news_web|`news:TQQQ:N:S:market`|604800|D 近期新闻|基础|
|12|news_web|`news:QQQ:N:S:market`|604800|D 近期新闻|基础|
|13|news_web|`news:SOXX:N:S:market`|604800|D 近期新闻|基础|
|14|news_web|`news:SOXL:N:S:market`|604800|D 近期新闻|基础|
|15|fred|`series:DFF:M:S:V`|604800|D 宏观|基础|
|16|fred|`series:DFII10:M:S:V`|604800|D 宏观|基础|
|17|fred|`series:VIXCLS:M:S:V`|604800|D 宏观|基础|
|18|news_web|`research:etf_holdings:TQQQ:S`|86400|D 官方持仓|扩展|
|19|news_web|`research:etf_holdings:QQQ:S`|86400|D 官方持仓|扩展|
|20|news_web|`research:etf_holdings:SOXX:S`|86400|D 官方持仓|扩展|
|21|news_web|`research:etf_holdings:SOXL:S`|86400|D 官方持仓|扩展|
|22|news_web|`research:index_metadata:TQQQ:S`|86400|D 指数元数据|扩展|
|23|news_web|`research:index_metadata:QQQ:S`|86400|D 指数元数据|扩展|
|24|news_web|`research:index_metadata:SOXX:S`|86400|D 指数元数据|扩展|
|25|news_web|`research:index_metadata:SOXL:S`|86400|D 指数元数据，当前adapter明确缺口|扩展|
|26|news_web|`research:earnings_event_calendar:TQQQ:S`|86400|D 成分公司事件日历|扩展|
|27|news_web|`research:earnings_event_calendar:QQQ:S`|86400|D 成分公司事件日历|扩展|
|28|news_web|`research:earnings_event_calendar:SOXX:S`|86400|D 成分公司事件日历|扩展|
|29|news_web|`research:earnings_event_calendar:SOXL:S`|86400|D 成分公司事件日历|扩展|
|30|news_web|`research:leveraged_etf_terms:TQQQ:S`|604800|D 3倍每日重置条款|扩展|
|31|news_web|`research:leveraged_etf_terms:SOXL:S`|604800|D 3倍每日重置条款，当前adapter明确缺口|扩展|
|32|alpaca|`corporate_actions:TQQQ:M:S`|86400|D 公司行动|扩展|
|33|alpaca|`corporate_actions:QQQ:M:S`|86400|D 公司行动|扩展|
|34|alpaca|`corporate_actions:SOXX:M:S`|86400|D 公司行动|扩展|
|35|alpaca|`corporate_actions:SOXL:M:S`|86400|D 公司行动|扩展|
|36|alpaca|`option_chain:TQQQ:S:O`|300|D 期权链/IV背景|扩展|
|37|alpaca|`option_chain:QQQ:S:O`|300|D 期权链/IV背景|扩展|
|38|alpaca|`option_chain:SOXX:S:O`|300|D 期权链/IV背景|扩展|
|39|alpaca|`option_chain:SOXL:S:O`|300|D 期权链/IV背景|扩展|
|40|fred|`release_calendar:S:C:V`|86400|D 共享宏观发布日历|扩展|

其中“基础”完整路径为 `crates/akzio-domain/src/workflow.rs`；“扩展”完整路径为 `crates/akzio-domain/src/instrument_evidence.rs`。`EvidenceNeed` 全字段参与去重（schema/source/resource/max_age），不是仅resource去重：`crates/akzio-domain/src/workflow.rs`。

**关键解释：** PositionPlan 不含 paper.quotes Need，但研究日线/期权的底层 `capture_stock` 仍会 GET 资产、clock、calendar、snapshot/quote/trade。故“六项执行 Need 不采集”不等于“研究时完全不访问 Paper只读端点/不获取任何quote”。两者用途和 Artifact 身份不同。依据：`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-ingest/src/session_bars.rs`。

## 4. 输入源、资源语法和授权限制

### 4.1 Rust-owned 三道请求约束

1. `EvidenceNeed::validate`：schema正确、source/resource非空、resource最多2048字符、max_age 1..604800秒、来源仅 alpaca/sec_edgar/fred/news_web。`crates/akzio-domain/src/workflow.rs`。
2. Paper/PositionPlan 的 daemon 在I/O前核对**整个Need集合完全等于冻结session对应政策**；每个Artifact为EvidenceNeed、producer=`scheduler.paper_snapshot`、RunScoped且同Run，拒绝多/少/重复项。Paper session从已持久化slot取；PositionPlan从冻结option_chain Need取得唯一session，不要求当前市场开放。`crates/akzio-daemon/src/evidence.rs`。
3. `EvidenceRuntime::authorize_request` 从Store重读Need，核对kind、同Run、source/resource/max_age与request完全一致，source在当前runtime allowlist且adapter.source一致；`EvidenceRequest::validate`再调用GovernedResource解析器。它不是任意URL代理。`crates/akzio-ingest/src/materialization/materialize_raw.rs`；`crates/akzio-ingest/src/runtime.rs`。

资源语法范围：四资产quotes/bars、公司行动跨度≤366日、option区间≤180日；普通bars limit≤252；FRED series名称≤64且仅ASCII字母数字/`._-`、窗口≤366日；release_calendar跨度≤90日且vintage<start；news近期窗口≤31日，topic仅market/rates/semiconductor/regulation/earnings/geopolitics；research类别只有holdings/index/leveraged terms/earnings calendar。SEC CIK/accession/文档名有独立格式验证。**通用GovernedResource仍存在NewsWeb query分支，不能笼统说底层完全没有自由查询；正式固定Need政策及当前补采接口并不让研究模型直接调用该分支。** `crates/akzio-ingest/src/runtime.rs`。

### 4.2 实际适配器注册与外部地址

- Daemon从环境构造Alpaca、SEC、FRED；构造失败不注册对应source，实际采集报告adapter不可用；news_web统一使用configured router，不因一次capability probe结果永久禁止采集。这里没有核验本地实际配置是否满足。`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- Alpaca仅接受精确Paper base URL（可尾随 `/`），行情host固定data.alpaca.markets；凭据为空/非Paper URL在I/O前拒绝；HTTP不follow redirect。股票必须显式IEX/SIP；期权默认Indicative，显式配置可改OPRA，不自动fallback。`crates/akzio-ingest/src/adapters.rs`。
- SEC直连data.sec.gov / www.sec.gov，环境键SEC_USER_AGENT；FRED直连api.stlouisfed.org，环境键FRED_API_KEY。客户端不follow redirect；SEC rate gate125ms、FRED250ms。FRED key仅放实际请求URL，provenance用无key public_url。SEC存在实现但**不属于固定40/34项**。`crates/akzio-ingest/src/direct.rs`。
- issuer direct按完整host允许：dng-api.invesco.com、www.invesco.com、accounts.profunds.com、www.proshares.com、www.ishares.com、www.direxion.com；HTTPS、无自动redirect、正文≤4MiB。`crates/akzio-ingest/src/official.rs`。
- native NewsWeb域名组：Reuters、AP、ETFChannel（含m子域）、ETF.com、Invesco、ProShares、iShares、BlackRock、Direxion、Nasdaq、NYSE；具体列表是代码，不是默认NativeWebPolicy那四个域名。host可为受限域合法子域，不允许后缀伪装；HTTPS、不带user/password/端口。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-model/src/native_web.rs`。
- 通用provenance URI检查禁止认证、fragment、token/secret/password/api_key/key/authorization等query，但其函数自身没有强制HTTPS或source-host匹配；这些靠各生产adapter/policy约束，不能把它描述成单函数完整SSRF边界。`crates/akzio-ingest/src/runtime.rs`。

## 5. 原始采集 → 规范化 → 逐项状态 → EvidenceGate

### 5.1 时间、质量、溯源的实际检查顺序

- 异步`acquire_validated_async`先authorize，再`adapter.acquire_at(request, now)`，再基本`validate_acquisition`（无CAS写入）；`materialize_validated`重新authorize，然后完整materialize。注意方法名“validated”不意味着完整时间基准已在第一次验证完成；`time_basis`及污染证书是在materialize阶段建立。`crates/akzio-ingest/src/materialization/materialize_raw.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- raw必须非空、media/source URI非空；provenance顶层source_uri/observed_at必须和AcquiredEvidence相等；dedupe_key非空；每条citation的原始byte slice必须精确等于quote；quality completeness≤1M且normalized=true，但**并不要求citations_complete=true**。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/runtime.rs`。
- Normalized payload保存schema/source/resource/Need ref/Raw ref/observed_at/time_basis/contamination_certificate/quant_features/financial_content/value/provenance/quality。Artifact为RunScoped、producer=`akzio.ingest.<source>.normalized`、source_refs精确Raw+Need；Raw producer是相应`.raw`。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- 异步materialize将completeness_ppm作为Normalized provenance.confidence_ppm；这是覆盖度元数据，不是预测置信度或语义正确率。同步fixture路径传入1M，所以两条入口的confidence构造不同。`crates/akzio-ingest/src/materialization/materialize_raw.rs`。

### 5.2 EvidenceGate的并发和部分成功

- 正式Paper/PositionPlan研究源用join_all；非fixture每项采集allowance=`max(max_wall_time_secs-15,1)`秒，余量留给验证/持久化。采完后用host `Utc::now()`冻结统一materialization cutoff；fixture沿用传入cutoff。`crates/akzio-daemon/src/evidence.rs`。
- 每项状态为available / unavailable / deferred_to_execution，并记录need ref、resource、criticality、diagnostic；collection-status写成SemanticDetail、producer=`evidence.collection_status`、source_family=`akzio.ingest`，缺方向材料的约定为neutralize_affected_slots。它不是ReadGrant。`crates/akzio-daemon/src/evidence.rs`。
- 普通覆盖错误类别：authorization、rate_limited、pending、adapter_unavailable、transport、permanent_provider_error、native-web细分类、data_quality、stale_content、acquisition_timeout；unknown/internal/invalid provenance/citation/need/policy/Store不降级。时间污染单独记unavailable/temporal_contamination，先保留状态与成功证据诊断，再返回Err。`crates/akzio-daemon/src/evidence.rs`。
- collection-status是先单独写入；成功Raw/Normalized一般随task结果返回。不能说“所有证据和状态是一次全有/全无事务”。门面只看返回Artifact是否为空决定NoOutput/Succeeded。`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/evidence_acquisition.rs`。
- registered Shadow canary在门面等待父证据（无则DeferredUntil+1秒），只将父成功材料引用连同精确collection-status包为`canary.evidence_snapshot`；不是重新获取一套市场证据。父成功/授权资格由Store接口负责，未在此报告重验Store实现。`crates/akzio-daemon/src/application/evidence_acquisition.rs`。

### 5.3 freshness / cutoff的准确含义

| 时间维度 | 实现 | 边界 |
|---|---|---|
| `event_time` | Alpaca从bars或snapshots的t/timestamp取最晚值；FRED取observations日期；SEC/news为None | 新闻逐事实event_date另验，非统一event_time |
| `available_at` | FRED=vintage日23:59:59 UTC；news/SEC=published_at，否则observed_at；Alpaca优先content_available_at，否则event_time/observed_at | 不能把所有来源一概称为精确发布时间 |
| `retrieved_at` | time_basis使用acquired.observed_at | Artifact.provenance.retrieved_at另用materialization now，二者不要混写 |
| 污染 | available/event/release/vintage≤DecisionClock；release≤availability、retrieval≥availability、event≤release（有值时） | 缺event/release时跳过相应比较，不是验证了一个不存在的时间 |
| stale | `now - acquired.observed_at > max_age` | 没有统一 `now - available_at` stale判断；新下载旧内容并不由此通用检查必然拒绝 |

表依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/runtime.rs`。

**尤其不能夸大：** EvidenceTimeBasis未显式要求retrieved_at≤cutoff。无可信published_at时available_at回退retrieval，历史晚抓会被挡；有过去published_at/vintage时，晚抓本身不必然违反此函数。证书五个true只表明上述实现条件通过（可选值为空的条件视为通过），不代表全部来源真的提供了原始时间字段。`crates/akzio-ingest/src/runtime.rs`。

## 6. 各资料族的获取和事实边界

### 6.1 新闻：discovery、model_reviewed、source_verified必须拆开

**策略与路由**

- policy version=4；非news_web直接VerifiedSource；`research:*`除earnings_event_calendar外也VerifiedSource；其余news_web为ModelReviewed。当前函数忽略RunPurpose参数，因此不是Paper才加强、Debug自动降级。旧DiscoveryOnly/VerifiedSource枚举和adapter路径仍存在，不代表当前news Need采用它。`crates/akzio-domain/src/workflow.rs`。
- discovery路由evidence.news_web，reviewer路由research.critic，各自缺route回退默认。router将holdings/index/terms发给official；recent news及earnings calendar发native。`crates/akzio-ingest/src/news.rs`。

**第一步 discovery**

- 一次`respond`，required native web，输出上限2000；prompt请求近期news最多三篇，但Rust NativeWebPolicy允许最多32个citation，review只取排序后前8个URL。三篇是文字指导，不是Rust硬上限。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/prompts.rs`；`crates/akzio-model/src/native_web.rs`；`crates/akzio-ingest/src/news.rs`。
- 必须有provider raw中的completed web_search_call和允许的search/open_page/find_in_page action；query/action.sources可缺，annotations等原生来源可参与引用提取；无真实hosted action的文字“我搜过”不能通过。`crates/akzio-model/src/native_web.rs`。
- DiscoveryOnly只证明URL存在于保留的provider envelope字节：citation.quote就是URL本身，不是文章摘录。status=provider_attributed_unverified、verified_source_count=0、fetch_count=0、exact_quote_count=0、citations_complete=false、completeness=250000。`crates/akzio-ingest/src/adapters.rs`。

**第二步独立 source review**

- reviewer不复用前次结论作为真值，而以URL集合+candidate_summary做独立required native-web请求，上限6000输出tokens。允许域从实际discovery provider_request回读，不另扩源。Prompt要求候选摘要和网页视为不可信数据。`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/prompts/source_verifier.md`。
- 返回SourceStatus四值supported/contradicted/unverifiable/irrelevant。每条事实有statement、可空published_at、event_date、date_basis；非supported不能贡献事实；supported须在review provider citations出现（只移除固定utm_source/medium/campaign/term/content做URL身份比较，不follow redirect，不合并业务URL）。`crates/akzio-ingest/src/news.rs`。
- 当前按来源/逐事实分区：重复/遗漏来源、身份歧义、未请求来源、无citation、无事实、窗口外事实进入validation_failures；合法事实仍可保留。因此不要把较早文档“遗漏/重复均拒绝”理解成任一问题必定整包丢弃。`crates/akzio-ingest/src/news.rs`。
- RecentNews的event_date必须在resource[start,end]且不晚于review完成UTC日期，published_at不得晚于now。事件日历不进入RecentNews窗口分支，所以未来计划可保留；具体计划是否已公布、是否属于该ETF/成分公司，主要由模型审阅和Prompt承担，Rust没有逐条证明公告时间/事实含义，也没有对该日历添加固定未来天数区间。`crates/akzio-ingest/src/news.rs`。
- `parse_review_envelope`容忍第一个完整JSON后的尾随bytes、顶层未知key；source/fact保留deny_unknown_fields；非supported source省略facts默认空。截断JSON仍解析失败。`crates/akzio-ingest/src/news.rs`。

**结果如何表达**

- accepted事实成为reviewed_facts及output_text；始终source_verified=false、verified_source_count=0，另计model_reviewed_source_count；有合法事实时news_evidence_status=model_reviewed，即使validation_failures非空；否则fetch_failed/malformed_json/facts_outside_window/facts_empty/source_unverified。`crates/akzio-ingest/src/news.rs`。
- citations_complete=是否至少有可用事实；completeness=“被选最多8个来源中支持来源数/选中来源数”，不是全新闻覆盖率、事实正确率。若所有事实都有published_at则包级取其最大值，否则None，再用实际review读取完成时间约束可用性；不猜时间。`crates/akzio-ingest/src/news.rs`。
- Raw追加完整review request/response/audit，Normalized移除provider_result/provider_request/discovery_output_text及大型review audit，只保留精简事实、失败记录和审阅来源摘要；human_review=not_performed、investment_inference=not_verified。`crates/akzio-ingest/src/news.rs`。
- 新闻策略身份不匹配会使daemon报InvalidInput；完全缺身份只warn并保持未验证，citations不完整同样warn而非返回Err。因此Gate层available意味着材料形成，不意味着有合格事实。`crates/akzio-daemon/src/evidence.rs`。

**第三种历史/显式模式 VerifiedSource**

- 仅requires_independent_fetch=true时，按canonical URL聚合并逐一Rust fetch。来源body≤2MiB、无redirect、允许text/JSON/xhtml；每个归属摘录须为16..4096 bytes且逐字落在body；每个来源全部摘录精确绑定才计verified_sources，所有来源完整才source_verified=true。Raw slice、hash、quote和citation再在materialization二次校验。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- 这证明“所引原文快照/字节闭包”，不证明报道真、投资方向正确或仓位安全；当前default新闻ModelReviewed不走这条Rust fetch路径。两模式的URL规范化不同：历史canonical_source_url只去fragment，不去UTM；review url_identity才去列明UTM。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/news.rs`。
- 数据侧交给研究的关键约束：当前news:未source_verified只可描述性ground，不得作为supporting_ref。只给消费接口定位，不在本报告展开研究验证逻辑：`crates/akzio-research/src/agent/errors_catalogue.rs`。

### 6.2 日线/市场快照/量化特征

- `capture_stock`先验资产symbol、active、tradable，读真实clock、近14日calendar，并从显式feed分别取snapshot/latest quote/latest trade。各组件按自己t/timestamp做cutoff过滤；缺时间也移除，但仅明确未来才计rejected。closed标closed_last_available_data，不伪称实时。`crates/akzio-ingest/src/market_capture.rs`。
- 研究bars窗口默认400日，calendar从start到cutoff美东日+30天；future close仅存forecast_session_closes元数据；已完成session必须close+20m≤cutoff。请求adjustment=all、sort=desc、limit=252，最多16页，每个body≤8MiB；重复bar/token、未完成session、非法OHLCV拒绝；最近完成session缺失或不足252根为Pending。`crates/akzio-ingest/src/session_bars.rs`；`crates/akzio-ingest/src/runtime.rs`。
- 最终只取最新252根、按日期升序；availability是所选session最晚close+20m。故“至少252根”是获取合格条件，最终研究payload不是无限长度一年历史。`crates/akzio-ingest/src/session_bars.rs`。
- 特征公式akzio.quant.daily.v1，含1/3/5/20/60/252日收益、20/60日年化已实现波动、ATR14、最大回撤、20日平均美元成交额、最新gap，保留adjustment/feed/sample/available_at；字段不足为None。**252日收益需要253根bar，所以固定252根payload的return_252d_ppm为空是源码规定，不应宣称已计算全年252期收益。** `crates/akzio-ingest/src/quant_features.rs`。
- raw_prices路径专供另类价格窗口/Outcome接口：不生成上述adjusted研究特征，还读取公司行动；不在此解释Outcome收益算法。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/session_bars.rs`。

### 6.3 FRED宏观与发布日历

- DFF/DFII10/VIXCLS窗口366自然日，vintage=S-1自然日，防同日revision穿越decision；请求realtime_start=realtime_end=vintage，响应两字段必须精确匹配。`crates/akzio-domain/src/workflow.rs`；`crates/akzio-ingest/src/direct.rs`。
- observations、vintage_dates、release_dates检查对应数组存在；此adapter并未对每条observation逐项证明经济发布时间、非空/数值完整或期待样本覆盖。空数组在shape门可通过；FRED availability由vintage日末确定，而非每项公布时刻。`crates/akzio-ingest/src/direct.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- release_calendar Need为[S,S+45]、前日vintage。实际/fred/releases/dates请求include_release_dates_with_no_data=true、limit1000、按release_date降序；窗口start/end不是专门发送的发布日期过滤参数，而在取得响应后retain。当前没有release-calendar分页循环或覆盖完整性证明。未来已公布日历是计划元数据，不是未来经济观测值。`crates/akzio-ingest/src/direct.rs`。

### 6.4 期权链：四种上限、时间独立、未知不能补成0

- Need声明到S+120日，资源解析允许≤180日；**实际capture把结束截到min(request.end,start+30日)**。先取得cutoff-valid标的latest_trade.p或完整dailyBar.c，strike只取其90%..110%。`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-ingest/src/runtime.rs`；`crates/akzio-ingest/src/market_capture.rs`。
- contracts API及snapshot chain各最多4页、page size128、contracts最多512；按完整OCC symbol关联；contract必须active、underlying/expiry/strike满足过滤，重复contract/token报错。快照只接纳在contracts map里的symbol。feed在整次capture固定，不自动切OPRA。`crates/akzio-ingest/src/market_capture.rs`。
- 缺quote才补latest option quotes，每批100、最多6次；已遭snapshot权限拒绝则不补，补采中401/403则停止。后续cutoff过滤会移除未来/无时间quote，**被过滤的quote不在此函数启动第二轮补采**。`crates/akzio-ingest/src/market_capture.rs`。
- OI由contracts的open_interest/open_interest_date按OCC合并，只有日期严格早于cutoff美东日且值存在才接受；不把chain默认值当OI。IV/Greeks数值可以保留，但iv_timestamp、greeks_timestamp显式null，status=provider_does_not_supply_timestamp_unknown，不能借quote/retrieval时间。`crates/akzio-ingest/src/market_capture.rs`。
- stale_contracts诊断阈值900秒（缺quote也计stale）；这是绝对年龄统计，**不同于Need max_age=300秒，也不自动拒绝整条期权证据**。coverage包含requested/returned/trade/bidask/iv/greeks/OI、缺quote数、未来剔除、权限、错误、分页完整性及supplement次数；collection_status可permission_denied/provider_error/no_market_data/bounded_partial/available。只要返回AcquiredEvidence，quality仍为default(1M,true,true)，故不能将quality默认满值误说为期权覆盖满分。`crates/akzio-ingest/src/market_capture.rs`。
- Raw保存请求URL、开始/完成时间、响应/错误；Normalized保留精简requests与coverage/bounds。若函数早期`?`直接Err（如asset/contracts失败），内存ledger并不会自动形成Raw Artifact，不能保证“所有失败HTTP完整原文均已入Store”。`crates/akzio-ingest/src/market_capture.rs`。

### 6.5 官方ETF持仓/指数/每日杠杆条款

- QQQ=Invesco JSON、TQQQ=ProShares全基金CSV中对应基金、SOXX=iShares CSV、SOXL=Direxion CSV；完整source snapshot保留。effective_as_of不得晚于requested_as_of或cutoff日期。`crates/akzio-ingest/src/official.rs`。
- QQQ/TQQQ绑定Nasdaq-100，SOXX/SOXL静态registry为NYSE Semiconductor Index；TQQQ/SOXL属于DailyResetLeveragedEtf、daily_leverage_multiplier=3。静态profile是要求/身份注册，不是当前外部条款的已采事实。`crates/akzio-domain/src/instrument_evidence.rs`。
- 当前官方index获取实现：QQQ同时读details+page证明基金/指数字符串；TQQQ解析ProShares产品版本；SOXX从page找身份/指数/as-of。**SOXL index_metadata直接返回NotConfigured**（代码reason为browser challenge），不把holdings CSV冒充指数说明。`crates/akzio-ingest/src/official.rs`。
- TQQQ leverage_terms校验TQQQ/daily investment results/3x/Nasdaq-100等字符串，输出daily_reset、multiple、benchmark、effective日期和terms_text_sha256；**没有结构化费用/损耗率估计，也没有完整条款正文进入这个Normalized输出**。SOXL terms直接NotConfigured，reason为未有issuer page/PDF text parser；本次没有实测网站挑战/PDF状况。`crates/akzio-ingest/src/official.rs`。
- 官方source_document标official_direct、verified_source、policy身份、source_closure=complete、required/verified count=1、raw hash/revision；不走模型新闻quote逐字绑定。provenance.published_at取HTTP Last-Modified/Date（部分产品版本parser另处理），若无则effective日期UTC午夜；它不是一律发行人精确发布时间。`crates/akzio-ingest/src/official.rs`。

### 6.6 执行数据侧接口（不扩展到ExecutionGate裁决）

`refresh_execution_snapshots`重找六个scheduler Need，先并发account/positions/open_orders/fills，再并发quotes/clock，生成execution.snapshot.account/quotes/clock，返回`ExecutionSnapshotRefresh { account, quotes, clock, quote_error }`；账户/clock缺失报错，quotes可以通过quote_error保留失败信息供下游fail closed。旧研究快照不充当本次refresh成功。`crates/akzio-daemon/src/evidence.rs`。

clock/quotes adapter根据真实clock+calendar判断TradingSession，Overnight额外检查四资产资格；SIP映射boats，基础IEX配置映射overnight，夜间不退回iex；其余时段继续显式股票feed。这里的GET成功不代表ExecutionVerdict、订单提交或成交。`crates/akzio-ingest/src/paper_session.rs`。

## 7. Context候选、精确授权、必需闭包和预算

### 7.1 从候选到Manifest

- `assemble`只接受调用方显式candidates，再按Contract允许召回Lesson/Experience；不是全Store自动供模型检索。Synthesizer可从Critique合法源Manifest闭合被审Claim，但须同Run、源Attempt/Contract和实际被源Manifest选择。`crates/akzio-context/src/context_broker/manifest.rs`。
- 拒绝Raw、未允许kind/source、错误内部producer；同Run或受控跨Run overlay/canary资格另验。精确内部配对包括：akzio.ingest+SemanticDetail仅collection_status/canary.evidence_snapshot/evidence.option_projection/research.supplement.result；akzio.agent按Claim/Critique/DecisionProposal/ProposalReview对应固定producer；执行和学习类型各有固定配对。**不是akzio.*通配；但外部source fallback自身只检查NormalizedEvidence/SemanticDetail/RegimeSnapshot kind，不能说该函数对所有外部producer也做相同枚举校验。** `crates/akzio-context/src/context_broker/policy.rs`。
- 外部NormalizedEvidence和SemanticDetail统一UntrustedEvidence；先扫描instruction-like strings，再按FinancialContentPolicy隔离高风险金融内容，quarantined只留refs/reason/indicators，不进入readable。金融内容评估中ModelReviewed只扫描reviewed_facts，official_direct扫描结构化normalized，其余合并raw+normalized；这是启发式治理，不是语义正确证明。`crates/akzio-context/src/selection.rs`；`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-ingest/src/financial_content.rs`。

### 7.2 必需闭包不是“选几份够用即可”

- Critic/Synthesizer/ProposalReviewer：候选中的Claim/Critique本体必需；Claim.source_refs为grounds，Critique.source_refs含target、grounds和核验引用。引用必须存在于同一候选集合且kind匹配；当前reviewed版本把二者引用都加入required。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/research.rs`。
- reviewed版还把DecisionProposal、ProposalReview、research.supplement.result列必需；proposal claims/critiques、numeric_basis.inputs、allocation.evidence_refs必须全部在候选中并入闭包；ProposalReviewer须恰好一份最终提案。此处只说明数据输入，非审阅业务判定。`crates/akzio-context/src/context_broker/manifest.rs`。
- Outcome另有Decision/DecisionContext/ExecutionContext/OutcomeSchedule、当前stage packet或sealed Outcome；DecisionContext引用的claims/critiques以及已有Retrospective列必需。`crates/akzio-context/src/context_broker/manifest.rs`。
- `required_role_inputs`在候选排序前构集合，预算循环必需超限报MissingRequiredInput；mint前用实际selected集合再验闭包。Critic还有Claim+NormalizedEvidence最小结构要求。**Analyst没有这类强制四资产/四新闻/宏观全齐的required集合**，其balanced bundle是尽量覆盖，不是强制完全覆盖。`crates/akzio-context/src/context_broker/manifest.rs`。

### 7.3 24件、字节与token精确预算

| 默认活动Contract | 顶层件数 | 模型投影max_bytes | 选中source总max_source_bytes | Context估算token |
|---|---:|---:|---:|---|
|Analyst / Critic / ProposalReviewer|24|131072（128KiB）|524288（512KiB）|角色definition.budget.max_input_tokens|
|Synthesizer|24|196608（192KiB）|786432（768KiB）|角色definition.budget.max_input_tokens|
|Outcome|24|131072（128KiB）|524288（512KiB）|32768（32*1024）|

依据：`crates/akzio-research/src/agent/errors_catalogue.rs`。`ContextPolicy.max_source_bytes=None`时回退max_bytes，历史Manifest.projected_bytes=None时也用total_bytes；因此不能把新表强套旧冻结Contract。`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/selection.rs`。

每件projection序列化后按ceil(bytes/4)、至少1token估算，source字节和projection字节分别累计；达到件数/source_bytes/projection_bytes/tokens任一上限跳过可选项、required报错。此估算不是provider tokenizer的精确计数；外层实际完整模型请求及累计输入预算由研究Runtime负责，未在本报告复述。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/lib.rs`。

**可选背景优先顺序**：Analyst和Synthesizer的balanced selector先collection status，再按资产分组bars/news，再DFF/DFII10/VIXCLS，再earnings_event_calendar，最后option_chain；组内按confidence降序、source bytes、ArtifactId稳定排序。它只优先级，不绕过预算；随后required再次置顶。其他角色不调用该balanced selector，而按purpose_rank/直接引用等排序。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/selection.rs`。

### 7.4 Manifest与ReadGrant不是一回事

- Manifest payload固化ordered selections、quarantined、total_bytes/projected_bytes/estimated_tokens、contract_hash、input_hash；input_hash只哈希有序ID/kind，不含reason或预算metadata。CAS Artifact仍绑定完整payload、producer、origin和source_refs。`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/manifest.rs`。
- 先持久化Manifest，再mint内存ReadGrant：精确manifest_id/run/task/attempt/lease/epoch/contract/readable/raw_source_closure/expiry；TTL不能绕过lease或当前Attempt权限。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/context.rs`。
- 每次读重载persisted Manifest，检查Artifact与payload、Contract、Run/Task/Attempt、producer、kind、source_refs、预算和input_hash；readable必须正好等于selected IDs。普通读取拒绝Raw与AgentTurn/ToolCall/ToolResult。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/grants.rs`。
- `read_raw`/`read_raw_document`是库中存在的独立接口，不等于模型有Raw权限；只有policy.allow_raw_reread时沿source_refs构raw closure，且只能显式读取；当前活动Contract该标志全false、读工具表不含read_raw。`crates/akzio-context/src/context_broker/grants.rs`；`crates/akzio-context/src/context_broker/policy.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
- `read_authority_document`只读当前Contract声明的精确governance/role/output schema/tool schema BlobRef，不接受任意文件或Blob。`crates/akzio-context/src/context_broker/grants.rs`。

### 7.5 父子与恢复（数据侧）

- child Context只可来自同Run当前成功父Attempt的已选文档或其真正输出，且source lineage闭合到父Manifest；child policy/预算再收缩，raw closure须为parent子集；不额外Lesson召回。`crates/akzio-context/src/context_broker/selection.rs`。
- success proof恢复父Manifest仅供历史验证；synthetic grant expires_at=now，不能以普通live读取复活父权限；真正child再mint当前身份。`crates/akzio-context/src/context_broker/grants.rs`。
- option projection复用只沿同Run同Task的Retry/Recovery祖先，要求source_ref、Blob、Contract、producer、RunScoped、origin完全匹配；相同内容可复用Artifact ID，但外层必须签发新Attempt Manifest/ReadGrant。改变source、Contract或projection内容不复用。`crates/akzio-context/src/context_broker/materialization.rs`。
- 实际消费者以ContextQueryScope::for_node生成scope，有parent走proof，其他走assemble；随后materialize+model_context，没有从这条接口直接绕过Context读全部Store。只核查此调用口，不展开Runtime状态机。`crates/akzio-research/src/agent/runtime_run.rs`。

## 8. Projection、遗漏与只读工具

### 8.1 模型到底得到什么

- materialize按Manifest逐项授权读取。每项先进入metadata ledger（id/kind/source/observed_at/tokens/reason/grant identity），只有must_read_class识别的对象才附内联value。当前research version>=65声明read_tools=[]，task_contract可见授权种类、来源及各预算。`crates/akzio-context/src/context_broker/materialization.rs`。
- research外层ledger projection_version=3；NormalizedEvidence投影version=1；option version=3；collection-status投影version=4；Outcome外层version=2。不要把这些不同对象的projection_version混成全系统一个版本。`crates/akzio-context/src/context_broker/materialization.rs`。
- 普通NormalizedEvidence投影只保留source/resource/time_basis/quality/quant_features/value_summary及full_document提示；bars/observations只保留最后5行并保留original_count；移除session_closes，但calendar source/forecast_session_closes等其他summary字段仍在。**Need/Raw/provenance/financial_content等不在该通用顶层投影中**，完整CAS不改写。`crates/akzio-context/src/context_broker/materialization.rs`。
- holdings识别rows或Invesco holdings数组，保留全部列，用发行人权重列优先排序，取前12行，保留精确数值、original/retained/omitted_count/ranked_by；找不到权重列保持源序。排序优先weight/holdingspercent/percentageoftotalnetassets，再exposure/notional，不擅自把名义金额当权重。`crates/akzio-context/src/context_broker/materialization.rs`。
- 所有Normalized投影以及Claim/Critique/Decision/Retrospective内的长字符串（函数递归，**不只字段名为narrative者**）超过600字符裁成前600+明确truncated提示；所以长statement、date_basis等也可能裁剪。数字不改。`crates/akzio-context/src/context_broker/materialization.rs`。
- collection-status按status分组仅暴露resource/criticality/diagnostic，省略Need Artifact refs，明确“Collection status不是ReadGrant；available但未选中不是不可用”。`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-context/src/context_broker/guidance.rs`。

### 8.2 期权投影的两层含义

- 聚合遍历完整已采snapshots计算IV min/max/mean、contracts/quote/OI/expiry计数；保留coverage/feed/bounds/decision_cutoff/time_basis/quality/provenance，缺失字段列missing_items。expiry最多列32、available_fields最多64；仅前两个OCC排序样例，显式sample_contracts_omitted及“非代表分布”说明。**聚合完整只指已采部分，不代表全市场/全120天完整**。`crates/akzio-context/src/context_broker/materialization.rs`。
- assembler条件满足时把每个option NormalizedEvidence都转SemanticDetail，代码**没有先判断它是否“过大”**，与函数注释“when too large”不同。新Artifact.source_refs=[原Normalized]，payload.source_artifact记录原id/kind/blob_hash/logical_bytes/producer/source_refs。Context预算算选中的投影Artifact自身字节，而不是连原链一起算；原链仍在CAS，但只有source_ref不是授予其readable。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。
- 对无读工具research，projection_only_guidance把**顶层**full_document替换为“本轮仅提供投影，原文未开放”。它不递归：holdings嵌套full_document和通用truncated marker仍可能说read original，应由上层实际read_tools=[]解释，不能把提示文字当权限。`crates/akzio-context/src/context_broker/materialization.rs`。

### 8.3 五个工具实际限制（当前仅Outcome有工具授权）

| 工具 | 读取范围/限制 | 源码 |
|---|---|---|
|read_document|精确selected文档；序列化响应value>32KiB返回DocumentRequiresRange，不静默截断|`crates/akzio-context/src/context_broker/reads.rs`|
|read_range|精确blob半开byte区间、单段≤32KiB、必须UTF-8边界；返回total_bytes|`crates/akzio-context/src/context_broker/reads.rs`|
|search_context|query≤256字符、1..16结果、仅当前selections，大小写不敏感子串；offset基于document_value_text，不是直接所有CAS bytes|`crates/akzio-context/src/context_broker/reads.rs`|
|read_claim_evidence|先读Claim，再去重展开每个ground，每个ref必须已授权且kind一致；不自动扩grant|`crates/akzio-context/src/context_broker/reads.rs`|
|compare_sources|2..4个不同已授权Artifact，返回source/time和compact projection，不判定谁正确|`crates/akzio-context/src/context_broker/reads.rs`|

补充`document_range_metadata`只在已授权后给可证的完整JSON顶层value范围，不能代替grant。32KiB硬检查在read_document_result和read_range中，不应宣称所有五工具组合响应各自都由本文件硬卡32KiB；read_claim_evidence在此未做合并响应32KiB检查，仍需外层累计输入预算。`crates/akzio-context/src/context_broker/reads.rs`。

## 9. Lesson查询与实际消费边界

1. Scope由已核验node生成：四资产全集、recipe完整ID；Synthesizer/ProposalReviewer全部T1/T3/T5，其余取execution_spec.horizon；未知recipe空scope。空query维度不是任意匹配，只匹配该维度未限定的Lesson。`crates/akzio-domain/src/context_scope.rs`。
2. `learning_query_scope`先清空caller regimes，仅从当前候选中通过kind/source/policy/Run/type校验的DecisionTime RegimeSnapshot重建，不取正文tag，不取ExPost。当前默认Synthesizer permitted_kinds并不含RegimeSnapshot，所以**默认路径下不能假定有regime标签**。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
3. `learning_candidates`只在Contract允许Lesson时扫描`active_lessons_snapshot()`全部Active heads；按scope、usage、governance、source、overlay筛选，再相关性regime/stage/asset/horizon、updated_at、lesson_id排序；去相同规范化内容/范围指纹但保留显式冲突。`crates/akzio-context/src/context_broker/manifest.rs`。
4. max4是**四条Lesson**，不是四组无限大小冲突组；冲突关系在eligible集合构无向连通组，整组放不下则不选。超过4条的大冲突组无法进入本轮候选。`crates/akzio-context/src/context_broker/manifest.rs`。后续assemble还会逐Artifact应用总体预算且Lesson非required，因此“冲突组最终必定完整送模型”不能只由这一召回算法证明。
5. governance要求无quarantine、valid_from≤now、有效期未过、使用次数未到上限、contradiction/post-use-failure为0、regime兼容；有compatibility但scope.regimes为空则不允许。`crates/akzio-domain/src/lesson.rs`。
6. 召回审计保存`learning.retrieval.audit`，含最终query scope及每项scope_mismatch/governance_ineligible/eligible/exact_duplicate/rank_or_conflict_capacity等记录。此处selected表示召回候选；后续总Context预算仍可能不选，需再看Manifest/context.coverage。`crates/akzio-context/src/context_broker/manifest.rs`。
7. 发现Lesson旧来源对应同source/resource的新Normalized、更晚于last_revalidated且blob不同，只生成RunScoped `learning.revalidation.suggestion`，lifecycle_changed=false，不认定矛盾、更不自动激活/失效Lesson。`crates/akzio-context/src/context_broker/coverage.rs`。
8. Experience另扫描最近100项，最多4条，须Canonical+canonical learning Run、learning_eligible、记录的influence subject一致、policy head状态允许。没有把隔离Debug/非合格Outcome经验自动送进正式上下文。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/policy.rs`。
9. **静态消费缺口：** 当前普通assemble给Lesson selection.reason=lesson，must_read_class没有Lesson/Experience/CandidatePolicy分支，默认只进入ledger metadata；metadata本身没有Lesson statement/recommended_behavior。research无工具，通用model_context也未读取额外正文。因此应表述为“具备受治理Lesson候选召回、授权与审计”，不能无保留说“默认Synthesizer已利用Lesson正文”。若主助手发现其他专门注入路径，应以额外源码/运行request证据修正此条；本报告看到的默认路径见 `crates/akzio-context/src/selection.rs`、`crates/akzio-context/src/context_broker/materialization.rs`、`crates/akzio-research/src/agent/runtime_run.rs`。

## 10. 缺口、过期、截断、恢复、补采数据接口

### 10.1 当前有界补采（仅数据侧，不展开角色重跑/修订）

- 公共门面`EvidenceAcquisition::prepare_supplemental / supplemental / note_abandoned`委托daemon；旧prepare接口收ResearchIntent并生成EvidenceNeed，当前shared research节点另走类型化SupplementalIntent→冻结Need绑定，不应混成模型自由写资源。`crates/akzio-daemon/src/application/evidence_acquisition.rs`；`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 当前SupplementalIntent只含kind(news/price/macro)、assets、series、query；唯一资产/序列、非空query、Macro序列仅DFF/DFII10/VIXCLS/DGS2/DGS10。query用于目的，不透传任意URL/日期；News/Price命中冻结同类Need，Macro可用冻结series模板替换受支持序列、保留窗口，重新走adapter同一个GovernedResource parser。`crates/akzio-domain/src/research_review.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 全run单轮最多8个distinct resources；四资产news展开4条。每项started先持久化再Need及I/O；恢复已done复用处置，started无done标unknown_after_crash并不重发，预算不重置。此条只定位数据侧审计接口，不接管协调逻辑。`crates/akzio-daemon/src/application/research_supplement.rs`。
- 当前shared节点取原Normalized的最早decision_cutoff，调用acquire_supplemental_evidence时传该原cutoff；底层recipe=research.supplement时materialize也用原cutoff，而旧其他调用者用新Utc::now。**不能笼统写“每次补采扩大cutoff到抓取完成时刻”。** `crates/akzio-daemon/src/application/research_supplement.rs`；`crates/akzio-daemon/src/evidence.rs`。
- 新事实条件=quality.citations_complete、news:还须source_verified=true、available_at≤原cutoff、同resource的事实内容不同；对比忽略retrieval/provider包装变化，bars比较bars/feed/adjustment，FRED比较observations/units，news比较reviewed_facts/verified。`crates/akzio-daemon/src/application/research_supplement.rs`。
- **重要边界推导：** 当前news默认ModelReviewed始终source_verified=false，因此即使新闻二次审阅发现新描述性事实，也不能通过该补采“新增合格事实”的news准入条件；可能记录no_new_facts，或因无发布时间导致晚于原cutoff而采集失败。不是自动切换VerifiedSource，也不是可用文字等于允许重跑。`crates/akzio-domain/src/workflow.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 旧validate_supplemental_need仅bars、五个宏观序列、六种news topic，SEC补采明确不启用；它与当前typed binding都不是运行时Agent联网工具。底层补采逐项写Raw/Normalized，未提供跨所有resource原子事务；shared节点调用一项一处置。`crates/akzio-daemon/src/evidence.rs`。

### 10.2 用户可理解的故障层级

| 情况 | 数据侧动作 | 不能推出的结论 |
|---|---|---|
|adapter未配置/权限/限流/暂未发布|逐Need unavailable+分类诊断，保留其他成功项|不是所有研究必定失败，也不是缺口已补齐|
|future-data污染|整Gate Err，状态与可保存成功项仍留诊断|不能说可选数据污染可以忽略|
|provenance/citation/policy/Store错误|保留原错误，fail closed|不是普通“新闻没搜到”|
|新闻审阅无事实|可保存news_evidence_status失败、零coverage；Gate可能available|available不等于facts_ready/source_verified|
|日线不足252/最新完成session缺失|Pending，不拿旧不完整历史冒充最新完整payload|下载成功不等于价格可用于研究|
|期权权限/分页/缺字段|coverage/bounds/unknown/truncated明确保留，可能仍有Normalized|quality默认1M不代表完整链|
|Context可选项超预算|不选并记录exclusion reason|未选中不等于未采到、不存在或内容被证伪|
|Context必需闭包放不下|MissingRequiredInput；不返回完整可用Context|不能靠只保留摘要引用绕过闭包|
|旧ReadGrant/lease过期|read校验拒绝；新Attempt须新grant|CAS对象存在不代表当前有权限|
|补采started后崩溃无结果|unknown_after_crash，不重发|不能猜provider没收到请求|

对应实现：`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/session_bars.rs`；`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/grants.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。

## 11. 文档 / 注释 / 代码差异与写总说明时的注意点

| # | 表述或容易误读处 | 当前实现结论与证据 |
|---:|---|---|
|1|契约6节写“14天新闻VerifiedSource”|与当前policy4/news router不符，当前为ModelReviewed。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-domain/src/workflow.rs`。news-evidence较新说明与代码一致：`docs/news-evidence.md`。|
|2|“新鲜度看内容时间，不看下载”|内容cutoff确有独立检查，但通用stale还是observed_at。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。|
|3|普通Context128KiB / Synth192KiB及Outcome原文128KiB|这些是projection max_bytes；活动构造max_source_bytes一律4倍，Outcome也是512KiB。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-research/src/agent/errors_catalogue.rs`。|
|4|40项全部前置采集|六项只deferred，PositionPlan没有这六Need；研究底层仍可能读clock/quote。`docs/agent-runtime-contract.md`较明确；代码`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-ingest/src/market_capture.rs`。|
|5|120天期权需求等于120天覆盖|registry+120、adapter+30且最多4页512合约；文档当前规则19行已记30天，但需总说明同时提请求/实际差异。`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-ingest/src/market_capture.rs`。|
|6|有杠杆条款Need即实现完整损耗/费用事实|SOXL terms/index NotConfigured；TQQQ只校验/输出有限daily_reset/3x/benchmark/hash等，无损耗率或费用结构。`crates/akzio-ingest/src/official.rs`。|
|7|news文档“遗漏/重复/窗口外均拒绝”可读为全包拒绝|当前逐事实分区，合法和失败共存，usable优先model_reviewed。`docs/news-evidence.md`；`crates/akzio-ingest/src/news.rs`。|
|8|“只在期权原文过大时做SemanticDetail”注释|assembler条件无大小判断，凡符合kind/source许可的option Normalized都转换。`crates/akzio-context/src/context_broker/materialization.rs`对比`crates/akzio-context/src/context_broker/manifest.rs`。|
|9|召回Lesson即模型收到正文 / 冲突组总能完整保留|默认Lesson仅metadata，无research读取工具；冲突分组是候选召回层，最终预算逐项再过滤。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。|
|10|原始网页从不进入研究Context|活动ModelReviewed移除provider transcript且research只投影；但库中历史DiscoveryOnly/VerifiedSource可保留provider_result/provider_request于Normalized，不能把新路径描述无条件套旧对象。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/news.rs`。|
|11|投影提示总能指导read original|当前研究无工具；只有顶层full_document被替换，嵌套和truncated提示仍有读原文用语。`crates/akzio-context/src/context_broker/materialization.rs`。|
|12|所有Context组装路径在mint前都走同样required算法|普通assemble确实先required/mint重验；assemble_child直接预算/建Manifest，无同样required_role_inputs调用，后续materialize中的validate_manifest_closure才检查当前selected闭包。应分别描述，不把普通路径排序保证套给child。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。|

另外契约21行仍有“其他purpose保持原协议”，126行已明确Paper/PositionPlan/Shadow统一单次结构化；当前活动Contract没有研究读工具，Outcome单独有。属于同文档滚动更新留下的范围说明不一致，由角色worker详细处理，本报告不展开。`docs/agent-runtime-contract.md`；`crates/akzio-research/src/agent/errors_catalogue.rs`。

## 12. 给主助手优先引用的18个源码入口

1. `crates/akzio-domain/src/workflow.rs` — 固定基础Need与E/D分类。
2. `crates/akzio-domain/src/instrument_evidence.rs` — 40项扩展清单/日期/去重。
3. `crates/akzio-daemon/src/evidence.rs` — 精确政策核对、deferred、逐项状态、部分成功/污染。
4. `crates/akzio-daemon/src/application/evidence_acquisition.rs` — Gate完成边界、Shadow父证据接口。
5. `crates/akzio-ingest/src/materialization/materialize_raw.rs` — Need/request/source授权。
6. `crates/akzio-ingest/src/materialization/materialize_normalized.rs` — Raw/Normalized血缘、time basis、stale实际公式。
7. `crates/akzio-ingest/src/news.rs` — discovery/review、事实分区、source_verified=false、Raw审计。
8. `crates/akzio-ingest/src/adapters.rs` — discovery与独立snapshot差异、source白名单。
9. `crates/akzio-ingest/src/session_bars.rs` — 交易calendar、252根、close+20m。
10. `crates/akzio-ingest/src/market_capture.rs` — 期权界限/权限/截断/时间未知。
11. `crates/akzio-ingest/src/direct.rs` — FRED vintage/发布calendar及校验边界。
12. `crates/akzio-ingest/src/official.rs` — 官方持仓、SOXL缺口、杠杆条款和来源身份。
13. `crates/akzio-research/src/agent/errors_catalogue.rs` — 当前24件/128–192KiB/4倍来源预算、无research读取工具。
14. `crates/akzio-context/src/context_broker/manifest.rs` — required闭包、精确校验、预算拒绝。
15. `crates/akzio-domain/src/context.rs` 与 `crates/akzio-context/src/context_broker/grants.rs` — 当前Attempt精确ReadGrant。
16. `crates/akzio-context/src/context_broker/materialization.rs` — 内联/metadata区别、投影/期权恢复。
17. `crates/akzio-context/src/context_broker/manifest.rs` 与 `crates/akzio-domain/src/context_scope.rs` — Lesson scope/全Active召回/四条冲突限制。
18. `crates/akzio-daemon/src/application/research_supplement.rs` — 补采绑定、原cutoff、started恢复、合格新事实。

## 13. 未核验边界与交付建议

- **未运行验证：** 无build/test/fixture；因此不证明当前dirty工作树可编译、单测通过或以上分支在某个Run中发生过。测试源码里的assert均未当作验收。
- **未联网：** 不确认当前provider可用性、账号IEX/SIP/OPRA资格、网站是否仍browser challenge、实际获取条数、新闻review质量、FRED样本完整度或实际报价新鲜度。
- **未读Store/配置：** 不确认某个Run冻结的是哪版Contract/policy、是否有active Lessons、实际Manifest选中多少件、哪个source被quarantine、哪个请求恢复后unknown。文中默认Contract参数来自构造代码，不代表历史Run或当前部署身份。
- **不覆盖其他worker职责：** 不判定Scheduler触发、Store事务/CAS权限实现、研究角色修订流程、Decision目标、ExecutionVerdict、Paper提交、Paper成交或Outcome完成。仅把接口边界交给主助手衔接。
- **静态推导须保留限定语：** Lesson仅metadata、候选冲突组可能被最终预算裁开、当前ModelReviewed新闻不能满足补采source_verified新事实门、期权无大小条件替换、stale基于retrieval，均由已列默认代码路径推导，未做针对性运行复现。
- 主说明建议固定使用“已采集 / 有可用事实 / 已source-verified / 已选入Context / 已给模型内联内容 / 已工具读取 / 已获方向使用资格”这些分开的状态，不能互相替代。本报告各节给出这些边界的源码实现。

## 14. Memory使用记录（只定位，不作为当前事实）

只查询了注册表以定位本仓库架构和证据边界，全部正文事实重新从当前文件核查；没有据旧memory宣称当前运行结果。

- `/Users/alixeu/.codex/memories/MEMORY.md`：仓库定位、源码核查与运行验证需区分、基础关键词。
- 对应历史rollout id：`01a0e619-cd21-7130-8b8a-f56a1adb918c`。未打开rollout文件，不引用其内容。


---

<!-- 分报告 2: 02-research-model.md -->

# 02 — 当前工作树研究 Agent 与模型协议：只读源码核查

日期：2026-09-29（Asia/Singapore）。源码定位基线 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`；以**当前工作树内容**而不是纯 HEAD 内容为准。

## 0. 结论、核查范围与证据等级

**当前研究主干是 Rust 固定编译的三期限 Analyst/Critic → 一轮共享补采与受影响重跑 → N+1 组 Synthesizer/ProposalReviewer，不是 Planner 自由扩图，也不是所有角色通用 Draft→Submit。当前研究 Contract=69、PromptBundle=38、freshness candidate=70；Outcome 保持 63/35。四个研究角色均 Submit-only。**【源码：`crates/akzio-research/src/agent/catalogue.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`】

- 已先读根 AGENTS、运行时契约第 3/5/6/11/12 节（并补读相关权威边界）、prompt ownership、budget configuration、retirement。遵守只读边界；仅新建本报告，不改源码、配置、Store，不执行测试、模型、HTTP API、capability probe、采集或 Broker。开始时工作树已有 AGENTS、README、CI、daemon、store/debug_bundle、若干 docs 和 run_core 脚本等改动，均保留；研究/model/domain/application 目标范围当时无 Git diff。
- 本文证据等级是 **source-inspected / implemented-in-source**；没有本轮 `offline-verified`、`real-Paper-verified` 或 `outcome/learning-verified` 证据。读取测试只能证明存在相应断言，不能宣称本轮测试通过。
- 未打开任何 SQLite/Store；“冻结”的下文有两种含义，必须区分：**源码构造出的 canonical Contract/新 Run 默认快照规则**已核；**某个实际 Run 的持久化 Contract、模型请求、usage、输出和调用时间**未核。不能由本文给出某 Run 真正使用的模型或成功率。
- 调度/Runtime/Store 主事务、Evidence acquisition/source verifier 的完整事实校验、DecisionPolicy/ExecutionGate、Outcome 数值和学习逻辑交给主助手/其他 workers；本文只追到必要交界点。规则出处：`AGENTS.md`。

## 1. 可用于中文全流程说明的流程图

```text
Rust 编译并冻结 NodeSpec / Contract / resolved TaskBudget
        │
EvidenceGate 已完成的授权证据候选（本报告不展开采集）
        ├─ Analyst(t1, round0) ─→ Critic(t1, round0) ─┐
        ├─ Analyst(t3, round0) ─→ Critic(t3, round0) ─┼─→ research.supplement
        └─ Analyst(t5, round0) ─→ Critic(t5, round0) ─┘       │
                   三条路径之间可并行；同一对内有先后        │
                                         全 Run 一轮 / ≤8 个去重资源
                                                         │
                仅新增合格事实影响的期限：Analyst(round1) → Critic(round1)
                其他 refined 节点显式 Skipped，旧 CAS 留存
                                                         │
                Rust 按 horizon 选择有效 Claim + 指向该 Claim 的 Critique
                                                         │
                Synthesizer(revision0) → ProposalReviewer(revision0)
                       │拒绝                            │全17项通过
                       └→ Synthesizer(revision1) → Review …    └→跳过剩余修订
                            至多 N 次业务修订；连续相同拒绝可提前停止
                                                         │
                精确 DecisionProposal + 精确 ProposalReview → Rust Decision 交界
```

**“三期限并行”不是六个模型同时启动。**首次三个 Analyst 无相互依赖，各 Critic 只依赖对应 Analyst；supplement 等全部首轮路径；每次 Synthesizer 等全部原/重跑路径及上一 Review。lowering 再添加 EvidenceGate 依赖。实际同时运行数量/排队时间需调度与运行证据，不能从 DAG 断言。【源码：`crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`】

`max_proposal_revisions=N` 表示**初稿之外**的修订次数，默认 2；`21+2N<=32` 限制 N 最大 5。研究部分为 6 个初轮任务+1 supplement+6 refined+2(N+1) 个终稿任务；PositionPlan 总节点 `17+2N`，Paper `21+2N`（默认 21/25）。默认图有 18 个潜在研究模型任务节点，不等于真的调用 18 次：未受影响 rerun、已通过后的修订、部分 Critic 会跳过；provider/schema retry 又可能增加单任务调用数。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；文档汇总：`docs/agent-runtime-contract.md`】

## 2. 冻结身份、实际模型路由、预算：三者不能混为一谈

### 2.1 当前常量与历史身份

| 项目 | 当前源码事实 | 来源文件 |
|---|---|---|
| 研究角色版本 | `ACTIVE_CONTRACT_VERSION=69`；`ACTIVE_PROMPT_BUNDLE_VERSION=38` | `crates/akzio-research/src/agent/catalogue.rs` |
| freshness candidate | 版本 70；克隆 active Analyst，附加 freshness 指导、重新算 hash、安装为 candidate，不自动替换 active。candidate 的 `prompt.version` 也设为 70，不能称候选仍是 bundle38 | `crates/akzio-research/src/agent/catalogue.rs` |
| Outcome | 构造分支显式 `prompt.version=35`、`contract.version=63`，保留读工具，Required deliberation | `crates/akzio-research/src/agent/errors_catalogue.rs` |
| 版本阈值 | structured eligibility 起点57；Submit-only 起点65；reviewed research起点67；typed review issues起点69；这些是兼容门槛，不是当前 active 版本 | `crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-domain/src/research_review.rs` |
| 安装/读取权威 | catalogue 检查退休和 canonical upgrade，从 Store active heads 恢复；按安装 hash 查询，不从 candidate/本地默认猜测 | `crates/akzio-research/src/agent/catalogue.rs` |

Outcome 文档还写 Contract hash `c9556a7ca9000cd06b96e385876013e3ce06a330fb4d01a473a43ef2db067ddf`（`docs/research-protocol-retirement.md`）。**本文只确认 63/35 构造分支，不把文档 hash 当作本轮重算或实际 Store 验证结果**；未执行会 stage CAS 的构造器或 hash 回归。

### 2.2 实际模型路由不是角色名硬编码

1. 生产模型 adapter 仅实现 `OpenAIResponses`；配置包括全局 model/reasoning/language 与 `routes`，route model/reasoning 覆盖全局，release_date/knowledge_cutoff 在 route 缺失时继承全局。字段可选不等于校准资格已足够；本文不推断模型真实发布日期/知识截止日。【源码：`crates/akzio-model/src/lib.rs`】
2. Daemon 从配置逐条构造 stage model，并要求匹配 capability snapshot；`AgentSession.run` 按 task recipe 调用 `model_for`，后者把 `research.proposal_reviewer` **强制映射到 `research.critic`**，其他 purpose 取同名 stage route，找不到就用全局 model。正式 Reviewer 不是独立选择某个模型名，也不读取独立 reviewer route。【源码：`crates/akzio-daemon/src/orchestration/bootstrap.rs`；`crates/akzio-daemon/src/application/agent_session.rs`；`crates/akzio-daemon/src/lib.rs`】
3. 研究请求 `reasoning_effort=None`，provider wire 回退到选中 route 的 reasoning；**只有 Outcome Submit 显式改为 low**，Outcome Draft 仍走配置。不要把 Outcome 的 formatting override 讲成当前研究角色的第二阶段降 reasoning。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-model/src/responses.rs`】
4. 文档表宣称 Analyst/Synth=Luna high、Critic=Sol high、Outcome=Luna medium，但仓库提交模板只有全局 `gpt-5.6-luna` / `low`，无逐角色 Sol route。故**可说明文档目标路由，不可声称当前代码强制该分工或当前运行必然如此**。实际本地配置、冻结 Runtime identity、AgentTurn requested_model/actual_model 需主助手另核。【文档：`docs/agent-runtime-contract.md`；模板：`config/akzio.toml`；`config/akzio.observatory.toml`；telemetry：`crates/akzio-research/src/agent/model_types.rs`】
5. 注意旁路差异：`debug verify-research-quality` 实验入口会先尝试显式 reviewer route，再回退 critic；这**不是**正式 Daemon 的 model_for 语义，不能用实验路由解释正式 Run。【源码：`crates/akzio-cli/src/cli/main.rs`；`crates/akzio-daemon/src/lib.rs`】

### 2.3 immutable Contract budget 与新 Run resolved TaskBudget

| 角色 | Contract 构造基线：input/output/read tools/wall | 新 Run 默认有效预算：input/output/read tools/Attempt wall |
|---|---|---|
| Analyst | 48,000 / 1,000,000 / 4 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| Critic | 48,000 / 1,000,000 / 4 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| Synthesizer | 48,000 / 1,000,000 / 2 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| ProposalReviewer | versioned budget 映射 Critic | resolved budget 映射 Critic（含覆盖） |
| OutcomeWorker | 12,000 / 4,000 / 2 / 180s | 1,000,000 / 4,000 / unlimited / 180s |

表中两列**有意不同，不应把 Contract 内 48k/120s 错报为新 Run 有效额度，也不能把新 Run 1M 写成 Contract hash 内预算**。`versioned_contract_budget` 负责前列，`default_agent_budget`/`AgentBudgetConfig.resolve` 负责后列；覆盖顺序为 role→default→代码默认。Reviewer resolved 时先映射到 critic，因此得到180s；不要直接拿 `default_agent_budget("research.proposal_reviewer")` 绕开正常 resolve 链来解释有效预算。【源码：`crates/akzio-domain/src/budget.rs`；Contract消费：`crates/akzio-research/src/agent/errors_catalogue.rs`】

- Rust 创建图时保存 resolved `agent_budgets`；node 要与图中的 budget 对应；AgentRuntime 再核对 permit、持久化 node、传入预算、retry、failure policy。修改配置不为既有 Task 充值。【源码：`crates/akzio-runtime/src/runtime.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`；`crates/akzio-runtime/src/runtime/compilation/validation.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- input/output 是 **Task 累计**，跨 provider retry、schema repair 和 Retry/Recovery lineage；wall 是当前 Attempt 的持久化起点。新的 proposal revision/refined 是**不同冻结任务、各自预算**，费用可累加到 Run，但不存在“整个 Run 只有1M”。【源码：`crates/akzio-research/src/agent/recovery.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；文档：`docs/agent-budget-configuration.md`】
- 研究单请求 output cap=`min(Task剩余, 1,000,000, provider声明上限)`，再受可选硬成本上限约束；不是目标生成长度，不保证 provider 支持1M。有限 read-tool limit 派生 `max_model_calls=retry.max_attempts*(limit+3)`；unlimited 取消此派生次数上限，**不**开放读工具，不取消 token/wall。`submit_result` 不按 read tool 次数收费。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】
- Context 独立限制仍为24项；Synth 192KiB、其他128KiB；研究 Contract 的 Context max_tokens 基线48k，不等于累计输入1M；max_source_bytes 为对应字节上限×4，Raw reread=false。实际模型仅得到投影，不因较大累计额度而拿到原文/SQL/联网权。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`】

配置值还受启动校验：input/timeout为非零u32，output为1..=1,000,000；read工具次数为0..=65535或`"unlimited"`，0只读工具不禁止terminal submit。未知字段/角色由serde deny_unknown_fields拒绝，各default/role覆盖逐项验证，不能用有效override遮住非法default。【源码：`crates/akzio-domain/src/budget.rs`】

## 3. Prompt 的真实消费者：正文不是直接随文件热加载

1. `prompts/mod.rs` 用 `include_str!` 编译嵌入共享治理及完整角色正文；安装 canonical Contract 时把两份正文和 envelope schema stage 到原 CAS，再计算 Contract hash。运行时从 **installed Contract 的 governance/role/schema blob**读，不直接读取本地 Markdown，不把 AGENTS/Skills作为运行时指令。【源码：`crates/akzio-research/src/agent/prompts/mod.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；边界说明：`docs/prompt-ownership.md`】
2. 当前请求由 `structured_request_prompt(governance,role,language,ledger,output_kind,budget,version)` 拼接；阶段协议是 Rust 常量；Analyst/ Critic/ Synth 分别取得单Claim/精确核验/12预测配置规则，Reviewer 则靠自己的完整 role 文本与 Review schema。NodeSpec 把 horizon/round/revision 渲染为 objective marker，不由模型自行决定期限。【源码：`crates/akzio-research/src/agent/prompts/phases.rs`；`crates/akzio-domain/src/workflow_definition.rs`】
3. 请求还追加 Analyst ground 去重、Critic supported/no-conflict 的短指导；授权 materialization、引用 ledger、动态 Schema 与角色正文共同组成实际输入。故“检查 role.md”不足以证明实际 Prompt；本文已追到 assembler 和 adapter。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/prompts/phases.rs`】
4. **Wire ArtifactRef 只有 artifact_id，不带 kind**：`bind_reference_schema` 从当前 Manifest 按原字段允许 kind 收窄 ID enum，并删除 kind property/required；返回后 Rust 从 immutable ledger 精确回填。不会修复错误ID、替换证据或偷偷去重；allocation refs只排序，重复保留给校验拒绝。【源码：`crates/akzio-research/src/agent/helpers.rs`】
5. Provider 层把 terminal tool 设为 `strict=true`、`RequiredFunction("submit_result")`。但是 `provider_schema` 会移除本地数值/长度/数组数量/unique等约束，并把所有 properties 写为 required；所以 **provider strict 并不是完整 Rust schema 已验证**。完整的 bound Schema 留在 Rust 返回路径再校验。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/schema.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
6. Prompt registry 记录静态ID、路径和内容；component hash 包括多个请求/校验源码和 registry，不能把“角色文字没改”推成 RuntimeIdentity 未变。【源码：`crates/akzio-research/src/prompt_registry.rs`；`crates/akzio-research/src/lib.rs`】

## 4. Analyst：每期限一个 Claim，而非每资产一个 Agent

**输入**：本 Run EvidenceGate 祖先产物中的 `NormalizedEvidence/SemanticDetail` 候选，经过 canonical evidence 检查、ContextPolicy/Manifest/ReadGrant 和投影。初轮不接收其他 Analyst 结论；refined 只在该 horizon受新增事实影响时运行，按 resource 以新证据替换旧候选，原CAS不改。角色 Contract 只准这两类输入，不准 Claim/Review/Lesson/RawEvidence。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_loop.rs`】

**输出**：一个 `ResearchClaim`，含 schema_version/topic/statement/horizon/stance/materiality_ppm/confidence_ppm/grounds/evidence_gaps；stance只有 bullish/bearish/neutral；单次只有一个horizon，且必须等于Rust NodeSpec；不是4份独立 stance。grounds 1–12、每个精确证据引用只出现一次；gap最多2。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】

**实际角色指导**要求使用给定投影的精确整数和单位、不凭心算误转ppm/百分比，不把未采用当未提供；区分“采集不可用、已采未选、投影省略、提供未采用”；官方 research:*材料不是近期新闻，未source-verified新闻只能作背景；模型不能自行联网补齐。【Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

### confidence 与 uncertainty 的精确关系

- Claim 自己的 `result.confidence_ppm` 与 `materiality_ppm` 都只在0..1M；没有在 `ResearchClaim.validate()` 中强制它与 wrapper 的 `deliberation.confidence_ppm` 相等。**不确定性守恒是针对 deliberation.confidence_ppm，不是自动针对 Claim.confidence_ppm。**【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-domain/src/contract.rs`】
- 所有角色 envelope 的 deliberation 有 selected_path、alternatives≤3、对应 alternative_match_ppm、uncertainties≤3、对应 uncertainty_weight_ppm、basis IDs≤8、confidence。当前研究 wire再要求至少1个 basis ID。两组分数数组长度须与文本一一对应；每个分数0..1M；uncertainty weights恰好合计 `1,000,000 - deliberation.confidence_ppm`。alternative_match 不要求合计1M；空 uncertainty array因此只有confidence=1M时守恒成立。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-domain/src/contract.rs`】
- Rust加 `assessment_source="model_assessed"`、用独立DeliberationNote保存；Artifact provenance confidence固定1M，**这只是Rust元数据，不是研究置信度100%**。自评不得提高后续Context选择优先级；deliberation不是隐藏思维链，也不能补正式 grounds。【源码：`crates/akzio-research/src/agent/runtime_helpers.rs`；`crates/akzio-research/src/agent/prompts/phases.rs`】

## 5. Critic：对本期限 Claim 做独立但受控的核验

**输入**：仅选择同horizon有效Claim；自身Contract还允许NormalizedEvidence、SemanticDetail、DeliberationNote。Context必须纳入Claim正式grounds闭包；当前69还必须纳入Critique引用闭包。其投影可以有Analyst未选中的额外证据；不能把“我的Context有”误判成“Analyst声称它在整个采集中不存在”。Prompt明确使用 producer_context_scope 标识区分。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/critic.md`】

**是否调用**：`should_run_structured_critique` 要求至少一Claim materiality≥500000、非neutral，或同topic/horizon多Claim方向冲突；当前单Claimhorizon路径中，中性且低materiality可跳过。69 bounded loop返回 **Skipped**；旧通用路径的“NoOutput”不应直接套到69。Claim仍保留，Synth依赖并引用它，未有资格的slot必须中性。【源码：`crates/akzio-runtime/src/runtime.rs`；`crates/akzio-runtime/src/runtime/compilation/helpers.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；旧路径对照：`crates/akzio-daemon/src/application/research_run.rs`】

**输出**：target Claim ref、topic、severity(low/medium/high)、blocker、rationale、grounds≤12、gaps≤2、verification_status、supporting_refs≤12、conflicting_refs≤12。实际JSON枚举小写 `supported/contradicted/not_enough_information`，Prompt大写是描述，不是wire值。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-domain/src/research.rs`】

关键硬检查：

- grounds与gaps不能同时为空；supporting/conflicting refs必须同时在Critique自己的grounds内，且不得重复。SUPPORTED必须有支持、无反证，每条支持都 `authority != unrated` 且 `temporal_validity=valid_at_decision_cutoff`；CONTRADICTED必须有反证。【源码：`crates/akzio-domain/src/research.rs`】
- 有blocking gap时blocker必须true。反过来blocker=true不必“一票否掉所有资产”：若其有scope blocking gaps则按slot命中；没有blocking gaps的blocker是Claim-wide。【源码：`crates/akzio-domain/src/research.rs`】
- Critic grounds不得超目标Claim grounds资产；gap scope可含目标Claim已声明gap资产（>=67），horizon不得越过目标Claim；SUPPORTED至少验证Claim的一条正式ground。Critic新证据/说明可以提供反证，但不能替Claim补缺失的price/macro formal grounds。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`】
- supporting_refs中的news额外读实际payload检查 `source_verified=true`。模型填写authority不构成来源验证；canonical schema也不能证明引用文字真的蕴含Claim。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；Prompt边界：`crates/akzio-research/src/agent/prompts/roles/critic.md`】

## 6. grounds、来源权限与 directional qualification

### 6.1 从授权到正式 ground 的检查链

- Context先拒RawEvidence、ExPost RegimeSnapshot，核对Contract允许kind/source family及精确内部producer；普通RunScoped材料还要同Run，只有既有canary父证据/学习overlay例外。**开发助手能读文件/工具可用，不等于运行时Agent有此能力。**【源码：`crates/akzio-context/src/context_broker/policy.rs`；治理正文：`crates/akzio-research/src/agent/prompts/shared.md`】
- wire Schema根据实际Manifest证据scope按相同scope聚合ID，构造anyOf：bars/news仅命名的单资产，受支持series宏观可覆盖四资产；未知scope或未verifiednews分支在当前contract只准descriptive、空assets、null domain。不能因关联ETF或同一指数而跨资产扩写。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- Rust返回路径再次验证同一个bound wire，然后正式ground逐项检查Manifest精确ID+kind、payload resource→domain/scope；Directional必须NormalizedEvidence、citations_complete=true、有domain/已知scope、非空且subset assets。news必须另有source_verified；`model_reviewed`或citation完整不能替代。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 官方research:*持仓/机制、corporate actions、calendar、options等不能自动当news方向事实。源代码仍有历史leveraged_terms→FundamentalsSemiconductor映射，但其asset scope为unknown，当前boundwire要求descriptive+null；不可借保留词汇复活方向能力。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/structured.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

### 6.2 单Claim资格，而不是把碎片拼成一个“支持”

`claim_slot_eligible(ref,claim,critiques,asset,horizon)`必须同时满足：

1. Claim合法、同horizon、非neutral、无命中asset×horizon的blocking gap；
2. **某一个**精确target该Claim的合法SUPPORTED Critique，没有该slot blocker；
3. **同一个Claim**正式grounds同时有price_market_structure和macro，均Directional且含该asset；
4. 该Critique自己的grounds逐个匹配这些ground的evidence、role、domain、asset，并有对应当前权威supporting_ref。

不能跨Claim拼price+macro，也不能拿Critic新增ground或deliberation代替Claim缺项。新闻不是当前最小方向资格的必需domain；news缺口是warning时不直接阻断，有实质blocking gap则照常阻断。【源码：`crates/akzio-domain/src/decision.rs`；gap规则：`crates/akzio-domain/src/research.rs`】

Context给Synth的`coverage_verification_matrix version3`直接调用同一资格函数，并列 eligible_claims及stance；提交校验也调用该函数链。**不是模型读矩阵后自行解释一套资格。** 非中性forecast还要求stance与expected_return符号一致；return=0时才用probability相对500000决定方向，允许均值与上涨概率因偏态分布符号不同。【源码：`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-domain/src/decision.rs`；Submit消费者：`crates/akzio-research/src/agent/errors_catalogue.rs`】

**资格的限度**：`is_current_authoritative()`检验的是typed authority/temporal标签；source/scope/citation/news_verified有Rust额外门，但这些函数不是自然语言蕴含证明器或通用数字重算器。完整上游timestamp/provider来源核验交给Evidence worker；不要把此处布尔通过说成“事实被独立数学证明”。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】

## 7. 一次正常 submit_result 的构建、校验、持久化

### 7.1 调用前

校验TaskWritePermit/task/Contract/hash/node与预算 → Context组装与materialize → 读冻结Prompt/schema → 收窄reference/ground/allocation wire → 选择Submit-only和terminal → 恢复原预算/continuation → 检查capabilities、input估算、output reservation和wall → **先写AgentTurnStarted，再poll模型Future**。【源码：`crates/akzio-research/src/agent/runtime_run.rs`】

能力检查要求 verified probe、stateless continuation，以及有工具时支持tool calls；即使首轮是Fresh/Submit-only，现实现仍要求stateless continuation，以支持受控repair。未知能力不能按“端点兼容”放行。【源码：`crates/akzio-research/src/agent/validation.rs`；未probe快照：`crates/akzio-model/src/model_client/client_setup.rs`】

### 7.2 Provider请求与返回

`ModelClientAdapter`的Fresh input包含objective、context_manifest、context；Continue用既有transcript+tool outputs+instruction。Responses发送`POST {base_url}/responses`，`store=false`、`stream=true`、`reasoning.summary=auto`、请求encrypted continuation；普通研究只暴露submit_result，没有native web工具，即使model crate有web能力也不会自动开给研究Agent。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/responses.rs`；研究工具面：`crates/akzio-research/src/agent/runtime_run.rs`】

SSE必须有状态匹配的completed/incomplete terminal；[DONE]或EOF不是业务完成证明。incomplete/refusal/空输出先失败；成功continuation保留实际历史input+全部response output，而不是依赖provider服务端会话。reasoning stream是provider摘要观察，不是正式DeliberationNote或成功提交。【源码：`crates/akzio-model/src/responses.rs`】

### 7.3 返回后的顺序不能倒置

1. 先持久化AgentTurn（包括原始terminal arguments），累计实际usage；超request cap、input/output超限或usage不一致则失败，不因得到JSON而修复/继续。
2. Submit不准assistant_text或其他tool calls；必须唯一terminal submission，多次submit也不择一接受。
3. 校验bound wire → 回填kind → Synth绑定Rust日历、Reviewer绑定精确身份 → 冻结canonical envelope schema → extract/validate deliberation → canonical result schema → Claim/Outcome horizon → 角色业务语义、引用闭包、directional与allocation资格。
4. 返回前写DeliberationNote，并stage正式result blob、构造`agent.<purpose>`、RunScoped output；source_refs含Manifest、AgentTurn/ToolResult/DeliberationNote trace与正式研究依据。**最终output Artifact此处返回给daemon；不是在这一函数里就发布成功Attempt输出。**主助手须在Runtime/Store整合时追TaskCompletion成功提交事务，不能把persist_stage当TaskSucceeded。

【源码：`crates/akzio-research/src/agent/runtime_run.rs`；adapter多submit拒绝：`crates/akzio-research/src/agent/model_types.rs`；返回TaskCompletion：`crates/akzio-daemon/src/application/research_loop.rs`】

## 8. Retry、schema repair、deliberation-only repair 与 usage fail-closed

### 8.1 四种“再来一次”必须分开

| 类型 | 边界/额度 | 证据 |
|---|---|---|
| Provider turn retry | Contract max_attempts=2、initial_backoff=250ms，transport/rate_limit/invalid_output flags=true；实际还必须满足usage与时间 | `crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs` |
| Rust InvalidOutput的schema/字段repair | 初提交之外至多一次；必须剩余至少当前phase deadline的50%；同Task同预算，feedback保留call_id | `crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/helpers.rs` |
| Scheduler Retry/Recovery | 新Attempt可能有新Attempt wall起点，但沿同Run/Task的Retry/Recovery历史恢复既花token/调用及失败状态；不是新Task预算 | `crates/akzio-research/src/agent/recovery.rs`；`crates/akzio-research/src/agent/runtime_type.rs` |
| Proposal业务revision | Reviewer拒绝后进入预冻结的另一个Synth/Reviewer节点，N为额外修订数，每节点独立Task budget；与格式repair不共用计数 | `crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-domain/src/research_review.rs` |

**retryable flag不保证真的发生第二次网络调用。** 当前generic provider失败没有闭合usage会`record_failed_turn`置`output_usage_unknown=true`；下一次`authorize_model_call`立即ProviderUsageUnknown（有硬cost还可能先CostUsageUnknown）。所以transport/timeout/429等即便被标为will_retry，也可能只记录意图后被账本阻断。`will_retry`不是已重试证据；需要下一条Started/实际request。ProviderIncomplete/ProviderUsageMissing不在普通自动retry允许列表。【源码：`crates/akzio-research/src/agent/helpers.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】

### 8.2 deliberation-only 是真正的结果冻结，不是“让模型重写一份差不多的”

- 判定目前是feedback.message包含字符串`deliberation`，不是完整typed路径分类器。Runtime从已持久化AgentTurn逆序恢复原始result与最近deliberation；使用仅含deliberation的terminal schema，清continuation/tool outputs，Fresh context只放旧deliberation、validation feedback、frozen_result_hash，外加immutable ledger/治理指导。模型看不到整份result，不能再次提交result。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- 修复响应若自己带result，即使内容相同，也被`bind_frozen_result`拒绝；缺result时由Rust把旧result补回，随后完整验证。记录前后result语义JSON hash/changed paths；仅deliberation错误却改result直接拒绝。普通non-deliberation repair仍可改变result，靠重跑全部校验；**没有通用Rust机制逐个锁住所有“前轮已合法字段”**，不能把Prompt“只改错误字段”夸大为全部字段的机器级锁定。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- **冻结不代表result已经业务通过。** 校验顺序中deliberation在角色语义/方向资格之前，第一次若只先暴露deliberation错误，后面的原result仍可能在修复后被拒绝。Rust不能通过修deliberation自动把未审result升级成合法产物。【源码：`crates/akzio-research/src/agent/runtime_run.rs`】

### 8.3 用量与恢复

- 真实OpenAIResponses缺input或output totals直接ProviderUsageMissing；只能fixture使用可见文本估算，防止遗漏隐藏reasoning。provider usage别名归一化，缺失保持None；reasoning属于output细分、cached属于input细分，不能大于total，也不再次叠加到token total。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/responses.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】
- 已发生usage先入累计账，再返回超限错误；输出reservation先放开再计实际，超单请求output也不“退款”。input缺失且output已知会留已知值/估算但调用仍失败，恢复仍保留Missing状态，不能推断合规。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`；`crates/akzio-research/src/agent/recovery.rs`】
- Started在I/O前持久化；恢复按attempt+start cursor闭合。Started无terminal、历史不可解析、hash/phase不匹配但曾有provider work，返回Unknown而非fresh budget；guard核对Contract、Manifest、read-grant/materialization identity、request hash、capability、budget-policy、工具集合和continuation。metadata-only有独立tool hash且必须对应deliberation反馈，不是任意工具变更例外。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/recovery.rs`】

### 8.4 审计事实与覆盖范围

AgentTurn总有domain_request/request、request_hash、Contract/Context、ReadGrant观察快照、resolved_budget/budget_usage、capability与tool/budget policy hashes；成功/失败分别持久化。实际provider request/raw只在debug启用时附model_debug，不能说每个Run都保存真实wire；telemetry另有provider_request_id/response_id/requested_model/actual_model/latency/tokens。【源码：`crates/akzio-research/src/agent/runtime_helpers.rs`；`crates/akzio-research/src/agent/model_types.rs`】

`PipelineLatency`记录provider / parse_validate / persist_stage，phase=structured/draft/submit，revision参数实际为model_turn。**此StageAcceptance只在存在DebugSession时写**；结构修复hash/diff审计同样debug_session条件。`SubmitRejected`则所有Run都写，供恢复精确重建反馈。审计`Pass`仅表“测量/记录成功”，其中succeeded可能false，不代表模型业务通过。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/structured.rs`】

## 9. 全Run一轮补采与受影响重跑

### 9.1 模型只提交类型化意图

当前69 wire替换旧`SupplementalNeed/ResearchIntent`：gap里是`supplemental_requests:[{kind,assets,series,query}]`，不接受resource/date/window字段。kind=news/price/macro，assets非空最多4且不重复，series最多5且不重复；macro支持DFF/DFII10/VIXCLS/DGS2/DGS10并要求非空series，news/price要求series空；query非空。每gap最多8请求、Claim/Critique最多2gap。【源码：`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-domain/src/research_review.rs`；`crates/akzio-domain/src/research.rs`】

retriable+blocks_directional_forecast必须带请求；空gap assets=所有资产、空horizons=继承Claim horizon。**retriable是分类，不是自动兑现一次采集**；warning或不可重试请求仍可形成skip disposition，只有实质blocking进入候选采集。Synthesizer没有此协调入口，requests不是模型的联网工具。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

提交阶段typed意图校验与采集阶段资源绑定是两道门：`ResearchClaim/Critique.validate`调用intent.validate；当前旧`validate_supplemental_resources`只遍历historical supplemental_needs，不是当前typed请求已被完整绑定。真正资源展开在Rust协调节点，随后同adapter的GovernedResource parser解析。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`】

### 9.2 Rust控制实际资源、时间与额度

- 图必须恰有一个supplement节点；冻结EvidenceNeed取自graph node inputs。news/price按asset前缀命中同Run冻结resource；四资产news展开4条，不算一条。macro可从冻结series模板换成受支持序列，保留后缀窗口，再parse与validate。**当前`expand_intent`没有把intent.query写入need.query，返回克隆冻结Need（macro只改resource）；query是意图说明，不是自由网络搜索原文指令。**【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- 原cutoff从祖先NormalizedEvidence的`time_basis.decision_clock.decision_cutoff`取最小可解析值，缺则拒绝；不使用重试时的现在替换。gap与request assets/horizon不得越界；请求稳定排序：horizon→非series优先→asset/series→kind，再以resource/requester/index打破平局。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- 本task历史读取started/disposition；distinct resource最多8。先检查done复用→started但未知完成则unknown_after_crash不重发→额度→**持久化started在EvidenceNeed创建和外部I/O之前**。原始CAS保留，恢复不重置额度。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- dispositions含skipped_impact、not_retriable、invalid_protocol、deduplicated、unknown_after_crash、budget_exhausted、collection_failed、no_new_facts、accepted；最终round汇总所有处置，即使没有新事实协调任务也可Succeeded，不能等同补采事实成功。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】

### 9.3 何时重跑

新引用必须citations_complete、news额外source_verified、available_at≤原cutoff，而且和同resource原证据的`fact_value`不同。news比较reviewed_facts+verified；bars比较bars/feed/adjustment；series比较observations/units，忽略retrieved/provider wrapper变更。因此“再抓一次换了retrieval时间”不算新增事实；这里是程序定义的新增合格事实判定，不是独立事实真值证明。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】

只有有新evidence的请求horizon写入affected_horizons（dedup复用同样传播受影响期限）；refined pair按此决定run/skip。之后每horizon选research_round最高Claim，Critique仅取target该有效Claim者；不会把旧Claim与新Critique拼配；不再创建第二轮supplement，refined请求即使仍存在也只保留缺口。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；固定唯一协调节点：`crates/akzio-runtime/src/runtime/workflow.rs`】

## 10. Synthesizer：12预测、四资产+现金、17份估计依据

**输入**：有效Claim/Critique及正式grounds闭包、受控Evidence/SemanticDetail、允许且合格的Lesson/Experience/CandidatePolicy、DeliberationNote，以及修订时上一ProposalReview及其绑定旧提案。Context必需集合优先于optional background，>=67包含Proposal/Review、numeric_basis inputs、allocation refs、补采结果；24项/字节/token上限放不下则MissingRequiredInput，不以删反证/弱化闭包“尽量继续”。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-context/src/context_broker/manifest.rs`】

**输出完整DecisionProposal（Rust类型alias DecisionDraft）**：summary、confidence_ppm、12 forecasts、research_allocation、claims/critiques/evidence、material_conflicts、hard_blockers、soft_warnings，另numeric_basis及可选学习应用/拒绝refs。12行必须完整4资产×T1/T3/T5，概率0..1M，expected_return为有符号整数ppm；thesis有exit_condition和≥1 invalidation_conditions。ForecastThesis文字不是自动下单规则。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-domain/src/decision.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

- 无资格slot必须 `probability=500000 && expected_return=0`。这里Rust拒绝非法非中性提交，**不是在Submit函数里偷偷把数字改为中性**；模型在正常输出/有界repair时应主动给出neutral。支持某Claim也不要求必须配非零仓位。【源码：`crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 四资产各唯一一行；整数非负weight≤1M，四行加cash严格1M。每行rationale；0行必须非空abstention_reason；非零行abstention_reason=null，supporting_horizons与evidence_refs非空，horizons/ref排序去重。非零多头至少有一个positive expected-return supporting horizon、同资产Bullish且eligible Claim并精确引用Claim/匹配Critique/方向grounds。Bearish不是买入机会，全部cash可合法。【源码：`crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- **17 numeric_basis**：`forecast.<asset>.<t1|t3|t5>`12项、`allocation.<asset>`4项、`allocation.cash`1项；exact set不得缺失/重复。每项inputs非空且精确选中，kind限定Claim/Critique/NormalizedEvidence/SemanticDetail；units/method/assumptions/uncertainty非空。Rust核对完整性与引用，不解析method自动重算收益、概率、风险折扣或重叠权重。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 所有已选Claim/Critique必须保留，包括neutral/blocked/rejected；grounds、supporting/conflicting证据必须在proposal.evidence及选中闭包中，不能因不投/中性删除血缘。numeric/allocation refs也进正式source_refs。当前wire还强制claims/critiques数组数量等于selected count且唯一。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- `thesis_valid_until/expected_holding_period_days`从wire移除；Rust读选中bars内forecast_session_closes，要求四资产都有日历且同日期close完全一致，选择**提交时刻之后**第1/3/5个共同close，填回expiry/holding期。模型若填timing被拒，日历是计划session元数据，不是未来价格。精确实现基准是`turn_now`，不能把Prompt“基准Session后”误说成程序直接拿固定cutoff按自然日加1/3/5。【源码：`crates/akzio-research/src/agent/structured.rs`；消费者：`crates/akzio-research/src/agent/runtime_run.rs`】

Prompt要求估计说明讲明观测、原单位、方法、假设、敏感性、资产重叠/现金理由，并明确这是模型原始估计，不能伪造校准/样本/公式。**此要求与Critic上游SUPPORTED不是“Critic核过最终12个数字”同一回事。**【Prompt：`crates/akzio-research/src/agent/prompts/roles/synthesizer.md`】

## 11. ProposalReviewer：精确提案审查、问题锁定与有界修订

### 11.1 输入/绑定/输出

- 每个Reviewer只选相同proposal_revision的DecisionProposal，沿它的正式source_refs展开Claim/Critique/Evidence闭包；Context必须恰一个完整proposal并保留必需basis。不是另一位Analyst，不设计替代组合，不做外网查证；路由/预算继承Critic。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 已检查真正的Context消费者：DecisionProposal/ProposalReview分别标为`final_proposal`/`proposal_review` must_read；`compact_governed_projection`对NormalizedEvidence压缩、对Claim/Critique等缩写叙事，但没有对DecisionProposal/ProposalReview走这些裁剪分支，因而完整提案/Review字段进入本次模型上下文。它们的证据输入仍是有界投影，不等于完整外部原文。【源码：`crates/akzio-context/src/context_broker/materialization.rs`】
- 模型wire不填proposal/proposal_hash/manifest/contract_hash；Rust取Manifest唯一proposal的实际blob.hash、当前Manifest和Reviewer Contract注入。Review绑定**精确ArtifactRef+内容hash**，同数值换正文也可能换hash，必须重审。【源码：`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-domain/src/research_review.rs`】
- 每份Review恰17 assessments，每scope accepted/rationale/evidence_refs/issues；每个拒绝项1–3 issues，通过项空issues。issue category枚举为support_missing/source_qualification/conflicting_support/temporal_mismatch/unit_error/estimate_basis_mismatch/allocation_inconsistency；field_path须等scope或scope.字段或numeric_basis.scope.字段，correction_criterion非空，refs限定已授权研究kind及Manifest。stable_id=`hash(scope,category,field_path)`，不含措辞。这里field_path是语义前缀检验，**不是验证每个字符串一定指向真实JSON字段**。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- `accepted()`仅当基本validate有效且所有assessment accepted；`authorizes()`再比较proposal ref/hash。**这个函数本身不再次证明Manifest/Contract来源，也不调用validate_for_contract(69)**；issues由提交边界验证，其他authority交由Store/Gate整合。不要把函数名authorizes翻成“允许交易”。【源码：`crates/akzio-domain/src/research_review.rs`；提交调用：`crates/akzio-research/src/agent/errors_catalogue.rs`】

Reviewer Prompt逐项查input/units/method/assumptions/uncertainty与数值是否相称、是否忽略反证、配置折扣/资产重叠/现金是否自洽；允许有边界的模型估计，不能以缺少经验校准迫使模型编造样本，尤其不能因中性先验弃权无校准就自动拒绝。优先查可证伪的ppm/百分比、方法与数值冲突、现金叙述、时间、虚构样本。**这仍是模型审查，不是Rust算术验证或预测正确性证明。**【Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

### 11.2 拒绝后如何修，哪些锁住

- 下一Synth显式收到最新Review和它引用的旧Proposal；提交先确认旧proposal实际blob hash与review匹配，且旧proposal也在当前Manifest，再调用`validate_proposal_revision`。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 锁定所有**已通过forecast的模型字段及其numeric_basis**，比较typed scope值，不依赖数组位置；排除Rust重绑的thesis_valid_until和holding_period。已通过allocation/cash不锁，因为权重守恒与新forecast修复可联动；配置对应basis也可调整。所谓“逐字保留”在机器上是语义JSON/typed value相等，而不是原JSON空白/键顺序字节相等。【源码：`crates/akzio-domain/src/research_review.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/synthesizer.md`】
- Reviewer每轮仍审完整17项，新proposal始终是新产物；通过则后面所有bounded research修订节点Skipped；到N额度仍未过不生成伪通过。终稿到Decision的最终选择与blocker由主助手追Store/Gate，不把本报告中的Review通过当Policy/Execution许可。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

### 11.3 相同拒绝提前停止的精确条件

连续两份Review都在69问题规则下有效且都拒绝；把每个拒绝scope的issue stable IDs+排序去重issue refs、assessment refs、该scope的proposal value+basis生成fingerprint。两次完全相同则stagnated；rationale/correction_criterion改写本身不算进展，accepted scopes不是比较对象。**注意锁定函数剔除了Rust timing，stagnation fingerprint使用完整scope_value没有剔除timing，两者相等语义不是完全同一套。**【源码：`crates/akzio-domain/src/research_review.rs`】

下一Synth运行前按revision排序两份Review和它们精确提案，命中则产出RunScoped SemanticDetail `research.revision.stop`，reason=unchanged_rejected_scopes、previous/current review refs、issue IDs、`decision_authorized=false`。后续研究节点见stop立即skip，不再调模型；保留拒绝Review，不把停滞当通过。【源码：`crates/akzio-daemon/src/application/research_loop.rs`】

## 12. 与 Outcome 的协议边界（不展开数值逻辑）

Outcome是单独角色63/35，输入Contract允许Decision/DecisionContext/ExecutionContext/ExecutionVerdict/Commitment/Receipt/Reconciliation/OutcomeSchedule/Outcome等，输出RetrospectiveDraft；仍Draft→受控读→Submit，只有它拿5个Context只读工具。Draft最多占原总output的一半、70%墙钟，必须有非空memo才转Submit；Submit低reasoning formatting但同累计预算。模型负责质性叙述，不输出权威收益/滑点/回撤/Policy；这些由另一流程的Rust计算。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；文档工具/叙述边界：`docs/agent-runtime-contract.md`】

当前研究首轮T1/T3/T5是“同时对不同期限预测”，不意味着等待T+1/T+3/T+5到期才运行Critic/Synth。历史Outcome时间轴与T0独立；本文没有检查任何实际Outcome成熟度、numeric metrics、calibration sample或学习晋升。【阶段Prompt：`crates/akzio-research/src/agent/prompts/phases.rs`；时间轴文档：`docs/agent-runtime-contract.md`】

## 13. 四类“正确”与授权的证据边界

| 层次 | 当前能硬验证/明确做什么 | 不能因此宣称什么 | 关键出处 |
|---|---|---|---|
| 结构合法 | schema字段/类型/枚举/数量、12slot唯一网格、17basis、引用存在且kind/scope符合Manifest、Review身份、deliberation守恒 | provider strict或JSON成功即业务成功 | `crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/validation.rs` |
| 事实/来源资格 | source/kind/producer/Run/Manifest受控；ground资产/域/citation；news source_verified；精确Claim+Critique price/macro资格 | 文本引用真的蕴含结论、未漏关键反证；模型authority标签即外部独立核验 | `crates/akzio-context/src/context_broker/policy.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-domain/src/decision.rs` |
| 数值正确/自洽 | PPM范围、weight+cash守恒、方向和非零long资格、deliberation算术由Rust；basis文本存在，Reviewer可查单位/数字/方法冲突 | Rust已从method重算出每个收益/概率/权重；Critic已审最终17项；Reviewer不会误收/误拒 | `crates/akzio-domain/src/decision.rs`；`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md` |
| 预测准确/概率校准 | 当前只是明确有边界的原始估计、未来独立Outcome/校准对象 | 两个模型一致、confidence高、SUPPORTED或Review通过就预测准确/概率校准 | `crates/akzio-research/src/agent/prompts/roles/synthesizer.md`；`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md` |
| 执行授权 | 本模块只提供研究Proposal与精确Review；后续Gate另管 | Review authorizes=Paper允许/订单已提交/已成交/校准已激活 | `crates/akzio-research/src/agent/prompts/shared.md`；`crates/akzio-domain/src/research_review.rs`；`AGENTS.md` |

## 14. 文档 / 注释 / 活动Prompt漂移清单

| 位置与原说法 | 当前代码对照与说明 |
|---|---|
| budget文档称当前67/37/candidate68 | 已是69/38/70；预算结构本身大体仍有效。`docs/agent-budget-configuration.md` 对照 `crates/akzio-research/src/agent/catalogue.rs` |
| retirement称研究65/36/candidate66，九节点fixture | 版本陈旧，固定研究图有supplement/refined/N+1 review pairs，默认PositionPlan21/Paper25。`docs/research-protocol-retirement.md` 对照 `crates/akzio-runtime/src/runtime/workflow.rs`、`docs/agent-runtime-contract.md` |
| runtime契约早段称仅新PositionPlan单Submit、其他purpose保留旧协议 | 后段126已修正为Paper/PositionPlan/Shadow全研究一致；实际运行按角色+version而非purpose选择。`docs/agent-runtime-contract.md` 对照 `crates/akzio-research/src/agent/runtime_run.rs` |
| role模型high/sol表 | 文档不是强制route，提交模板全局Luna low；有效冻结值需运行证据。`docs/agent-runtime-contract.md` 对照 `config/akzio.toml`、`crates/akzio-daemon/src/lib.rs` |
| 模板注释“research Attempt120s” | 当前resolved默认180s；不可用注释覆盖冻结代码。`config/akzio.toml` 对照 `crates/akzio-domain/src/budget.rs` |
| “不支持slot强制降级为neutral” | 若被理解为Rust自动改写会误导；当前validator拒绝非中性不合格值，模型再提交neutral。`docs/agent-runtime-contract.md` 对照 `crates/akzio-domain/src/decision.rs` |
| “阶段审计记录provider/parse/persist、修复hash”未标Debug条件 | PipelineLatency、SubmissionRevision汇总仅debug_session；SubmitRejected全Run。`docs/agent-runtime-contract.md` 对照 `crates/akzio-research/src/agent/runtime_run.rs`、`crates/akzio-research/src/agent/structured.rs` |
| role正文要求复制artifact_id及kind | 当前wire删kind，Rust事后回填；这不是只存在于历史文档的差异：role正文实际进入69请求。应读作保持精确身份，不能按字面输出kind。`crates/akzio-research/src/agent/prompts/roles/analyst.md`、`crates/akzio-research/src/agent/prompts/roles/synthesizer.md` 对照 `crates/akzio-research/src/agent/helpers.rs`。Outcome builder明确说wire无kind（`crates/akzio-research/src/agent/prompts/phases.rs`），当前structured builder只给ledger（同文件79-94），未同样消歧。 |
| Analyst正文还提supplemental needs.max_results、needs为空、news_web/fred/alpaca | 当前wire仅kind/assets/series/query，无max_results/source_family/window。原词汇可解释业务意图但不应作为实际输入字段。`crates/akzio-research/src/agent/prompts/roles/analyst.md` 对照 `crates/akzio-research/src/agent/proposal_review.rs` |
| Prompt预算措辞“Attempt累计” | 核心实际Task跨Retry/Recovery累计；避免以新Attempt重置input/output。`crates/akzio-research/src/agent/prompts/phases.rs` 对照 `crates/akzio-research/src/agent/recovery.rs`；权威预算文档 `docs/agent-budget-configuration.md` |
| 新版Claim/Review保持通过项“逐字” | 实现是语义scope比较，排除Rust timing，且只锁accepted forecasts；allocation/cash允许联动。`crates/akzio-research/src/agent/prompts/roles/synthesizer.md` 对照 `crates/akzio-domain/src/research_review.rs` |
| supplement文件导读称async join_all并发采集 | 当前外层for逐resource `.await acquire_supplemental_evidence`，不能凭导读断言8条并发；底层adapter是否批内并行应另核。`crates/akzio-daemon/src/application/research_supplement.rs` |
| Critic正文SUPPORTED或CONTRADICTED至少复制Claim ground | 当前精确overlap检查只针对SUPPORTED；CONTRADICTED要求非空反证且在自己grounds里、scope合法，不必同一个旧Claim evidence。不要把Prompt的较强要求写成已被硬校验。`crates/akzio-research/src/agent/prompts/roles/critic.md` 对照 `crates/akzio-research/src/agent/errors_catalogue.rs`、`crates/akzio-domain/src/research.rs` |

以上为只读差异记录，未修复任何文档/Prompt/代码；不据此改旧CAS、Contract、哈希或执行权限。

## 15. 已读测试证据（均未执行）与整合交接

- 当前structured prompt只出现一次、角色规则不串到别的角色：`crates/akzio-research/src/agent/prompts/phases.rs`。
- scope wire拒绝跨资产news/保持macro，Rust日历不允许模型覆盖，synthesis全provenance/零非零分支：`crates/akzio-research/src/agent/structured.rs`。
- delib修复重用精确result并拒模型replacement；差异路径含grounds/verdict/blocker/数值：`crates/akzio-research/src/agent/structured.rs`。
- usage未知阻断下一调用、部分usage已知保留且不能恢复未知output：`crates/akzio-research/src/agent/runtime_type.rs`。
- 恢复provider terminal必须闭合精确Attempt/start cursor、SubmitRejected反馈保留：`crates/akzio-research/src/agent/recovery.rs`。
- four-asset news展开冻结单资产resource、retrieval变化非新fact：`crates/akzio-daemon/src/application/research_supplement.rs`。

### 给主助手的整合要点

1. 调度图以`runtime/workflow.rs`为研究拓扑事实；实际并发/worker lease取主助手Runtime/Store证据，不从DAG推实际运行。
2. 正式研究入口按installed version>=67直接进入bounded_research（`crates/akzio-daemon/src/application/research_run.rs`），不要把下面旧通用候选/Analyst补采路径混进69主流程。
3. 最终模型产物是`TaskCompletion::Succeeded(vec![output])`，其正式attempt output发布要跟主助手Store事务证据接上；DeliberationNote/AgentTurn/SubmitRejected先保存不等于任务成功（`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/runtime_run.rs`）。
4. 需要实际Run断言时补查精确Proposal/Review/hash/Manifest/Contract、所选revision、Started/terminal/usage、真实provider wire或telemetry；本文没有打开Store，不能证明哪些修订/补采/重试真的发生。
5. 执行层必须分别说明研究提案、正式Decision目标、ExecutionVerdict、订单提交、成交、Outcome；本报告没有验证任何交易或校准结果。

**交付范围完成：只读源码核查与指定报告。未执行：测试/构建/fixture、模型/API/Broker、Store读取或写入、配置修改、Git提交。**


---

<!-- 分报告 3: 03-decision-calibration.md -->

# 03 — DecisionGate 与 DecisionPolicy SQL 校准全过程（当前工作树只读核查）

## 0. 核查口径与结论

- 日期：2026-09-29（会话本地时区 Asia/Singapore）。核查对象为 `/Users/alixeu/project/akzio-signal-intelligence` **当前工作树**，不是仅 HEAD。核查时 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`。
- 已先读根 AGENTS、运行时契约 3/7/8/11/12（并补读相关 1/2/4、研究协议）、开发 Workflow readiness/SQL 校准及相关 CONTEXT、Debug 控制。记忆仅作定位线索；以下业务结论均重新查当前源码。
- **唯一写入为本报告及其必要父目录**；没有修改源码、配置、Store、历史 CAS 或内存规则，没有读取实际配置/密钥/认证 token，没有打开实际 Store，没有模型、网络、Broker 请求。未启动 daemon、CLI 校准命令、测试或构建。读取到的测试代码不是本次通过的测试。
- 用户许可 worker；当前可用工具没有独立 subagent/worker 派发接口，因此由本核查直接完成，没有擅自创建新用户 chat，也没有接管其他 worker 的订单执行或 Outcome 数值学习任务。
- 初始与收尾期间既有改动包括 `.github/workflows/ci.yml`、`AGENTS.md`、`README.md`、daemon 的 `http.rs/http_launch.rs/lib.rs/orchestration/health_canary.rs`、Store `debug_bundle.rs`、四份 docs、`scripts/run_core.py` 和未跟踪 `scripts/test_run_core.py`。均未触碰；此处事实只对应当时读取的工作树，后续变化须重新核查。

**结果先行：**

1. `DecisionProposal` 的 Rust wire 类型仍是 `DecisionDraft` 别名。模型给 12 forecasts、四资产+现金的研究意图、引用、numeric_basis 等；Rust 独立保留 `research_plan.raw/validated`，再用 **forecasts + 冻结校准 + 风险模型** 另算 `DecisionContext.target` 与同值的 `Decision.targets`。不是“照抄 allocation 后下单”。[^D01][^D02][^D03]
2. 缺 active Policy 的 canonical PositionPlan 可以完成合法 Decision，研究配置可非零、targets 全零、无执行/Outcome；canonical Paper 冷启动另走无交易 approval 的研究/NoOrder 入口以积累未来真实标签。缺 Policy 的真实隔离 Debug PositionPlan 只允许手动研究、禁止 resume/step Decision。[^D04][^D05][^D06][^D07]
3. **拒绝提案、正式目标归零、Execution NoOrder 是三种不同结果。** provenance/12 slots/精确方向资格/Reviewer 失败会在 Decision commit 前返回错误；已通过研究检查但校准/信号不足才可能形成研究非零、targets 零。研究过程质量等 hard blocker 也可能与非零 targets 同存，由 ExecutionGate 阻断。[^D02][^D08][^D09]
4. SQL 链为 `readiness → set-risk-limits → collect → build → inspect → validate → activate`。readiness 的成熟度不是 candidate 存在性；build 不激活；validate 的结构合法不是 decision-capable；activate 才更新 installation/history/head。[^C01][^C02][^C03][^S01][^S02]
5. **发现实际边界/漂移：** canonical daemon 未显式重做 active Policy 与当前 Synthesizer Contract 的比较；Policy 启动后是内存快照而非逐 Decision 热加载；collect 的历史 model version hash 是从当前配置写入样本，不是逐 AgentTurn 比对；preflight 非严格 Store 只读；asset_eligibility 摘要仍要求 NewsEvent 而实际 slot 谓词只强制 price+macro。详见第 10 节。以上为静态源码结论，不是已经复现的生产事故。[^I01][^I02][^I03][^I04][^C04][^D10]

## 1. 角色与名词不混用

| 对象 | 输入/权威 | 输出及边界 |
|---|---|---|
| 研究 proposal / allocation | Synthesizer 已授权 projections；Rust 提交校验 | 模型意图，不是订单；`target_weight_ppm` 字段名也不赋予执行权 |
| `ResearchPlanReview` | raw allocation、forecasts、Claim/Critique、静态 gross cap | raw/validated、adjustments、研究状态、execution_status |
| `DecisionPolicy` | Rust execution crate 定义；SQL active Artifact 装载 | 概率校准、风险参数与目标计算限制，不是模型生成的批准 |
| `DecisionContext` / `Decision` | Rust DecisionRuntime | Context 的风险、provenance、blockers、trace；Decision 的 forecasts 和正式 targets |
| domain `PolicySubject/PolicyState/CandidatePolicy` | Memory / Contract / Topology 生命周期 | 属于学习/候选治理 namespace，**不是**上述资金配置 DecisionPolicy |
| domain `QualificationStageReceipt` | behavior bundle、qualification key、场景、双侧输出、评估者哈希 | Replay/Adversarial/ExecutionSimulation/Shadow/Canary 的不可变 receipt；不替代校准或 Paper approval |

`QualificationVerdict` 只有 Pass 是可晋级；receipt 的 identity hash 覆盖输入身份、metrics、verdict、source_refs、时间；本报告不展开这些阶段的执行和学习算法。`DecisionRuntime` 对应用的 Experience/CandidatePolicy 另查 Canonical Paper 来源及 subject active head，不能把经验 active 与 DecisionPolicy active 混为一个 head。[^D01][^D11][^D12][^Q01][^Q02]

## 2. 从研究提案到持久化 Decision

```text
成功研究提交 → RunScoped DecisionProposal（DecisionDraft wire）
  ├─ proposal / 唯一 Synthesizer Manifest / permit / Contract provenance
  ├─ draft.validate：12 slots、研究配置守恒、引用 kind 等
  ├─ Contract >= 67：numeric_basis 17项 + final ProposalReview 精确授权
  ├─ Claim/Critique 当前版本语义与方向资格（>=57 单 Claim 闭合规则）
  ├─ review_research_plan(raw) → raw + validated + adjustments + statuses
  ├─ critical Claim / conflicts / consensus / 过程质量 → hard/soft blockers
  ├─ DecisionPolicy(forecasts, effective_confidence, now)
  │     → target + PortfolioRiskAssessment + runtime/horizon trace
  └─ DecisionContext(target, policy_hash, validity, research_plan, blockers...)
        + Decision(targets=同一target, forecasts=原draft.forecasts, research_plan)
          → 单次 Store::commit_attempt，两 Artifact + TaskStatus::Succeeded
```

- daemon 先从 Decision task 的成功依赖链找 final review：accepted 则选择该 Review 绑定的 proposal；有拒绝 review 则直接失败，**不回退旧 proposal**；无 review 才取 terminal proposal，随后当前 Contract 的 DecisionGate 仍会要求 review。[^D13]
- `DecisionRuntime::new` 只校验并按值持有 Policy。`decide` 先校验 TaskWritePermit，再重读 Store 中 proposal 和闭包；不会在此拟合或更换 Policy。[^D02]
- `DecisionContext` 额外保留 applied/rejected learning refs、material conflicts、policy hash、portfolio risk、有效期、过程质量、每资产资格投影、数值分支 trace；`behavior_bundle_hash` 在此构造为 `None`。`Decision` 保存有效共识置信度和原 forecasts，没有用 calibration 覆写原 forecast 数字。[^D03]
- Context source_refs 为 proposal + 唯一 Manifest + selected；Decision sources 为 Context + proposal。Paper 按 purpose 生成 Canonical Decision artifacts，其余为 RunScoped；**这不能替代 Store 隔离资格**，隔离 Debug Paper 即使 payload lifecycle 为 Canonical，collect 仍拒绝整个 Store。[^D03][^C04]
- 原子发布在两个 payload 都 validate 后；任何前置 Err 都不发布半份 Decision。不可把“原 proposal 已存”说成“Decision 已生成”。[^D03]

## 3. Provenance、闭包、12 slots 与 ground 精确资格

### 3.1 Proposal 与 Manifest

Proposal 必须：RunScoped；producer=`agent.research.synthesizer`；source_family=`akzio.agent`；origin Run 等于 permit Run；task/attempt 存在；provenance producer_contract_hash 等于 origin Contract。它必须恰好引用一个 ContextManifest。[^D14]

Synthesizer Manifest 必须：RunScoped；producer=`context.research.synthesizer`；source_family=`akzio.context`；同 Run、同 proposal task/attempt/Contract；payload schema 与 Contract 相符，selections 非空，每项 reason 非空、estimated_tokens 非零，NormalizedEvidence/SemanticDetail 信任级为 UntrustedEvidence。[^D15]

递归闭包核查：selected/quarantined 无重复且互斥；manifest.source_refs **精确等于** selected ∪ quarantined ∪ ancestor manifests；input_hash 重算匹配；quarantine kind 与 indicators 合法；重算 total/projected bytes 和 estimated tokens；拒绝 selected RawEvidence/AgentTurn/ToolCall/ToolResult；祖先来源需同 Run、有完整 origin，并防环。这里不能说所有祖先必须和当前 Synthesizer 同一 Contract——每个祖先绑定自己的 Contract。[^D15]

Draft claims、critiques、evidence、allocation evidence_refs、applied/rejected learning refs、conflict refs 都需在 selected 集。selected Lesson/Experience 必须明确 applied 或 rejected。**numeric_basis.inputs 的 selected 检查在研究提交层；Decision 的 validate_draft_closure 未再次遍历 numeric_basis.inputs**。Decision 只重做 numeric basis 格式校验及 Review 匹配，不宜把“上游检查”说成这个函数逐项重算。[^D16][^R01]

### 3.2 完整网格及中性

- 恰好 `TQQQ/QQQ/SOXX/SOXL × T1/T3/T5 = 12` 个唯一 `(asset,horizon)`，缺失或重复均 Err。
- 概率范围 `0..=1_000_000`；中性精确定义为 `expected_return_ppm == 0 && probability == 500_000`，不是单独概率 0.5。
- 方向先按 expected_return 的符号；只有 return 为零才用 probability 相对 500000 的方向。故“均值为正但正收益概率低于一半”不自动结构非法。
- Decision 要求 thesis 存在；valid_until 为 `min(now + maximum_execution_delay_ms, 12 forecasts最早thesis_valid_until)`；evidence_cutoff 为 selected 中不晚于 now 的最大 observed_at，缺项/读取失败在该统计处 best-effort 过滤，空结果回退 now，不应误称逐条数据新鲜度证明。[^D17][^D18]

### 3.3 新协议的单 Claim 精确资格

源码最低方向 Contract=47；单 Claim 精确 slot Contract=57；终稿 review Contract=67；structured issues=69；当前 catalogue=69 / PromptBundle=38 / freshness candidate=70。历史合同按安装版本选 validator，不重写历史 CAS。[^D02][^D17][^R02][^V01]

`claim_slot_eligible(reference, claim, critiques, asset, horizon)` 精确要求：

1. Claim horizon 相同，Claim.validate 成功，stance 非 Neutral，无命中该 slot 的 blocking EvidenceGap。
2. **存在同一份** target=该 ClaimRef 的合法 Supported Critique，且不阻断该 slot。
3. 对 PriceMarketStructure 与 Macro **两域各自**：在**同一个 Claim**找到 Directional、覆盖该 asset 的 ground；在**上述同一个 Critique**找到同 evidence ArtifactRef、同 role、同 domain、包含该 asset 的 reviewed ground；且 supporting_refs 对同 evidence 有 current-authoritative 核验。
4. current-authoritative 的代码含义是 authority != Unrated 且 temporal_validity=ValidAtDecisionCutoff。因此 EstablishedSecondary 也能满足，不是仅 Primary/Official；这个枚举判断本身不是现场网络来源验证。
5. Forecast 非中性还要该 Claim 已列入 draft.claims，且 stance 匹配方向。不能用不同 Claim 拼 price/macro，不能用 Critic 新证据或 deliberation 补 Analyst 正式 grounds。[^D17][^R03]

Gap 的空 assets 表示所有资产，空 horizons 继承 Claim horizon。Critique blocker=false 不挡；blocker=true 且无 directional blocking gaps 表示 Claim-wide；有 scoped gaps 则只挡命中的资产/期限。Supported 还要求非空 supporting_refs、无 conflicting_refs，且所有 supporting refs 当前权威。[^R03]

**ground 真正资源范围的验证发生在上游提交：**从 Manifest 精确 ArtifactRef 读规范化 payload，验证 domain 与 resource、资产 subset、Directional 必须 NormalizedEvidence 且 citations_complete=true、scope/domain 已知；未 source_verified 的 news（>=63）只能 descriptive、assets=[]、domain=null，不能入 supporting_refs。Decision 的纯 domain 谓词不重新联网、不独立证明来源真实或预测正确；不要把它扩写成更强保证。[^R04]

### 3.4 allocation 的研究资格与 final Review

- 研究计划恰好四资产一行加显式 cash，权重总和严格 1,000,000。每行有 rationale；horizons、refs 排序去重。非零行需非空 supporting_horizons/evidence_refs 且 abstention_reason=None；零行需非空 abstention_reason。[^D01]
- 新结构化 allocation gate：每个非零行，至少一个所列 horizon 有同资产正 expected_return forecast、Bullish 且 `claim_slot_eligible` 的 Claim，且行引用关联到该 Claim、其该资产 ground，或其 Critique。只要一个非零行完全不合格，整个 Decision 前置 Err；不是先写“blocked research”来兜底全部非法输入。[^D17]
- 17 个 numeric basis scopes =12 forecasts+4 allocations+cash；每项 inputs 非空且 kind 受限，units/method/assumptions/uncertainty 非空。这是估计说明完整度，不是代数证明、事实正确率或实证校准。[^R02]
- `Store::final_proposal_review` 只取 Decision 依赖祖先的 succeeded task 正式输出；冻结 revision 排序取最后，重复 revision 为歧义 Err；校验 Review producer、origin Run/Contract、Task Contract、source_refs，proposal 哈希、同 Run、成功 Synthesizer 任务且其正式输出含 proposal，Review Manifest 与 Review 同 origin、其 input_hash 与 Contract 一致且选中该 proposal。**Review.manifest 是 Reviewer Manifest，不是 Synthesizer Manifest。**[^R05]
- Contract >=69：拒绝 assessment 要 1–3 issues，通过项 issues 为空，scope/field_path、稳定 ID 去重和证据 kind 受约束。`accepted` 要基础结构合法且17项全通过；`authorizes` 再要求精确 proposal ref+blob hash。失败对应 `ProposalReviewRequired` 或 Store/daemon error，不会产生“已正式决策的零目标”来代替。[^R02][^R05][^D02]

## 4. raw → validated research plan 与 blockers 的真实影响

`review_research_plan` clone raw，不改 proposal；逐非零行过滤 supporting_horizons：有同资产正收益 forecast，且本地 `research_slot_supported` 找到 Bullish Claim、price+macro grounds、Supported 未阻断 Critique及关联引用。剩余期限空则该行归零并记理由。再以 `policy.max_gross_weight` 等比例缩放超限研究配置，整数向下取整，cash 重算为 `1_000_000 - Σweights`。默认研究 gross cap=500000，所以 raw 80% 风险资产不等于 validated 80%。[^D19]

注意：前置新协议 validator 使用更严格的 `claim_slot_eligible`，本地 review helper 自身**没有**重做每个 ground 的 current-authoritative 精确匹配。它是后续整理，不是上游强规则的替代品。当前合法路径已先通过严格门，但不要声称每个辅助函数都独立证明同样强的性质。[^D17][^D19]

研究状态：

- validated 仍有非零行 → `qualified_recommendation`；
- raw 有非零、validated 全零 → `blocked_by_research`（包含 static cap 裁至全零的情况）；
- raw 已全现金 → `explicit_cash`。

execution_status 的**先后顺序**：先研究 blocked → `blocked/research_plan_blocked`；否则 PositionPlan → `not_applicable/position_plan_does_not_enter_execution`；否则 Policy 不 decision-capable → `blocked/decision_policy_not_ready`；否则 `pending_execution_gate/execution_gate_not_run`。因此“PositionPlan 一律 N/A”需排除研究 blocked 的分支；此字段也不是 ExecutionVerdict。[^D19]

**allocation 不约束正式 targets 的数值：**target 函数没有 allocation 参数，raw/validated 权重没有进入其 sizing 公式；包括显式现金，也没有一个 allocation-cash veto 分支。由代码可推得：若研究允许非中性 forecasts 却选择现金，当前 target 计算仍可能从 forecasts 得出非零目标；不能宣称正式目标必小于等于 validated allocation。这里是数据流结论，不是本次真实 Run 的观察。[^D03][^D08]

其他 blocker 与置信度：

- 从 draft.hard_blockers 开始；materiality>=500000 的 active Claim 要**恰一份**匹配 Supported Critique且 slot 不阻断，否则加入 UnverifiedClaim；active Claim 的 material conflict 加 MaterialConflict；校准 horizon 冲突加 HorizonConflict。[^D09][^D20]
- consensus 按 Claim task 分组、capability snapshot hash、evidence content_similarity_cluster（缺失退 Artifact ID）计数；最大 pairwise Jaccard overlap 门槛500000。多参与者非 independent 时 `effective_confidence=min(raw,min_confidence)`，**等于最低门槛仍通过 `< min` 的判断**，不是必然归零；相关性加 soft warning。其他部分重叠分支按 cluster/participant 比例缩放并夹在原置信度与最低门槛之间。[^D20][^Q03]
- 研究过程质量为 grounding/premise/temporal/logic 四项最小值；任一 None 则 floor=None。门槛默认900000，缺失或不足加入 UnverifiedClaim。执行的 mandate/portfolio/action 三项此时 None，不伪造分数；也不因这些 blocker 在 Decision 代码内重新把 target 改成零。[^D09][^D21]
- `DecisionContext::accepted()` 只判断 hard_blockers 和 material_conflicts 空；不能用 targets 非零、asset_eligibility.eligible 或 ResearchPlanStatus 替代 accepted。[^D22]

## 5. DecisionPolicy 的合法、capable、当次可用是三层

### 5.1 缺失与加载失败

`load_decision_policy_from_store` 只从 active head 读取，不挑“最新 candidate”。真的缺 head 返回合法 `DecisionPolicy::default()`，status=`store_active_head_missing`，input_hash/contract_hash/artifact_id 全 None。已有 active Artifact 但 envelope、hash、身份、Policy validate 失败，则返回 Err，**不 fallback default**。模型比较用有效 Synthesizer route。[^I01]

严格 decode 要完整 provenance-bearing `DecisionPolicyArtifact`，不能裸旧 Policy。validate 核查 schema=1、元数据非空、assets/horizons固定、sample_count/source_runs非空、provider/route/Contract存在，重算 policy output_hash 和 risk_model_hash。**单独 decode/validate 不重新读取原 dataset 重算 provenance.input_hash，也不是重做 collect。**[^C05]

### 5.2 实际默认值（不是推荐的 operator 风险限制）

| 字段 | 缺 Policy 默认值 |
|---|---:|
| min_confidence_ppm | 250000（25%） |
| max_gross_weight | 500000（50%） |
| horizon weights T1/T3/T5 | 333333 / 333333 / 333334 |
| maximum_execution_delay_ms | 300000（5分钟） |
| minimum_process_quality_ppm | 900000 |
| min_probability_edge_ppm | 50000（概率偏离中点5个百分点） |
| min_calibration_samples | **u32::MAX = 4294967295**，不是30 |
| max_brier_score_ppm | 250000 |
| active_forecast_calibration | None |
| forecast_calibrations / asset_calibrations | 空 |
| target_annualized_volatility_ppm | 150000 |
| max_portfolio_beta_ppm | 500000 |
| risk model version | audit-unapproved-v1 |
| covariance_sample_count / covariance | 0 / 空 |
| max_expected_shortfall_ppm / max_gap_loss_ppm | 0 / 0 |
| max_leveraged_holding_days | 1 |

默认值可通过结构 validate，因为空 asset_calibrations 对应未配置风险模型允许为空；但不 decision-capable，正常 target 资产查找均缺校准而归零。不能把 `min_calibration_samples` 默认理解为“已经有4294967295样本”，也不能把0尾部限制理解为批准的安全模型。[^D23][^D24]

### 5.3 validate 与 decision_capable

- Policy.validate：confidence/process/Brier/gross/horizon weights在ppm范围；execution delay>0；edge<=500000；min_samples>0；target vol在1..=1000000；beta在1..=3000000；三个 horizon 键齐全且总权重1000000；校准 `(scope,asset,horizon)` 唯一。
- Asset风险：vol/beta/ES/gap在1..=3000000；liquidity<=max_capital<=1000000；decay<=1000000，非TQQQ/SOXL的decay必须0。资产 sample_count 的足量性不是该子类型 validate 的结构条件，而在 capable/当次计算检查。
- 有资产风险时：risk model版本非空，samples>0且>=min_samples，持有日1..=5，ES/gap limits在1..=3000000；协方差全 pair 存在、对称、幅度不超过两资产vol乘积、对角>0。没有全矩阵正半定证明；实际 `wᵀΣw < 0` 再 Err。
- decision_capable：有active scope、4份资产风险、4行covariance、risk samples足量；各资产samples/Brier合格；**12面**匹配scope的forecast calibration总样本足量/Brier合格/bins非空。
- capable 本身不检查“当前预测所在 bin 样本足量”、frozen_at<=本次now或最终alpha/edge；也不直接检查当前Config/Contract。它需要配合结构校验及入口身份核验。[^D24][^D25]

关键限制：`target_with_risk_traced` 并没有 `if !decision_capable {全局归零}`。它逐资产逐参与forecast检查；正常 CLI activation 会拒绝 incapable candidate，但不能推广成任意结构合法的局部 Policy 在函数层都全局归零。[^D08][^I05]

## 6. 正式 targets 的计算与风险裁剪

令 `S=1_000_000`，所有下述除法为代码中的整数除法/向下量化（有符号项按Rust整数规则截断）。[^D08]

### 6.1 先筛资格

1. effective_confidence < min_confidence → 全零，risk=Default（数值None而非测得0），first_zeroing_branch=`decision.confidence.minimum`。
2. horizon_trace 先按**校准概率**分类：p>=500000+edge 为 Bullish；p+edge<=500000 为 Bearish；其余Neutral；原始中性永远Neutral；无校准为Uncalibrated。同资产任两horizon一多一空产生冲突，该资产排除，Context另加全局HorizonConflict blocker。
3. 缺资产risk calibration；样本<min；Brier>max → 排除资产。
4. 原始中性 forecast 不参加任何正向bin“复活”。TQQQ/SOXL只纳入 horizon_days<=max_leveraged_holding_days 的非中性预测，QQQ/SOXX不做此持有期限过滤。
5. 对每个参加的forecast：匹配active `(model_id,model_version_hash,regime)`+asset+horizon；frozen_at<=decision_at、forecast calibration总样本>=min、Brier<=max、**命中bin自身count>=min**。任一参加horizon缺校准，整个资产不使用其余部分信号。

注意 horizon_trace 的冲突扫描在上述杠杆持有期过滤**之前**且包含全部forecasts：一个随后被持有期过滤的T5仍可能引发冲突。`included_in_target` 在trace中初始仅指calibrated.is_some再扣冲突，不等于该sleeve最后真的带来非零仓位。[^D08][^D25][^D26]

### 6.2 信号与单资产资本上限

对参与期限，权重h取Policy.horizon_weights：

```text
E = [Σ ((calibrated_p - 500000) * h / S)] * S / Σh
A = [Σ (calibrated_expected_alpha * h / S)] * S / Σh
合格条件：看到了参与forecast，校准完整，E >= min_probability_edge，A > daily_reset_decay
signal = clamp(2*E + A - daily_reset_decay, 0, S)
vol_cap  = target_annualized_volatility * S / asset_vol
beta_cap = max_portfolio_beta * S / asset_beta
cap = min(vol_cap, beta_cap, max_capital_weight, liquidity_weight_cap)
w = cap * signal / S
```

少部分期限因中性/杠杆持有期被排除时，按**实际参与权重**重新归一；缺校准不是这种可忽略排除。`confidence` 达最低门槛后不再连续乘进仓位。当前raw expected_return主要用于研究方向/中性；正式A来自历史bin实现收益均值，并非模型raw预期收益。liquidity cap 是operator静态额度，不是该函数实测实时订单簿。[^D08][^C06]

### 6.3 组合层

先以max_gross等比例缩放。然后：

- variance = Σ(w_i*w_j*cov_ij / S²)，vol = integer_sqrt(variance)；非零资产缺cov或最终variance负 → Err。
- beta / ES / gap = Σ(w_i*各资产metric)/S；这是线性聚合风险指标，不是新做组合尾部情景模拟。
- 取vol、beta、ES、gap各超限项目 `limit*S/measured` 的**最小**比例统一缩放所有weights（不扩大）；重算最终assessment。
- 若初始eligible集合空 → `decision.eligible_set.empty`；若曾有eligible但静态额度/signal/整数缩放把最终组合压为零 → `decision.post_scale.zero`。保留各资产首个排除分支；runtime_trace主要记录排除/归零，不是每个成功计算的完整逐式日志。
- accepted且非零的DecisionContext必须有risk_model_hash及四项风险指标；缺项validate报错。未接受的非零Context可以保留为审计候选，但不能经Allocator放行。[^D08][^D27][^D22]

**不是严格金融保证：**风险模型的对称/幅度约束不等于PSD证明；组合裁剪后虽重算风险但没有在本函数内循环直到每项再次低于限值的单独断言。这里描述实现，而非宣称已经实证覆盖全部数值极端值。[^D24][^D27]

## 7. 何时研究非零但 targets 全零；为何不等于强制清仓

| 情况 | 研究计划 | Decision / targets |
|---|---|---|
| 合格proposal + canonical缺active Policy | validated可非零（受gross cap） | 默认Policy产生零目标，Decision可成功 |
| effective confidence不足 | 不按该门槛删除研究建议 | 全局零目标 |
| 所有资产缺风险/forecast/bin校准、样本不足或Brier差 | 研究证据仍可合格 | eligible集合空，零目标 |
| 各资产被校准方向冲突或参与期限限制排除 | raw/validated研究仍保留 | 资产零；若全部排除则组合零 |
| 校准edge不足，或alpha<=decay | 研究正收益估计仍保留 | 相应资产零；全部失败则组合零 |
| max_gross/cap/signal/risk缩放及整数取整到0 | raw保留；validated可能非零也可能被其独立gross cap裁零 | post_scale.zero |
| proposal/Manifest/12slots/精确ground/Reviewer不合法 | proposal可能已存在 | **Decision Err，不是已完成的零目标Decision** |
| hard blocker/过程质量缺失 | 研究可非零 | **不保证targets归零**；Context不accepted、Execution阻断 |
| 真实隔离Debug PositionPlan缺Policy | 手动研究可存在 | 控制层禁止Decision，不能报告targets=0已生成 |

依据为两条独立数据流与实际分支，不是本次Run统计。[^D02][^D08][^D09][^D19][^D06]

**targets=0是目标权重，不是卖出指令：**ExecutionGate先继承Decision blockers，再校验purpose、Paper approval/qualification、有效期、freeze、账户/quote/clock等，blockers为空才调用Allocator；其后仍有Gate检查。缺approval明确加UnqualifiedRuntime，跳过allocation。[^E01]

Allocator还要求DecisionContext.accepted、快照session一致、市场tradable；它计算 `delta = account.equity * target_weight/S - current_market_value`。当前空仓且零目标→没有delta→NoExecutableOrder；当前有多头且零目标→只有经过前述门后才**可能**生成sell意图；后续执行Gate、Commitment及Broker结果仍决定能否提交/成交。NoOrder意味着没授权新计划，不能解释为替用户清仓。这里停止在接口含义，不接管执行实现。[^E02]

## 8. canonical、隔离 Debug 与 Policy 状态矩阵

| 路径/状态 | 当前代码行为 | 不可推出 |
|---|---|---|
| canonical PositionPlan，无active head | 正式`POST /runs`准备34研究Need、正式研究图；Decision用启动时默认Policy；在Decision结束 | 不产生ExecutionVerdict/Outcome，不提供校准标签 |
| canonical Paper，无active head | scheduler当次SQL检查cold_start，binding=None（即使Store有旧approval）；正常研究/Decision后由原ExecutionGate挡单 | Paper purpose不是approval；等待真实标签≠已经成熟 |
| 已有active、当前model/version不匹配 | strict loader Err，serve在能力probe前停止 | 不是缺Policy fallback；不能伪造日期消除hash差异 |
| 已有active、仅当前Synthesizer Contract不匹配 | activate拒绝；readiness可报contract_mismatch；真实Debug ready preflight拒绝；**canonical serve/decide缺对应显式比较，见第10节** | 不能依据文档声称所有canonical入口必然拒绝 |
| active envelope/hash/数值非法 | loader/constructor返回错误；readiness捕获部分decode身份错误投影为invalid_store_policy_or_identity | 不是所有Store错误都被转为该JSON状态；active读取本身的Err可直接传播 |
| 结构合法但校准面不足 | 状态validated_but_no_asset_calibration或validated_but_insufficient_samples；正常activate拒绝 | validate=valid不意味着capable；也别泛化任意部分Policy都会全局归零 |
| 隔离真实Debug PositionPlan缺Policy | 允许prepare/手动研究；冻结identity为PositionPlan+非Fixture+缺policy artifact，阻止resume和直达gate.decision | 重启/等Outcome不会变canonical；不是正式PositionPlan的零目标完成语义 |
| 隔离fixture | 明确fixture例外可验证Decision流程 | 不是真实校准、模型或Paper成交验收 |

证据：正式入口/拓扑/冷启动[^D04][^D05][^D07]；loader和启动[^I01][^I02]；Debug冻结身份与控制[^D06]；状态投影[^C01][^C03]。

**启动快照特别提示：**serve加载Policy，Daemon bootstrap clone进DecisionRuntime，此后`decision_gate`调用该实例；未见逐次active head刷新。scheduler却每tick读取SQL active head判断是否cold_start。CLI activate成功不等于已有daemon内存Policy即时换新；需核对实际运行实例policy_hash，不能自动给已有进程授予“已切换策略”的结论。报告不启动/重启进程，也不建议绕过原RuntimeIdentity/审批。[^I02][^I03][^D13][^D05]

## 9. readiness → 风险限制 → collect → build → inspect → validate → activate

### 9.1 readiness：只读成熟度，不是候选生成

定义：`calibration readiness --store <既有Store> --min-samples 30`；30是readiness/collect CLI默认值而不是默认Policy样本门槛。代码拒绝min_samples=0。使用`Store::open_existing`：SQLite READ_ONLY、要求当前schema18；不创建主库或迁移，但有当前连接TEMP staging准备。不存在/旧版本Store会报错，不等同于“空样本”。[^C01][^C02][^S03]

扫描最近最多500份Decision（先限额后筛Paper），每Run去重；非Paper不进入行。每种risk/dataset/policy仅展示最近20个Artifact，因此列表为空/截断也不是全库不存在证明。隔离Store报告store_scope=isolated_debug、calibration_eligible=false、reason=isolated_debug_store、next_step=use_canonical_store，成熟数不计入。[^C01]

canonical Run沿Decision→Schedule/ExecutionContext→Outcome与bars检查：

- Decision必须Canonical且validate；Schedule如存在必须指向该Decision。
- sealed Outcome必须Canonical、validate_sealed、绑定相同Schedule、有可用四资产Alpaca bars及完整12标签，才记sealed。
- **分类优先sealed-label结果**，再baseline missing，再no_schedule，再pending。
- 有ExecutionContext但account_snapshot或quote_snapshot缺失→baseline_snapshot_missing；如果ExecutionContext本身完全缺失，这个is_some_and布尔不是true，应按后续分类解释，不能泛化成所有缺基线必同一reason。
- pending仅列尚未持久化的T1/T3/T5 windows与baseline_trading_day，不以自然日推断成熟；自然日过去不补写历史基线。
- gap = min_samples - sealed Run数（饱和到0），12 slots各写同一成熟Run数；够数只提示collect。该阶段**不按当前模型/Contract过滤每一个成熟Run**，不检验完整价格训练面和operator limits，因此会被collect进一步筛掉。[^C07]

若有active head，readiness读取配置身份和**已存**active Synthesizer Contract；绝不调用可能安装catalogue的helper。active缺失时直接status=store_active_head_missing，不说明有待激活candidate。[^C01]

### 9.2 operator 显式风险限制

`set-risk-limits` 必填14个参数，均无CLI默认：min-confidence-ppm、max-gross-weight-ppm、maximum-execution-delay-ms、minimum-process-quality-ppm、min-probability-edge-ppm、max-brier-score-ppm、target-annualized-volatility-ppm、max-portfolio-beta-ppm、max-expected-shortfall-ppm、max-gap-loss-ppm、max-capital-weight-ppm、liquidity-weight-cap-ppm、max-leveraged-holding-days、daily-reset-decay-ppm。**本报告不提供替operator选择的数值。**[^C02]

限制：capital与liquidity均>0，liquidity<=capital<=1000000；ES/gap limit在1..=3000000；holding1..=5；decay<=1000000；其余复用Policy.validate。落为Canonical CalibrationRiskLimits CAS，无origin/source_refs。Store拒绝隔离写入。可inspect后再collect；这只是保存限制，不批准交易。[^C08][^S01]

### 9.3 collect：正式Store中的成熟标签与身份

1. 读取完整config，要求有效Synthesizer route有release_date、knowledge_cutoff（可继承共享）；模型ID/版本hash按当前配置计算。**load_config也解析credential配置/环境占位符，所以本次没有运行collect或调用它来“只看状态”。**日期真实性由operator提供方资料负责，代码不网络核实。[^C04][^I01][^I06]
2. 只读打开源Store，拒绝debug_environment；risk-limits ID必须是本Store的Canonical CalibrationRiskLimits，重新validate；训练边界可选RFC3339，start<=end。
3. 最近500 Decisions筛Paper+Canonical、Decision/Context可解码validate、Canonical Outcome存在且sealed、Schedule精确绑定Decision。
4. 沿同Run且created_at<=Decision时刻的DAG找最近 research.synthesizer AgentTurn，最多256 visited；provider来自capability_snapshot，model优先response.telemetry.actual_model、无则capability.model_id；Contract优先turn顶层、无则request。对齐当前provider与model；缺失/mismatch跳过。
5. 候选按RunId排序，取**首个候选的Contract hash**为训练组，其他Contract全部skip。它不是“自动挑当前active Contract”或“挑最多样本组”。模型变更后旧组可能collect/build成功、最终activate因Contract不符被拒。
6. 仅从Outcome.market_evidence中NormalizedEvidence、payload.source=Alpaca、resource前缀bars:读取价格；四资产都需有series，同资产同日不同close会阻断。不重新请求行情。
7. 对每个forecast找到T1/T3/T5 Outcome window；必须有schedule baseline date与window observed date价格、realized时刻晚于Decision。当前标签是每资产价格 `(future-base)*S/base`，**不是成交收益、组合Outcome净收益或学习评分**；NoOrder也可贡献forecast标签。[^C04][^C09]

**时间精度/身份边界必须保留：**

- collect用`session_close_time(date)=当日23:59:59 UTC`作为日级realized_at；它不是交易所实际close，不重跑共同交易Session选择。共同Session真实性依赖已冻结Outcome的上游生产链；本报告不接管该算法。
- 未显式training_start/end时，start=所选Decision最早created_at；end=所选Outcome windows日期对应日末的最晚值。
- 一个Run要12条forecast全部落入窗口才加入，不能只收其部分horizon。panel按日期裁窗、冲突检测，再取四资产日期交集；每资产至少min_samples+1个共同price points（默认31），即至少30个returns。每slot至少30条、总至少360条；这不等于360个独立Run或独立市场状态。
- 原始中性forecasts没有在collect中过滤；它们的预测概率和后续真实方向也参加Brier/bin统计。运行时仍禁止把新的中性forecast复活。
- readiness sealed数与collect实际选中样本数是不同指标；collect尚未做完整builder的risk值/Brier/capable检查。
- collect样本provenance.model_version_hash **直接填写当前configured_synthesizer_identity计算的hash**。历史identity helper只返回provider/model/Contract，不返回历史版本hash。当前配置hash包含base_url（去末尾斜线）、release_date、knowledge_cutoff、reasoning、language等，但这里没有逐历史turn核对这些字段；仅“hash相等”是在后续builder里验证本次组装的字段自洽，不能宣称历史日期/route参数已全部独立核实。
- collect/build没有把model release/cutoff作为字段与每个历史forecast_at逐样本比较；不能从日期字段存在推出完全无训练污染。禁止用猜测日期制造身份匹配。[^C04][^C09][^I01][^C10]

不足时输出status=BLOCKED、counts、price_conflicts、skipped_runs，不写dataset、不build、不activate；成功才写`StoredCalibrationDataset { input, report }`，source_refs包含同Store risk limits + 选中Decisions + 对应Outcomes，之后verify_integrity。input包含schema1、policy_version=`offline_historical_v2`、algorithm_version=`offline_historical_risk_v1`、route=`research.synthesizer`、regime=`all`。[^C10][^S01]

### 9.4 build：离线纯拟合 + SQL candidate

CLI用可写Store打开指定dataset，要求Canonical+kind匹配；纯函数`build_offline_decision_policy(input,Utc::now())`不访问模型/行情/Store；成功后persist candidate并verify_integrity，输出activated=false。[^C03]

纯输入校验：training_start<=training_end<=frozen_at、min_samples>0、source_runs非空唯一；四资产价格序列恰好一次、日期严格递增、正价格且在训练窗内；每预测`forecast_at < realized_at <= training_end`且forecast_at>=training_start、cutoff=forecast_at、provider/model/version/route/Contract与input一致。source_decision/context只校验kind，不在纯函数重新打开Store。该函数未额外验证 `(source_run,asset,horizon)` 样本唯一；正式数据可信度依赖collect及Store合法生产路径，不应把任意构造input视为真实Paper样本。[^C05][^C11]

概率拟合：

- 每asset×horizon独立，总样本<min时Err；10等宽概率bins：0..99999，100000..199999，…，900000..1000000。
- calibrated_probability=正实现收益样本数*S/count；实现收益=0算非正。
- calibrated_expected_alpha=该bin realized_return_ppm均值；名字alpha**没有减去benchmark**，不是raw预期收益回归。
- 空bin存p=500000、alpha=0、count=0；不是虚构观测，运行时bin count门仍拒绝。
- forecast面与资产级Brier都取原始p对实际二元方向的平方误差均值，ppm单位；不是“拟合后校准概率在独立holdout上的Brier”。本builder没有独立holdout/rolling out-of-sample评估步骤。
- 每面fit_dataset_hash绑定全部input.forecasts，trained_through=input.training_end、frozen_at=build时刻。[^C06]

风险拟合（只说明DecisionPolicy数值，不扩展Outcome学习）：

- 四资产共同日期，相邻close计算ppm returns；样本variance/covariance分母n-1，年化252。
- vol=round(sample_std_ppm*sqrt252)；beta=`abs(cov(asset,QQQ)/var(QQQ))*S`，benchmark固定QQQ；无QQQ方差、非正vol/beta或没有负收益尾部则RiskUnavailable。
- ES为最坏`ceil(returns总数*0.05)`个负收益幅度均值（至少1、最多实际loss数），gap为最大**日close-to-close损失**，不是实测overnight gap。
- liquidity/capital/decay来自operator，decay只给TQQQ/SOXL，不从这些bars拟合。
- risk.sample_count=共同returns数；asset.sample_count=该资产所有horizon预测数（完整30 Run时90）；forecast面sample_count=30，bin可能远小于30，需分开解释。
- covariance×252；对角用舍入vol²，非对角限制在±vol_i*vol_j；风险浮点必须有限、>0且<=3000000；cov绝对值<=9000000000000。
- build应用显式risk limits和input.min_samples；未给的horizon权重沿用默认三等分。最后Policy.validate与envelope.validate；**不强制decision_capable=true才保存candidate**，Brier差的合法候选可能build成功而不能activate。[^C12][^C13][^C14]

### 9.5 inspect、validate、activate与CAS哈希

- inspect：既有只读Store，按Artifact ID读元数据+payload，允许3种calibration kind。代码只检查kind，未像calibration_artifact helper那样检查Canonical，注释“canonical检查”比实际分支强。它不验证拟合也不激活。[^C03]
- validate：既有只读Store，要求Canonical DecisionPolicy；decode_strict，输出status=valid与decision_capable布尔；**不读config、不匹配当前Contract、不要求capable=true**。[^C03]
- activate：读当前模型config并resolve；可写Store；拒绝隔离；读同一Canonical policy Artifact、strict decode + 当前Synthesizer模型身份检查；读**已存active Synthesizer Contract**，无则Err，hash不同则Err；helper再次strict decode且要求decision_capable；生成descriptor并调用Store activation事务。[^C03][^I05]
- 文档要求operator显式inspect→validate→activate；源码没有“必须先持久化inspect/validate操作receipt”的状态机，activate自身重新校验即能执行，不能谎称SQL强制记录了这两步审批历史。[^C03][^S02]

| 名称 | 实际含义 |
|---|---|
| dataset `input_hash` / policy provenance.input_hash | 完整OfflineCalibrationInput的canonical JSON hash；不含dataset report |
| forecast `fit_dataset_hash` | input.forecasts序列的canonical JSON hash |
| provenance.output_hash / descriptor.policy_hash / DecisionContext.decision_policy_hash | 完整DecisionPolicy正文hash（非整个envelope） |
| provenance.risk_model_hash / assessment.risk_model_hash | PortfolioRiskModel正文hash |
| descriptor.envelope_hash | 整个Policy Artifact blob hash |
| loader的`input_hash`，Debug/部分CLI展示`decision_policy_input_hash` | **envelope bytes hash**，不是上面的训练input_hash |
| Artifact ID | Store Artifact身份；CLI要求的--dataset/--policy/--artifact是它，不是文件路径或任选policy hash |

build生成的Policy Artifact **source_refs为空**，Store descriptor也要求无origin且无source_refs；dataset才有risk/Decision/Outcome引用DAG。Policy回溯dataset通过provenance.input_hash等字段，不存在policy→dataset ArtifactRef边，不能把“完整血缘”误报成直接全DAG闭合。[^C03][^C05][^C11][^I01][^S01][^S02]

Store层：

1. `write_calibration_artifact`禁止Debug和错误lifecycle/origin；dataset至少有RiskLimits、Decision、Outcome三类引用；Immediate事务重读每个来源的kind+Canonical并insert Artifact/refs；不写Policy installation/head。
2. `activate_decision_policy`校验descriptor，Immediate事务：同policy_hash已有记录必须descriptor+Artifact完全相同，否则冲突；未安装则写Artifact+installation。若不是当前hash，检查激活时间不倒退，append(previous_policy_hash,policy_hash,activated_at)，更新singleton_id=1 head；同当前hash重放不append历史。
3. 这里CAS指内容寻址存储；head更新是串行Immediate事务的upsert，接口没有由调用方提供expected_previous_hash的比较交换参数。不要混淆为额外operator版本CAS批准。
4. Doctor重建安装、activation previous链、时间单调与head最后一条一致；本次没有运行Doctor。
5. `bootstrap_active_decision_policy_from`只复制源active immutable Policy bytes+descriptor，**会在目标Store调用activate**；源无active则None。它不复制Run/Outcome/凭据、不解除隔离样本资格；不是collect/build，也不是“只读复制”。底层activate没有Debug拒绝，由CLI普通activate拒绝，而bootstrap可服务隔离策略快照复用。[^S01][^S02][^S04]

## 10. 文档漂移、实现不对称与不能夸大的保证

### A. canonical当前Contract绑定存在显式检查缺口（优先关注）

文档说“已安装policy仍须严格匹配当前模型及Synthesizer Contract”。实际loader检查model/version和descriptor↔envelope Contract自洽，**不读当前active Contract**；serve只有`debug_control && !uncalibrated_research`分支调用当前catalogue hash比较；canonical普通serve只把内嵌DecisionPolicy交给Daemon。DecisionRuntime只收到Policy正文（没有envelope.provenance.contract_hash），decide验证proposal producer Contract已安装/版本够，但不比较它与校准Contract。

因此activate当时的Contract绑定、readiness警告、Debug preflight，不构成canonical以后每次使用时绑定未漂移的证明。特别是catalogue升级或现有active Policy沿用时，不能按文档推断“必拒绝/targets必零”。这是已核实的数据流缺口；没有运行构造或攻击，不能断言当前Store已发生错配，更不能据此推断Broker已下单。[^V02][^I01][^I02][^I03][^D02]

### B. SQL active head不等于正在运行的Policy

canonical readiness读SQL+配置+已存Contract；health读DecisionRuntime.policy，非Debug常用capable映射ready/unconfigured/insufficient，而不是逐次做上述身份检查。独立activate不会自动替换runtime内存；SQL、health和Run Context应分开看。[^I04][^I02][^I03][^D05]

### C. preflight并非严格只读

calibration.rs文件导读/分派注释说readiness/preflight只读；实际Preflight调用`Store::open(scratch)`，可创建/初始化/升级；eligible时调用`canonical_synthesizer_contract_hash`，内部`ActiveResearchCatalogue::install`可写Contract。它不发模型调用不等于不写Store。readiness明确避免这个helper。故本次未运行Preflight。[^C03][^I07][^S03][^C01]

### D. 资格摘要与真正授权谓词不一致

实际`claim_slot_eligible`强制price+macro；`build_asset_eligibility.directional_evidence`仍强制Price+Macro+NewsEvent，且聚合逻辑较粗；`eligible`只是返回的诊断字段，未用于回写target或Context.accepted。因此可能targets有值但摘要MissingEvidence/eligible=false；不能用摘要解释某次真实首个归零分支，应看runtime_trace和target实际分支。`horizon_trace.included_in_target`也不计杠杆持有期和最终risk/edge全部条件。[^D17][^D10][^D26][^D22]

### E. 模型时间身份并非历史turn全字段复核

collect严格要求当前配置有日期，却只从历史turn核provider/model/Contract；版本hash用当前配置赋予样本。builder验证这些已组装字段一致，而不验证日期真实性或模型发布/知识时间相对历史预测的关系。故“模型Contract绑定”必须分别说清当前加载、历史收集和当次proposal三个边界。[^C04][^C09][^C11][^I01]

### F. CONTEXT版本与术语落后

CONTEXT.md 的版本说明仍为Store17 / research67 / Prompt37 / freshness68；当前源码Store18 / research69 / Prompt38 / freshness70，与运行时契约当前版本一致。CONTEXT Canonical Run 专指scheduler-owned Paper；当前日常PositionPlan使用canonical Store/正式Core但Decision仍RunScoped。必须区分“canonical Store/正式入口”与“Canonical Paper artifact/样本”。[^V01][^V03][^D03]

### G. 注释的其他过强表述

- `decision_gate.rs` 的模块导读仍提model DecisionDraft，实际public vocabulary已是DecisionProposal别名；不代表旧Draft模型阶段复活。[^D01][^D28]
- Store active读取注释说“head存在但安装不完整Err”，实际read_decision_policy缺installation row返回None，active_decision_policy直接转交该Option；正常FK/Doctor应保障一致性，但仅这个函数不能声称覆盖所有损坏情况。[^S04]
- readiness的“sealed”只表示其持久化标签资格检查成功；不能推导独立holdout表现、足够bin count、risk可测、当前Contract组足样本、candidate存在或已激活。
- risk字段gap_loss的实现是最坏日收盘收益；calibrated_expected_alpha是bin原始实现收益均值；不要在报告中改叫实际隔夜跳空损失或超额收益验证。[^C01][^C06][^C12]

这些问题均未修复，符合只读范围。也没有通过恢复旧流程、伪造policy/模型元数据、降低样本门槛或开启交易来绕过。

## 11. 验证边界与交接

本次证据级别：**source-reviewed（源码/文档交叉核查）**；报告已写入。没有业务implemented变更；不标记运行时offline-verified、real-Paper-verified或outcome/learning-verified。

原始核查使用本地`git status/rev-parse`、限定目录`rg`与源码阅读；当前版本按仓库相对文件定位并复核链接，未因此验证运行时。未运行cargo、Python仓库测试、fixture、readiness、preflight、collect、build、inspect/validate CLI、activate、Doctor，也未读取真实样本数、已安装Policy ID或当前进程hash。没有证据说明当前用户机器Policy缺失/已错配，只报告代码对这些条件的处理。

静态交付需复核来源文件、脚注定义、代码围栏与行末空白；`git diff --check`只覆盖 Git 记录的差异。收尾`git status --short`与初始既有改动列表一致；原始报告曾存于忽略目录；当前 `docs/reports/` 副本是未跟踪文档，需独立做格式与链接检查。这里的通过仅覆盖文档交付与静态工作树检查，不升级为业务运行时验证。

读取到但未运行的测试包括：默认Policy不capable、低confidence与empty eligible不同归零分支、中性forecast不被正bin复活；offline candidate可出非零targets、359/360样本不足、strict decode拒裸旧policy；CLI隔离readiness和preflight匹配测试；Store activation幂等/历史保留。这些是测试设计证据，不是本次执行成功。[^T01]

给执行worker的最小交接：以Context.accepted/hard_blockers、validity、target、原approval和ExecutionVerdict解释下单资格，不把research weights或targets=0直接翻译为订单；本文只核到Allocator delta接口。给Outcome/学习worker的最小交接：collect消费Canonical sealed Outcome的windows/market_evidence作为标签来源，模型日期、隔离Store、同Contract训练组与sample/bin统计问题见第9节；不把数值密封自动等同叙事/学习晋级。[^E01][^E02][^C04][^C09]

## 12. 源码文件与职责索引

以下为本次工作树的仓库相对源码文件索引；脚注按职责说明定位，负面断言需结合实际参数、分支和调用链重新核查。

[^D01]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（proposal alias、研究配置与schema）。

[^D02]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（构造、permit、Contract、Review和slot先验）。

[^D03]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（Context/Decision同target及原子commit）。

[^D04]: [crates/akzio-daemon/src/http_launch.rs](../../crates/akzio-daemon/src/http_launch.rs)（正式Run入口）；[crates/akzio-daemon/src/orchestration/workers.rs](../../crates/akzio-daemon/src/orchestration/workers.rs)（34Need PositionPlan图）。

[^D05]: [crates/akzio-daemon/src/scheduler/scheduler_tick.rs](../../crates/akzio-daemon/src/scheduler/scheduler_tick.rs)（SQL active head与cold-start approval）。

[^D06]: [crates/akzio-domain/src/debug.rs](../../crates/akzio-domain/src/debug.rs)（冻结research-only身份）；[crates/akzio-daemon/src/debug.rs](../../crates/akzio-daemon/src/debug.rs)（prepare资格）；[crates/akzio-daemon/src/debug.rs](../../crates/akzio-daemon/src/debug.rs)（禁止resume和Decision）。

[^D07]: [crates/akzio-runtime/src/runtime/compilation/evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)（PositionPlan终止于Decision）；[docs/development-workflow.md](../development-workflow.md)（canonical Paper NoOrder与Outcome边界）。

[^D08]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（target函数完整资格与sizing分支）。

[^D09]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（hard blockers与confidence）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（质量只追加blocker）。

[^D10]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（asset eligibility诊断投影）。

[^D11]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（ResearchPlanReview字段与校验）。

[^D12]: [crates/akzio-execution/src/decision_gate/commit.rs](../../crates/akzio-execution/src/decision_gate/commit.rs)（learning influence边界）。

[^D13]: [crates/akzio-daemon/src/application/paper_execution.rs](../../crates/akzio-daemon/src/application/paper_execution.rs)（final proposal选择和调用DecisionRuntime）。

[^D14]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（proposal provenance）；[crates/akzio-execution/src/decision_gate/helpers.rs](../../crates/akzio-execution/src/decision_gate/helpers.rs)（唯一Manifest）。

[^D15]: [crates/akzio-execution/src/decision_gate/validate.rs](../../crates/akzio-execution/src/decision_gate/validate.rs)（Manifest/递归闭包/bytes与hash）。

[^D16]: [crates/akzio-execution/src/decision_gate/validate.rs](../../crates/akzio-execution/src/decision_gate/validate.rs)（draft引用集合和learning归因）。

[^D17]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（概率/中性/方向）；[crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（12slots和精确qualification）；[crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（方向Contract47）。

[^D18]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（cutoff与有效期）。

[^D19]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（raw/validated处理与状态优先级）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（本地research slot helper）。

[^D20]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（consensus构建和confidence）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（关键Claim唯一review）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（materiality与overlap常量）。

[^D21]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（研究质量None与floor）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（四项质量的计数口径）。

[^D22]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（accepted、target/risk字段验证）。

[^D23]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（全部默认值）。

[^D24]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（bin连续覆盖与总数守恒）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（asset与risk模型校验）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（Policy.validate）。

[^D25]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（decision_capable谓词）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（calibrated_forecast的point-in-time/bin门）。

[^D26]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（horizon方向冲突与included标记）。

[^D27]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（组合risk计算）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（比例缩放与整数平方根）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（排除分支trace）。

[^D28]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（模块标题与历史wire措辞）。

[^R01]: [crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（numeric refs selected、全Claim/Critique闭包）。

[^R02]: [crates/akzio-domain/src/research_review.rs](../../crates/akzio-domain/src/research_review.rs)（版本与17scope numeric basis）；[crates/akzio-domain/src/research_review.rs](../../crates/akzio-domain/src/research_review.rs)（Review issues/accepted/authorizes）。

[^R03]: [crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（authority/current定义）；[crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（gap scope）；[crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（Critique blocker/Supported）。

[^R04]: [crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（news supporting refs与scope）；[crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（ground与payload精确范围）；[crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（news source_verified）。

[^R05]: [crates/akzio-store/src/store/research_review.rs](../../crates/akzio-store/src/store/research_review.rs)（final成功依赖review、revision、provenance与hash）。

[^Q01]: [crates/akzio-domain/src/qualification.rs](../../crates/akzio-domain/src/qualification.rs)（资格阶段/verdict）；[crates/akzio-domain/src/qualification.rs](../../crates/akzio-domain/src/qualification.rs)（receipt身份与校验）。

[^Q02]: [crates/akzio-domain/src/evaluation/policy.rs](../../crates/akzio-domain/src/evaluation/policy.rs)（CandidatePolicy和PolicySubject）；[crates/akzio-domain/src/evaluation/policy.rs](../../crates/akzio-domain/src/evaluation/policy.rs)（允许influence的状态）。

[^Q03]: [crates/akzio-domain/src/market_safety.rs](../../crates/akzio-domain/src/market_safety.rs)（consensus independent与Jaccard）。

[^C01]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（只读readiness扫描、状态、隔离与limits）。

[^C02]: [crates/akzio-cli/src/main.rs](../../crates/akzio-cli/src/main.rs)（全部calibration CLI定义与显式风险参数）。

[^C03]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（命令分派、build/validate/activate/preflight）。

[^C04]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（collect Store资格、metadata、成熟样本和Contract选择）。

[^C05]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（Policy envelope和严格decode）。

[^C06]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（10bins、p/alpha、Brier及fit hash）。

[^C07]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（分类和gap）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（Run readiness lineage/labels）。

[^C08]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（OfflineRiskLimits校验）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（Canonical helper和CAS写入）。

[^C09]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（时间与每个sample组装）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（日末时间、historical identity和Alpaca labels）。

[^C10]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（价格交集、count、BLOCKED和dataset持久化）。

[^C11]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（builder、input时间/身份/价格验证）。

[^C12]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（QQQ benchmark、vol/beta/ES/gap/decay）。

[^C13]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（covariance、共同returns与sample variance）。

[^C14]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（有限值/数值范围）。

[^I01]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（配置模型hash、SQL加载、envelope身份与缺head）。

[^I02]: [crates/akzio-cli/src/cli/run_commands.rs](../../crates/akzio-cli/src/cli/run_commands.rs)（serve只对Debug做额外Contract比较）；[crates/akzio-cli/src/cli/run_commands.rs](../../crates/akzio-cli/src/cli/run_commands.rs)（传入DaemonConfig内存Policy）。

[^I03]: [crates/akzio-daemon/src/orchestration/bootstrap.rs](../../crates/akzio-daemon/src/orchestration/bootstrap.rs)（catalogue安装及DecisionRuntime快照）。

[^I04]: [crates/akzio-daemon/src/orchestration/health_canary.rs](../../crates/akzio-daemon/src/orchestration/health_canary.rs)（health用runtime Policy的投影）。

[^I05]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（激活要求capable并生成descriptor）。

[^I06]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（load_config解析凭据）；[docs/development-workflow.md](../development-workflow.md)（模型日期真实资料与SQL顺序）。

[^I07]: [crates/akzio-daemon/src/lib.rs](../../crates/akzio-daemon/src/lib.rs)（所谓canonical hash helper会install catalogue）。

[^S01]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（calibration CAS write seam）。

[^S02]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（descriptor）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（activation完整事务）。

[^S03]: [crates/akzio-store/src/store/schema.rs](../../crates/akzio-store/src/store/schema.rs)（read-only与可写打开语义）。

[^S04]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（active read、bootstrap、缺row分支）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（history完整性）。

[^E01]: [crates/akzio-execution/src/execution_gate/core.rs](../../crates/akzio-execution/src/execution_gate/core.rs)（blockers和有条件Allocator入口）；[crates/akzio-execution/src/execution_gate/core.rs](../../crates/akzio-execution/src/execution_gate/core.rs)（Accepted与NoOrder）。

[^E02]: [crates/akzio-execution/src/allocation.rs](../../crates/akzio-execution/src/allocation.rs)（Decision接受态与session）；[crates/akzio-execution/src/allocation.rs](../../crates/akzio-execution/src/allocation.rs)（delta方向与无单）。

[^V01]: [crates/akzio-research/src/agent/catalogue.rs](../../crates/akzio-research/src/agent/catalogue.rs)（当前研究版本）；[crates/akzio-store/src/store/prelude.rs](../../crates/akzio-store/src/store/prelude.rs)（Store18）；[crates/akzio-domain/src/schema.rs](../../crates/akzio-domain/src/schema.rs)（Domain10）。

[^V02]: [docs/agent-runtime-contract.md](../agent-runtime-contract.md)（文档policy当前身份与正式/Debug区别）；[docs/agent-runtime-contract.md](../agent-runtime-contract.md)（双Gate与SQL边界）。

[^V03]: [CONTEXT.md](../../CONTEXT.md)（Canonical术语）；[CONTEXT.md](../../CONTEXT.md)（过期版本段落）；[docs/agent-runtime-contract.md](../agent-runtime-contract.md)（契约当前版本）。

[^T01]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（未运行Decision测试）；[crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（未运行calibration测试）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（未运行隔离readiness测试）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（未运行preflight Contract测试）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（未运行Store激活测试）。


---

<!-- 分报告 4: 04-paper-execution.md -->

# 04 — Paper 执行完整流程：当前工作树只读核查

## 0. 结论、范围与证据级别

- 核查日期：2026-09-29，Asia/Singapore（UTC+08）。基线 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`。**依据当前未提交工作树，不是仅依据 HEAD。**
- 结论：现有源码具备 `Decision → fresh execution snapshots → ExecutionGate → durable Commitment → effect intent → Alpaca Paper dispatch/reconcile → Evaluate/OutcomeSchedule` 链路；但**源码存在完整链路不等于本次已经验证可下单、可成交、可自动恢复或可密封 Outcome**。
- 核心语义：研究建议、正式目标、Gate Accepted、请求前承诺、Broker submitted/accepted、实际 filled、对账 Complete、OutcomeSchedule 是八个不同层次。`Complete` 可以由取消/过期/拒绝收束，未必全部成交。
- 已先读当前 `AGENTS.md`，运行时契约 3/7/8/11/12（补读涉及 Rust/Store 的 1/2/4）和 Debug 文档。执行核查采用并行只读源码搜索；没有创建新 chat，没有声称已启动并不存在的 worker。
- **本次唯一写入是本报告及其必要父目录。**未修改源码、配置或 Store；未打开 canonical 数据库、BLOB、密钥或环境凭据；未运行 Core/模型/API/Broker；未运行 cargo、fixture、Store Doctor 或 Paper 验收。报告不包含真实账户状态、Run ID、订单或成交证据。
- 证据标签：`source-audited`；报告静态检查另见末节。没有可报告的 `real-Paper-verified` 或 `outcome/learning-verified`；没有源码实现改动可标 `implemented`。先前记忆只用于定位，本报告技术结论均重新沿当前源码核查。
- 初始工作树已有 13 个 tracked modified 文件及一个 untracked 脚本：CI、AGENTS、README、daemon HTTP/launch/lib/health、Store debug bundle、四份 docs、run_core.py、test_run_core.py；本任务不修改、不格式化、不清理、不提交这些内容。原始报告当时存于忽略的 `.akzio/`；当前副本位于 `docs/reports/`。

规则依据：`AGENTS.md`、`AGENTS.md`；`docs/agent-runtime-contract.md`、`docs/agent-runtime-contract.md`、`docs/agent-runtime-contract.md`、`docs/agent-runtime-contract.md`；`docs/debug-control.md`、`docs/debug-control.md`。

## 1. 总流程与产物语义

```text
研究 Proposal（forecast + research_allocation；模型建议）
  → DecisionGate：生成 Decision.targets / DecisionContext.target（Rust 正式目标）
  → ExecutionGate task：执行时重新采集六项 Need，合成三类 typed snapshots
      ├─ 新鲜 Closed + Decision 未过期 → Deferred；没有 verdict，重领后全量刷新
      ├─ 完整性/获取错误 → Err → 按冻结 task policy Retry/Failed（不是 NoOrder）
      ├─ business blockers → ExecutionContext + NoOrder（可无 Plan）
      └─ blockers 全空 + complete plan closure → Plan + Context + Accepted
  → PaperCommit task
      ├─ NoOrder → NoOutput / Succeeded；不取执行 lease，不造 Commitment
      └─ Accepted → session slot + approval + lease + permit 检查，原子落 Commitment
  → Reconcile task / PaperDispatchRuntime
      ├─ NoOrder → NoOutput / Succeeded；不拿 broker，不取执行 lease
      ├─ Debug forbidden / Frozen / fencing / identity 等边界 → 阻断或等待
      └─ durable effect intent → 先 lookup r1/r0 → 仅缺单且发送窗口仍有效才 POST
           → 查询当前回执 → 必要时 Regular 受控撤改单 → 领域 Reconciliation
             ├─ Pending/Partial → 仅持久进度 → Deferred ≥1s → 后续重新领取
             └─ Complete → 回执、对账、effect terminal、task succeeded 原子提交
  → Evaluate
      ├─ NoOrder lineage（无需 Commitment）
      └─ Accepted + Commitment + Complete Reconciliation lineage
           → OutcomeSchedule（不是立即 T+1/T+3/T+5，也不是立即 learning）
```

**图的真实依赖**：PositionPlan 在插入 Evidence 和 Decision 后直接返回；Paper 才添加 ExecutionGate、PaperCommit、Reconcile、Evaluate 的串行终端依赖。不是 UI 隐藏执行按钮，而是图根本没有这些节点。源码：`crates/akzio-runtime/src/runtime/compilation/evidence.rs`。daemon 路由：`crates/akzio-daemon/src/dispatch.rs`。

| 层次 | 当前实现的含义，不应越级解释 | 精确来源 |
|---|---|---|
| 研究建议 | `research_plan.raw/validated` 独立于目标；可以非零但 `execution_status=blocked/not_applicable` | `crates/akzio-execution/src/decision_gate/decide.rs` |
| 正式 Decision | `Decision.targets` 与 Context 的 `target` 来自 Rust policy；不是把 research allocation 直接抄成订单 | `crates/akzio-execution/src/decision_gate/decide.rs`、`crates/akzio-execution/src/decision_gate/decide.rs` |
| Context accepted | `hard_blockers` 与 `material_conflicts` 均为空；零目标也可能 accepted | `crates/akzio-domain/src/decision.rs` |
| Execution Accepted | blockers 全空并满足 plan closure；只代表执行方案获准，不代表 HTTP 已发出 | `crates/akzio-execution/src/execution_gate/core.rs` |
| Commitment | 请求前不可变订单身份和计划绑定；PaperCommit 本身不发 HTTP | `crates/akzio-daemon/src/application/paper_execution.rs` |
| submitted / Broker accepted | adapter 已得到订单响应；new/accepted 被映射为 Accepted，尚未成交 | `crates/akzio-execution/src/paper/reconcile.rs`、`crates/akzio-execution/src/paper.rs` |
| filled | 必须数量守恒、filled=requested、remaining=0、正均价 | `crates/akzio-domain/src/execution/effects.rs` |
| Reconciliation Complete | 覆盖全部 commitment 资产、所有 durable successor 已观察、全部为无后继终态；取消/拒绝也可满足 | `crates/akzio-execution/src/reconciliation.rs` |
| OutcomeSchedule | 冻结评估血缘和 baseline 交易日；实际跨 Session 工作另行进行 | `crates/akzio-daemon/src/application/outcome_sealing.rs` |

## 2. 真实源码默认参数（不是本机运行配置）

生产 daemon 直接以两套 `Default` 构造 ExecutionRuntime，以默认 PaperDispatchRuntime 构造 dispatch；运行身份哈希包含 allocation、ExecutionGate 和 dispatch 超时参数。未读取本地配置来确认当前进程值。源码：`crates/akzio-daemon/src/orchestration/bootstrap.rs`、`crates/akzio-daemon/src/orchestration/bootstrap.rs`、`crates/akzio-daemon/src/lib.rs`。

| 参数 | 默认值 / 真实口径 | 来源 |
|---|---|---|
| 可执行资产及固定枚举顺序 | TQQQ、QQQ、SOXX、SOXL | `crates/akzio-domain/src/core.rs` |
| allocation gross cap | 1,000,000 ppm = 100% 资本权重 | `crates/akzio-execution/src/lib.rs` |
| 单次新增名义金额默认上限 | 2,000,000 USD cents = **$20,000**；生产还受 approval 和 buying power 限制 | `crates/akzio-execution/src/lib.rs` |
| allocation daily turnover cap | 1,000,000 ppm = 100%；包含当天已成交 turnover + 本次买卖总名义金额 | `crates/akzio-execution/src/lib.rs`、`crates/akzio-execution/src/allocation.rs` |
| Account / Quote / Clock 最大年龄 | **各 5 秒**；未来偏移各最多 15 秒；三快照最大时间差 15 秒 | `crates/akzio-execution/src/lib.rs` |
| spread / limit protection | 20 bps 最大价差；买 ask +10 bps、卖 bid −10 bps，再按 $0.01 tick 买向下、卖向上舍入 | `crates/akzio-execution/src/lib.rs`、`crates/akzio-execution/src/lib.rs` |
| Gate factor caps | leveraged equity、Nasdaq、semiconductor、paired index 全为 500,000 ppm；TQQQ/SOXL 以 3x 同日经济暴露计算 | `crates/akzio-execution/src/policy.rs`、`crates/akzio-domain/src/execution/snapshots.rs` |
| Gate turnover / Mandate | 换手 50%；Mandate 总敞口 50%、最低现金 50%、projected drawdown 15%、换手 50%；禁资产/动作列表默认空 | `crates/akzio-execution/src/policy.rs`、`crates/akzio-domain/src/longitudinal.rs` |
| Capacity | 参与率 5%、估计滑点 1%、impact slope 20%；daemon homogeneous_agent_count=1 | `crates/akzio-domain/src/market_safety.rs`、`crates/akzio-daemon/src/application/paper_execution.rs` |
| Compliance | 撤单率上限 50%；允许 Licensed；restricted/watch lists 默认空；只要求五项 Paper baseline controls | `crates/akzio-domain/src/market_safety.rs`、`crates/akzio-domain/src/market_safety.rs` |
| Decision validity | 默认最多 300,000 ms = 5 分钟，且取所有 forecast thesis expiry 的最小值 | `crates/akzio-execution/src/decision_gate.rs`、`crates/akzio-execution/src/decision_gate/decide.rs` |
| Dispatch 短轮询 | deadline 15 秒；每 500 ms 查询；timeout=0 也至少查询一次。deadline 不取消正在执行的单个 GET | `crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs` |
| Regular action grace | 距 `broker_updated_at` ≥60 秒；每资产最多 r0→r1 一次，不是每 60 秒追价 | `crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs` |
| Broker HTTP | connect timeout 15 秒、请求 timeout 30 秒、HTTP/1、IPv4 local bind、禁止 redirect | `crates/akzio-execution/src/paper/execute.rs` |
| HTTP 重试 | `get_json` 的 send transport 最多 5 次，退避 250/500/750/1000 ms；不重试 HTTP 非 2xx；lookup 自己直接 send，不用该循环；POST/PATCH/DELETE 不自动重试 | `crates/akzio-execution/src/paper/transport.rs`、`crates/akzio-execution/src/paper/reconcile.rs` |
| task budget | ExecutionGate 90 秒、最多 2 次（transport/rate-limit）；PaperCommit/Reconcile/Evaluate 30 秒，RetryPolicy::none = 最多 1 次 | `crates/akzio-runtime/src/runtime/catalogue.rs`、`crates/akzio-domain/src/core.rs` |
| lease | task lease 30 秒，heartbeat/recovery tick=lease/3 即默认10秒；scheduler lease 30 秒 | `crates/akzio-runtime/src/runtime/task.rs`、`crates/akzio-runtime/src/runtime/task.rs`、`crates/akzio-daemon/src/scheduler/scheduler_core.rs` |
| market feed | 配置是 `Option<AlpacaMarketDataFeed>`，不是默认已获 SIP；普通执行 quotes 要求显式 feed；IEX/overnight 的额外阻断见第4节 | `crates/akzio-cli/src/main.rs`、`crates/akzio-ingest/src/paper_session.rs` |

**解释限制**：allocation 100% 上限不代表生产能投入100%；后续 Mandate 和 Gate 有更紧的50%及3x因子限制。Capacity 中 fill probability 是公式估计，不是 Broker 成交证明。

## 3. Decision 后的 fresh snapshots、Session 与身份链

### 3.1 六项 Need → 三类快照

研究 EvidenceGate 对 `ExecutionSafety` 只记 `deferred_to_execution`，不提前采集。Paper + 已安装 Alpaca production adapter 时，ExecutionGate task 调用 `refresh_execution_snapshots`；否则读取已绑定的执行输入，不从任意研究内容猜账户。来源：`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。

刷新严格从本 Run 的 session slot 和 EvidenceGate input 找 `scheduler.paper_snapshot` 六项，集合完整且不得重复：account、positions、open_orders、`fills:<slot date>`、quotes、clock。先并发取账户四项，再并发取 quotes/clock；并非六项天然具有同一观察时刻。来源：`crates/akzio-daemon/src/evidence.rs`。

| Need | 实现所读取路径 / 内容 | 源码 |
|---|---|---|
| account | GET `/v2/account`；active、trading_blocked、equity、buying_power | `crates/akzio-ingest/src/adapters.rs`、`crates/akzio-ingest/src/paper_decode.rs` |
| positions | GET `/v2/positions`；四资产 quantity_micros/market_value，其他 symbol 进入 external_positions | `crates/akzio-ingest/src/adapters.rs`、`crates/akzio-ingest/src/paper_decode.rs` |
| open_orders | GET `/v2/orders?status=open&limit=500&direction=asc&nested=true`；收集 client_order_id（字段缺失才尝试 id），非空即 blocker，不豁免 akzio 自己的单 | `crates/akzio-ingest/src/adapters.rs`、`crates/akzio-ingest/src/paper_decode.rs` |
| fills | GET `/v2/account/activities/FILL?date=<session>&direction=asc&page_size=100`；day turnover = Σ abs(qty)×abs(price)/1e6。**≥100条直接报需要分页，并未在此自动翻页** | `crates/akzio-ingest/src/adapters.rs`、`crates/akzio-ingest/src/paper_decode.rs` |
| quotes | 自己先取 clock/calendar 选 feed，再 GET `/v2/stocks/quotes/latest?symbols=TQQQ,QQQ,SOXX,SOXL&feed=...` | `crates/akzio-ingest/src/paper_session.rs`、`crates/akzio-ingest/src/paper_session.rs` |
| clock | GET `/v2/clock` + calendar [美东日期−1天, +14天]；Overnight 再逐一 GET 四资产 `/v2/assets/<symbol>` | `crates/akzio-ingest/src/paper_session.rs` |

账户四项的聚合 observed_at 取 **最早值**；不让较新响应续期旧 positions/fills。Account typed snapshot 不保存 broker account ID；该 ID 的绑定主要在 approval/scheduler。来源：`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-domain/src/execution/snapshots.rs`。

Quotes 的每个 `quote.t` 必须解析为 provider 时间戳；Clock 强制用 provider `timestamp`，不用传入的下载时间兜底。聚合 QuoteSnapshot.observed_at 使用采集传入时间，因此真正订单报价年龄还要逐资产检查。来源：`crates/akzio-ingest/src/paper_decode.rs`、`crates/akzio-execution/src/allocation.rs`。

Need/normalized 严格检查 source=Alpaca、resource、Need ID、scheduler producer、同 Run；typed materializer 检查同 permit origin、RunScoped normalized、同 source family、RawEvidence 引用，typed snapshots 为 Canonical，带完整 Raw+Normalized refs。来源：`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-execution/src/snapshot.rs`。

刷新产物按 raw→ingest-normalized→typed snapshot 逐项 `write_task_artifact`，**不是整个刷新批次一个事务**。中途失败不抹掉早先写入。account/clock 未封存直接 Err；只有明确 `Execution BLOCKED: InvalidQuote` 的 bad bid/ask 被转成 quote_error 继续生成持久 NoOrder。缺字段、其他 decode/provenance/获取错误不能一概解释为 NoOrder。来源：`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-daemon/src/evidence.rs`。

### 3.2 市场 Session 不是单独的 is_open

- 以 `America/New_York` 日历转换处理日期/DST，区间均为半开区间：每个实际交易日的**前一日20:00→04:00 Overnight；04:00→calendar.open PreMarket；open→close 且原 is_open=true Regular；calendar.close→20:00 AfterHours**。不匹配则 Closed，next_open 指向后续日历交易日前夜20:00。空 calendar 即使 is_open=true 仍 Closed。源码：`crates/akzio-domain/src/execution/session.rs`。
- 原始 `is_open` 没有被伪造为 true；`MarketClockSnapshot.tradable()` 看细分 session !=Closed。旧 snapshot 缺 session 时才按 is_open 兼容 Regular/Closed。源码：`crates/akzio-domain/src/execution/snapshots.rs`。
- 三个扩展时段生成 `extended_hours=true` 的 limit/day 订单。Overnight feed：SIP→BOATS；其他→overnight，不退回 IEX。资格必须 symbol匹配、status active、tradable true、overnight_tradable true、overnight_halted false（支持 attributes/顶层字段）。源码：`crates/akzio-domain/src/execution/session.rs`、`crates/akzio-ingest/src/paper_session.rs`。
- Gate 在计划产生后检查 Overnight feed和**实际有订单资产**是否均在 overnight_assets；POST前又用最新 Clock/calendar 和该资产资格重查。不是只检查研究时的资格。来源：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/paper/reconcile.rs`。
- Closed defer 在 pretrade/Gate 前执行：仅有新鲜 Closed snapshot 且 Decision 当前有效，唤醒为 `max(now+1s, min(next_open或now+5min, validity.valid_until))`；不形成 Plan/Verdict，重领执行时重新获取全部快照。过期 Decision 不会补单，而会继续到 StaleDecision 阻断。源码：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。此函数按整数秒比较 clock 年龄，Gate 的 freshness helper 则比较完整 Duration，不应混为完全相同边界。

### 3.3 Approval、runtime identity、kill switch 分层

1. `approve_paper` 先校验请求身份等于当前 daemon identity、资格报告当前有效、证据引用已存在/类型正确、session日期、operator/reason非空、valid_hours在(0,168]、最大金额≤$20k；通过后才构造 Paper client 读真实 account ID，冻结 RuntimeManifest/PaperLaunchApproval。**本次未调用此入口。**源码：`crates/akzio-daemon/src/orchestration/workers.rs`、`crates/akzio-daemon/src/orchestration/workers.rs`。
2. RuntimeIdentity 包括 code/Cargo.lock/config/model/prompt/contract/topology/decision-execution-evaluation policy/feed 及模型/治理 bundle；qualification key 是其中投影，不等同于全部 identity 字段。报告要求 replay/adversarial/execution_simulation/shadow/canary 全通过且critical_regressions=0，key相同、哈希合法、now处于资格时间窗。源码：`crates/akzio-domain/src/runtime_manifest.rs`、`crates/akzio-domain/src/longitudinal.rs`。
3. scheduler 创建新已批准 session 前比较 manifest identity 与 daemon expected identity，并通过 broker GET 比对 account ID、配置feed；旧 slot 优先返回原 Run，不另建重复Run。源码：`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`、`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`。
4. ExecutionGate 重查该 Run 已消费 binding 的 qualification和mandate hash；不是独立再次获取当前 daemon identity 全量比较。Commitment另查manifest/approval expiry、session date、买入金额；Dispatch另查policy hash、原source有效窗。边界不要统称“每次HTTP前重做全部启动审核”。源码：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-store/src/store/impl_workflow.rs`、`crates/akzio-execution/src/paper/submission_authorization.rs`。
5. kill switch 是 Store 最新 `FreezeState` 历史记录，不是内存开关。Gate加入Frozen，Commitment拒绝，Dispatch在任何execute/lookup前也拒绝。写freeze本身不会发cancel；没有发现逐POST读取FreezeState的实现，所以不能声称撤回已经在途的HTTP。源码：`crates/akzio-store/src/store/impl_core.rs`、`crates/akzio-execution/src/execution_gate/validation.rs`、`crates/akzio-execution/src/paper_commitment.rs`、`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs`。

## 4. ExecutionGate 全部阻断面（含 Err 与后置 Gate）

### 4.1 输入完整性错误和可持久 business blocker 不同

`evaluate()` 首先校验输入 kind、读/validate DecisionContext、Run一致性和来源闭包；bad artifact/JSON/Store 错误返回 Err，不生成NoOrder。完整性检查包括唯一market-state ContextManifest、Decision source refs、policy influence必须经该Manifest选中、canonical Paper来源、subject/head/state合法。源码：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/validation.rs`。

### 4.2 blocker 汇总

| 检查 / blocker | 触发条件与真实归类 | 精确来源 |
|---|---|---|
| 继承Decision全部hard_blockers | BTreeSet拷贝去重；不是重新把上游拒绝变成可下单 | `crates/akzio-execution/src/execution_gate/core.rs` |
| StaleDecision | validity缺失或当前已不在有效窗 | `crates/akzio-execution/src/execution_gate/core.rs` |
| InvalidProvenance | horizon_trace或investment_logic缺失；typed snapshot非Canonical；三snapshot skew>15s；allocation的SessionMismatch/Domain/InvalidPolicy | `crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs` |
| MaterialConflict | Decision material_conflicts非空 | `crates/akzio-execution/src/execution_gate/core.rs` |
| NonCanonicalRun | Run purpose不是Paper | `crates/akzio-execution/src/execution_gate/core.rs` |
| UnqualifiedRuntime | 无该Run已消费approval，或资格key/完整性/期限不通过 | `crates/akzio-execution/src/execution_gate/core.rs` |
| MandateViolation | approval mandate hash≠当前Gate mandate；或后置Mandate评估未permitted | `crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs` |
| Frozen | 最新FreezeState.frozen=true | `crates/akzio-execution/src/execution_gate/core.rs` |
| MissingAccount / StaleAccount | 缺account ref / observed_at超5s年龄或超15s未来偏移 | `crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs` |
| ExternalPosition / UnmanagedOpenOrder | external_positions非空 / 任意open_order_ids非空 | `crates/akzio-execution/src/execution_gate/snapshots.rs` |
| MissingQuote / InvalidQuote | 无quotes且无明确坏价错误→Missing；坏价error→Invalid；allocator missing/invalid映射相应项；Overnight非boats/overnight也Invalid | `crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs` |
| StaleQuote | quote snapshot过期/未来；account与quotes的broker_session不同；allocator每订单quote过期 | `crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/lib.rs` |
| MarketClosed | 缺clock；clock非tradable或过期/未来；account与clock session不同；allocator MarketClosed；Overnight某订单资产无资格 | `crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/execution_gate/core.rs` |
| UnsupportedUniverse | allocator ForbiddenAsset或InvalidWeight | `crates/akzio-execution/src/execution_gate/snapshots.rs` |
| FactorLimit / PairExposureLimit / TurnoverLimit | allocation gross/daily turnover超限；或计划factor/paired exposure、turnover超当前Gate上限 | `crates/akzio-execution/src/execution_gate/snapshots.rs`、`crates/akzio-execution/src/policy.rs` |
| NoExecutableOrder | DecisionRejected；账户inactive/blocked；不足购买力；short；新增金额错误；最终无订单。这些不同allocation错误折叠成同一枚举，不能从该枚举还原细因 | `crates/akzio-execution/src/execution_gate/snapshots.rs` |
| CapacityLimit | 有任意Buy（risk_increasing）且capacity不存在或不满足 | `crates/akzio-execution/src/execution_gate/core.rs` |
| ComplianceViolation | missing classifications 或任意订单资产违规集合非空 | `crates/akzio-execution/src/execution_gate/core.rs` |
| DependencyDegraded | 有Buy而new-risk dependency不通过；纯Sell而risk-reduction dependency不通过 | `crates/akzio-execution/src/execution_gate/core.rs` |
| 缺pretrade safety | 已有plan后同时加CapacityLimit、ComplianceViolation、DependencyDegraded；若前置blockers已存在则不进入allocation/该分支 | `crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs` |

`HardBlocker`全集还列有MissingEvidence、UnverifiedClaim、HorizonConflict、PlanHashMismatch、DuplicateCommitment、NonPaperEndpoint、RecoveryIncomplete等；**枚举存在不代表ExecutionGate当前直接派生它们**。上游可继承；后续Commitment/endpoint/recovery常是专用Error而非回填既有Verdict。本节所列是当前evaluate直接路径及allocation映射的完整面。枚举来源：`crates/akzio-domain/src/decision.rs`。

前置blockers为空才进入allocator；后置factor/mandate/pretrade失败时，Plan仍可被保留以供审计，但Verdict是NoOrder。最终plan/context/verdict与ExecutionGate任务成功状态一次commit_attempt；NoOrder是节点成功输出的业务拒绝，不是技术任务Failed。源码：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/execution_gate/core.rs`。

### 4.3 Pre-trade safety 的数据与真实限制

- **Capacity**：有任意Buy就对执行后假设target中全部非零资产要求正20日ADV；aggregate notional是 `account_equity × target_weight × homogeneous_agent_count`，不是本次delta；participation=aggregate/ADV，slippage=participation×impact_slope，fill_probability=1−participation（截限）。纯卖单跳过capacity但不跳过合规/关键依赖。来源：`crates/akzio-execution/src/pretrade_safety.rs`、`crates/akzio-domain/src/market_safety.rs`。
- **Compliance**：classified quotes/bars默认Public；Decision evidence中的financial_content可以收紧到Unknown/更严分类。逐订单资产检查restricted/watch list、MNPI/Confidential/Unknown、self-trade、cancel ratio、spoof/layering等。实际recent activity最多扫描500回执+500计划，过去24h按client ID保留最新；同资产双方向活跃视作self-trade，submissions≥6且取消≥一半触发spoof模式。MarkingTheClose/Prearranged/FrontRunning在daemon输入中固定false且不属于默认五控制覆盖，不能声称完整八项监控。来源：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-domain/src/market_safety.rs`。
- **Dependency**：结构上需十类：ModelProvider、NewsProvider、MarketData、MacroData、Broker、DNS、Clock、Storage、Identity、ComplianceData。新增风险要求全部permits_new_risk；纯减仓仍要求market/broker/clock/storage/compliance五类健康。fallback_policy字段不是自动切换供应商的执行器。来源：`crates/akzio-domain/src/market_safety.rs`、`crates/akzio-domain/src/market_safety.rs`。
- **重要实际feed限制**：daemon仅把SIP/BOATS标Healthy；IEX和indicative overnight标Degraded。因此即使Gate的Overnight feed白名单允许overnight，当前pretrade依赖仍会令其NoOrder；纯Sell也不例外，因为MarketData在risk-reduction必要集合中。来源：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-domain/src/market_safety.rs`、`crates/akzio-domain/src/market_safety.rs`。
- 模型依赖取本Run最近Synthesizer AgentTurn capability（全局最多500个AgentTurn中筛选），比较manifest provider:model和实际值；观察新鲜度用Decision.maximum_execution_delay_ms/1000至少1秒，默认300秒。News/Macro依赖从Decision已列证据聚合，默认300秒；market/broker/clock/DNS及内部依赖30秒。没有paper approval时返回None，**没有作出“安全通过”断言**。来源：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。

## 5. Allocation、sell/buy顺序、limit、qty和rounding

### 5.1 目标转订单的精确算法

输入是已validate且accepted的DecisionContext与同broker_session的account/quotes/clock；市场必须tradable。账户active、!trading_blocked、equity>0。AccountSnapshot领域校验也拒绝负buying_power/turnover、负数量/市值，故一些坏账户会在allocator前就Err，而非到NoExecutableOrder。来源：`crates/akzio-execution/src/allocation.rs`、`crates/akzio-domain/src/execution/snapshots.rs`。

1. `target_value_microUSD = floor(equity_microUSD × target_weight_ppm / 1e6)`；按四资产固定顺序计算 `delta = target_value − current_position.market_value`。delta=0不生成单；delta>0买，<0卖，notional=abs(delta)。**按市值差而不是直接按持仓数量差生成。**来源：`crates/akzio-execution/src/lib.rs`、`crates/akzio-execution/src/allocation.rs`。
2. 每个非零delta资产需正bid、ask>bid、quote在5s/15s窗口；spread计算为 `floor((ask−bid)×10000 / floor((bid+ask)/2))`，>20才拒绝；不能说未取整实数价差绝对≤20bps。来源：`crates/akzio-execution/src/lib.rs`。
3. Buy limit：先整数计算`ask×10010/10000`，再向下到$0.01；Sell limit：`bid×9990/10000`，再向上到$0.01。无market单、无市场追价。来源：`crates/akzio-execution/src/lib.rs`。
4. 买侧金额限额=`min(approval.manifest.maximum_notional, account.buying_power)`；买单超额按比例整数缩放，余数按TQQQ/QQQ/SOXX/SOXL顺序各补1微美元；零金额买单移除，卖单不缩。`maximum_total_notional`是历史名字，真实口径只限制Buy，不是buy+sell总额。**卖单预计所得不提前增加购买力**。来源：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/allocation.rs`、`crates/akzio-domain/src/execution/plan.rs`。
5. `turnover_ppm = floor((account.day_turnover + Σbuy + Σsell)×1e6/equity)`。先allocation≤100%，再Gate/Mandate≤50%。最终sort key为 `(isBuy, Asset)`：**所有Sell先发，Buy后发，同侧按枚举序**。但adapter不等待Sell filled再提交Buy，买力也不假设Sell filled。来源：`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/paper/execute.rs`。
6. `plan.target` 是按最终notional（含买侧缩放）推导的假设执行后组合；不包括wire qty截尾误差、实际成交价和未成交部分；不是Broker真实持仓。factor exposure基于此target。来源：`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/allocation.rs`。

### 5.2 wire quantity 和卖出数量边界

- OrderIntent只冻结asset/side/notional/limit/extended_hours；HTTP不是notional订单，而是 `qty=floor(saturating_i64(notional_microUSD×1e6)/limit_microUSD)`，格式固定六位小数；≤0返回ZeroQuantity。limit同样六位小数格式化。`type=limit,time_in_force=day`固定，extended_hours显式布尔，client_order_id来自Commitment。来源：`crates/akzio-domain/src/execution/plan.rs`、`crates/akzio-execution/src/paper/protocol.rs`。
- **潜在边界缺口（源码推导，未运行）**：卖单qty同样notional/limit，生成处未用`position.quantity_micros`作上限。全清仓delta按market_value，但保护卖价通常低于该市值对应的估值价，故请求数量可能大于持仓。例如已持10股、市值$1000、bid99.99/ask100.01、目标0：sell limit99.90，qty10.010010股，超过10股。此例仅代入当前公式；是否Broker拒绝、是否有其他运行前提使其不可达均未验证，**不能称已安全完成全清仓**。来源：`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/paper/protocol.rs`。后续Outcome数量重建会拒绝负仓，但那不是请求前数量裁剪：`crates/akzio-learning/src/evaluation.rs`。
- price/qty舍入造成的微小差异不会回写Decision/plan；极小notional可能在Gate/Commitment后才遇到ZeroQuantity。该错误属于adapter Err，既有Accepted不会变成filled或自动NoOrder。来源同上及`crates/akzio-execution/src/paper/reconcile.rs`。

## 6. Commitment、确定性client ID、approval消费与Store事务

### 6.1 Commitment前验证

NoOrder在daemon `commit()` 的Accepted模式匹配处即返回NoOutput，早于读取session和active_lease。Accepted才进入PaperCommitmentRuntime：Paper purpose、最新FreezeState、Accepted Verdict→ExecutionContext来源、complete plan closure、同Run/session、非frozen、Plan被source refs保留、plan hash、当前3x风险模型、approval仍有效、Buy总额不超过approval。源码：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-execution/src/paper_commitment.rs`。

`ExecutionContext.validate_complete_plan_closure`强制三快照、plan、factor、turnover、plan_hash、broker_session齐备，mandate permitted、pretrade permits_execution、!frozen。不是Accepted枚举的kind校验就足够。源码：`crates/akzio-domain/src/execution/plan.rs`。

### 6.2 确定性到底覆盖什么

- Plan哈希固定投影包含schema、DecisionContext及三snapshot refs、policy hash、买侧额度、target/orders、gross/net/factors/turnover、broker_session、created_at，不含自身plan_hash。所以重新生成时间/快照不同的plan并非同一哈希；恢复应重用已落盘原plan。源码：`crates/akzio-domain/src/execution/plan.rs`。
- r0 ID：`identity = ContentHash::of_bytes(session + NUL + plan_hash + NUL + order_index)`，取hash前16字符，输出`akzio-<prefix>-<index>-r0`。按排序后orders枚举，每资产唯一；r1只换后缀，不无限递增。源码：`crates/akzio-execution/src/paper/protocol.rs`、`crates/akzio-execution/src/paper_commitment.rs`。
- **不是整个Commitment对象都是确定性的**：`PaperCommitmentId::new()`和created_at是新建字段；防重复依赖session slot、精确context/plan/client IDs，以及既有payload恢复，不应声称commitment UUID也是hash导出。源码：`crates/akzio-execution/src/paper_commitment.rs`。
- 重复slot只能复用同context/hash/session/clientIDs；不同计划报ExistingCommitmentMismatch / DuplicateExecutionCommitment，不覆盖历史。源码：`crates/akzio-execution/src/paper_commitment.rs`、`crates/akzio-store/src/store/execution.rs`。

### 6.3 approval消费比Commitment更早

approved session reservation先验证Manifest/Approval source/hash/日期/期限；随后同一事务写graph、setup、slot和`rebuild_paper_approval_consumptions`。表的approval_artifact_id是PRIMARY KEY、session_key UNIQUE，即一份approval只消费一次、一个session只一个binding。**不是在POST或Commitment才消费token**；即使后续NoOrder，已批准reservation的消费事实不会自动退回。cold-start binding=None则没有consumption。来源：`crates/akzio-store/src/store/lease.rs`、`crates/akzio-store/src/store/impl_workflow.rs`、`crates/akzio-store/src/store/free_validation.rs`。

`paper_approval_for_run`不是取“最新全局approval”，而是 Run→slot→consumption→immutable Manifest/Approval，重验hash。Commitment事务再确认已消费授权对应session、日期期限、买入金额。来源：`crates/akzio-store/src/store/lease.rs`、`crates/akzio-store/src/store/impl_workflow.rs`。

### 6.4 Store原子性与fencing

`commit_execution`使用SQLite IMMEDIATE transaction；重验daemon owner/epoch/expiry、task permit、Paper purpose、origin，沿Commitment→Accepted Verdict→Context→Plan全量精确source refs核对；slot必须属于同Run且无其他commitment。首次写入在一个事务中完成Artifact、slot commitment、attempt output index、ExecutionCommitted事件、task/attempt succeeded。Commitment返回之后才能进入下一task。来源：`crates/akzio-store/src/store/execution.rs`、`crates/akzio-store/src/store/execution.rs`、`crates/akzio-store/src/store/impl_workflow.rs`。

Task permit比对Run/status=running/leaseID/epoch/activeAttempt/contractHash，以及实际`Utc::now()`时lease_until仍有效；daemon fence比对owner+epoch+expiry。**SQLite事务不覆盖HTTP**，有效lease检查也不是跨系统原子锁。来源：`crates/akzio-store/src/store/free_lifecycle.rs`、`crates/akzio-store/src/store/lease.rs`。

## 7. Dispatch、短发送窗口、HTTP与重启防重复

### 7.1 Dispatch请求前持久边界

Reconcile先检查Paper purpose和NoOrder；NoOrder立即NoOutput。Accepted需从成功祖先输出取Commitment；Debug forbidden先暂停/记录阻断并Deferred；再拿已注入Paper broker和scheduler lease。来源：`crates/akzio-daemon/src/application/paper_execution.rs`。

Dispatch再次检查Paper/Debug policy、拒绝历史SimulatedOnly；从Store确认该Commitment是slot中那一份，Context/Plan与Run/session/hash一致；构建授权，检查FreezeState、lease、permit，先写effect intent，再调用broker.execute_commitment。来源：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs`。

`record_paper_effect_intent`在IMMEDIATE事务内再次assert lease/permit/Paper/Debug/Artifact/events；以Run+effect Artifact查唯一intent。已有未终结intent返回recovered=true；已有terminal则PaperEffectAlreadySettled；新intent写`ExecutionEffectIntent`。**只写事件，不结束task**（旧doc comment说terminally completes，与函数体不符）。来源：`crates/akzio-store/src/store/execution.rs`。事件验证要求唯一intent、至多一个且更晚的terminal：`crates/akzio-store/src/store/free_lifecycle.rs`。

### 7.2 临时发送授权不延长冻结计划寿命

授权窗口取交集：

```text
start = max(Decision.generated_at, 每个观察时间 − allowed future skew)
end   = min(Decision.valid_until,
            manifest.expires_at, approval.expires_at, qualification.expires_at,
            frozen session.ends_at,
            account/quote/clock/各订单quote时间 + 各自最大年龄,
            每个account/positions/open_orders/fills组件时间 + account最大年龄)
```

plan_hash必须相同、执行policy_hash必须相同、snapshot broker_session相同、clock tradable。缺validity/approval、失配、缺account来源或交集为空，生成window=None；该对象仍可用于lookup，但不允许POST/PATCH。来源：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper/submission_authorization.rs`、`crates/akzio-execution/src/paper/submission_authorization.rs`。

窗口按代码为含端点`start<=now<=end`；市场session区间另外是半开区间，POST前还重取市场session。因此“到边界失效”不能仅用一个包含/不包含比较替代全部检查。原plan不在Dispatch重新取Account/Quote延长授权；Closed defer后的**ExecutionGate重试**才是全量刷新路径。

### 7.3 首发和重启都先查订单

1. `validate_commitment`在任何Broker I/O前重算plan/hash/session/订单数/每个r0 ID。
2. **第一遍扫描全部plan.orders**，每单先GET r1 client ID，再GET r0，已有返回标reused。即使window过期也允许lookup。
3. 第二遍仅对None考虑POST：assert_current→GET最新clock/calendar、核对frozen session kind/date及extended_hours；Overnight额外GET资格；再assert_current→一次POST。Sell在前，但不等Sell成交。
4. 若已观察至少一个订单而后续授权失效/MarketClosed/InvalidCommitment，停止补发并返回已知subset，不伪造缺单receipt；若一个订单都没有，则返回该Error。其他网络错误直接Err，下一次合法恢复应依同ID重新查。

来源：`crates/akzio-execution/src/paper/reconcile.rs`、`crates/akzio-execution/src/paper/execute.rs`、`crates/akzio-execution/src/paper/reconcile.rs`。

POST只到`/v2/orders`；PATCH只改变frozen limit与r1 ID；DELETE按durable cancel intent的broker ID。写请求不自动重试；HTTP非2xx是Error，**HTTP reject不等于已经有一个`OrderReceiptState::Rejected`的终态订单**。真正receipt解析要求client ID完全相同、id/symbol/status/qty/filled_qty/updated_at存在，decimal最多六位，remaining=requested−filled不能负，均价可选但随后Domain按状态校验。来源：`crates/akzio-execution/src/paper/protocol.rs`、`crates/akzio-execution/src/paper/transport.rs`、`crates/akzio-execution/src/paper/reconcile.rs`。

### 7.4 防重复 ≠ exactly-once保证，也 ≠ 自动完成

| 中断点 | 源码可证明的恢复/保留语义 | 仍有的限制 |
|---|---|---|
| Commitment事务前 | 尚无durable commitment；正常路径不能得到可dispatch的slot承诺 | 不能声称该次已产生订单 |
| Commitment已提交，Reconcile尚未领取 | graph task结果及slot已原子保存，后续使用同一plan/IDs | 发送窗口可能已经过期 |
| effect intent后、HTTP前 | intent留存；未来合法dispatch返回recovered并lookup同ID | 默认Reconcile崩溃恢复可能已耗尽一次预算，见下一段 |
| POST成功但响应丢失/本地未保存receipt | 未来先查r1/r0，不直接换ID重发；传输层不自动POST重试 | 真正Broker去重/可见性时序未调用官方API验证；不是本地事务能证明远端exactly-once |
| 部分订单已可见，其余未发且window过期 | 保留subset回执，缺单不补发 | 不满足全量receipt count，可能长期Pending/Partial；没有自动把未发单变NoOrder/取消receipt |
| 正常pending轮询结束 | progress保留；Deferred closes attempt为deferred，queued ≥1s，无失败预算消耗 | 后续仍依赖Core继续运行、合法claim和broker读成功 |
| Complete原子commit后 | output/task/effect terminal一致；正常调度不再领取成功节点 | 直接再调用同effect登记会AlreadySettled，不是重新执行 |

来源：`crates/akzio-store/src/store/execution.rs`、`crates/akzio-store/src/store/execution.rs`、`crates/akzio-store/src/store/execution.rs`、`crates/akzio-execution/src/paper/execute.rs`、`crates/akzio-store/src/store/workflow/commits.rs`。

**当前默认自动恢复缺口（高重要性，源码推导）**：Reconcile recipe默认`RetryPolicy::none()`=1次，wall time30s。Broker/PaperDispatch错误未被daemon错误分类映射成可重试项；TaskRuntime超时虽返回Retry(Timeout)，但该recipe.retry_transport=false，最后Failed。崩溃后`recover_expired_tasks`以running/retried/failed/abandoned计数，当前running也计1，条件`attempts < max_attempts`不成立，收束Failed。故“intent在Store，重启一定自动继续Reconcile”与当前默认调度不相符。已有slot也不会新建另一个Run；通用operator retry明确拒绝Paper Run。尚未验证是否另有获准的专门修复流程能恢复这个终止状态，不建议绕过Store修改状态。来源：`crates/akzio-runtime/src/runtime/catalogue.rs`、`crates/akzio-domain/src/core.rs`、`crates/akzio-daemon/src/lib.rs`、`crates/akzio-runtime/src/runtime/task.rs`、`crates/akzio-runtime/src/runtime/task.rs`、`crates/akzio-store/src/store/workflow/helpers.rs`、`crates/akzio-store/src/store/workflow/outputs.rs`、`crates/akzio-daemon/src/orchestration/control.rs`。

只读`WorkflowRuntime.recover/replay`读取journal/graph/checkpoint并检验一致性，**不会因为调用名叫recover就恢复Broker执行权限**。来源：`crates/akzio-runtime/src/runtime/replay.rs`、`crates/akzio-store/src/store/run_control.rs`。

## 8. Reconcile：每种状态、Regular60s、扩展时段及只读边界

### 8.1 adapter status → Domain → 对账状态

| Broker status | Domain | 单订单是否无后继终态 / 对账含义 |
|---|---|---|
| new / accepted / pending_new / accepted_for_bidding | Accepted | 否；要求零filled、remaining=requested、均价None |
| partially_filled | PartiallyFilled | 否；要求filled>0、remaining>0、正均价；通常对账Partial |
| filled | Filled | 是；必须filled=requested、remaining=0、均价存在 |
| pending_cancel / pending_replace | 对应Pending状态 | 否；不能按请求已送出推断cancel/replace成功 |
| canceled / expired / rejected / failed | 对应状态 | 是；可以保留已有partial fills；全体终态且successor闭合时对账Complete |
| replaced | Replaced | 否；需要durable r1 successor被观察并终态 |
| done_for_day / stopped / suspended / calculated | 对应状态 | 否；继续可能有更新；不是Complete |
| 未知status | UnsupportedReceiptStatus Error | 不猜测，不默认成功/失败 |

状态映射：`crates/akzio-execution/src/paper.rs`；数量和终态约束：`crates/akzio-domain/src/execution/effects.rs`、`crates/akzio-domain/src/execution/effects.rs`；整体Complete/Partial/Pending：`crates/akzio-execution/src/reconciliation.rs`。

`ReconciliationState::Failed`虽然在schema里存在，当前`reconciliation_state_with_reprices`不产生它；单个rejected/failed且其他订单也终态可产生**Complete**，不意味着fill成功。Partial是按存在PartiallyFilled/Filled状态派生，不是任意`filled_quantity>0`都必然显示Partial（如仍缺别的订单而可见的是partial后canceled，可能仍Pending）。来源同上。

### 8.2 读回执与数量闭包

`reconcile_committed`只对已知receipts逐单GET broker order ID，核对plan_hash和原/r1 client ID；不为缺资产补发。短轮询必须覆盖commitment全部资产且无重复/错误client ID，终态不足只返回未settled。源码：`crates/akzio-execution/src/paper/execute.rs`、`crates/akzio-execution/src/paper_dispatch.rs`。

ReconciliationRuntime读取durable reprice/cancel索引，要求同一commitment/asset及精确Artifact；每资产最多一个最终输入receipt，plan_hash和clientID必须闭合。replacement receipt若requested等于原requested，要求累计filled不倒退；若等于prior.remaining，则将两段filled相加、检查不超原量并数量加权平均价格；其他数量语义拒绝。源码：`crates/akzio-execution/src/reconciliation.rs`、`crates/akzio-execution/src/reconciliation.rs`、`crates/akzio-execution/src/reconciliation.rs`。

存在durable reprice但successor一直不可见时，即使r0已canceled/filled，也不能证明r1未到Broker，因此不Complete。这是fail-closed的不确定状态保留，不是自动补单授权。来源：`crates/akzio-execution/src/reconciliation.rs`。

### 8.3 Regular60s实际动作

短轮询后仍未settled、**整个plan没有extended_hours订单**、至少一个可cancel状态距broker_updated_at≥60s，才进入stale action。先write_progress，再操作，不先把任务标成功。可cancel集合为Accepted/PartiallyFilled/DoneForDay/Stopped/Suspended/Calculated；pending_cancel/pending_replace/replaced不进入这轮新动作。来源：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper_dispatch.rs`。

- 已有cancel intent不再创建新的动作。
- Accepted/PartiallyFilled、reprice_count<1、无现成reprice、**原发送窗口仍有效**才创建r1；新limit复用frozen plan.price，不取新报价追价。
- 其他符合stale的分支创建cancel intent，reason=SettlementTimeout；先保存canonical action Artifact+commitment/asset唯一索引，再record effect intent，最后PATCH/DELETE。
- action之后只做零timeout的一轮刷新，再计算Complete/进度。

来源：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-store/src/store/execution.rs`。

**默认5s窗口与60s阈值的组合**：在正常broker时间与本机时间关系下，最初观察+5s远早于订单更新后60s；到stale时通常已无POST/PATCH资格，所以真实默认更可能直接cancel，而非执行r1。不能把“支持一次reprice”写成“生产默认60秒一定改一次价”。这是公式/控制流推导，不是运行统计。来源：`crates/akzio-execution/src/lib.rs`、`crates/akzio-execution/src/paper/submission_authorization.rs`、`crates/akzio-execution/src/paper_dispatch.rs`。

扩展时段的判断依据是**frozen plan.orders.extended_hours**，不是每次对账当前clock；扩展day订单跨时段仍跳过Regular60s分支，只保存进度等待Broker终态。源码：`crates/akzio-execution/src/paper_dispatch.rs`。

### 8.4 Cancel/reprice恢复与“只读”到底指什么

- cancel：先lookup并校验broker ID；terminal或pending_cancel直接返回reused；replaced拒绝，必须通过successor取消。DELETE遇404/422仍GET确认；其他错误返回。没有发送window要求，属于risk-reduction，但**仍是Broker写入**。源码：`crates/akzio-execution/src/paper/reconcile.rs`。
- reprice：先lookup r1，已有则reused不PATCH；否则assert原window、lookup r0，r0终态或pending_replace/replaced而r1不见则拒绝，不盲目重复；再assert window后PATCH。源码：`crates/akzio-execution/src/paper/reconcile.rs`。
- action恢复重新record intent；AlreadySettled跳过；reprice SubmissionUnauthorized保留不确定intent并允许后续cancel路径；返回已知receipt后才settle action effect。action effect settled只表示该动作已观测/处理，不等于整个commitment成交；cancel返回pending_cancel也可先settle动作intent，订单仍继续pending。源码：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-store/src/store/execution.rs`。
- **只读恢复分三层**：①workflow replay只读本地Store；②`reconcile_committed`只GET已知订单；③完整`dispatch`不是只读，若还在window会补发缺单，也可能创建cancel/reprice。授权过期不禁止lookup，但不代表freeze或Debug forbidden时完整dispatch仍可跑。Freeze检查和Debug guard在lookup前，当前完整Reconcile路径会整体被挡。不能把文档“已提交订单只读对账继续可用”泛化到所有冻结/禁写场景。源码：`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。

### 8.5 持久进度与终态

Pending/Partial只逐个fenced写Receipt/Reconciliation中间Artifact，不发布成功attempt_outputs；Complete才通过`commit_fenced_attempt_with_effect`在同一事务写正式outputs、task/attempt终态和commitment effect终结事件。最终daemon看settled=true才返回Committed，false则Utc::now()+1s Deferred。来源：`crates/akzio-execution/src/reconciliation.rs`、`crates/akzio-store/src/store/execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。下游terminal_input只取成功祖先outputs且必须唯一，不随意拿最新中间Artifact：`crates/akzio-daemon/src/dispatch.rs`、`crates/akzio-store/src/store/workflow/outputs.rs`。

Reconciliation的`achieved_target`按冻结account.market_value ± filled_qty×fill_avg_price，再除原equity，不再GET完整account/positions，也不是后续真实NAV；这里与后续Outcome按quantity+cash重建是不同算法。来源：`crates/akzio-execution/src/reconciliation.rs`；Outcome数量/现金：`crates/akzio-learning/src/evaluation.rs`、`crates/akzio-learning/src/evaluation.rs`。

## 9. 冷启动、PositionPlan与Evaluate入口

### 9.1 无Policy、无approval为何仍能NoOrder完成

1. CLI从SQL active head载入DecisionPolicy；无active head返回默认policy并标`store_active_head_missing`，不是自动生成/激活候选。默认active_forecast_calibration=None、asset_calibrations空、risk sample_count=0、min_calibration_samples=u32::MAX。每资产校准缺失退出eligible，eligible空返回zeroed target。来源：`crates/akzio-cli/src/cli/identity.rs`、`crates/akzio-execution/src/decision_gate.rs`、`crates/akzio-execution/src/decision_gate.rs`、`crates/akzio-execution/src/decision_gate.rs`。
2. 正式scheduler无active policy时强制binding=None，**即使Store已有旧approval也不绑定**；有policy才要求approval。此分支用于未来真实Outcome标签积累，不等于开交易权限。来源：`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`、`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`。
3. 研究仍可形成非零合格recommendation，与零正式target分开保存；ExecutionGate缺approval加UnqualifiedRuntime，前置blockers非空不进allocator，pretrade没有approval时None、不宣称安全。来源：`crates/akzio-execution/src/decision_gate/decide.rs`、`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`。
4. Gate持久Context+NoOrder，不要求complete_plan_closure；PaperCommit和Reconcile均在取执行lease/broker前NoOutput。之后Evaluate走NoOrder lineage，不需要Commitment/Reconciliation。来源：`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/application/outcome_sealing.rs`。
5. **“零写入”只准确到零Broker交易写入。**真实cold start仍会研究请求、执行快照GET、Store图/Need/evidence/Decision/Context/Verdict/任务/OutcomeSchedule等写入，scheduler预约也用lease；不是零Store、零网络、零任何lease。Closed期间可能先Deferred，获取/完整性错误也可能Failed；只有这些技术前提通过才有持久NoOrder。
6. 若已有有效approval且全部前置gate通过，零Decision target+空仓会allocation NoExecutableOrder；零target+已有多头可尝试归零Sell但还受qty、turnover、mandate、quote等限制。**无approval的cold start绝不会因target=0而自动进入allocator清仓。**来源：`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/execution_gate/core.rs`。

### 9.2 PositionPlan的根本边界

图编译在Decision后return，没有ExecutionGate/PaperCommit/Reconcile/Evaluate，因此也没有Paper OutcomeSchedule；Debug prepare PositionPlan不占Paper slot。独立入口即使被误调用，ExecutionGate加NonCanonicalRun、Commitment/Dispatch拒绝非Paper，Outcome入口非Paper也NoOutput（Shadow另支）。来源：`crates/akzio-runtime/src/runtime/compilation/evidence.rs`、`crates/akzio-daemon/src/debug.rs`、`crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-execution/src/paper_commitment.rs`、`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-daemon/src/application/outcome_sealing.rs`。

文档还区分正式PositionPlan缺policy仍可零target结束，与隔离Debug缺policy只允许手动研究、Decision不放行；不能互相套用。来源：`docs/debug-control.md`、`crates/akzio-daemon/src/debug.rs`。

### 9.3 Evaluate不是立即评估成交后的真实收益

- 读取本Run成功祖先Decision、DecisionContext、ExecutionContext、Verdict。NoOrder只要求NoOrder verdict与context绑定；Accepted还需Commitment及**Complete Reconciliation**。来源：`crates/akzio-daemon/src/application/outcome_sealing.rs`、`crates/akzio-learning/src/outcome_schedule.rs`、`crates/akzio-learning/src/outcome_schedule.rs`。
- baseline_trading_day来自scheduler slot.session_key，不是当前UTC日期；生成Canonical OutcomeSchedule，冻结完整执行来源。来源：`crates/akzio-daemon/src/outcome/collection.rs`、`crates/akzio-learning/src/outcome_schedule.rs`。
- OutcomeSchedulingRuntime构造默认enqueue_worker=false；**production bootstrap显式with_worker_enabled(true)**，即使当前processing disabled也保存未来worker。开启processing且有Alpaca adapter才恢复/领取待评估worker。commit schedule时可将schedule、成功attempt和post-terminal worker同事务提交。来源：`crates/akzio-learning/src/outcome_schedule.rs`、`crates/akzio-learning/src/outcome_schedule.rs`、`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- 后续Outcome会重建quantity+cash、逐clientID去重、验证全量终态receipts，拒绝负持仓；NoOrder不允许夹带plan/fills。零订单仍评估预测，不意味着已有持仓被清零。跨T+1/T+3/T+5共同完成Session与后续学习资格是另一条时间轴。本次仅核查入口和相关重建边界，未执行跨日Outcome。来源：`crates/akzio-learning/src/evaluation.rs`、`crates/akzio-learning/src/evaluation.rs`、`docs/agent-runtime-contract.md`、`docs/agent-runtime-contract.md`。

## 10. Paper endpoint硬拒绝与Debug forbidden独立控制

**Paper-only endpoint**：`AlpacaPaper::new`先调用`is_alpaca_paper_base_url`，不通过立刻NonPaperEndpoint，然后才build HTTP client。解析条件：scheme=https、host精确paper-api.alpaca.markets、parsed port=None、无userinfo/password、path=/、无query/fragment；构造后保存固定Paper base，redirect禁用，没有localhost例外或Live fallback。默认env缺base URL时使用Paper固定地址，存在非法值则拒绝；本次只读这些代码，未读取env值。源码：`crates/akzio-execution/src/paper.rs`、`crates/akzio-execution/src/paper/execute.rs`。这是parsed URL条件，不把源码注释“无端口”扩大成未经验证的任意原始URL字符串规则。

**Debug policy与交易Gate独立**：prepare时broker policy默认为Forbidden，只有explicit paper_allowed为PaperAllowed，learning_scope恒Isolated；该身份持久且原session复用要求runtime identity与paper_allowed一致。源码：`crates/akzio-daemon/src/debug.rs`、`crates/akzio-daemon/src/debug.rs`。

三层真正阻断点：

1. daemon Reconcile `block_debug_broker_task`：before broker getter/lease；PauseRequested、清permit、记录Blocked验收，再Deferred。
2. Dispatch `assert_debug_broker_write`：before load/authorization/effect/HTTP；legacy SimulatedOnly明确退役。
3. Store `record_paper_effect_intent`：事务内再assert_broker_write，覆盖commitment/cancel/reprice。只有已有Debug session且PaperAllowed才放行；隔离环境中缺session也拒绝。普通非隔离Run无Debug session不受该额外限制，但仍需原Gate。

来源：`crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-store/src/store/debug.rs`、`crates/akzio-store/src/store/debug.rs`、`crates/akzio-store/src/store/execution.rs`。

PaperAllowed只解除Debug禁写，不提供approval/policy/qualification/quotes或资金。Debug runtime identity变化，除Pause/Abort外控制操作拒绝；隔离Store不能通过重启变canonical学习来源。源码/规则：`crates/akzio-store/src/store/debug.rs`、`docs/debug-control.md`、`docs/debug-control.md`。

注意：Debug forbidden不等于“没有任何Store写入”，也不在当前Commitment runtime里作为前置检查；可先有Accepted和durable Commitment，随后在Reconcile/effect边界阻断。源码：`crates/akzio-daemon/src/application/paper_execution.rs`。

## 11. 文档、注释与实现差异 / 不能过度承诺的事项

| 项目 | 文档/容易形成的理解 | 当前代码结论与证据 |
|---|---|---|
| T0触发时间 | 契约3节“每个交易日开市时”容易被理解成仅Regular开市 | 实际scheduler open_session_key接受任何非Closed的细分session，含前夜Overnight；按trade_date唯一slot。`docs/agent-runtime-contract.md` 对照 `crates/akzio-daemon/src/scheduler.rs` |
| Commitment之后HTTP | 图式简写Commitment→API容易认为PaperCommit发单 | 实际PaperCommit不HTTP，下一Reconcile task才Dispatch。`docs/agent-runtime-contract.md` 对照 `crates/akzio-daemon/src/application/paper_execution.rs` |
| approval消费 | 容易认为是Commitment时消费 | 已批准reservation就消费，Commitment重验已消费binding。`crates/akzio-store/src/store/impl_workflow.rs`、`crates/akzio-store/src/store/execution.rs` |
| 冷启动“零写入/无lease” | 把零交易写入等同零Store/零GET/零scheduler lease | 执行快照先刷新并落Store，NoOrder后短路的只是PaperCommit/Reconcile的执行lease与Broker交易请求。`docs/development-workflow.md` 对照 `crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-daemon/src/scheduler/scheduler_tick.rs` |
| 严格自动崩溃恢复 | 契约“依据相同ID幂等恢复”未展示调度预算限制 | ID恢复机制存在，但默认Reconcile一次预算，crash/transport/outer timeout可直接Failed，不能保证重启自动完成。`docs/agent-runtime-contract.md`；详见7.4的recipe/counter/fencing源码 |
| 60秒撤改单 | 容易理解默认会先reprice | 60s是broker_updated_at后的阈值；默认5s frozen窗口通常已过期，reprice不授权，走cancel。`docs/agent-runtime-contract.md` 对照 `crates/akzio-execution/src/paper_dispatch.rs`、`crates/akzio-execution/src/paper/submission_authorization.rs` |
| 只读对账继续可用 | 容易扩展为Frozen/Debug forbidden也能正常完整Reconcile | 仅adapter lookup/reconcile不要求发送window；完整Dispatch在这些guard前置阻断，也可能包含DELETE，不能称纯只读。`docs/agent-runtime-contract.md` 对照 `crates/akzio-execution/src/paper_dispatch.rs` |
| IEX/overnight可执行 | 看到overnight feed被支持就认为可执行 | 捕获和schema支持不等于依赖Gate允许；当前仅SIP/BOATS健康，IEX/overnight连纯减仓也被依赖Gate挡。`docs/agent-runtime-contract.md` 对照 `crates/akzio-daemon/src/application/paper_execution.rs`、`crates/akzio-domain/src/market_safety.rs` |
| 全部Execution失败都NoOrder | Gate模块头注释“every rejection durable NoOrder”过宽 | bad input/provenance/serde/Store/多数acquisition失败为Err，不是NoOrder；只有business blocker转换。`crates/akzio-domain/src/execution/effects.rs` 对照 `crates/akzio-execution/src/execution_gate/core.rs`、`crates/akzio-daemon/src/evidence.rs` |
| effect intent终结task | Store旧doc comment说terminally completes | 实现仅intent event；终结要等Complete的fenced attempt-with-effect。`crates/akzio-store/src/store/execution.rs` 对照 `crates/akzio-store/src/store/execution.rs` |
| Complete=filled | T0执行链结束容易被当作成交验收 | canceled/expired/rejected/failed亦为无后继终态，可以Complete，甚至零fill；OutcomeSchedule不证明成交。`crates/akzio-domain/src/execution/effects.rs`、`crates/akzio-execution/src/reconciliation.rs` |
| 执行后持仓全程按quantity重建 | 契约11描述quantity/cash正确适用于Outcome | Reconciliation achieved_target仍按原market_value±成交美元额；Outcome才quantity/cash重建。`docs/agent-runtime-contract.md` 对照 `crates/akzio-execution/src/reconciliation.rs`、`crates/akzio-learning/src/evaluation.rs` |
| qty已被执行前持仓限制 | 仅凭short禁止/默认long-only推断卖单不会超持仓 | allocation未用quantity作Sell cap，按market_value/保护limit可能超股数；需独立回归，不能用Outcome事后拒绝替代。`crates/akzio-execution/src/allocation.rs`、`crates/akzio-execution/src/paper/protocol.rs` |
| 30秒HTTP超时 vs 15秒poll | “短轮询15秒”不等于整个task<=15秒 | 每GET可能30秒、get_json可5次transport尝试，Reconcile task整体30秒可更早取消future；未返回不等于远端未发生。`crates/akzio-execution/src/paper/execute.rs`、`crates/akzio-execution/src/paper/transport.rs`、`crates/akzio-runtime/src/runtime/task.rs` |

上表部分是文档概述需要补充限定，不一概指“实现违约”；其中默认崩溃恢复、Sell qty、freeze下只读入口是尤其需要后续独立验证的边界。此次只读请求不修复它们。

## 12. 未确认点与后续证据需求（未执行）

1. **运行配置与真实状态**：当前daemon实际policy/approval、runtime identity、account、positions/orders/fills、quote feed entitlement、kill switch、Debug身份均未读；不知道当前是否具备一次真实Accepted条件。缺policy/approval的解释是条件性源码流程，非“已确认用户canonical Store缺这些”。
2. **Broker外部语义**：未调用API、未查外部文档。本报告只陈述实现如何编码/解析；没有验证Alpaca当前Paper对overnight、fractional limit/day、精度、重复clientID、reprice累计qty、HTTP404/422可见性时序的实际行为。
3. **Crash/retry liveness**：同ID防重复与默认recipe一次预算之间的恢复缺口，需要受控离线故障注入验证；不能用fixture pass代替真实Paper重启恢复。未启动诊断failpoint。
4. **发送时序**：六项刷新、Store提交、两轮lookup、每单clock/calendar在5s原窗口内是否可完成，需实际延迟证据；仅源码不能宣称能及时发完全部单。部分提交后剩余缺单、授权到期不Complete的生命周期需专门验收。
5. **数量及估值**：Sell qty>held shares的代数边界、wire截尾与plan目标差异、repricing predecessor在intent冻结后新增fill的数量合并、Reconciliation美元额投影与Outcome数量/现金投影偏差，需独立case验证。
6. **身份和即时控制范围**：本次查到启动/scheduler身份核验和Debug identity guard，但在标准Dispatch路径未看到逐请求再次比较当前完整daemon RuntimeIdentity或重新核对account ID；Freeze只在入口检查且不自动cancel。不能从“身份字段存在”推导没有检查间竞态。
7. **Outcome成熟与校准资格**：只确认Evaluate入口和NoOrder lineage合法；四资产真实baseline/后续共同Session、风险真值、叙事、样本资格与inspect/validate/activate均未运行。隔离Debug不能计canonical样本。
8. **测试证据等级**：已阅读现有源码内的若干断言，不代表本次测试通过：cold-start reservation `crates/akzio-daemon/src/scheduler/lease.rs`；Closed wait `crates/akzio-daemon/src/application/paper_execution.rs`；full-commitment coverage `crates/akzio-execution/src/paper_dispatch.rs`；未知successor不终结 `crates/akzio-execution/src/reconciliation.rs`；session/calendar与overnight flag案例 `crates/akzio-domain/src/execution/session.rs`。

## 13. 静态验收与可复核锚点

- 原始核查使用只读 `git status --short`、`rg` 与源码阅读；当前报告另作来源文件、链接与格式静态检查。`git diff --check` 不覆盖未跟踪报告，也不证明业务行为。
- 报告的来源文件存在性、链接、代码围栏与空白格式需要按当前版本单独复核；不再以可移动的源码位置作为交付保证。四个关键源码指纹在交付前复查一致；前后Git状态列表一致。这里只确认报告引用的机械有效性，不将其当作编译、测试或运行正确性的证明。
- 未运行构建、单元/集成测试、fixture、Store Doctor、daemon/API/Broker或模型。报告静态验证不能标成执行流程offline-verified，更不能标成real-Paper-verified。
- 关键源码SHA-256（本次读取快照）：
  - `crates/akzio-execution/src/paper_dispatch.rs` — `61d3135f440191ceb3423b3deb950a7bbd260ce0e08b43e8254ac9b10241884b`
  - `crates/akzio-daemon/src/application/paper_execution.rs` — `726d7c9c213b3a370a975acbc2a6d35dd4843afd791d773581e2a3fa7fa15eef`
  - `crates/akzio-store/src/store/execution.rs` — `149806ec2ab4be0557c968425d51abb63e569693890ad11f109597390a1aa239`
  - `crates/akzio-domain/src/execution/effects.rs` — `cc1f9b37821aa5a1799b7eb6b9d4dfefa0383c592387060454212bc0573728e6`
- 本报告是唯一交付文件：`.akzio/reports/process-map-20260929-01/04-paper-execution.md`。未提交Git。


---

<!-- 分报告 5: 05-outcome-learning.md -->

# Outcome → 学习 → Lesson / Canary：当前工作树只读过程核查

核查日期：2026-09-29。仓库：`/Users/alixeu/project/akzio-signal-intelligence`。

核查起点 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`。本文针对**当前工作树内容**，不是仅针对该提交；工作树已有未提交修改。开始与结束时检查了 `git status --short`，没有改动源码、配置或 Store。唯一新增交付为本文件。

## 0. 结论与证据边界

**现有代码实现的是分层、受控、可中断恢复的 Outcome/学习机制，而不是“运行完 T0 就自动学会并上线”。**

1. Paper 的 Evaluate 只冻结 `OutcomeSchedule`。NoOrder 也安排未来评估；Accepted 必须有完整对账血缘。PositionPlan 没有 Evaluate。
2. 后续 worker 在同一 Run 上、原研究图之外，按四资产共同完成的第一、第三、第五个交易 Session 推进。T0 的 completed 不等于 T5 completed。
3. Rust 用冻结 account、execution quote midpoint、唯一终态 receipts 和未来 raw close 计算数值；主路径 metric 为 `frozen_post_execution_exposure_v3`，不是实际后续账户 NAV。
4. 有数值、叙事 Complete、Experience 可学习、Policy 晋升、Lesson Active、Canary Advance、Paper 发单/成交是不同事实，不能互相替代。
5. 存在类型、计算函数或 Store 接口，不意味着自动生产链已经接通。本文特别列出风险真值生产、叙事 node 身份校验、Shadow 风险绑定、Canary 必需指标、Topology 消费、qualification/release/experiment 的实际接入范围。

**本次证据等级：源码/文档只读核查；未运行 Rust build/test、fixture、Core、模型、Broker、行情 API、Store doctor 或跨交易日任务。** 所有 SQL 均仅作为源码阅读，没有连接任何数据库。没有读取 canonical 数据库、token、API key 或用户本地配置。文中的公式是对源码的解释，不是账户、收益或运行验收报告。

已先读取 `AGENTS.md`、运行时契约第 3/7/8/11/12 节，并补读相关第 1/2/4/5/6 节、`CONTEXT.md`、development workflow 与 debug-control 对应内容。本次只描述已有代码，不提出改造。

### 0.1 当前版本与文档漂移

| 项目 | 当前源码 |
|---|---|
| Domain schema | 10 |
| Store schema | 18 |
| 当前研究 Contract / PromptBundle | 69 / 38 |
| Analyst freshness candidate | 70 |
| Outcome Contract / PromptBundle | 63 / 35 |
| 主路径 Outcome metric | frozen_post_execution_exposure_v3 |
| Experience evaluation context | 1 |
| Benchmark definition | 1 |

`CONTEXT.md` 的 “Versions and Verification” 仍写 Store 17、研究 67/37、candidate 68，后面的 12k 模型输入表述也不能代表新 Run 的实际预算。契约摘要与当前源码优先用于本次说明；**历史冻结 Contract/Run 继续使用自身已存版本，不能拿新默认值回写。**

证据：

- `crates/akzio-domain/src/schema.rs`
- `crates/akzio-store/src/store/prelude.rs`
- `crates/akzio-research/src/agent/catalogue.rs`
- `crates/akzio-research/src/agent/errors_catalogue.rs`
- `docs/agent-runtime-contract.md`
- `crates/akzio-domain/src/budget.rs`

## 1. 总流程与各层输出

```text
T0 Paper 原图
  Evidence → 研究/审查 → Decision → ExecutionGate
    ├─ NoOrder：没有 Commitment/fill；PaperCommit、Reconcile 走短路
    └─ Accepted：PaperCommitment → Broker → 完整 Reconciliation
  → Evaluate：冻结 OutcomeSchedule + 安装同 Run post-terminal worker
  → T0 Run completed

独立的历史 Outcome 队列
  → 最早未完成且实际到期的 T1 / T3 / T5
  → 治理采集 raw bars + 公司行动资料
  → 四资产共同 Session + 阶段 cutoff
  → Rust stage facts
  → Outcome AgentRuntime 的 Draft / 可选受控读 / Submit 协议
  → T1/T3：RunScoped Partial Outcome + Retrospective，defer
  → T5：
      ├─ 无合格叙事：数值 Outcome + ModelUnavailable，结束；不学习
      ├─ isolated debug：仅封存；不进入 canonical policy
      ├─ 普通 canonical：资格复核 → Experience/Evaluation
      └─ Canary parent：先封存，再等三个注册 Shadow，配对/评估

学习副线
  → PolicySubject 的 head / transition / pair-consumption cursor
  → DecisionContext 记录 applied/rejected 的 Lesson 的观察性 evidence
  → 模型提出的新 Lesson：Draft + quarantine，等待独立治理

明确的 repair
  → 原 Run 新 repair Task → 复用 sealed Outcome → 新叙事 revision
  → 原资格/消费事务；不重算历史数值，不直接写 Proven
```

这张图是**处理入口和状态分支**，不声称所有分支在当前工作树已经端到端运行成功。第 13 节列出静态联读发现的接线限制。

### 1.1 PositionPlan、Paper、Shadow 的图并不相同

- PositionPlan：研究节点外加 Evidence 和 Decision，Decision 后结束。
- Paper：再加 ExecutionGate、PaperCommit、Reconcile、Evaluate。
- Shadow：包含 ExecutionGate、Reconcile、Evaluate，**没有 PaperCommit**；不能把 Shadow 描述成只有研究节点，亦不能把其 ExecutionGate 当成 Paper 执行许可。
- 默认研究修订数 N=2 的同一研究主体有 `15+2N=19` 个研究节点，故 PositionPlan 21、Paper 25、从该主体 lower 的 Shadow 24；候选 topology 应按其实际冻结 graph 计数，不能把 24 泛化给所有历史/候选图。
- post-terminal `learning.outcome_worker` 不计入原冻结研究图的节点数。

证据：

- `crates/akzio-runtime/src/runtime/compilation/evidence.rs`
- `docs/agent-runtime-contract.md`
- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-execution/src/execution_gate/core.rs`

## 2. Evaluate：排期、NoOrder 血缘与 baseline

### 2.1 输入与两类 execution lineage

`OutcomeSealing::execute` 先读取 Store 中 Run purpose。Paper 以本 Run 已成功终态节点的 Decision、DecisionContext、ExecutionContext、ExecutionVerdict 为输入；Shadow 转独立 `execute_shadow_evaluate`，其余 purpose 返回 NoOutput。

- Verdict 为 NoOrder：冻结 `OutcomeExecutionLineage::NoOrder { execution_verdict }`，不要求 Commitment。
- Verdict 为 Accepted：冻结 `ReconciledPaper { execution_verdict, commitment, reconciliation }`。
- `OutcomeSchedulingRuntime::schedule` 复核 permit、Paper purpose、Decision→DecisionContext、ExecutionContext→DecisionContext、本 Run 身份及执行引用闭包。
- ReconciledPaper 进一步要求 Accepted verdict、对应 Commitment、`ReconciliationState::Complete`。完整对账是终态 lineage 条件，**不是每个订单必须全额成交**；最终取消且零成交、最终取消但部分成交等结果仍应按真实终态 receipts 描述。
- schedule 字段为新 outcome_id、上述冻结引用、baseline_trading_day、created_at；自身不含未来收益、未来行情或学习许可。

证据：

- `crates/akzio-daemon/src/application/outcome_sealing.rs`
- `crates/akzio-learning/src/outcome_schedule.rs`
- `crates/akzio-learning/src/outcome_schedule.rs`
- `crates/akzio-learning/src/outcome_schedule.rs`

**执行恢复边界：** Commitment 提供已持久化的确定性执行身份，不保证 Reconcile 自动恢复。当前 Reconcile recipe 使用 `RetryPolicy::none()`（max_attempts=1）；过期 running Attempt 被计入失败次数时，恢复分支要求 `attempts < max_attempts`，计数为 1 就可能走 `TaskRecoveryExhausted` 并失败。Outcome 只消费实际已完成的 Reconciliation，不替执行链补做恢复或把 pending 升格为 Complete。

证据：

- `crates/akzio-runtime/src/runtime/catalogue.rs`
- `crates/akzio-domain/src/core.rs`
- `crates/akzio-store/src/store/workflow/helpers.rs`
- `crates/akzio-store/src/store/workflow/outputs.rs`

### 2.2 “冻结 baseline”具体冻结什么

1. **交易日**来自该 Paper Run 的 scheduler SessionSlot `session_key`，按 `%Y-%m-%d` 解析；不是 worker 当前日期、UTC 自然日或模型给的日期。
2. **账户起点**沿 schedule.execution_context.account_snapshot 读取冻结 AccountSnapshot。
3. **计价起点**沿同一个 ExecutionContext 的 QuoteSnapshot 读取四资产 `midpoint=(bid+ask)/2`，不是 T0 日线 close。四资产均须存在且价格为正。
4. **成交来源**只沿冻结 Reconciliation 读取 ExecutionPlan 和 broker receipts，不查询当前账户代替 T0。
5. **NoOrder 不等于全现金**：若冻结账户本已有持仓，NoOrder 不应用新成交，但仍评估原有持仓的冻结价格敞口；只有原账户确为空仓时组合价格效应才为零。
6. 排期验证允许引用一个缺 optional account/quote 的 ExecutionContext；实际 collection/materialization 会明确报 baseline missing。**已有 OutcomeSchedule 不证明 baseline 已足够，等待也不能补回原 T0 缺失快照。**
7. 不把“baseline 冻结”扩展成所有参数都冻结：schedule 结构不存 OutcomeCostModel；普通 worker 从当前 Daemon 的 `self.paper.outcome_cost_model` 取费率。Canary 另有 cohort.cost_model 匹配检查。本文不据此断言所有历史阶段使用的成本参数相同。

证据：

- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-daemon/src/outcome/materialization.rs`
- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-domain/src/evaluation/outcome.rs`
- `docs/development-workflow.md`

## 3. post-terminal worker：耐久队列、隔离租约与 retry

### 3.1 创建与启动恢复

`OutcomeSchedulingRuntime::new` 的库默认 `enqueue_worker=false`，但**生产 bootstrap 明确用 `with_worker_enabled(true)`**；处理开关关闭时仍持久化未来工作，不要求另一次 T0 才能发现旧 Outcome。

`commit_outcome_schedule_with_worker` 在一个 Immediate 事务里提交 schedule、成功 Attempt、动态 worker task、`OutcomeWorkerEnqueued` event 与控制面唤醒。worker 没有原图 dependencies，priority=100，输入含 schedule、其 source_refs，以及同 Run 成功 Task/Attempt 对应的 DeliberationNote。Contract 优先取 Outcome catalogue head，budget 优先使用 Run graph 冻结的 agent_budgets。

启用处理需 `outcome_processing && 已配置 Alpaca evidence adapter`，**不以 auto_paper=true 为条件**。bootstrap 可补建最多 1000 个符合“Paper completed、schedule 是成功 output、无 worker、无最终 Outcome”的旧排期；它逐 Run 调用原提交接口，不是一次覆盖全批的事务。

证据：

- `crates/akzio-learning/src/outcome_schedule.rs`
- `crates/akzio-learning/src/outcome_schedule.rs`
- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-daemon/src/orchestration/bootstrap.rs`

### 3.2 为什么 T0 completed 后还可以继续

Task claim SQL 特许 `r.status='completed' AND recipe=learning.outcome_worker`；普通 Task 仍需 Run queued/running。Outcome worker 的终态处理不刷新已结束 Run 的状态，失败也不反向取消原研究图。因此可以同时观察到：

- Run completed；
- Outcome worker queued / deferred / failed；
- T1/T3 已持久化而 T5 未密封。

同样要保留细节：claim 还受 RunControl 的 running/stepping、取消标志、身份及 task lease 条件约束。“post-terminal”不是绕过控制面的后台特权。

证据：

- `crates/akzio-store/src/store/workflow/commits.rs`
- `crates/akzio-store/src/store/free_policy_helpers.rs`

### 3.3 两层租约与服务容量

| 层次 | 当前默认/行为 |
|---|---|
| TaskRuntime task lease | 30 秒；heartbeat/recovery tick 为 lease/3，默认约 10 秒 |
| 每 Outcome daemon lease | `akzio.local.outcome_worker:<run_id>:<outcome_id>`，5 分钟 |
| Outcome lease 争用 | DeferredUntil(now+30 秒)，不算失败 |
| 普通尚未成熟轮询 | now+20 分钟；只是调度时间，不是市场已完成证明 |
| 一次补跑已有多个成熟阶段 | T1/T3 提交后约 1 秒再领取下一阶段 |
| CLI 生产启动 worker_count | 未配置时 4 |
| fixture CLI / WorkerPoolConfig 库默认 | 2，不能拿来当生产 CLI 默认 |
| 多 worker 分配 | 第 0 个保留 Session，第 1 个保留 Outcome，其余 Any |
| 单 worker / Any | Session、Outcome 交替优先；空闲时可回退 Any |

OutcomeLeaseGuard 持有 Store 与 lease，正常完成、`?` 提前退出、Future 取消、panic unwind 均尝试 release；release 失败记录 warning，仍由 durable expiry/fencing 管理。进程硬终止不能依靠 Drop，仍需租约到期恢复。写入使用 task permit 与 Outcome lease 双重 fencing，不凭本地 Future 存活决定写权限。

证据：

- `crates/akzio-runtime/src/runtime/task.rs`
- `crates/akzio-runtime/src/runtime/task.rs`
- `crates/akzio-runtime/src/runtime/task.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`
- `crates/akzio-daemon/src/outcome/helpers.rs`
- `crates/akzio-daemon/src/worker.rs`
- `crates/akzio-cli/src/cli/run_commands.rs`

### 3.4 Deferred、失败 retry、模型内修复不是同一额度

- Deferred 把当前 Attempt 记成 deferred、Task 回 queued；不读也不消耗失败 retry budget。TaskRuntime 至少延后 1 秒，避免忙循环。
- 当前 Outcome Contract 的 `RetryPolicy` 为 max_attempts=2、initial_backoff_ms=250，transport/rate-limited/invalid-output 均允许重试，on_failure=FailTask。默认是**每阶段最多两次计入失败预算的尝试**，不是两次模型调用。
- Outcome 实际退避按当前阶段持久失败计数：`min(max(initial,30000) × 2^min(attempts-1,10),300000)` 毫秒。默认第一轮重试 30 秒，上限 5 分钟；不是每一阶段都实际重试到 5 分钟。
- 阶段失败计数以该 Task 最近已提交 `RetrospectiveCreated` 所属 Attempt rowid 为边界，统计其后的 running/retried/failed/abandoned；deferred 不计。SQL **没有再次按 horizon 过滤**，准确说是“提交阶段事件重置失败额度”。
- AgentRuntime 的 Retry/Recovery 又沿同 Run/Task 的 AttemptRelation 恢复调用、工具与用量；已发生但无法复原的 provider usage 保留 unknown 并 fail closed。阶段切换不能被描述成无限清空预算。
- 普通 Outcome 模型错误通常被 worker 转成 narrative diagnostic 后继续数值路径；Store 错误被重新抛出。因此“模型失败会触发任务 retry”不是普遍成立的规则。

证据：

- `crates/akzio-store/src/store/workflow/commits.rs`
- `crates/akzio-store/src/store/workflow/helpers.rs`
- `crates/akzio-runtime/src/runtime/task.rs`
- `crates/akzio-research/src/agent/errors_catalogue.rs`
- `crates/akzio-research/src/agent/recovery.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`

## 4. T1/T3/T5 的时间真值与迟到补跑

### 4.1 Session 对齐

collection 对每只 ETF 请求：

```text
bars:<symbol>:1d:<baseline>:252:raw:<min(market_day,baseline+366d)>
```

Need max_age_secs=604800 / 7 天。主实现的四次 asset acquisition 位于顺序 `for` 循环，不能依据文件导读中的 `join_all` 字样称它为并发采集。

流程：

1. 美东 market_day≤baseline 时直接等待，尤其覆盖 Overnight T0 属于下一交易日的情况。
2. adapter 从实际 exchange calendar 解析纽约时区 close；只保留 `close + 20分钟 <= cutoff` 的可用日线，支持 DST/提前收盘，不以下载时刻替代 close。
3. parser 拒绝重复日期；按四资产 bars 日期交集取严格晚于 baseline 的前 5 日。
4. T1/T3/T5 分别是交集下标 0/2/4；T3 是从 T0 到第三个共同 Session 的累计窗口，不是 T1 到 T3 的增量窗口。
5. 某只资产缺日期不会凭另一只资产或自然日补齐。搜索达到 252 根/366 天而仍不足 5 个共同日期时显式失败。

这里“共同完成”落实为**经治理可用日线日期的交集**，不是仅查询日历得出已过几天。日历完成但某资产缺 bar，窗口仍可能不足。

证据：

- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-ingest/src/session_bars.rs`
- `crates/akzio-ingest/src/paper_decode.rs`
- `crates/akzio-learning/src/evaluation.rs`

### 4.2 迟到 T1 不能读到 T3/T5

worker 根据已存在 Retrospective 筛掉已完成 horizon，排序后只推进最早 pending horizon。若一次采集已包含五个 Session：

- horizon observations 截到当前 horizon；
- daily observations 截到当前 stage_day；
- prior retrospectives 仅保留相同 outcome_id 且更早 horizon；
- Rust 生成带 `market_cutoff_session`、`facts_authority=rust`、metric_basis 的 `outcome_stage_context`；
- 完整 provider evidence 作为 source closure 持久化用于审计，但不会因为被引用就自动成为模型顶层可读文档。模型候选是阶段 packet、原决策/上下文、Claim/Critique 与以前复盘。

阶段 packet 还显式声明 actual_account_nav unavailable，原因是没有后续成交/现金流账本。T1/T3 的物化是不可变 RunScoped 快照；最终 T5 使用当时取得的治理输入构建三窗口，不意味着直接拼接此前 Partial CAS，亦不承诺 provider 历史数据永远无修订。

证据：

- `crates/akzio-daemon/src/outcome/worker.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`
- `crates/akzio-learning/src/evaluation/materialize_outcome.rs`

### 4.3 Corporate actions：不猜调整因子

主路径使用 raw prices，`validate_outcome_price_window` 要求 corporate_actions_response 存在且分页完整；出现非空 next_page_token 即拒绝。对 action 类型按实际日期字段读取：

- splits、stock/cash dividends、spin-offs、rights、capital-gains distributions：ex_date；
- unit_splits、各种 merger：effective_date；
- redemptions、name_changes、worthless_removals：process_date；
- 未知类型或缺日期语义：拒绝。

若 `baseline < effective_date <= 当前stage_day`，返回“需要 quantity/cash adjustment ledger”，不自动乘 split factor、不填 dividend return。采集范围更晚的公司行动不会让较早阶段因未来事件失效；baseline 当日不在这个开区间检查内。worker 先持久 provider evidence，再跑该 gate，失败仍有审计材料，但没有有效数值 Outcome。

证据：

- `crates/akzio-ingest/src/session_bars.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`

## 5. Rust 权威算法：frozen post-execution exposure v3

### 5.1 数量、现金、同一 equity 分母

为便于解释，以下金额/数量公式省略 micros 的 10^6 缩放；实际实现使用整数、i128 中间值及转换/校验，ppm 除法存在整数截断。

设：

- `E0`：冻结 AccountSnapshot.equity；
- `q0_i`、`M0_i`：起始持仓数量及 account mark 市值；
- `p0_i`：冻结 execution quote midpoint；
- `q_i`：应用唯一终态 receipts 后的数量；
- `ph_i`：第 h 个窗口的 raw close。

```text
cash0 = E0 - Σ M0_i
买入：q_i += filled_qty，cash -= filled_qty × fill_price
卖出：q_i -= filled_qty，cash += filled_qty × fill_price

w_i = q_i × p0_i / E0                  （ppm 化）
turnover = Σ |filled_qty × fill_price| / E0
估算费用 = 实际成交名义额 × transaction_cost_rate
返回 cash = 重建现金 - 估算费用
```

不是按最新 account NAV 重估，也不是直接拿 Decision.targets 冒充 achieved positions。NoOrder 禁止携带 plan/fills。ReconciledPaper 要求：

- receipts.validate；
- client_order_id 相同且内容完全相同可去重，内容不同拒绝；
- 一个资产最多一个唯一终态订单；
- state.is_final_without_successor；
- plan_hash 匹配、属于 plan、requested quantity 不超过 plan notional/limit 所允许数量；
- 每个 plan order 有对应终态 receipt；
- 有成交量必须有平均成交价；应用后不得产生负持仓。

证据：

- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-daemon/src/outcome/materialization.rs`

### 5.2 v3 净收益的拆分

```text
单资产价格收益 ri(h) = (ph_i - p0_i) / p0_i
G(h) = Σ w_i × ri(h)                          # portfolio_return_ppm：冻结敞口价格效应

I = [Σ买 filled_qty×(p0-fill)
     + Σ卖 filled_qty×(fill-p0)] / E0          # 有符号 implementation_effect
V = Σ(q0_i×p0_i - M0_i) / E0                  # initial_valuation_effect
F = 估算费用 / E0

N(h) = G(h) + I + V - F                       # v3 冻结净收益
B(h) = (QQQ_h - QQQ_0) / QQQ_0
U(h) = N(h) - B(h)                            # utility，相对 QQQ
NAV(h) = 1,000,000 + N(h)                     # ppm 归一化，不是真实账户 NAV
```

费用和 I/V 是同一 T0 的一次性累计调整；每个 horizon 都展示相同初始调整，不是每天重复扣费或每个窗口再次交易。daily return 由相邻 NAV 相除导出，第一天与 1,000,000 基点比较。现金未计利息；后续再平衡、现金存取、分红入账都没有被当作已观测账户流。

`implementation_effect_ppm=Some(...)` 识别 v3，即使值为零也仍是 v3。存在该 signed effect 时 `deductible_slippage_ppm()` 固定 0。`limit_shortfall` 是相对于 plan limit 的单边不利价差诊断，只计负面偏离，不将有利改善写成负成本；**不得在 I 已计入后再扣一次 shortfall。**

兼容 `realized_execution` 仅有账户 mark/limit，必要时用 1 美元占位计价，随后明确清空 I/V 和 baseline attribution；物化为 v2。**生产 daemon 调的是 `realized_execution_at_prices`，不能把这个兼容入口的占位价描述成主路径。** 老数据 metric_basis=None 仍为未知，不能自动改标 v3。

证据：

- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-learning/src/evaluation/materialize_outcome.rs`
- `crates/akzio-domain/src/evaluation/outcome.rs`

### 5.3 能归因与不能归因的执行成本

每订单可保留 asset、side、filled/unfilled quantity、plan limit、baseline_mid、baseline_to_fill_effect、fill VWAP、limit_shortfall。

当前重建器明确将 decision_mid、arrival_mid、spread_cost、market_impact、unfilled_opportunity_cost 留为 None。不能从 limit/fill 的差异声称已测得市场冲击、点差、机会成本或 decision-to-arrival 延迟。

`OutcomeCostModel` 的 Rust Default 两项都是 0，CLI 未显式配置也 serde default 为 0。这是代码默认，不是对 Alpaca 实际收费为零的事实断言。transaction_cost_ppm 是成交名义额费率，经实际 turnover 转为 equity 成本；v3 不会额外扣配置 slippage rate。

证据：

- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-domain/src/evaluation/outcome.rs`
- `crates/akzio-cli/src/main.rs`

## 6. Forecast、benchmark、路径风险与独立风险真值

### 6.1 Forecast 真值不能混入配置权重

每个 horizon 的每资产 up probability，与**该资产自身 raw close 相对 p0 的实际方向**评分；严格 return>0 才算正事件，零收益属于非正。

- Brier：`(p-y)^2`，转 ppm，越低越好。
- 同 horizon 对资产分数等权平均，不按 target weight 平均概率；组合上涨概率不能由资产边际概率加权得到。
- 10 个 probability bins，p=1,000,000 落最后一档。
- 一个四资产 horizon 通常有 4 个 binary samples。Outcome `calibration_ppm=None`，单次 ForecastScore 不是“已校准”。
- 聚合校准最少 30 个 samples，按 sample_count 加权 Brier、按 bin 算 ECE。这里的 30 不是 30 个 Session，更不是 30 次 Paper 成交。三 horizon 分别聚合。
- 主研究合同有 12 forecasts；底层评分 helper 兼容每 horizon 单资产或精确四资产，不能据 helper 的宽度声称正式 Synthesizer 可以只交单资产。
- 本评分链主要评价 positive_return_probability；没有在该函数中为 expected_return_ppm 另算 MAE/RMSE。不能把 Brier 报告扩写成所有收益幅度估计都已验证。

证据：

- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-learning/src/evaluation/materialize_outcome.rs`
- `docs/agent-runtime-contract.md`

### 6.2 七类 benchmark 的口径

| Benchmark | 已有定义 |
|---|---|
| Cash | 恒定 NAV；0 收益，不计现金利息 |
| QQQ | 100% QQQ 静态持有 |
| SOXX | 100% SOXX 静态持有 |
| FourAssetEqualWeight | 四资产各 250,000 ppm，静态持有，不是日再平衡 |
| NoLlmDeterministic | QQQ 500,000 + SOXX 500,000 |
| BetaMatchedQqq | 同窗口 gross portfolio 对 QQQ 的 OLS beta，缩放 QQQ 日收益路径 |
| VolatilityTargetedQqq | 同窗口 portfolio 样本波动率 / QQQ 样本波动率，缩放 QQQ 日收益路径 |

所有 benchmark definition 都携带 version/hash，cost assumption=0。对比的是组合净收益减零成本参考收益，不是双方完全相同交易成本的策略对照实验。beta/volatility matched 是**同窗口事后诊断**，不是 T0 可获得的 ex-ante 策略。

普通 benchmark 至少 1 个样本，派生 QQQ 至少 5 个，并需覆盖当前 horizon；少样本、缺 horizon path、退化参考路径、非正 derived NAV 等均报告 Unavailable reason，不能填 0 代替。缩放因子需有限且>0；零/负 beta 不会被悄悄解释成有效做空基准。

证据：

- `crates/akzio-domain/src/evaluation/outcome.rs`
- `crates/akzio-learning/src/evaluation/benchmarks.rs`

### 6.3 路径风险

- 最大回撤：有至少一个 NAV 点即可算，初始 peak=1,000,000。
- tracking error：至少 5 个日收益样本，portfolio daily return−QQQ daily return 的样本标准差，未年化。
- beta：至少 5 个样本；Cov/Var，QQQ 方差退化时 None。
- Sortino：至少 5 个样本，均值 / `sqrt(负收益平方和 / 全部样本数)`；无 downside 则 None，未年化、无另行风险无息基准。
- ES/tail loss：至少 20 个样本；将负收益转为非负损失，取最大 `ceil(n/20)` 个平均，属于 95% 尾部诊断实现。
- 标准 collection 最多取前 5 个共同 Session，因此其 T1/T3/T5 路径不会自然得到 20 个样本的 ES。**标准 T5 的 ES=None 是算法门槛的结果，不能填成 0 风险。**

证据：

- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-daemon/src/outcome/collection.rs`

### 6.4 Evidence completeness 不等于研究充分

collection 的 expected_evidence_count 是本轮四资产成功 acquisitions 数；observed_evidence_count 是生成的 NormalizedEvidence 数。正常四项齐全可得到 100% **价格窗口材料完整度**，并不意味着 40 项 T0 证据都完整，更不意味着 Claim/Critique 足以支撑每个研究槽位。

CountedRatio 保留 expected/observed/ratio；Wilson 95% 下界只有分母≥5 才生成，z=1.959963984540054。四资产本轮分母通常为 4，所以不能期待它有 Wilson 下界。默认 Evaluation 的证据与风险门槛比较 ratio_ppm，不是 Wilson lower bound。

证据：

- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-learning/src/evaluation.rs`

### 6.5 Risk recall 真值是独立输入，不由 Outcome 模型自评

RiskGroundTruthAssessment 每 horizon 一份，绑定 schedule、Decision、observed_trading_day、decision producer identity、expected/detected risk ID 集合、独立 basis_refs，以及 reviewer/verifier 的 identity/version。

- reviewer、verifier、Decision producer 三者身份按 trim+大小写不敏感比较，必须相异；
- expected 非空，detected 必须为其子集；
- basis_refs 非空、排序去重，允许 RawEvidence/NormalizedEvidence/SemanticDetail，不能拿被评估 Decision 或 schedule 自证；
- 必须 sealed，使用时在 sealed_at 与 valid_until 之间；
- 同一 horizon 不得重复，日期和 schedule/Decision 身份必须精确匹配；
- recall=detected_count/expected_count；缺 assessment 保持 None，没有风险标签生产器时不会填满分。

`record_risk_ground_truth_assessment_fenced` 是**接收外部独立评审并提交**的接口，不生成风险标签或 reviewer 身份。本次检索 `crates/**/*.rs` 找到该记录函数及 collection 消费方，未找到 daemon 自动调用它生成三期 assessment 的生产入口。因此不能声称当前 worker 会自动把风险真值补齐。

证据：

- `crates/akzio-domain/src/evaluation/risk_ground_truth.rs`
- `crates/akzio-learning/src/evaluation/risk_ground_truth.rs`
- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-learning/src/evaluation/materialize_outcome.rs`

## 7. Outcome 模型协议、预算与数值密封

### 7.1 两阶段协议实际含义

Outcome Contract 63 / PromptBundle 35 保留：

1. Draft：可使用五种受控 Context 读工具，也可以不读，写非空叙事 memo；
2. Draft 工具调用先入 ToolCall，执行后入 ToolResult，作为 continuation 继续，均计入同一预算；
3. 有已持久化模型 turn/memo 后才切 Submit；Draft 不能夹带 terminal submission；
4. Submit 只允许结构化 `submit_result`，不能同时带 assistant_text/读工具调用，提交 `RetrospectiveDraft` 与所需 deliberation；
5. 当前 Submit 格式化请求 reasoning override 为 low，不能把该请求与 Draft 所用 route effort 混成一个值。

可读工具只有 read_document、read_range、search_context、read_claim_evidence、compare_sources；read/range 32 KiB，compare_sources 2–4 个；search_context 是小写子串包含，不是语义搜索。没有任意文件、SQL、RawEvidence、web 或交易工具。source closure 的存在也不是 ReadGrant。

模型输出是 summary、findings、counterfactuals、diagnostic gaps、scoped lesson proposals 等；不提供权威收益、滑点、风险召回或 Policy 决策。引用 Rust 整数只是复述，不成为第二份数值权威。

证据：

- `docs/agent-runtime-contract.md`
- `crates/akzio-research/src/agent/runtime_run.rs`
- `crates/akzio-research/src/agent/prompts/roles/outcome.md`
- `crates/akzio-domain/src/evaluation/outcome.rs`

### 7.2 预算必须区分三层

| 层次 | Outcome 默认 |
|---|---|
| 不可变 Contract baseline budget | 12,000 input / 4,000 output / 2 read-call budget / 180 秒 |
| 新 Run 的 default_agent_budget | 1,000,000 input / 4,000 output / ToolCallLimit::Unlimited / 180 秒 |
| Run 实际冻结预算 | default → 全局 override → outcome_worker override；从 graph.agent_budgets 读取 |
| Context 授权限制 | 24 artifacts、128 KiB 投影视图；Context max_tokens=32×1024，max_source_bytes=128 KiB×4 |
| Draft 的份额 | 最多动用总 output 的一半；70% wall time 截止；默认 180 秒的 70% 为 126 秒 |
| Submit | 使用总预算剩余额度；不会重新开始 180 秒，当前版本还保留最多 1 秒审计余量 |

“unlimited”仅表示读次数维度不另设有限数字，仍受白名单、输入/output、墙钟、恢复与 Context 限制。它不开放新工具。首轮 provisional input×2 放不进剩余输入预算时直接拒绝，不能跳过 Draft。

本文没有读取实际本地配置或 Run graph，因此上述是**源码默认/解析顺序**，不是任意真实 Run 的预算实测。

证据：

- `crates/akzio-domain/src/budget.rs`
- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-research/src/agent/errors_catalogue.rs`
- `crates/akzio-research/src/agent/runtime_run.rs`

### 7.3 数值与叙事分开落库

T1/T3：

- Rust `materialize_partial_outcome`；
- Outcome 和 Retrospective 都为 RunScoped；
- Partial Outcome 未 sealed；Retrospective 有自身阶段 sealed_at；
- 有匹配 draft 则 status=Complete，无则 ModelUnavailable；
- 专用 fenced 事务共同写入，不关闭长期 worker，随后 Deferred。

T5：

- 必须有 T1/T3/T5 三个唯一窗口且交易日递增，`validate_sealed` 要求 sealed_at；
- Rust-only 路径可以提交 Canonical Outcome 和 ModelUnavailable Retrospective，但没有 Experience、Evaluation 或 Policy influence；
- 有叙事并不立即表示 learning_eligible，仍进入第 9 节资格复核；
- 既有同 Run/outcome_id 的最终 Outcome 复用，不重写已封存 CAS；
- Canary seal 的 complete_task=false 只发布 CAS/events 进度，不进入 succeeded Attempt output index；完成事务才发布相应正式输出。不要把可见中间 Artifact 当成已成功 Attempt。

证据：

- `crates/akzio-learning/src/evaluation/policy_learning.rs`
- `crates/akzio-learning/src/evaluation/materialize_partial.rs`
- `crates/akzio-learning/src/evaluation/outcomes.rs`
- `crates/akzio-domain/src/evaluation/outcome.rs`
- `crates/akzio-store/src/store/learning/outcome.rs`

## 8. Narrative repair：只修叙事，不改历史数值

Store 提供显式 `request_outcome_narrative_repair`：

- 先找到 sealed Outcome 与 T5 Retrospective；
- 有 queued/running repair 时返回原 Task ID；原 worker 仍活跃则拒绝；
- 同 Run 最多创建 2 个 repair Tasks；不是允许改写两次数值 Outcome；
- 新 Task 保留 Run，引用 sealed Outcome/旧 Retrospective；Contract 取当前 Outcome catalogue head，budget 优先沿用原 graph 的 Outcome budget；
- 不更改原 task、session slot、Commitment、baseline 或数值 CAS。

daemon repair 只读原 Outcome、旧 T5、T1/T3、原 Claim/Critique/Decision/ExecutionContext，不采新价格。若旧 T5 已 Complete，尝试复用其 committed Draft，不再调用模型；普通 memory:paper:default 已评估时直接 NoOutput。

合法 revision 是相同 Run/outcome_id/outcome_ref 的 **T5 ModelUnavailable→Complete**，新 source_refs 必须含旧 Retrospective。`retrospective_for` 允许这对旧/新记录，返回有效修复版；不允许任意多个同身份不同 payload。

修复后复用 `evaluate_sealed_with_retrospective`，仍检查原数值的研究/风险资格、subject/outcome once-only 消费与 pair cursor。因此：

- 修复不会把缺失的 sealed risk_recall 变成 measured；
- 已落库但不可学习的 Evaluation 不会仅因后来有新 memo 就被覆盖重评；
- Canary 已记录的 process_quality unknown 也不会被 repair 默默覆盖；
- repair 不是自动授予 Proven 或 Active Lesson 的接口；
- 当前 node objective 接线限制见第 13.1 节，不能据接口存在声称修复实际已跑通。

证据：

- `crates/akzio-store/src/store/learning/history.rs`
- `crates/akzio-store/src/store/free_reads.rs`
- `crates/akzio-daemon/src/outcome/narrative_repair.rs`
- `crates/akzio-daemon/src/outcome/canary.rs`

## 9. Experience / Evaluation / PolicySubject：什么才叫学习

### 9.1 资格的四个维度

`evaluate_frozen` 从 CAS 重读 Outcome、schedule、DecisionContext、Claim/Critique，要求 Paper purpose、sealed Outcome 与同 outcome_id 的 T5 draft。研究充分定义为：

```text
research_coverage_is_complete(claims, critiques)
AND DecisionContext.hard_blockers 为空
AND soft_warnings 不含 IncompleteEvidence
```

质量已测量定义为：

```text
所有窗口 risk_recall 为 Some
AND 所有窗口 evidence_completeness 为 Some
AND research_sufficient
```

默认 evidence completeness 与 risk recall 均须≥900,000 ppm。`learning_eligible = quality_metrics_measured && !degraded`。已测量低于门槛属于 degradation；None 不能解释为满分，也不能单凭“未知”证明退化。

有合格 T5 draft 但风险/研究不充分时仍可能创建 **不可学习的 Experience/Evaluation 审计事实**。无合格 draft 的 Rust-only T5 则走专门 seal，连 Experience/Evaluation 都不创建。二者不要混为一谈。

证据：

- `crates/akzio-learning/src/evaluation/materialization.rs`
- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`

### 9.2 producer、evaluator、被评估 subject 三者分离

Experience 记录：

- 原 Decision、DecisionContext、ExecutionContext、原 policy verdict、Outcome；
- producer Run、实际 Claim/Critique/Synthesizer Contract 集；
- producer Workflow Artifact + revision、topology；
- evaluator Outcome worker 的 Contract hash；
- metric basis；
- market_window_complete、research_sufficient、retrospective_valid、risk_ground_truth_measured、learning_eligible。

Canary 对 candidate Contract/Topology 做评估时，其 Experience 的 producer 仍是**父 Paper Decision 的生产者**；candidate 身份在 PolicySubject、CandidatePolicy、ShadowPair 里，不能偷换成“候选已经生产了父单/父 Outcome”。

`market_window_complete` 当前字段赋值来自“所有窗口 completeness 均有测量”，不是独立再跑一次行情完整性证明；数值结构完整还依赖前面的 sealed/domain 校验。

证据：

- `crates/akzio-learning/src/evaluation/materialization.rs`
- `crates/akzio-domain/src/evaluation/policy.rs`
- `crates/akzio-store/src/store/impl_learning.rs`
- `crates/akzio-daemon/src/outcome/canary.rs`

### 9.3 三类成本不能混

1. **Decision production cost**：从 Decision 的 producing task 出发，递归依赖闭包，汇总闭包所有任务的真实模型调用，包括 retry。后来的 Outcome worker 不是其依赖，不能改变 T0 production cost。
2. **Outcome / retrospective cost**：同 Run 中 recipe=learning.outcome_worker 的任务集合，包括 repair；独立查询。
3. **Lifecycle cost**：全 Run model usage。它比 Decision production cost 广，不能当成 T0 决策成本。

Evaluation 保存的 token_cost、latency_millis 来自第一项；`marginal_utility_ppm` 是三窗口 utility 的算术平均。**代码未将 token 数/模型美元费用自动折算到组合 utility**；Canary `cost_adjusted_utility_ppm` 沿 Outcome 的执行费用调整，不是“已扣全部 AI 成本”。未闭合调用或缺 telemetry 保留 missing/unknown，不据失败状态猜花费。

证据：

- `crates/akzio-store/src/store/trajectory.rs`
- `crates/akzio-store/src/store/learning/history.rs`
- `crates/akzio-learning/src/evaluation/materialization.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-domain/src/canary.rs`

### 9.4 Memory 状态与 fresh pair 消费

PolicySubject 是 typed namespace：

- Memory(MemoryId)，普通 worker 使用 `paper:default`；
- Contract(ContentHash)；
- Topology(TopologyId)。

MemoryLifecycle：Candidate、Active、Proven、Contested、Retired。默认无 degradation 时 Candidate→Active→Proven；Contested 不会在普通无 target 分支自动恢复。degraded 时普通状态→Contested，Contested 再退化→Retired，Retired 保持。

**每次前向迁移默认要求 T1/T3/T5 各至少 3 个 fresh paired outcomes，并有完整质量测量。** 普通 T5 本身不会凭三个 horizon 自造三组 ShadowPair；没有 fresh pair 就保留现状。收益为正本身也不构成足够晋级条件。

Store 的 pair key 是比较身份（parent/candidate decisions、execution context、candidate Contract/Topology、horizon），不是完成时间。同 subject/outcome 最多一次 canonical Evaluation；无 transition 的 no-op Evaluation **也会消费该次 snapshot 的 through_cursor**。因此不能重放同样 pair 累计晋升，也不能假定多个 no-op 后这些 pair 仍全部 fresh。

证据：

- `crates/akzio-domain/src/evaluation/policy.rs`
- `crates/akzio-learning/src/evaluation.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-store/src/store/learning/policy.rs`

### 9.5 原子边界与后续可召回影响

`record_policy_evaluation_fenced` 在同一 Immediate 事务检查 lease/permit、Paper、当前 subject head、pair snapshot，提交 Outcome、最终 Retrospective、Experience、Evaluation、可选 CandidatePolicy、DecisionContext 记录 applied/rejected 的 Lesson evidence、transition/head 和 pair-consumption cursor。该归因记录本身不证明模型读到了 Lesson 正文。前向转移要求 evaluation_context 的学习证明字段均成立；restriction-only 转移不要求授予新影响的资格。

**新 Lesson proposals 的逐条 `write_lesson` 在上述事务之后。** 后续 Lesson 写失败不回滚已经提交的 Evaluation/Policy。这是已有边界，不应写成 Outcome、Policy、新 Lesson 全部是单一总事务。

Context 对 Experience 要求 canonical Paper、evaluation_context.learning_eligible、已记录 influence subject、当前 head 允许影响（Memory Active/Proven）；CandidatePolicy 另要求其 source Evaluation/Experience 完整及当前 Contract/Topology Active。当前实现扫描最近 100 个 Experience，至多加入 4 个及允许的附带 Retrospective。**记录、进入候选、实际选入 Manifest、正文进入模型内联视图、模型声明采用是不同步骤。** 默认 Lesson/Experience/CandidatePolicy 的 selection reason 不触发 must_read，也没有对应 kind 分支，因而只进入 metadata ledger；研究角色又没有读取工具，不能据上述资格或 Manifest 引用称学习正文已被 Synthesizer 使用。详见第 10.3 节。

证据：

- `crates/akzio-store/src/store/learning/policy.rs`
- `crates/akzio-store/src/store/impl_learning.rs`
- `crates/akzio-learning/src/evaluation/materialization.rs`
- `crates/akzio-context/src/context_broker/policy.rs`
- `crates/akzio-context/src/context_broker/manifest.rs`

## 10. Lesson：范围、生命周期、召回冲突与 forgetting

### 10.1 新 Lesson 不是 MemoryLifecycle::Proven

LessonLifecycle 只有 Draft/Active/Contested/Retired，和 PolicySubject::Memory 的 Candidate/Active/Proven 是**两套状态域**。不能把 memory head Proven 描述成某条 Lesson 已通过验证。

Outcome 的 lesson_proposals 最多 4 条，必须显式具备：

- statement、recommended_behavior；
- 1–4 资产、1–3 horizons；
- 1–4 exclusions，每项非空且≤1200 字节；
- 1–4 evidence refs；
- statement/behavior 各≤4000 字节。

当前 prompt 要求旧 `lesson_candidates` 为空。兼容领域字段仍可解码旧 free text，但 materializer **只遍历 lesson_proposals**，不把旧字符串转成全资产教训。

新生成 Lesson：

- origin=OutcomeDerived；
- lifecycle=Draft，confidence=500,000；
- scope 取 proposal 资产与 horizon；
- source_refs 含 Retrospective 与 evidence；
- authored_by/approved_by=None；
- governance=`outcome_quarantined`：uncertainty=1,000,000、usage budget=1、quarantine reason=`paired cross-regime validation pending`。

budget=1 并不意味着它能先被用一次：quarantine 本身就阻断检索。

证据：

- `crates/akzio-domain/src/lesson.rs`
- `crates/akzio-domain/src/evaluation/outcome.rs`
- `crates/akzio-learning/src/evaluation/outcomes.rs`
- `crates/akzio-research/src/agent/prompts/roles/outcome.md`

### 10.2 激活/争议/退役的真实写路径

`write_lesson` 原子插入主 source、Lesson CAS、revision=1 head 与 created event；同 lesson_id 不同内容拒绝。`transition_lesson`：

- actor/reason 必须非空；
- 新建 successor Artifact，supersedes 加旧 revision；
- Immediate 事务 CAS 比较旧 head Artifact ID；
- Active/Contested 要有 approved_by 与 governance；
- Active 不能带 quarantine；
- 检查声明的 conflicts_with 所指 Lesson 若仍 Active，则拒绝激活；
- Retired 不能重新激活；同状态 Active/Contested 可以创建新治理 revision；
- 旧 CAS 不删除、不覆盖。

Active 治理模板：

| 来源 | 有效期 | max usage | uncertainty | verifier |
|---|---:|---:|---:|---|
| Operator | 90 天 | 100 | 250,000 ppm | operator-review / v1 |
| OutcomeDerived 经 transition Active | 30 天 | 20 | 500,000 ppm | operator-paired-review / v1 |

**重要实现边界：** `outcome_revalidated` 的名称和 verifier 文本不是量化验证证明。当前 `transition_lesson(...Active...)` 本身检查内容、审批字符串、治理和冲突，并设置此模板；没有在该函数中查询“三期配对样本/跨 regime 达标”后才允许 Active 的独立统计 gate。自动 Outcome producer 仅有 Draft 权限；人工/受控生命周期接口与模型的权限不能混写。

证据：

- `crates/akzio-store/src/store/lesson/write.rs`
- `crates/akzio-domain/src/lesson.rs`

### 10.3 检索范围由 Rust 推导，不从正文猜

当前 ContextQueryScope 从已校验的 frozen node 得到四资产全集、recipe stage；Analyst/Critic 取 typed horizon，Synthesizer/ProposalReviewer 显式覆盖 T1/T3/T5。regime 只能来自经授权/身份校验的 DecisionTime snapshot，不能由标签正文或 ExPost snapshot 冒充。

查询空维度表示 unknown：仅匹配该维度未限定的 Lesson，不表示任意匹配。注意领域旧 `LessonScope::matches` 在某些空请求维度更宽，**当前召回实际调用的是 `ContextQueryScope::matches`**，应以调用点为准。

当前研究角色中只有 Synthesizer 的 Contract 允许 Lesson 成为 Context 候选；查询能表达某维度，不等于该角色获准读取 Lesson，更不等于正文已提供给模型。

召回过程：

1. 同一 Deferred SQL snapshot 按 lesson_id keyset 每页 128 扫描所有 Active heads，不先截最新 50；
2. scope → usage/governance → permitted source family →精确 context/overlay gate；
3. 排序：regime overlap、stage overlap、asset overlap、horizon overlap，再 updated_at 降序、lesson_id；
4. 对完整文本、scope/exclusions 做精确重复去重；显式冲突保留；
5. 在合格候选中构建无向 conflict connected components；**至多 4 条 Lesson，不是 4 个任意大小冲突组**，完整组放不下则整组不选；
6. 写 `learning.retrieval.audit`（RunScoped），含实际 scope、排除理由、selection_limit=4；
7. Manifest 最终仍受 24 artifacts、字节与必需输入闭包限制。

**默认模型视图的限制：**

- Manifest selection 默认通过 `selection_reason(kind)` 生成 reason：Lesson 为 `lesson`、Experience 为 `experience`、CandidatePolicy 为 `candidate_policy`。
- `must_read_class` 仅对显式 `must_read` / `mandatory_observation` 原因、列明的 SemanticDetail producer 或列明的 ArtifactKind 返回 Some；没有 Lesson/Experience/CandidatePolicy 的 kind 分支，三者的默认 reason 也不属于上述显式原因。
- 因此默认选中的这三类材料保留 document_id、kind、source、时间、reason 等 metadata，但不进入 `must_read` 的正文 value；研究 `model_context` 把非 must_read 项放 metadata ledger，只有 must_read 项获得内联 value。
- 当前研究角色只提供 submit_result，没有读工具。本次默认路径未见另一个将 Lesson 正文额外注入的入口。**不能把“召回成功”“selection audit=selected”“Manifest 引用了 Lesson”“DecisionContext 声明 applied”升级成“模型已读到或理解 Lesson 正文”。**
- 显式 must_read 原因是通用例外，但默认 Lesson selection 并未使用它；其存在不能用来解释默认路径已内联。
- Rust 为排序、去重或审计读取 Lesson payload，与把该 payload 交给模型是两件事。完整显式冲突组被选入，也不证明冲突双方正文已呈现给模型。

source 更新只生成 `learning.revalidation.suggestion`：同 source_family/resource、不同 blob、created_at 晚于 last_revalidated_at；不替换旧 evidence，不自动判矛盾、不自动改 lifecycle。

证据：

- `crates/akzio-domain/src/context_scope.rs`
- `docs/agent-runtime-contract.md`
- `crates/akzio-store/src/store/lesson/write.rs`
- `crates/akzio-context/src/context_broker/manifest.rs`
- `crates/akzio-context/src/context_broker/coverage.rs`
- `crates/akzio-context/src/selection.rs`
- `crates/akzio-context/src/context_broker/manifest.rs`
- `crates/akzio-context/src/context_broker/materialization.rs`
- `crates/akzio-research/src/agent/errors_catalogue.rs`

### 10.4 到期、使用预算、负面观察与 forgetting

检索要求 Active、无 quarantine、在 valid_from/valid_until 范围内、usage<max_usage、contradiction_count=0、post_use_failure_count=0。有 regime_compatibility 表时要求请求 regime 至少一项匹配≥500,000 ppm；未知 regime 不算匹配。

usage 取**当前 Lesson revision Artifact** 被 ContextManifest 与 DecisionContext 引用的数量之和；不是“用户读了多少次”，也不是必然一 Run=一次。一条进入 Manifest 又进入 DecisionContext 可能贡献两次引用。

生产 WorkerPool recovery maintenance 可调用 `contest_lessons_due_for_revalidation`，对到期、usage 达上限、quarantine、contradiction/post-use-failure 的 Active Lesson 创建 Contested successor。没有调用 verifier，也不会自动恢复 Active。它逐 Lesson 独立转换，不是全扫描一个总写事务。

Outcome 对 DecisionContext 中已记录 applied/rejected 的 Lesson 生成 `LessonEvidence`，按稳定 lesson_id + DecisionContext + Outcome 去重，并保留被引用的确切 revision、三窗口 utility。它是**模型声明与持久引用层面的观察性日志，不是正文内联或阅读证明**：

- applied 后亏损不证明 Lesson 造成亏损；
- rejected 后的结果不是“使用 Lesson”的反事实；
- 不能仅凭一条负 utility 自动 contest/retire；
- 当前自动 contest 是治理过期/使用阈值等路径，不是从收益推因果。

“forgetting”在该闭环中主要表现为撤销可召回影响、到期争议、退役、Policy 降级与保留历史，而非删除 CAS 或训练/擦除模型权重。跨模型/候选能力保留还有第 12 节的 retention matrix，但不能把未接入的方法/字段称成完整自动遗忘防护。

证据：

- `crates/akzio-domain/src/lesson.rs`
- `crates/akzio-store/src/store/lesson/write.rs`
- `crates/akzio-daemon/src/worker.rs`
- `crates/akzio-learning/src/lesson_evidence.rs`

## 11. Canary：三个注册 Shadow、比较与受控晋升

### 11.1 登记候选不授予交易权

当前 Canary scheduler 限定：

- campaign 当前 stage 有 cohort；session market day 必须有明确登记 regime；
- 当前 approval/runtime identity、code revision、maximum_notional、broker account、feed 一致；
- candidate Contract 为 canonical research.analyst、未 activated、baseline hash 等于当前 active analyst；
- candidate topology 为 RunScoped WorkflowGraph，且匹配注册的 structured-critique candidate topology 与 cohort；
- parent Paper + contract/topology/bundle 三个 Shadow、session slot、campaign reservation 在 Store 事务一起发布。

三个 Shadow：

| Shadow | Analyst Contract | Topology |
|---|---|---|
| contract | candidate | parent |
| topology | active | candidate |
| bundle | candidate | candidate |

真正有 PaperCommit 的只有 parent。Shadow 的存在、评价或者 candidate status 不替代 parent 的 Approval、DecisionGate、ExecutionGate、Commitment、Broker receipt。

Campaign schema 允许的 maximum_total_notional 上限为 `MoneyMicros::from_usd_cents(100000)`，即 **1000 美元**，并须>0；这是该 Canary spec 的硬上限，不是全系统风险配置默认值。

证据：

- `crates/akzio-daemon/src/scheduler/canary.rs`
- `crates/akzio-domain/src/canary.rs`

### 11.2 T0 evidence 复用有精确边界

只有 reservation 中登记的三个 Shadow 可以取父 Run 的**成功 EvidenceGate Attempt 正式 outputs**：

- NormalizedEvidence，及精确 producer 的 evidence.collection_status；
- gate 未完成返回等待，失败拒绝；
- 不把 RawEvidence 作为授权顶层文档；
- 不使用父执行刷新或任意更晚 evidence 冒充 T0 资料；
- 源完整性、kind/producer、Run 身份和 read grant 仍另行检查。

Shadow Outcome 的跨 Run 引用是另一条白名单：学习 Need 可引用父 canonical OutcomeSchedule；candidate schedule 只能引用父 schedule 冻结的 ExecutionContext、Verdict、必要的 Commitment/Reconciliation。不是“父子 Run 可以任意互读”。

证据：

- `crates/akzio-store/src/store/canary/history.rs`
- `docs/agent-runtime-contract.md`

### 11.3 Shadow Outcome 是反事实比较材料，不是候选账户实绩

`execute_shadow_evaluate` 等父 schedule 和 T5 共同窗口，用父 schedule 调 collection，再建立新的 RunScoped candidate schedule/outcome：

- Decision/DecisionContext 换 candidate 的；
- baseline day、execution context、execution lineage 仍是父冻结来源；
- materialization.target 换成 **candidate Decision.targets**，不是 research_allocation，也不是候选真实成交后持仓；
- forecasts 换 candidate forecasts；
- **observed_execution 不被替换**，因此成本/implementation/initial valuation 使用父重建量；
- risk_recall 全部清 None，父 assessment 不能充当候选 Decision 的真值；
- 当前这个函数不调用候选 Outcome 叙事模型；只有数值 Shadow Outcome。

未来行情是按同一 baseline/共同 Session 规则另行受控采集，不能保证 parent/candidate 使用同一下载批次或完全相同 BLOB。精确同一事实对比仍需冻结引用/实际运行证据，不能只从日期相同推断。

Canary `metrics` 从 schedule.execution_context.final_process_quality 取 measured_floor；candidate schedule 正是父 ExecutionContext，因此这里也会沿父冻结来源取 process quality。它检查 parent retrospective_draft 存在，**并没有等三份 Shadow narrative**。源码注释中“不能借父级质量”不能替代这一实际赋值链。

证据：

- `crates/akzio-daemon/src/outcome/shadow.rs`
- `crates/akzio-daemon/src/outcome/canary.rs`

### 11.4 配对记录与分 subject 提交

parent T5 seal 后：

1. campaign stage 已改变或缺 parent narrative：结束当前 worker，不生成配对评价，不等于 campaign Advance；后续可在适用 stage 走明确 repair。
2. 三个 Shadow Outcome 任一未完成：返回 false，由 caller defer。
3. 每 subject×T1/T3/T5 写 9 个 ShadowPair；contract 保持父 topology，topology 保持 active Contract，bundle 对应 Memory(`paper:default`)。
4. 按 cohort 记录 3 个 horizon observations，每项包含 contract/topology/bundle 的 parent/candidate metrics。
5. 从**已持久观察集合**算 cohort verdict。
6. 依次对 Contract、Topology、Memory 复用父 sealed Outcome/draft 执行 canonical Evaluation；complete_task=false，产物属于耐久中间进度，不发布成成功 Attempt output。
7. 最后提交 campaign transition，再 finish task。

这些步骤不是一个横跨全部 subject 的总事务。中断不撤回前面已完成 subject；subject/outcome 幂等与 pair cursor 避免重复消费。最后 succeeded 只证明该处理边界收束，不自动证明所有 candidate Active 或真实订单成交。

证据：

- `crates/akzio-daemon/src/outcome/canary.rs`
- `crates/akzio-store/src/store/learning/policy.rs`
- `crates/akzio-store/src/store/canary/cohort.rs`

### 11.5 晋升 verdict 的算法

CanaryPromotionPolicy 必须显式提供阈值，没有这里可宣称的统一生产默认值：三 horizon paired session 要求、distinct market days、required regimes、evidence/risk/process 下限、utility delta、drawdown/tail-loss 允许差额、confidence 等。

`evaluate_canary_cohort`：

- 每 session_key+horizon 唯一；cohort、date/regime、asset universe、cost model、calendar、generation/promotion datasets 必须一致；
- 覆盖要求分别比较三个 horizon、不同市场日、regime；不是把同一 Session 的三窗口算成三天；
- 三 subject×三 horizon 的平均 candidate−parent utility 均需达到 policy.minimum_cost_adjusted_utility_delta；
- 各 subject/horizon parent/candidate 的 ForecastScore 聚合至少 30 binary samples；confidence 用 `1,000,000-ECE` 比较门槛，**不是置信区间覆盖率或预测正确概率**；
- promotion integrity、capability retention、search-bias certificate 缺失不能 Advance。

verdict 优先级：

1. **Rollback**：任一 observation 的 candidate utility 低于 parent，或已测 evidence/risk/process 低于最低值/低于 parent，或 drawdown/tail delta 超限，或已提供 integrity/retention 失败。
2. **Defer**：无已测退化，但覆盖、confidence、任何必需 metric、certificate 或治理材料缺失。
3. **Hold**：上述齐全，但平均 utility 增益不足。
4. **Advance**：全部通过。

因此“均值更好”不能掩盖一个 observation 触发的 rollback；None 不被补成通过。标准 Shadow 风险为 None、标准五日路径 tail loss 为 None，会满足 Defer 条件，见第 13 节。

证据：

- `crates/akzio-domain/src/canary.rs`
- `crates/akzio-learning/src/campaign.rs`

### 11.6 状态名、catalogue activation 与 topology 的差别

Campaign 当前状态：

```text
Staged → ValidationStage1 → ValidationStage2 → ValidationStage3
       → ActiveValidation → Completed
另有 Frozen
```

Canary10/25/50 是旧名称/PolicyState 映射，**不是这里实现了 10%/25%/50% 下单流量或仓位放大**。

Contract/Topology PolicyState 的允许前向边：

```text
Candidate → Canary10 → Canary25 → Canary50 → Active
```

stage/resume 仅登记/启动 campaign，不直接激活 candidate。不能进一步夸大成“学习链永远不激活 Contract”：

- `record_policy_evaluation_fenced` 会调用 `apply_contract_catalogue_transition`；
- Contract 真正转到 Active 时，Store 在同一事务检查当前 head 仍是 frozen baseline、candidate installation/Policy 匹配、capability bounded，然后追加 activation history 并切 catalogue head；
- Active Contract 被降级时，在确认当前 head 正是该 candidate 后，指回安装时冻结的 baseline，保留历史；
- 未涉及 Active 的 Contract 转移不改 catalogue。

candidate 受 Context/tool capability ceiling、同 purpose/output kind、evidence requirement、child/depth 上限限制，不能凭学习扩大工具/数据/交易能力。

Topology 有 policy head 与 CandidatePolicy source validation；上述 Contract activation helper 对非 Contract 立即 no-op，**但 scheduler 中确有另一个有限消费者**：`StorePaperWorkflowSource::proposal_sync()` 读取 `PolicySubject::Topology(STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)`，仅当 head 为 Topology Active 时，把 `preferred_topology` 设为该注册候选 ID，否则使用 `"active"`。

随后它扫描最近最多 500 条 **WorkflowProposal artifacts**，再筛来源 Run 为 Paper、proposal.topology_id 匹配 preferred_topology、且不引用不允许的外 Run RunScoped Need 的 proposal，找到则返回第一条。这里不是“预先筛出 500 条合法 Paper proposal”，也不是接受任意 Topology Active head。

未找到匹配 proposal 时，使用已有 bootstrap 中配置的 workflow/topology_id 生成 approved Paper proposal；没有 bootstrap 则 WorkflowUnavailable。因此可以确认 **Active head→偏好 topology→选择既有可用 Paper proposal** 这条接线，但 head 本身不保证最近 500 条里有可用候选 proposal，也不保证 bootstrap 恰好产出该候选，更不证明当前进程内存 catalogue 已即时切换、旧 frozen Run 已改图或新 T0 已成功执行候选图。

证据：

- `crates/akzio-domain/src/canary.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-learning/src/campaign.rs`
- `crates/akzio-store/src/store/learning/policy.rs`
- `crates/akzio-store/src/store/impl_core.rs`
- `crates/akzio-store/src/store/free_validation.rs`
- `crates/akzio-store/src/store/impl_learning.rs`
- `crates/akzio-daemon/src/scheduler.rs`

## 12. qualification / release / experiment：是否真的接入自动闭环

### 12.1 接入矩阵

| 域/能力 | 确认的实际入口/消费者 | 不应声称的能力 |
|---|---|---|
| Offline model qualification | CLI `model-qualification assemble` 读 JSON，调用 learning 纯函数，写报告；报告是 Paper approval 与 ExecutionGate 的检查输入 | Outcome 自动跑五阶段资格实验、自动安装资格报告、自动切模型 |
| ModelQualificationGate | daemon Paper approval 检查身份/完整性/时效/同 Store evidence；ExecutionGate 再查 | 通过报告就跳过 approval 或执行风控 |
| PromotionIntegrityEvidence | Canary cohort evaluator 检查已有 evidence.permits_promotion | worker 自动生成 hidden evaluator、完成独立审计 |
| CapabilityRetentionMatrix | offline qualification 与 Canary 的基础 permits_promotion 有消费者 | 所有扩展 retention policy 字段已接入自动 Canary |
| ReleaseEvidence | Store 只读聚合 → daemon GET → CLI export；domain 计算 tier/status/issues/hash | 自动发布、部署、交易开闸、自动补做欠缺的验收 |
| ExperimentTrial / SearchBiasCertificate | Store ledger/Doctor；Canary stage 对已有 certificate 和完整 trial ledger 验证；cohort evaluation 要求 certificate 存在 | Outcome 自动生成 trial、自动算 DSR/PBO/bootstrap/FWER、自动搜参发证 |
| Debug `experiment` 命令 | 调 debug_fork 建隔离实验 Run | 等于上述 ExperimentTrial/SearchBias 的自动统计平台 |

### 12.2 Qualification 的五阶段是输入收据，不是本函数实际跑模型

`run_offline_model_qualification` 的输入包括 key、frozen manifest、champion/challenger/rollback snapshot、必需 scenarios、scenario 结果、可选 QualificationStageReceipt、五个 stage evaluation refs、审批人和有效期。它校验、聚合、计算 fingerprint/report hash；自己无模型、证据 API、Broker、Store I/O。

五阶段：Replay、Adversarial、ExecutionSimulation、Shadow、Canary。report.complete 要五个 pass 且 critical_regressions=0。某些场景无 receipt 时仍保留 scenario 中传入的 challenger_passed，并留下空 measured metrics；所以报告不能被解释成本次真实执行的五阶段证据。

资格报告的使用却是真接线：Paper approval 要求当前 qualification key、完整/有效期、引用在同一 Store 持久存在并 kind 正确；ExecutionGate 再查资格，失败为 UnqualifiedRuntime。它是独立授权前提，不是 Outcome 的自动学习后继节点。

证据：

- `crates/akzio-cli/src/cli/model_qualification.rs`
- `crates/akzio-learning/src/qualification.rs`
- `crates/akzio-daemon/src/orchestration/workers.rs`
- `crates/akzio-execution/src/execution_gate/core.rs`
- `crates/akzio-domain/src/longitudinal.rs`

### 12.3 Longitudinal 防退化：有调用的方法与仅存在的字段

PromotionIntegrity 的 pass 检查：evaluator commitment/reveal 匹配、reveal 晚于 candidate seal、trajectory hash 不变；unauthorized actions、mandate violations、fabrications、critical regressions、skipped scenarios、grader/metric mutations 全为 0。

基础 CapabilityRetentionMatrix 要求 1–256 个唯一 scenarios、required subset 完整、rollback_verified、所有 critical 且 champion 原本通过的能力仍由 challenger 通过。固定 golden bank 有 23 项，包含 gap/crash/liquidity/halt/split、timestamp/news、missing quote、broker timeout/partial fill/idempotency/reconcile、conflict/risk、malformed JSON、context/grant/clock 等。

**当前 Canary 调用的是基础 `permits_promotion()`，不是 `permits_promotion_with_policy()`。** 后者存在零容忍、非关键回归数量、drawdown/tail delta 检查；本次 crate 源码调用检索未找到外部消费者。其结构里的 weighted regression、aggregate noninferiority、per-regime floor、scenario_bank_hash 也不能仅因字段存在就说已在该方法中执行；实际函数只检查代码列明的子集。基础矩阵只要求 manifest 自己声明的 required_scenarios，不自动强制整个 golden bank。

证据：

- `crates/akzio-domain/src/longitudinal.rs`
- `crates/akzio-learning/src/campaign.rs`
- `crates/akzio-learning/src/qualification.rs`

### 12.4 Release 是只读证据投影

`release_evidence_bundle` 从 Store 聚合 workflow、contracts、approval、source snapshots、execution/receipts、Outcome、learning transition、Canary 与 post-outcome approval。多次读取不是一个跨 API 的一致 SQL 快照。它不写 Artifact、不激活 policy。

tier 算法区分 E0Fixture、E1HistoricalReplay、E2PaperMechanics、E3ForwardPaper、E4Robustness；E3 要真实 Broker、execution/reconciliation receipts 及三期 Outcome，E4 再要求 learning、Completed Canary、后 Outcome 人工审批与完整性检查。**合法 NoOrder Outcome 不必满足这个面向执行的 E3 条件；不能把 release tier 低解释成 NoOrder 无法成熟。**

release.learning 当前查询最近一条 transition_id 非空的 Evaluation；一个成功但 no-op 的 Evaluation 不会被它展示成 learning transition。环境分类还依据 broker trust，有缺 broker 的材料可能落 OfflineFixture；这是投影规则，不是对未知运行来源的额外独立验证。

证据：

- `crates/akzio-store/src/store/release.rs`
- `crates/akzio-domain/src/release.rs`
- `crates/akzio-daemon/src/http.rs`
- `crates/akzio-cli/src/cli/dispatch.rs`

### 12.5 Experiment/SearchBias：验证了输入，未发现自动统计生产器

ExperimentTrial 定义候选/参数/Prompt/model/provider 身份、真实 release_date/knowledge_cutoff、Bright/identifier/calendar/fully masked/post-cutoff-forward 条件、generation/validation/holdout dataset 和时间窗、候选冻结时间、holdout access、状态与指标。

SearchBiasCertificate 定义 DSR、PBO、stationary/moving-block bootstrap、FWER、bright-vs-masked、contamination、trial refs、holdout、metric identities 等。`seal()` 是计算身份哈希后 validate，不是计算这些统计量。

Canary stage 确实验证：

- certificate canonical，source_refs 等于 trial_refs；
- selected trial 归属实际 candidate Contract/Topology；
- certificate trial_refs 与该 subject 的完整 ledger 精确对齐；
- 有显式 SearchBiasAcceptancePolicy 时逐项比较阈值；
- 无 acceptance policy 时 `is_promotion_ready` 只要求 DSR/PBO/stationary-bootstrap/FWER **存在**、holdout_access_count=1、contamination_risk != Some(true)，不是自动证明这些统计值优良；None contamination 也不是 measured clean。

本次 `crates/**/*.rs` 检索未找到自动创建 ExperimentTrial/SearchBiasCertificate 实例并计算上述统计量的生产入口；找到的主要是 domain 校验、Store 读取/Doctor、Canary 消费。Debug experiment 是另一个 fork 接口，不能混作统计证书生产器。

证据：

- `crates/akzio-domain/src/experiment.rs`
- `crates/akzio-store/src/store/experiment.rs`
- `crates/akzio-store/src/store/canary/reservation.rs`
- `crates/akzio-cli/src/cli/debug_commands.rs`

## 13. 当前工作树的静态接线限制：不能写成“自动闭环已跑通”

以下仅报告联读所得代码行为/限制，不改代码、不运行复现、不作历史 incident 归因。

### 13.1 Outcome / repair 的临时 objective 与 frozen node 校验不一致

普通 worker 把 task.node.clone() 的 objective 改为 `[outcome_horizon=...] Review outcome ...`；repair 把 objective 改为 `[outcome_horizon=t5] Repair only ...`，再传 `agents.run`。

当前 `AgentRuntime::run_inner` 读取 Store 中 frozen node，比较时只覆盖 input_artifacts、排序 dependencies，然后 `execution == *stored`；WorkflowNode 派生全字段 PartialEq，包含 objective。原普通 worker objective 是 `Seal governed T+1/T+3/T+5 Paper outcome and record evaluation.`，repair 持久 objective 则是 `[narrative_repair] Repair T5 narrative ...`，都与临时 objective 不同。

**静态推论：上述标准调用路径会在模型请求之前命中 NodePolicyMismatch。**

- 普通 Outcome 捕获非 Store ResearchError，归类 `model_or_contract_error`，可能仍写 ModelUnavailable 并数值封存；
- repair 的 `agents.run(...).await?` 传播该错误，不会凭接口存在自然修成 Complete；
- 因而第 7 节协议是真实存在的 AgentRuntime 机制，但不能说当前 daemon 自动调用路径已经成功执行该协议。

证据链：

- `crates/akzio-store/src/store/learning/outcome.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`
- `crates/akzio-store/src/store/learning/history.rs`
- `crates/akzio-daemon/src/outcome/narrative_repair.rs`
- `crates/akzio-research/src/agent/runtime_run.rs`
- `crates/akzio-domain/src/workflow.rs`

### 13.2 独立 risk assessment 有记录接口，但自动 producer 未见

collection 只读既有 assessment。无记录时数值路径可完成，risk_recall=None，canonical learning forward gate 不通过。已有 Rust-only/无风险的 sealed Outcome 也不能靠 narrative repair 重写原数值补真值。这里是明确缺测边界，不能由模型自述填补。

证据：第 6.5、8、9.1 节所列记录/消费/资格代码。

### 13.3 Shadow 复用父 collection 时的 assessment Run 绑定

Shadow 将 **shadow task** 与 **parent schedule** 一起传给 `collect_outcome_materialization`。该函数内部的 assessment 查询会找到引用 parent schedule 的记录，但校验 `artifact.origin.run_id == task.run_id`。

**静态条件推论：若父 schedule 已有合法、origin 为父 Paper Run 的 canonical risk assessment，此处会因 parent≠shadow Run 而拒绝；发生在 Shadow 后面将 risk_recall 清 None 之前。** 如果父 assessment 根本不存在，则不触发这一校验，但候选风险仍 None。未运行该分支，不声称某个实际 Shadow 已报这个错误。

证据：

- `crates/akzio-daemon/src/outcome/shadow.rs`
- `crates/akzio-daemon/src/outcome/collection.rs`

### 13.4 标准 Canary 数值不自然满足全部晋升指标

标准 collection 只取 5 Session，ES 要 20；Shadow 强制 risk=None；Canary required_metric_unmeasured 要 parent/candidate risk、evidence、process、drawdown、tail 都有值。因此**当前标准这组输入不能自然走到 Advance**：无已测退化时 Defer，有已测退化仍可按优先级 Rollback。

此外候选 target 与父 observed_execution 组合、process_quality 沿父 ExecutionContext，故这些是受控比较材料，不是候选单独的成交、费用、过程或账户 NAV 实测。不能用 parent/candidate 两栏的存在消除该来源差别。

证据：

- `crates/akzio-daemon/src/outcome/collection.rs`
- `crates/akzio-learning/src/metrics.rs`
- `crates/akzio-daemon/src/outcome/shadow.rs`
- `crates/akzio-learning/src/campaign.rs`
- `crates/akzio-daemon/src/outcome/canary.rs`

### 13.5 文档/注释或结构不能代替实际接线

- `CONTEXT.md` 版本与 12k 描述旧于当前默认；库预算与新 Run 预算分层见第 7.2 节。
- Outcome collection 文件导读提 join_all，实际四资产循环逐项 await。
- Canary 注释笼统说不激活 Contract，实际 Store 的 Active transition 能改 catalogue head；Topology Active head 也有 scheduler 的有限消费者，但只影响注册候选 topology 的 proposal 选择偏好，并不保证匹配 proposal 存在或内存 catalogue 即时切换，详见第 11.6 节及 `crates/akzio-daemon/src/scheduler.rs`。
- 默认 Lesson/Experience/CandidatePolicy 选入 Manifest 后仅进 metadata ledger，未进入正文 must_read；研究无读工具，不能把召回/归因记账写成学习正文已被模型使用，详见第 10.3 节。
- Lesson “paired-review”治理模板不等于 transition 接口实际查询量化配对证明。
- Retention/qualification/experiment/release 中可解码的字段、类型、校验方法与实际自动 producer 必须分开；详见第 12 节。
- 本文没有把上述现象标成测试失败或实际事故，亦没有提出修改方案。

## 14. canonical 与 isolated debug；DecisionPolicy 校准是另一条治理链

### 14.1 Purpose 和 Store scope 是两个条件

Debug 可以使用正式 Paper graph/purpose 并得到隔离 Outcome，但不因此成为 canonical learning：

- Store `debug_environment` 是永久标记；必须新 Store；普通 Core 不能通过关闭 debug_control 消除；
- `debug_learning_isolated` 检查 Store metadata 或 Run DebugSession.learning_scope；
- Outcome T5 debug 分支只 seal，绕开 canonical Evaluation；
- `record_policy_evaluation_fenced` 拒绝 isolated learning；
- Lesson Active/Contested 写入和转换拒绝 isolated Store；
- Broker forbidden 与 Paper approval/Gates 仍独立，不由 Outcome 开关放权。

因此一个 Artifact lifecycle 字段写 Canonical 或 RunPurpose=Paper，不能单独证明其可参与正式学习/校准；要核 Store scope、生产身份与具体 gate。

证据：

- `crates/akzio-store/src/store/debug.rs`
- `crates/akzio-daemon/src/outcome/worker.rs`
- `crates/akzio-store/src/store/learning/policy.rs`
- `crates/akzio-store/src/store/lesson/write.rs`
- `docs/debug-control.md`

### 14.2 不把 PolicySubject 学习与 DecisionPolicy 校准混同

DecisionPolicy 的 risk limits、dataset、候选、active head 属于独立 SQL/CAS 流程：

```text
只读 readiness
→ operator 显式风险限制
→ canonical Outcome collect
→ build 候选
→ inspect / validate / 显式 activate
```

collect 拒绝 isolated Store，筛 canonical Paper Decision/Outcome，要求模型 release_date/knowledge_cutoff、真实三期限标签和风险限制。NoOrder 的正式 Paper 成熟 Outcome 可以成为后续候选资料；PositionPlan 没有 Outcome。

**Rust-only T5 不取得自动 Experience/PolicySubject 学习资格，不等于所有“离线标签采集”接口都以 narrative Complete 为共同前置。** 当前 calibration collect 的相关路径检查 sealed Outcome/价格标签等，没有在这里通过 Experience.learning_eligible 或 Retrospective.status 决定样本；两个流程不要画成一个布尔开关。build/collect/readiness 从不代替 operator activate，也不证明已有 active policy。

证据：

- `docs/development-workflow.md`
- `crates/akzio-cli/src/cli/calibration.rs`
- `docs/agent-runtime-contract.md`

## 15. 数字与默认值速查

| 项目 | 数值/规则 | 主证据位置 |
|---|---|---|
| 时间窗口 | 1/3/5 个四资产共同完成 Session | `crates/akzio-learning/src/evaluation.rs` |
| daily bar availability | exchange close + 20 分钟 | `crates/akzio-ingest/src/session_bars.rs` |
| bars search | 252 根 / baseline+366 天；只取前 5 共同日 | `crates/akzio-daemon/src/outcome/collection.rs` |
| acquisition max age | 604800 秒 / 7 天 | `crates/akzio-daemon/src/outcome/collection.rs` |
| bar adapter 防护 | 16 页 / 8 MiB provider payload 上限 | `crates/akzio-ingest/src/session_bars.rs` |
| Task lease / tick | 30 秒 / 约 10 秒 | `crates/akzio-runtime/src/runtime/task.rs` |
| Outcome lease / contention | 5 分钟 / Deferred 30 秒 | `crates/akzio-daemon/src/outcome/worker.rs` |
| 成熟性 polling / catch-up | 20 分钟 / 多阶段约 1 秒 | `crates/akzio-daemon/src/outcome/helpers.rs`、`crates/akzio-daemon/src/outcome/worker.rs` |
| 失败 retry | max_attempts=2；默认 30 秒起、上限 300 秒 | `crates/akzio-research/src/agent/errors_catalogue.rs`、`crates/akzio-runtime/src/runtime/task.rs` |
| repair task 上限 | 每 Run 2 个 | `crates/akzio-store/src/store/learning/history.rs` |
| CLI 生产 workers / 库默认 | 4 / 2 | `crates/akzio-cli/src/cli/run_commands.rs`、`crates/akzio-daemon/src/worker.rs` |
| Outcome 新 Run budget | 1M in / 4k out / unlimited 工具次数维度 / 180 秒，可被 override | `crates/akzio-domain/src/budget.rs` |
| Outcome Contract baseline | 12k in / 4k out / 2 / 180 秒 | `crates/akzio-domain/src/budget.rs` |
| Draft | output 半数预留 Submit；70% wall | `crates/akzio-research/src/agent/runtime_run.rs` |
| 模型输出 bounds | ≤12 findings / 3 counterfactuals / 4 lesson_proposals / 8 gaps / 8 refs | `crates/akzio-domain/src/evaluation/outcome.rs` |
| 学习质量门槛 | completeness≥900000、risk recall≥900000 ppm | `crates/akzio-learning/src/evaluation.rs` |
| Memory forward pair 门槛 | 每 horizon≥3 fresh pairs | `crates/akzio-learning/src/evaluation.rs` |
| Forecast aggregation | 10 bins；≥30 binary samples | `crates/akzio-domain/src/evaluation/outcome.rs`、`crates/akzio-learning/src/evaluation.rs` |
| Wilson lower bound | denominator≥5，95% z≈1.95996398454 | `crates/akzio-learning/src/metrics.rs` |
| tracking/beta/Sortino | ≥5 daily samples | `crates/akzio-learning/src/metrics.rs` |
| ES | ≥20 samples；最坏 ceil(n/20) | `crates/akzio-learning/src/metrics.rs` |
| Outcome costs 默认 | transaction/slippage 两项 0；不是实际费用证明 | `crates/akzio-domain/src/evaluation/outcome.rs` |
| Lesson recall | 最多 4 条，完整显式冲突组 | `crates/akzio-context/src/context_broker/manifest.rs` |
| Operator Lesson | 90 天 / usage<100 | `crates/akzio-domain/src/lesson.rs` |
| Outcome revalidated Lesson | 30 天 / usage<20 | `crates/akzio-domain/src/lesson.rs` |
| Regime compatibility | 至少一请求 regime≥500000 ppm | `crates/akzio-domain/src/lesson.rs` |
| Canary total notional schema cap | 1000 USD | `crates/akzio-domain/src/canary.rs` |
| CanaryPromotionPolicy | 需显式配置；不能借 EvaluationPolicy 默认补填 | `crates/akzio-domain/src/canary.rs` |
| Retention scenarios | 1–256；固定 golden bank 23 项 | `crates/akzio-domain/src/longitudinal.rs` |

本表源码引用使用仓库相对文件路径和语义说明；上下文、分支条件与证据边界见前述各节。

## 16. 未核点、交付检查与最终措辞

### 未核点

1. 未打开任一 canonical 或隔离数据库：未知当前 schedule、T1/T3/T5、lease、risk assessments、Experience、Lesson、Canary campaign、active Contract/DecisionPolicy 的真实数量/状态。
2. 未读本地配置：未知有效 worker_count、Outcome 开关、override budget、成本费率、模型路由及 Canary 阈值。源码默认不代替运行配置。
3. 未调用模型或运行 fixture：未知当前 provider 能否完成两阶段、工具回读、叙事提交、repair；第 13 节是静态控制流判断，不是运行日志。
4. 未接行情/Broker：未知真实 Session 完整度、公司行动分页、baseline 可用性、任何订单 accepted/filled/reconciled；没有产生真实 Paper 或 T+5 验收。
5. 未执行 Store doctor/迁移：本文不证明既有 Store 满足 source closure、幂等历史或 schema 升级条件。
6. qualification/release/experiment 的“未找到自动 producer/consumer”以本次 `crates/**/*.rs` 调用检索及所列实际入口为范围，不排除仓库外 operator 流程；也不把仓库外未知流程当作已接入。
7. 未验证所有历史 Contract 版本的具体预算/兼容行为；历史 CAS 不重解释。

### 本次完成的检查

- 已读取工作树约束与指定业务资料，沿调用方、数值算法、Store 事务、Context recall、Canary 与相邻治理入口核查。
- `git diff --check` 实际执行通过；它检查现有 Git diff 的空白问题，不是 Rust 编译/业务测试。
- 报告更新后检查来源文件、相对链接、章节和尾部空白；不运行会初始化 Store/模型/Broker 的命令。
- 本次没有源码实现变更，故不将范围标为 `implemented`；报告静态检查可标 `offline-verified（仅报告/引用检查）`。
- `real-Paper-verified`：未执行。
- `outcome/learning-verified`：未执行。

### 面向完整中文说明的推荐事实表述（非改造建议）

> 当前系统把 T0 决策与跨交易 Session 的 Outcome 独立推进。Rust 冻结执行血缘、按 raw 市场事实重建冻结敞口收益，并把叙事、研究充分性、独立风险真值和学习影响分别审计。缺叙事仍可数值密封，但不能因此获得自动学习资格。Experience、PolicySubject 与 Lesson 不是同一生命周期；默认学习材料召回/选入 Manifest 也不等于正文已内联给模型。Canary 使用三个注册 Shadow 做受控比较，缺指标/治理材料保持不晋升，不扩展交易权限。Topology Active head 有受限的 scheduler proposal 选择消费者，但不保证可用候选图存在或已执行。当前工作树仍有明确的静态接线限制，不能把类型、接口、测试代码或局部成功说成自动闭环已端到端跑通。

报告路径：`.akzio/reports/process-map-20260929-01/05-outcome-learning.md`。


---

<!-- 分报告 6: 06-entry-observatory-debug.md -->

# 06｜用户入口、Observatory、CLI、Core、Debug 与导出操作面

核查日期：2026-09-29。对象：`/Users/alixeu/project/akzio-signal-intelligence` **当前工作树**，不是只看 HEAD。HEAD 为 `e4292f09acf5b3798bf16de26718ceb85046a190`。

## 0. 证据与执行边界

- 本次只做源码、文档、Git 状态和文件存在性核查；唯一写入为本报告。没有启动/停止 Core，没有打开 UI，没有执行 CLI 业务命令、HTTP GET/POST、Broker、模型、Doctor 或 fixture，没有读取实际 token、Keychain 或 canonical DB，没有修改源码/配置，也没有提交 Git。下文命令是接口说明，不是执行记录。
- 不标记 `implemented`、`offline-verified`、`real-Paper-verified` 或 `outcome/learning-verified`；准确标签是 **current-worktree source-inspected**。测试代码存在不等于测试已通过。证据标签定义：`docs/development-workflow.md`。
- 已先读 AGENTS、运行时契约第 12 节以及 Debug/Workflow/开发/退休文档；调度器、worker lease、事务恢复只描述操作接口含义，不深入主线负责的实现。约束：`AGENTS.md`；`docs/agent-runtime-contract.md`。
- 核查开始和报告前 `git status --short` 一致：修改了 `.github/workflows/ci.yml`、`AGENTS.md`、`README.md`、`crates/akzio-daemon/src/{http.rs,http_launch.rs,lib.rs,orchestration/health_canary.rs}`、`crates/akzio-store/src/store/debug_bundle.rs`、`docs/{agent-runtime-contract,debug-control,development-workflow,research-protocol-retirement}.md`、`scripts/run_core.py`；另有未跟踪的 `scripts/test_run_core.py`。这些不是本次修改。
- 重点工作树文件 SHA-256：`scripts/run_core.py` = `3abaa1e6b5f1239b92437804bbcd1484ef22340837605387b309f1b116b90f83`；`crates/akzio-daemon/src/http_launch.rs` = `fcb676ff23c3cc6e4bc8e5979a94855405cdf672e16bed419430e9b15ad7346f`；`crates/akzio-daemon/src/http.rs` = `e03735d4638c37974c76a38312cbb222896fe6cf1f6852583b1c3e8d5e6eb6c7`；`crates/akzio-store/src/store/debug_bundle.rs` = `c0e397aa54621df287d80f9e84e2549bd4f2e7df0656c59dcaea42bb7edd48c8`。这些指纹只对应当时读取的工作树快照；后续变化须重新核对源码行为。

## 1. 全流程总图：入口不是权威，显示不是执行

```text
原生 App                         正式运行 Python 客户端
  配置/启动受管 Core                 不启动 Core、不改配置
  └─POST /runs {purpose}────────────┘
                 │
        loopback + token + native 来源检查
                 │
      ┌──────────┴─────────────────────────┐
 PositionPlan                           Paper
 prepare + commit 正式研究图              请求已有 Paper scheduler tick
 Evidence / Research / Reviewer           真实 TradingSession / session slot
 Rust Decision                           同一研究主干 → Decision
 结束；无 Execution/Outcome               ExecutionGate → Commitment/提交
                                       Reconcile → OutcomeSchedule
                                       历史到期 Outcome 独立推进
                 │
           Rust / V2Store 持久权威
                 │
       Observer snapshot/detail + SSE invalidation
                 │
         Swift live projections → 页面

另一条独立入口：
新隔离 Debug Core → prepare（Paused）→ inspect → step / resume
                       ↑ pause / retry-node / abort（受身份和 revision 约束）
                       └─fork / experiment → 新研究身份，不复制 Paper 执行权

旁路只读：
Blueprint / inspection / checkpoint / journal / replay / doctor / export
不产生业务成功，不授予模型、Decision 或 Broker 权限。
```

对应入口证据：`crates/akzio-daemon/src/http_launch.rs`；`scripts/run_core.py`；`crates/akzio-daemon/src/debug.rs`；业务结束边界：`docs/agent-runtime-contract.md`。

## 2. App / Core 启动、配置、凭据与鉴权

### 2.1 正常 App 启动与受管 Core

1. `ObservatoryLauncher.main → ObservatoryApp → AppShell`；主窗口和独立 Run Detail 窗口由 SwiftUI 管理，AppShell `.task` 调用 `bootstrapCore()`。普通构造自动走 live；capture 构造明确 `autoStartsCore:false`，属于 Mock/离屏展示。证据：`apps/Sources/ObservatoryKit/App/ObservatoryLauncher.swift`；`apps/Sources/ObservatoryKit/App/AppShell.swift`。
2. Supervisor 先解析必填配置；缺配置时进入 `needsConfiguration`。二进制优先 `AKZIO_CORE_EXECUTABLE`，再查 Bundle 内 `Contents/MacOS/akzio-core` 和工作区 debug/release 候选。配置优先 `AKZIO_CORE_CONFIG`，否则 `AKZIO_HOME/config.toml` 或默认用户 home 下 `.akzio/config.toml`；Store 为同一 home 下 `store`。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`。
3. 默认配置不存在时，App 调 Rust `observatory-config --config ... init --template ... --store-root ...`，并不是 Swift 自己拼完整 TOML。模板为仓库 `config/akzio.observatory.toml` 或 Bundle 资源。已有默认配置不会自动迁移为新模板。证据：`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`；`crates/akzio-cli/src/cli/observatory_config.rs`。
4. Supervisor 固定使用 `http://127.0.0.1:7342`；端口已响应就报占用，**不会自动附着到现有正式 Core**。随后执行 `akzio-core --config <path> daemon serve`，工作目录为配置父目录，注入 Store 路径、stdin EOF 退出标志、模型与 Paper 凭据等环境；Paper URL 固定为原生 Paper endpoint。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`。因此任意改 TOML `http_addr` 不意味着 App supervisor 会随之改端口。
5. 启动后最多等待 300 秒，200ms 间隔轮询认证 `/ready`，再成功读取 snapshot 才置 `ready`。这是连接就绪，不是 Policy/订单就绪。App 退出调用共享 supervisor `stop()`；受管进程先关闭 stdin/SIGTERM，最多等五秒，再 SIGKILL。重启/保存配置/清理凭据同样属于真实生命周期操作，不能在只读核查时点击。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryLauncher.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。

### 2.2 凭据不是当前实现中的 Keychain

- **当前读取路径是本地 TOML + 环境占位符，不是 Keychain。** Swift `CoreCredentialStore` 调 Rust CLI `get/set`，`get` 的编辑投影含模型 API key、Alpaca key/secret 等；`set` 将其写回 TOML。源码扫描未发现 `SecItem`/Keychain 存取调用。`import Security` 实际用于 `SecRandomCopyBytes` 生成 daemon token，不能据此讲成 Keychain 存储。证据：`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`；`crates/akzio-cli/src/cli/observatory_config.rs`；`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`。
- 文件权限隔离不等于加密：home/config 父目录 0700，配置临时文件 0600 后 rename；token 文件为 Store Root 的 `.daemon-token`，App 校验非空、无换行并收紧 0600，首次创建使用 32 字节安全随机数。Rust daemon 首次创建用独占临时文件和 hard-link 发布；**CLI 观察客户端只读已有 token，不创建、不 chmod**。证据：`crates/akzio-cli/src/cli/identity.rs`；`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-cli/src/http_client.rs`。
- `observatory-config get` 不能当成安全的公开诊断输出；它会返回编辑凭据。本次未调用。Core 日志路径与权限管理见 `apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`、`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；不能自动把日志/配置加入分享 ZIP。

### 2.3 loopback / token / Origin / CORS 的真实范围

- Core bind 和已有 listener 均校验 `is_loopback()`；通用 `authorize()` 精确匹配 `x-akzio-token`，失败 401。`/health`、`/ready`、Observer、SSE、inspection 等也需要认证，不是公开健康页。证据：`crates/akzio-daemon/src/http.rs`。
- 正式 `POST /runs` 和 Debug mutation 额外拒绝任意 `Origin`，以及 `sec-fetch-site != none`；Debug mutation 还要求 Debug enabled。**不是所有 handler 都有 Origin 检查**：一般 GET 及旧 control 路由主要只调用 token `authorize()`。当前 Router 未安装允许跨源的 CORS layer、未注册统一 OPTIONS 放行。不能讲成“所有接口做同一 Origin 校验”，也不能把 CORS 代替认证。证据：`crates/akzio-daemon/src/http_launch.rs`；`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-daemon/src/http.rs`；`crates/akzio-daemon/src/http_runtime.rs`。
- Rust CLI 和 Python 明确禁用代理及重定向；Swift 为 ephemeral URLSession + 拒绝重定向，初始 endpoint 限定 HTTP loopback，但没有同样显式设置 `no_proxy`。不要把 Rust/Python 的代理策略移植成 Swift 的已证实行为。证据：`crates/akzio-cli/src/http_client.rs`；`scripts/run_core.py`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`。

### 2.4 CLI Core 启动的副作用前置

- `daemon serve` 先加载 Policy、校验 Debug policy 前置，再调用真实模型 capability probes，构造 runtime identity 和 Daemon；**“仅启动 Core”也可能调用模型，不能用于本次只读验收。**生产 worker 默认是 CLI 的 **4**，不是 WorkerPool 类型的默认 2。`auto_paper=true` 以 **30 秒**间隔启动 scheduler；否则只启动 HTTP + workers。证据：`crates/akzio-cli/src/cli/run_commands.rs`。
- `manual_paper=true` 与 auto 共用 Broker/runtime identity/成本/feed/profile 启动校验，但不自动创建新 T0。App 模板 `auto_paper=false, manual_paper=true`。`/ready` 的 `store_scope` 来自 Store 隔离标记，`formal_run_bundle_supported` 仅 canonical 非 Debug 为 true；`ready()` 检查的是当前服务能力，不证明任何 Run 成功。证据：`config/akzio.observatory.toml`；`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-daemon/src/orchestration/health_canary.rs`。

## 3. 正式 `POST /runs`：PositionPlan / Paper / auto_paper

| 入口/条件 | 实际含义和输出 | 来源文件 |
|---|---|---|
| App 模式选择 | 用户入口仅 `.positionPlan`、`.paper`；请求只编码 purpose，返回 Run ID 后转 Workflow；按钮“已受理”不是完成。 | `apps/Sources/ObservatoryKit/PresentationModels/DomainVocabulary.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift` |
| 请求协议 | `RunLaunchRequest { purpose }` 且 `deny_unknown_fields`；旧通用 workflow/fixture 字段不能混入。非 PositionPlan/Paper 拒绝。 | `crates/akzio-daemon/src/http_launch.rs` |
| 正式 PositionPlan | 服务端取当前美东日期作研究 session，`prepare_position_plan → commit_position_plan`；不使用 Paper slot，也不是让客户端选任意图。每次成功 POST 创建新研究 Run，应避免丢响应后重发。 | `crates/akzio-daemon/src/http_launch.rs`；`scripts/run_core.py` |
| 正式 Paper | 要有 Paper observer 和 runtime identity；用 `AlpacaPaperSessionClock` 调已有 scheduler tick，返回 reservation 对应 Run ID；未获 slot 返回拒绝原因。手动请求不会修改 auto_paper。 | `crates/akzio-daemon/src/http_launch.rs` |
| TradingSession | clock adapter 使用真实 `market_clock` 的 `session.kind != Closed` 和 `session_date`；不是只允许 Regular/`clock.is_open=true`，不是 Python 用本机日期猜交易日。 | `crates/akzio-daemon/src/scheduler.rs` |
| auto_paper | 自动发现新 T0 的独立开关；手动 Paper 使用同一 scheduler/session slot 规则，不能把按钮与 auto 设置等同。 | `crates/akzio-cli/src/cli/run_commands.rs`；`docs/development-workflow.md` |
| Debug Core | 正式 `/runs` 在 Debug enabled 时 409，必须走隔离 prepare；不能切 UI 模式解除 Store 隔离。 | `crates/akzio-daemon/src/http_launch.rs` |
| 缺 active Policy 的正式 PositionPlan | 仍可有 `research_plan.raw/validated`，正式 `Decision.targets` 全零并结束；无 ExecutionVerdict/Commitment/Outcome。 | `docs/agent-runtime-contract.md`；工作树测试断言 `crates/akzio-daemon/src/http_launch.rs`（本次未执行） |
| 缺 Policy 的正式 Paper 冷启动 | 独立的零目标/NoOrder/OutcomeSchedule 路径；不是 PositionPlan 自动切 Paper，不代表具备下单授权或已成交。 | `docs/development-workflow.md`；工作树测试 `crates/akzio-daemon/src/http_launch.rs`（本次未执行） |

时段关闭后的延期、恢复刷新快照和重新 Gate、accepted/new/partially_filled 与最终成交的区别，按运行时契约第 12 节向主线衔接即可，不在此展开执行引擎：`docs/agent-runtime-contract.md`。

## 4. `scripts/run_core.py`：现有 canonical Core 的一次运行客户端

### 4.1 参数与前置检查

- 公开参数只有可选正式配置路径、`--mode position-plan|paper`（默认 position-plan）、`--keep-artifacts`。没有 `--research-only`、`--faker-online`、`-fakerOnline`、启动 daemon、关闭 daemon 或拷贝 Store 的参数。`run()` 内部有默认 1800 秒观察窗口、10 秒轮询，但不是公开 CLI 参数。证据：`scripts/run_core.py`。
- 默认配置为用户 home 下 `.akzio/config.toml`；拒绝 `debug_control=true`、非回环 IP/无端口/URL userinfo 等，要求 `store_root` 为绝对路径；当前进程 `AKZIO_STORE_ROOT` 若与文件不一致也拒绝。只读取既有 token，缺失时提示先启动 Core。证据：`scripts/run_core.py`。
- 创建报告目录和发 Run 前先 GET `/ready`，核对 `status=ok`、`store_scope=canonical`、`formal_run_bundle_supported=true`。Paper 还检查原配置启用了 manual 或 auto。旧 Core/隔离 Core/未 ready 都在付费研究创建前失败。**这不是对远端 Store 路径做独立证明**：依赖配置 token 绑定和 server 返回的 scope，并未比对 server 返还的绝对 Store 路径。证据：`scripts/run_core.py`；`crates/akzio-daemon/src/orchestration/health_canary.rs`。

### 4.2 单次提交、观察和不确定结果

- 仅发送一次 `POST /runs`。5xx、断连、超时、JSON 不完整或缺 Run ID 都可能“已提交但响应丢失”，报告 `unknown_after_post`，**不自动重试**。尤其 PositionPlan 不是客户端幂等请求；先用正式 Run 查询确认。Paper 同 Session 复用属于 Rust，不属于 Python 的重试算法。证据：`scripts/run_core.py`。
- 轮询 `/runs/{id}/replay`，核对 run_id/purpose。PositionPlan 遇 `decision_completed` 或合法终态即可；Paper 遇 Outcome 已 scheduled 时可以结束 T0 观察并报告 `t0_completed_outcome_pending`。Paper 单到 Decision 不算完整；PositionPlan 的 execution-rejection 状态反而视为不匹配。窗口到期报告 pending，不暂停、取消、停止或复制 Core。证据：`scripts/run_core.py`。
- 再读取 Observer detail；观察失败/有界投影漏 artifact 与“没有订单/已完成”严格分开。summary 和终端输出分别列 proposal 审查状态与引用、validated research allocation、正式 Decision targets、ExecutionVerdict、receipt 中的 state/filled_quantity、OutcomeSchedule、numeric seal 和 retrospective status；不会把 receipt_observed 标为 filled。证据：`scripts/run_core.py`。
- 当前 summary 的 `paper_commitment` 在 Observer 白名单限制下可能一直 `not_observed_in_bounded_projection`（详见第 9 节）；它不读 canonical DB 补结论。脚本同样没有把 raw 研究配置完整打印出来：summary 仅摘要 validated，raw/adjustments 应在授权 bundle 的 Decision/DecisionContext 中查。证据：`scripts/run_core.py`；`crates/akzio-daemon/src/observer/broker.rs`。

### 4.3 报告、ZIP 和退出码

- 在仓库 `.akzio/` 下新建 `position-plan-<UTC timestamp>-*` 或 `paper-*` 临时报告目录；umask 077。通过认证 POST `/control/store/export-run-bundle` 让 Core 写 `bundle/`，再核对文件 `EXPORT_STATUS` 与返回 manifest；本地摘要另做秘密值替换。**这是导出文件副作用，不是 Store mutation，也仍属于 POST；本次未调用。**证据：`scripts/run_core.py`。
- ZIP 只收报告根的 JSON 和 Core bundle 文件，拒绝 symlink，校验 checksum 清单的路径、完整文件集合和每个 SHA-256，再以 exclusive 模式建 ZIP 并 `testzip()`。Core bundle 字节不二次替换，否则破坏 hash；不要宣称 Python 又独立做了一遍 Rust bundle 内容脱敏。证据：`scripts/run_core.py`。
- `EXPORT_STATUS=partial` 可能只是按权限省略 raw payload；`_export_issue` 将 missing/corruption/dropped/unknown_after_crash 视为异常，而不是把所有 uncaptured 都当 Run 失败。**T0 完成、导出完整、订单提交、成交、Outcome 成熟为五个不同结论。**证据：`scripts/run_core.py`。

| 退出码 | 当前判断 |
|---|---|
| 0 | 本次 T0 完成，Observer 可用，PositionPlan 的 Decision/research_plan 观察满足要求，导出无异常缺失；允许因授权省略而 partial。 |
| 1 | 未启动、运行失败/取消或提交/观察出错；提交不确定不能读成“未创建 Run”。 |
| 2 | 已完成但观察不全/导出异常，或原拟返回 0 的归档失败。 |
| 3 | Run 仍 pending/等待，脚本不请求取消。 |

退出码证据：`scripts/run_core.py`。ZIP 成功后默认只删除本次临时报告目录，`--keep-artifacts` 保留该目录；归档失败保留目录。任何选项都不删除正式 Run/Store/Core。证据：同文件 `440-451`。

## 5. Observer / SSE / 页面：投影的来源、范围与误读风险

### 5.1 Rust 返回什么，Swift 如何续接

- snapshot schema 2 包括 generated_at、event_cursor、Core/approval、current_run、recent_runs、portfolio、outcome、learning。最近 Run 上限 20，Run events 100，trajectory 200，learning 100；current_run 是最近 workflows 的第一项，**不保证是刚点击启动的 Run**。证据：`crates/akzio-daemon/src/observer.rs`；`crates/akzio-daemon/src/observer/snapshot.rs`。
- Run detail 的 artifacts 从有界 trajectory 提取并按类型白名单反序列化，包含 Claim/Critique/DecisionProposal/DecisionContext/Decision/ExecutionContext/Verdict/Plan/OrderReceipt/Reconciliation/Outcome 等；并非任意 CAS 浏览器，不输出 RawEvidence 或原始 AgentTurn/Tool payload。证据：`crates/akzio-daemon/src/observer/broker.rs`。
- `ObserverSection` 的 available/pending/unavailable、reason、observed_at/data 要一起读；空数据不等于数值 0。尤其 snapshot/portfolio-history 虽为 GET，**服务端可能真实调用 Broker GET 与行情 adapter**，不是纯本地 DB 读；本次禁止 Broker，因此也未请求这些 Observer GET。证据：`crates/akzio-daemon/src/observer.rs`；`crates/akzio-daemon/src/observer/broker.rs`；`crates/akzio-daemon/src/observer/snapshot.rs`。
- 全局 `/v1/observer/events?after=` 每 500ms 看 Store cursor，发送 `invalidate`（带 id/cursor），另发无 durable cursor 的 reasoning-start/delta/end 广播；它不是全局 durable 事件正文流。单 Run `/runs/{id}/events?after=` 每 200ms 分页输出持久事件 `event:akzio`，附带该 Run 的 reasoning 广播。cursor 是观察水位，不是队列 ack/执行许可；服务端看 query `after`，不是 Last-Event-ID。证据：`crates/akzio-daemon/src/http.rs`。
- Swift 每轮先取 snapshot、Debug listing、曲线，再从 snapshot cursor 订阅 SSE；invalidate 重新拉 snapshot，reasoning 仅合并本地临时记录；出错时无旧投影→offline，有旧投影→stale，退避 1→2→4…最多 15 秒。重连不重发 Run POST；取消 SSE 不取消 Run。证据：`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`。
- **没有完整无损重放保证**：全局 invalidation 合并变化，reasoning 广播可以 lag/drop；Swift 未单独处理 `event:error`，落入忽略分支。snapshot 的业务读取与末尾 cursor 读取分处两次 StoreExecutor 调用，中间还读 Broker，不能把它讲成“内容与 watermark 同一 SQLite snapshot”。精确事件追溯用 journal/单 Run SSE/导出。证据：`crates/akzio-daemon/src/http.rs`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`；`crates/akzio-daemon/src/observer/snapshot.rs`。这是静态机制限制，未复现实例。
- CLI `run events --after` 打开一次流，只解析/打印 data；没有 App 的自动重连循环或自动保存 cursor。有限分页是 `run journal`，不是 `run events`。证据：`crates/akzio-cli/src/http_client.rs`；`crates/akzio-cli/src/cli/dispatch.rs`。

### 5.2 七个页面的讲解地图

页面定义和顺序为 Overview、Workflow、Intelligence、Portfolio、Outcome、Learning、Run Archive；Settings 为覆盖层。证据：`apps/Sources/ObservatoryKit/App/AppRoute.swift`。

| 页面 | 应讲什么 / 不应推断什么 | 来源文件 |
|---|---|---|
| Overview | 当前 Run、agents、health 和最近事件；服务 ready、Policy capable、approval 状态分开。live 无数据不自动退回 Mock。 | `apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift` |
| Workflow | 任务 DAG、task 状态、role/horizon、stage inspector、research audit；另有 Runtime Inspector。纯 Rust 节点显示 Rust/N/A，不虚构模型。普通 inspector 的“模型输出”只列指定模型 artifact，JSON 限 80 行/6000 字符，不等于完整审计导出。 | `apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift` |
| Intelligence | 基于同一 Run 的 trajectory、已验证 artifact、模型公开返回的 deliberation、工具生命周期和临时 reasoning；confidence/alternative score 为模型说明，不是经校准正确率。 | `apps/Sources/ObservatoryKit/LiveData/LiveIntelligenceProjection.swift` |
| Portfolio | Broker 当前账户/持仓；PositionPlan 的 target 来自 `DecisionContext.research_plan.validated`，副标题明确 research target / execution N/A；Paper target 来自 ExecutionPlan。Orders 来自持久 receipt，Fills 来自 Broker activity 投影，两者不同。 | `apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift` |
| Outcome | 显示 T1/T3/T5 已有窗口与完成 session 进度，不用自然日推算；聚合选已有 Outcome 中窗口最多/最近的一份，可能属于历史 Run，而非页头 current_run。服务端 `actual_account_nav_available=false`，比较路径不能叫真实账户 NAV。 | `crates/akzio-daemon/src/observer/runs.rs`；`apps/Sources/ObservatoryKit/LiveData/LiveOutcomeProjection.swift` |
| Learning | 汇总 Outcome/Retrospective/Experience/Evaluation 和 transition，保留 pending/unavailable；候选/impact 展示不表示 Active Lesson 或 DecisionPolicy 已激活。独立数值 seal、叙事有效性、学习资格要分开。 | `crates/akzio-daemon/src/observer/runs.rs`；`apps/Sources/ObservatoryKit/LiveData/LiveLearningProjection.swift`；`CONTEXT.md` |
| Run Archive | 列表是有界 recent_runs 的展示，可选行再 fetch detail；完成率包含合法终态，非盈利率。复制 ID/模型、查看详情、独立窗口不是“重新运行/导出”。 | `apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/Features/Archive/RunPreviewPanel.swift` |

### 5.3 必须逐级区分的业务数据

1. **研究 raw**：`ResearchPlanReview.raw` 是模型原始研究配置，**不是 RawEvidence、模型 raw transport 或隐式思维链**。
2. **研究 validated**：Rust 研究层检查后保留/调整的配置；有 adjustments/status/execution_status/execution_blockers，非零不表示可执行。
3. **正式 targets**：`Decision.targets` / `DecisionContext.target` 是执行侧目标，可在研究 validated 非零时仍为零。
4. **ExecutionVerdict**：Rust wire 为 `{"verdict":"accepted",...}` 或 `{"verdict":"no_order","no_order":...}`；是 Gate 结果，不是订单。
5. **Commitment / submit / receipt**：Commitment 的持久化先于请求；receipt observed 仅证明有回执，要看 state/filled_quantity。
6. **Fill / Reconcile**：accepted/new/partial 不等于最终 filled；只读账户/成交 activity 也不能替代 Store 中正式 Reconcile 状态。
7. **OutcomeSchedule / sealed Outcome / narrative / learning**：分别是安排、成熟数值窗口、模型叙事和学习处理；T0 结束可以仍等未来 session。

字段证据：`crates/akzio-domain/src/decision.rs`；`crates/akzio-domain/src/execution/effects.rs`；提交与后评估边界：`docs/agent-runtime-contract.md`。页面并未为以上每一层提供独立完整面板；精确 raw/validated/targets 应看授权 artifact/bundle，不能只看图标。

## 6. 独立 Debug：准备、检查、控制、重试与新实验

### 6.1 隔离和 Policy 前置

- 操作规范要求新 Store 位于仓库 `.akzio/`，`debug_control=true, auto_paper=false`；不能打开用户 canonical Store。代码在 Store 层拒绝 `$HOME/.akzio/store`，首次标记须 Run 数为零，隔离标记永久，普通 Core 重开拒绝。**“必须放仓库 .akzio”是操作规则；当前 Store guard 不是全路径必须属于仓库 .akzio 的校验器。**证据：`docs/debug-control.md`；`crates/akzio-store/src/store/debug.rs`；`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- App 通过同时设置 `AKZIO_DEBUG_ENDPOINT` 和 `AKZIO_DEBUG_STORE_ROOT` 连接已有隔离 Core；只读取所指 token，查 Debug listing 要求 enabled 和 store_identity，直接进入 Workflow，不启动默认受管 Core。连接失败保留 offline。证据：`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。
- 真实 `daemon serve` 在模型 probe 前校验冻结 Policy；有 Policy 时须 decision-capable/status/input hash/artifact 和 Synthesizer Contract 匹配。没有 active head 的非 auto 研究模式有例外。针对**真实 Debug PositionPlan**，无 Policy 的身份 `research_only_without_policy()` 只允许手动研究，禁止 resume 或直接 step Decision。不能把这个限制外推成“所有正式 PositionPlan 都不能 Decision”，也不能把它无差别表述成所有 Debug purpose 的同一判定。证据：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-domain/src/debug.rs`；`crates/akzio-daemon/src/debug.rs`。
- 三种 preflight 不同：`debug preflight` 默认真实 capability probe，`--resolve-only` 仅解析路由；`calibration preflight --scratch` 没有模型调用，但会 `Store::open(scratch)`，不是零文件写入的纯只读命令；`calibration readiness --store` 用既有 Store 只读检查。不能根据 preflight 名称就执行。本次均未调用。证据：`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

### 6.2 prepare → inspect

- `POST /v1/debug/runs` 接受 session_key/purpose/paper_allowed，拒绝未知字段；purpose 默认 Paper，CLI 为 `paper|position-plan`，JSON 为 `paper|position_plan`。PositionPlan 不得 `paper_allowed=true`。prepare 发布图、Need、冻结身份和 paused 控制 head，**不执行首节点**。Paper 同 session 已存在时校验 runtime identity/Broker policy，一致才返回原 session；PositionPlan 不占 Paper slot。证据：`crates/akzio-daemon/src/debug.rs`；`docs/debug-control.md`。
- GET listing 返回 enabled/store_identity 和最近 100 workflows 中的 Debug sessions；GET inspect 可带 task/attempt，展示节点、尝试、预算、产物引用、事件、Acceptance、inspection 和 allowed_actions。局部 task/attempt 查询不会假装完整研究进度。inspect 不重跑节点；RawEvidence 被排除，其他 JSON 递归脱敏，损坏 JSON 可能显示 Null。证据：`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-store/src/store/debug.rs`。
- 新身份包含 store identity、run purpose、real/fixture、broker policy、learning scope、code/runtime identity、policy hash/artifact、contracts、dataset 与 parent lineage；界面不能就地修改这些授权维度。App prepare 总是 `paper_allowed:false`，仅 CLI 有显式 `--paper-allowed`。证据：`crates/akzio-daemon/src/debug.rs`；`apps/Sources/ObservatoryKit/LiveData/DebugPayloads.swift`；`crates/akzio-cli/src/cli/debug_commands.rs`。

### 6.3 控制矩阵（不是新增执行引擎）

共用 `POST /v1/debug/runs/{id}/control`，请求为 `{action, expected_revision, task_id?}`；token/native/Debug 校验在 HTTP，revision 与身份在 Store。普通 canonical Run 不因此获得 Debug 权。证据：`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-domain/src/debug.rs`。

| 操作 | 前置/结果 | 当前用户入口 |
|---|---|---|
| step | Paused、无在途任务、精确 TaskId、依赖/due/identity 合法，授权一个调度单位；成功/失败/Deferred 后暂停，不自动跑 sibling 或无限重试。 | CLI `debug step ... --task ...`；App 节点单步按钮。 |
| pause | 阻止新 claim，清未消费 permit；在途自然收尾，Running→PauseRequested→Paused，不杀执行 future。 | CLI 和 App。 |
| resume | 同身份、Paused→Running/continuous；缺 Policy 的真实 Debug PositionPlan 拒绝；不是清空成功产物/预算。 | CLI 和 App。 |
| retry-node | Paused 且原重试策略仍允许、Task 已可领取/due；原 Task 新 attempt，不把 succeeded 节点重跑，不扩充预算。 | CLI 和 App。 |
| abort | 必须先 pause 并排空，仍有 running 返回 `pause_and_drain_before_abort`；控制状态 Aborted，不替代订单撤销/业务恢复。 | **领域 enum + HTTP API 有；CLI 无 `debug abort`，App 无按钮。** |
| run_until / run-until | 当前 DebugAction、CLI、App 均没有；resume 是 continuous，不是“精确运行至某节点”。 | **不存在，不能编造命令。** |

动作代码：`crates/akzio-store/src/store/debug.rs`；调度单位结束语义：`docs/debug-control.md`；CLI 全枚举：`crates/akzio-cli/src/cli/debug_commands.rs`；App 按钮：`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。

- CLI 先 inspect 取 revision，再只 POST 一次；step/retry 默认等 300 秒、最多 3600 秒，以 250ms 轮询 inspect。超时不撤回 permit、不再授予。App `debugBusy` 串行化请求，CAS 冲突后只刷新、不猜 revision。证据：`crates/akzio-cli/src/cli/debug_commands.rs`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。
- runtime identity 改变仍可 inspect，只保留 pause/abort；相关节点不可 step/retry。Outcome processing 关闭/adapter 不可用时相关 step/retry 返回不可用；这些不是“模型失败”的同義词。证据：`crates/akzio-daemon/src/debug.rs`。

### 6.4 fork / experiment / acceptance / Broker

- `debug fork --task <successful_task>` 要有真实成功 attempt/output；`debug experiment` 不带 task，用父 WorkflowGraph 作 lineage。都调用 fork API，要求非空 reason，新 experiment_id；相同 ID、父/Task/reason 一致可返回既有结果，冲突拒绝。重采 governed evidence，不把父成功结果复制成新成功。**Paper parent 明确拒绝 fork/experiment，须 fresh prepare 与 Session reservation。**证据：`crates/akzio-daemon/src/debug.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`。
- App 显示“成功阶段 fork / new experiment”，但按钮只按连接/busy/reason 禁用，并没有完全复制服务端 succeeded/Paper 检查；可见按钮不是可执行授权。证据：`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。
- Acceptance POST 只追加 typed checks/evidence，path Run ID 必须一致且为 DebugSession；记录成功不改变 workflow/Gate。UI 单独列 Business / Test / NOT_RUN，不能用业务 succeeded 自动生成 PASS。证据：`crates/akzio-daemon/src/http_debug.rs`；`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。
- Broker 默认 Forbidden，显式 PaperAllowed 只是解除 Debug 额外限制，仍需审批/全部 Gate/Commitment；SimulatedOnly 仅历史解码，执行禁止。学习 scope 默认 Isolated，不能激活 canonical policy/Active Lesson。证据：`crates/akzio-daemon/src/debug.rs`；`crates/akzio-domain/src/debug.rs`；`crates/akzio-execution/src/paper_dispatch.rs`；`docs/debug-control.md`。

## 7. Blueprint / inspection / checkpoint / journal / replay / Doctor

| 能力 | 入口与可见内容 | 边界与证据 |
|---|---|---|
| Blueprint 预览 | GET `/v1/workflows/blueprint?purpose=position_plan|paper|shadow`；CLI `workflow blueprint --purpose ... --format json|mermaid`。 | 当前已安装定义的同一编译器输出，不创建 Run/Need/模型。`crates/akzio-daemon/src/http_runtime.rs`；`crates/akzio-cli/src/main.rs` |
| Run inspection | GET `/v1/observer/runs/{id}/inspection`；CLI `run inspect`。 | 持久 graph、blueprint、共享 control、checkpoint、recovery、allowed_actions；普通 Run 的动作列表为空。`crates/akzio-store/src/store/run_control.rs` |
| checkpoint | CLI `run checkpoint` 实际复用 inspection，仅打印 checkpoint 字段；无独立 checkpoint POST/恢复命令。 | 不是进程 dump，不能脱离 Store 重启副作用，也不保证 external exactly-once。`crates/akzio-cli/src/cli/dispatch.rs`；`docs/workflow-runtime.md` |
| journal | GET `/v1/observer/runs/{id}/journal?after=0&limit=100`，支持 task_id/attempt_id；after≥0，limit 1..500。 | 返回 events、next_cursor、has_more，以及 artifact/source refs；没有原始模型/工具 payload。`crates/akzio-daemon/src/http_runtime.rs`；`crates/akzio-store/src/store/run_control.rs` |
| Runtime Inspector UI | Workflow 面板区分“本次冻结定义”与“当前预览”，展示 recovery/checkpoint，手动每页 50 条 journal。 | 切 Run 清本地 cursor，结果按 Run ID 防过期写回；读取不推进控制。`apps/Sources/ObservatoryKit/Features/Workflow/RuntimeInspectorPanel.swift`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift` |
| replay | GET `/runs/{id}/replay` / CLI `run replay`。 | 重建/核验持久图和 lifecycle，返回 status/revision/task_count/terminal_task_count/cursor/cancel_requested；terminal 包括 Failed/Cancelled/Skipped，不能当成功节点数。不是重新调用模型。`crates/akzio-daemon/src/orchestration/control.rs` |
| trajectory / retrospectives | GET `/runs/{id}/trajectory`、`/retrospectives`；对应 CLI。 | 读取持久审计和回顾，不安排补跑；不是 raw-model dump。`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/cli/dispatch.rs` |
| Doctor | GET `/control/store/doctor` / CLI `store doctor`。 | 认证 HTTP maintenance；检查 SQLite/BLOB/hash/外键/事件/来源/控制 checkpoint，不修复、不证明预测正确或 Paper 成交。`crates/akzio-daemon/src/http.rs`；`crates/akzio-store/src/store/doctor.rs` |

Blueprint definition hash 仅标识控制结构，不保证模型逐字重现；完整 provenance 还需 graph CAS、RuntimeManifest、Contract/Prompt、模型记录与证据快照。历史 Run 可无 checkpoint，不伪造回填。证据：`docs/workflow-runtime.md`。

## 8. 导出与其余 CLI / HTTP 操作面

### 8.1 三种“导出”不能混为一个安全等级

| 类别 | 当前入口 | 输出/权限 |
|---|---|---|
| 正式分享安全 Run bundle | 新 POST `/control/store/export-run-bundle`；当前 Python 使用它。 | 只允许非隔离、非 DebugSession 的 Paper/PositionPlan；formal raw_access 强制 false，不授予 raw 权限。HTTP 新入口尚无独立 `store export-run-bundle` CLI 子命令。 |
| Debug 诊断 bundle | POST `/control/store/export-debug-bundle`；CLI `debug export-bundle <id> --out <new-dir>`，可 `--store <root>` 离线。 | 隔离 session/store/run/purpose/learning-scope 匹配才可包含脱敏 provider detail；历史 RunPurpose::Debug 无 session 有显式兼容读取分支。普通 formal Run 经这一 exporter 仍识别为 formal-safe，不因此获得 raw 权限。 |
| 旧 `store export-run` | POST `/control/store/export-run`；CLI `store export-run <id> <target> [--include-raw-model]`。 | 导出到独立 SQLite payload 库与清单；raw-model flag 有身份检查，但默认隐藏的只有 AgentTurn/ToolCall/ToolResult，**不能等同新分享安全 bundle**，可能包含 RawEvidence/embedded blobs。不要放入正式脚本 ZIP。 |

证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-store/src/store/debug_bundle.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-cli/src/main.rs`；`crates/akzio-store/src/store/blob.rs`；`crates/akzio-store/src/store/free_paper_checks.rs`。

- Debug 离线 `--store` 用 `Store::open_existing`，不启 daemon/模型/迁移/修复；但 CLI main 仍先加载常规配置再 dispatch，因此**不是完全 config-free**；本次不能选 canonical Store。证据：`crates/akzio-cli/src/cli/main.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`。
- 分享 bundle 从单一 SQLite Deferred 读事务提取事实，释放事务/连接后写目标文件；目标须不存在且不能位于 Store Root 内，允许创建父目录，后段错误可能留下部分目录。导出不调用模型、不抓新证据、不重跑 Decision。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- 主要文件：workflow/tasks_attempts/model_routes；timeline/llm_calls/tools/rust_decisions JSONL；llm_transcript/rust_decisions/SUMMARY/README Markdown；evidence_status/research_review/context_coverage/draft_submit_coverage/stage_acceptance/decision_matrix/policies_and_risk/failures_and_missing JSON；artifacts/ 和 artifact_index；manifest、EXPORT_STATUS、checksums.sha256。文件名 `draft_submit_coverage` 仍可用于历史/Outcome，不意味着新研究恢复 Draft。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- 各 artifact 分开记录 source hash 与 export payload hash；脱敏后两者可能不同。raw/跨 Run 未授权 payload 写 omitted marker，不能补造；跨 Run payload 须匹配授权 dataset/parent refs。导出对嵌套敏感字段、headers/cookie/key/secret、encrypted continuation 和可识别 credential text 递归替换；它不是密码学上保证发现任意秘密字符串的检测器。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- `partial` 可来自授权省略，也可来自 crash unknown、缺 blob/记录，必须看 manifest 分类。未配对 AgentTurnStarted 记 unknown_after_crash，不假定失败/成功。transcript 只含已持久化 provider-visible 材料，不含 hidden chain-of-thought。证据：`crates/akzio-store/src/store/debug_bundle.rs`。

### 8.2 CLI 和 HTTP 操作清单（均未执行）

- CLI 默认配置是仓库相对 `config/akzio.toml`，**不是** App/Python 的用户配置；讲解运行命令时应显式指定目标配置。CLI 主命令含 Workflow/Debug/ObservatoryConfig/Daemon/Run/Store/Canary/ModelQualification/Calibration/Evidence；部分本地命令在主函数先分派。证据：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/main.rs`。
- **只读 HTTP 操作**：health/ready；observer snapshot/run/history/events；blueprint/inspection/journal；run replay/trajectory/retrospectives/events；store doctor/inventory/metrics/executor/alerts/session/release-evidence/events/lessons；canary status；debug list/inspect。注意 snapshot/history 可能 Broker GET，Doctor 为完整 Store 检查；HTTP-only `/control/store/executor`、`/control/store/events/{id}` 不代表已有同名 CLI。证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/main.rs`。
- **状态变更 HTTP 操作**：正式 run launch、run cancel/retry/repair-narrative、freeze/unfreeze、paper-approval、canary stage/resume、lesson add/transition、Debug prepare/control/fork/acceptance。**文件操作 POST**：backup/restore/三类 export；restore 尤其不是观察。证据：同一 Router `crates/akzio-daemon/src/http.rs`，handlers `499-590,760-846,877-884,1209-1226`。
- 普通 `run retry` 是为合法终态 PositionPlan 创建**新 Run**，不是 Debug retry-node/resume；Paper 拒绝 operator retry，Debug Core 要求专用 Debug 命令，退休 workflow 先拒绝。`run cancel` 是业务取消请求；不等于 Debug abort 或撤销已完成订单。证据：`crates/akzio-daemon/src/orchestration/control.rs`。
- `run repair-narrative` 只入队 sealed T5 narrative repair，返回 task_id，不代表 repair 完成或 Policy 激活。`store approve-paper` 参数含 session/operator/reason/notional/valid_hours/qualification_report，还会真实模型 capability probe；purpose=paper 不替代此批准。证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/observatory_config.rs`。
- Calibration readiness/inspect 与 collect/build/activate 不同，build 候选不自动激活；`set-risk-limits` 要 operator 明确输入。Evidence market-audit 会创建新审计输出和调用行情 GET，debug verify-fixture 会创建隔离 Store 并执行离线图，均不是本次允许的“只读源码核查”动作。证据：`docs/development-workflow.md`；`crates/akzio-cli/src/cli/main.rs`。

## 9. 文档漂移、已删入口与当前显示缺口

以下是**本次静态核查发现**，不是 UI 复现或安全漏洞实测；不在本任务中修复。

| 项目 | 当前证据与讲解修正 |
|---|---|
| CONTEXT 版本过旧 | `CONTEXT.md` 仍写 Store17 / Contract67 / Prompt37 / candidate68；当前 `crates/akzio-store/src/store/prelude.rs` 为 Store18，`crates/akzio-research/src/agent/catalogue.rs` 为研究69 / bundle38 / candidate70。运行时契约已有当前版本：`docs/agent-runtime-contract.md`。 |
| 退休清单版本更旧 | `docs/research-protocol-retirement.md` 仍写65/36/66；这是历史协议整合节点，不是当前活动版本。当前版本同上，不改写历史冻结身份。 |
| CONTEXT 12k input 描述过旧 | `CONTEXT.md` 写模型调用仍限12k。`versioned_contract_budget` 的 Outcome 历史默认仍12k，但新 Run 的 `default_agent_budget` 把 input 设为1,000,000，并有配置覆盖；不能混用历史 Contract budget、新 Run budget、Context grant 和 provider 单请求上限。证据 `crates/akzio-domain/src/budget.rs`。 |
| “九节点”漂移 | `docs/research-protocol-retirement.md`、`README.md`、CLI help 注释 `crates/akzio-cli/src/cli/debug_commands.rs` 仍称九节点；当前 fixture 成功条件明确 `nodes.len()==21`（同文件392-394）；开发 workflow 47 行写默认21，runtime 文档7行写 PositionPlan21/Paper25，修订配置可改变数量。 |
| README 旧 fixture-controller | `README.md` 仍建议加 `--fixture-controller`；当前 CLI 无该参数，且专门有拒绝旧参数测试 `crates/akzio-cli/src/cli/debug_commands.rs`。不要照旧命令执行。 |
| `/runs` 退休文字易误读 | `docs/research-protocol-retirement.md` 的“通用 POST /runs 创建路由删除”只指旧通用提交协议；当前 Router96行和 `http_launch.rs` 明确保留正式 purpose-only `/runs`。 |
| “所有 HTTP 同一 Origin 检查”过度概括 | `docs/workflow-runtime.md` 的表述不能理解成每个 GET 都验 Origin。`crates/akzio-daemon/src/http_runtime.rs` 只调用 token authorize；native 拒绝集中在正式 launch 与 Debug mutation。 |
| preflight “只读”有文件边界例外 | `crates/akzio-cli/src/cli/calibration.rs` 注释称只读，但 `Store::open(scratch)` 是可创建/迁移路径；没有模型调用不等于零写盘。应选 readiness 的 open_existing 做真正既有 Store 读取；本次不调用任何一种。 |
| App 当前没有 Keychain 存储路径 | 当前 TOML get/set 和 `.daemon-token` 机制见第2节。不能因文件名 CoreCredentials 或 import Security 宣称 Keychain。 |
| App 不是通用 canonical attach 客户端 | Supervisor 固定7342且已占用就拒绝；只有显式 Debug endpoint/root 走 attach 分支。Python 才按配置连接已有 canonical Core。证据 `apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。 |
| Portfolio Accepted 映射不匹配 | `apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift` 检查 JSON 顶层是否含 key `"accepted"`；Rust 当前是内部标签 `verdict:"accepted"`，见 `crates/akzio-domain/src/execution/effects.rs`。按该 typed payload 形状推导，Accepted 会落 `.noOrder`；缺 verdict 也落 `.noOrder`。这是源码可确定的映射缺口，不能把该 UI 标签当 Gate 权威。 |
| 空订单表文案过强 | `apps/Sources/ObservatoryKit/Features/Portfolio/OrdersFillsTable.swift` 仅凭 orders.isEmpty 就显示“No executable order was produced”；它可能是尚未回执、bounded projection 缺失或 N/A。且 fills 表嵌套在有 orders 的分支。讲解必须回到具体 verdict/receipt/availability。 |
| 脚本 Commitment 可见性缺口 | `scripts/run_core.py` 试图从 Observer artifacts 找 ExecutionCommitment；当前白名单 `crates/akzio-daemon/src/observer/broker.rs` 没有该 kind。因此正常 detail 不会提供它；summary 的 not_observed 不是“未持久化”。应在授权 bundle 的相应 artifact/journal 引用查证。 |
| Outcome/Learning 不必与当前 Run 同一条 | `crates/akzio-daemon/src/observer/snapshot.rs` current_run 取最新；`crates/akzio-daemon/src/observer/runs.rs` 独立跨近期 artifact 选择。页头 T0 与历史 T+N 并列是允许的，不应给后者强加前者 Run ID。 |
| Debug 文档与 UI/CLI覆盖不同 | `docs/debug-control.md` 称 Abort 为 Core 内部动作；当前 HTTP 反序列化支持 Abort（domain/debug.rs118-131 + http_debug.rs112-129），但没有 CLI/App 对应按钮。run_until 完全不在当前接口中；不要把用户期望列成已实现。 |
| 旧导出不是新安全 bundle | `store export-run` 可写 SQLite payload，默认过滤不含 RawEvidence；新 formal bundle 才强制 raw access=false/敏感脱敏。见第8节，不得因同有“export”字样而混用。 |

### 9.1 已删除/不可活动创建的入口

- CLI `run fixture-debug`、`run paper-dry-run`、`run submit`、`debug prepare --fixture-controller` 不在活动命令中，退休解析测试覆盖拒绝；`read_range_probe`、`fixture_controller` 不在严格 Debug prepare/fork 请求结构。证据：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-daemon/src/debug.rs`。
- 当前脚本参数拒绝 `--research-only`、`-fakerOnline`、`--faker-online`；文件存在性检查确认旧 `scripts/debug_goal_run.zsh`、`scripts/paper_canary_run.zsh`、`config/task2-fixture.toml` 不存在。已删除文件没有现行源码可定位；以现有参数定义 `scripts/run_core.py` 和删除说明 `docs/development-workflow.md`、`docs/research-protocol-retirement.md` 为现行证据。
- 历史 Planner/PaperDryRun/旧 AgentTurn/旧预算类型继续读、显示、导出、replay/doctor，不恢复执行。Store 退休判定包括 paper_dry_run、research.planner、研究 Contract<65；返回 `legacy_workflow_retired`。这是退休识别阈值，不是“当前活动 Contract 为65”。证据：`crates/akzio-store/src/store/workflow/helpers.rs`；`docs/research-protocol-retirement.md`。
- App `PaperDryRun`/Planner/SimulatedOnly 标签仍有历史解码用途，不在 userLaunchModes；Mock 界面仍存在且有明确演示 banner，**Mock UI 不等于已删除的 faker Broker 模拟**。证据：`apps/Sources/ObservatoryKit/PresentationModels/DomainVocabulary.swift`；`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`；`apps/Sources/ObservatoryKit/App/AppShell.swift`。

## 10. 可供主线直接使用的中文讲解顺序

1. **先分客户端和权威**：App 管受管进程、Python 连接现有 Core、CLI 提交控制/检查；Rust/V2Store 决定状态与权限。（第1–4节）
2. **说明启动不是无副作用**：配置/文件 token、loopback/native 检查、模型 probe；只读调查不能为了截图启动真实 App/Core。（第2节）
3. **再分正式两模式**：PositionPlan 到 Decision；Paper 加执行与未来 Outcome；manual 是能力，auto 是新 T0 自动调度开关；Paper purpose 不等于交易 approval。（第3节）
4. **逐级解释结果**：raw 研究配置→validated 研究配置→正式 targets→ExecutionVerdict→Commitment/提交→fill/reconcile→OutcomeSchedule→数值 seal→叙事/学习。（第5.3节）
5. **带着证据看 UI**：snapshot 是有界投影，SSE 是观察/失效通知；看 Run ID、artifact、available/pending/unavailable、cursor 和 lifecycle，不只看 badge/空表。（第5节、第9节）
6. **最后讲隔离 Debug 与导出**：prepare 暂停、inspect、精确 step、pause/resume/retry、非 Paper fork；说明不存在 run_until 和未暴露的 abort；分享安全 bundle 与旧 SQLite 导出分开。（第6–8节）

### 交付状态

本报告完成了当前请求的入口/显示/操作面只读映射。不存在本次真实 Run ID、模型调用、Paper 提交/成交、Outcome 或 UI 验收结果。没有运行构建/测试/fixture/Doctor，因而不宣称它们通过；源码测试仅作为预期行为的旁证。后续若主线引用本报告，应保留“源码行为”“静态映射缺口”“实际运行事实”的三层区分。

文档静态检查包括 `git diff --check`、来源文件存在性与链接检查；这些检查不证明源码行为在更新后仍相同。原始报告曾存于忽略目录；当前 `docs/reports/` 副本需独立做格式与链接检查。原始核查时四个重点源文件 SHA-256 曾复查一致。这些检查只证明报告定位与工作树边界，不构成程序行为验证。


---

<!-- 分报告 7: 07-integration-boundaries.md -->

# 跨模块整合：应怎样读“完整过程”，以及哪些结论当前不能成立

本章由主助手在六个独立模块核查之后联读源码整理。它不修改其他报告的历史判断，不把静态推导当作真实故障复现，也不声称当前运行二进制与本工作树相同。

## 1. 十种状态必须分开

| 状态 | 说明 | 不能自动推出 |
|---|---|---|
| EvidenceNeed 已冻结 | Rust 规定要找什么 | 该来源已实现、已经采到 |
| collection available | 形成了可保存材料 | 存在可用事实、来源独立核验完成 |
| selected in Manifest | 本次选中和授权 | 全文已内联、模型实际使用 |
| Claim/Critique 合格 | 精确资产/期限/来源资格通过 | 最后所有预测数字正确 |
| ProposalReview 通过 | 精确完整提案的 17 项审查通过 | DecisionPolicy 已校准、可执行 |
| Decision 已保存 | Rust 正式目标和 blocker 已持久化 | Context accepted、ExecutionGate 通过 |
| ExecutionVerdict Accepted | 完整执行方案通过 Gate | 请求已发出、Broker 已接受 |
| Commitment 已保存 | 计划和确定性 client IDs 已绑定 | 实际提交或成交 |
| Reconciliation Complete | 全部订单血缘已终态收束 | 所有订单都 filled |
| Outcome sealed | 三期数值口径合法封存 | 叙事可用、可学习、政策或 Lesson 已晋升 |

关键代码：[Decision 生成](../../crates/akzio-execution/src/decision_gate/decide.rs)、[执行 verdict](../../crates/akzio-execution/src/execution_gate/core.rs)、[对账终态](../../crates/akzio-execution/src/reconciliation.rs)、[Outcome worker 分支](../../crates/akzio-daemon/src/outcome/worker.rs)。

## 2. 四条容易被画错的箭头

### 2.1 研究配置不是正式目标的上限

`review_research_plan` 产生 raw/validated。随后 `target_with_risk_traced` 的输入只有 now、effective confidence 和 forecasts，没有 research_allocation 参数。所以不能画成“把 validated allocation 按风险缩一点就成订单”。

显式研究现金不自动构成目标函数的 cash veto；非零 Decision 也可能与 hard blocker 并存，最终不允许执行。真正解释一次为何归零、为何阻断，要看 target 的 runtime trace、Context blockers 与 ExecutionVerdict，而非只看研究 allocation 或 UI badge。

证据：[decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)。

### 2.2 PaperCommit 不是发单函数

PaperCommit 只提交不可变 Commitment。Broker execute / lookup / cancel / reprice 在后续 Reconcile 节点的 Dispatch 中发生。NoOrder 在这两个节点分别短路，不取执行 lease/Broker；但前面的研究、GET 和 Store 写入已经可能发生。

证据：[paper_execution.rs](../../crates/akzio-daemon/src/application/paper_execution.rs)。

### 2.3 数值密封、学习资格、DecisionPolicy 校准不是一个开关

- 缺叙事时可以数值密封，但不生成可学习的 Experience。
- 有叙事但缺独立风险真值时，仍可有审计用、不可学习的 Experience/Evaluation。
- canonical calibration collect 检查的是 sealed Outcome、完整价格标签和生产身份，不直接以 Retrospective Complete / Experience.learning_eligible 作为同一个前置。
- 所以“不能自动学习”不等于“不能收集正式预测标签”；反过来“收了标签”也不等于学习晋升或 Policy 已激活。

证据：[Outcome worker](../../crates/akzio-daemon/src/outcome/worker.rs)、[学习资格](../../crates/akzio-learning/src/evaluation/materialization.rs)、[collect](../../crates/akzio-cli/src/cli/calibration.rs)。

### 2.4 Outcome 的价格基线与 calibration collect 的标签基线不同

主 Outcome 计算从冻结 ExecutionContext 的 quote midpoint 取得 `p0`。calibration collect 则从 Outcome market bars 取 `baseline_trading_day` 当天日线 close 作为 base，再取各窗口当天 close 算 realized_return。

这是两个实际计算调用的差异，不应将它们都笼统称为“完全一样的 T0 收益标签”。它们可能数值接近，也可能不同；本次没有实际数据证明差值大小。

证据：[Outcome midpoint](../../crates/akzio-daemon/src/outcome/materialization.rs)、[collect baseline close](../../crates/akzio-cli/src/cli/calibration.rs)。

## 3. 主助手复核的当前实现限制

### 3.1 Outcome / narrative repair 的调用节点身份存在静态不一致

Store 创建 Outcome worker 时保存固定 objective。daemon 调用 AgentRuntime 前克隆该节点、替换 objective 为当前 horizon 说明。AgentRuntime 从 Store 重读冻结节点，只允许 input_artifacts 等候选处理差异，比较节点其余全部字段；objective 参与 PartialEq。

按所查标准路径，这会在真正发模型请求前形成 `NodePolicyMismatch`。普通 Outcome 会把该错误归为叙事不可用，仍可能继续合法的 Rust 数值路径；repair 需要模型的分支则传播错误。

因此，可以详细说明 Outcome AgentRuntime 的两阶段协议，但不能说当前 daemon 已成功调用该协议或自动修好叙事。本次未运行到期任务验证这条静态推导。

证据：[持久节点](../../crates/akzio-store/src/store/learning/outcome.rs)、[普通调用改 objective](../../crates/akzio-daemon/src/outcome/worker.rs)、[冻结节点逐字段比较](../../crates/akzio-research/src/agent/runtime_run.rs)、[repair 调用](../../crates/akzio-daemon/src/outcome/narrative_repair.rs)。

### 3.2 同 ID 恢复机制并不保证 Reconcile 被再次调度

Reconcile 默认 retry=none、max_attempts=1、wall=30s；正常 Pending 的 Deferred 不消耗失败数，crash/transport/timeout 却可能失败终止。已有 session 不会因为失败再造一张 Paper 图。

因此要区分 durable evidence、lookup 可复用 ID、安全不重复、调度活性和最终收束五个问题。

证据：[默认 recipe](../../crates/akzio-runtime/src/runtime/catalogue.rs)、[失败次数含 running](../../crates/akzio-store/src/store/workflow/helpers.rs)、[恢复额度](../../crates/akzio-store/src/store/workflow/outputs.rs)。

### 3.3 Lesson 召回不证明正文进入当前研究请求

默认 selection reason 为 `lesson`，materialization 的 `must_read_class` 没有 Lesson / Experience / CandidatePolicy 分支。这些对象可以进入 metadata ledger，但并不在该默认路径内联正文；研究角色没有后续读取工具。

因此必须分别查 retrieval audit、最终 Manifest、实际 materialized request 和 applied refs，不能把第一步当作最后一步。本次确认的是这个默认代码路径，未打开任何真实 Run 请求。

证据：[selection reason](../../crates/akzio-context/src/selection.rs)、[内联分支](../../crates/akzio-context/src/context_broker/materialization.rs)、[must_read_class](../../crates/akzio-context/src/context_broker/materialization.rs)。

### 3.4 Policy 的当前加载、历史校准身份、当次 proposal 身份不是同一个检验

loader 比较 descriptor/envelope 与当前模型身份，但不读取当前 active Synthesizer Contract；canonical serve 未执行 Debug 分支里的额外 Contract 比较。DecisionRuntime 持有启动时 Policy 正文快照，不能凭后来 SQL activation 推断已运行进程热切换。

collect 的历史 identity 读取 provider/model/Contract，而样本 model_version_hash 赋为当前配置 hash；不能据后续字段一致声称历史请求所有版本/日期/route 参数已逐项独立核验。

这些都不证明某个实际 Store 已经错配，更不证明产生订单；这里只指出不能跨层夸大的保证。

证据：[Policy loader](../../crates/akzio-cli/src/cli/identity.rs)、[serve 分支](../../crates/akzio-cli/src/cli/run_commands.rs)、[DecisionRuntime 快照](../../crates/akzio-execution/src/decision_gate/decide.rs)、[collect 版本赋值](../../crates/akzio-cli/src/cli/calibration.rs)。

### 3.5 新闻已被两次模型处理仍不是 source_verified

默认 recent-news 路径是 discovery + 独立模型 review，明确保存 `source_verified=false`。合格事实可供描述性背景，但不能冒充当前研究支持性新闻引用；补采的 news 新事实准入还要求 source_verified。

“没有新增合格补采事实”可能是资格原因，不等于没有获得任何文字。SOXL index metadata / leveraged terms 还有明确 NotConfigured 路径，Need 存在不等于完整适配器已实现。

证据：[news.rs](../../crates/akzio-ingest/src/news.rs)、[supplement.rs](../../crates/akzio-daemon/src/application/research_supplement.rs)、[SOXL index](../../crates/akzio-ingest/src/official.rs)、[SOXL terms](../../crates/akzio-ingest/src/official.rs)。

### 3.6 Unknown risk 和短窗口会阻止自动 Canary 晋升

已看到风险真值记录接口和消费方，未见 daemon 自动 producer。Shadow 数值构造显式把 risk_recall 清 None；标准 Outcome 只取前五个共同 Session，而 ES 门槛为 20。Canary 将 risk/tail 等必需指标缺失判为 Defer，而不是通过；已测退化仍可以优先 Rollback。

这解释了“有完整类型/接口/比较图”为什么不等于“标准默认链路能自动晋升”。

证据：[risk record API](../../crates/akzio-learning/src/evaluation/risk_ground_truth.rs)、[Shadow 清 risk](../../crates/akzio-daemon/src/outcome/shadow.rs)、[ES 阈值分支](../../crates/akzio-learning/src/metrics.rs)、[Canary verdict](../../crates/akzio-learning/src/campaign.rs)、[必需指标](../../crates/akzio-learning/src/campaign.rs)。

### 3.7 卖出股数不应仅凭 long-only 的名义认为已安全封顶

Allocator 从当前 market_value 与目标市值之差得到卖出 notional，wire qty 再除保护 limit。所查路径没有用持仓 quantity 做请求前上限。因此需要区分“不接受负目标/负持仓”与“生成的卖出股数永远不超过持仓”。

这是公式级静态边界；未发订单，不断言实际 Broker 接受了超量卖单或真的产生空头。

证据：[市值 delta](../../crates/akzio-execution/src/allocation.rs)、[数量公式](../../crates/akzio-execution/src/paper/protocol.rs)。

### 3.8 页面和 bounded Observer 不是执行真相

- Swift Portfolio 检查顶层 key `"accepted"`，Rust verdict 实际是 `{"verdict":"accepted",...}`；不能把这个页面 badge 当 Gate 权威。
- Python summary 找 ExecutionCommitment，但 Observer 白名单没有此 kind；not_observed 不能解释成 commitment 没落库。
- snapshot/current_run、历史 Outcome 和 Learning 分别选择材料，不保证来自同一 Run。
- 一般源码注释“所有 Origin 检查”“所有获取错误生成 NoOrder”等都比当前分支宽，不能照抄。

证据：[Swift verdict 映射](../../apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift)、[Rust wire](../../crates/akzio-domain/src/execution/effects.rs)、[Observer 白名单](../../crates/akzio-daemon/src/observer/broker.rs)、[Python summary](../../scripts/run_core.py)。

## 4. 对 Topology 消费方的整合纠正

不是所有 PolicySubject 的 Active 都只停留在类型层。当前 `StorePaperWorkflowSource::proposal_sync()` 确实读取注册 candidate topology 的 Active head，从最近最多 500 份合法 Paper WorkflowProposal 中选相应 topology；找不到则回到受控 bootstrap。

因此准确说法是：**存在有限、明确的 scheduler topology 消费方，但 head 激活并不单独证明下一次新 Run 一定使用了某个已有 candidate graph**，还要看可用 proposal、冻结输入和 runtime catalogue。Contract 的 Active transition 则有独立的 catalogue head 更新。

证据：[scheduler.rs](../../crates/akzio-daemon/src/scheduler.rs)、[Contract catalogue transition](../../crates/akzio-store/src/store/impl_core.rs)。

## 5. 这次交付真正证明了什么

- 已做：六模块独立源码核查，主助手整合与关键分支复核，报告生成、来源文件/链接静态检查，工作树状态检查。
- 没做：源码实现修改、Rust 编译/测试、真实模型调用、Core 启停、Broker API、Paper 下单/成交、T+1/T+3/T+5、真实学习晋升。
- 所以证据级别是 **current-worktree source-inspected**。不把报告机械检查通过写成业务 `offline-verified`，也不写 `real-Paper-verified` 或 `outcome/learning-verified`。
- 本报告不能断言用户当前 canonical Store 的 policy、样本数量、账户或某个历史 Run 的真实结果；那需要单独读取相应获准运行证据。


---
