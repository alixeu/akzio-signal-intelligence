# 任务二：全链路 Debug 入口与证据要求

先阅读 [任务一修复表与版本规则](task1-repair-handoff.md)。本文件列出的 CLI/测试函数真实存在；“入口存在”不代表场景已经验证。2026-09-08 已执行的组合场景、修复与仍待验证项见 [任务二离线 Debug 记录](task2-offline-debug.md)。

## 隔离 Store 与离线入口

以下从仓库根目录运行。显式设置命令级 `AKZIO_STORE_ROOT`，避免继承 App/终端指向真实 Store 的环境变量。配置不含模型或 broker 凭据，`auto_paper=false`。

```bash
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml run fixture-debug
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml run paper-dry-run
```

`fixture-debug` 实际创建 **PaperDryRun**，内部启动临时 HTTP/worker 并执行 Store Doctor，结束后关闭服务。不是 canonical Paper、真实模型或完整三 horizon 拓扑证据。通用 `fixture_model_client()` 仍是固定用途的脚本响应，不是可自动适配所有多 horizon 场景的模型。不要用 `daemon serve` 加此无模型配置来声称已搭好完整模拟交易服务。

以下现有离线诊断同样可以指定该配置与 Store。任务二应先读每项断言，检查是否足以证明本次目标：

```bash
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml test crash-recovery
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml test lease-takeover
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml test evidence-integrity
AKZIO_STORE_ROOT="$PWD/target/task2-fixture-store" cargo run -p akzio-cli -- --config config/task2-fixture.toml test learning-transitions
```

`crash-recovery` 覆盖过期 claim 回收；`learning-transitions` 只检查非 canonical Run 没有晋升。现有 `test concurrent-runs` 依次调用 run_one 且允许 Failed 终态，不能充当 WorkerPool 并行成功证明；`test retrospective` 只查询最新复盘，空库返回 None 也可成功，不能证明生成过 T1/T3/T5。没有现成的一条 CLI 命令完成跨 Session + Mock Broker 故障注入全场景。

独立 `store doctor`、`run replay`、`run retrospectives`、`run trajectory` 和新增 `run repair-narrative <run-id>` 都是认证 HTTP 客户端，要求**对应隔离配置的 daemon 正在运行**。fixture 命令退出后不能直接假设该服务仍在。Rust harness 可直接使用 `Store::verify_integrity()` 与 `Store::run_lifecycle_health()`；不需要绕过 Store 写 SQLite。

## 可直接执行的定点测试

```bash
cargo test -p akzio-daemon --lib task2
cargo test -p akzio-domain --test task2_sources
cargo test -p akzio-research --test task1_handoffs
cargo test -p akzio-domain --test task1_coverage
cargo test -p akzio-learning --test task1_accounting
cargo test -p akzio-ingest --lib session_bars::tests
cargo test -p akzio-context --lib search_snippet_surrounds_late_unicode_match
cargo test -p akzio-store --lib task1_migration_tests
cargo test -p akzio-runtime --lib review_retry_delay
cargo test -p akzio-daemon --lib review_cancelled_outcome_future
```

- `akzio-daemon --lib task2`：真实 Scheduler/TaskRuntime/Daemon dispatch/Store，配合隔离模型、行情和 Broker fixture，覆盖完整十二格研究的 NoOrder、Accepted 清仓与先 Commitment 后 Broker、T1/T3/T5 补跑及叙事补评估、三个 Canary Shadow 和首个 subject 后中断恢复。另有真实 WorkerPool 的单 worker 交错、双 worker 双向阻塞，以及短 Deferred 到提交时过期的回归。它们不代表真实模型、Alpaca Paper HTTP 或生产压力测试。
- `task2_sources`：精确 collection-status producer/kind/source/origin 正反验证，禁止以同前缀放宽 SemanticDetail 来源。
- `task1_handoffs`：正式 AgentRuntime 的三组 Analyst/Critic 使用合法 9 条单期限依据达到 12-slot 覆盖；Outcome 两阶段工具审计、大输入预算、最低 Context 集合、staged/committed 生命周期；Scheduling/Learning/Store 的 Rust-only 密封→fixture 叙事补评估与幂等性。还覆盖图结构、跨 Run 拒绝、workload claim、阶段失败预算、租约释放和旧 Contract 阻断。临时 Store 自动建在仓库 `target/task1-harness` 下。
- `task1_coverage`：12-slot 必需证据域、SUPPORT 验证、单资产/单 horizon 缺口、legacy scope 兼容与单 horizon 中性降级。只检查纯领域规则，完整 proposal 的 thesis/引用链由 Agent 和 Gate 测试另验。
- `task1_accounting`：盈利全卖、部分成交取消、重复/冲突 Receipt、缺失 Receipt、零成交；额外覆盖不利买入/有利卖出的有符号实施差额、费用单次扣除和初始账户估值差额，调用生产数量/现金计算函数。
- `session_bars::tests`：DST/提前收盘、本机 HTTP 短首屏+分页 token+倒序研究请求、共同四资产 Session；采集保留晚于 T5 的行动，目标阶段 gate 只阻断受影响窗口。不是在线 API 测试。
- `task1_migration_tests`：隔离 v13 结构经 Store::open 串联升级，第二步失败后保留 v14 并可重开，旧任务阻断保留，已标 15 的缺失索引修复；v14→15 CAS 保留和查询计划。不是全部用户历史数据库的 migration replay。

## 可注入接口与组合 harness

| 能力 | 真实位置 / 接口 | 现有能力与限制 |
|---|---|---|
| 模型逐轮协议 | `akzio-research::AgentModel::turn`；`task1_handoffs.rs::ProtocolModel` | 可断言 Draft/Submit 工具集合、返回 tool calls、错误 JSON、timeout 或正常 Submit。定点协议与 Daemon 多阶段图已有离线 fixture；真实模型及完整失败矩阵仍待验证。 |
| Daemon 模型响应 | `akzio-model::ModelClient::fixture_sequence` / `fixture_by_purpose`；`Daemon::with_model` | 可以注入有序/按目的响应。多 horizon 的响应必须跟当前 task/Artifact ID 对齐，不能直接复用单 Claim fixture。 |
| 证据响应/时间 | `akzio-ingest::FixtureEvidenceAdapter`、`AsyncEvidenceAdapter::acquire_at`；`session_bars.rs` 内本机 HTTP 测试 | 可生成带获取 cutoff 的正常/错误响应；HTTP adapter 测试在 crate 内访问私有配置，不向生产开放任意 endpoint。 |
| Session 时钟 | `akzio-daemon::BrokerSessionClock`、`PaperScheduler::tick(..., now)` | 注入开市 Session 与账户 ID。市场开市与 Outcome 收盘是两种独立时钟条件，不能只推进自然日。 |
| Task 时钟 | `Daemon::run_one_with_task_clock`（`pub(crate)`）以及 Store claim/defer/retry/recover 的显式时间参数 | Daemon crate 内测试可控制任务观察时刻；没有公开 CLI 全局时间旅行开关。 |
| Broker | `akzio-execution::paper::CommittedPaperBroker`；`AlpacaPaper` 生产 endpoint guard；PaperDryRun 路径 | `task2_tests.rs::FilledBroker` 在实际 dispatch 中检查 Commitment 已持久化后才返回 fixture fills，覆盖 Accepted 清仓和 reconcile。没有放宽生产 endpoint guard，也不把 fixture 当作实际 HTTP 或所有订单边界证明。 |
| 故障注入 | `AgentModel` 错误；EvidenceAdapter typed error；Store lease/recover；Canary 测试构建注入 | 已验证首个 Canary subject 后的中断与租约恢复。没有任意生产 failpoint CLI；其他 stage/receipt/reconcile 边界和 OS 进程恢复仍需分别验证。 |
| 存储诊断 | `Store::verify_integrity`、`run_lifecycle_health`、indexed run artifacts；认证 replay/observer | projection 可从 CAS 重建。迁移 SQL 仅用于测试构造隔离旧结构；业务测试必须通过 Store 写入。 |

## 完整 Debug 必须覆盖的断言

1. 在隔离 Store 上从正式 catalogue 和 Rust Paper 图开始，执行 EvidenceGate → 三组 Analyst/Critic → Synthesizer → DecisionGate → ExecutionGate → NoOrder 或模拟 broker commitment/reconcile → OutcomeSchedule。模型轮次必须实际经过 Context、工具、Submit 和 Store，不能仅构造最终 Outcome/Experience 宣称成功。
2. 控制四资产 Session 序列：T0 后按共同完成日期推进 T1/T3/T5；含周末、休市、单资产缺数据、提前收盘/DST、供应商未就绪和迟到补跑。T2/T4 与已完成阶段不能调用模型；补跑 T1 的模型可读材料不得包含后续阶段事实。
3. 两个历史 Outcome 与新 Session T0 同时就绪，分别验证单 worker 交错、多 worker 保留、任务 aging、per-outcome 租约、旧 owner fencing 与真实 Store 故障上报；不能仅断言 RunId 不同或任意终态。
4. 注入 401/403、429+Retry-After、5xx、Pending、未来数据污染、重复分页 token、超限和公司行动；比较错误类别、ready_at 与 failure budget。重复 Deferred 后的第一次真实故障仍有预算；T1 故障后成功不得吃掉 T3/T5 的额度。验证 30 秒起的指数退避与 5 分钟上限。
5. 覆盖盈利清仓、部分成交取消、重复/冲突 Receipt、midpoint→fill 实施差额、费用单次扣除、并发外部现金流和下一 Run 交易。验证冻结果口径；缺 actual NAV/company-action ledger 时保持 unavailable，不填伪收益。
6. 验证 Decision production cost 在后续 T1/T3/T5/repair 后不增加；原研究失败重试仍纳入闭包。切换 Outcome 模型后 producer Contract/Workflow 不变，evaluator 单独变化。Canary PolicySubject 不冒充原 producer。
7. T5 模型失败时只封存数值/失败叙事，不产生合格学习。通过原 Run repair 追加 linked revision，无重复 Outcome、无 CAS 覆写；修复必须生成/复用原治理下的 Experience/Evaluation，保持数值与生产成本不变；不得绕过资格检查。Canary 要覆盖冻结 cohort、三 subject 中途失败重试、消费去重和 task 最终完成。四资产 bars 齐全但研究不足、risk None 或叙事不合格仍不得晋升。
8. 在隔离旧 Store 样本上验证 v13→14→15、旧 Contract blocker、所有角色 preflight 与启动竞争；验证 old CAS、ExecutionPlan hash、Commitment/client_order_id 不变。全量 Doctor、索引重建和大历史查询也要执行。
9. 再执行 workspace fmt/check/clippy/test、完整 fixture 和以上场景。只有实际经过的检查才能升级为 offline-verified；真实模型/Paper 或 T+5 学习分别提供对应环境证据。

Task 1 没有获得访问真实券商写接口、开启 auto_paper、修改真实 Store 或替换审批的授权；任务二执行若需要这些范围，应按用户当时的明确授权处理。

任务一窄域复核的历史执行记录见 [task1-followup-review.md](task1-followup-review.md)，本轮 Daemon 组合与剩余边界见 [task2-offline-debug.md](task2-offline-debug.md)。v13 结构 fixture/打开测试不等于完整用户历史迁移；完整 Daemon 配合脚本模型和 Broker fixture 也不能替代真实环境证据。
