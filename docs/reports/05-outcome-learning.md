# Outcome → 学习 → Lesson / Canary：当前工作树只读过程核查

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

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
