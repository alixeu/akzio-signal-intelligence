# 04 — Paper 执行完整流程：当前工作树只读核查

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

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
