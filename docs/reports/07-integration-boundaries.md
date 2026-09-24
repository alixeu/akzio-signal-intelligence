# 跨模块整合：应怎样读“完整过程”，以及哪些结论当前不能成立

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

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
