# DecisionPolicy：T1/T3/T5 从冷启动、历史校准到正式仓位

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

> **源码基线**：仓库固定提交 `e4292f09acf5b3798bf16de26718ceb85046a190`，核对日期 2026-09-29。下文以该提交的仓库相对源码文件和命名职责为导航；文件级引用不承诺会随编辑移动的数字位置，也不代表未提交工作树或某次 Run 的观察。
>
> **权限与证据边界**：仅作静态源码研究；没有打开这台机器的 canonical/Debug Store、配置或密钥，没有运行 CLI 校准、Core、模型、Broker、构建或测试。因此无法判断本机是否已有风险限制、成熟样本、候选、active head，或已运行 Core 正持有什么 Policy；下文的“冷启动”是条件性代码路径，不是对本机状态的断言。

配套的[合并版离线交互式流程图](../../.archify/akzio-process/akzio-complete-process.html)把全流程与冷启动、三期限价格标签、离线拟合、显式激活和后续 T0 正式仓位放在同一份 HTML 中。图固定到上述提交，不是实时 Core 监控。

## 0. 先把五种东西分开

| 名称 | 准确含义 | **不能**推出 |
|---|---|---|
| `DecisionPolicy::default()` | 无 active head 时加载的、结构可校验的 fail-closed 内存值：有默认的三期限权重和若干上限，但没有 active forecast scope、12 份校准或四资产风险模型；`min_calibration_samples=u32::MAX`。 | 它不是经过训练或激活的 Policy；不是交易许可。 |
| `CalibrationRiskLimits` / `CalibrationDataset` | operator 明确写入的风险限制、以及从合格历史 Paper Outcome 收集的训练输入；各有自己的 SQL CAS（内容寻址存储）Artifact。 | 保存它们不等于生成 Policy。 |
| candidate `DecisionPolicyArtifact` | `build` 拟合并保存的 provenance-bearing、不可变 Policy 候选；`decision_capable` 可能为真，也可能为假。 | 即使候选有效、能决策，也不是 active head。 |
| Store **active head** | `activate` 显式选择的单例 SQL head，指向安装过的不可变 CAS Policy，有 activation 历史。 | head 存在本身不保证与当前配置模型、当前 Synthesizer Contract 或在运行的进程快照匹配。 |
| `decision_capable()` | 对 Policy 内容的资格谓词：12 个资产×期限校准槽、四资产风险和样本/Brier 门槛齐备。 | 不保证某个实时预测落入有足够样本的 bin、研究证据通过、ExecutionGate 通过或订单成交。 |

**源码**：`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-store/src/store/decision_policy.rs`。

由此，**不是“三个 T 期限各生成一份可交易 Policy”**：一份候选/active Policy 内含同一模型 scope 下四资产×T1/T3/T5 的 **12 个 forecast calibration surface**、四资产风险项、一份组合 covariance 风险模型及三期限权重。`build` 的确循环构造这 12 项，却没有从历史数据学习新的 horizon 权重，而是继承默认的 `333_333 / 333_333 / 333_334 ppm`。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。

```text
无 active head
  ├─ 正式 PositionPlan：合格研究 → 默认 Policy 的零正式目标 → Decision 后结束
  ├─ 隔离 Debug PositionPlan：research_only_incomplete，只手动研究，不放行 Decision
  └─ canonical Paper scheduler：无审批绑定的冷启动 Run → 正式研究/Decision
       → 缺审批使 ExecutionGate 为 NoOrder → 有完整基线时仍可排 Outcome
       → 四资产共同完成的 T+1/T+3/T+5 → sealed Outcome
       → 只读 readiness → operator 的 14 项风险限制
       → collect SQL dataset → build SQL 候选 → inspect/validate → 显式 activate
       → 后续新加载的 Core 读取 active 快照 → DecisionGate 另算正式目标
```

这是一张**条件链**：研究失败、证据不全、基线缺失、Outcome 未到期、样本/价格面不足、模型/Contract 不匹配，都可能中断对应后继步骤；T0 的运行与历史 Outcome 的 worker 是独立时间线。**源码**：`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`；`crates/akzio-daemon/src/application/outcome_sealing.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

## 1. 无 active head：PositionPlan 与正式 Paper 的两种冷启动

1. **默认 Policy 不会填造校准。** SQL active head 不存在时，加载器返回 `DecisionPolicy::default()`、`store_active_head_missing`、空 input hash/Artifact ID。默认值能通过结构校验，但 `decision_capable()` 为假；即使研究给出非零 `research_plan.validated`，它也不变成可执行目标。若有效置信度先低于默认门槛，走 `decision.confidence.minimum` 全局零目标；否则缺资产校准会逐项排除并走 `decision.eligible_set.empty`。两者都不是订单。**源码**：`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`。
2. **受控的真实模型 PositionPlan 入口**：`calibration preflight` 对缺 head 返回 `research_capable=true`、`decision_capable=false`、`position_plan_mode=research_only_incomplete`。隔离 Debug `prepare` 可接受这个明确的未配置身份，但 `resume`/直接进入 `gate.decision` 受 daemon 与 Store 的 research-only 条件阻挡；只能手动推进研究到终稿，不可把诊断提案标作正式 Decision。已有 Policy 时，PositionPlan 图在 Decision 后结束：无 ExecutionGate、PaperCommit、Reconcile、Evaluate，不占 Paper slot，也不提供 canonical Paper Outcome 校准样本。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/debug.rs`；`crates/akzio-store/src/store/debug.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`；`crates/akzio-store/src/store/workflow/contracts.rs`。
3. **正式 canonical Paper scheduler 的冷启动恰好不同。** 它先向真实 Broker clock 取得非 Closed session；若该 session 尚无 slot，且 Store 当下无 active Policy，就**不绑定任何旧审批**，仍可在有 workflow proposal/leader lease 等前提下预约新的 Paper Run。若有 active Policy，需取得当前 approval binding，并对 RuntimeIdentity、账户和行情 feed 核对；缺审批则等待，不预约新 Run。预约成功仅说明图被持久化，并不保证模型或未来 Outcome 成功。**源码**：`crates/akzio-daemon/src/scheduler.rs`；`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`。
4. 冷启动 Paper 可以经过研究和 Rust Decision，**不能借“为了校准”获交易权限**：该 Run 无 approval，ExecutionGate 写 `UnqualifiedRuntime` blocker，不进行 allocation，最终是可持久化的 `NoOrder`；PaperCommit/Reconcile 对 `NoOrder` 不取得执行 lease、也不发订单。若仍取得完整冻结账户/报价等基线，Evaluate 可保留 `NoOrder` 血缘并建立后续 OutcomeSchedule。没有完整基线或后续交易 Session，并不能只靠等待补出合格标签。**源码**：`crates/akzio-execution/src/execution_gate/core.rs`；`crates/akzio-daemon/src/application/paper_execution.rs`；`crates/akzio-daemon/src/application/outcome_sealing.rs`；`crates/akzio-daemon/src/outcome/materialization.rs`。

**入口差异，不能一概而论（静态联读）**：上述“无 head 的 PositionPlan 到终稿即停”明确适用于 **preflight/隔离 Debug 受控入口**。固定 HEAD 的非 Debug Native App `POST /runs` PositionPlan handler 则直接 `prepare_position_plan → commit_position_plan`，源码中没有在这个 handler 重复 preflight 限制；编译器仍将 `gate.decision` 加入 PositionPlan 图。因此不能声称**所有** PositionPlan 入口必然在 Decision 前停住：若这条普通入口继续运行到 Decision，当前加载的默认 Policy 仍会 fail-closed 为零目标，但目的不是 Paper，Decision Artifact 为 RunScoped，且没有后续执行/Outcome。这里是源码路径间的条件性区别，**未做运行复现**。**源码**：`crates/akzio-daemon/src/http_launch.rs`；`crates/akzio-daemon/src/orchestration/workers.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`。

## 2. T+1/T+3/T+5 如何变成校准价格标签

1. T0 Paper 的 `OutcomeSchedule` 冻结 Decision、DecisionContext、ExecutionContext 和**基线交易日**：基线来自 scheduler 的 Paper session slot，不从 worker 执行当天猜。`NoOrder` 的 schedule 只绑定 verdict；`Accepted` 分支需要 Commitment 与 `Complete` Reconciliation。创建 schedule **不代表订单成交、价格观测或学习完成**。**源码**：`crates/akzio-daemon/src/outcome/collection.rs`；`crates/akzio-learning/src/outcome_schedule.rs`；`crates/akzio-daemon/src/application/outcome_sealing.rs`。
2. Outcome worker 独立采集 Alpaca 的四资产日线，以交易所真实 calendar close **再加 20 分钟**作为 bar 可用边界；每只资产最多查 252 根、从基线起最多 366 天。严格取**晚于基线且 TQQQ/QQQ/SOXX/SOXL 全都有 bar 的日期交集**，依次第 1、3、5 个共同完成的 Session 才是 T1/T3/T5；不是三个自然日，也不能拿一只资产的第 5 日替另一只缺失值。缺窗口时 Deferred，受界限耗尽则明确失败；迟到补跑按当前最早尚未完成期限截断模型可见阶段数据。**源码**：`crates/akzio-ingest/src/session_bars.rs`；`crates/akzio-daemon/src/outcome/collection.rs`；`crates/akzio-ingest/src/paper_decode.rs`；`crates/akzio-daemon/src/outcome/worker.rs`。
   三个标签都从**同一个 T0 baseline** 算累计窗口：T3 是 baseline→第三个共同 Session，T5 是 baseline→第五个共同 Session，**不是** T1→T3 或 T3→T5 的增量收益。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/outcome/collection.rs`。
3. T1/T3 写 Partial Outcome/阶段复盘并延期；T5 才有三期限和 `sealed_at` 的最终 Outcome。校准 collect 所用的**标签**不是账户后来实际 NAV、成交利润或研究的预估收益，而是 Outcome 引用的 Alpaca `bars:` NormalizedEvidence 中，**该资产基线交易日收盘价到对应 Outcome window 观察交易日收盘价**的相对收益：`(future_close − baseline_close) × 1_000_000 / baseline_close`。两端必须为正；`realized_return_ppm > 0` 才是概率校准的正例，零收益不是正例。Outcome v3 的冻结成交后敞口数值口径也不等于未来账户 NAV。**源码**：`crates/akzio-daemon/src/outcome/worker.rs`；`crates/akzio-domain/src/evaluation/outcome.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。
4. Collect 把 `Decision.created_at` 作为该样本预测时刻；标签时间用 `session_close_time(window.observed_trading_day)`。**这个函数固定取交易日的 UTC 23:59:59 作为校准窗口比较点，源码明确不是交易所收盘钟点的再次测量**；还要求标签时间晚于 Decision 创建时刻并有两端 bars。拟合又要求 `forecast_at < realized_at <= training_end`，不能把日历流逝直接认作价格标签已可用。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。

**NoOrder 与样本资格**：collect 筛选的是 canonical **Paper** Decision、同 Run 的 sealed Outcome、schedule/四资产 bars/12 个期限标签和模型身份；代码没有要求“成交过订单”，也不以 `Experience.learning_eligible` 或叙事 Complete 作为同一个收集布尔门。所以**有完整真实基线与成熟标签的 canonical NoOrder Run 可以成为预测校准候选**，这不等于把它记录为 Paper 成交或把隔离 Debug 变为 canonical。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/application/outcome_sealing.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

## 3. 第一道操作门：只读 readiness，不是“自动生成 Policy”

`readiness` 用 `Store::open_existing` 看至多最近 **500 个 Decision Artifact**，只取 Paper purpose、每 Run 一份；逐 Run 观察 `sealed / pending / blocked / no_schedule`，未封存时列具体缺的期限，基线快照缺失另标 `baseline_snapshot_missing`。它按合格 sealed Run 数给四资产×三期限的 12 个槽位填**进度数字**，并各展示最近最多 20 个 RiskLimits/Dataset/Policy Artifact 的 ID；500 条扫描上限达到时会标 `scan_limit_reached`。**源码**：`crates/akzio-cli/src/cli/calibration.rs`。

输出 `store_scope=canonical` / `calibration_eligible=true` 只表示该 Store **未被标为隔离 Debug**，不是已有足额数据、有效模型身份或候选；隔离 Store 的 Run 一律 `isolated_debug_store`，`next_step=use_canonical_store`。成熟度达到 CLI 给定的 `min_samples` 只提示可以尝试 collect，后者另查训练窗口、价格、风险限制和 provenance。`store_active_head_missing` 也不意味着“已经有候选、只差 activate”。**源码**：`crates/akzio-cli/src/cli/calibration.rs`。

`readiness` 的上述只读承诺**不能借给 `preflight`**：后者实际调用可写 `Store::open(scratch)`，合格分支的 `canonical_synthesizer_contract_hash` 还会调用 catalogue 安装入口。它不发模型请求，并不等于不可能写 scratch Store。当前仅为说明源码差异，未执行它。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/lib.rs`。

CLI 的 `readiness` 与 `collect` 都把 `--min-samples` 默认设为 **30**，而 Rust 输入校验仅要求 `min_samples > 0`；“30”是这条操作流程的默认请求值，不是从 `DecisionPolicy::default()` 推得的已存在样本，也不是一次 Run 的总二元预测条数。默认以完整 Run 计时，**30 个合格 Run 各提供 12 个 forecast 样本，即至少 360 个 slot 样本**；风险价格面另需四资产至少 **31 个共同价格点 → 30 个相邻收益**。30 个 sealed Run 并不自动保证四资产共同面板、尾部损失或 Brier 达标；不应通过降低参数、伪造标签或复制隔离 Run 来“完成”正式校准。**源码**：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。

## 4. 第二道门：operator 明确写入 14 项 RiskLimits

`set-risk-limits` 的 14 个 clap 字段都是必填、没有数值默认；转成 `OfflineRiskLimits`，先校验，才写 canonical `CalibrationRiskLimits` Artifact。它们是 operator 的**风险选择**，不是模型或历史训练自动给定的安全值。下面保留源码字段名，以免把概率质量、资金上限和训练样本门槛混为一项。**源码**：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。

| 类别 | 必填字段 | 进入候选 Policy 的含义 |
|---|---|---|
| 研究/过程 | `min_confidence_ppm`、`minimum_process_quality_ppm` | 决策置信度与研究过程质量下限。 |
| 信号/统计 | `min_probability_edge_ppm`、`max_brier_score_ppm` | 校准概率优势下限、资产及期限 Brier 上限。 |
| 时效 | `maximum_execution_delay_ms` | Decision 的最晚有效时间限制之一。 |
| 仓位/流动性 | `max_gross_weight_ppm`、`max_capital_weight_ppm`、`liquidity_weight_cap_ppm` | 总权重及单资产资本/流动性权重上限。 |
| 组合风险 | `target_annualized_volatility_ppm`、`max_portfolio_beta_ppm`、`max_expected_shortfall_ppm`、`max_gap_loss_ppm` | 波动、beta、尾部/跳空风险预算。 |
| 杠杆 ETF | `max_leveraged_holding_days`、`daily_reset_decay_ppm` | 跨期限持有上限与仅 TQQQ/SOXL 的日重置衰减门槛。 |

`validate()` 要求单资产资本和流动性 cap 为正且后者不大于前者，ES/gap cap 在 `1..=3_000_000 ppm`，持有上限 `1..=5` 天，decay 不超过 `1_000_000 ppm`；其他字段还经过 `DecisionPolicy::validate()` 的范围/三期限权重校验。这段校验不能证明 operator 选定的数值经济上合理。`min_calibration_samples` **不在这 14 项里**，而由 collect 的 `--min-samples` 写入构建输入。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

## 5. 第三道门：collect 将历史变成一个 SQL Dataset

1. **先验身份与来源**：collect 要求有效 `research.synthesizer` 模型配置显式含（或继承）`release_date` 和 `knowledge_cutoff`，取当前配置构成的 provider/model/版本 hash；只打开已有、非隔离 Store，读取**同一 Store 的 canonical RiskLimits Artifact ID**并校验，而不是从配置文件或本次 CLI 参数补造风险数值。日期字段存在并不独立证明填写的模型历史日期真实。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-cli/src/cli/calibration.rs`。
2. **逐 Run 筛选**：扫描最近至多 500 份 Decision；仅 Paper purpose 且 canonical、可验证的 Decision/DecisionContext，同 Run canonical sealed Outcome、正确的 OutcomeSchedule/Decision 引用、四资产 Alpaca `bars:` NormalizedEvidence 和每个 forecast 的 baseline/未来窗口价格。沿同 Run Decision 的 Artifact 来源图，最多遍历 256 个节点，寻找在 Decision 截止前、具有 provider/model/Contract 身份的 Synthesizer AgentTurn；当前 provider、model ID 不一致就跳过。被跳过的 Run 留 `skipped_runs`，不能靠别的 Run 填这个 Run 缺的 12 项。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-domain/src/decision.rs`。
3. **Contract 与训练窗口**：候选按 Run ID 排序，取第一份候选的 Synthesizer Contract hash，其他 Contract 不混合。`--training-start/end` 若提供须为带时区 RFC3339 且 start≤end；缺省从所选 Decision 最早 `created_at`、Outcome 最晚观察交易日的**名义 UTC 23:59:59**推导。每个 Run 在窗口内必须仍贡献完整 12 个 forecast；价格面只保留窗口内四资产共同日期，同资产/日期价格冲突阻断。输入拟合再核对每个样本 `forecast_at < realized_at <= training_end`、`forecast_at >= training_start`、预测 cutoff 一致、身份一致，且 `training_end <= frozen_at`、四资产价格严格升序/正值/在窗口内。这里是训练时间与来源校验，**不是独立留出集或样本外收益证明**。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。
4. **阈值与写入**：12 个资产×期限槽位每个至少 `min_samples`；总预测数至少 `12×min_samples`；四资产共同面板各至少 `min_samples+1` 个点；无价格冲突，才得到 `ready_for_build`。此时输入身份为 `regime="all"`，保存指定的训练窗口、风险限制、全部样本和价格面；把完整 `OfflineCalibrationInput` 与筛选报告写为 SQL `CalibrationDataset` CAS Artifact，source refs 指向 RiskLimits 和所选 Run 的 Decision/Outcome；`BLOCKED` 只报缺口，不写 dataset。`collect` 不生成、更不激活 DecisionPolicy。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-store/src/store/decision_policy.rs`。

**版本身份的准确边界（静态联读）**：历史 Synthesizer AgentTurn 提取并比较的是 provider、model ID 与 Contract hash；`HistoricalForecastSample.provenance.model_version_hash` 在 collect 组样本时填的是**当次当前配置的** `model_version_hash`，不是从每个旧 Turn 独立重新验出的历史版本 hash。离线 builder 随后只核对这些已填入的身份字段一致。故不能仅凭 dataset 此字段断言每个历史 Run 的实际模型发布日期、知识截止或版本参数都经过了历史级重验。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-execution/src/calibration.rs`。

## 6. 第四道门：build 的 12×10 bins 与四资产风险拟合

- `build` 只读指定 Dataset Artifact 的完整 `OfflineCalibrationInput`，纯 Rust `build_offline_decision_policy` 先验证，再对**四资产×T1/T3/T5 各一组**拟合。每组有覆盖 `0..=1_000_000 ppm` 的 **10 个连续原始概率 bin**（前九个宽 100,000，最后一个到 1,000,000）；样本按预测概率入 bin，正例是未来价格收益 `>0`。有样本的 bin：校准概率＝正例比例、校准 alpha＝实现收益均值；空 bin 留 `sample_count=0`、概率 `500_000`、alpha `0`，**不插值**。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`。
- 对每个资产×期限计算**原始概率**的样本均值 Brier，二元标签 `y∈{0,1}`，即以 ppm 表示的 `mean((p_ppm−y×1_000_000)^2/1_000_000)`；不是把十个 bin 等权平均，也不是对最后的组合收益打分。资产级 Brier 又汇总该资产全部三个期限的原始预测样本。fit 先要求各槽样本数达到 N；Brier 是否低于 operator 限额主要在后续 `decision_capable()` / 实际预测资格中检验。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。
- 风险拟合另使用**四资产共同日期相邻收盘**形成等长日收益面板，至少 N 个收益；QQQ 是固定 benchmark，零方差拒绝。分别量年化波动、相对 QQQ 的绝对 beta、负收益最差 5%（向上取整、限于实际负收益数）的平均损失 ES、最大单日负收益 gap、四资产有序 pair 的年化 4×4 对称 covariance。任何资产没有可测负收益尾部/正波动/beta，或风险值越界，都会失败；TQQQ/SOXL 才写非零 `daily_reset_decay_ppm`。风险模型是一份资产/组合层模型，不是三个独立 T1/T3/T5 风险模型。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。
- 候选将这 12 份 forecast calibration、四资产风险、组合矩阵和 14 项 operator 风险选择装入**同一** Policy；其他字段（特别是三期限权重）来自默认策略。Policy envelope 记训练起止、来源 Run、provider/route/Contract、模型 scope、input/output/risk-model 三种 hash；各槽的 `fit_dataset_hash` 则只取 forecasts 列表的 hash，不等同于完整 input hash。解码校验内层 Policy 与风险/输出 hash。没有源码中的独立 holdout/test-set 评估环节可把这里的**训练集 Brier**宣称成未来准确率。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。

**最容易误读的两个“足够样本”**：`decision_capable()` 检查 12 个整槽与资产/风险模型的 N 样本和 Brier 上限，却**不要求每个概率 bin 都有 N 个样本**。某次非中性 forecast 真正使用某个 bin 时，`calibrated_forecast()` 才要求该 bin `sample_count >= N`、校准冻结时间不晚于本次 Decision，且该槽的总样本/Brier 合格；不足则该预测没有校准值，资产可以归零。因此“30 个成熟 Run → 每个 bin 都能交易”是错误推断。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/calibration.rs`。

## 7. 第五道门：inspect、validate、activate 才能改变 active head

1. `build` 持久化 canonical `DecisionPolicy` **候选 Artifact** 并返回 Policy Artifact ID、input/output hash、样本数、`decision_capable` 与 `activated:false`。`inspect` 按指定校准类 kind 显示 Artifact 的元数据和正文；该分支没有独立重做 lifecycle Canonical 校验，不应把注释口径扩大为运行保证。`validate` 则要求指定 Canonical Policy Artifact，严格解码 envelope/结构/hash 并**单独报告** `decision_capable`，不切 head。`validate` 的 `"status":"valid"` 与 `"decision_capable":false` 可以同时出现：前者是结构/身份的局部检验，不能替代激活资格。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。
2. `activate` 明确拒绝隔离 Store；严格解码指定 Policy、核对当前配置的 Synthesizer provider/route/model ID/版本 hash，以及 Store **当前 active Synthesizer Contract hash**。安装 helper 再拒绝 `!decision_capable()`；没有任何“build 完成自动 activate”或从默认值生成 active head 的路径。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-cli/src/cli/identity.rs`。
3. Store 的不可变 CAS 正文与 SQL installation/activation/head 是不同层：descriptor 带 policy hash、envelope BLOB hash、provider、model ID/版本、route、Contract。`TransactionBehavior::Immediate` 事务核查同 hash 的 descriptor/Artifact 冲突，安装时记身份，然后在 head 不同的情况下追加带 `previous_policy_hash` 的 activation、更新 singleton head；重复激活当前 hash 幂等，时间不得倒退。完整性检查另核对 activation previous 链、时间及最终 head；CLI 不传客户端 `expected_head`，不要把这个过程误写成“任意并发更新都有调用方提供的版本 CAS 参数”。**源码**：`crates/akzio-store/src/store/decision_policy.rs`。
4. **血缘精度**：Dataset Artifact 的 source refs 指向 RiskLimits、Decision、Outcome；built Policy Artifact 的 `source_refs` 在当前 CLI 实现中是空的，Policy envelope 自带 `input_hash`/`source_runs`，build 输出另外给出 dataset Artifact ID。激活核验 envelope、Policy 与风险 hash 和模型/Contract 身份；源码中的 activate 路径并未重新读取 Dataset 并用其原文重算 `input_hash`。因此可以审计候选的 provenance，但不能把“Policy Artifact 通过一条直接 CAS source-ref 边引用 Dataset”或“activate 又完整重跑历史训练”写成已实现。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/calibration.rs`；`crates/akzio-store/src/store/decision_policy.rs`。

## 8. 激活以后：下一次 Core 的内存快照与身份边界

- 正常加载只取 **SQL active head 指向的不可变 Policy Artifact**；descriptor 与 envelope/hash/Policy 本体交叉校验，要求 OpenAI Responses provider、`research.synthesizer` route，并把 Policy 的 `active_forecast_calibration` 模型 ID/版本 hash 与**当前配置**比对。该配置 hash 涵盖 provider、base URL、模型 ID、release date、knowledge cutoff、reasoning effort、response language；修改这些语义字段可能使旧 Policy 不再适用。**源码**：`crates/akzio-cli/src/cli/identity.rs`。
- `preflight` 的 `decision_capable=true` 还要求 active Artifact ID、input hash、合格状态及**当前 Store 的 Synthesizer Contract**精确匹配；`readiness` 对已有 head 也只读比较 persisted active Contract。`activate` 在写 head 前同样检查 Contract。故“候选结构合格”“内容 `decision_capable()` 为真”“对当前配置/Contract 可用”“已经成为 active head”是四个不同断言。**源码**：`crates/akzio-cli/src/cli/calibration.rs`。
- 进程 `serve` 先加载并复制 Policy 到 `DaemonConfig`，`Daemon::open` 构造 `DecisionRuntime::new(store, config.decision_policy.clone())`；`DecisionRuntime` **按值持有**这份快照，后续 `decide` 从 `self.policy` 算 Policy hash/目标。固定 HEAD 没显示每次 Decision 都热重读 active Policy：**在进程已启动后激活新 head，不应推定旧进程已切换到新 Policy**。另一方面 scheduler 每个新 tick 会查 Store 是否已有 active head；若 head 变化而旧进程仍持默认快照，二者可能处于不同观察时点——这是静态联读风险，**未运行复现**。**源码**：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-daemon/src/orchestration/bootstrap.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`。
- **Contract 检查覆盖范围也要精确说**：`activate`、`preflight` 与 real Debug `serve` 路径明确比较当前 active Synthesizer Contract；普通非 Debug `serve` 在所示启动分支只加载 active Policy并核对当前配置的模型版本，未见同位置再次比较 active Contract。Health 的 `decision_capable`/Policy hash 取自 **内存中的** `DecisionRuntime`，不是实时 SQL head 状态。因此不能把“Store 中 active”或“Health 显示 ready”单独当作这一进程对最新 Contract 已复核、已获审批或已下单的证明；这是源码边界，**不是已发生故障报告**。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-daemon/src/orchestration/health_canary.rs`。

## 9. 激活后每个 T0 的输入：仍是新的 12 个 Forecast

**不要把历史 T+1/T+3/T+5 标签当成今天的信号。**历史标签在 `build` 时冻结为 `FrozenForecastCalibration`；新交易 Session 的三个 Analyst/Critic 期限组和 Synthesizer 再提出本次四资产×T1/T3/T5 的 12 个 `Forecast`。每份 Forecast 有原始正收益概率、模型估计收益、期限和 `ForecastThesis`；持有期必须与 1/3/5 个交易日相符。若 `expected_return_ppm == 0 && positive_return_probability_ppm == 500_000`，它是**精确中性/弃权**，后续不得被一个历史高收益 bin“复活”。研究方向通常先按模型 `expected_return_ppm` 的正负判，只有期望收益为零才用概率相对 500,000 打破方向平局。**源码**：`crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-domain/src/decision.rs`。

Synthesizer 还必须给出四资产＋现金的 `research_allocation` 和覆盖 12 预测、4 资产配置、现金的 17 份 `numeric_basis`，并经过精确 ProposalReview、Claim/Critique、Manifest 和引用闭包校验。任何不合法提案在正式 Decision 提交之前返回错误；合法研究配置另存为 `research_plan.raw/validated`，其权重**没有**作为 `target_with_risk_traced(decision_at, effective_confidence_ppm, forecasts)` 的参数。它不是正式组合的自动上限；研究显式现金也没有在该目标函数中形成一个独立的 `cash veto`。这只是代码数据流判断，绝不意味着可以越过证据门、审批或 ExecutionGate。**源码**：`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-domain/src/research_review.rs`；`crates/akzio-execution/src/decision_gate.rs`。

此时使用的是**当前进程持有的 Policy 快照**，不是每次 Decision 临时从 SQL 拟合或读“最新候选”。Policy 的 `horizon_weights` 恰有 T1/T3/T5 三项且整数和为 1,000,000 ppm。离线 builder 没覆盖这些权重，所以普通离线候选沿用 `333_333/333_333/333_334 ppm`；这是**固定配置比例**，不是历史回报自动学出的最佳期限分配。当前 collector 的 `regime="all"` 也不能描述成实时的状态自适应路由。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/calibration.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/orchestration/bootstrap.rs`。

## 10. 第一道运行时门：置信度、精确 bin 与三期限方向

按源码顺序，`target_with_risk_traced` 先 `Policy.validate()`；有效置信度非法则错误，**低于** Policy 最低值时立即给四资产零目标，记录 `decision.confidence.minimum`，风险指标保持未测量的默认值。有效置信度先由 DecisionGate 的来源独立性/共识逻辑从提案原置信度得到；达到门槛之后，它**没有再作为连续乘数**按置信度大小缩放目标。研究过程质量不足是另一个 `hard_blocker`，可能与计算出的非零 target 同时保存，留待 ExecutionGate 阻断。**源码**：`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-execution/src/decision_gate.rs`。

对于每份**非中性**的本次 Forecast，`calibrated_forecast` 精确找 `active_forecast_calibration.scope + asset + horizon`；该冻结校准的 `frozen_at <= decision_at`、整槽 `sample_count >= min_calibration_samples`、整槽 Brier 不超上限，且**本次 raw probability 命中的那个 bin**也至少有 `min_calibration_samples`，才返回 `(calibrated_probability_ppm, calibrated_expected_alpha_ppm)`。不找邻近 bin、不插值、不拿另一个期限代替；中性则直接返回 `None`。注意：正常 CLI 激活拒绝 `!decision_capable()`，但这个纯 target 函数内部并没有单独一句 `if !decision_capable() {全局归零}`；它按资产逐项核验，不能把默认值的零目标泛化成任意部分策略在所有调用下必全局归零。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-cli/src/cli/identity.rs`。

`horizon_trace` **先于杠杆持有期过滤**对 12 个 Forecast 分别赋方向：

| 该期限情况 | `HorizonSignalDirection` |
|---|---|
| 原始 Forecast 精确中性 | `Neutral` |
| 非中性但无当次合格校准 | `Uncalibrated` |
| 校准概率 `p >= 500_000 + min_probability_edge_ppm` | `Bullish` |
| 校准概率 `p + min_probability_edge_ppm <= 500_000` | `Bearish` |
| 其他有校准的情形 | `Neutral` |

同一资产只要任两个期限出现 `Bullish` 对 `Bearish`，就记录一对 `HorizonConflict`、把**该资产**从权重计算中排除；DecisionGate 同时把 `HorizonConflict` 加入**整个 DecisionContext 的 hard_blockers**。所以不能说“其余未冲突资产一定可交易”：ExecutionGate 会继承全局 blocker。特别是 TQQQ/SOXL 即使操作员只准最多持有 1 个交易日，随后目标计算会过滤其 T3/T5，但方向冲突扫描已经读过它们；T5 与 T1 相反仍可能阻断。`horizon_trace.sleeves[].included_in_target` 初始只反映合格校准并扣除冲突，**没有**完整代表最终持有期过滤、signal 或风险缩放后的非零权重。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-execution/src/execution_gate/core.rs`。

## 11. 第二道运行时门：三个期限如何汇总为某资产的信号

对一个**没有期限冲突**且资产风险样本/Brier 合格的资产，Rust 逐个取参加计算的非中性期限。QQQ/SOXX 不因 `max_leveraged_holding_days` 被过滤；TQQQ/SOXL 只纳入 `horizon_days <= max_leveraged_holding_days` 的期限。**参与**的任一期限若缺上一节的精确校准，整只资产停止计算，而不是仅忽略坏期限；原本中性或因杠杆持有上限被跳过的期限则不进入求和。未参加的期限不贡献权重，剩余权重按参加项的权重和重新归一。**源码**：`crates/akzio-execution/src/decision_gate.rs`。

令 `S = 1_000_000`，`H_a` 为当前资产参加的期限集合，`w_h` 为 Policy 三期限整数权重，`p_h` 为**历史 bin 校准后的**正收益概率，`a_h` 为该 bin 的历史价格实现收益均值；`d_a` 是资产 `daily_reset_decay_ppm`（QQQ/SOXX 必须为 0）。代码不是浮点一次算完，而是**每一项先做 Rust 整数除法，再求和，再按 `Σw_h` 归一**：

```text
E_a = [Σ(h∈H_a) ((p_h − 500000) × w_h / S)] × S / Σ(h∈H_a) w_h
A_a = [Σ(h∈H_a) (a_h × w_h / S)]             × S / Σ(h∈H_a) w_h

需：至少一个参与 Forecast，所有参与项校准完整，
    E_a ≥ min_probability_edge_ppm，且 A_a > d_a

signal_a = clamp(2 × E_a + A_a − d_a, 0, S)
```

上述除法对有符号中间项遵循 Rust 整数语义（向零截断），不是统计上不带舍入的精确均值。`a_h` 虽名为 *alpha*，builder 只是对对应 bin 的资产实现收益求均值，**没有减 QQQ benchmark**；模型本次 `raw_expected_return_ppm` 决定研究方向/精确中性和研究资格，却**不是**此处 `A_a` 的直接数值输入。无参与项、概率优势不够或 `A_a <= d_a` 分别留在资产排除 trace；所有资产被排除时记录 `decision.eligible_set.empty`，不是本函数报“已卖出”。**源码**：`crates/akzio-execution/src/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-domain/src/decision.rs`。

## 12. 第三道运行时门：单资产 cap、组合 cap 与现金

对 signal 合格的每只资产，先取四种额度，再计算初始权重（均为整数 ppm）：

```text
vol_cap  = floor(target_annualized_volatility_ppm × S / asset_annualized_volatility_ppm)
beta_cap = floor(max_portfolio_beta_ppm          × S / asset_beta_ppm)
cap_a    = min(vol_cap, beta_cap, max_capital_weight, liquidity_weight_cap)
w_a      = floor(cap_a × signal_a / S)
```

`max_capital_weight` 与 `liquidity_weight_cap` 来自**显式 operator 风险限制**，此处不是通过实时订单簿测出来的流动性。先将四资产总权重超过 `max_gross_weight` 的部分按相同比例向下缩放；再用组合冻结 covariance 的 `wᵀΣw` 开整数平方根估波动率，beta、ES、gap 则按各资产风险值线性加权。对超过波动、beta、ES、gap 上限的项目分别计算 `floor(limit × S / measured)`，取**最严格**比例统一缩所有资产，最后重算风险评估。它不会因此扩大权重；缩放到全零另记 `decision.post_scale.zero`。`TargetPortfolio` 恰含四资产，**现金是未分配 equity 的隐含余量**，不是把 Synthesizer 显式 `research_allocation.cash_weight_ppm` 直接复制进来。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-domain/src/core.rs`；`crates/akzio-execution/src/calibration.rs`。

### 12.1 只用于理解的完整整数算例（不是本机 Policy）

假设一份**已合法激活**的 Policy，三期限当前都为 QQQ 的非中性且同向预测、各命中 bin 有至少 N 条样本，`frozen_at <= decision_at`、总样本和 Brier 达标，并且没有别的 hard blocker。为便于展示，假设命中校准值如下，三期限权重沿用默认值：

| QQQ 期限 | `w_h` | `p_h` | `a_h` | `(p_h−500000)×w_h/S` 整数项 | `a_h×w_h/S` 整数项 |
|---|---:|---:|---:|---:|---:|
| T1 | 333333 | 800000 | 20000 | 99999 | 6666 |
| T3 | 333333 | 700000 | 14000 | 66666 | 4666 |
| T5 | 333334 | 600000 | 8000 | 33333 | 2666 |
| 合计 | 1000000 | — | — | **E=199998** | **A=13998** |

令 QQQ 的 `d=0`，则 `signal = 2×199998+13998 = 413994 ppm`。再**假设** operator 波动预算为 150000、QQQ 历史年化波动为 300000，得 `vol_cap=500000`；beta 预算 500000、QQQ beta 为 1000000，得 `beta_cap=500000`；operator 资本 cap=400000、流动性 cap=300000，则最紧 `cap=300000`，该资产**组合二次裁剪前**的 `w=floor(300000×413994/1e6)=124198 ppm=12.4198%`。若同一组合还有其他资产使 gross、vol、beta、ES、gap 超限，最终 QQQ 权重还可能更低。所有数值均为刻意设定的演示值，**不代表真实 Store 样本、已批准风险参数、交易建议或订单**；公式与整数步骤的依据为 `crates/akzio-execution/src/decision_gate.rs`。

**风险解释的边界**：Policy 对 covariance 检查完整、有界、对称、正对角，并不作完整正半定证明；组合方差若算成负数会返回错误。组合缩放后虽重算风险，并没有本函数内再循环直到所有指标全部低于限值的独立断言。ES/gap 是由历史日线估计并按资产线性聚合，不是本次账户的未来尾部情景模拟。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/calibration.rs`。

## 13. Decision 成功、Execution 放行与实际订单仍相隔几层

`DecisionGate::decide` 在已有合法 Proposal 的情况下把 horizon conflict 放进 `hard_blockers`，用 Policy 生成 target/risk/trace，然后保存 `DecisionContext` 与 `Decision`。`DecisionContext.target == Decision.targets`，同时仍保存 `research_plan.raw/validated`、`decision_policy_hash`、十二个 `horizon_trace.sleeves`、`runtime_trace.first_zeroing_branch` 和 per-asset 风险资格。**研究方案非零与正式目标全零**是可同时存在的；反过来，**非零 target 与全局过程质量/horizon hard blocker**也可同时存在。非法 Proposal/Manifest/Claim/Review 则在提交前返回 `Err`，**不是**一个“零正式目标的已完成 Decision”。有效期取 `min(decision_at + maximum_execution_delay_ms, 12 个 thesis_valid_until 中最早值)`，不是三期限到期日可以无限续期当前执行。**源码**：`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-domain/src/decision.rs`。

对目的为 Paper 的 Run，ExecutionGate 还要重新取得 Account/Quote/Clock 快照，继承所有 Decision hard blockers，复核有效期、原审批/资格/mandate、FreezeState、账户持仓和开放订单、报价新鲜度、市场 Session、因子、换手、容量、合规与依赖。**只有前置 blocker 为空才运行 Allocation**，后置检查仍可给 `NoOrder`；`DecisionContext.accepted()` 只表示无 hard blockers/material conflicts，不表示 Execution Verdict 已 Accepted。PositionPlan 图根本没有 ExecutionGate。**源码**：`crates/akzio-execution/src/execution_gate/core.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`；`crates/akzio-domain/src/decision.rs`。

若当前账户有权益 `Q`、某资产现有市值 `V_a`，Allocator 对正式目标的唯一直接金额转换是 `target_value_a=floor(Q×target_weight_a/S)`、`delta_a=target_value_a−V_a`：正差额才可能买、负差额才可能卖、零差额无单。随后还要限价、买入上限/购买力、换手及安全检查。**目标为零不是指示系统绕过 Gate 清仓**：无审批冷启动根本不进 Allocator；有审批且当前持有多头时，才可能生成减仓 Sell，仍不保证已提交或成交。`ExecutionVerdict::Accepted` 只证明当次计划获准；`PaperCommit` 先持久化 Commitment 和确定性 client_order_id，后续 Reconcile/Dispatch 才能触及 Alpaca Paper；Broker accepted、filled、Reconciliation Complete 各有独立事实与时间。**源码**：`crates/akzio-execution/src/allocation.rs`；`crates/akzio-execution/src/execution_gate/core.rs`；`crates/akzio-daemon/src/application/paper_execution.rs`。

**一个静态诊断不对称**：`claim_slot_eligible` 的真实方向资格强制同一 Claim/Critique 的 PriceMarketStructure＋Macro；`build_asset_eligibility` 的 `directional_evidence` 摘要还要求 NewsEvent，且这个诊断 `eligible` 字段不直接回写已计算 target。故应看实际 `runtime_trace` 与 `hard_blockers`，不能仅凭某个 `asset_eligibility.eligible=false` 断言此资产在目标函数中的首个归零分支。`horizon_trace.included_in_target` 也不是最终持仓证明。此处是源码联读，未用真实 Run 重现。**源码**：`crates/akzio-domain/src/decision.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-execution/src/decision_gate.rs`。

## 14. 从冷启动到“正常运行”的操作与状态地图

下表是**条件性、按依赖推进的既有流程**，不是已在本机执行的 runbook，也不建议伪造风险限制数值或绕过审批：

| 阶段 | 此时权威与可观测事实 | 下一步仍需什么 |
|---|---|---|
| A. 初次无 active head | SQL 读取 `None`，Daemon 冻结 Default；三期限权重虽在，12 个预测校准/风险面为空。 | 正式研究可观察；不能借默认值申请 Paper 下单。 |
| B. 正式 PositionPlan | 有合格提案时可保留非零 validated 研究分配，Decision 正式四资产目标零，在 Decision 截止。隔离 Debug 的 research-only 前置另行阻止 Decision。 | PositionPlan 本身不产历史 Outcome/正式标签。 |
| C. canonical Paper 冷启 | 真实非 Closed Session、合法图、研究/Decision/执行快照等前提成立且无审批绑定时，ExecutionGate 可 NoOrder，Evaluate 可冻 schedule。 | 不保证已产生 schedule，更没有订单或成交。 |
| D. 旧 Run 跨交易日 | 四资产共同完成 T+1、T+3、T+5，T+5 有三窗口 sealed Outcome。 | 无真实四资产基线/价格则不能填标签；时间流逝不补证据。 |
| E. 只读 readiness | 观察 Store 资格、封存/待期/阻断、每槽成熟数和缺口。 | 30 个成熟 Run 是默认收集门槛提示，不保证 collect/bins/risk 可用。 |
| F. operator 风险与 collect | 显式 RiskLimits 已入 canonical SQL；collect 筛模型/Contract、12 价格标签、共同价格和训练窗口，成功写 Dataset。 | 不能使用隔离样本；不能编造模型日期/来源。 |
| G. build 与复核 | 离线生成一个候选：12 校准面＋四资产风险＋默认三期限权重；inspect/validate 可读结构及 capable。 | 还未 active，且本次 raw p 命中 bin 仍可能样本不足。 |
| H. 显式 activate | 当前配置模型/版本、已存 Synth Contract、capable 和非隔离条件满足后 SQL head 更新。 | 已运行 Core 不自动获得新 Policy/审批；核新进程身份。 |
| I. 新一次 T0 决策 | 经正式 Core 读取 active 快照并获得新的 12 预测，逐 bin→冲突→E/A→资产/组合 cap 计算 targets。 | 非零 target 不是 Paper approval，也不是订单。 |
| J. 可能的 Paper 执行 | 原审批、当次 Account/Quote/Clock、全部 ExecutionGate 通过→Accepted→Commitment→受限 Broker I/O→回执/对账。 | 订单 accepted 不是 filled；Outcome 和将来的再校准仍另走周期。 |

注意“最终正常运行”不是一次性从 `NoOrder` 自动变成订单：后续新增 Outcome 不在线重拟合现有 Policy；要换政策仍需**再次**成熟标签、collect、build、核验、显式 activate，并确认实际 Core 使用的是哪份内存快照、原资格及审批是否与新 RuntimeIdentity 相符。活跃 Policy 也可能因某次证据/置信度/bin/方向/风险而给零目标，或因当前执行快照/审批而 `NoOrder`；这仍是合法的 fail-closed 路径。**源码**：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/execution_gate/core.rs`。

供对照现有 CLI 的**命令轮廓**如下；其中 ID 均指同一个 Store 的 Artifact ID，不是本地文件路径。本报告没有运行这些命令，`<limits...>` 必须由有授权的 operator 自行确定合法数值，不能用本说明中的算例替代。

```text
akzio calibration readiness --store <canonical-store> --min-samples 30
akzio calibration set-risk-limits --store <canonical-store> <14项显式限制>
akzio --config <config> calibration collect --store <canonical-store> --risk-limits <risk-limits-id> --min-samples 30
akzio calibration build --store <canonical-store> --dataset <dataset-id>
akzio calibration inspect --store <canonical-store> --artifact <policy-id>
akzio calibration validate --store <canonical-store> --policy <policy-id>
akzio --config <config> calibration activate --store <canonical-store> --policy <policy-id>
```

`readiness` 和 `validate` 不发订单；`set-risk-limits`、成功的 `collect/build/activate` 分别会写入**正式 Store**，并非“全流程只读”。`build` 输出的 `decision_capable=true` 和 `activated=false` 应同时看，后续才按既有身份/审批流程运行 Paper。**源码**：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

## 15. 五个最容易说错的结论

1. **“有 T1/T3/T5 就有三份 Policy”——错。** 一份 active Policy 内有三份 horizon weights、12 个 asset×horizon forecast calibration、四资产风险与组合模型；只有本次 Forecast 用它们算目标。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/calibration.rs`。
2. **“凑够 30 个 T5 Outcome 便能下单”——错。** readiness、collect 选中 Run 数、每槽样本、共同价格、整槽 Brier、当次 bin 样本、激活、运行身份、研究证据、审批和执行快照是连续但不同的门。尤其 30 个整槽样本分散于十 bin 后可能没有一个当次 bin 达30。**源码**：`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-execution/src/decision_gate.rs`。
3. **“模型 forecast 的 expected_return 直接乘三分之一就是仓位”——错。** 正式 `E/A` 消费历史 bin 的校准 p/实现收益均值，再经 signal、vol/beta/capital/liquidity/gross/风险缩放；模型的 expected_return 影响研究方向与中性资格，不直接充当正式 `A`。**源码**：`crates/akzio-domain/src/decision.rs`；`crates/akzio-execution/src/decision_gate.rs`。
4. **“T5 杠杆预测被过滤就不能阻断今天交易”——错。** TQQQ/SOXL 的持有期过滤晚于 horizon conflict；反向 T5 可造成全局 blocker。**源码**：`crates/akzio-execution/src/decision_gate.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`。
5. **“Policy active、target>0 就会发单/成交”——错。** Policy 内存是否更新、精确 ProposalReview、质量/冲突 blocker、当前审批、ExecutionGate、Commitment、Broker 订单及 filled 回执都要分别核。**源码**：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-execution/src/decision_gate/decide.rs`；`crates/akzio-execution/src/execution_gate/core.rs`；`crates/akzio-daemon/src/application/paper_execution.rs`。

## 16. 证据结论与本机未知项

**源码可以支持**：缺 head 时默认 fail-closed；canonical Paper 的无审批 NoOrder 冷启动在具备真实基线时可能持续到 Outcome；合格四资产×三期限的真实标签经 operator 风险限制、SQL Dataset、离线拟合、明确 inspect/validate/activate，才可能成为后来新加载 Core 的 active Policy。T0 新的 12 预测再按校准 bin、期限冲突、`E/A` 信号和风险限额生成 Rust 正式 targets。**源码不能替代**：本机 Store 的 active head/候选/样本数、真实模型版本日期、某次 Run 是否取得四资产行情及完整基线、T5 是否 sealed、是否已成交，以及激活后的进程是否已重新加载。以上均未访问或运行，状态为**未知**。**源码**：`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-daemon/src/scheduler/scheduler_tick.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/orchestration/bootstrap.rs`；`crates/akzio-execution/src/decision_gate.rs`。

**静态边界，而非已发生事故**：普通 canonical Core 对“已激活 Policy 与当前 Synth Contract”未见与 `activate` 同强度的每次显式比较；collect 历史 version hash 从当前配置赋值；Outcome 模型调用存在临时 objective 与冻结节点比较可能不一致的静态风险；Reconcile 的持久 effect intent 不保证一次 Attempt 耗尽后自动再次调度。这些都需要针对性的运行证据，不能把完整拓扑或这份文档当成真实闭环验收。**源码**：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-cli/src/cli/calibration.rs`；`crates/akzio-daemon/src/outcome/worker.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-runtime/src/runtime/catalogue.rs`；`crates/akzio-store/src/store/workflow/outputs.rs`。
