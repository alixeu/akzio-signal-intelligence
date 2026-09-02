# 任务一复核补修记录（2026-09-08）

本轮保留上一轮工作区实现，针对复核指出的交接问题补修，不进行全量架构重写。原交接中“AKZ-01～34 全部 FIXED”的结论已撤回；逐项状态见 [修订后的交接表](task1-repair-handoff.md)。这里的通过仅指下列实际执行的定点测试与检查。

本文件保留任务一复核时的历史记录。后续 Daemon 整链、Canary 与恢复测试，以及当前版本和验证结果，见 [任务二离线 Debug 记录](task2-offline-debug.md)；不能把本文件当时的“未执行”误读为后续结果。

## 四个主要交接

| 问题 | 当前实现 | 已执行的回归与限制 |
|---|---|---|
| 真实资源范围下的 12-slot 覆盖 | Claim grounds、Critique grounds/supporting_refs/conflicting_refs 上限从 8 调为 12，保留三组 horizon 节点。每期限最低合法组合为 4 条单资产价格、4 条单资产新闻、1 条共享宏观。Prompt、Contract 与输出预算同步。 | `review_production_scoped_claims_and_critiques_reach_twelve_slots` 实际调用正式 AgentRuntime、Context、Schema 和资源范围校验，执行 T1/T3/T5 三组 Analyst/Critic，结果通过 `research_coverage_is_complete`。模型为脚本 fixture；未运行 Synthesizer、DecisionGate 或完整 Daemon 图。领域测试也改成合法的单资产依据。 |
| v13 串联迁移 | v13→14 固定写入目标版本 14；下一步成功才写 15。已标 15 但缺少可重建索引时，在打开时补建；旧任务 preflight 仍阻断升级。 | 从隔离 v13 结构 SQL fixture 调用 `Store::open`，检查串联升级、重开、第二步故障后仍为 14、移除测试故障后恢复、旧任务跨重开持续阻断、15 缺索引修复。没有迁移用户 Store，也没有证明全部历史业务负载兼容。 |
| Draft 与最低 Context | Submit 必须有非空且已持久化的 Draft memo。输入预算无法容纳两阶段就显式拒绝；输出/时间预留边界不能静默完成 Draft。Manifest 先保障角色必需对象，再分配可选背景；缺失或必需集合超限时拒绝。 | 大输入测试分别验证可行的 Draft→Submit 两轮及不可行输入零模型调用；Context 测试验证缺 DecisionContext、可选大背景挤占、必需对象自身超限。Outcome 正常协议测试仍执行只读工具、Memo、错误 Submit 和修正 Submit。Context 最低集合测试使用最小 JSON fixture，不能替代完整历史上下文规模测试。 |
| Rust-only T5 后补评估 | `evaluate_sealed_with_retrospective` 读取已提交 Outcome，进入普通研究/风险/叙事资格检查及消费事务。Complete revision 引用旧复盘、受治理 Draft 和原 Outcome；以 subject/outcome 幂等复用 Evaluation。原生产身份、数值与生产成本保留。 | `review_rust_only_t5_repair_reenters_governed_evaluation_once` 通过 Scheduling/Learning/Store 完成阶段写入、Rust-only 密封、repair enqueue、fixture Draft 补评估；核对原 Outcome 字节、revision 来源、生产身份/成本、资格不足不晋升、重复评估复用。分别验证终结任务与暂不终结任务的事务模式。未执行完整 Daemon repair Agent 或真实模型。 |

可直接观察到的原缺陷失败包括合法第 9 条 grounds 被 `schema.maxItems` 拒绝，以及 v13 打开报 `v15 migration requires v14 metadata`、第二步失败后保留了错误版本。修复后对应回归通过。其余新增回归不统称为已经完成先红后绿的实验。

关键实现：

- `crates/akzio-research/src/agent/{schemas,catalogue,errors_catalogue,runtime_run}.rs`
- `crates/akzio-context/src/context_broker/manifest.rs`
- `crates/akzio-store/src/store/{migration,free_validation}.rs`
- `crates/akzio-learning/src/evaluation/materialization.rs`
- `crates/akzio-daemon/src/outcome/{narrative_repair,canary,worker}.rs`

## 其余边界修复

**租约与重试。** per-outcome 租约在正常返回、报错及 future 取消时通过 Drop guard 按 owner/epoch 释放，保留递增 epoch；释放失败仍由 TTL 兜底。失败预算以已提交的阶段 Retrospective 事件为边界重新计数，Deferred 不计失败。Outcome 普通可重试错误从 30 秒开始指数退避，上限 300 秒；供应商明确给出的 Retry-After 继续单独处理。已测试立即重新获取并拒绝旧 epoch、异步取消释放、T1 故障成功后 T3/T5 独立计数，以及退避序列；多进程争用和完整迟到补跑仍待任务二。

**公司行动窗口。** 采集保留 raw bars 与公司行动响应，选择当前阶段后再校验 `baseline < effective_date <= stage_cutoff`。T8 行动不会阻断不受影响的 T1/T3/T5；窗口内行动、未知日期语义和不完整分页仍不可计价。请求包括 `data_quality=all`，避免供应商默认过滤不完整记录；日期字段按行动类型区分 ex_date、effective_date、process_date。字段依据为 [Alpaca 官方 API](https://docs.alpaca.markets/us/reference/corporateactions-1) 和 [官方 Python SDK 模型](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/data/models/corporate_actions.py)。本机 HTTP 测试验证 raw 请求、晚期行动保留和目标窗口校验；没有验证线上供应商报告时效。

**冻结收益与实施成本。** 具有执行上下文基线报价和实际成交记录的路径采用 `FrozenPostExecutionExposureV3`：

```text
冻结净收益 = 冻结价格效应
           + 有符号 baseline_mid→fill 实施差额
           + 初始账户估值→baseline_mid 的估值差额
           - 估算费用
```

买入差额按 `数量 × (baseline_mid - fill)`，卖出按 `数量 × (fill - baseline_mid)`，再除以初始权益。limit_shortfall 只作诊断，不重复扣除。`baseline_mid` 来自原执行上下文报价；没有实际决策时点/券商到达时点的报价证据，因此 `decision_mid`、`arrival_mid` 继续为 None。费用仍是配置费率估计，不冒充券商实际费用；也没有补造后续实际账户 NAV。

原反例（权益 1000、baseline 99.5、买入/limit 100、数量 10、末价 100、无费用）现在由约 +5000 ppm 价格效应与 -5000 ppm 实施差额相抵，净值误差在 1 ppm 舍入范围内。另测有利部分卖出、单次费用扣除和初始持仓估值差额。Rust 窗口/净值/benchmark、Observer 和 Swift 展示已接入可选新字段；旧 V2/缺失字段保留原语义，未自动改标。

**Canary 评估交接。** 补叙事沿用原 cohort 检查，密封数值后可保持 Attempt 有效，多个 subject 的评估分别受原租约约束，cohort 处理完成后再结束任务。已存在的 immutable observation 不反向改写成新质量结论。定点回归只证明 Learning/Store 的非终结提交接口；完整 cohort、三 subject 中途失败重试、样本消费与最终晋升未验收，AKZ-27/28 保持 PARTIAL。

## 本轮实际检查

| 实际命令 | 结果 |
|---|---|
| `cargo test -p akzio-research --test task1_handoffs` | 12 passed |
| `cargo test -p akzio-store --lib task1_migration_tests` | 6 passed |
| `cargo test -p akzio-learning --test task1_accounting` | 7 passed |
| `cargo test -p akzio-ingest --lib session_bars::tests` | 5 passed |
| `cargo test -p akzio-domain --test task1_coverage` | 3 passed |
| `cargo test -p akzio-context --lib search_snippet_surrounds_late_unicode_match` | 1 passed |
| `cargo test -p akzio-runtime --lib review_retry_delay` | 1 passed |
| `cargo test -p akzio-daemon --lib review_cancelled_outcome_future` | 1 passed |
| `cargo check --workspace --all-targets` | passed |
| 改动/新增的 86 个 Rust 文件：`rustfmt --edition 2024` 后执行 `--check` | passed |
| 改动的 6 个 Swift 文件：`xcrun swiftc -parse` | passed；仅语法检查，不是 App 构建/类型检查 |
| `git diff --check`；三份交接文档的本地链接检查 | passed |

合计 **36 个定点测试通过**。它们使用仓库内临时 Store、脚本模型、本机 HTTP 或纯计算 fixture；都不是 real-Paper-verified 或 outcome/learning-verified 的证据。

## 版本与未完成验收

相对上一轮实现，正式 Contract 16→18、Prompt bundle 12→13、freshness candidate 17→19；Domain schema 仍为 10，Store 仍为 15。新观察执行的 metric basis 为 V3，旧 V2 保留；evaluation context 1 与 benchmark definition 1 不变。版本更新进入现有 RuntimeIdentity，未改写旧任务 Contract hash、旧 CAS、ExecutionPlan 序列化或 Commitment ID。

本轮是 `implemented` 加上述限定用例的 `offline-verified`，不代表完整系统离线验收。未执行 workspace clippy/test 全套、完整 fixture-debug/Doctor、完整 NoOrder/Accepted 图、多 Session/多进程恢复、完整 Canary、真实 LLM/Paper、T+5 学习闭环或 App 分发打包。也未对用户真实 Store 做迁移、创建券商订单、开启 auto_paper 或更换审批。

任务二按 [完整 Debug 断言](task2-debug.md) 继续，并特别保留：正式图产生实际 12-slot proposal、正常/补修评估的完整消费者、真实历史隔离副本迁移、冻结 cohort 的重复消费与恢复，以及真实运行成本/身份不变性。上述组合路径通过前，不再将 34 项统称为全部闭合。
