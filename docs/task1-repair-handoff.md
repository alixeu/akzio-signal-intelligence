# 任务一实现交接（2026-09-08）

**撤回原版“AKZ-01～34 全部 FIXED”的结论。** 本文件记录工作区已有实现与本次复核后的证据等级；定点测试不能替代正常流程、故障恢复、真实模型或 Paper 的组合验收。复核提出的容量、迁移、Draft、必需 Context、补评估、租约、重试、行动窗口与成本问题已继续修正，具体证据见 [复核修复记录](task1-followup-review.md)。

状态含义：`TARGETED` = 对相关缺陷已有代码修正与限定范围的离线回归；`PARTIAL` = 实现已推进，但该编号仍包含未完成的消费者/组合验收；`SAFE_CONVERGENCE` = 明确限定指标语义；`HARNESS_ONLY` = 有可执行测试基础，整链未验收；`DOC_SYNC` = 文档已修订。任何状态都不表示 real-Paper-verified 或 outcome/learning-verified。后续完整 Debug 入口见 [任务二](task2-debug.md)。

**任务二续修更新：** [离线 Debug 记录](task2-offline-debug.md) 已补充完整 NoOrder/Accepted、三阶段补跑与叙事修复、Canary 三 subject 及中断恢复的限定组合证据。下表保留未完成验收；`OFFLINE_COMPOSED` 只表示报告中列出的组合场景通过，不表示 AKZ-01～34 全部闭合。本文件末尾的任务一执行记录仍是历史记录。

没有运行真实券商写接口，没有创建 Paper order，没有开启 auto_paper，没有对用户真实 Store 执行迁移。临时 Rust Store 在仓库 `target/task1-harness` 下，迁移测试使用内存 SQLite 和隔离的 v13 结构文件；没有读取用户真实数据。保留现有 Rust Gate、任务 fencing、单 Session Commitment 与不可变 CAS。

## 实际实现选择

- Context/Agent：统一精确内部 source/kind/producer 规则和读授权；保持 Run、学习资格、lease/epoch 约束。向模型提供有界正文和 coverage/stage projection。角色 Prompt 分离，正式预算和 Draft/Submit 预留同时更新。
- Research：采用三个按 horizon 分工的 Analyst/Critic 对，而不是引入 ClaimBatch；保留单 Claim Schema，Synthesizer 直接依赖三份 Claim 和 Critic 路径。非中性 slot 由 Rust 检查已验证的 price/macro/news grounds，局部缺口按资产/horizon 降级。
- Scheduler/Task：Outcome 租约按 Run/outcome 独立，未到期和争用都是 Deferred；失败预算不计 Deferred；每个已提交阶段后重新计数，Outcome 退避 30 秒起、上限 5 分钟；阶段结束/取消时按 epoch 释放租约。按 Session/Outcome 保留 worker 服务能力，单 worker 交错处理。
- Evidence：逐项保留 40 项采集状态；执行安全错误仍 fail closed。行情基于交易所 calendar、美东收盘及 20 分钟可用性余量；T0 最近 252 根，Outcome 按 raw 价格、日期窗口和有界分页求共同 Session。
- Execution/Outcome：成交按数量、现金和去重 Receipt 重建。Outcome 衡量冻结的 T0 执行后敞口，未实现后续账户 NAV 账本。公司行动不进行猜测调整，无法建立一致 raw 口径就返回数值不可用。
- Learning：原研究身份、评估器身份和三类成本分离；市场窗口、研究充分性、叙事有效、风险真值、学习资格分开记录。只从有范围、有失效条件、有来源的 LessonProposal 生成 Draft/quarantine。
- Store/运维：复用现有 CAS、Task/Attempt 和查询投影。增加 Run/kind 表达式索引，查询改用 Run 定位；提供 execution/outcome/learning/cost 健康投影。叙事修复只在原 Run 上追加有界 revision，数值 Outcome 不改写。

主要变化是 Bug 修复与显式语义调整。没有引入替代持久化框架，没有单独的可写健康状态库。索引和 producer/kind helper 属于支撑修复的局部结构调整。

## AKZ-01～34 实现与验证状态

| 项目 | 状态 | 当前关键位置与实际做法 | 任务二优先断言 |
|---|---|---|---|
| AKZ-01 staged/commit 生命周期 | TARGETED | `crates/akzio-daemon/src/outcome/worker.rs::execute_outcome_worker` 先 `read_blob` 校验 Agent 返回值，再 fenced 写 RetrospectiveDraft；Learning 把已提交 draft 加入来源。 | 在 stage、Artifact commit、Attempt commit 三处中断，恢复时无 MissingArtifact/悬空引用；T1/T3 不提前结束 Worker。 |
| AKZ-02 kind/source 冲突 | PARTIAL | `akzio-context/src/context_broker/policy.rs`、`selection.rs` 和 `akzio-research/src/agent/errors_catalogue.rs` 对齐精确 producer/kind/source；同 Run、合法 overlay、sealed/eligibility 仍检查。 | 对 Decision/Execution/Outcome/历史合格学习对象逐类正反授权测试；伪造同前缀 producer 仍拒绝。 |
| AKZ-03 ToolGrant 冲突 | PARTIAL | `context_broker/manifest.rs` 与 catalogue 的来源集合一致，保留 attempt-bound grant 和引用闭包。 | Critic 实际 `read_claim_evidence` 读 Claim+授权 grounds；未授权引用、跨 Run、失效 epoch 拒绝。 |
| AKZ-04 全局 Outcome lease | TARGETED | `akzio-daemon/src/outcome/worker.rs` lease key 包含 Run/outcome；争用返回 DeferredUntil。 | A/B Outcome 可分别占租约，同一 Outcome 的两个 owner 只能一个写；跨进程恢复。 |
| AKZ-05 Deferred 耗尽重试 | TARGETED | `akzio-store/src/store/workflow/commits.rs` 与 `helpers.rs` 的失败统计排除 Deferred，过期恢复沿用同一规则。 | 多次等待后第一次真实失败仍有预算；真实失败预算不能被 defer 清零。 |
| AKZ-06 收盘与 UTC 日期冲突 | TARGETED | `akzio-ingest/src/session_bars.rs::session_closes` 使用 America/New_York calendar close+20m；`outcome/helpers.rs` 20m 轮询只代表唤醒时间。 | DST、提前收盘、收盘前后、供应商延迟与未来 bar 污染分类。 |
| AKZ-07 固定 6 bars | TARGETED | `outcome/materialization.rs`、`collection.rs` 请求 baseline 后有界日期窗；`session_bars.rs` 分页；按四资产共同完成日期选第 1/3/5 个。 | 单资产缺日期、短首屏、重复 token、长时间缺失。最多 16 页/响应 8 MiB/窗口 366 天，达到上限必须诊断。 |
| AKZ-08 已完成阶段重复调模型 | PARTIAL | `execute_outcome_worker` 先查 sealed/pending，再取最早未完成 horizon；Canary 已封存父分支恢复读取已有 draft，不重调 T5 模型。 | T2/T4 轮询模型调用为零；迟到补跑按 T1→T3→T5，每阶段只提交一次。 |
| AKZ-09 明确阶段任务包 | PARTIAL | `outcome/worker.rs` 提交 `learning.outcome_stage` v1，包含 outcome/horizon/baseline/cutoff、Rust 数值和原决策、先前复盘来源；运行目标绑定 horizon。 | T1 context/tool 中无 T3/T5 日价格或叙事；错误 outcome/horizon Submit 必须拒绝。 |
| AKZ-10 最近日线窗口 | TARGETED | `akzio-ingest/src/session_bars.rs` 研究行情 `sort=desc`、跟随分页、取最近 N 根再升序；不足最新完成 Session 的 252 根返回 Pending。 | 与含额外历史数据的已知序列比较 SMA/return 输入，不能使用最老 252 根。 |
| AKZ-11 内容新鲜度 | PARTIAL | `akzio-ingest/src/materialization/materialize_normalized.rs`、`quant_features.rs` 使用内容 available_at、latest_completed_session 和窗口元数据。 | 旧内容重新下载仍旧；历史 fixture 的显式兼容回退不得外溢生产。 |
| AKZ-12 adjusted/raw 混合 | TARGETED | 研究标注 adjusted_research，Outcome 使用 raw 基线一致数据；取得目标阶段后检查 baseline→阶段截止日的 corporate actions。窗口内行动、分页未完或行动日期不可靠时返回 DataQuality/不可用；窗口外行动保留来源。 | 拆股、分红、报告延迟和缺失行动数据。没有 quantity/cash 公司行动账本，因此不支持推算受影响窗口收益。 |
| AKZ-13 40 项全绑失败 | PARTIAL | `akzio-daemon/src/evidence.rs` 采集每项 status，保留成功 Artifact 与不可变 collection status；安全类失败拒绝，方向研究与增强类缺口进入后续研究。 | 非安全新闻/FRED 失败仍产生有缺口研究；账户/身份/时间污染/Store 错误不能变成普通缺口。 |
| AKZ-14 错误统一 Transport | PARTIAL | `akzio-ingest/src/adapters.rs`、`session_bars.rs::classify_evidence_response` 和 daemon dispatch 区分 auth、rate limit、pending、transport、permanent、data quality。 | 401/403 不盲重试；429 使用 Retry-After；5xx/timeout 有界重试；future contamination 仍拒绝。 |
| AKZ-15 Critic 触发门槛 | TARGETED | `akzio-daemon/src/application/research_run.rs` 按重要度、方向性或矛盾触发 Critic；`akzio-execution/src/decision_gate/decide.rs` 与逐 slot verification 一致。 | 低置信但重要/方向 Claim 不跳过审查；未参与非中性预测的局部缺口不误伤整个组合。 |
| AKZ-16 NoOutput 丢 Claim | PARTIAL | `akzio-runtime/src/runtime/workflow.rs` 给 Synthesizer 直接 Claim 依赖；`application/agent_session.rs` 使用 succeeded outputs-or-empty。 | Critic NoOutput 后 Claim 仍在依赖和 context；缺失 Critique 使相应预测中性。 |
| AKZ-17 12-slot 覆盖 | OFFLINE_COMPOSED | Rust 默认拓扑三组 horizon 对；Claim/Critique 依据上限 12，允许单期限 4 条单资产价格 + 4 条单资产新闻 + 1 条共享宏观；`research_coverage_is_complete` 检查四资产×三 horizon。 | 任务二已用真实资源范围的 fixture 完成 Scheduler→Evidence→三组 Agent→Synthesizer→双 Gate→OutcomeSchedule，并断言十二格研究充分性。真实 LLM 质量与日预算实测仍待验证。 |
| AKZ-18 局部缺口全局中性 | TARGETED | `akzio-domain/src/research.rs::EvidenceGap::blocks_slot` 加有界资产/horizon scope，旧缺省 horizon 继承 Claim；Schema/Prompt/Consumer 同步。 | SOXL T1 缺口不清空 QQQ/T3/T5；真正组合级安全 blocker 仍全局生效。 |
| AKZ-19 角色 Prompt 串用 | TARGETED | `akzio-research/src/agent/runtime_run.rs` 仅 Synthesizer 追加 claims/blockers 约束；Analyst 和 Outcome 使用各自 horizon 任务约束。 | 对各角色最终 prompt/Schema 组合做快照和真实协议测试。 |
| AKZ-20 Submit 预算不足 | TARGETED | `agent/runtime_run.rs` 为 Submit 预留预算，必须完成并持久化非空 Draft memo 才能进入 Submit；无法完成两阶段时显式报输入预算不足或 Draft 未完成，重试/恢复不重置消耗。 | 大正文、慢 Draft、最后一次修正、provider 实际 usage 和进程恢复；验证不会隐式跳 Draft 或超预算。 |
| AKZ-21 Manifest 只有清单 | TARGETED | `context_broker/manifest.rs` 先保障角色最低输入集合，缺失/超限显式拒绝，再分配可选背景；正文投影、coverage/stage packet 与受控全文读取保留。 | 真实 252-bar 数据下 24 Artifacts 与 128/192 KiB 限制；投影与全文授权预算分别受控，不能丢失必要对象后继续。 |
| AKZ-22 snippet/UTF-8 | TARGETED | `context_broker/reads.rs::search_snippet_range` 围绕命中返回片段，映射小写后的 Unicode 字节位置；read_range 拒绝切开 UTF-8。 | 长中文、大小写长度变化、多命中与边界；32 KiB 上限保留。 |
| AKZ-23 数量/现金重建 | TARGETED | `akzio-learning/src/evaluation.rs::realized_execution_at_prices` 以数量和现金处理实际 fill、费用、Receipt 去重和冲突；生产路径显式传原基线报价。 | 盈利全卖不负仓、部分成交/取消、重复回执、外部现金流缺失、实际 short 拒绝。 |
| AKZ-24 frozen/actual NAV | SAFE_CONVERGENCE | 新观察执行路径为冻结执行后敞口 V3，分列价格效应、有符号实施差额、初始估值差额和估算费用；旧 V2 保留，actual_account_nav_available=false。 | 前一 Run 评估期间后一 Run 交易不改变原冻结指标；UI/API 不称真实账户 NAV；midpoint→fill 差额与费用不重复计入。 |
| AKZ-25 成本口径 | PARTIAL | `akzio-store/src/store/trajectory.rs::model_usage_for_producing_run` 沿 Decision 任务依赖闭包；`outcome_model_usage` 和 `run_model_usage` 单独提供。 | T1/T3/T5、repair 和失败重试后，T0 production cost 恒定；其生产路径重试成本保留。 |
| AKZ-26 producer/evaluator 身份 | PARTIAL | `akzio-domain/src/evaluation/policy.rs::ExperienceEvaluationContext` v1；daemon/learning 填原研究 Contracts、Workflow Artifact/revision、evaluator；Store 校验 lineage。 | 更换 Outcome 模型不改变 producer；Canary subject 与父决策 producer 可区分；旧数据 unknown 不假装新语义。 |
| AKZ-27 完整度/学习资格 | PARTIAL | `akzio-learning/src/evaluation/materialization.rs`、Store policy gate 拆分 market/research/narrative/risk/eligible；缺失 RiskGroundTruth 继续 None。 | 四资产 bars 齐全但研究不全/叙事无效/风险未测量均不得晋升；合法降级/撤销不能被新资格检查阻止。 |
| AKZ-28 诊断和 narrative repair | PARTIAL | `outcome/worker.rs` 持久化可区分失败类别；`outcome/narrative_repair.rs`、Store repair enqueue 与 CLI/HTTP 使用原 Run，最多两次 repair task。修复后通过密封 Outcome 的普通资格/消费事务补评估；Canary 保留 cohort 检查。 | 数值 seal 后 schema/context/timeout/budget 区分；只允许 ModelUnavailable→Complete 链接 revision，不覆盖数值，不重复学习。 |
| AKZ-29 自由文本 Lesson | PARTIAL | `akzio-domain/src/evaluation/outcome.rs::LessonProposal`、Agent Schema 和 `akzio-learning/src/evaluation/policy_learning.rs` 限定资产/horizon、行为、排除条件、来源，最多四项。 | 缺排除条件/越界范围/非法 refs 拒绝；旧 lesson_candidates 文本不变成全资产 Proven Lesson。 |
| AKZ-30 Worker 保障 | PARTIAL | `akzio-daemon/src/worker.rs`、`akzio-store/src/store/workflow/commits.rs::claim_next_task_for_workload`：多 worker 保留两类服务，单 worker 交错，ready_at aging；失效 permit 可继续。 | 大 backlog+慢模型下双方都有进展；单 task 可恢复错误不终止池；真正 Store/supervisor 错误仍上报。 |
| AKZ-31 完成与健康状态 | PARTIAL | `akzio-store/src/store/learning/history.rs::run_lifecycle_health` 派生分维状态，经 replay/observer 输出；scheduler 已有 slot 报告原任务恢复需求，不新建 Commitment。 | T0 Completed 但 T3/Worker失败在查询中可见；同 Session 重试保持原 Run、client_order_id、Commitment。 |
| AKZ-32 交接测试基础 | OFFLINE_COMPOSED | `task1_handoffs.rs` 保留协议与定点回归；`akzio-daemon/src/task2_tests.rs` 和 `task2_worker_tests.rs` 补完整 dispatch、Canary、恢复与 WorkerPool 组合。 | 已执行范围见任务二报告；真实 LLM/Paper、真实历史库迁移、多进程压力与符合全部资格的正向晋升仍未验收。 |
| AKZ-33 文档事实 | DOC_SYNC | README、AGENTS、CONTEXT 和本交接同步；修正搜索大小写、学习条件、同 Run 成本、预算/版本、CLI fixture 语义。 | 对照任务二实际结果更新验证等级，不把 fixture 或 Doctor 单项等同 E2E。 |
| AKZ-34 全历史扫描 | TARGETED | v13→14 固定写 14，后续迁移可重开；Store schema 15 索引 `rebuild_artifacts_run_kind`；`free_reads.rs` 与 `learning/history.rs` 的 Outcome/Retrospective/Shadow 查询先按 Run/kind 定位。 | 迁移/重建后结果与 CAS 一致；大量 Run 的查询计划使用索引；同 Run 冲突 revision 仍被检测。 |

表中省略的 crate 路径前缀均为仓库 `crates/`；没有以“旧审计已经解决”为由跳过其上下游检查。

## 版本与兼容

| 维度 | 旧 → 新 | 兼容处理 |
|---|---|---|
| Domain schema | 10 → 10 | 新 scope、lesson、metric、evaluation context 使用 serde default/空值省略；旧负载仍可读取，旧 absence 仍是 unknown/legacy，不补造已验证身份或口径。模型新输出要求由新 Contract/Prompt/Schema 绑定。 |
| Store schema | 14 → 15 | `migrate_v14_to_v15` 单事务新增 Run/kind 表达式索引和元数据版本。v13→14 固定写 14；失败后重开继续下一步；已标 15 但缺失索引时补建索引。没有改写 CAS、ExecutionPlan 序列化或 Commitment ID。 |
| 正式 Contract | 14 / 18 → 20 | v20/Prompt 14 使用固定的 release envelope；Outcome 原文授权上限 32k tokens、128 KiB，模型输入仍为 12k。保留 v18 历史升级记录校验，安装与 Doctor 规则一致；候选权限仍走原 subset 检查。 |
| Prompt bundle | 11 / 13 → 14 | 角色 scope、coverage、stage、Submit reserve 与 Outcome projection v2 配套。 |
| Freshness candidate | 15 / 19 → 21 | 候选 Contract/prompt 使用其独立版本 21；保留候选审核/晋升路径，不自动激活。 |
| Outcome metric | 旧缺失 / V2 → 新观察执行路径 V3 | 观察执行路径使用 `frozen_post_execution_exposure_v3`，分列价格效应、实施差额、初始估值差额与估算费用；旧 `None` / V2 原样保留。未实现 actual NAV 或公司行动现金/数量账本。 |
| Evaluation context | 无 → version 1 | 新 Experience 将 producer 与 evaluator 分离；旧 Experience 不取得新上下文资格。 |
| Benchmark definition | 1 → 1 | 没有虚构 benchmark 版本升级。 |

升级先在**迁移事务写 Store v15 前**检查旧 active Contract 是否仍被 queued/leased/running 任务或未终结 Session 使用；发现 blocker 保留 v14 元数据并报 ContractUpgradeBlocked。启动 catalogue 还会先预检所有角色，避免遇到某个忙碌角色才发现已更新其他角色 head。单次安装仍在事务内重检。

旧任务不自动换哈希、不自动取消，不改写旧 Run。应先停止旧 worker 的新调度，在旧版本正常完成或由操作员合法处理未完成任务后再升级；终态 NoOrder/失败 Session 本身不会永久阻断。隔离 v13 结构的打开/重开/忙碌任务阻断已做定点回归；跨版本并发启动及含完整历史负载的存量迁移回放尚未验证，任务二必须使用隔离副本验证。新二进制不是旧任务的无条件兼容执行器。

Prompt/Contract/topology、时间与数据治理、Outcome 成本/metric 组件进入 RuntimeIdentity。旧 qualification/approval 不能自动复用到新身份；本轮没有签发、刷新或修改任何审批。

## 必要检查与未验证边界

原版记载的 18 个测试是上一轮记录，不能作为本轮全部改动的验收证据。本轮最终运行命令、结果、fixture 范围和仍需任务二覆盖的组合路径统一记录在 [复核修复记录](task1-followup-review.md)。

未执行真实模型/券商读写、多进程压力、完整 NoOrder/Accepted 图、全量 Doctor、App 分发打包或真实 T+5 学习周期。完整工作区验收应继续按任务二执行；本次没有将定点回归升级为全量验收。
