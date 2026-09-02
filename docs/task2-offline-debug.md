# 任务二：离线组合 Debug 记录（2026-09-08）

本轮继续保留任务一及复核修复，从真实 Scheduler、Daemon dispatch、AgentRuntime、双 Gate、Learning 和 Store 的交接执行离线组合测试。结论为：**下列限定场景达到 `offline-verified`，不是“AKZ-01～34 全部 FIXED”。** 原编号和剩余断言仍在 [任务一交接表](task1-repair-handoff.md)。

模型逐轮响应、行情、账户、审批/qualification 和 Broker receipt 均为隔离 fixture。测试中的 `RunPurpose::Paper` 用于执行 canonical Rust 治理路径，不代表发生了 Alpaca Paper HTTP 或真实 LLM 调用。测试显式保持 `auto_paper=false`；未更换真实审批，未修改或迁移用户 Store，未安装 App 分发包。

## 本轮发现并修正的实际交接阻断

| 触发与观察到的失败 | 修正与约束 | 实际验证 |
|---|---|---|
| Scheduler 编译三 horizon 图时，多次引用同一批 EvidenceNeed，WorkflowProposal 被 `artifact.source_refs` 拒绝。 | 三个 proposal 构造入口按 ArtifactRef 去重；保留 Artifact 全局无重复约束。 | Canonical NoOrder、Accepted、Canary 均从实际 Scheduler reservation 开始。 |
| EvidenceGate 的 collection status 只引用 EvidenceNeed，被 SemanticDetail 的证据来源规则拒绝。 | 仅精确的 `evidence.collection_status`、`akzio.ingest`、RunScoped、具备 task origin、来源全部为 EvidenceNeed 的状态记录例外；普通 SemanticDetail 规则不变。 | 完整 EvidenceGate 执行；另有错误 producer 前缀、source family、origin、来源 kind 的反例。 |
| 单资产清仓计划仍带四资产 information classifications，Snapshot 校验失败；组合账户 Snapshot 的 URI 为空，又被依赖健康检查判为 Partial。 | classifications 限于实际 ordered assets。仅 `execution.snapshot.account` 可从四个规范化账户来源推导服务观测，要求来源 family 与 scheme/authority 一致，使用最早 retrieved_at；缺失或混合来源仍维持 Partial。 | Accepted 清仓进入 Commitment→Broker fixture→Reconciliation；正常成功产生一次 Broker 调用。 |
| 完整研究依据进入 Outcome 后，请求估算突破 12k 两阶段预算；修复叙事时完整原文授权也超过旧 12k grant 估算上限。 | Outcome projection v2 减少重复 metadata、grounds 和路径细节；十二项预测用列/行表无损表达，聚合数值保留，省略项显式说明。完整原文保留在原 128 KiB / 24 Artifact 沙箱，grant 估算上限为 32k；实际模型总输入预算仍为 12k。 | 完整 T1、T3 和 T5 repair 的 Draft→Submit 正常执行，最低输入完整；T1 grant 不含后来采集的原始/规范化行情。过大输入仍会显式拒绝，不能据此承诺任意内容长度都能完成。 |
| Experience 要求 WorkflowGraph 的 origin.run_id 等于 producer Run，但正常编译的 CAS 图没有 Run origin。 | 改为验证 Store 中该 producer Run / revision 实际绑定的 graph artifact；不移除 topology、producer、metric basis 等校验。 | Rust-only T5 后的受治理叙事修复生成一个 Experience/Evaluation，原生产成本和身份保持。 |
| 三个 Canary Shadow 沿用父 EvidenceNeed，却按子 Run permit 重新采集，触发 `InvalidEvidenceNeed`。 | 登记的 Shadow 等待父 EvidenceGate 成功后，引用该成功 Attempt 的冻结 NormalizedEvidence；精确 snapshot 保存 collection status。Context 和 Doctor 使用同一登记关系，RawEvidence、后续刷新、无关 Run 不进入该 grant。 | 三个 Shadow 均完成研究和 T5；测试明确拒绝 RawEvidence 的 grant。 |
| 四个 Run 共用原父 schedule 的 Outcome 采集查找命中了其他 Shadow 的 EvidenceNeed；三类 Shadow 的 Contract/Topology 身份又被统一写成候选组合。 | Need 按 Run、task、schedule 查找。Contract arm 保留父 topology，Topology arm 保留 active Contract，Bundle 使用两个候选。Rust `decision.bound` 的候选身份从已成功 Analyst tasks 和 producing Run topology 核对，不伪造模型 origin。 | 三个受治理 subject 完成配对和评估。 |
| Canary 等待 Shadow 或中途失败时，已写入的中间产物出现在“成功 Attempt 输出”索引，Doctor 报 terminal-event lineage 错误。 | 非终结密封/评估保留 CAS、事件与评估账本，不写成功输出索引；最终处理完 cohort 后再结束任务。Doctor 仅允许登记的冻结证据/执行 lineage 例外。 | 中断时、恢复后均通过 Doctor；原 Outcome 和首个 Evaluation 保持，最终每个 subject 一个 Experience。 |
| 密封 Canary 恢复先重新采集行情，可能依赖新的外部可用性。 | 优先读取已封存 Outcome 和已提交 Draft，复用 cohort 冻结观测。 | 首个 subject 提交后注入测试中断，回收过期 Attempt，补齐另外两个 subject；模型调用数、采集数均不增加。 |
| 新 Contract 扩展原文 grant 后，旧 v18 release 边界拒绝升级；历史 Doctor 又只认可 candidate subset，不能认可已授权的 canonical 扩展。 | 当前明确支持 v20/Prompt 14 的限定升级边界，保留 v18/Prompt 13 历史记录校验；正式升级和 Doctor 使用同一规则。只有存在 canonical activation 的历史才适用例外，candidate 安装仍走原 subset 校验。 | 隔离 v14/v18 Contract 升级，忙碌旧任务阻断，旧 Contract 保留，候选越权拒绝，升级后 Doctor 通过。未对真实历史库做重放。 |
| 显式包含 Outcome 节点的 Debug 图查询时，节点被当成图外 worker 一律排除，触发 WorkflowGraphMismatch。 | 只把未在原图中的 scheduler 后置 worker 排除出图比较；原图节点继续逐项匹配。 | WorkerPool 的 Debug 调度图和 Contract 升级隔离测试均通过 snapshot / Doctor。 |
| 并发重跑中，处理器返回的短暂 Deferred 时间在状态提交前已过期，触发 InvalidTaskDeferral。 | TaskRuntime 提交时将过期时间推进至至少一秒以后，保留 Deferred 语义和原失败额度；Store 对直接传入非法时间的校验不变。测试的 T0 使用当前时间，不混用固定 fixture 时钟与实际任务提交时钟。 | 专门返回过去时间的回归仍得到 Pending、失败计数为零，随后完整 workspace 并发重跑通过。 |

## 已执行的组合场景

代码入口为历史测试场景；相关测试代码现已移除，本节保留当时的离线执行记录。

| 测试 | 主要断言 | 证据边界 |
|---|---|---|
| `task2_canonical_research_to_no_order_and_outcome_schedule` | 实际 40 项需求；三对 Analyst/Critic；每 horizon 的 4 条单资产价格、4 条单资产新闻、1 条共享宏观；完整 12 Forecast；研究覆盖成立；默认校准零样本导致 NoExecutableOrder；无 Commitment，仍创建 OutcomeSchedule。 | Fixture 通过正式 Schema、资源范围和 Gate，不能证明模型自行获得这些结论。 |
| `task2_accepted_liquidation_commits_before_broker_and_reconciles_once` | 原有 10 股 TQQQ、目标零敞口，Accepted 卖单；Broker fixture 被调用前已能从 Store 读取相同 Commitment/plan hash/client IDs；ReconciledPaper；同 Session tick 复用原 Run，不重复调用 Broker。 | 无真实下单；未覆盖每个 cancel/replace 或 HTTP 断点。 |
| `task2_daemon_catchup_t1_t3_rust_only_t5_and_narrative_repair` | 显式推进至第五个共同 Session 后按 T1→T3→T5 补跑；阶段 packet/window/cutoff 和最低 Context 一致；T5 模型不可用仍密封；repair 保留数值字节、不重采行情，进入正式资格评估；研究充分但风险未测量，learning_eligible=false；生产成本不因 Outcome 调用改变。 | 时间与行情均注入，不是实际等待五个交易日。 |
| `task2_frozen_canary_three_subjects_finish_without_promoting_unknown_risk` | 一个 canonical parent + 三个登记 Shadow，各自 T5 密封；Contract/Topology/Bundle 三个 Evaluation；风险/质量等证明不足时保持 Stage1 + Defer；Store 完整性通过。 | 没有证明满足全部条件后的 Advance 或正式晋升。 |
| `task2_canary_recovers_after_first_subject_without_reconsuming_frozen_facts` | 每实例、仅测试构建的 panic 注入在首个 Evaluation 之后；遗留 Attempt Running，经实际 Store 租约回收再执行；3 个 Evaluation/Experience，原四个 Outcome、首个 Evaluation、cohort observations 均不改写；零新增模型调用/行情采集。 | 模拟任务执行中断和租约恢复，不是 OS 杀进程/断电或多进程压力。日志中的该条 panic 是断言要求且被捕获。 |
| 两个 `task2_worker_tests` | 双 worker 分别阻塞 Session 和 Outcome，另一类都继续完成；单 worker 在两类 ready backlog 下交错执行；退出后所有 task succeeded，snapshot/Doctor 通过。 | 使用真实 WorkerPool/TaskRuntime/Store，handler 只模拟工作，不运行模型。不是大规模积压性能基准。 |
| `task2_elapsed_deferral_stays_pending_without_spending_failure_budget` | 处理器明确返回过去的 Deferred 时间，TaskRuntime 正常持久化未来 ready_at，任务 Pending、失败计数为零，Doctor 通过。 | 不改变传输重试、阶段失败额度或 Store 的直接调用校验。 |

## 交付检查与可复现命令

全部 Rust 命令从仓库根运行。测试将 `TMPDIR` 和 `AKZIO_STORE_ROOT` 指向仓库 `target/task2-harness`；Daemon 每个场景创建独立临时 Store，未绕过 akzio-store 写业务 SQLite。

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace
AKZIO_STORE_ROOT="$PWD/target/task2-harness/cli-fixture-final" \
  cargo run -p akzio-cli -- --config config/task2-fixture.toml run fixture-debug
swift build --package-path apps --scratch-path "$PWD/target/task2-harness/swift-build"
CFFIXED_USER_HOME="$PWD/target/task2-harness/swift-home" \
  "$PWD/target/task2-harness/swift-build/debug/AkzioChecks"
```

已执行记录：

- Rust workspace：53 个测试通过，0 failed、0 ignored；包含上述 8 个新增 Daemon/WorkerPool 场景和 collection-status 领域反例，以及原有迁移、协议、计算、租约、重试与公司行动测试。53 是测试函数计数，不是产品场景覆盖率。
- `cargo fmt --all`、`cargo check --workspace`、`cargo clippy --workspace --all-targets` 成功。Clippy 仍有两处非阻断建议：Learning 复盘提交函数的参数数量，以及既有 accounting 测试中可省略的 clone；未声称零 warning。
- 隔离 CLI 返回 `purpose=paper_dry_run`、`evidence=fixture/offline`、`status=completed`，Run ID 为 `3be1042182ca4082`。命令内部运行 Store Doctor，结束后临时服务关闭。
- Swift 编译通过，`AkzioChecks` 的 **2,216 项检查通过**。没有执行 macOS 分发打包、签名、安装或真实 App/Core UI 联调。
- 每条完整 Daemon 场景最终调用 `Store::verify_integrity()`；Canary 故障场景在中断后也检查。独立 CLI `store doctor` 是 HTTP 客户端，不把服务已关闭后的命令失败当作 Store 损坏。

本地日志位于 `target/task2-harness/`：`check.log`、`clippy.log`、`workspace-tests.log`、`fixture-debug-final.log`、`swift-build.log`、`swift-checks.log`。日志和生成 Store 不纳入 Git。

## 当前版本与剩余验收

Domain schema **10**，Store schema **15**，正式 Contract **20**，Prompt bundle **14**，freshness candidate **21**。Outcome metric 保留冻结执行后敞口 V3 / 旧 V2 的区别；ExecutionPlan 哈希和 Commitment ID 算法未更改。旧 active task 仍需在旧版本完成或由操作员合法处置后升级，不能静默替换 Contract。

以下仍未得到本轮证据，保留为明确的后续验收边界：

1. 真实 LLM 在合法资料、两阶段预算与工具限制下产出研究/复盘，以及真实 Responses usage、超时与协议兼容性。
2. 真实 Alpaca Paper 行情和账户只读对接、全部 HTTP 分类与分页边界的组合；实际订单提交、cancel/replace、断网后的 broker 幂等恢复。
3. 真正跨交易日的 T1/T3/T5、独立风险真值测量、足够样本/holdout/完整性证明齐备后的正向学习晋升。
4. 用户历史业务负载的隔离数据库迁移重放、跨版本并发启动、OS 进程崩溃/恢复和大积压压力测试。
5. App 签名分发及真实 UI/Core/SSE 联调。公司行动现金/数量账本和真实后续账户 NAV 仍未实现；当前行为继续拒算不可靠窗口并明确标注冻结敞口指标。

这些限制不会用 fixture、Rust-only 数值密封、Defer 或编译成功替代。真实环境验证应按 [任务二入口说明](task2-debug.md) 与既有授权边界继续。
