# 等价减法审计实施与验证

日期：2026-09-08。输入为用户提供的 `akzio-subtraction-audit.md`、`akzio-subtraction-audit-evidence.md`、`akzio-subtraction-audit-metrics.json` 及对应说明。本记录以当前工作区源码和本次实际运行结果为依据，原审计的 ZIP 扫描结果不充当编译或运行证据。

本轮已实施 A01–A05、B01–B08、C01。C02/C03 已逐项核对并保留公开入口：仓内没有调用不足以证明外部消费者不存在，也不足以证明治理能力已经废弃。D 类作为等价性边界保留。

## 已实施项目

| 编号 | 实施结果 | 保持的边界 |
|---|---|---|
| A01 | Store 直接调用现有 `validate_paper_session_reservation` | 校验仍在获取连接/事务之前，原校验块与被复用实现逐字核对一致 |
| A02 | 三处来源构造改用 `permit.artifact_origin()` | Run、Task、Attempt、Contract 四个来源字段完整保留 |
| A03 | 金额解析删除第二次 `whole.is_empty()` 分支 | 前置输入检查及整数/小数精度规则保持 |
| A04 | 删除插入 collection-status 后必不成立的空集合检查 | 非 Paper 路径原有空集合错误与 fallback 保持 |
| A05 | 删除 `LiveDomainProjection.swift`、`ObserverPayloads.swift` 两个只有 import 的文件 | Swift Package 的动态源码发现、现有 typed/JSON projection 保持 |
| B01 | 两个事务入口共用私有 `insert_session_slot_transaction` | 入口原有 lease 检查的位置和次数保持；普通预约仍先查重；Canary 原有检查保持；setup、proposal、approval、workflow、events、slot、consumption 仍在同一事务 |
| B02 | 三个 Paper 入口共用 `paper_proposal_artifact` 和 `prepare_paper_workflow_commit` | 原 Run ID、传入时间、Proposal refs 排序去重、校验顺序、staging Store 保持；无 binding 路径仍不创建 Proposal；Runtime 仍先返回已有 Session |
| B03 | Tool Artifact 共用固定元数据；AgentTurn 共用请求快照和 Artifact 构造 | ToolCall 先持久化再执行；成功/失败分别记录原事件、原 payload、各自 source_refs；模型失败的可选诊断、错误返回与审计写失败传播保持 |
| B04 | Outcome 共用基础校验、执行/NAV 准备和快照构造 | Full 先检查完整观察，Partial 先计算 NAV 再检查到期观察；各自验证和 sealed_at 保持；纯市场引用排序去重集中到快照构造；窗口计算算法保持 |
| B05 | Draft/正式 Proposal 共用私有 DFS | 两种输入各自先做 UnknownDependency 检查；BTreeMap 和依赖数组的遍历顺序保持 |
| B06 | `akzio-domain::manifest_input_hash` 同时供 Context 和 DecisionGate 使用 | 只哈希有序 `(artifact_id, kind)` 元组，经相同 JSON 转换和哈希步骤；顺序、重复项均有意义 |
| B07 | 在 daemon 现有控制协议层公开 `LessonInput`，CLI 和 HTTP 直接使用 | 字段顺序、500000 默认置信度、空数组默认值、未知字段拒绝、CLI/HTTP 校验和认证保持 |
| B08 | `RegimeSnapshot` 测试构造复用本文件私有 fixture | snapshot hash、classification、decision_at、as_of 显式传入，前视和 ExPost 两个场景及断言保持 |
| C01 | 删除 9 项未使用 dev-dependency 声明，以及无人直接引用的 workspace `tower` 声明 | 已分别检查涉及的 8 个 crate；`Cargo.lock` 仅移除对应 9 条直接依赖边，没有升级包版本或删除传递依赖包 |

C01 的具体清理：cli/context/execution/ingest/learning/runtime 的 `tempfile`，ingest 的 `rusqlite`，daemon 的 `tower`，model 的 `tokio`。daemon/research/store 仍实际使用的 `tempfile` 保留。

主要实现位置：

- [Store Session 写入](../crates/akzio-store/src/store/impl_workflow.rs)、[Runtime Paper 构造](../crates/akzio-runtime/src/runtime/workflow.rs)。
- [工具审计](../crates/akzio-research/src/agent/tools.rs)、[模型调用审计](../crates/akzio-research/src/agent/runtime_helpers.rs)。
- [完整 Outcome 与共享准备](../crates/akzio-learning/src/evaluation/materialize_outcome.rs)、[Partial Outcome](../crates/akzio-learning/src/evaluation/materialize_partial.rs)。
- [共享输入哈希](../crates/akzio-domain/src/context.rs)、[Proposal DFS](../crates/akzio-domain/src/workflow.rs)、[共享 Lesson DTO](../crates/akzio-daemon/src/lib.rs)。

## 已核对并保留的公开 API

以下结论来自当前仓库的 Rust、文档、脚本及 Swift 引用扫描。没有证据可以确认工作区是这些公开 Rust API 的唯一消费者；`publish = false` 也不能排除外部 path/git 依赖。

| 候选 | 当前证据 | 处理结论 |
|---|---|---|
| `observatory_configuration` / `set_observatory_configuration` / `clear_observatory_configuration` | 当前 CLI 配置写入走配置文件；三个 Store 方法在仓内只找到定义，既有 Store schema 仍保留配置表 | 保留公开 API 和历史数据解释；没有废弃声明或外部迁移证据 |
| `plan_retention` / `apply_retention` | 有完整的计划、受保护引用闭包、游标防过期检查及事务实现；仓内未接调用入口 | 保留维护能力，不根据文本引用数删除 |
| `record_experiment_trial` / `record_search_bias_certificate` | 写入入口未找到调用，但 Canary 的 cohort 预约实际读取 `experiment_trial_ledger` 和搜索偏差证书 | 存在治理消费端，不能把尚未接入的生产入口当死代码 |
| `assemble_lesson_ablation` | 实现独立 grant、基线闭包、overlay 资格和有无 Lesson 的差分检查 | 保留受控消融能力 |
| `record_repair` | 实现 grant/source 检查和 `ContextRepaired` 审计事件；本次仅复用其 origin 构造 | 保留公开修复接口 |
| `record_risk_ground_truth_assessment_fenced` | 实现 Paper 限定、阶段封存、来源、重复评估与 fenced 写入检查 | 保留风险真值入口，缺少调用属于接入证据缺口 |
| `assess_capacity_study` | 提供账户规模和同质 Agent 数量的容量研究网格，复用 `assess_capacity` | 保留公开研究计算，未证明能力已废弃 |

## 保持的不同语义

- D01：恢复事件与普通 Context 事件的 Attempt 权限规则保持；未修改 `free_events.rs` / `free_trajectory.rs`。
- D02：canonical Paper 与 Debug/PaperDryRun 的证据 adapter / fixture fallback 保持，`evidence.rs` 仅删除 A04 的不可达检查。
- D03：Partial/Sealed、V2/V3、未知风险、NoOrder 后评估及 T1/T3/T5 学习资格保持。新增回归覆盖 Partial 窗口与对应 Full 窗口一致、拒绝提前封存、缺失/重复观察及首个错误顺序。
- D04：typed Swift horizon 默认值与动态 JSON 的 nil 行为保持，未修改 projection 实现。
- D05：历史 migration 与最新建表 SQL 保持，未改 schema_version、表结构或迁移逻辑。
- D06：Draft→Submit、累计预算、Execution Gate、Commitment 先落盘、恢复与独立 Outcome 时间轴保持；原有相关测试全部保留。

## 本次验证

修改前：`cargo test --workspace` 通过，53 个测试、26 个 suite。

| 检查 | 本次结果 |
|---|---|
| `cargo fmt --all`、最终 `cargo fmt --all -- --check`、`git diff --check` | 通过 |
| `cargo check --workspace --all-targets` | 通过 |
| 8 个 crate 分别执行 `cargo check -p <crate> --all-targets` | 全部通过；覆盖 C01 的全部清理对象 |
| `cargo test -p akzio-daemon --lib task2` | 第一批实现后 8 个测试通过 |
| `cargo test -p akzio-research --test task1_handoffs` | 最终实现及新增 Session/DAG/审计断言后 14 个测试通过 |
| `cargo test -p akzio-learning --test task1_accounting` | 新增 Full/Partial、V2/V3 与错误顺序断言后 7 个测试通过 |
| `cargo clippy --workspace --all-targets` | 退出 0；2 个原有警告，见下文 |
| `cargo test --workspace --locked` | 57 个测试通过，28 个 suite |
| 隔离 `run fixture-debug` 与内置认证 HTTP Store Doctor | 通过；Run `8a805baf4175414b`，`paper_dry_run`，`completed` |
| `swift build` | macOS 构建通过 |
| `swift run AkzioChecks` | 2216 项检查通过 |

Clippy 的两个警告均位于原有代码：`evaluation/outcomes.rs` 的 `seal_outcome_for_evaluation_fenced` 有 8 个参数；`task1_accounting.rs` 原有测试中 `[r.clone()]` 可用 `std::slice::from_ref`。本轮没有以抑制警告改变 API，也没有宣称 `-D warnings` 通过。

新增的精确字节契约回归覆盖 manifest 哈希和 Lesson 输入。Store/Runtime 回归验证了校验先于 fencing、Store 对重复 Session 仍检查 lease、Runtime 原有先查重行为、失败后无新增 Artifact/Session/事件。审计回归分别注入正常响应、越界工具读取、模型失败，检查事件顺序、来源和原错误保留。

Fixture 使用 `config/task2-fixture.toml`，命令级 `AKZIO_STORE_ROOT` 指向本次新建的 `target/subtraction-fixture-8egk_pt6`。实际命令为 `cargo run -p akzio-cli --locked -- --config config/task2-fixture.toml run fixture-debug`。`fixture-debug` 是 PaperDryRun；其临时 daemon 内部通过与 `store doctor` 相同的认证 HTTP 入口调用 `Store::verify_integrity()`，完成后关闭。本次 Doctor 由 fixture 内部执行，没有单独向已经退出的 daemon 发送 `store doctor` 命令。没有把本机 App 的 Store 或审批用于测试。

## 行数与运行身份

相对本轮修改前的 Git HEAD，统计物理行，包含空行、注释；测试目录单列：

| 范围 | 净变化 |
|---|---:|
| 本次触及的非 `tests/` Rust 源文件 | -262 行 |
| Rust `tests/`（包含新增契约文件） | +406 行 |
| 全部 Rust | +144 行 |
| Swift | -2 行、删除 2 个空文件 |
| Cargo manifests / lockfile | -31 行 |

上述统计不把新增测试隐藏为“全仓净删行”。收益是相同校验、构造和哈希规则集中维护，同时补齐本轮等价性证据。

业务 Domain/Store/Contract/Prompt 版本及序列化契约保持。源码字节和 `Cargo.lock` 的变化会正常进入新的构建身份、Prompt/Contract/Topology 组件身份；对应旧 Manifest/Approval 是否还能匹配由原有检查决定，本轮未写新审批、未绕过身份比较。

交付等级：`implemented`、`offline-verified`。本轮未执行真实模型/Alpaca Paper HTTP，也未经历实际 T+5 交易日闭环，因此不标记 `real-Paper-verified` 或 `outcome/learning-verified`。
