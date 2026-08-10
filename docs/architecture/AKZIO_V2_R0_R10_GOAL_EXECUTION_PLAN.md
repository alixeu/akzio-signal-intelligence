# Akzio v2 R0–R10 Goal 执行计划

日期：2026-08-10

状态：**R0–R6 complete / R7 ready**
适用 checkout：`codex/akzio-v2-max-refactor`，R6 阶段快照（基线 HEAD `faf493c8ba40e0c839ab221d8435757db1a319f6`）

## 1. Goal objective

在当前 Akzio v2 workspace 上，严格按 R0→R10 重建本地常驻、scheduler-owned、Paper-only 的 Multi-Agent Research System。最终系统必须满足：

- 可执行资产闭集仅为 `TQQQ`、`QQQ`、`SOXX`、`SOXL`；
- Live Trading 不存在可构造路径，非 Alpaca Paper endpoint 在任何 HTTP I/O 前失败；
- Rust 是状态、权限、Contract、预算、Workflow Gate、Store、学习迁移和执行策略的唯一权威；
- `V2Store` 是 CAS、SQLite graph、耐久事件、lease/epoch、permit、policy head 和 broker commitment 的唯一持久化权威；
- Agent 只能通过 `akzio-context` 的 `ContextManifest` 和 `ReadGrant` 获取最小授权资料；
- Planner 只能提出研究图，Rust lowering/compiler 必须注入不可绕过 Gate；
- canonical learning 只能来自 sealed Paper Outcome；Debug、Replay、Shadow、Paper Dry Run 全部 noncanonical；
- Paper commitment 只能由 scheduler 创建，每个 broker session 最多一次；
- 唯一业务控制面为 loopback HTTP + SSE；删除 Unix JSON-line 业务协议；
- R10 完成完整验证和删除清单后，Goal 才可标记 complete。

本文件是当前 active Goal 的执行记录。R0–R6 已按顺序实现并重新认证；只有阶段 exit gate 的当前源码、测试和运行证据才能推进状态，不能仅凭 commit message 或历史描述快进。

## 2. Source of truth 与冲突优先级

1. `AGENTS.md`、当前源码、当前测试与真实命令结果；
2. `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md`；
3. `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN.md`；
4. 用户补充的历史 `PLAN.md`；
5. `AKZIO_V2_REFACTOR_HANDOFF.md` 的历史描述；
6. 外部网站资料仅提供方法论，不覆盖上述本地权威。

补充 `PLAN.md` 当前位于 `/Users/alixeu/Downloads/PLAN.md`，共 749 行，SHA-256 为 `a2e030b9ef51e3de4c12c86a556cee2a6c341a888b6eceb9db6c7a7781931326`。其中 TQQQ-only、Unix 业务协议等旧设计按上述优先级不采用。

外部一手资料及逐项映射见 [2026-08-09-v2-goal-source-research.md](./2026-08-09-v2-goal-source-research.md)。这些链接是根据历史计划点名的主题匹配的官方来源，不是历史文件中可核对的显式 URL。

## 3. 当前事实基线

### 3.0 R6 完成快照（2026-08-10）

- 分支：`codex/akzio-v2-max-refactor`；R6 基线 HEAD：`faf493c8ba40e0c839ab221d8435757db1a319f6`。
- R0–R5 已完成差分回归，不需要返工；R6 的 Domain/Store/Context/Learning/Execution 改动未削弱前置 invariant。
- R6 已完成 sealed Paper Outcome、immutable Experience/CandidatePolicy、typed PolicySubject、shadow pair cursor snapshot、candidate transition、promotion/rollback 与 noncanonical 拒绝链路。
- `V2Store` 已禁止 generic artifact API 写入 Outcome/Experience/Evaluation/CandidatePolicy；专用事务同时验证 canonicality、source closure、evaluation reverse binding 和 policy head。
- Context policy influence 必须重载持久 Manifest/blob，并验证 permit、contract、expiry、grant closure、raw closure、budget 与 Store-recorded evaluation binding。
- 删除旧 `LearningLedger`、`TopologyLedger`、`topology.rs` 与可绕过 cursor/原子事务的 transition surface。
- 当前验证：fmt 通过；workspace check 通过；workspace clippy `-D warnings` 通过；workspace tests 170 passed；fresh `/tmp` Store Root 的 fixture-debug 退出码 0，Doctor 返回 `{"ok":true}`。
- `CONTEXT.md` 仍是用户 untracked 文件，不修改、不提交。

以下 3.1–3.4 是 R6 开始前的恢复快照，仅保留为历史证据；不得覆盖 3.0 的完成状态。

### 3.1 R6 开始前 Checkout（已失效）

- 分支：`codex/akzio-v2-max-refactor`
- HEAD：`faf493c8ba40e0c839ab221d8435757db1a319f6`
- HEAD 已包含：
  - `69e7dc6 refactor: establish v2 foundations through workflow runtime`
  - `faf493c refactor: complete v2 workflow runtime path`
- tracked dirty：
  - `crates/akzio-daemon/src/lib.rs`
  - `crates/akzio-domain/src/{core,decision,evaluation,schema}.rs`
  - `crates/akzio-execution/src/{allocation,execution_gate}.rs`
  - `crates/akzio-learning/src/rebuild.rs`
  - `crates/akzio-store/src/{store_v2,v2}.rs`
- untracked：`CONTEXT.md`；它是当前领域术语补充，不得删除或覆盖。
- 本轮曾因子任务状态混淆执行 `cargo fmt -p akzio-learning`；语义补丁已撤销，但 pre-format dirty 快照不存在，因此保留当前文件，不 reset/checkout/猜测恢复。
- 不得 reset、clean、stash、checkout、覆盖或丢弃任何上述工作。

### 3.2 R6 开始前命令证据（已失效）

- `cargo metadata --offline --format-version 1 --no-deps`：通过；workspace 为 11 个 Rust 2021 crate。
- `cargo fmt --all -- --check`：失败；当前唯一输出是 `crates/akzio-domain/src/schema.rs` 的一处缩进差异。
- `cargo check --workspace --offline`：失败；当前唯一编译错误位于 `crates/akzio-store/src/store_v2.rs`，仍读取已被 Domain v7 删除的 `Outcome.execution_context`，而新模型要求经 `Outcome.schedule` 解析 durable `OutcomeSchedule`。
- `cargo test -p akzio-domain --offline`：31 passed。
- `git diff --check`：通过。
- 因 workspace 尚未编译，本轮未运行也不声称通过：workspace clippy、workspace test、Daemon 测试、`fixture-debug`、`store doctor`。
- 旧计划记录的 scheduler 死锁属于 2026-08-09 历史基线；当前 Store 已重写相应 surface，本轮没有重跑，不能继续表述为当前已复现或已修复。
- CodeGraph index 可读，但仍包含已删除的 `crates/orchestrator-*` 符号；本轮结构判断以当前 filesystem、Cargo、调用者和测试为准。

### 3.3 R6 开始前架构状态（已失效）

- R0 文档已提交：invariants、test matrix、deletion graph 和本 Goal 计划均存在；README/config 仍明确标注过渡 surface。
- R1 Domain v2 facade、typed artifact/contract/workflow/decision/execution/event/policy 已存在；当前 dirty 正将 schema 推进到 v7，新增 Replay noncanonical、OutcomeSchedule、typed PolicySubject 和 Decision policy influences，但尚未完成跨 crate 传播。
- R2 `V2Store` 已有 CAS、atomic workflow/attempt、lease/epoch、session slot、commitment、Doctor 和 policy evaluation cursor；当前只剩一处已知 Domain v7 编译错误，但 legacy Store 仍保留供旧消费者使用。
- R3 Context Broker、ReadGrant、Evidence Runtime 已存在，并在 active Daemon 的 Agent/Evidence task path 使用 v2 Store；旧 learning/execution 仍引用 legacy Context/DocumentRecord。
- R4 Contract Catalogue 与 AgentRuntime 已存在，active Daemon 已用它执行 planner/analyst 与 fixture evidence；legacy fixed-role research 仍在 hidden module 和旧工具 surface 中。
- R5 WorkflowRuntime/TaskRuntime 已成为 Daemon 的 active research path，动态 proposal 和 mandatory terminal recipes 已有测试；但 Daemon 对 DecisionGate、ExecutionGate、PaperCommit、Reconcile、Evaluate 仍返回 `UnsupportedTaskClass`。
- R6 正在进行：Domain/Store/Learning 的 OutcomeSchedule、sealed outcome materialization、typed policy subject、shadow pair cursor/no-op evaluation consumption 正处于 dirty 且未编译状态；旧 learning ledger/topology 仍公开存在。
- R7 owner 模块已实现 typed gates、allocation、strict Alpaca Paper origin、fenced commitment、dispatch/reprice/reconciliation 及 fixture tests；但 active Daemon 未调度这些 task class，旧 `ExecutionRuntime`/DocumentRecord surface 仍被导出。
- R8 只有部分实现：worker pool、loopback HTTP auth、SSE cursor 和 v2 Agent/Evidence dispatch 已存在；真正 scheduler loop、market-clock slot creation、freeze API、crash recovery 和 stale-leader broker fencing 尚未接通，Unix JSON transport 仍在运行。
- R9 未完成：CLI 仍使用 `UnixStream`/`DaemonCommand`，serve 同时启动 HTTP 与 Unix，config 仍含 `unix_socket`。
- R10 未完成：legacy modules、旧 domain vocabulary、旧 learning/execution surface 和兼容性文字仍有大量静态命中；没有完整进程级 crash/recovery、multi-run、canonicality、Paper Dry Run 和最终 deletion harness。

### 3.4 R6 开始前阶段判断（已失效）

| 阶段 | 当前证据状态 | 不能宣告完成的原因 |
| --- | --- | --- |
| R0 | 文档与规则已落地，需重新认证 | 当前 checkout/命令证据已变化，workspace 不绿 |
| R1 | 核心实现已落地，v7 传播中 | Domain v7 尚未完成下游传播 |
| R2 | 核心实现已落地，当前被一处编译错误阻塞 | Store 对 OutcomeSchedule lineage 尚未完全适配 |
| R3 | v2 模块已实现并用于 research path | legacy Context 仍被旧 learning/execution 消费，窄测未重跑 |
| R4 | v2 Catalogue/AgentRuntime 已用于 Daemon | legacy role/tool surface 未删除，窄测未重跑 |
| R5 | dynamic DAG/TaskRuntime 已部分 active | terminal gate task class 未接通 Daemon |
| R6 | in progress / unverified | dirty 跨 Domain/Store/Learning，workspace 不编译 |
| R7 | owner modules present / integration pending | Daemon 不 dispatch，旧 runtime 仍公开 |
| R8 | partial | scheduler/freeze/recovery 未完成，Unix 仍存在 |
| R9 | not started | CLI/config 仍是 Unix 业务协议 |
| R10 | not started | legacy/dead code 与完整 harness 尚未清理 |

### 3.5 2026-08-09 历史 Checkout（已失效）

- 分支：`master`
- HEAD：`24e512e2f0c09b54bebfa04480f95cd27c0675b3`
- 用户既有 untracked 文件：
  - `docs/architecture/AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md`
  - `docs/architecture/AKZIO_V2_REFACTOR_HANDOFF.md`
- 本次规划新增：
  - `docs/architecture/2026-08-09-v2-goal-source-research.md`
  - 本文件
- 不得 reset、clean、stash、checkout、覆盖或丢弃上述文件。

### 3.6 2026-08-09 历史命令基线（已失效）

- `cargo check --workspace --offline`：通过，有 warning；
- `cargo clippy --workspace --all-targets --offline`：退出码 0，但有 14 个唯一 warning；
- `cargo fmt --all -- --check`：失败，差异只在五个 `rebuild.rs` 原型；
- 78 个测试已确认通过；
- 2 个 scheduler 测试确定性死锁：
  - Store：`paper_schedule_slot_is_singleton_fenced_and_doctor_checked`；
  - Daemon：`paper_schedule_recovers_a_reserved_slot_after_leader_takeover`；
- 根因：`reserve_paper_schedule_slot` 提交事务后仍持有 connection mutex guard，随后调用 `paper_schedule_slot` 再次获取同一非重入锁；
- 临时 Store Root 中 `run fixture-debug` 与 `store doctor` 已通过；这只是 fixture/local 证据，不是真实模型、真实市场或真实 Paper 订单。

### 3.7 2026-08-09 历史架构差距（已失效）

- 五个 `rebuild.rs` 仅被公开 re-export，active Daemon/CLI/Execution 没有消费其新 runtime；
- active path 仍存在固定 `AgentRole`、`PlannedResearchRole`、Phase-like `TaskKind` 和旧 `WorkflowCompiler`；
- workflow install、attempt completion、artifact 和 event 仍存在 split transaction crash window；
- `register_document` 不验证 attempt/lease/epoch/contract permit；
- active Context 仍会从 run-wide 文档集合选取，ToolRuntime 可绕过 manifest closure；
- `DecisionDraft.blockers` 仍是自由字符串；
- `PaperDryRun` 仍参与 topology 初始化，且执行输入路径会调用真实 ingest；
- Alpaca Paper endpoint 仍用字符串 `contains` 判断；
- execution commitment 不携带 daemon lease/epoch；
- Daemon 同时保留 HTTP/SSE 与 Unix JSON-line；CLI 仍使用 `UnixStream`；
- 没有完整进程级 crash/recovery、multi-run、atomic failure-injection 和 canonicality E2E harness。

## 4. 从官方网站吸收的原则

| 主题 | 采用原则 | Akzio 落点 | 不照搬 |
| --- | --- | --- | --- |
| [Harness Engineering](https://openai.com/index/harness-engineering/) | repo-local system record、rules-as-code、短验证回路 | R0 invariants/删除图/测试矩阵；R10 fixtures/Doctor/replay | agent 自动合并、吞吐量优先、宽开发权限 |
| [Context Engineering](https://openai.com/index/inside-our-in-house-data-agent/) | 最小、分层、继承权限的 context；少而清晰的工具 | R3 Manifest/Grant/source closure/repair | 任意企业数据源、更多 context 即更好、模型直写 SQL |
| [Codex Agent Loop](https://openai.com/index/unrolling-the-codex-agent-loop/) 与 [Agents Runtime](https://openai.com/index/the-next-evolution-of-the-agents-sdk/) | 模型提出 tool request，harness 验证执行；transport 与 durable state 分离 | R4 AgentRuntime；R5 TaskRuntime/WorkflowRuntime | provider run object、response id、workspace 代替 `V2Store` |
| [Self-improving Agents](https://openai.com/index/building-self-improving-tax-agents-with-codex/) | trace→targeted eval→regression→candidate→canary→rollback | R6 sealed Outcome、paired Shadow、policy transition | 模型运行时自改 Rust、单次反馈直接晋升 |
| [Symphony](https://openai.com/index/open-source-codex-orchestration-symphony/) 与 [SPEC](https://github.com/openai/symphony/blob/main/SPEC.md) | 常驻 supervisor、稳定 identity、bounded concurrency、reconciliation、stall recovery | R5 durable task lifecycle；R8 daemon/scheduler | Linear/GitHub/worktree/PR 成为业务权威 |
| [Computer Environment](https://openai.com/index/equip-responses-api-computer-environment/) | 模型提出、受控环境执行；权限、网络和凭据由 harness 管理 | R3 typed evidence adapters；R7 Rust-owned broker boundary | 通用 shell、Python、filesystem、SQLite CLI、自由网络暴露给 runtime Agent |
| [Streaming Workflows](https://openai.com/index/responses-api-websocket/) | 流式 transport 可降延迟，但连接和 provider cache 不是 durable state | R8 SSE cursor；R10 model transport observability | WebSocket 代替 event log、replay、HTTP control 或 task lease |

这些原则必须在 R0 转换成一个本地 invariant、ADR 或可运行测试后才进入实现。网站文章本身不能作为 Akzio 验收证据。

## 5. 三层权威模型

| 层 | 允许拥有 | 禁止拥有 |
| --- | --- | --- |
| Model proposal | EvidenceNeed、研究提案、Claim、Critique、Decision Draft、候选改进建议 | durable state、权限、任务完成、canonical learning、broker commitment |
| Rust harness/runtime | schema/contract、预算、tool dispatch、workflow/task gates、execution policy、learning transition | 绕开 Store 的 durable state、未记录副作用 |
| `V2Store` | CAS、graph、events、leases/epochs、frozen plans、permits、policy heads、commitments、outcomes | 模型推理、网络获取、broker I/O |

所有副作用都必须满足：**typed capability + current permit + single durable transaction + event**。

## 6. Goal 执行协议

### 6.1 阶段顺序

`R0 → R1 → R2 → R3 → R4 → R5 → R6 → R7 → R8 → R9 → R10`

- 阶段严格串行；前一阶段 exit gate 未通过时不得进入下一阶段；
- R0–R2 不并行；R2 public interface 稳定后，只允许按 owner crate 分工，公共 schema 仍只能由 owner 修改；
- 不为保持旧调用者而创建长期 compatibility adapter；replacement 通过后在同阶段删除旧实现；
- 不因网站文章而提前实现 Agents SDK、WebSocket、复杂 topology 或性能优化。

当前 checkout 的恢复规则：用户明确“开始执行 R0”后，先用 R0 重新认证现有实现，不盲目重写已存在模块，也不因 commit message 快进阶段。某阶段只有在当前 tree 上实际满足 entry/exit gate 才可标记 complete；当前未完成的 R6 dirty 必须原样保留，并在 R1/R2 owner interface 重新稳定后由 R6 收口。

### 6.2 每阶段固定循环

1. 记录 branch、HEAD、dirty/untracked 和残留进程；
2. 复核本阶段 entry gate 与前阶段报告；
3. 从当前源码/调用者/Store consumers/测试确定真实修改面；
4. 先写 characterization 或失败测试，再修改 owner interface；
5. 修改直接消费者，删除同阶段被替代实现；
6. 运行 touched crate 的 fmt/check/clippy/窄测试，全部离线；
7. 检查 durable artifact/event/permit/crash behavior，而不只检查编译；
8. 输出阶段报告；只有 exit gate 全部满足才进入下一阶段。

### 6.3 固定阶段报告

```text
阶段：R?
状态：completed / blocked / not started
Checkout：branch / HEAD / dirty-untracked
Entry gate：逐项结果
实现：
删除：
架构纠偏及源码证据：
验证：
  - command
  - exit code / timeout
  - passed / failed / ignored
  - static / fixture / mock / Dry Run / Paper 分类
未解决风险：
下一阶段依赖：
是否允许进入下一阶段：yes/no
```

## 7. R0–R10 计划

### R0 — 冻结 invariants、删除图与可信基线

**Entry**

- 五份本地架构输入、外部原则研究和当前 checkout 已确认；
- 当前 checkout 已有 R0–R8 不同深度实现和跨 R1/R2/R6 dirty work；本阶段先认证，不假设任何阶段完成。

**Objective**

刷新目标边界、crate owner、删除时机、当前 dirty ownership 和验收矩阵；建立能反映 2026-08-10 tree 的可信离线基线。

**Deliverables**

- `docs/architecture/AKZIO_V2_INVARIANTS.md`；
- `docs/architecture/AKZIO_V2_TEST_MATRIX.md`；
- `docs/architecture/AKZIO_V2_DELETION_GRAPH.md`；
- capability/permit/transaction/event 对照表；
- canonical/noncanonical purpose 对照表；
- 当前接口与原型的保留/吸收/删除清单；
- 将当前 dirty 按 R1/R2/R6 owner 分类，明确哪些是已实现、未传播、未验证或应删除；
- 修正当前 `schema.rs` 格式差异，并仅做恢复 workspace 编译所需的 owner-owned propagation；不得在 R0 引入新行为或扩大权限；
- 将 2026-08-09 scheduler 死锁记录标为历史，只有当前测试再次复现时才进入缺陷清单。

**Deletions**

- 删除文档中声称 Phase、FileStore、旧 Store Root 或 Unix 业务协议仍受支持的现行表述；
- 暂不删除仍被 active path 调用的生产代码，删除图必须为每个目标指定唯一 replacement phase。

**Tests**

- `cargo metadata --offline --format-version 1 --no-deps`；
- `cargo fmt --all -- --check`；
- `cargo check --workspace --offline`；
- 当前 Store session-slot/fencing 窄测必须终止并通过；
- 临时 Store Root 下的 fixture-debug 和 Doctor；
- 静态 inventory：`orchestrator`、Phase、FileStore、旧 outputs、Unix、Live、直接 Paper submit。

**Exit gate**

- 当前离线 metadata/fmt/check 基线实际通过，历史死锁不再作为未经复现的当前事实；
- 每条 invariant 都有 owner、目标阶段和测试；
- 每个旧路径都有唯一删除阶段；
- 没有未决 compatibility 决策。

### R1 — 重建 `akzio-domain`

**Objective**

用稳定类型和验证规则使非法权限、provenance、canonicality、资产和执行输入无法静默进入系统。

**Current-state focus（2026-08-10）**

- 以现有 v2 Domain/schema v7 为基础收口，不盲目重写已经通过的 ID、Artifact、Contract、Workflow、Decision、Execution、Event 与 policy 类型；
- 先完成 `OutcomeSchedule`、typed policy subject 等新 schema 向 Store/Learning/Execution 的单向传播，并保持 `akzio-domain` 无 I/O；
- 当前 `akzio-domain` 31 个离线测试已通过，但格式检查仍有一处差异；R1 完成标准必须包含下游编译边界，不把“Domain 自测通过”误写成 workspace 已迁移完成。

**Deliverables**

- ID 与 content-bound `ArtifactId`；
- Artifact、Contract、Recipe、WorkflowProposal/CompiledGraph；
- Run/Task/Attempt/lease/permit 类型；
- Raw/Normalized/Detail、Claim/Critique、DecisionContext；
- typed `HardBlocker`、`SoftWarning`、material conflict、factor/pair exposure；
- Evaluation、Outcome、Experience、PolicyTransition；
- ExecutionContext、NoOrder、PaperCommitment、Reconciliation；
- durable Event envelope 与 canonicality vocabulary；
- canonical JSON/hash，hash 不信任 payload 自声明值；
- 四资产闭集及严格验证。

**Deletions**

- `AgentRole`、`PlannedResearchRole` 及其所有下游 import；
- `DocumentRecord`/Artifact 双 authority；
- free-form blocker；
- string task/permission authority、UUID-first artifact identity、自信任 contract hash；
- 被新 domain 类型替换的 `rebuild.rs` 重复定义。

**Tests**

- deterministic canonical JSON/hash golden；
- hash mismatch、serde round-trip；
- unsupported asset；
- invalid lifecycle/provenance/source closure；
- malformed Contract/Proposal/Graph/budget；
- 模型 payload 无法创建 grant、permit、endpoint 或执行权限。

**Exit gate**

- 所有下游 crate 使用新 domain schema 编译；
- `akzio-domain` 不依赖 SQLite、网络、模型或其他 workspace crate；
- model-originated data 不能扩大 source/tool/execution authority。

### R2 — 重建 `V2Store`

**Objective**

使所有 durable state content-addressed、transactional、permit-bound、crash-safe、daemon-fenced。

**Current-state focus（2026-08-10）**

- 保留并认证现有 CAS、SQLite graph、ordered events、attempt/session lease 与 epoch-fenced slot/commitment 实现，只有测试证明不满足 invariant 时才重写；
- 第一阻塞是 `store_v2.rs` 仍读取已删除的 `Outcome.execution_context`；先迁移到 `Outcome.schedule`/`OutcomeSchedule`，再验证 source closure 与 sealed-outcome 原子提交；
- legacy Store 只能作为待删除对象，不得为旧调用者增加 reader、migrator 或双写兼容层。

**Deliverables**

- 新 schema version 与 fresh Store Root；旧 `outputs/v2-store` fail-closed；
- CAS 原子写、SQLite graph、append-only ordered events；
- WAL、busy timeout、多连接并发策略；
- `commit_workflow`：run、frozen plan、tasks、dependencies、event 单事务；
- `claim_task` 返回 `ClaimedAttempt + TaskWritePermit`；
- `commit_attempt`：artifacts、task/attempt transition、event 单事务；
- daemon lease/epoch-fenced session slot；
- daemon lease/epoch-fenced execution commitment；
- policy head/transition transaction；
- frozen plan/task IDs 的 scheduler recovery；
- Doctor 覆盖 CAS、closure、DAG、event、attempt、lease、slot、commitment、policy head、canonicality。

**Deletions**

- 裸 `register_document`；
- split create run/plan/task/dependency/event API；
- Store 外直接 SQLite；
- 旧 reader/migrator/compatibility；
- 临时 `RebuildStore` 和 reentrant mutex slot 路径。

**Tests**

- 每个旧 split-write 边界的 crash injection；
- stale worker permit；
- stale daemon leader slot/commitment；
- CAS dedupe/并发同 Blob/损坏；
- artifact closure；
- event ordering/cursor/resume；
- session singleton 与 frozen plan recovery；
- 多 Run 并发与单 Run lease；
- Doctor corruption fixtures。

**Exit gate**

- partial workflow、stale attempt artifact、stale leader commitment 均不可观察；
- Store tests 全部终止并通过；
- Doctor 对正常 fixture 全绿、对损坏 fixture fail-closed。

### R3 — 重建 Context Broker 与 Evidence Runtime

**Objective**

将 evidence acquisition、normalization、selection 和 Agent read 收口为受治理数据平面。

**Current-state focus（2026-08-10）**

- active Agent/Evidence task path 已使用 Context Broker、Manifest、Grant 与 v2 Store；优先做权限、closure、repair、provenance 的重新认证；
- 将仍依赖 `DocumentRecord`、run-wide documents 或 legacy context 的 Learning/Execution 消费者迁移到受 grant 约束的 artifact refs；
- Goal 内所有 adapter 验证只使用本地 fixture，不访问本节列出的任何网站或真实数据 endpoint。

**Deliverables**

- `RawEvidence -> NormalizedEvidence -> SemanticDetail`；
- typed `AcquisitionRequest`；
- allowlisted adapters：Alpaca、SEC EDGAR、FRED、显式 News/Web；
- 完整 fixture adapters，Goal 实施不调用真实网络；
- `ContextManifest`；
- `ReadGrant` 绑定 run/task/attempt/contract/permit；
- kind、source family、byte/token、expiry、read-count、closure 限制；
- manifest/grant mint/use/reject/repair durable events；
- freshness、sealing、provenance 和 credential-redaction validators。

**Deletions**

- run-wide `documents_for_run` 隐式扩张；
- Document-ID-only raw/detail reread；
- 无 manifest 的 ToolRuntime 路径；
- daemon-specific ingest policy；
- Agent filesystem/HTTP/raw-byte access。

**Tests**

- raw dedupe、normalized/detail source closure；
- manifest-only read；
- kind/source/expiry/byte/token 越权拒绝；
- stale source；
- repair 生成新的 immutable artifact/event；
- fixture adapter deterministic replay。

**Exit gate**

- 模型代码没有网络和文件系统能力；
- Agent 无法读取 active manifest/grant 外的 artifact；
- 所有 evidence refs 可追溯到 sealed raw source。

### R4 — 重建 Contract Catalogue 与 AgentRuntime

**Objective**

让 prompt、schema、tools、budget、retry、termination 和 output lifecycle 只来自已安装的 canonical Contract。

**Current-state focus（2026-08-10）**

- Catalogue 与 AgentRuntime 已进入 planner/analyst active path；先认证 contract hash、版本安装、grant/budget、retry/termination 和 durable turn events；
- 迁移并删除剩余 fixed-role registry、legacy prompt/tool surface 与仅 re-export 的原型；不得为旧 `AgentRole` 保留转换层；
- 本阶段不引入 Agents SDK 或新的 provider framework；model transport 继续是可替换 adapter，不能拥有 Akzio 领域状态。

**Deliverables**

- versioned Contract Catalogue；
- Active/Candidate contract versions 与 immutable install event；
- capability-bounded Recipe Catalogue；
- 初始 Contract purposes：planner、analyst、critic、synthesizer；
- multi-turn AgentRuntime；
- structured output validation、retry、termination、wall-time/tool/token budget；
- turn/tool/result durable trace；
- EvidenceNeed 和候选 Contract proposal；
- `akzio-model` 只拥有 provider transport/protocol/fixture。

**Deletions**

- role registry/default topology maps；
- prompt/schema/tool permission duplication；
- direct tool dispatch；
- synchronous `rebuild.rs` AgentRuntime 原型。

**Tests**

- contract/hash mismatch；
- invalid output retry/failure；
- allowed source/tool/grant expansion rejection；
- budget/termination/wall-time；
- fixture multi-turn research；
- 仅凭 contract hash、manifest/grant、turn events 和 output artifacts 可审计重放。

**Exit gate**

- Runtime 不知道固定角色、Phase 或业务执行权限；
- provider SDK 不能决定 task completion、retry authority、learning promotion 或 execution。

### R5 — 重建 Planner、WorkflowRuntime 与 TaskRuntime

**Objective**

允许 bounded adaptive research DAG，同时保证 Rust Gate 和 terminal paths 永不可被 Planner 绕开。

**Current-state focus（2026-08-10）**

- 现有 WorkflowRuntime/TaskRuntime 已承担 Agent/Evidence task；在此基础上补全 lowering、graph patch、attempt recovery 与 deterministic replay；
- Daemon 当前对 DecisionGate、ExecutionGate、PaperCommit、Reconcile、Evaluate 返回 `UnsupportedTaskClass`。R5 只建立不可绕过的 typed terminal task/owner-dispatch 边界，不把 R6/R7 policy 塞进 runtime 或 daemon；
- frozen scheduler plan、稳定 task IDs 和 mandatory terminal recipes 必须在 Store 中可恢复，Planner patch 不得改变其权限与执行语义。

**Deliverables**

- proposal lowering 和 Recipe resolver；
- bounded DAG compiler 与 graph patch transaction；
- TaskRuntime queue/attempt/lease/heartbeat/retry/cancel；
- deterministic recovery reducer/replay；
- mandatory Evidence/Decision/Execution/Reconcile/Audit gates；
- fanout/depth/node/token/tool/time budgets；
- scheduler slot frozen plan hash 和稳定 task IDs。

**Deletions**

- 当前固定 `WorkflowCompiler`；
- Phase-like `TaskKind` authority；
- 固定 Plan/Investigate/Challenge/Synthesize lifecycle；
- special-case `PlanPatch` 和 lifecycle successor helper；
- partial-submit recovery inference。

**Tests**

- parallel DAG、cycle/fanout/depth/budget；
- capability expansion rejection；
- Planner 删除 terminal gate 拒绝；
- graph patch/process-death recovery；
- retry/cancel/expired attempt；
- deterministic replay。

**Exit gate**

- Planner 可改变研究拓扑，但不能增加权限、删除 Gate 或改变 frozen Paper plan；
- recovery 完全来自 `V2Store` truth。

### R6 — 重建 Evaluation、Experience、Shadow 与学习状态机

**Objective**

只从 sealed canonical Paper Outcome 产生有界、可追溯、可回滚的 Memory/Contract/Topology transition。

**Current-state focus（2026-08-10）**

- Domain/Store/Context/Learning/Execution 的 R6 schema 传播与权限收口已完成，当前 workspace 编译、lint、全测和 fresh-root fixture/Doctor 均通过；
- 用 Store 事件与 immutable documents 证明每次 candidate transition、pair completion、promotion、rollback 和 retirement；不维护第二套 ledger；
- 为 Debug、Replay、Shadow、PaperDryRun 建立负向写入断言，任何 noncanonical 输入都不得创建 canonical Experience 或 policy/topology head。

**Deliverables**

- stable Experience identity；
- sealed outcome materializer；
- T+1/T+3/T+5 utility、benchmark、calibration、evidence completeness、risk recall、cost/latency、ablation；
- Shadow pair 绑定 parent Decision、parent ExecutionContext、candidate Decision、contract/topology 和同一 outcome horizon；
- Memory lifecycle：`Candidate -> Active -> Proven -> Contested -> Retired`；
- Topology/Contract canary：`Candidate -> Canary10 -> Canary25 -> Canary50 -> Active`；
- 每个 canary level 必须使用 fresh paired outcomes；
- durable policy head、transition artifact 和 influence trace；
- freeze/rollback 为第一等状态。

**Deletions**

- summary-per-decision Memory；
- timestamp-only Shadow identity；
- noncanonical promotion path；
- `PaperDryRun` topology 初始化；
- 无 sealed-outcome gate 的 materializer/policy transition；
- 历史计划中的固定 `±2pp` overlay 常量。

**Tests**

- Debug/Replay/Shadow/PaperDryRun promotion rejection；
- unsealed Outcome rejection；
- same-timestamp pair idempotency；
- delayed horizon materialization；
- fresh-pair canary promotion；
- risk recall/evidence completeness rollback；
- ablation attribution 和 influence reconstruction。

**Exit gate**

- 没有 sealed canonical paired evidence 和 durable transition 就不能改变 active policy；
- noncanonical run 对 Memory、Topology、Contract policy head 的写入为零。

**阶段报告（2026-08-10）**

- 状态：completed。
- Checkout：`codex/akzio-v2-max-refactor`，基线 HEAD `faf493c8ba40e0c839ab221d8435757db1a319f6`；用户 untracked `CONTEXT.md` 未触碰。
- 实现：`OutcomeSchedule` 与交易日 horizon；sealed Paper-only Outcome materialization；typed `PolicySubject`；immutable Experience/CandidatePolicy；shadow pair snapshot cutoff；Candidate/Canary/Active/Proven/Contested/Retired 转移；Context policy influence 的 permit/contract/manifest/grant/closure 校验。
- Store 原子性：generic API 拒绝 governed learning artifacts；`commit_outcomes` 和 `record_policy_evaluation` 负责 source closure、canonicality、cursor、reverse binding、transition/head/event 单事务提交。
- 删除：旧 `LearningLedger`、`TopologyLedger`、`topology.rs`、无 cursor 约束的 transition 写入口及重复 ledger authority。
- 架构纠偏：Shadow 只能作为 paired evaluation 比较材料；Debug、Replay、PaperDryRun 永不写 canonical learning；Experience/CandidatePolicy 只有 Store-recorded exact evaluation binding 才可影响 Decision。
- 验证：`cargo fmt --all`；`cargo check --workspace --offline`；`cargo clippy --workspace --all-targets --offline -- -D warnings`；`cargo test --workspace --offline`（170 passed）；fresh Store Root 下 `run fixture-debug` 退出码 0，`store doctor` 返回 `{"ok":true}`；`git diff --check` 通过。
- 证据分类：全部为本地 static/test/fixture 证据；未访问真实市场、真实 Alpaca、真实订单或外部 API。
- 未解决风险：R7 尚需完成 Decision/ExecutionRuntime owner surface、plan hash 重算、sealed input 派生 policy、durable NoOrder 和 reconciliation Doctor；R8 尚未接通 terminal dispatch。
- 下一阶段依赖：R7 只消费本阶段已密封的 Outcome/policy influence 与 V2Store permit/event surface。
- 是否允许进入下一阶段：yes。

### R7 — 重建 Decision/ExecutionRuntime 与 Alpaca Paper 边界

**Objective**

实现 scheduler-owned、自动但 fail-closed、幂等、可 reconciliation 的四 ETF Paper execution。

**Current-state focus（2026-08-10）**

- 现有 owner 模块已包含 typed gates、allocation、strict Paper origin、fenced commitment、dispatch/reprice/reconciliation 与 fixture tests；先按 R1/R2/R6 类型重新编译和认证；
- 将 legacy `ExecutionRuntime`/`DocumentRecord` 表面迁移到 `DecisionContext`、`ExecutionContext`、permit 和 durable lineage，删除 target-only/free-form authority；
- 只用 fake broker 与本地 ingest 测试 fail-closed、幂等和 reconciliation；本 Goal 不构造真实 Alpaca 请求，也不把 fixture 结果表述成真实 Paper 下单。

**Deliverables**

- DecisionGate/ExecutionGate；
- typed `AcceptedDecisionContext` 或 audited `NoOrder`；
- freshness、material conflict、account、quotes、allocation、turnover、gross/net、factor/pair exposure、plan hash、idempotency gates；
- strict URL parser：只允许精确 Paper scheme/host，任何 HTTP 前验证；
- durable freeze；只能通过 loopback operator HTTP API 或 CLI unfreeze；
- session intent、commitment、stable client order IDs；
- submission/reprice/partial-fill/cancel/reconciliation lineage；
- PaperDryRun 完全使用本地 fake ingest/fake broker，不调用 Alpaca。

历史 `PLAN.md` 中的 20% exposure、$25k notional、固定 spread/price 常量不自动继承；只有当前 Rust policy/config 经 R0/R1 明确确认后才可采用。

**Deletions**

- target-only planner input；
- free-form blocker interpretation；
- URL `contains`；
- manual per-order confirmation；
- direct CLI/API Paper submit/retry；
- 无 daemon permit 的 execution commitment；
- Dry Run 的 broker/network/learning/topology side effect。

**Tests**

- 每种 blocker 产生 typed NoOrder；
- stale account/quote、market closed；
- conflict/factor/pair/turnover；
- freeze/unfreeze；
- non-Paper endpoint before I/O；
- stale daemon lease/epoch；
- duplicate/restart/reprice；
- fake broker partial fill/reconciliation；
- one broker-session commitment。

**Exit gate**

- 任何 broker-visible action 前存在 durable Accepted verdict、有效 daemon permit、唯一 slot 和 plan hash；
- Live、非 Paper endpoint、直接 submit 路径不可构造。

### R8 — 重建 Daemon、scheduler、worker 与 HTTP/SSE

**Objective**

通过单一 loopback control protocol 运行 durable workload、leadership fencing、scheduler 和 crash recovery。

**Current-state focus（2026-08-10）**

- 保留并认证现有 worker pool、loopback HTTP auth、SSE cursor 与 Agent/Evidence dispatch，补齐 scheduler loop、market-clock slot、freeze/unfreeze 和 process crash recovery；
- 在 R6/R7 owner 接口稳定后接通 Decision/Execution/Reconcile/Evaluate task class；Daemon 只负责 leadership、scheduling、transport 和 dispatch，不接管 policy；
- 以双 Daemon、本地 fake clock/fake broker、stale leader 进程级 harness 证明 fencing；随后删除 Unix JSON-line reachable path。

**Deliverables**

- supervisor/leader lease/epoch；
- Alpaca Paper market-clock scheduler adapter；
- 每 broker session date 唯一 durable `SessionSlot`；
- bounded worker pools 和 reconciliation loop；
- loopback HTTP auth/control；
- SSE durable cursor/resume；
- freeze/unfreeze、cancel/retry、replay/Doctor endpoints；
- thin dispatch，只路由到 owner runtime；
- CLI 在本阶段切换到 HTTP client，保证删除 Unix 后 workspace 可运行。

**Deletions**

- `serve_unix`；
- Unix JSON `DaemonCommand` business protocol；
- socket path 与清理逻辑；
- Daemon 内 research/learning/execution policy；
- 重复 command reducer。

**Tests**

- HTTP auth、loopback-only bind；
- SSE resume；
- cancel/retry；
- two-daemon epoch fencing；
- process crash/recovery；
- multi-run concurrency；
- scheduler slot uniqueness/recovery；
- stale leader broker commitment rejection；
- freeze persistence。

**Exit gate**

- CLI/未来 UI 使用同一 HTTP/SSE API；
- Unix 业务协议没有 reachable path；
- 每个 runtime transition 可由 Store events/artifacts 重建。

### R9 — 重写 CLI/config 和操作表面

**Objective**

删除旧 CLI/config 表面，只暴露安全、HTTP-backed、scheduler-owned v2 操作。

**Current-state focus（2026-08-10）**

- CLI 当前仍使用 `UnixStream`/`DaemonCommand`，serve 仍同时启动 HTTP 与 Unix，config 仍有 `unix_socket`；本阶段将其整体切到 loopback HTTP/SSE；
- 删除 direct Paper submit/retry、Live、Phase、legacy prompt/output 和旧 Store Root 配置，不提供兼容 flag 或隐式 fallback；
- CLI 只能调用 operator API；不得直接写 Store、创建 Paper commitment 或绕过 scheduler/freeze。

**Deliverables**

- config、HTTP client、commands、diagnostics；
- loopback bind/token、workers、scheduler、fresh Store Root；
- four-asset policy；
- contract catalogue/candidate bounds；
- governed evidence adapters/model provider；
- secrets environment-only；
- README/config/help 全面更新；
- 明确的旧 Store Root incompatibility error。

**Deletions**

- Unix socket config/client；
- Phase/legacy prompt/output flags；
- direct Paper run/retry；
- Live flag/endpoint/credential 类型；
- 未显式配置的历史 provider 默认源；
- 旧 outputs 默认值和 dead re-export/dependency。

**Tests**

- non-loopback bind rejection；
- exact four-symbol universe；
- old Store Root rejection；
- missing token/secret handling；
- CLI help static inventory；
- HTTP fixture command contract；
- scheduler-owned Paper enforcement。

**Exit gate**

- `akzio --help`、README、config 和 reachable code 中没有 Phase、Unix、Live 或 direct-Paper 表面。

### R10 — Observability、replay、E2E 与最终破坏性清理

**Objective**

从 fresh Store Root 证明完整 rebuilt system，再删除全部 displaced implementation 和 dead code。

**Current-state focus（2026-08-10）**

- 当前尚无覆盖全部强制场景的进程级 harness，且 legacy modules、五个 `rebuild.rs`、旧 vocabulary 与兼容文字仍存在；R10 负责最终吸收和删除，不提前删除仍被 active consumer 使用的实现；
- 全套验证必须在离线、fresh 临时 Store Root 下实际终止；Daemon 卡住按失败或阻塞记录，不能用局部测试替代；
- 只有 legacy inventory、完整验证矩阵和 fixture/debug/dry-run 证据分类全部满足，才允许将整个 Goal 标记 complete。

**Deliverables**

- unified durable event coverage；
- deterministic replay/report；
- process-level failure injection；
- cross-crate fixture builders；
- Doctor corruption fixtures；
- isolated Debug 和 PaperDryRun harness；
- model transport diagnostics；provider WebSocket 仅可作为可选性能层，不进入 correctness path；
- final architecture、operations、recovery 和 security docs。

**Deletions**

- 五个临时 `rebuild.rs`；
- 被替换的 monolithic active implementation；
- v1/Phase/FileStore/Unix 代码和过时文档；
- dead adapters、features、dependencies、legacy output assumptions。

**Mandatory harness/tests**

- Daemon process crash/recovery；
- 多 Run 并发与单 Run lease；
- epoch fencing，包括 broker commitment；
- Evidence provenance/source closure；
- Context grant 越权；
- Memory 自动晋升/降级；
- Shadow pair 幂等；
- 自动 PaperDryRun 全流程；
- Dry Run 不污染 canonical Memory/Topology/Contract；
- exact Paper endpoint fail-closed；
- scheduler 单 broker-session commitment；
- artifact + task + event 原子故障注入。

**Final commands**

全部在离线模式、fresh 临时 Store Root 下实际完成：

```bash
cargo fmt --all -- --check
cargo check --workspace --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo run --offline -p akzio-cli -- run fixture-debug
cargo run --offline -p akzio-cli -- store doctor
```

另外运行本计划定义的 crash/concurrency/evidence/grant/learning/PaperDryRun harness。`cargo deny` 只有在已安装且 workspace policy 明确采用时才运行，不得联网安装。

**Exit gate**

- 所有命令实际终止并通过；
- legacy inventory 为零，或只出现在明确标记的历史/删除说明中；
- fixture、mock、PaperDryRun、实际 Paper 证据分类清楚；
- 本 Goal 不包含真实 broker/model/network 验证，不得宣称真实 Paper 或生产验证；
- 没有 commit、push、PR、deploy、外部数据库迁移或 Live action；
- 此时 Goal 才可标记 complete。

## 8. 历史 T0–T15 映射与纠偏

| 历史任务 | 当前阶段 |
| --- | --- |
| T0 | R0 |
| T1 | R1 |
| T2 + T3 | R2 |
| T4 | R3 |
| T5 + T7 | R4 |
| T6 | R5 |
| T9 + T10 | R6 |
| T8 + T11 | R7 |
| T12 | R8 |
| T13 | R9 |
| T14 + T15 | R10 |

继续继承：canonical JSON/SHA-256/stable IDs、CAS 原子写、SQLite durable events、provenance、dynamic Planner、Context Broker、contract-driven turns、fail-closed execution、stable order ID、reconciliation、Doctor/replay、noncanonical learning 隔离、Shadow/canary/rollback、deep modules 和单向依赖。

必须纠正：

- `TQQQ` 唯一可执行资产 → 四 ETF 可执行闭集；
- QQQ/SOXX 仅研究资产 → 失效；
- Unix Socket 与 HTTP 并存 → HTTP/SSE 单一业务协议；
- `outputs/v2-store` 继续复用 → 新 schema/fresh root，旧 root fail-closed；
- v1 行为迁移/兼容 → 只记录删除语义，不恢复兼容层；
- fixed role/agent kind → ContractPurpose + Recipe；
- summary Memory/固定 `±2pp` overlay → immutable Experience + Rust policy transition；
- 历史 hard-coded risk 常量 → 只有当前 Rust policy/config 明确确认后采用；
- direct CLI Paper → scheduler-owned only；
- 真实模型/真实摄取 Debug 验收 → 本 Goal 只做离线 fixture；
- provider WebSocket/SDK state → 可替换 transport，不是 durable truth。

## 9. Goal 进度表

阶段状态只由当前 exit gate 证据推进。R0–R6 已完成；R7 是下一阶段。

| 阶段 | 当前状态 | 进入条件 | 完成证据 |
| --- | --- | --- | --- |
| R0 | complete | 已完成 | invariants/test/deletion docs 与当前离线基线已重新认证 |
| R1 | complete | R0 complete | Domain v7、四资产、canonicality、authority 与下游编译边界通过 |
| R2 | complete | R1 complete | CAS、atomic/fencing/cursor/Doctor/OutcomeSchedule closure；Store 35 tests |
| R3 | complete | R2 complete | Manifest/Grant/repair/source closure 与 policy influence 越权拒绝；Context 14 tests |
| R4 | complete | R3 complete | Contract Catalogue、AgentRuntime、budget/tool grant 与 capability ceiling 通过 |
| R5 | complete | R4 complete | DAG/Task/recovery/replay 与 mandatory terminal owner-dispatch 边界通过 |
| R6 | complete | R5 complete | sealed Outcome/Shadow/canary/no-op cursor/noncanonical tests；workspace 170 tests、fixture/Doctor 通过 |
| R7 | ready / audit in progress | R6 complete | Decision/Execution/Paper owner tests；typed dispatch surface 可供 Daemon 接入 |
| R8 | partial | R7 complete | Daemon 接通全部 terminal owner runtime；scheduler、crash/concurrency、epoch fencing、HTTP/SSE、freeze tests；Unix path 不再 active |
| R9 | not started | R8 complete | HTTP-only CLI/config/help/incompatibility tests |
| R10 | not started | R9 complete | full offline matrix、fixture harness 和 deletion inventory |

## 10. 全程禁止事项

- 不 reset、clean、stash、checkout 或覆盖既有 dirty work；
- 按用户对当前 Goal 的明确授权，可提交并推送当前 `codex/akzio-v2-max-refactor` 分支；不得创建 PR 或部署；
- 不读取或输出 `.env`、token、cookie、credential；
- Goal 实施阶段不访问网页、外部模型、broker、数据库或 API；
- 不执行 Live 或真实资金操作；
- 不在 `akzio-store` 外写 SQLite；
- 不创建 parallel JSON/cache 作为状态权威；
- 不给 runtime Agent HTTP、filesystem、shell、raw-evidence 权限；
- 不从 Debug、Replay、Shadow、PaperDryRun 学习；
- 不保留 v1、Phase、FileStore、Unix compatibility；
- 不把静态检查、fixture 或 Dry Run 表述成真实 Paper/生产验证；
- 不在 owner interface 稳定前并行修改公共 schema；
- 不因外部文章案例而降低 Akzio 的 Rust/Store/security gate。
