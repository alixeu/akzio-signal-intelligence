# Akzio v2 R0–R10 Goal 执行计划

日期：2026-08-09

状态：**Ready / Not Started**
适用 checkout：`master` @ `24e512e2f0c09b54bebfa04480f95cd27c0675b3`

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

本文件是未来实施 Goal 的执行输入。当前用户尚未重新明确说“开始执行 R0”，因此所有阶段保持 `pending`，不得据此修改 Rust。

## 2. Source of truth 与冲突优先级

1. `AGENTS.md`、当前源码、当前测试与真实命令结果；
2. `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md`；
3. `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN.md`；
4. 用户补充的历史 `PLAN.md`；
5. `AKZIO_V2_REFACTOR_HANDOFF.md` 的历史描述；
6. 外部网站资料仅提供方法论，不覆盖上述本地权威。

历史 `PLAN.md` 在 2026-08-09 当前文件系统中已不存在。本计划对它的引用来自 2026-08-07 已完成的 749 行全文读取记录，已知 SHA-256 为 `a2e030b9ef51e3de4c12c86a556cee2a6c341a888b6eceb9db6c7a7781931326`。不得声称本轮重新打开了该路径。

外部一手资料及逐项映射见 [2026-08-09-v2-goal-source-research.md](./2026-08-09-v2-goal-source-research.md)。这些链接是根据历史计划点名的主题匹配的官方来源，不是历史文件中可核对的显式 URL。

## 3. 当前事实基线

### 3.1 Checkout

- 分支：`master`
- HEAD：`24e512e2f0c09b54bebfa04480f95cd27c0675b3`
- 用户既有 untracked 文件：
  - `docs/architecture/AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md`
  - `docs/architecture/AKZIO_V2_REFACTOR_HANDOFF.md`
- 本次规划新增：
  - `docs/architecture/2026-08-09-v2-goal-source-research.md`
  - 本文件
- 不得 reset、clean、stash、checkout、覆盖或丢弃上述文件。

### 3.2 已验证基线

- `cargo check --workspace --offline`：通过，有 warning；
- `cargo clippy --workspace --all-targets --offline`：退出码 0，但有 14 个唯一 warning；
- `cargo fmt --all -- --check`：失败，差异只在五个 `rebuild.rs` 原型；
- 78 个测试已确认通过；
- 2 个 scheduler 测试确定性死锁：
  - Store：`paper_schedule_slot_is_singleton_fenced_and_doctor_checked`；
  - Daemon：`paper_schedule_recovers_a_reserved_slot_after_leader_takeover`；
- 根因：`reserve_paper_schedule_slot` 提交事务后仍持有 connection mutex guard，随后调用 `paper_schedule_slot` 再次获取同一非重入锁；
- 临时 Store Root 中 `run fixture-debug` 与 `store doctor` 已通过；这只是 fixture/local 证据，不是真实模型、真实市场或真实 Paper 订单。

### 3.3 当前架构差距

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
- 尚未开始生产重构。

**Objective**

把目标边界、crate owner、删除时机和验收矩阵机械化，并解除阻止可信全测基线的现有死锁。

**Deliverables**

- `docs/architecture/AKZIO_V2_INVARIANTS.md`；
- `docs/architecture/AKZIO_V2_TEST_MATRIX.md`；
- `docs/architecture/AKZIO_V2_DELETION_GRAPH.md`；
- capability/permit/transaction/event 对照表；
- canonical/noncanonical purpose 对照表；
- 当前接口与原型的保留/吸收/删除清单；
- 最小修复 scheduler slot mutex 自死锁，并保留 Store/Daemon 回归测试；
- 格式化五个 `rebuild.rs`，建立可重复 fmt 基线。

**Deletions**

- 删除文档中声称 Phase、FileStore、旧 Store Root 或 Unix 业务协议仍受支持的现行表述；
- 暂不删除仍被 active path 调用的生产代码，删除图必须为每个目标指定唯一 replacement phase。

**Tests**

- `cargo metadata --offline --format-version 1 --no-deps`；
- `cargo fmt --all -- --check`；
- `cargo check --workspace --offline`；
- 两个 scheduler slot 测试必须终止并通过；
- 临时 Store Root 下的 fixture-debug 和 Doctor；
- 静态 inventory：`orchestrator`、Phase、FileStore、旧 outputs、Unix、Live、直接 Paper submit。

**Exit gate**

- 两个已知测试不再死锁；
- 每条 invariant 都有 owner、目标阶段和测试；
- 每个旧路径都有唯一删除阶段；
- 没有未决 compatibility 决策。

### R1 — 重建 `akzio-domain`

**Objective**

用稳定类型和验证规则使非法权限、provenance、canonicality、资产和执行输入无法静默进入系统。

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

### R7 — 重建 Decision/ExecutionRuntime 与 Alpaca Paper 边界

**Objective**

实现 scheduler-owned、自动但 fail-closed、幂等、可 reconciliation 的四 ETF Paper execution。

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

| 阶段 | 状态 | 进入条件 | 完成证据 |
| --- | --- | --- | --- |
| R0 | pending | 用户明确“开始执行 R0” | invariants/test/deletion docs；死锁解除；基线通过 |
| R1 | pending | R0 complete | Domain tests + workspace check |
| R2 | pending | R1 complete | atomic/fencing/crash/Doctor tests |
| R3 | pending | R2 complete | Evidence/Manifest/Grant tests |
| R4 | pending | R3 complete | Contract/AgentRuntime tests |
| R5 | pending | R4 complete | DAG/Task/recovery/replay tests |
| R6 | pending | R5 complete | sealed Outcome/Shadow/canary tests |
| R7 | pending | R6 complete | Decision/Execution/Paper tests |
| R8 | pending | R7 complete | Daemon/crash/concurrency/HTTP/SSE tests |
| R9 | pending | R8 complete | CLI/config/help/incompatibility tests |
| R10 | pending | R9 complete | full offline matrix + deletion inventory |

## 10. 全程禁止事项

- 不 reset、clean、stash、checkout 或覆盖既有 dirty work；
- 不提交、推送、创建 PR、部署；
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
