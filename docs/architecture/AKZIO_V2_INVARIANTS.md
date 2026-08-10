# Akzio v2 Invariants

状态：R0 冻结。本文是实施约束，不是现有 active path 已完整达到的声明。

优先级为 `AGENTS.md`、当前源码和测试事实，其后依次为计划-续、原始计划和交接历史。任何实现与本表冲突时，先修实现；不得加兼容层规避约束。

## 不变量与唯一 owner

| ID | 不变量 | 唯一 owner | 首次完整强制阶段 | 验证类别 |
| --- | --- | --- | --- | --- |
| I01 | 可执行资产闭集严格为 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。 | `akzio-domain` | R1 | schema、config、execution 拒绝测试 |
| I02 | 不存在 Live Trading 构造路径；任何非精确 Alpaca Paper URL 在 HTTP I/O 前失败。 | `akzio-execution` | R7 | URL/parser、adapter 构造测试 |
| I03 | Rust 是 state、权限、预算、gate、学习迁移和 execution policy 的唯一权威；模型只提交受 schema 限制的提案。 | `akzio-domain` + owning runtime | R1–R5 | contract 和 authority-negative 测试 |
| I04 | `V2Store` 是唯一耐久状态 authority；无平行 JSON 状态、无 Store 外 SQLite 写入。 | `akzio-store` | R2 | source inventory、transaction 测试 |
| I05 | Artifact 写入、任务完成和相应事件在一次 permit-validated transaction 中提交。 | `akzio-store` | R2 | failure injection、permit/fencing 测试 |
| I06 | Agent、workflow、tool、context、decision、execution、memory 的每一耐久行为均有 append-only event。 | `akzio-store` | R2 / R10 | event-closure、replay 测试 |
| I07 | Agent 只能经 `ContextManifest` + task/attempt-bound `ReadGrant` 读取资料；无裸文件、HTTP、Raw Evidence 通路。 | `akzio-context` | R3 | grant 越权、expiry、raw-read 测试 |
| I08 | Evidence、claim、decision、outcome、memory 必须保留有效 provenance 与 source closure。 | `akzio-store` + `akzio-context` | R3 | reference-integrity、Doctor 测试 |
| I09 | Contract catalogue 只安装已审计版本；Candidate 不得扩大 source、tool 或 execution 权限。 | `akzio-research` | R4 | hash、schema、authority-escalation 测试 |
| I10 | Planner 只提出 `WorkflowProposal`；Rust lowering 注入不可绕过的 decision/execution/commit gates。 | `akzio-runtime` | R5 | graph lowering、gate-bypass、recovery 测试 |
| I11 | Canonical learning 只来自 sealed Paper Outcome；Debug、Replay、Shadow、Paper Dry Run 均不得晋升 memory 或 topology。 | `akzio-learning` | R6 | canonicality、promotion、rollback 测试 |
| I12 | 每个 broker session 最多一个 scheduler-owned durable Paper slot；每次 slot 写入以 daemon lease owner + epoch fenced。 | `akzio-store` + `akzio-daemon` | R2 / R8 | singleton、epoch、takeover、recovery 测试 |
| I13 | Rust 可自动 freeze；仅 loopback operator HTTP API 或 CLI 经该 API 可 unfreeze。 | `akzio-daemon` | R8 / R9 | freeze、HTTP auth、CLI surface 测试 |
| I14 | 唯一 public control plane 是 loopback HTTP/SSE；Unix JSON-line 业务协议不属于 v2，R9 必须删除。 | `akzio-daemon` + `akzio-cli` | R9 | transport、help、legacy-inventory 测试 |

## 耐久 mutation 对照

| 行为 | capability / permit | 原子 transaction | 必需 event |
| --- | --- | --- | --- |
| 创建 workflow | scheduler/workflow authority | Run、冻结 Plan、初始 Task、依赖 | `workflow.created` |
| 写 task artifact | `TaskWritePermit`（run/task/attempt/lease/epoch/contract hash） | artifact、task-output edge、任务状态、event | `artifact.created` + task event |
| Context manifest / repair | 有效 task permit 与 manifest grant | manifest/detail、source edges、grant、event | `context.manifested` / `context.repaired` |
| Agent turn / tool result | 已安装 Contract + task permit + tool grant | produced artifact、turn reference、event | `agent.turn.*` |
| decision / execution | accepted decision context、Rust policy verdict、daemon fence | decision/execution document、commitment state、event | `decision.*` / `execution.*` |
| memory / topology transition | sealed canonical Paper outcome | immutable successor document、head edge、event | `learning.*` / `topology.*` |

任何表外 durable mutation 都是 defect；不得以 cache、日志文件或 adapter 内状态替代上述记录。

## Canonicality

| Run purpose / data | 可写诊断证据 | 可成为 canonical memory/topology 输入 | 条件 |
| --- | --- | --- | --- |
| Debug | 是 | 否 | 永远 noncanonical |
| Replay | 是 | 否 | 永远 noncanonical |
| Shadow | 是 | 否 | 仅可作为 paired evaluation 的比较材料 |
| Paper Dry Run | 是 | 否 | 永远 noncanonical |
| Paper | 是 | 仅 sealed Outcome | outcome、source closure、execution context 全部完整且 policy 通过 |

“Paper”本身不足以授权学习；未密封 outcome、当前预测和未完成 market data 都不得被自动晋升。

## R0 决策

- 不新增依赖、Repository/Service/Factory/Builder 层或为旧调用者保留的适配器。
- 公共数据使用 newtype、enum、`serde` derive 和穷尽 `match`；安全 gate 保持显式、短小、可审计。
- 所有旧路径均必须在 `AKZIO_V2_DELETION_GRAPH.md` 有唯一 replacement phase。没有 replacement 的代码不得保留为“以后兼容”。
- R0 不声称 active runtime 已满足 R1–R10；每阶段只有在本表对应测试实际通过后才可宣告该不变量已落地。
