# Akzio 应用运行时契约

本文保存从根 `AGENTS.md` 分离的应用运行时约束，按原章节编号便于定位。它不是开发助手的工具白名单；开发 Workflow 见 [development-workflow.md](development-workflow.md)。修改相关模块前必须读取对应章节，业务保障不会因移动文档而解除。

版本、模型和预算描述属于项目契约，不跟随 Codex 开发模型自动更新。发现本文、代码与实际冻结 Contract 不一致时，应明确报告差异并查明适用版本；不得静默改写历史对象、降低 Gate 或切换模型来消除差异。历史交接报告只证明其记录时的状态。

## 1. 项目定位与核心红线

- **定位**：本仓库是 Rust 2021/2024 (固定构建 Rust `1.96.0`) workspace，构建本地常驻、仅支持 Alpaca Paper 的多智能体量化投研系统（Multi-Agent Research System）。
- **标的范围**：系统限定可执行资产严格为 4 只美股 ETF：`TQQQ`、`QQQ`、`SOXX`、`SOXL`。
- **纯 v2 架构**：项目只维护 v2；严禁恢复旧 `orchestrator-*` crates、Phase 0–8、FileStore、旧 prompts 或 `outputs/store` 兼容路径。
- **物理绝缘实盘**：Live Trading 永不实现；`AlpacaPaper::new` 必须在发生任何实际 HTTP I/O 前强制校验并拒绝非 Paper endpoint。
- **生产模型协议**：当前唯一实现的生产模型协议是 **OpenAI Responses** (`openai_responses`)，必须声明模型发布日期 (`release_date`) 与知识截止时间 (`knowledge_cutoff`)；端点兼容不等于语义兼容。

---

## 2. 权威与数据边界

- **Rust 唯一权威**：Rust 是系统状态、授权、Task Contract、模型预算、Workflow Gates、持久化存储、学习迁移和执行策略的唯一权威。
- **统一 CAS 存储权威**：`V2Store` (底层存储于 SQLite `.akzio/store/akzio.sqlite3`) 是系统唯一持久化权威。严禁增加改变语义的并行 JSON 状态、文件缓存，或绕过 `akzio-store` 直接写 SQLite。
- **溯源保留**：Evidence、Claim、Critique、DecisionProposal、Decision、Execution、Outcome 和 Memory 必须完整保留 provenance、时间戳、版本与有效 `source_refs`。
- **受控上下文沙箱**：`akzio-context` 是 Agent 任务获取投研资料的唯一通道。模型代码严禁获取任意本地文件系统、Raw Evidence、底层 SQLite 或实盘/交易凭据的访问权。
- **Git 干净边界**：生成的 Store Root、BLOB、socket、报告、认证凭据 (`.daemon-token`) 和本地配置覆盖不得提交至 Git。
- **不可随意破坏不变量**：重构或存储演进严禁隐式修改 schema_version、`ExecutionPlan` 序列化哈希、Paper Gate 风控逻辑或 Transaction 事务边界。

---

## 3. 双并行时间轴架构 (Dual Timeline Invariant)

系统在时间维度上严格解耦为两条并行运转的时间轴，**绝非串行阻塞等待**：

```text
时间轴 A (交易日 Session T0)：
  每个美股交易日开市时，由 Scheduler 独占触发一次完整的 Paper 投研与下单决策 Run。
  [EvidenceGate] -> [Analyst] -> [Critic] -> [Synthesizer] -> [DecisionGate] -> [ExecutionGate] -> [PaperCommit] -> [Reconcile] -> [Evaluate] -> 创建 [OutcomeSchedule]

时间轴 B (跨交易日 T+1 / T+3 / T+5 评估)：
  每个历史完成的 Paper Run 拥有专属的 OutcomeSchedule，在独立的交易 Session 里被唤醒评估：
  - T+1 交易日：计算第一窗口收益与指标，生成 T1 阶段复盘 (Partial Outcome, RunScoped)
  - T+3 交易日：计算第二窗口收益与指标，生成 T3 阶段复盘 (Partial Outcome, RunScoped)
  - T+5 交易日：计算第三窗口收益，密封最终 Outcome (Sealed)，生成 T5 复盘；只有研究、风险真值和叙事均合格时才允许学习评估
```

- **并行推进机制**：在交易日 Session $S_n$ 中，系统可以同时执行当天 Run 的 T0 投研，以及历史 Run A 的 T+5 评估、Run B 的 T+3 评估和 Run C 的 T+1 评估。
- **交易日对齐规则**：T+1、T+3、T+5 严格按**四只 ETF 共同完成的实际交易 Session 数**计算，周末与休市日不计入。

---

## 4. 模块边界与职责矩阵

| Crate | 核心职责 | 边界约束 |
|---|---|---|
| `akzio-domain` | 领域模型稳定 Schema 与静态校验 | 纯数据结构与计算验证；**严禁包含任何 I/O** |
| `akzio-store` | SQLite 统一 CAS BLOB、Artifact、事件日志、Lease 与 Doctor | 唯一数据读写层；统一使用 `rebuild_*` 表 |
| `akzio-context` | 投研材料 Manifest 组装与只读受控访问 | Agent 数据获取唯一沙箱，拦截非授权读取与越界 |
| `akzio-runtime` | Workflow 编译、节点生命周期管理与崩溃恢复 | 状态推进，串联任务执行流水线 |
| `akzio-research` | Agent Contract 编排与模型双阶段调用治理 | 限制 Prompt 组装、Tool 调度与输出 Schema 校验 |
| `akzio-execution` | Rust 决策双闸门 (Decision/Execution) 与 Paper 提交 | 负责资金分配、风控裁剪、幂等 Commitment 与 Broker 交互 |
| `akzio-learning` | T+1/T+3/T+5 Outcome 评估与策略/Memory 演化 | Rust 权威计算投资指标，受控吸收复盘经验 |
| `akzio-daemon` | 进程领导权、定时调度器、HTTP/SSE 传输与 Worker 分发 | 严禁把 Policy 或 Durable Invariant 堆入调度层 |

---

## 5. 智能体系统规范与运行拓扑

### 5.1 智能体拓扑与阶段分工

| 阶段 / 智能体 | 角色与说明 | 调用模型 | 预算限制 (Token/时限/Tools) | 核心产物 |
|---|---|---|---|---|
| **首轮 T0 (无 Planner)** | 首轮正式 Paper **不调用 Planner Agent**，直接由 Rust 编译固定拓扑 | - | - | 确定性 WorkflowProposal |
| **Analyst Agent** | 评估市场证据，输出结构化主张 (Claim) 与局限 | `gpt-5.6-luna` (high) | 48k in / 6k out / 120s / 4 tools | `ArtifactKind::Claim` |
| **Structured Critic** | 对 Analyst Claim 独立审查与交叉验证 | `gpt-5.6-sol` (high) | 48k in / 4k out / 120s / 4 tools | `ArtifactKind::Critique` |
| **Synthesizer Agent** | 综合证据与审查，生成 12 项 Forecast 及四资产+现金研究分配提案 | `gpt-5.6-luna` (high) | 48k in / 5k out / 120s / 2 tools | `ArtifactKind::DecisionProposal` |
| **Outcome Worker** | T+1/T+3/T+5 阶段产出质性归因复盘 | `gpt-5.6-luna` (medium) | 12k in / 4k out / 180s / 2 tools | `RetrospectiveDraft` |

正式 Paper 默认拓扑包含按 T1/T3/T5 分工的三个 Analyst/Critic 对，Synthesizer 直接依赖全部 Claim 和 Critic 路径。单 Claim 协议保留，预算按任务累计。任一 Critic NoOutput 不得切断 Claim 血缘。

### 5.2 两阶段模型调用协议 (Two-Phase Invocation)

所有研究型 Agent 均受统一的 `Shared Governance Prompt` 治理，执行严格的两阶段调用：
1. **Draft 阶段 (草稿与研究)**：
   - 只能读取 `ContextManifest` 授权范围内的 Artifact；
   - 允许调用受控的只读工具，`tool_choice = auto`；
   - 模型在上下文内撰写可审计的简体中文研究 Memo。
2. **Submit 阶段 (提交结果)**：
   - 一旦 Memo 完成，Rust 将所有只读工具撤除，仅保留 `submit_result`；
   - `tool_choice = required submit_result`，模型必须且只能调用一次该工具提交严格 JSON；
   - Rust 校验 Schema 与业务不变量。若校验失败，返回结构化错误并在预算内重试。
3. **调用审计落库**：
   - 任何 `ToolCall` 必须**先持久化写入 Store，再执行读取，随后持久化 `ToolResult`**。确保进程在工具执行中断时具备确定性审计和恢复依据。

### 5.3 严格受控的 5 个只读工具

Agent 严禁暴露 `web_search`、`shell`、`http_request`、`file_read`、`sql` 或交易执行工具。模型仅被允许使用以下 5 个只读 Context Tool（以及提交阶段的 `submit_result`）：
1. `read_document(artifact_id)`: 读取当前 Manifest 授权的完整规范化文档；超过 32 KiB 时显式要求改用 read_range，不静默截断。
2. `read_range(artifact_id, start_byte, end_byte)`: 切片读取，单次上限 32 KiB。
3. `search_context(query, max_results)`: **非向量/语义搜索**，底层实现为小写子串包含匹配（`text.to_lowercase().contains(&needle)`）。提示词必须指导模型输入简短关键字（如 `TQQQ`, `VIXCLS`），避免长句子。
4. `read_claim_evidence(artifact_id)`: 读取 Claim 及其 Grounds 引用且已授权的标准化证据，不开放 RawEvidence。
5. `compare_sources(artifact_ids)`: 对比 2～4 个已授权数据源的时间与内容。

### 5.4 各 Agent 行为与约束不变量

- **Analyst 约束**：
  - 单次运行产出单一 Horizon (`t1`, `t3` 或 `t5`) 的 Claim。
  - 最多声明 2 个 `evidence_gaps`；若存在阻塞性缺口 (`blocks_directional_forecast`)，Rust 仅允许系统补充抓取一次证据并重跑 Analyst，禁止模型自行联网。
  - 必须保证不确定性权重和置信度守恒：$\sum \text{uncertainty\_weight\_ppm} = 1,000,000 - \text{confidence\_ppm}$。
- **Critic 约束**：
  - 只能输出 `supported`、`contradicted`、`not_enough_information` 三种状态。
  - 缺少跨域证据支撑时必须标记阻断 (`blocker: true`)。
- **Synthesizer 约束**：
  - 必须生成完整的 **4 资产 × 3 Horizon = 12 个 Forecast**（涵盖 `TQQQ`, `QQQ`, `SOXX`, `SOXL` 的 `t1`, `t3`, `t5`）。
  - 任何未获得 `SUPPORTED` Critique 的预测项，强制降级为中性预测（`positive_return_probability_ppm = 500000`, `expected_return_ppm = 0`）。
  - 必须额外提交 `research_allocation`：四个资产各一行、显式 `cash_weight_ppm`，总和严格为 `1,000,000 ppm`。非零行必须列出 supporting horizons、精确 evidence refs 和 rationale；零行必须明确 abstention_reason。这是研究意图，不是订单或执行许可。
- **Outcome Worker 约束**：
  - **严禁输出权威的收益率、滑点、回撤或政策决策**。所有量化指标由 Rust 权威计算；Agent 仅能就因果链、证据得失与反事实进行叙事复盘。

---

## 6. 证据采集与 Context 隔离

- **40 项固化证据需求**：Rust 在每个 Paper Session 确定性生成 40 个唯一 `EvidenceNeed`（包含 Broker 账户/持仓/报单状态、四资产向前 400 天/至少 252 根日线、14 天新闻 VerifiedSource、FRED 宏观序列 `DFF`/`DFII10`/`VIXCLS`、杠杆 ETF 损耗条款等）。
- **Context 限制与隔离**：
  - 每个 Agent 的 `ContextManifest` 顶层上限为 **24 个 Artifact**。
  - 普通 Agent 上下文限制 128 KiB，Synthesizer 限制 192 KiB。
  - 严禁向模型注入 `RawEvidence`，必须经过 Rust 标准化为 `NormalizedEvidence`。

---

## 7. Rust 决策双闸门与执行边界

```text
DecisionProposal (LLM: Forecast + research_allocation)
       ↓ 
[DecisionGate (Rust)]    -> 校验研究分配并记录 raw→validated；另行校验校准/风险 -> 产出 Decision / DecisionContext
       ↓ 
[ExecutionGate (Rust)]   -> 校验账户资金、最新报价、流动性、审批令牌 -> 产出 ExecutionVerdict
       ↓ 
[PaperCommitment (Rust)] -> 写入确定性 client_order_id 到 SQLite -> 调用 Alpaca Paper API -> Reconciliation
```

1. **DecisionGate 默认 Fail-Closed**：
   - 若未配置经过验证的离线校准政策文件 (`decision_policy_path`)，Rust 默认校准样本为零，**目标组合头寸强制为 0**。
   - 这只阻断执行侧 `Decision.targets`；有合格证据的非零 `research_plan.validated` 必须保留，并明确 `execution_status=blocked` 或 PositionPlan 的 `not_applicable`，不得冒充可执行目标。
   - 若当前为空仓，产生 `NoExecutableOrder`；若账户已有多头仓位，Allocator 将生成卖单清仓至 0 敞口。
2. **ExecutionGate 与幂等 Commitment**：
   - 严禁直接提交订单。必须在判定 `Accepted` 后，先生成带确定性 `client_order_id` 的 `ExecutionCommitment` 并持久化至 SQLite，随后才发起 Alpaca API 请求。崩溃恢复时依据相同 ID 幂等恢复。
3. **无订单仍须评估**：
   - 即使 ExecutionGate 产出 `NoOrder`，系统的预测意图仍由 `Evaluate` 生成 `OutcomeSchedule`，并在后续 T+1/T+3/T+5 持续跟踪其实际市场表现。

---

## 8. 统一 CAS 存储映射规范

系统严禁建立针对业务模型的平铺表格（无 `claims`, `forecasts`, `orders` 等表），完全统一至内容寻址存储 (CAS)：

| 存储表名 | 存储内容与职责 | 关键字段 / 索引 |
|---|---|---|
| `rebuild_blobs` | 存储所有 JSON 强类型负载（Zstd 压缩） | `blob_hash`, `logical_bytes`, `stored_bytes`, `payload` |
| `rebuild_artifacts` | 统一 Artifact 元数据与生命周期 | `artifact_id`, `kind`, `blob_hash`, `lifecycle`, `provenance_json` |
| `rebuild_artifact_refs` | Artifact 之间的全量 DAG 依赖血缘关系 | `source_artifact_id`, `target_artifact_id`, `ref_type` |
| `rebuild_runs` | Run 生命周期与图拓扑元信息 | `run_id`, `purpose`, `topology_id`, `graph_artifact_id`, `status` |
| `rebuild_tasks` | Task 调度、Contract 校验与 Lease 租约 | `recipe_id`, `objective`, `contract_hash`, `budget`, `status`, `lease` |
| `rebuild_attempts` | 任务单次尝试的执行状态与重试记录 | `attempt_id`, `task_id`, `status`, `error_json` |
| `rebuild_attempt_outputs`| 成功 Attempt 产生的正式输出映射 | `attempt_id`, `artifact_id` |
| `rebuild_session_slots` | Paper 交易 Session 的排他性原子插槽 | `session_key`, `run_id`, `commitment_artifact_id` |
| `rebuild_policy_*` | T+5 封存后的评估记录、经验回放与转移头 | `rebuild_policy_evaluations`, `rebuild_policy_transitions` |
| `rebuild_lesson_*` | 学习演化生成的 Lesson 与隔离证据 | `rebuild_lesson_heads`, `rebuild_lesson_events` |

---

## 11. 当前实现补充与交接边界

- ContextManifest、ReadGrant 与工具结果使用一致的精确 source/kind/producer 规则；内部对象还要校验 Run、生命周期、lineage 和学习资格，禁止放开 `akzio.*` 通配权限。
- Outcome 原文授权可占用既有 128 KiB / 24 Artifact 沙箱（估算上限 32k tokens），模型两阶段及工具回读仍共用 12k 输入预算。projection v2 明确列出省略项；完整原文仍经原 Manifest/ReadGrant 获取，省略不代表为空或已验证。
- Canary 仅允许已登记的三个 Shadow 复用父 EvidenceGate 成功 Attempt 的冻结 NormalizedEvidence；collection status 由精确 snapshot producer 引用，禁止把 RawEvidence 或后续刷新当作授权材料。Shadow Outcome 的跨 Run 引用只取父 OutcomeSchedule 的冻结执行来源。
- Draft 最多使用总输出的一半和约 55% 时限；必须先有已持久化 Draft memo 才能进入 Submit；首轮预算不足以完成两阶段时明确拒绝，不跳过 Draft。恢复与阶段切换不能重置预算。新 Contract 的完整 Schema、role Prompt 和 Rust 验证必须同步。
- 40 项证据逐项留状态：前置 EvidenceGate 将六项 ExecutionSafety 标记 deferred_to_execution 且不采集；ExecutionGate 原 refresh_execution_snapshots 即时获取并继续 fail closed。研究和增强数据失败保留成功项与缺口。新鲜度看内容可用时间，日线以交易所收盘（美东 DST/提前收盘）加 20 分钟余量判断；不以下载时间或 UTC 日期替代 Session。
- 每个 Outcome 是同 Run 的 post-terminal 工作；先检查未完成阶段再调用模型，迟到补跑按各阶段自己的 cutoff 截断事实。每 Outcome 租约单写，争用 Deferred；完成、报错及取消时按 epoch 释放租约。Deferred 不进入失败预算，已提交的阶段事件为下一阶段建立独立额度；Outcome 重试从 30 秒指数退避，最多 5 分钟。Worker 对 Session/Outcome 进行服务保留或单 Worker 交错。
- 执行后持仓按数量和现金重建，Receipt 去重并验证实际成交。Outcome 是冻结执行后敞口（metric v3），不代表真实后续账户 NAV；按当前阶段实际计价窗口检查公司行动，窗口内无调整账本则不可用，禁止推测调整因子。
- Decision 生产成本取依赖任务闭包；Outcome/叙事成本与生命周期总成本分开。Experience 保留 producer Run/Contracts/Workflow revision 与 evaluator Contract。行情完整、研究充分、叙事有效、风险真值已测量分别记录。缺失风险真值继续为 None，不得填满分。
- Rust-only T5 不取得学习资格。叙事修复在原 Run 上提交可追溯 revision，保留原数值 CAS；修复后复用密封 Outcome，经原资格检查幂等补评估；不得直接写入 Proven。LessonProposal 显式声明资产、horizon、排除条件与证据，只有 Draft/quarantine 权限。
- 当前版本：Domain schema 10（兼容可选字段）；Store 16；正式 Contract 40；Prompt bundle 24；freshness candidate 41；Outcome metric `frozen_post_execution_exposure_v3`；evaluation context 1；benchmark definition 1。旧未知口径和 v2 数据不自动改标 v3。
- v14→v15 迁移只新增 Run/kind 表达式索引。旧 Contract 的未完成任务会在写新 Store 版本前阻断升级；旧任务哈希不改写，旧 CAS/Commitment 不改写。停止旧 worker 并合法处理旧任务之后再升级。RuntimeIdentity 与旧审批不自动复用。
- Canary 密封/多 subject 评估的中间进度保存在 CAS、事件和评估账本；只有随 Attempt 成功一并提交的产物进入 `rebuild_attempt_outputs`。等待或中断不撤回已完成 subject，不把未完成 Attempt 发布成成功输出。

修复证据与 34 项状态见 [任务一交接](task1-repair-handoff.md)；隔离 Store、内部测试与入口见 [任务二 Debug](task2-debug.md)，实际执行结果见 [离线 Debug 记录](task2-offline-debug.md)。历史任务一检查不等于任务二结果，离线组合测试不等于真实 LLM/Paper 或实际跨交易日的学习验收。

本次复核补充：每期限 Claim 与 Critique 的 grounds 上限为 12，四资产价格/新闻保持单资产范围，共享宏观可覆盖四资产，最低完整依据为 4+4+1。Context 先保证角色必需输入，再分配可选背景；必需集合放不下必须显式拒绝。冻结净收益按价格效应 + 有符号实施差额 + 初始估值差额 - 估算费用计算，limit_shortfall 只作诊断，不能重复扣除。

---

## 12. 分流程 Debug 控制

- Debug Core 必须使用新隔离 Store，禁止打开 `~/.akzio/store` 或启用 `auto_paper`。Store 的隔离标记不可通过重启成普通 Core 移除。
- 正式 Debug prepare 复用共享 Rust-owned research proposal。Paper 保留 40 项 EvidenceNeed 和 Session reservation；PositionPlan 只保留 34 项研究 Need，不占用 Paper session slot，在 Decision 后结束。调度权由 Store 的 DebugSession、revision CAS 和 claim 事务决定；App/CLI 不推断 readiness。
- Store 16 增加 Debug 控制表及 RunScoped DebugRecord CAS 类型。升级前必须停止旧 worker；旧 running task 或有效 daemon lease 会阻断升级。原 Contract、CAS、Commitment 不重写。
- Broker 写入默认 forbidden，在 Reconcile、Dispatch 和 Store effect intent 边界强制阻断；paper_allowed 仍需原审批和全部业务 Gate。
- Outcome processing 独立于新 T0 调度，仍保留 Paper purpose 与原跨交易日算法。隔离 Debug 不写 canonical policy/Active Lesson。
- Debug Store 放在 `.akzio/` 内，避免 App 打包清理 `target/` 时丢失 checkpoint。打包验证默认保留构建产物并使用新 Bundle 路径，具体规则见 [开发 Workflow](development-workflow.md)。
- 本阶段的离线控制器证明、命令、限制见 [分流程 Debug 基础设施](debug-infrastructure-phase1.md)。Fixture 控制器结果不能作为真实模型或 Paper 业务验收。

研究与执行边界：现有 `RunPurpose::PositionPlan` 表示 research + Decision / target position plan，无 ExecutionGate、PaperCommit、Reconcile 或 Evaluate；`RunPurpose::Paper` 使用同一 `approved_research_proposal` 三组 T1/T3/T5 Analyst/Critic 和 Synthesizer，再进入原执行链。没有新增 RunMode。Paper 的 account / positions / open_orders / fills / quotes / clock Need 保留 scheduler identity 与 provenance，前置只记录 Deferred，执行时才刷新。PositionPlan 执行显示 N/A，Debug manual/continuous、Broker policy、learning scope 仍是独立控制维度。补充研究保留冻结 Session 日期及原 future-data/cutoff 校验，不再要求市场当前开放。NewsWeb acquisition purpose mapping 和 policy hash 未改，缺失或未验证的方向证据不得变成有效 grounds。
