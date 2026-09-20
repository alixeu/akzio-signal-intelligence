# Akzio 应用运行时契约

本文保存从根 `AGENTS.md` 分离的应用运行时约束，按原章节编号便于定位。它不是开发助手的工具白名单；开发 Workflow 见 [development-workflow.md](development-workflow.md)。修改相关模块前必须读取对应章节，业务保障不会因移动文档而解除。

版本、模型和预算描述属于项目契约，不跟随 Codex 开发模型自动更新。发现本文、代码与实际冻结 Contract 不一致时，应明确报告差异并查明适用版本；不得静默改写历史对象、降低 Gate 或切换模型来消除差异。历史验证只证明其记录时的状态。

## 当前研究与证据规则

当前 Contract、PromptBundle 与 freshness candidate 版本见 [catalogue.rs](../crates/akzio-research/src/agent/catalogue.rs)；历史冻结对象继续按原身份验证。以下描述当前行为，不是历史验收记录。

非中性预测要求同资产、同期限、同方向的 Claim 支撑；非零多头研究配置要求对应看多依据、正预期收益和相关引用；显式现金仍合法。旧 proposal 不按新规则重新解释，旧未完成任务继续阻断升级，旧 CAS 和哈希不改写。模型 route、reasoning 与预算没有随这次升级修改。

仅新研究角色的默认累计输出上限改为 1000000，可由既有角色配置下调；Outcome 默认 4000 不变。输入、ContextPolicy、工具、墙钟、重试和模型 route 不随之扩大。旧任务、预算及哈希保持冻结，未完成旧任务仍经原升级阻断规则处理。配置与单请求上限语义见 [Agent Budget 配置](agent-budget-configuration.md)。

新指导仅压缩重复说明，保留全部必需字段、事实、数值和引用；沿用 Contract 49 引入的输出预算及单请求上限，不扩大任何资源额度。已冻结的 Contract 49 与更早版本保留原指导、预算和哈希；未完成旧任务及有效 lease 仍通过原升级机制阻断切换。

新输出显式声明 EvidenceGap.retriable；可重查的阻断缺口必须携带受控请求。一次补采及最多一次 Analyst 重跑的额度跨恢复保持；第二次仍无事实时保留缺口。既有冻结 Contract、CAS 和哈希不变，未完成任务继续阻断升级。真实 PositionPlan 启动前由 Rust 预检 policy；已安装 policy 必须严格匹配当前模型及 Synthesizer Contract。缺 active policy 时的显式 research-only 边界见下文。

Alpaca 股票研究捕获先核验资产，保留市场时钟、交易日历、显式 IEX/SIP 请求、snapshot/quote/trade 及各自时间戳；收盘日线沿用交易所 Session close + 20 分钟可用性规则。期权捕获默认显式 indicative，不自动切换 OPRA；到期范围最多 30 天、行权价为截止前标的成交价（或完整日线）的 90%-110%、最多 4 页及 512 合约，缺 quote 时最多 6 次、每次 100 合约批量请求；明确权限拒绝后停止补采。按 OCC 合并 contracts 的 dated open interest；缺失保持 unknown。IV/Greeks 没有独立 provider 时间戳时明确为 unknown，不能借用 quote/retrieval 时间。覆盖率和截断状态随 NormalizedEvidence 投影保留；原始请求账本随 RawEvidence 保存。休市旧数据不标为实时。

新 PositionPlan 的 Analyst/Critic/Synthesizer 直接提交结构化 result + deliberation，Rust 校验后持久化；没有 Draft 或第二次格式化模型调用。其他 purpose 与旧冻结 Contract 保留原阶段协议。新的方向资格由同一个 Claim 的正式 price/macro grounds、精确资产/期限、对应 Critique 对这两类 grounds 的当前权威核验和无 slot blocker 共同决定；Context matrix、Submit 与 DecisionGate 共用该规则。Critic 新 evidence 和 deliberation 不能补齐 Claim。模型不填写 expiry/holding period，Rust 从四资产共同的 Alpaca 计划交易日历计算；日历元数据不构成未来价格事实。原 12 forecasts、allocation 守恒、引用闭包仍强制校验。阶段审计记录 provider、parse_validate、persist_stage 耗时，含 structured/draft/submit 标识及 revision。结构化修复显式记录原始调用引用、前后 result 哈希和差异路径；仅修复 deliberation 时切换为只含 deliberation 的修复 Schema，缩减为该元数据和校验错误上下文；Rust 从原始 AgentTurn 冻结并复用 result，模型不能再次提交 result。越权重写 result 会直接拒绝。恢复核对完整提交和元数据修复的各自工具哈希，累计预算保持不变。

PositionPlan 启动器默认要求 decision_capable；缺 active policy 时在启动 Core/LLM 前 fail-fast。显式 `--research-only` 无论 Policy 是否就绪都只逐步执行 Evidence → 首轮 Analyst/Critic → 受控补采与受影响期限重跑 → Synthesizer/ProposalReviewer，Decision 保持未运行。Rust 禁止该缺 policy 的真实 Debug session resume 或 step Decision。不得伪造或自动激活校准 policy；Broker 仍 forbidden。`--keep-artifacts` 可保留隔离 Store 以复核原始 provenance，ZIP 不含 Store、配置或密钥。

共享治理与 Analyst/Critic/Synthesizer 正文不再规定阶段，实际请求构建器按 Contract 与角色选择研究单次结构化协议或 Outcome 两阶段协议；旧冻结正文、CAS 与 Contract 不重写，原升级阻断继续生效。Context 对 Invesco JSON holdings 也应用精确权重排序和 12 行投影；期权保留完整聚合、时间与覆盖字段，样例缩为前两项并记录省略数。相同任务的 Retry/Recovery 只复用精确来源、相同 Contract 和相同内容的历史期权投影，仍签发当前 Attempt 的新 ReadGrant，不放宽恢复身份校验。研究角色的 Context 描述明确没有读取工具。

Debug bundle v2 支持经过同一脱敏器处理的 UTF-8 文本和完整 NDJSON 记录；无权限时不导出 RawEvidence 内的 provider 明细。搜索审计从已持久化的 discovery/reviewer provider 响应提取 action/source，不从文章、摘要或模型自述推断；缺失审计显示 unknown/null，搜索发生与来源核验仍是不同事实。研究不完整与导出完整性独立记录；DecisionPolicy 缺失仍不运行 PositionPlan Decision。

新 PositionPlan 的 Claim/Critique wire grounds Schema 按 Rust 读取的 Manifest 证据资产范围绑定 anyOf 分支，单资产新闻不能同时声明另一资产；共享宏观仍可声明其合法多资产范围。按相同 scope 合并 ID，避免逐文档展开 Schema。Rust 在恢复 kind 和执行原业务校验前也验证同一个 bound wire Schema，不能仅依赖 provider。新 Run 的 Analyst/Critic 默认 Attempt 时限由 120s 调整为 180s，Synthesizer 保持 180s；原冻结 Contract/Run 的序列化预算不改，累计调用用量与未知用量阻断继续生效。

当前研究发布使用 Contract 69 / Prompt bundle 38（freshness candidate 70）；Outcome 保持 Contract 63 / Prompt bundle 35。新研究提交对未 source-verified 的 news 仅接受描述性 ground，不接受其 supporting_ref；source scope 和 citation 完整性不再代替来源验证。补采建议在提交时校验类型化业务意图，Rust 再绑定冻结资源和时间窗口，并使用与 adapter 相同的 GovernedResource 解析器。旧 CAS/Contract 不重写，未完成任务的升级阻断保留。

新闻复核仅规范化已列明的 utm 跟踪参数，不推断重定向或合并不同业务 URL；逐事实验证并记录 URL 绑定与失败原因，有效事实与失败项可同时保留，model_reviewed 不升级为 source_verified。Context 在必需闭包之后优先覆盖价格、新闻、宏观和事件日历，再分配期权背景；24 项及字节预算不扩张。无读取工具的 PositionPlan 投影明确原文未开放。缺失 Policy 的真实 PositionPlan inspect 和控制入口复用冻结 Session 判定；仍不执行 Decision。

导出将研究 AgentTurn、采集 provider 响应、hosted web 操作、含 action.sources 的操作以及来源复核失败分别计数。reasoning effort 优先读取 telemetry，否则读取已保存的实际 provider request；未保存不推断。上下文导出列出本包内未选中的证据，不将其解释为在当时 cutoff 已可用。数值依据是模型估计说明，不代表 Critic 已审查具体数值或 Policy 已校准。

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
  [EvidenceGate] -> [Analyst/Critic × 3] -> [Supplement + affected reruns] -> [Synthesizer/ProposalReviewer] -> [DecisionGate] -> [ExecutionGate] -> [PaperCommit] -> [Reconcile] -> [Evaluate] -> 创建 [OutcomeSchedule]

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
| `akzio-research` | Agent Contract 编排与研究单次提交、Outcome 两阶段治理 | 限制 Prompt 组装、Tool 调度与输出 Schema 校验 |
| `akzio-execution` | Rust 决策双闸门 (Decision/Execution) 与 Paper 提交 | 负责资金分配、风控裁剪、幂等 Commitment 与 Broker 交互 |
| `akzio-learning` | T+1/T+3/T+5 Outcome 评估与策略/Memory 演化 | Rust 权威计算投资指标，受控吸收复盘经验 |
| `akzio-daemon` | 进程领导权、定时调度器、HTTP/SSE 传输与 Worker 分发 | 严禁把 Policy 或 Durable Invariant 堆入调度层 |

---

## 5. 智能体系统规范与运行拓扑

### 5.1 智能体拓扑与阶段分工

| 阶段 / 智能体 | 角色与说明 | 调用模型 | 预算限制 (Token/时限/Tools) | 核心产物 |
|---|---|---|---|---|
| **首轮 T0** | Rust 编译固定研究拓扑 | - | - | 确定性 WorkflowProposal |
| **Analyst Agent** | 评估市场证据，输出结构化主张 (Claim) 与局限 | `gpt-5.6-luna` (high) | 1M in / 1M out / 180s / Submit | `ArtifactKind::Claim` |
| **Structured Critic** | 对 Analyst Claim 独立审查与交叉验证 | `gpt-5.6-sol` (high) | 1M in / 1M out / 180s / Submit | `ArtifactKind::Critique` |
| **Synthesizer Agent** | 综合证据与审查，生成 12 项 Forecast 及四资产+现金研究分配提案 | `gpt-5.6-luna` (high) | 1M in / 1M out / 180s / Submit | `ArtifactKind::DecisionProposal` |
| **Outcome Worker** | T+1/T+3/T+5 阶段产出质性归因复盘 | `gpt-5.6-luna` (medium) | 1M in / 4k out / 180s / 受控读取与 Submit | `RetrospectiveDraft` |

上表预算为新 Run 的默认冻结值，配置覆盖及累计用量语义见 [Agent Budget 配置](agent-budget-configuration.md)。工具预算不改变 Contract 授权。

正式 Paper 默认拓扑包含按 T1/T3/T5 分工的三个 Analyst/Critic 对，Synthesizer 直接依赖全部 Claim 和 Critic 路径。单 Claim 协议保留，预算按任务累计。任一 Critic NoOutput 不得切断 Claim 血缘。

### 5.1 有界补采与终稿审查

新冻结图包含首轮三个 Analyst/Critic 对、一个 Rust `research.supplement` 协调节点、三个可跳过的重跑对以及 N+1 个 Synthesizer/ProposalReviewer 对。`[agent.research] max_proposal_revisions = 2` 表示初稿外最多两次修订；0 只审初稿。PositionPlan 节点数为 `17 + 2N`，Paper 为 `21 + 2N`，默认分别为 21、25。配置拒绝负数、非整数及超过 32 节点的值（当前 N 最大 5），冻结后改配置不改变已有图或额度；未触发节点明确为 skipped。

Analyst/Critic 提交 news、price、macro 类型化意图、资产或受支持序列及目的，Rust 从 Run 冻结的 EvidenceNeed 绑定资源、窗口和 cutoff。只有可重试的实质性方向阻断缺口进入全 Run 一轮、最多八个去重资源的补采；四资产新闻展开为四条。请求开始在 I/O 前持久化；恢复保留已耗额度，未知完成状态不重发。处置记录包含合并、影响不足、额度耗尽、非法协议、采集失败和无新增事实。只有新增合格事实才重跑受影响期限的 Analyst/Critic 各一次；Rust 选择有效 revision，不混用原结论，不开启第二轮。

独立 `research.proposal_reviewer` 继承 Critic 的模型路由与有效单任务预算，在 Run 创建时冻结。输入包含完整提案、有效 Claim/Critique、numeric_basis 和关键反证。numeric_basis 恰好覆盖 12 个 forecast、四资产与现金共 17 项，说明输入、单位、方法、假设与不确定性；配置说明仓位、重叠风险与现金理由。ProposalReview 逐项通过/拒绝并给出原因，由 Rust 绑定精确 proposal Artifact、内容哈希、Manifest 和 Contract；任何内容变化都需新 Review。拒绝意见进入下一次 Synthesizer；通过后跳过剩余修订。耗尽、上下文不足或审查失败明确阻断 Decision。结构化格式修复与业务提案修订独立计数，恢复不重置预算。

Context 的必需集合包括有效 Claim/Critique、已审 grounds、numeric_basis 引用和反证，先于可选背景；24 项上限不变，关键材料放不下即报覆盖缺口。创建 Manifest 时持久化候选覆盖记录；导出时才出现的证据只能标记存在，不能回推历史可用性。Rust 输出研究进度、审查状态、revision 与 Decision 阻断；脚本与 App 显示该投影，workflow 原状态保留。审查通过只证明依据与推理被审查，SQL active Policy、适配和风险 Gate 独立生效，不生成或自动激活 Policy。Outcome 协议与历史哈希不变。

Contract 69 的拒绝 assessment 必须有 1–3 个类型化 issues；通过项 issues 为空。Rust 生成稳定问题 ID，冻结已通过 forecast 的模型字段及 numeric_basis，保留配置联动和 Rust 日历绑定。两次拒绝的问题、引用及被拒绝内容完全不变时停止剩余修订，保留拒绝 Review 并阻断 Decision。Lesson 召回先扫描同一 SQL 快照的全部 Active heads 再筛选排序，四条上限内保留完整显式冲突组；来源更新只生成 RunScoped 重验建议。具体审计和实验入口见[研究质量验证](research-quality.md)。

### 5.2 研究提交与 Outcome 协议

运行时提示词按功能所有权维护，统一索引见 [提示词所有权与验证](prompt-ownership.md)。治理与角色仍通过冻结 PromptBundle/CAS 读取；动态 Schema、权限、预算、引用绑定及 Gate 由 Rust 决定。

Contract **69** / PromptBundle **38** 的 Analyst、Critic、Synthesizer 在 Paper、PositionPlan 和 Shadow 一律从 Submit 开始，只提供授权 projections 和 `submit_result`，提交 `result + deliberation`。没有独立 Draft、备忘录续传或读取/搜索工具。Rust 保留方向资格、引用闭包、补采约束、交易日历绑定和仅 deliberation 修复机制。freshness candidate 为 **70**。

Outcome 保持 Contract **63** / PromptBundle **35** 及冻结哈希，继续 Draft → 受控读取 → Submit。其 ToolCall 先持久化再执行，ToolResult 随后持久化；恢复不重置预算，也不跳过已要求的 Draft。

Planner 活动 Contract、配方及扩图能力已退休。旧 purpose 和 Artifact 类型仅供历史解码、展示、导出和完整性审计。旧研究任务的领取、继续、重试和 fork 返回 `legacy_workflow_retired`；升级预检列出未完成任务及有效 lease，不自动取消或释放。

### 5.3 严格受控的 5 个只读工具

Agent 严禁暴露 `web_search`、`shell`、`http_request`、`file_read`、`sql` 或交易执行工具。只有 Outcome Draft 可使用以下 5 个只读 Context Tool；四个研究角色只提供 `submit_result`：
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
- **Lesson 查询范围**：`ContextQueryScope` 由 Rust 根据已核验的冻结节点生成；资产为当前四 ETF 研究全集，期限取 `NodeSpec`，Synthesizer / ProposalReviewer 显式覆盖 T1/T3/T5，阶段使用完整 recipe ID。查询维度为空表示未知，只匹配该维度未限定的 Lesson，不表示任意匹配。市场状态只取通过原来源、Contract、Run 和类型校验的 DecisionTime RegimeSnapshot；正文字符串、标签与 ExPost 快照不能提供范围。实际召回仍受原 Contract 限制，当前研究链只有 Synthesizer 允许 Lesson。历史节点复用既有只读 NodeSpec 适配器，不改写历史 Artifact；子任务沿用父 Manifest 的权限收缩，不额外扩大召回。最终查询范围保存在 `learning.retrieval.audit` 中。

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
   - 若 Store 没有已激活且通过验证的 `DecisionPolicy`，Rust 默认校准样本为零，**目标组合头寸强制为 0**。canonical Store 数据库不存在时由 Rust Store 初始化 schema，初始化不会生成或激活 policy；已有 canonical Store 在 PositionPlan 引导阶段只读打开。运行时只读取 SQL Store 中的 CAS Artifact、激活历史和 active head；旧文件路径兼容字段与 JSON 文件导入链路已移除。operator 将风险限制写入 SQL，collect 从 canonical Outcome 生成 SQL dataset，build 生成 SQL 候选 policy；inspect、validate、activate 均按 Store Artifact ID 操作，生成候选不会自动更新 active head。
   - 这只阻断执行侧 `Decision.targets`；有合格证据的非零 `research_plan.validated` 必须保留，并明确 `execution_status=blocked` 或 PositionPlan 的 `not_applicable`，不得冒充可执行目标。
   - 校准 `collect` 只接受非隔离 canonical Store 的真实 Decision/Outcome，并要求有效 Synthesizer 模型的发布日期、知识截止日期及显式风险限制。readiness 的 Store 资格与成熟度不代表候选存在或可激活；隔离 Debug（含原生 Paper）的运行报告 `isolated_debug_store`，不计入成熟样本。等待、复制或修改 purpose 不能赋予隔离运行或历史模拟数据正式校准资格。
   - 只有通过原审批和所有 ExecutionGate blocker 检查后才进入 Allocator：当前为空仓时产生 `NoExecutableOrder`，已有多头仓位时才可能生成归零卖单；缺 approval 的冷启动不进入 Allocator，也不会触发清仓。
2. **ExecutionGate 与幂等 Commitment**：
   - 正式 scheduler 在 SQL 没有 active policy 时创建不绑定 approval 的 canonical Paper run，以积累未来真实校准标签；已有 active policy 时仍要求原审批。未校准 Paper 冷启动没有 approval 时，pre-trade safety 返回无评估，不作安全断言，也不以缺 manifest 抛错阻断 verdict。原 `UnqualifiedRuntime` blocker 保留，跳过 allocation/plan closure，形成持久化 `NoOrder`；PaperCommit/Reconcile 在获取执行 lease 或访问 Broker 前短路。Evaluate 仍可生成 `OutcomeExecutionLineage::NoOrder` 的 OutcomeSchedule，无需 ExecutionCommitment。此语义不扩张任何执行权限；真实密封仍要求完整 baseline 与四资产共同完成的真实 T+1/T+3/T+5 Session，policy 必须由 operator 显式 inspect、validate、activate。
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
| `rebuild_decision_policy_*` | 冻结 DecisionPolicy 的不可变 CAS 索引、激活历史与单例 active head；JSON 正文仍在 `rebuild_blobs` | `policy_hash`, `artifact_id`, `activation_id`, `previous_policy_hash` |
| `rebuild_lesson_*` | 学习演化生成的 Lesson 与隔离证据 | `rebuild_lesson_heads`, `rebuild_lesson_events` |

---

## 11. 跨模块不变量

- ContextManifest、ReadGrant 与工具结果使用一致的精确 source/kind/producer 规则；内部对象还要校验 Run、生命周期、lineage 和学习资格，禁止放开 `akzio.*` 通配权限。
- Outcome 原文授权可占用既有 128 KiB / 24 Artifact 沙箱（估算上限 32k tokens），模型两阶段及工具回读共用原 Run 冻结输入预算。projection v2 明确列出省略项；完整原文仍经原 Manifest/ReadGrant 获取，省略不代表为空或已验证。
- Canary 仅允许已登记的三个 Shadow 复用父 EvidenceGate 成功 Attempt 的冻结 NormalizedEvidence；collection status 由精确 snapshot producer 引用，禁止把 RawEvidence 或后续刷新当作授权材料。Shadow Outcome 的跨 Run 引用只取父 OutcomeSchedule 的冻结执行来源。
- Outcome Draft 最多使用总输出的一半和 70% 时限；必须先有已持久化 Draft memo 才能进入 Submit；首轮预算不足以完成两阶段时明确拒绝，不跳过 Draft。恢复与阶段切换不能重置预算。新 Contract 的完整 Schema、role Prompt 和 Rust 验证必须同步。
- 40 项证据逐项留状态：前置 EvidenceGate 将六项 ExecutionSafety 标记 deferred_to_execution 且不采集；ExecutionGate 原 refresh_execution_snapshots 即时获取并继续 fail closed。研究和增强数据失败保留成功项与缺口。新鲜度看内容可用时间，日线以交易所收盘（美东 DST/提前收盘）加 20 分钟余量判断；不以下载时间或 UTC 日期替代 Session。
- 每个 Outcome 是同 Run 的 post-terminal 工作；先检查未完成阶段再调用模型，迟到补跑按各阶段自己的 cutoff 截断事实。每 Outcome 租约单写，争用 Deferred；完成、报错及取消时按 epoch 释放租约。Deferred 不进入失败预算，已提交的阶段事件为下一阶段建立独立额度；Outcome 重试从 30 秒指数退避，最多 5 分钟。Worker 对 Session/Outcome 进行服务保留或单 Worker 交错。
- 执行后持仓按数量和现金重建，Receipt 去重并验证实际成交。Outcome 是冻结执行后敞口（metric v3），不代表真实后续账户 NAV；按当前阶段实际计价窗口检查公司行动，窗口内无调整账本则不可用，禁止推测调整因子。
- Decision 生产成本取依赖任务闭包；Outcome/叙事成本与生命周期总成本分开。Experience 保留 producer Run/Contracts/Workflow revision 与 evaluator Contract。行情完整、研究充分、叙事有效、风险真值已测量分别记录。缺失风险真值继续为 None，不得填满分。
- Rust-only T5 不取得学习资格。叙事修复在原 Run 上提交可追溯 revision，保留原数值 CAS；修复后复用密封 Outcome，经原资格检查幂等补评估；不得直接写入 Proven。LessonProposal 显式声明资产、horizon、排除条件与证据，只有 Draft/quarantine 权限。
- 当前版本：Domain schema 10（兼容可选字段）；Store 18；WorkflowDefinition 1；研究 Contract 69 / Prompt bundle 38 / freshness candidate 70；Outcome Contract 63 / Prompt bundle 35，metric `frozen_post_execution_exposure_v3`；evaluation context 1；benchmark definition 1。旧未知口径和 v2 数据不自动改标 v3。
- v14→v15 迁移只新增 Run/kind 表达式索引。旧 Contract 的未完成任务会在写新 Store 版本前阻断升级；旧任务哈希不改写，旧 CAS/Commitment 不改写。停止旧 worker 并合法处理旧任务之后再升级。RuntimeIdentity 与旧审批不自动复用。
- Canary 密封/多 subject 评估的中间进度保存在 CAS、事件和评估账本；只有随 Attempt 成功一并提交的产物进入 `rebuild_attempt_outputs`。等待或中断不撤回已完成 subject，不把未完成 Attempt 发布成成功输出。


每期限 Claim 与 Critique 的 grounds 上限为 12，四资产价格/新闻保持单资产范围，共享宏观可覆盖四资产，最低完整依据为 4+4+1。Context 先保证角色必需输入，再分配可选背景；必需集合放不下必须显式拒绝。冻结净收益按价格效应 + 有符号实施差额 + 初始估值差额 - 估算费用计算，limit_shortfall 只作诊断，不能重复扣除。

---

## 12. 分流程 Debug 控制

- Debug Core 必须使用新隔离 Store，禁止打开 `~/.akzio/store` 或启用 `auto_paper`。Store 的隔离标记不可通过重启成普通 Core 移除。
- 正式 Debug prepare 复用共享 Rust-owned research proposal。Paper 保留 40 项 EvidenceNeed 和 Session reservation；PositionPlan 只保留 34 项研究 Need，不占用 Paper session slot，在 Decision 后结束。调度权由 Store 的 DebugSession、revision CAS 和 claim 事务决定；App/CLI 不推断 readiness。
- Store 16 增加 Debug 控制表及 RunScoped DebugRecord CAS 类型；Store 17 增加 DecisionPolicy 安装、激活链和 active head。升级前必须停止旧 worker；旧 running task 或有效 daemon lease 会阻断升级。原 Contract、CAS、Commitment 不重写。
- Store 18 将 Debug 控制迁入唯一共享 RunControl head，增加结构化 NodeSpec 与 RunScoped RuntimeCheckpoint。queued/leased/running 任务与有效 daemon lease 继续阻断升级；历史 CAS 不回写。图预览、运行检查和恢复边界见 [Workflow Runtime](workflow-runtime.md)。
- Broker 写入默认 forbidden，在 Reconcile、Dispatch 和 Store effect intent 边界强制阻断；paper_allowed 仍需原审批和全部业务 Gate。
- `--paper` 使用正式 Paper 图与真实 Alpaca Paper API；旧 `-fakerOnline` / `--faker-online` 及本地模拟 Broker 已删除，历史 `simulated_only` 身份仅保留解码，禁止新执行。原始 `clock.is_open` 不改写，TradingSession 根据 Alpaca Clock、交易日历和美东时间确定；PreMarket / AfterHours / Overnight 发送 `limit + day + extended_hours=true`，Overnight 使用 BOATS 或 overnight 行情并检查当前资产资格。Policy、审批、风险、行情新鲜度与 Commitment 检查不放宽。
- Closed 时仅延期执行任务；恢复后重新获取 Account / Quote / Clock 并重跑 ExecutionGate，Decision 过期不允许补单。`accepted/new/partially_filled` 持久化为待成交进度，短轮询结束仍可后续 Reconcile；扩展时段不触发 Regular 的 60 秒撤改单逻辑。提交授权在时段边界到期，已提交订单的只读对账继续可用。启动器关闭 auto_paper、启用 Outcome worker 并保留隔离 Store；流程完成至 OutcomeSchedule 不代表成交或跨交易日评估完成。
- Outcome processing 独立于新 T0 调度，仍保留 Paper purpose 与原跨交易日算法。隔离 Debug 不写 canonical policy/Active Lesson。
- Debug Store 放在 `.akzio/` 内，避免 App 打包清理 `target/` 时丢失 checkpoint。打包验证默认保留构建产物并使用新 Bundle 路径，具体规则见 [开发 Workflow](development-workflow.md)。
- 控制命令、恢复与限制见 [分流程 Debug 基础设施](debug-control.md)。Fixture 控制器结果不能作为真实模型或 Paper 业务验收。

研究与执行边界：现有 `RunPurpose::PositionPlan` 表示 research + Decision / target position plan，无 ExecutionGate、PaperCommit、Reconcile 或 Evaluate；`RunPurpose::Paper` 使用同一 `approved_research_proposal` 三组 T1/T3/T5 Analyst/Critic 和 Synthesizer，再进入原执行链。没有新增 RunMode。Paper 的 account / positions / open_orders / fills / quotes / clock Need 保留 scheduler identity 与 provenance，前置只记录 Deferred，执行时才刷新。PositionPlan 执行显示 N/A，Debug manual/continuous、Broker policy、learning scope 仍是独立控制维度。补充研究保留冻结 Session 日期及原 future-data/cutoff 校验，不再要求市场当前开放。NewsWeb acquisition purpose mapping 和 policy hash 未改，缺失或未验证的方向证据不得变成有效 grounds。
