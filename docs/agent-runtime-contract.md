# Akzio 应用运行时契约

本文保存从根 `AGENTS.md` 分离的应用运行时约束，按原章节编号便于定位。它不是开发助手的工具白名单；开发 Workflow 见 [development-workflow.md](development-workflow.md)。修改相关模块前必须读取对应章节，业务保障不会因移动文档而解除。

版本、模型和预算描述属于项目契约，不跟随 Codex 开发模型自动更新。发现本文、代码与实际冻结 Contract 不一致时，应明确报告差异并查明适用版本；不得静默改写历史对象、降低 Gate 或切换模型来消除差异。历史验证只证明其记录时的状态。

## 当前研究与证据规则

活动版本和阶段见[研究提交与 Outcome 协议](#52-研究提交与-outcome-协议)。历史对象按原身份验证，退役范围和升级阻断见[研究协议与退役清单](research-protocol-retirement.md#历史兼容与执行阻断)。

非中性预测要求同资产、同期限、同方向的 Claim 支撑。非零多头研究配置要求对应看多依据、正预期收益和相关引用。显式现金仍合法。

研究输出显式声明 `EvidenceGap.retriable`。可重查的阻断缺口必须携带受控请求，补采和 Analyst/Critic 重跑额度跨恢复保持。补采无新增合格事实时保留缺口。正式 PositionPlan 缺 active Policy 时仍由 Rust DecisionGate 校验研究提案，保留合格的 `research_plan.validated`，正式目标为零。已安装 Policy 必须匹配当前 Synthesizer 模型和 Contract。

Alpaca 股票研究捕获先核验资产，保留市场时钟、交易日历、请求的 IEX/SIP feed、snapshot/quote/trade 及各自时间戳。允许省略 feed 的调整后研究日线使用供应商默认流，保留 feed 未知。原始 Outcome 日线和期权依赖的股票快照仍要求明确的股票 feed。日线在交易所收盘 20 分钟后才具备发布时间条件。期权捕获默认显式 indicative，不自动切换 OPRA；到期范围最多 30 天、行权价为截止前标的成交价（或完整日线）的 90%-110%、最多 4 页及 512 合约，缺 quote 时最多 6 次、每次 100 合约批量请求；明确权限拒绝后停止补采。按 OCC 合并 contracts 的 dated open interest；缺失保持 unknown。IV/Greeks 没有独立 provider 时间戳时明确为 unknown，不能借用 quote/retrieval 时间。覆盖率和截断状态随 NormalizedEvidence 投影保留；原始请求账本随 RawEvidence 保存。休市旧数据不标为实时。

`forecast_calendar` 冻结未来计划收盘，`bar_session_coverage` 保存本次日线请求范围的计划收盘、实际返回和缺失日期。模型紧凑投影省略这两个原始元数据字段。新版日线滚动特征使用最新一段连续的已证交易日。缺少所需交易日时相应统计量留空，252 日收益至少需要 253 根连续收盘价。

方向资格由同一个 Claim 的正式 price/macro grounds、精确资产/期限、对应 Critique 对这两类 grounds 的当前权威核验和无 slot blocker 共同决定；Context matrix、Submit 与 DecisionGate 共用该规则。Critic 新 evidence 和 deliberation 不能补齐 Claim。模型不填写 expiry/holding period，Rust 从四资产共同的 Alpaca 计划交易日历计算；日历元数据不构成未来价格事实。原 12 forecasts、allocation 守恒、引用闭包仍强制校验。阶段审计记录 provider、parse_validate、persist_stage 耗时，含 structured/draft/submit 标识及 revision。结构化修复显式记录原始调用引用、前后 result 哈希和差异路径；仅修复 deliberation 时切换为只含 deliberation 的修复 Schema，缩减为该元数据和校验错误上下文；Rust 从原始 AgentTurn 冻结并复用 result，模型不能再次提交 result。越权重写 result 会直接拒绝。恢复核对完整提交和元数据修复的各自工具哈希，累计预算保持不变。

正式 Shell 和原生 CLI 入口见[正式运行与历史 Debug 退役](#12-正式运行日志与历史-debug-退役)。

Context 对 Invesco JSON holdings 也应用精确权重排序和 12 行投影；期权保留完整聚合、时间与覆盖字段，样例缩为前两项并记录省略数。相同任务的 Retry/Recovery 只复用精确来源、相同 Contract 和相同内容的历史期权投影，仍签发当前 Attempt 的新 ReadGrant，不放宽恢复身份校验。研究角色的 Context 描述明确没有读取工具。

分享安全 bundle 支持经过同一脱敏器处理的 UTF-8 文本和完整 NDJSON 记录；无权限时不导出 RawEvidence 内的 provider 明细。搜索审计从已持久化的 discovery/reviewer provider 响应提取 action/source，不从文章、摘要或模型自述推断；缺失审计显示 unknown/null，搜索发生与来源核验仍是不同事实。研究完整性与导出完整性分别记录。

当前研究的 Claim/Critique wire grounds Schema 按 Rust 读取的 Manifest 证据资产范围绑定 `anyOf` 分支。单资产新闻只能声明对应资产，共享宏观仍可声明合法的多资产范围。按相同 scope 合并 ID，避免逐文档展开 Schema。Rust 在恢复 kind 和执行业务校验前验证同一个 bound wire Schema。

当前研究提交对未 source-verified 的 news 仅接受描述性 ground，不接受其 supporting_ref；source scope 和 citation 完整性不再代替来源验证。补采建议在提交时校验类型化业务意图，Rust 再绑定冻结资源和时间窗口，并使用与 adapter 相同的 GovernedResource 解析器。旧 CAS/Contract 不重写，未完成任务的升级阻断保留。

新闻复核仅规范化已列明的 utm 跟踪参数，不推断重定向或合并不同业务 URL；逐事实验证并记录 URL 绑定与失败原因，有效事实与失败项可同时保留，model_reviewed 不升级为 source_verified。Context 在必需闭包之后优先覆盖价格、新闻、宏观和事件日历，再分配期权背景；24 项及字节预算不扩张。研究投影明确告知角色只可使用已提供的材料。

导出将研究 AgentTurn、采集 provider 响应、hosted web 操作、含 action.sources 的操作以及来源复核失败分别计数。reasoning effort 优先读取 telemetry，否则读取已保存的实际 provider request；未保存不推断。上下文导出列出本包内未选中的证据，不将其解释为在当时 cutoff 已可用。数值依据是模型估计说明，不代表 Critic 已审查具体数值或 Policy 已校准。

## 1. 项目定位与核心红线

- **定位**：本仓库是 Rust 2021 edition (固定构建 Rust `1.96.0`) workspace，构建本地常驻、仅支持 Alpaca Paper 的多智能体量化投研系统（Multi-Agent Research System）。
- **标的范围**：系统限定可执行资产严格为 4 只美股 ETF：`TQQQ`、`QQQ`、`SOXX`、`SOXL`。
- **纯 v2 架构**：项目只维护 v2；严禁恢复旧 `orchestrator-*` crates、Phase 0–8、FileStore、旧 prompts 或 `outputs/store` 兼容路径。
- **物理绝缘实盘**：Live Trading 永不实现；`AlpacaPaper::new` 必须在发生任何实际 HTTP I/O 前强制校验并拒绝非 Paper endpoint。
- **生产模型协议**：当前唯一实现的生产模型协议是 **OpenAI Responses** (`openai_responses`)，必须声明模型发布日期 (`release_date`) 与知识截止时间 (`knowledge_cutoff`)；端点兼容不等于语义兼容。

---

## 2. 权威与数据边界

- **Rust 唯一权威**：Rust 是系统状态、授权、Task Contract、模型预算、Workflow Gates、持久化存储、学习迁移和执行策略的唯一权威。
- **统一 CAS 存储权威**由 `akzio-store` 提供，SQLite 位于 `<store_root>/akzio.sqlite3`。正式配置与仓库 fixture 的路径见[正式运行与历史 Debug 退役](#12-正式运行日志与历史-debug-退役)。严禁增加改变语义的并行 JSON 状态、文件缓存，或绕过 `akzio-store` 直接写 SQLite。
- **溯源保留**：Evidence、Claim、Critique、DecisionProposal、Decision、Execution、Outcome 和 Memory 必须完整保留 provenance、时间戳、版本与有效 `source_refs`。
- **受控上下文沙箱**：`akzio-context` 是 Agent 任务获取投研资料的唯一通道。模型代码严禁获取任意本地文件系统、Raw Evidence、底层 SQLite 或实盘/交易凭据的访问权。
- **Git 干净边界**：生成的 Store Root、BLOB、socket、报告、认证凭据 (`.daemon-token`) 和本地配置覆盖不得提交至 Git。
- **不可随意破坏不变量**：重构或存储演进严禁隐式修改 schema_version、`ExecutionPlan` 序列化哈希、Paper Gate 风控逻辑或 Transaction 事务边界。

---

## 3. 双并行时间轴架构 (Dual Timeline Invariant)

系统在时间维度上严格解耦为两条并行运转的时间轴，**绝非串行阻塞等待**：

```text
时间轴 A (交易日 Session T0)：
  Paper 调度器按实际可交易 Session 排他预约。启用自动调度或收到正式手动请求时，可创建或复用该 Session 的 Paper Run。
  [EvidenceGate] -> [Analyst/Critic × 3] -> [Supplement + affected reruns] -> [Synthesizer/ProposalReviewer] -> [DecisionGate] -> [ExecutionGate] -> [PaperCommit] -> [Reconcile] -> [Evaluate] -> 创建 [OutcomeSchedule]

时间轴 B (跨交易日 T+1 / T+3 / T+5 评估)：
  Paper Run 的 Evaluate 产生 OutcomeSchedule 后，同一 Run 的后续阶段按各自交易日窗口处理：
  - T+1 交易日：计算第一窗口收益与指标，生成 T1 阶段复盘 (Partial Outcome, RunScoped)
  - T+3 交易日：计算第二窗口收益与指标，生成 T3 阶段复盘 (Partial Outcome, RunScoped)
  - T+5 交易日：计算第三窗口收益，密封最终 Outcome (Sealed)，生成 T5 复盘；只有研究、风险真值和叙事均合格时才允许学习评估
```

- **并行推进机制**：在交易日 Session $S_n$ 中，系统可以同时执行当天 Run 的 T0 投研，以及历史 Run A 的 T+5 评估、Run B 的 T+3 评估和 Run C 的 T+1 评估。
- **交易日对齐**。带冻结日历的 Run 按四资产一致的交易所日历取 T0 严格之后第 1、3、5 个计划 Session。Paper 的 T0 取持久化 Session slot，包括隔夜场次。PositionPlan 取 EvidenceGate 的期权 Need 冻结日期，可能是周末。Shadow 核对 canary 预约与父 Paper slot 的一致性。预测到期为对应 Session 的精确 UTC 收盘，包含提前收盘及美东 DST。
- **Outcome 窗口**。Outcome 沿 `OutcomeSchedule`、`DecisionContext` 和已接受的 `ContextManifest` 读取同一日历。日历 v1 的发布时间固定为交易所收盘加 20 分钟。每个窗口须等到该时刻，并要求截至该窗口的每个计划 Session 均有四资产日线，日线收盘时间须与冻结值一致。原定日缺数据时继续等待该日，后续交易日不能补位。无日历标记的历史 Outcome 保留原有读取和阶段采集口径。

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

| 阶段或角色 | 职责 | 配置模型路由 | 新 Run 默认 Task input / output / Attempt 时限 | 核心产物 |
|---|---|---|---|---|
| **T0 编译** | Rust 编译固定研究拓扑 | — | — | `WorkflowProposal` |
| **Analyst** | 按期限评估证据并声明主张和缺口 | `research.analyst` | 1M / 1M / 180s | `Claim` |
| **Critic** | 独立核验对应 Analyst 的 Claim | `research.critic` | 1M / 1M / 180s | `Critique` |
| **Synthesizer** | 综合有效证据和审查，生成 12 项预测及四资产和现金的分配提案 | `research.synthesizer` | 1M / 1M / 180s | `DecisionProposal` |
| **ProposalReviewer** | 独立审查每份提案的数值依据和分配 | 继承 Critic 的有效路由 | 继承 Critic（默认 1M / 1M / 180s） | `ProposalReview` |
| **Outcome Worker** | 在 Paper 的 T+1/T+3/T+5 阶段产出定性复盘 | `learning.outcome_worker` | 1M / 4k / 180s | `RetrospectiveDraft` |

Core 按角色读取 `[model.routes]`，未配置该角色时使用 `[model]`。ProposalReviewer 使用 Critic 的有效模型和 reasoning 配置。仓库的 fixture 示例和正式配置模板都声明 `gpt-5.6-luna`、`reasoning_effort = "low"`，且未设置角色路由。实例实际路由以解析后的配置为准，CLI 支持 `AKZIO_MODEL_ROUTES_JSON` 覆盖路由表。参见 [CLI 模型配置解析](../crates/akzio-cli/src/cli/identity.rs)、[Core 路由选择](../crates/akzio-daemon/src/lib.rs)、[fixture 示例](../config/akzio.toml)和[正式配置模板](../config/akzio.observatory.toml)。

上表是新 Run 在配置覆盖前的有效预算。运行时对每个字段按角色覆盖、全局覆盖、代码默认值的优先级解析，再冻结到图和任务。ProposalReviewer 继承 Critic 的解析后预算。默认只读工具次数为 `unlimited`，实际工具首先由 Contract 和阶段授权。Token 按 Task 累计，180s 是单 Attempt 墙钟。参见 [Agent Budget 配置](agent-budget-configuration.md#冻结与执行)。

活动 Contract 对象本身序列化 `versioned_contract_budget`。Analyst、Critic 和 ProposalReviewer 是 48k input / 1M output / 4 次 / 120s，Synthesizer 是 48k / 1M / 2 次 / 120s，Outcome Worker 是 12k / 4k / 2 次 / 180s。该字段用于冻结 Contract 身份。新任务按 `default_agent_budget` 与配置覆盖解析有效预算，研究角色即使有次数额度也只提供 `submit_result`。参见[预算定义与解析](../crates/akzio-domain/src/budget.rs)。

Paper 与 PositionPlan 的 T0 先并行运行三个 T1/T3/T5 Analyst/Critic 对。T1/T3/T5 使用第 3 节的交易 Session 契约定义，每个 Claim 只覆盖一个期限。Synthesizer 依赖有效 Claim 与 Critic 路径，Critic 的 `NoOutput` 仍须保留 Claim 血缘。随后由 ProposalReviewer 审查，Rust DecisionGate 消费通过审查的提案。

### 5.1 有界补采与终稿审查

新冻结图包含首轮三个 Analyst/Critic 对、一个 Rust `research.supplement` 协调节点、三个可跳过的重跑对以及 N+1 个 Synthesizer/ProposalReviewer 对。`[agent.research] max_proposal_revisions = 2` 表示初稿外最多两次修订；0 只审初稿。PositionPlan 节点数为 `17 + 2N`，Paper 为 `21 + 2N`，默认分别为 21、25。配置拒绝负数、非整数及超过 32 节点的值（当前 N 最大 5），冻结后改配置不改变已有图或额度；未触发节点明确为 skipped。

Analyst/Critic 提交 news、price、macro 类型化意图、资产或受支持序列及目的，Rust 从 Run 冻结的 EvidenceNeed 绑定资源、窗口和 cutoff。只有可重试的实质性方向阻断缺口进入全 Run 一轮、最多八个去重资源的补采；四资产新闻展开为四条。请求开始在 I/O 前持久化；恢复保留已耗额度，未知完成状态不重发。处置记录包含合并、影响不足、额度耗尽、非法协议、采集失败和无新增事实。只有新增合格事实才重跑受影响期限的 Analyst/Critic 各一次；Rust 选择有效 revision，不混用原结论，不开启第二轮。

每个提案修订节点单独冻结任务预算。ProposalReviewer 的输入包含完整提案、有效 Claim/Critique、numeric_basis 和关键反证。numeric_basis 恰好覆盖 12 个 forecast、四资产与现金共 17 项，说明输入、单位、方法、假设与不确定性；配置说明仓位、重叠风险与现金理由。ProposalReview 逐项通过/拒绝并给出原因，由 Rust 绑定精确 proposal Artifact、内容哈希、Manifest 和 Contract；任何内容变化都需新 Review。拒绝意见进入下一次 Synthesizer；通过后跳过剩余修订。耗尽、上下文不足或审查失败明确阻断 Decision。结构化格式修复与业务提案修订独立计数，恢复不重置预算。

Context 的必需集合包括有效 Claim/Critique、已审 grounds、numeric_basis 引用和反证，先于可选背景；24 项上限不变，关键材料放不下即报覆盖缺口。创建 Manifest 时持久化候选覆盖记录；导出时才出现的证据只能标记存在，不能回推历史可用性。Rust 输出研究进度、审查状态、revision 与 Decision 阻断；脚本与 Observer 只读接口展示该投影，workflow 原状态保留。审查通过只证明依据与推理被审查，SQL active Policy、适配和风险 Gate 独立生效，不生成或自动激活 Policy。Outcome 协议与历史哈希不变。

Contract 69 的拒绝 assessment 必须有 1–3 个类型化 issues；通过项 issues 为空。Rust 生成稳定问题 ID，冻结已通过 forecast 的模型字段及 numeric_basis，保留配置联动和 Rust 日历绑定。两次拒绝的问题、引用及被拒绝内容完全不变时停止剩余修订，保留拒绝 Review 并阻断 Decision。Lesson 召回先扫描同一 SQL 快照的全部 Active heads 再筛选排序，四条上限内保留完整显式冲突组；来源更新只生成 RunScoped 重验建议。具体审计和实验入口见[研究质量验证](research-quality.md)。

### 5.2 研究提交与 Outcome 协议

运行时提示词按功能所有权维护，统一索引见 [提示词所有权与验证](prompt-ownership.md)。治理与角色仍通过冻结 PromptBundle/CAS 读取；动态 Schema、权限、预算、引用绑定及 Gate 由 Rust 决定。

活动研究 Contract **69** / PromptBundle **38** 覆盖 Analyst、Critic、Synthesizer 和 ProposalReviewer。Paper、PositionPlan、Shadow 的四个研究角色从 Submit 开始，使用授权投影，通过 `submit_result` 提交 `result + deliberation`。请求只提供提交工具。Rust 校验方向资格、引用闭包和补采约束，绑定交易日历并处理仅限 deliberation 的修复。Analyst freshness candidate 的 Contract 和 PromptBundle 均为 **70**。指定该候选的 Shadow 只替换 Analyst 的冻结 Contract 引用。Analyst 事实候选使用 Contract **72**，沿用 PromptBundle **38**。Rust 验证非空事实来自实际呈现、满足原 cutoff 的授权证据。无法证明时保留 `fact: null`。Synthesizer Lesson 候选使用 Contract / PromptBundle **71**。这两个候选均须显式安装并由运行冻结候选哈希，活动研究版本仍为 **69** / **38**。参见[活动目录及候选](../crates/akzio-research/src/agent/catalogue.rs)和[阶段选择](../crates/akzio-research/src/agent/runtime_run.rs)。

经接受的新 Synthesizer 提案通过 `accepted_synthesizer_submit` 记录原 `result` 的 Turn、最终接受的 Submit Turn 及对应哈希。仅修复 deliberation 时，两者可以来自不同 Turn。可取得的已配置 Responses 路由、端点摘要和 provider 回报的模型名随完成的调用保存，用于核对该次调用。provider 侧不可变版本和资格仍需独立证明，校准沿用既有 provider、model 与 Contract 过滤。

Outcome Worker 使用冻结的 Contract **63** / PromptBundle **35**。Draft 可按授权读取上下文，Rust 持久化 Draft memo 后进入 Submit。只读 ToolCall 在执行前持久化，读取结果或受控失败可随后持久化为 ToolResult。恢复复用冻结的预算和阶段约束。Outcome Draft 使用配置的 reasoning，Submit 请求的 reasoning 由运行时设为 `low`。参见 [Outcome Contract 注册](../crates/akzio-research/src/agent/errors_catalogue.rs)。

历史上的 PositionPlan 专用结构化路径、旧研究 Draft 和 Contract 49 引入的输出预算规则按各自原版本解读。原冻结正文、预算、CAS 和哈希保持其历史身份。Planner 的活动 Contract、配方及扩图能力已退休，旧 purpose 和 Artifact 类型仅供历史解码、展示、导出和完整性审计。正式 Worker 不执行退役任务。现存 `run retry` 受退役检查约束，旧独立 Debug 的 resume、step 和 fork 入口已移除。升级预检列出未完成任务及有效 lease，保留原阻断。参见[历史兼容与执行阻断](research-protocol-retirement.md#历史兼容与执行阻断)和[输出与单请求边界](agent-budget-configuration.md#输出与单请求边界)。

### 5.3 严格受控的 5 个只读工具

只有 Outcome Draft 在剩余工具额度内可使用以下五个只读 Context 工具。每次读取须通过当前 Attempt 的 `ReadGrant`、Contract 和 Manifest 校验。Agent 权限严格限定于协议所列工具，禁止通用 `web_search`、`shell`、`http_request`、`file_read`、`sql` 和交易执行权限。参见[工具 Schema](../crates/akzio-research/src/agent/schemas.rs)和 [Context 读取验证](../crates/akzio-context/src/context_broker/reads.rs)。

1. `read_document(artifact_id)` 读取已授权的完整文档。超过 32 KiB 时要求改用 `read_range`。
2. `read_range(artifact_id, start_byte, end_byte)` 读取已授权文档的半开字节区间 `[start_byte, end_byte)`，单次最多 32 KiB，区间须落在有效 UTF-8 边界上。
3. `search_context(query, max_results)` 在当前 Manifest 所选文档中做大小写不敏感的子串匹配。查询为 1–256 个字符，结果数为 1–16。
4. `read_claim_evidence(artifact_id)` 读取已授权的 Claim 及其 Grounds 中已授权的规范化证据。`RawEvidence` 不在授权范围内。
5. `compare_sources(artifact_ids)` 并列返回 2–4 个不同的已授权来源的时间与受控投影。

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

- **40 项固化证据需求**：Rust 在每个 Paper Session 确定性生成 40 个唯一 `EvidenceNeed`（包含 Broker 账户/持仓/报单状态、四资产向前 400 天、每资产请求至多 252 根日线、14 天 `news:*`、FRED 宏观序列 `DFF`/`DFII10`/`VIXCLS`、杠杆 ETF 损耗条款等）。
- **新闻与发行方证据的采集模式**由 Rust 的 `evidence_acquisition_mode` 决定。`news:*` 使用 `ModelReviewed`。发行方直采的 `research:*` 使用 `VerifiedSource`，其中 `research:earnings_event_calendar:*` 仍使用 `ModelReviewed`。参见[采集策略](../crates/akzio-domain/src/workflow.rs)和[新闻证据](news-evidence.md)。
- **Context 限制与隔离**：
  - 每个 Agent 的 `ContextManifest` 顶层上限为 **24 个 Artifact**。
  - 普通 Agent 上下文限制 128 KiB，Synthesizer 限制 192 KiB。
  - 严禁向模型注入 `RawEvidence`，必须经过 Rust 标准化为 `NormalizedEvidence`。
- **Lesson 查询范围**：`ContextQueryScope` 由 Rust 根据已核验的冻结节点生成；资产为当前四 ETF 研究全集，期限取 `NodeSpec`，Synthesizer / ProposalReviewer 显式覆盖 T1/T3/T5，阶段使用完整 recipe ID。查询维度为空表示未知，只匹配该维度未限定的 Lesson，不表示任意匹配。市场状态只取通过原来源、Contract、Run 和类型校验的 DecisionTime RegimeSnapshot；正文字符串、标签与 ExPost 快照不能提供范围。实际召回仍受原 Contract 限制，当前研究链只有 Synthesizer 允许 Lesson。历史节点复用既有只读 NodeSpec 适配器，不改写历史 Artifact；子任务沿用父 Manifest 的权限收缩，不额外扩大召回。最终查询范围保存在 `learning.retrieval.audit` 中。v71 候选以原始 `result` 和已接受 Submit 的证据核验归因。只有预算内完整下发的 Lesson 可以计入，只有元数据的条目不计。

---

## 7. Rust 决策双闸门与执行边界

```text
DecisionProposal (LLM: Forecast + research_allocation)
       ↓ 
[DecisionGate (Rust)]    -> 校验研究分配并记录 raw→validated；另行校验校准/风险 -> 产出 Decision / DecisionContext
       ↓ 
[ExecutionGate (Rust)]   -> 校验账户资金、最新报价、流动性、审批令牌 -> 产出 ExecutionVerdict
       ↓ 
[PaperCommitment (Rust)] -> 持久化 Commitment 和确定性 client_order_id -> Store 逐单提交账本 -> Alpaca Paper API -> Reconciliation
```

1. **DecisionGate 默认 Fail-Closed**：
   - 若 Store 没有已激活且通过验证的 `DecisionPolicy`，Rust 默认校准样本为零，**目标组合头寸强制为 0**。canonical Store 数据库不存在时由 Rust Store 初始化 schema，初始化不会生成或激活 policy；已有 canonical Store 在 PositionPlan 引导阶段只读打开。运行时只读取 SQL Store 中的 CAS Artifact、激活历史和 active head；旧文件路径兼容字段与 JSON 文件导入链路已移除。operator 将风险限制写入 SQL，collect 从 canonical Outcome 生成 SQL dataset，build 生成 SQL 候选 policy；inspect、validate、activate 均按 Store Artifact ID 操作，生成候选不会自动更新 active head。
   - 这只阻断执行侧 `Decision.targets`；有合格证据的非零 `research_plan.validated` 必须保留，并明确 `execution_status=blocked` 或 PositionPlan 的 `not_applicable`，不得冒充可执行目标。
   - `calibration collect` 只接受非隔离 canonical Store 的实际 Paper Decision/Outcome，并要求配置 Synthesizer 模型发布日期、知识截止日期及显式风险限制。带 `--cohort`、训练范围或 `--contract-hash` 的 `calibration readiness` 与 `collect` 共享同一候选过滤。默认选择活动 Synthesizer Contract，可用 `--contract-hash` 指定已安装的 Synthesizer Contract。`--training-start` 与 `--training-end` 接受 RFC3339。Decision 必须在窗口内，每个已证标签的实际可用时间必须不晚于训练截止，两个边界均包含等值。查询按 Decision 时间和 Artifact ID 翻页，扫描截断时 `collect` 阻断 Dataset 写入。无范围的 readiness 报告成熟度和缺口。`isolated_debug_store` 的运行不计入样本。
   - `venue_calendar_and_observed_bars_v1` 要求目标日线的实际收件不早于冻结 Session 收盘加 20 分钟。合格标签时间取该发布门槛与四资产基准和目标日线实际收件时间中的最大值。早期部分 Outcome 只有在同一 Schedule、同一窗口及四资产两端价格均与密封结果一致时，才能证明更早的可用时间。未指定训练截止时，`collect` 取入选标签的最晚时间。无日历标记的旧 Outcome 可读取，新增校准报告标记其 `label_timing_unknown`。
   - 风险样本使用同一已知 feed 的原始价格。每只资产都须证明两根日线属于同一请求覆盖中的相邻计划 Session，两端收盘加 20 分钟和四资产收件时间均须在训练截止内。缺失计划 Session 时该日两侧不拼接跨日收益。风险面按这些相邻收益估计，`build` 再校验冻结日历与收件时间。成熟度报告本身不授予 Dataset 或 Policy 资格。
   - 只有通过原审批和所有 ExecutionGate blocker 检查后才进入 Allocator：当前为空仓时产生 `NoExecutableOrder`，已有多头仓位时才可能生成归零卖单；缺 approval 的冷启动不进入 Allocator，也不会触发清仓。
2. **ExecutionGate 与幂等 Commitment**：
   - 正式 scheduler 在 SQL 没有 active policy 时创建不绑定 approval 的 canonical Paper run，以积累未来真实校准标签；已有 active policy 时仍要求原审批。未校准 Paper 冷启动没有 approval 时，pre-trade safety 返回无评估，不作安全断言，也不以缺 manifest 抛错阻断 verdict。原 `UnqualifiedRuntime` blocker 保留，跳过 allocation/plan closure，形成持久化 `NoOrder`；PaperCommit/Reconcile 在获取执行 lease 或访问 Broker 前短路。Evaluate 仍可生成 `OutcomeExecutionLineage::NoOrder` 的 OutcomeSchedule，无需 ExecutionCommitment。此语义不扩张任何执行权限；真实密封仍要求完整 baseline 与四资产共同完成的真实 T+1/T+3/T+5 Session，policy 必须由 operator 显式 inspect、validate、activate。
   - `Accepted` 后先将带确定性 `client_order_id` 的 canonical `ExecutionCommitment` 绑定原 Run / Session。Store 在记录 effect intent 的同一事务登记原始订单 ID；获准的替换 ID 随原替换 effect intent 登记。每个 ID 只有在带 lease 和 Attempt 校验的 SQLite 事务提交 `Ready → PossiblySubmitted` 后，才可发起一次对应的 Alpaca Paper POST 或 PATCH。实际回执按 ID 持久化后才推进下一笔写入。
   - 恢复发现 `PossiblySubmitted` 时，停止整批新增提交和替换，按原账户和已登记 ID 查询。404 或暂不可采纳的券商读回保持待观察，保留已核验回执并继续查询同一 ID。已记录的保护性取消仍须通过原审批及回执检查。只有仍处于 `Ready` 的 ID 可以在持久化 Run 取消、冻结提交上界已过，或批次因其他 ID 状态不明停止提交时，记录 `ProvenUnsubmitted`。暂时休市时保留待定状态。
3. **无订单仍须评估**：
   - 即使 ExecutionGate 产出 `NoOrder`，系统的预测意图仍由 `Evaluate` 生成 `OutcomeSchedule`，并在后续 T+1/T+3/T+5 持续跟踪其实际市场表现。

---

## 8. 统一 CAS 存储映射规范

业务产物统一保存至内容寻址存储 (CAS)。SQL 表保存调度、审批、执行状态与 CAS 引用。

| 存储表名 | 存储内容与职责 | 关键字段 / 索引 |
|---|---|---|
| `rebuild_blobs` | 存储所有 JSON 强类型负载（Zstd 压缩） | `blob_hash`, `logical_bytes`, `stored_bytes`, `payload` |
| `rebuild_artifacts` | 统一 Artifact 元数据与生命周期 | `artifact_id`, `kind`, `blob_hash`, `lifecycle`, `provenance_json` |
| `rebuild_artifact_refs` | Artifact 之间的全量 DAG 依赖血缘关系 | `artifact_id`, `source_artifact_id`, `source_kind` |
| `rebuild_runs` | Run 生命周期与图拓扑元信息 | `run_id`, `purpose`, `topology_id`, `graph_artifact_id`, `status` |
| `rebuild_tasks` | Task 调度、Contract 校验与 Lease 租约 | `task_id`, `run_id`, `recipe_id`, `contract_hash`, `budget_json`, `retry_json`, `node_spec_json`, `status`, `lease_id`, `lease_epoch`, `lease_until` |
| `rebuild_attempts` | 任务单次尝试的执行状态与重试记录 | `attempt_id`, `task_id`, `status`, `error_json` |
| `rebuild_attempt_outputs`| 成功 Attempt 产生的正式输出映射 | `attempt_id`, `artifact_id` |
| `rebuild_session_slots` | Paper 交易 Session 的排他性原子插槽 | `session_key`, `run_id`, `commitment_artifact_id` |
| `rebuild_paper_submission_batches`, `rebuild_paper_submission_transitions` | 绑定 Commitment 和 effect intent，逐 ID 追加状态、实际回执的 CAS 引用与未提交依据 | `commitment_artifact_id`, `client_order_id`, `transition_index`, `receipt_artifact_id` |
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
- 有逐单账本的正式 `Reconciliation` 完成时，Store 校验同一 Commitment 的每个原始及已登记替换 ID 都有实际终态回执或可核验的 `ProvenUnsubmitted` 记录。已存在替换单时，原单的最新回执须独立确认终态或 `Replaced`，再按两单的原始回执合并成交。未提交记录只证明对账覆盖。执行后持仓按已验证并去重的实际成交数量和现金重建，成交成本也取实际成交。Outcome 是冻结执行后敞口（metric v3），不代表真实后续账户 NAV；按当前阶段实际计价窗口检查公司行动，窗口内无调整账本则不可用，禁止推测调整因子。
- Decision 生产成本取依赖任务闭包；Outcome/叙事成本与生命周期总成本分开。Experience 保留 producer Run/Contracts/Workflow revision 与 evaluator Contract。行情完整、研究充分、叙事有效（结构合格的 T5 Retrospective，不做独立语义质量评分）、风险真值已测量分别记录。缺失风险真值继续为 None，不得填满分。
- Rust-only T5 不取得学习资格。叙事修复在原 Run 上提交可追溯 revision，保留原数值 CAS；修复后复用密封 Outcome，经原资格检查幂等补评估；不得直接写入 Proven。LessonProposal 显式声明资产、horizon、排除条件与证据，只有 Draft/quarantine 权限。恢复可从已提交 Evaluation 幂等补写缺失 Draft。单条冲突保留原记录，同批其他合格 Draft 可以继续。
- 当前版本包括 Domain schema 10（兼容可选字段）、Store 19、WorkflowDefinition 1、evaluation context 1 和 benchmark definition 1。研究与 Outcome Contract 见[阶段协议](#52-研究提交与-outcome-协议)。Outcome metric 为 `frozen_post_execution_exposure_v3`，旧未知口径和 v2 数据保留各自版本。Store 升级见[开发 Workflow](development-workflow.md#store-18--19-升级)。
- v14→v15 迁移只新增 Run/kind 表达式索引。旧 Contract 的未完成任务会在写新 Store 版本前阻断升级；旧任务哈希不改写，旧 CAS/Commitment 不改写。停止旧 worker 并合法处理旧任务之后再升级。RuntimeIdentity 与旧审批不自动复用。
- Canary 密封/多 subject 评估的中间进度保存在 CAS、事件和评估账本；只有随 Attempt 成功一并提交的产物进入 `rebuild_attempt_outputs`。等待或中断不撤回已完成 subject，不把未完成 Attempt 发布成成功输出。


每期限 Claim 与 Critique 的 grounds 上限为 12，四资产价格/新闻保持单资产范围，共享宏观可覆盖四资产，最低完整依据为 4+4+1。Context 先保证角色必需输入，再分配可选背景；必需集合放不下必须显式拒绝。冻结净收益按价格效应 + 有符号实施差额 + 初始估值差额 - 估算费用计算，limit_shortfall 只作诊断，不能重复扣除。

---

## 12. 正式运行日志与历史 Debug 退役

- 独立 Debug Core、CLI/App 控制与可操作 fixture 服务已退役。Store 16–18 的历史身份、事件、RunControl 与 checkpoint 继续读取和验证；旧隔离标记不可移除，旧 Run 不得恢复执行或升级成 canonical 校准/学习样本。旧有效 lease 与 Contract 升级阻断保持。
- Shell 入口为 `bash scripts/run_core.sh [--config FILE] position-plan|paper`。默认配置为 `~/.akzio/config.toml`，`--config FILE` 放在模式前，相对路径按调用者目录解析。脚本通过 `cargo run --locked -p akzio-cli` 调用当前源码的 `akzio --config FILE run start --purpose position-plan|paper --auto-serve`。它先核验已有 canonical Core，只有在所选回环端口明确未监听时才可能用同一配置启动 `daemon serve` 并等候 ready。正式提交只发一次 `POST /runs`，返回 Run ID 表示提交或复用已被确认。`unknown_after_post` 须先查原提交。PositionPlan 按原提交给出的候选 Run ID 查询，Paper 可通过 `akzio run submission REQUEST_ID` 查询绑定的 Session 和 Run。见[正式运行与 Core 生命周期](development-workflow.md#4-正式运行与-core-生命周期)。
- 原生 `run`、`daemon`、`store` 的 `--config` 位于子命令前。原生 CLI 默认配置路径为相对路径 `config/akzio.toml`，对应仓库中使用 `.akzio/store` 的 fixture 示例。正式命令显式指定正式配置，例如 `akzio --config "$HOME/.akzio/config.toml" run detail RUN_ID`。正式客户端要求绝对 `daemon.store_root`，常用位置为 `$HOME/.akzio/store`，认证 token 位于该 Store Root 下。`run detail` 查看 Observer 投影，`run replay` 读取持久化过程，`store doctor` 检查 Store，`store backup TARGET` 和 `store restore SOURCE TARGET` 处理备份恢复。见[原生命令定义](../crates/akzio-cli/src/main.rs)和[正式客户端配置验证](../crates/akzio-cli/src/formal_run.rs)。
- Paper 沿用正式审批和 Gate。`paper` 仍使用真实 Alpaca Paper API，非 Paper endpoint 在 HTTP I/O 前拒绝。TradingSession 使用真实 Clock/交易日历；Closed 延期后刷新账户、报价和 Clock 并重跑 Gate；`accepted` 和 `new` 表示订单已受理。`partially_filled` 的已核验成交量计入对账，其余数量仍需继续观察或取得终态证明。
- 自动启动的 Core 进程日志在 Store Root 的同级 `logs/core-*.log`，常用路径为 `~/.akzio/logs/`。正式阶段日志则在 `~/.akzio/log/YYYYMMDD/` 发布不可覆盖的私有 ZIP。PositionPlan 和 Paper 都归档 T0，Paper 还按各已提交的 T1/T3/T5 阶段分别归档，启动阶段的模型探测有独立 ZIP。运行状态以 Store 为准，ZIP 用于私有审计。`run export-bundle RUN_ID /NEW/ABS/DIR` 可生成分享安全快照目录，独立于私有阶段 ZIP 和 Store 备份。见[运行日志](run-log.md)和[正式运行与 Core 生命周期](development-workflow.md#4-正式运行与-core-生命周期)。
- 日志写入/归档异常独立于 provider 和业务错误；阻止新模型调用、订单及改价，保留已记录 Paper effect 的只读对账及受控保护性撤单。持久业务成功不因 ZIP 失败改写，未知外部调用不盲重发或重置预算。详见[运行日志](run-log.md)和[历史 Debug 退役说明](debug-control.md)。旧 Shell 的 `--research-only`、`--keep-artifacts`、Python 自动等待与自动 ZIP 属于已退役入口。

研究与执行边界：现有 `RunPurpose::PositionPlan` 表示 research + Decision / target position plan，无 ExecutionGate、PaperCommit、Reconcile 或 Evaluate；`RunPurpose::Paper` 使用同一 `approved_research_proposal`，由 Analyst、Critic、Synthesizer 和 ProposalReviewer 完成研究，再进入执行链。通过 Evaluate 创建 OutcomeSchedule 后，Outcome Worker 在同一 Run 的后续阶段复盘。Shadow 是实验编译用途，其研究角色使用相同提交协议。Paper 的 account / positions / open_orders / fills / quotes / clock Need 保留 scheduler identity 与 provenance，前置只记录 Deferred，执行时才刷新。PositionPlan 执行显示 N/A；旧 Debug control 和 Broker policy 仅供历史身份读取。补充研究保留冻结 Session 日期及原 future-data/cutoff 校验，不再要求市场当前开放。NewsWeb acquisition purpose mapping 和 policy hash 未改，缺失或未验证的方向证据不得变成有效 grounds。
