# Akzio v2 Goal 外部一手资料研究与 R0-R10 映射

日期：2026-08-09
用途：为 Akzio v2 最大力度重构的 Goal/执行计划提供外部一手资料依据，不替代仓库内的原始计划、计划-续、`AGENTS.md` 或当前源码与测试事实。

## 1. 范围、来源纪律与结论边界

- 用户补充计划点名的是主题，没有在当前可访问文本中保留可核对的外部 URL。本文因此按主题映射 OpenAI 官方文章、官方文档和官方开源仓库；这些是研究者选定的官方来源，不应描述成原文件逐字列出的链接。
- 未打开 `AKZIO_V2_REFACTOR_HANDOFF.md` 中的 ChatGPT 对话链接。
- 只使用第一方资料：OpenAI 官方网站、OpenAI Developers 文档和 `openai/*` 官方 GitHub 仓库。
- 所有外部来源访问日期均为 **2026-08-09**。网页和 `main` 分支会变化；真正实施依赖某项接口或规范前，应记录所用 SDK/API 版本或固定源码提交。
- 外部文章描述的是通用 Codex/Agents 产品或 OpenAI 自身工程实践。它们提供设计原则，不自动覆盖 Akzio 的本地常驻、Rust-owned policy、`V2Store`、Context Broker、Paper-only 和 canonical-learning 约束。

## 2. 总体判断

七个主题共同支持一个清晰边界：**模型负责提出受约束的下一步，Rust harness 负责验证和执行，`V2Store` 负责耐久事实。**

对 Akzio v2 应采用的核心结构是：

1. 模型 turn 是暂态计算，不是系统权威；任何 tool request、artifact、task transition、Decision、Execution、Outcome 和 learning transition 都必须经过 Rust policy 并进入统一耐久事件流。
2. Context 不是“把更多东西塞进 prompt”，而是由 Context Broker 编译出来的最小、可追溯、可授权视图。`ContextManifest` 是地图，`ReadGrant` 是能力票据；二者都不能成为任意文件系统或 Raw Evidence 后门。
3. Provider SDK、Responses WebSocket、`previous_response_id` 和容器工作目录都只能是可替换的执行/传输机制，不能成为 durable state、recovery 或 replay 的来源。
4. 自我改进必须是 outcome-backed、候选化、可评测、可回滚的状态机；模型不能自行扩大数据源、工具、broker 或执行权限，也不能自行晋升候选。
5. 常驻 orchestration 需要 bounded concurrency、lease/epoch fencing、stall recovery、reconciliation 和可观察性；但 Akzio 的控制面必须是 `V2Store + Daemon + loopback HTTP/SSE`，不能把 Linear、Git worktree、shell hook 或外部服务升级为业务权威。

## 3. 来源研究

### 3.1 Harness Engineering

**来源**

- 标题：Harness engineering: leveraging Codex in an agent-first world
- 发布者：OpenAI
- 发布日期：2026-02-11
- URL：https://openai.com/index/harness-engineering/
- 访问日期：2026-08-09

**官方资料的关键结论**

- 大规模 agentic 工程的瓶颈从“写代码”转向环境设计、意图表达和反馈回路设计。
- 仓库内文档、结构化计划、架构规则、机械约束、运行时可观察性和能由 agent 自己复现实验的环境，构成 harness。
- 文档应是分层地图而非一个巨大说明文件；关键规则应尽可能由 linter、测试和结构检查机械执行。
- 让 agent 能看到本地应用状态、日志、指标和 traces，会显著缩短验证回路。
- 文中团队的具体实践服务于其内部开发场景；文章也明确承认合并策略、技术债和吞吐优化之间存在取舍。

**Akzio v2 可落地原则**

- 把 `AGENTS.md`、R0-R10 plan、crate ownership、领域不变量和删除图做成分层、可发现的 repo-local system of record。
- 将边界写成可执行的检查，而不是只写在提示词里：例如禁止非 `akzio-store` SQLite 写入、禁止 Agent 任意文件/网络访问、禁止非 Paper endpoint、禁止 noncanonical learning promotion。
- 为每个 R 阶段维护明确的验收命令、失败证据、fixture、Doctor 检查和 durable event 断言；“静态可编译”与“运行时正确”分开表述。
- 让本地 Daemon、fixture harness、Store Doctor、事件流、metrics 和 replay 对 Codex/维护者可观察，但观测入口不得绕开生产权限模型。
- R0 的计划文档应作为实现输入；真正冻结到每个 Paper session slot 的则是 canonical workflow plan、contract hashes 和 task IDs，而不是随时变化的 Markdown。

**明确不应照搬**

- 不以“尽量少的人类阻塞 gate”“agent 自动合并”或代码吞吐量作为金融安全系统的首要目标。
- 不接受“agent 可修改任何代码/脚本后直接运行”的默认权限；Akzio runtime agent 与开发 Codex 是两种不同信任边界。
- 不把生成代码量、PR 数量或弱同步开发模式当作 Akzio 运行时多 Agent 架构的正确性证明。
- 不用文档替代 Store invariant、类型系统、事务、lease/fencing 和测试。

### 3.2 Context Engineering：OpenAI 内部 Data Agent

**来源**

- 标题：Inside OpenAI’s in-house data agent
- 发布者：OpenAI
- 发布日期：2026-01-29
- URL：https://openai.com/index/inside-our-in-house-data-agent/
- 访问日期：2026-08-09

**官方资料的关键结论**

- 高质量 agent 的关键不只是底层模型，而是给模型提供正确、分层、可维护的业务语境。
- OpenAI 的内部 Data Agent 组合多类 context：表结构与元数据、人工注释、代码用法、组织知识和运行时查询验证。
- 元数据只能提供结构；真正语义还来自字段用途、业务约定、历史用法和数据所有者知识。
- 权限需要端到端继承；agent 不应因为统一检索层而看到用户原本无权访问的数据。
- 工具数量不是越多越好；更小、更清晰的工具集合更容易选择正确，也更容易审计。
- 可用系统需要显示假设、输入、执行步骤和结果依据；这不等同于保存或暴露模型的隐藏思维链。

**Akzio v2 可落地原则**

- Context Broker 应把上下文编译成分层 artifact view，而不是把 run 内所有文档自动并入候选集：
  `RawEvidence -> NormalizedEvidence -> SemanticDetail -> Claim/Critique -> DecisionContext -> Experience`。
- `ContextManifest` 只列出当前 contract、task、attempt 所需的 artifact IDs、kind、摘要、source closure 和预算；详情按需通过 `ReadGrant` 展开。
- `ReadGrant` 必须绑定 `run_id/task_id/attempt_id/contract_hash/lease-or-epoch`，并限制 artifact kind、字节数、source family、有效期和读取次数；Store 记录 mint/use/reject 事件。
- Context repair 应生成新的、可审计的 manifest/grant，保留导致 repair 的缺失证据和选择理由；不能静默扩大到 run-wide access。
- 对 Agent 展示 provenance、artifact 摘要和可验证事实；对用户展示 assumption、source refs、gate 和决策依据。不要把 chain-of-thought 设计成 durable artifact。

**明确不应照搬**

- 不接入任意 Slack、云盘、外部文档或数据库作为默认 context；Akzio 数据源扩展必须显式配置并受 Rust allowlist 管理。
- 不把用户可编辑的自然语言 memory 直接视为 canonical learning；Akzio canonical learning 只能来自 sealed Paper Outcome。
- 不允许模型直接写 SQL、自由浏览 CAS、查询 Raw Evidence 或通过 schema/tool 反推出未授权数据。
- 不把“更多 context”当作成功指标；正确指标是最小授权、来源闭包、召回完整性、字节预算和决策质量。

### 3.3 Codex Agent Loop 与 Agents Runtime

**来源 A**

- 标题：Unrolling the Codex agent loop
- 发布者：OpenAI
- 发布日期：2026-01-23
- URL：https://openai.com/index/unrolling-the-codex-agent-loop/
- 访问日期：2026-08-09

**来源 B**

- 标题：The next evolution of the OpenAI Agents SDK
- 发布者：OpenAI
- 发布日期：2026-04-15
- URL：https://openai.com/index/the-next-evolution-of-the-agents-sdk/
- 访问日期：2026-08-09

**来源 C**

- 标题：OpenAI Agents SDK — Agent orchestration / Results / Streaming / Model context protocol
- 发布者：OpenAI
- URL：https://openai.github.io/openai-agents-python/
- 访问日期：2026-08-09

**官方资料的关键结论**

- 基本 agent loop 是：harness 发送 instructions/tools/input，模型返回最终输出或 tool request，harness 执行工具并把结果加入后续 turn，直到终止。
- Codex 的核心能力来自“模型 + harness”；harness 决定工具可用性、tool execution、context 演进、权限、sandbox、状态外置和终止语义。
- 长任务需要 context 管理和 compaction；但压缩上下文是继续推理的手段，不天然等于可靠长期记忆。
- Agents SDK 的新 runtime 把 harness、execution 和 interaction 分离；同一个 durable logical run 可以在本地、云端或混合环境执行。
- SDK 的 `Manifest`/workspace、execution backend、streaming 和 approval 机制体现了“声明需要什么、平台执行什么”的分离。
- 官方资料明确提醒：MCP 或第三方工具仍需自己建立 sandbox/permission 边界，不能因为进入统一 runtime 就自动安全。

**Akzio v2 可落地原则**

- `AgentRuntime` 只拥有 model turns、schema validation、tool request parsing、context updates 和 contract termination；`TaskRuntime` 拥有 claim/attempt/lease/retry/cancel，`V2Store` 拥有 durable truth。
- 每次 tool request 先转成 Rust domain command，经 contract grant、task permit、Context grant、budget 和 lifecycle 校验后执行；模型不能直接调用 adapter 或 Store。
- Assistant 的“完成了”不是 task completion。只有 Rust terminal gate 验证 required artifacts、source closure、schema、permit 和 durable commit 后，任务才进入 completed。
- Contract catalogue 应版本化并覆盖：输入 policy、evidence access、tool grants、output schema、prompt、预算、retry、termination 和 failure policy；每个 attempt 固定 `contract_hash`。
- Compaction 只能生成引用源 artifact 的派生 context artifact，带有 loss/coverage 元数据；不能晋升为 canonical memory，也不能成为 replay 的唯一输入。
- 可以让官方 SDK 拥有 provider transport/types/stream handling，但 Akzio 的权限、任务状态、workflow gate、learning 和 execution policy 继续由 Rust 拥有。

**明确不应照搬**

- 不给运行时 Agent 通用 shell、patch、任意 MCP、工作目录挂载或自由网络；这些适合 coding agent，不符合 Akzio 研究与执行边界。
- 不让 provider 的 `response_id`、cached context、SDK run object 或 workspace filesystem 取代 `V2Store`。
- 不让 provider SDK 决定任务完成、重试、权限升级、候选晋升或 broker execution。
- 不把 compaction、自然语言总结或模型自报的 tool result 当成有 provenance 的事实。

### 3.4 Self-improving Agents

**来源**

- 标题：Building self-improving tax agents with Codex
- 发布者：OpenAI
- 发布日期：2026-05-27
- URL：https://openai.com/index/building-self-improving-tax-agents-with-codex/
- 访问日期：2026-08-09

**官方资料的关键结论**

- 自我改进的有效输入不是泛化的“记忆”，而是高质量运行痕迹、专家纠正和可复现评测。
- 反馈需要变成可执行任务，并在更改前后运行针对性评测与回归评测；否则一次局部修复可能破坏其他行为。
- 明确任务可自动化程度更高；高歧义或高风险判断仍应升级给人类。
- 文章展示的是一个与真实使用数据、专家 correction 和 eval loop 连接的工程系统，不是模型自由修改自身权威边界。

**Akzio v2 可落地原则**

- canonical feedback 必须是 sealed Paper Outcome；Debug、Replay、Shadow、Paper Dry Run、未密封行情和当前预测只能用于诊断或候选评测，不能直接推进 active policy。
- 将 Decision、ExecutionContext、broker commitment、reconciliation、Outcome 和 Evaluation 物化成 immutable artifacts，形成完整 experience lineage。
- 改进对象严格限定为 Prompt、预算、Contract version 和候选 research topology；source/tool/execution 权限是外部治理输入，不属于自动优化变量。
- Candidate 的必经链路：生成 -> 静态/fixture targeted eval -> regression suite -> fresh paired Shadow outcomes -> canary -> Rust state-machine promotion；每一步都可失败、回滚和退休。
- 模型/Codex 可以提出候选 diff、原因和预期指标；`akzio-learning` 依据 sealed evidence 和硬阈值决定状态迁移，`V2Store` 原子写入 policy head 与事件。
- 对高不确定性、数据不足、material conflict 或风险阈值失败的候选，进入 contested/review/freeze，而不是自动继续。

**明确不应照搬**

- 不允许 runtime agent 自动改 Rust 源码、提交代码、扩充工具或上线配置。
- 不把一次人工 correction、当前预测、模型自评或单个 Shadow 结果直接视为 canonical learning。
- 不跳过配对 Outcome、freshness、canary level 和回归测试，也不让模型自行宣告“改进成功”。
- 不把税务场景的成功率、任务拆分或人工 PR 流程直接移植为交易风险阈值。

### 3.5 Symphony：常驻 Orchestrator

**来源 A**

- 标题：OpenAI Symphony: Turning project work into isolated, autonomous implementation runs
- 发布者：OpenAI
- 发布日期：2026-04-27
- URL：https://openai.com/index/open-source-codex-orchestration-symphony/
- 访问日期：2026-08-09

**来源 B**

- 标题：OpenAI Symphony — SPEC.md
- 发布者：OpenAI
- URL：https://github.com/openai/symphony/blob/main/SPEC.md
- 访问日期：2026-08-09

**官方资料的关键结论**

- Symphony 是常驻轮询/调度 service：读取权威工作项状态，为每个任务建立隔离 workspace，启动 agent，维持 bounded concurrency，并把进度与状态反馈回控制面。
- reference spec 明确区分 tracker state、orchestrator state 和 agent runtime state；需要 validation、retry/backoff、reconciliation、stall detection、cancellation 和 cleanup。
- 每个 run 使用稳定 identity 与隔离目录；同一任务不应并发启动多个实例。
- workflow 定义可以配置，但 orchestrator 必须持续将外部状态与本地运行状态对账，而不能只依赖一次启动事件。
- 官方仓库把规范与 Elixir reference implementation 分开，强调 spec 是行为契约，reference implementation 不是唯一实现。

**Akzio v2 可落地原则**

- 把 `V2Store` 视为唯一 tracker/control-plane truth；Daemon 只是持 lease/epoch 的当前 supervisor，不持有无法恢复的私有业务状态。
- 每个 Task/Attempt 有稳定 identity、单活动 lease、心跳、超时、retry/backoff、cancel 和 durable events；每个 Run 可并发，但同一 Task/Run 的互斥语义由 Store 强制。
- Daemon 启动和周期循环都执行 reconciliation：恢复过期 attempt、重放未完成 slot、检查 stale owner、重建内存队列，但不得重建或改写 frozen workflow plan/task IDs。
- bounded concurrency 应按资源和工作类型设置，并保留 Evidence/Model/Evaluation/Execution 的独立预算；不能让研究洪峰饿死 scheduler 或 reconciliation。
- Planner 可以提出动态 DAG patch；WorkflowRuntime 负责 lowering、验证 artifact kind/source closure 和自动注入不可绕过 terminal gates。
- 配置变更只影响新 run/session slot；正在执行的 plan、contract hash、policy head 和 task IDs 保持冻结。

**明确不应照搬**

- 不使用 Linear/GitHub issue 等外部 tracker 作为 Akzio 业务控制面；没有网络时系统仍应从 `V2Store` 完整恢复。
- 不采用 Git worktree、PR、shell hook、issue comment 作为 trading workflow 的状态或完成条件。
- 不允许任意 `WORKFLOW.md` 热加载改变进行中 Paper session 的计划或 gate。
- 不把“每个待办启动一个 coding agent”照搬成“每个市场信号启动一个执行 Agent”；Paper commitment 仍受每 broker session 一次和 Rust execution gate 限制。

### 3.6 Responses API Computer Environment

**来源 A**

- 标题：Equip Responses API agents with a computer environment
- 发布者：OpenAI
- 发布日期：2026-03-11
- URL：https://openai.com/index/equip-responses-api-computer-environment/
- 访问日期：2026-08-09

**来源 B**

- 标题：OpenAI Developers — Container tools / Shell / Skills
- 发布者：OpenAI
- URL：https://developers.openai.com/api/docs/guides/tools-shell
- 访问日期：2026-08-09

**官方资料的关键结论**

- 新 computer environment 把 shell、container/workspace 和 skills 组合为一个受控执行环境；模型提出命令，平台负责执行并返回有界输出。
- workspace 文件系统允许把大量中间状态放在 context window 外，只把需要的片段带回模型。
- 官方安全模型强调隔离、受控网络、domain allowlist、审计和凭据不进入模型上下文。
- 文中通过文件、SQLite、脚本和 skill 展示“context in / state out”：环境是计算介质，而不是必须把全部状态重复放进 prompt。
- local shell 模式由调用方执行命令，官方明确把 sandbox、审计和权限责任留给调用方。

**Akzio v2 可落地原则**

- 采用“模型提出，Rust 执行”的分离，但将可执行动作收窄为 typed domain tools：请求证据、读取 grant、提交 claim/critique/decision draft；不是通用 shell。
- Evidence adapter 负责网络、域名 allowlist、认证、raw capture 和 normalization。凭据只存在于 Rust adapter/OS secret boundary，绝不进入 prompt、artifact payload 或 tool result。
- 大中间状态放在 CAS/SQLite，但 Agent 只通过 Context Broker 获取受约束的 immutable artifacts；不得把 Store Root 当成 agent workspace。
- Tool 输出必须有大小上限、typed status、source refs 和 truncation metadata；超限结果写入 artifact，再通过新 manifest/grant 选择性读取。
- 每次外部获取都记录 request class、source adapter、时间、raw hash、normalization version 和 error event，形成可重放审计线。

**明确不应照搬**

- 不给 Akzio runtime agent 暴露 shell、任意 Python、SQLite CLI、通用文件系统或任意网络，即使它们运行在容器里。
- 不让模型在工作目录自行创建平行 JSON/SQLite 状态；所有 durable state 必须经过 `V2Store`。
- 不依赖 hosted container 的生命周期、workspace snapshot 或 skill bundle 作为 recovery truth。
- 不允许模型选择或扩大 network allowlist、读取 broker credential、直接构造 Alpaca 请求或访问 Raw Evidence bytes。

### 3.7 Streaming Workflows：Responses API WebSocket

**来源 A**

- 标题：Accelerating agentic workflows with WebSockets in the Responses API
- 发布者：OpenAI
- 发布日期：2026-04-22
- URL：https://openai.com/index/responses-api-websocket/
- 访问日期：2026-08-09

**来源 B**

- 标题：OpenAI Developers — WebSocket mode
- 发布者：OpenAI
- URL：https://developers.openai.com/api/docs/guides/websocket-mode
- 访问日期：2026-08-09

**官方资料的关键结论**

- WebSocket mode 通过持久连接和只追加新 input 的方式减少多 turn/tool-heavy loop 的重复传输与连接开销。
- `previous_response_id` 可以复用连接内缓存的加密上下文，但这是 provider 侧、连接级优化。
- 官方文档列出关键限制：每个连接同一时间只能有一个 in-flight response，不支持 multiplexing；连接最长 60 分钟；连接内只保留最近 response 链；失败可能导致缓存链被逐出。
- 断开后可用先前 response id 建立新连接继续，但调用方仍必须处理断线、失败和重建。

**Akzio v2 可落地原则**

- 将 WebSocket 视为 `akzio-model` 的可选 provider transport，用于降低长 model/tool loop 延迟；它不改变 Domain、TaskRuntime、Context Broker、Store 或控制面协议。
- 一个连接只绑定一个活跃 model turn/attempt 链；Akzio 自己的并发由 TaskRuntime 管理，不尝试在一个 provider socket 上 multiplex 多任务。
- 每个模型请求前，仍从 `V2Store`/Context Broker 编译完整的可恢复输入边界；缓存命中只是性能收益，缓存丢失必须能无语义差异地重新发起。
- 将 provider response id、transport mode、重连次数和延迟写入 diagnostic event；不要将 response id 当作 artifact identity 或 provenance root。
- Loopback control API 继续使用 HTTP；运行进度继续使用 SSE。Provider WebSocket 与 operator control/observability 是不同层。

**明确不应照搬**

- 不以 WebSocket session 作为 durable run、task lease、replay log 或 crash recovery 状态。
- 不用 provider WebSocket 替代本地 Daemon HTTP/SSE，也不让 CLI 直接连模型 provider。
- 不假设连接无限持续、支持多路复用或缓存必然存在；60 分钟、single in-flight、eviction 都必须被设计为正常故障。
- 在 R0-R9 的正确性、安全性和恢复性未完成前，不把 transport 性能优化列为关键路径。

## 4. 跨来源架构原则

### 4.1 三层权威模型

| 层 | 允许拥有 | 禁止拥有 |
| --- | --- | --- |
| Model proposal | 研究提案、evidence request、Claim、Critique、Decision Draft、候选改进建议 | durable state、权限、任务完成、canonical learning、broker commitment |
| Rust harness/runtime | schema/contract、预算、tool dispatch、workflow/task gates、execution policy、learning transition | 绕开 Store 的私有 durable state、未记录的副作用 |
| `V2Store` | CAS、graph、events、leases/epochs、frozen plans、permits、policy heads、commitments、outcomes | 模型推理、网络获取、broker 调用 |

这三层是 R0 应冻结的首要不变量。Provider SDK、container、WebSocket、MCP 或 future UI 都只能附着在其中一个边界上，不能创造第四个事实源。

### 4.2 Context 是编译产物，不是共享目录

- Agent 输入由 contract、workflow state、artifact policy、source closure 和预算共同编译。
- Manifest 只描述可见集合；Grant 才允许具体读取，且两者必须绑定 attempt 与 permit。
- Progressive disclosure 和 compaction 只改变读取粒度，不改变 provenance 或 canonicality。
- Agent 看不到 Store Root、任意文件路径、raw adapter 响应或 run-wide 文档集合。

### 4.3 所有副作用都需要 capability + transaction + event

- 读取：`ReadGrant`。
- 写 artifact/完成 task：`TaskWritePermit`，在同一 transaction 内提交 artifact、task state 和 event。
- Scheduler 写入：daemon lease owner + epoch permit。
- Paper commitment：session slot + daemon lease/epoch + execution permit + idempotency key。
- Learning promotion：sealed Outcome refs + evaluation window + policy transition permit。

### 4.4 自我改进是候选状态机，不是运行时自改代码

- 反馈来源受 canonicality gate 限制。
- 每个候选保存 parent policy、变化维度、证据窗口、eval 结果、shadow pairs 和 canary level。
- Rust 决定 transition；模型只能提案。
- rollback/freeze 是第一等状态，不能依赖人工记得执行。

### 4.5 流式传输不是耐久性

- SSE/WebSocket 用于及时传递观察或减少延迟。
- append-only Store event 才是恢复、replay 和审计依据。
- 任何连接重启后，系统都能从 V2Store 恢复，而不需要 provider cache、CLI 内存或 agent workspace。

## 5. 对 R0-R10 的映射

| 阶段 | 从官方资料吸收的原则 | 应写入该阶段的明确交付/验收 |
| --- | --- | --- |
| R0 | Harness rules-as-code；三层权威；Context/Computer boundary；transport 非 state | 冻结 invariants、crate ownership、删除图、canonicality 表、capability/transaction/event 矩阵；新增结构检查和完整安全测试矩阵；明确不实现 arbitrary shell/MCP/network/live trading |
| R1 | Agent loop 的 typed proposal；context/artifact provenance；self-improvement candidate | 重建 ID、Artifact、Contract、Workflow、typed blocker、Decision/Execution/Outcome/Experience/Evaluation/Event；不得持久化 chain-of-thought；定义 lifecycle/source closure/canonicality validation |
| R2 | Symphony 单一权威与 reconciliation；computer environment 的 externalized state；WebSocket 非耐久 | `V2Store` 原子 workflow/attempt/commitment/promotion transactions，CAS+SQLite graph+append-only events，lease/epoch permits，policy heads，Doctor；故障注入证明无 split write/crash window |
| R3 | Data Agent 的多层 context、permission inheritance、少工具；computer environment 的受控 egress | Raw/Normalized/Detail pipeline；ContextManifest/ReadGrant task-attempt binding；allowlisted adapters；repair/progressive disclosure；越权、source closure、byte/source-family 和 credential-redaction 测试 |
| R4 | Codex loop 与 Agents Runtime 的 harness/execution 分离、schema/tool/termination | 版本化 Contract Catalogue；AgentRuntime model loop；严格 output schemas；budget/retry/termination；tool request Rust dispatch；compaction 只产生有 lineage 的 noncanonical context artifact |
| R5 | Symphony 的 daemon scheduling、isolation、bounded concurrency、retry/reconcile；harness plan-as-artifact | Planner proposal lowering、动态 DAG、TaskRuntime claim/attempt/retry/cancel/recovery；Rust 注入 terminal gates；frozen plan/task IDs；多 Run 并发与单 Task/Run lease 测试 |
| R6 | Self-improving 的 trace/correction/eval/regression/canary loop | sealed Paper Outcome canonicality gate；Experience/Evaluation；Shadow pair；Candidate -> Active -> Proven -> Contested -> Retired；targeted/regression/fresh paired/canary/rollback；noncanonical 污染测试 |
| R7 | “模型提出、平台执行”；凭据与网络在 harness 外；高风险升级 | Decision/ExecutionRuntime typed gates；account/quote/freshness/allocation/turnover/conflict/exposure；严格 Paper host；lease/epoch-fenced idempotent commitment/reconciliation；模型永不接触 broker/credential |
| R8 | Symphony 常驻 supervisor、stall recovery、reconciliation；流式观察与 durable state 分离 | Daemon leadership、heartbeat、epoch fencing、queue/recovery、scheduler session slot、crash harness；loopback HTTP/SSE；freeze/CLI-HTTP unfreeze；删除 Unix JSON 业务协议 |
| R9 | SDK/backend 可替换但 domain authority 不变；provider transport 隔离 | CLI 成为 loopback HTTP client；typed/redacted config；禁止 direct Paper submit/retry/Store mutation；provider WebSocket 仅可选 model-adapter 配置，不能进入控制面 |
| R10 | Harness observability/feedback loops；WebSocket failure model；Symphony cleanup/reconciliation | structured logs/metrics/traces/events、deterministic replay、fixture harness、Store Doctor、crash/concurrency/fencing/grant/canonicality/scheduler tests；若启用 WebSocket，补 single-in-flight/reconnect/cache-loss 测试；完成旧代码/协议/路径删除 |

## 6. 对 Goal 执行计划的建议约束

1. **顺序不变**：继续采用 R0 -> R10 依赖链；外部文章不构成跳过 Domain/Store/Context 基础去先做 Agents SDK、WebSocket 或 self-improvement 的理由。
2. **R0 先冻结边界**：把本文第 4 节转成 invariants 和测试条目；未机械验证的规则不应只留在 prompt。
3. **每阶段双重验收**：既检查 crate API/编译/窄测试，也检查跨层 durable event、permit 和 crash behavior。
4. **性能优化后置**：provider WebSocket、context cache 和更复杂的并行 agent topology 进入 R10 或更晚；先证明可恢复、可审计、fail-closed。
5. **开发 harness 与产品 runtime 分离**：Codex 可在仓库内读取/修改源码并运行测试；Akzio runtime Agent 只能通过 Contract + Context Broker + typed tools 工作。任何计划项都不得混淆这两个权限模型。
6. **不以文章案例作完成证据**：Akzio 的验收仍以当前 checkout、离线构建、测试、fixture、Doctor、持久化 artifacts/events 和 Paper-only 安全边界为准。

## 7. 官方来源索引

| 主题 | 官方来源 | URL | 访问日期 |
| --- | --- | --- | --- |
| Harness Engineering | Harness engineering: leveraging Codex in an agent-first world | https://openai.com/index/harness-engineering/ | 2026-08-09 |
| Context Engineering | Inside OpenAI’s in-house data agent | https://openai.com/index/inside-our-in-house-data-agent/ | 2026-08-09 |
| Codex Agent Loop | Unrolling the Codex agent loop | https://openai.com/index/unrolling-the-codex-agent-loop/ | 2026-08-09 |
| Agents Runtime | The next evolution of the OpenAI Agents SDK | https://openai.com/index/the-next-evolution-of-the-agents-sdk/ | 2026-08-09 |
| Agents Runtime docs | OpenAI Agents SDK documentation | https://openai.github.io/openai-agents-python/ | 2026-08-09 |
| Self-improving Agents | Building self-improving tax agents with Codex | https://openai.com/index/building-self-improving-tax-agents-with-codex/ | 2026-08-09 |
| Symphony | OpenAI Symphony: Turning project work into isolated, autonomous implementation runs | https://openai.com/index/open-source-codex-orchestration-symphony/ | 2026-08-09 |
| Symphony spec | OpenAI Symphony — SPEC.md | https://github.com/openai/symphony/blob/main/SPEC.md | 2026-08-09 |
| Computer Environment | Equip Responses API agents with a computer environment | https://openai.com/index/equip-responses-api-computer-environment/ | 2026-08-09 |
| Computer tools docs | OpenAI Developers — Shell | https://developers.openai.com/api/docs/guides/tools-shell | 2026-08-09 |
| Streaming Workflows | Accelerating agentic workflows with WebSockets in the Responses API | https://openai.com/index/responses-api-websocket/ | 2026-08-09 |
| WebSocket docs | OpenAI Developers — WebSocket mode | https://developers.openai.com/api/docs/guides/websocket-mode | 2026-08-09 |
