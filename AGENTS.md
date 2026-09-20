# Akzio 开发协作规则

本仓库是仅支持 Alpaca Paper 的 Rust 多智能体投研系统，包含 macOS Observatory App。本文指导开发助手；应用运行时 Agent 的工具、预算和业务协议见下文按任务索引。开发助手使用 GPT-6 Astra 不意味着更换应用模型、调整冻结 Contract 或放开交易权限。

## 执行与授权

- 默认中文，先给结果和必要证据。实施请求推进到修改、验证和交付；只审阅、分析或访谈的请求按其范围完成。沿用已明确的任务目标与授权，不因后续状态问题或上下文切换重新开工。
- 从现有上下文和代码解决普通选择。只有无法推断且会改变业务结果、任务范围、授权或难以撤回选择的信息才询问；继续不依赖答案的工作。
- 明确的删除、工作区外写入、生产、凭证、发布及其他审批要求继续生效。复用仍有效且范围未变的明确授权，不把可逆操作、文件存在或工具可用当作额外授权。Skill 的流程建议不得凭空增加审批门。
- 开始修改前检查工作区状态，保留无关改动。提交、格式化和清理不得顺带处理他人的工作；用户只要求审阅时不修改源码。

## 按任务读取上下文

先定位文件、调用方和失败路径，再读取足够上下文；不把下面所有资料一次装入上下文。下列业务约束是强制规则，按任务加载不改变其效力。

| 任务涉及 | 修改或执行前读取 |
|---|---|
| Rust 模块、领域模型、持久化 | [运行时契约](docs/agent-runtime-contract.md) 第 1、2、4 节及涉及功能的章节；需要术语时查 [CONTEXT.md](CONTEXT.md) |
| 研究拓扑、Prompt、模型预算、Context、Evidence | 运行时契约第 3、5、6、11、12 节；同步检查实际 Contract、Prompt 消费方及 Rust 校验 |
| Decision、Execution、Broker、学习与迁移 | 运行时契约第 3、7、8、11、12 节；核验被冻结的来源、版本和现有 Gate |
| Debug、真实模型、App/Core 联调 | 运行时契约第 12 节；[Debug 基础设施](docs/debug-control.md) 中对应入口与权限 |
| 旧研究链路退役、历史兼容 | [研究协议与退役清单](docs/research-protocol-retirement.md)；区分历史读取和活动创建、执行能力 |
| DecisionPolicy 缺失、校准准备 | [开发 Workflow](docs/development-workflow.md) 的 readiness 与 SQL 校准流程；核对 Store 资格、真实 Outcome、模型身份和显式风险限制 |
| 测试、CI、打包、交付 | [开发 Workflow](docs/development-workflow.md) 的相关验证分支 |
| 历史问题或特定验收证据 | 按相关文件的 Git 历史定位当时记录；历史结果不视为当前验证 |

按任务使用会话中可用的 Skill。该仓库对本次优化过的 Skill 提供项目副本，位于 `.agents/skills/`；在本仓库工作时优先使用这些同名项目副本。只加载匹配当前阶段的 Skill 及必要引用，不为使用某 Skill 初始化无关配置。一般 Skill 建议服从用户当前任务；明文审批与文件边界继续有效。若指令导致暂停或缩小交付，引用具体文件和原文说明。

## 始终保持的业务边界

- 仅维护 v2，不恢复旧 `orchestrator-*` crates、Phase 0–8、FileStore、旧 prompts 或 `outputs/store` 兼容路径。Rust 工具链按仓库固定版本，当前为 `1.96.0`。
- 可执行资产仅 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。Live Trading 永不实现；非 Paper endpoint 必须在 HTTP I/O 前拒绝。
- Rust 是状态、授权、Contract、预算、Gate 和执行策略唯一权威。V2Store 是唯一持久化权威，不绕过 `akzio-store` 写 SQLite，不增加改变语义的并行状态。
- 运行时研究 Agent 只经 `akzio-context` 获取授权资料；不开放 RawEvidence、任意文件、SQL、联网或交易工具。开发工具可用不改变运行时沙箱。
- Paper、PositionPlan、Shadow 的新 Analyst/Critic/Synthesizer 使用同一单次结构化提交协议，只接收授权 projections 并调用 `submit_result`。Outcome 保留独立两阶段协议；不得用旧研究 Draft 或 Planner fallback 恢复执行。版本以运行时契约和源码为准。
- Planner / PaperDryRun 旧研究的创建和执行能力已退役；历史读取、展示、导出与完整性审计保留。遇到 `legacy_workflow_retired` 不恢复旧 recipe、fixture 图或隐藏入口；离线验证使用正式拓扑的 `debug verify-fixture`。
- T0 研究与历史 T+1/T+3/T+5 评估独立推进；按四资产共同完成的交易 Session 计算。NoOrder 仍须评估，未知风险和缺失校准保持 fail closed。
- DecisionPolicy 的风险限制、校准 dataset、候选版本和激活记录均以 SQL Store/CAS 为权威。先用只读 readiness 定位缺口，再按既有 collect/build/inspect/validate/activate 流程推进；不伪造样本、模型发布日期或知识截止日期，不以生成候选代替显式激活。
- Paper 写入须通过原审批和全部 Gate，并先持久化确定性 Commitment。PositionPlan 在 Decision 后结束，没有 ExecutionGate、PaperCommit、Reconcile 或 Evaluate。
- Debug Core 使用 `.akzio/` 下的新隔离 Store；禁止打开 `~/.akzio/store` 或启用 `auto_paper`，Broker 默认 forbidden，不写 canonical policy 或 Active Lesson。
- Alpaca Paper 是唯一交易模拟环境；`-fakerOnline` / `--faker-online` 和本地模拟账户、时钟、成交适配器已删除。显式 `--paper` 使用正式 Paper 图和原生 Paper API，仍须原审批与全部 Gate。时段由真实 Clock 与交易日历确定，隔夜使用 BOATS / overnight 行情并检查资产资格；休市延期后刷新快照、重跑 Gate，提交成功不等于成交。隔离运行不能计入正式校准样本，完成或等待 Outcome 不改变 Store 资格。
- 不静默改写历史 CAS、Contract、Commitment、哈希或迁移边界；旧任务与有效 lease 的升级阻断仍须合法处理。业务证明与完整 provenance 必须保留。
- Store、BLOB、socket、生成报告、认证凭据和本地配置覆盖不得提交 Git。

## 工具与任务推进

优先 `rg`、现有 CLI/连接器和仓库脚本，按当前工具实际接口调用。独立只读搜索和检查可并行；依赖动作及有副作用的操作顺序执行，检查每项结果，不重复发起尚未完成的动作。遵循已加载的全局 RTK 原始输出例外。

获准的独立子任务可委派，主任务继续有用的本地工作并负责整合和验证；派发、计划和中间结果不等于完成。外部依赖或审批仅阻塞相关动作；无法验证时交付已证实部分并明确剩余项，不猜测成功。

## 验证与完成

[开发 Workflow](docs/development-workflow.md) 是检查命令、隔离配置和 App 打包的统一入口。先做受影响行为的窄域检查，再完成适用的项目强制检查；已有证据仍适用于最终状态时复用，无新改动或疑点不重复扩展验证。

报告 `implemented`、`offline-verified`、`real-Paper-verified`、`outcome/learning-verified` 的实际范围。这些是证据标签，不是每个任务都必须执行到 T+5。完成须满足当前请求和适用的强制检查；失败或缺失的必需检查必须明确披露，不能由另一成功命令替代。

分别报告研究提案、正式 Decision 目标、ExecutionVerdict、Paper 订单提交、Paper 成交和 Outcome。`NoOrder` 可以是完整流程的业务结果；离线 fixture、只读 API 或订单 accepted 均不能标成真实 Paper 成交或正式校准完成。具体 Run ID、样本数、本地配置缺口与当次验收结果留在运行证据中，不固化为本文件的长期规则。
