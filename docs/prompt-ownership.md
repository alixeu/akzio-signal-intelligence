# 提示词所有权、组装与验证

运行时提示词由其功能模块维护；统一来源清单与 RuntimeIdentity 负责跨模块追踪。长正文以 Markdown 编译嵌入，短指令使用 Rust 常量或有类型的函数。不引入 TOML/YAML 模板解释器，不从磁盘热加载。开发助手的 AGENTS.md、Skills 不进入应用提示词。

## 从哪里修改

| 内容 | 所有者与入口 |
| --- | --- |
| 共享治理 | [research shared.md](../crates/akzio-research/src/agent/prompts/shared.md) |
| 四个研究角色及 Outcome | [Analyst](../crates/akzio-research/src/agent/prompts/roles/analyst.md)、[Critic](../crates/akzio-research/src/agent/prompts/roles/critic.md)、[Synthesizer](../crates/akzio-research/src/agent/prompts/roles/synthesizer.md)、[ProposalReviewer](../crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md)、[Outcome](../crates/akzio-research/src/agent/prompts/roles/outcome.md) |
| 角色选择与候选附加指令 | [研究入口](../crates/akzio-research/src/agent/prompts/mod.rs) |
| 结构化研究提交、Outcome 两阶段及 repair | [阶段与请求构建器](../crates/akzio-research/src/agent/prompts/phases.rs) |
| 只读工具描述、参数 Schema、权限与 handler | [工具契约](../crates/akzio-research/src/agent/schemas.rs)、[工具执行](../crates/akzio-research/src/agent/tools.rs) |
| 新闻发现及官方资源查询 | [采集构建器](../crates/akzio-ingest/src/prompts.rs) |
| 独立来源核验长正文 | [source_verifier.md](../crates/akzio-ingest/src/prompts/source_verifier.md) |
| 投影解释、证据引用范围 | [ContextBroker guidance](../crates/akzio-context/src/context_broker/guidance.rs) |
| Native capability probes | [probe_prompts.rs](../crates/akzio-model/src/model_client/probe_prompts.rs)；独立 [preflight 示例](../crates/akzio-ingest/examples/native_web_preflight.rs) 的两句指令留在示例内 |
| Workspace 来源清单与哈希 | [prompt_registry.rs](../crates/akzio-research/src/prompt_registry.rs)、[组件哈希入口](../crates/akzio-research/src/lib.rs) |

一个角色文件保存该角色的完整正文，不再打开六个片段才能理解 Analyst。共享治理仍是一份独立 policy。证据使用及 deliberation 的说明保留在完整角色文档中，这是为了完整审阅而接受的正文重复；若更改跨角色规则，必须同时核对涉及角色及 Rust 校验，不能只改其中一份。

整理文件时保留冻结正文、顺序和空白。Markdown 中部分空白属于现有冻结正文，不能顺手 trim。新增标题、重排或改写要求属于真实行为变更，应按正常版本升级与旧任务阻断机制处理。

## 组装、权威与 trace

共享治理和四个研究角色正文只保存治理、角色与业务规则，实际阶段协议由 phases.rs 选择。Contract 69 的 Analyst/Critic/Synthesizer/ProposalReviewer 在 Paper、PositionPlan、Shadow 一律直接 Submit，不带 Draft 或读取工具，不要求模型填写 Rust-owned timing；只有 Outcome 保留两阶段请求构建。旧 Store 中的冻结正文不被源码覆盖，原版本迁移与未完成任务阻断仍生效。

安装时将共享治理和完整角色正文写入原有 V2Store CAS；运行时仍读取已冻结 Contract 的两个 blob。Rust 根据角色、阶段、旧版本及修复状态选择对应指导。ledger、TaskBudget、时间和语言通过函数参数注入；证据、工具和 Schema 沿用既有授权路径。

工具描述跟随工具契约维护，Schema/Gate、权限、资源上限和 Broker policy 始终由 Rust 决定。提示词文件不能激活候选、改变拓扑或开放工具。

来源清单按稳定 ID（例如 `research.analyst`、`context.guidance`）登记路径及编译内容，明确哪些来源影响 Contract 组件身份。原有 `prompt_component_hash()` 继续覆盖所有登记来源；源码和模块路径移动会改变组件来源身份，原审批是否可用仍由 RuntimeIdentity 判断。

实际调用已有 [请求审计](../crates/akzio-research/src/agent/runtime_helpers.rs)：持久化 request_hash、domain_request/request、运行快照及响应。恢复会按原 request hash、Contract、Context 和工具身份校验。复用该 trace 与 CAS，不另建 Prompt ABI、数据库或并行状态。

## 检查入口

- [角色正文基线](../crates/akzio-research/src/agent/prompts/mod.rs)：五个完整角色的 SHA-256、未知角色、阶段/版本/修复分支。
- [来源清单检查](../crates/akzio-research/src/prompt_registry.rs)：稳定 ID 和路径唯一、正文有效、角色及采集长文档无漏登记。
- 受影响 crates 检查及 workspace、隔离 fixture 验证遵循 [开发 Workflow](development-workflow.md)。

离线结构和渲染回归不等于真实模型行为评测。未来若改写正文，需另行选择证据缺失、冲突、过期、描述性证据、补采与工具预算等实际案例做行为评测。
