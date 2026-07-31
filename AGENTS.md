@/Users/alixeu/.codex/RTK.md

# Agent 指令

本仓库是一个 Rust workspace,用于 AI 辅助的市场信号研究与面向 TQQQ 的报告工作流。

## 项目速览

- 语言:Rust 2021。
- Workspace crate:
  - `orchestrator-core`:共享配置、路径、ticker 解析、提示词辅助函数与产物校验。
  - `orchestrator-store`:原子化 FileStore 持久化、manifest、会话、草稿、索引与执行恢复。
  - `orchestrator-llm`:OpenAI Responses/Chat Completions 执行与类型化 ToolManaged 分发。
  - `orchestrator-ingest`:技术市场数据与金十(Jin10)输入摄取。
  - `orchestrator-workflow`:阶段编排、摘要、配置、报告与 FileStore 运行时适配器。
  - `orchestrator-cli`:CLI 可执行文件、运维操作与提示词 lint。
- 提示词模板位于 `prompts/` 下,归其运行阶段所有:
  - `phase_summary`:已完成阶段的压缩。
  - `phase1`:技术与新闻/宏观研究。
  - `phase2`:Topic Generator、多空(Bull/Bear)辩论、有界 Web 证据研究、Topic Controller 与引导消息。
  - `phase3`:Research Manager 概率决策。
  - `phase4`:Trader 转换。
  - `phase5`:激进、中性与保守风险审查员。
  - `phase6`:Portfolio Manager 最终决策。
  - `common`:可复用契约/组件;`system`:agent 循环消息。
- 提示词组件按角色划分范围。Topic Generator 与 Research Manager 使用分析
  轨迹;Trader 与 Portfolio Manager 使用执行轨迹;Phase Summary 使用摘要
  轨迹。Bull/Bear 数据包、Topic Controller 与 Phase 5 风险审查员保留各自
  紧凑的数据包/约束审计数据。
- Phase 2 构建一个共享的 Bull/Bear 预热检查点,并独立运行 Topic
  Generator。每个议题的 Bull/Bear 对话从预热检查点 fork,而 Topic
  Controller 从 Topic Generator fork。
  在一次相关的 Phase 1 Detail 展开之后,Topic Generator 与 Bull/Bear 可以
  将一个明确的证据缺口委托给仅限 Web 的子代理。Rust 负责按角色/议题的
  额度、去重、校验与证据 ID。辩论压缩仍由 Rust 拥有。
- Phase 0 历史评分/任务选择、Phase 7 配置以及 Phase 8 决策快照/归档是
  Rust 拥有的阶段。Phase 0 使用专用的历史反思器提示词进行因果分析。
- 非 mock 工作流将结果背书的历史案例记录为 Experience Index/Detail 条目,
  供后续检索。
- 对 Phase 2–6 的模型角色而言,Phase Summary 是唯一的跨阶段语义接口。
  角色先列出摘要再展开明细;Rust 强制各角色的来源阶段、分页、Detail 额度
  与证据 ID 策略。
- Phase 5 审查员独立并行运行。Portfolio Manager 合并它们各自单独压缩的
  Phase 5 摘要。
- 生成的运行输出位于 `outputs/` 下,不应提交。
- 运行时默认值位于 `config/config.yaml`。
- 实时 agent 运行默认使用运行封存的 FileStore 输入快照。

## 命令

交付代码变更前使用以下检查:

```bash
rtk cargo fmt --all
rtk cargo test
rtk cargo clippy --workspace --all-targets
```

常用本地运行:

```bash
rtk cargo run -p orchestrator-cli --bin orchestrator-exec -- --mock
rtk cargo run -p orchestrator-cli --bin report-email -- --help
```

## CodeGraph

本项目配置了 CodeGraph MCP 服务器(`codegraph_*` 工具)。CodeGraph 是由 tree-sitter 解析的知识图谱,覆盖每个符号、边和文件。

结构性问题使用 CodeGraph:

| 问题 | 工具 |
| --- | --- |
| 符号在哪里定义? | `codegraph_search` |
| 什么调用了某符号? | `codegraph_callers` |
| 某符号调用了什么? | `codegraph_callees` |
| 一个符号如何到达另一个符号? | `codegraph_trace` |
| 修改会影响什么? | `codegraph_impact` |
| 查看签名/源码/文档字符串 | `codegraph_node` |
| 获取任务区域上下文 | `codegraph_context` |
| 探索相关源码 | `codegraph_explore` |
| 浏览已索引文件 | `codegraph_files` |

架构、功能或 bug 上下文问题优先使用 `codegraph_context`。原生 `rg` 只用于字面文本查询、生成文件,或已确定具体文件之后。

## 编码规则

- 保持变更范围收敛,并与现有 crate 边界对齐。
- 优先使用 `orchestrator-core` 和 `orchestrator-store` 中的现有辅助函数,再考虑新增工具函数。
- 在 CLI 与系统边界校验输入。
- 不要硬编码密钥;使用环境变量。
- 保留 mock 路径,以便在没有 `LLM_GATEWAY_API_KEY` 的情况下进行本地开发。
- 不要让实时 `orchestrator-exec` 直接消费可变的市场输入;先在运行 FileStore 中封存原子化的 Technical/金十输入快照。
- 提示词路径保持配置在 `orchestrator.prompts` 下,配置的提示词文件缺失时应尽早失败。
- 保持 `mediator.topic` 仅使用证据:它可以使用 Phase 1 索引和既往阶段
  摘要,而议题产物的运行时封套与确定性回退由 Rust 拥有。
- 不要创建诸如 `phase25` 的跨阶段提示词桶;角色提示词应随其执行阶段迁移,
  并同步更新配置默认值、`include_str!` 路径、提示词 lint 角色推断、golden
  渲染测试、README 与本文件。
- 保持三个 Phase 5 审查员使用各自独立的提示词路径。共享约束放在
  `prompts/phase5/risk_analyst.md`,立场特定行为保留在
  `prompts/phase5/{aggressive,neutral,conservative}.md`。
- 在摄取、FileStore 上下文读取器、角色注册、提示词与调度全部配置完成之前,
  不要将 YouTube 或 Reddit/X 描述为活跃输入。
- 保持反思为结果背书且面向历史:绝不从 mock 运行、未评分预测或当前预测
  中学习。Experience Index 写入必须保持幂等,且反思失败不得使已完成的
  投资决策失效。
- MemoryOS 评估仅限 FileStore:规范 Decision/Outcome 写入只允许在显式启用
  的 Paper/Live 上下文中进行。Debug 使用自己的命名空间,Mock 不写正式
  Decision/Outcome,回放必须使用独立的根目录/命名空间。不要在代码中选择
  基准或价格基础:缺失严格配置是可审计的物化缺口。
- 避免提交本地配置、FileStore 数据、构建输出或报告产物。

## 文档规则

- 当命令、安装步骤、环境变量或 crate 职责变化时更新 `README.md`。
- 只有在对未来维护者有帮助时,才将持久性项目知识写入现有文档或模块级注释。
- 除非任务明确需要,否则不要创建新的顶级文档。
