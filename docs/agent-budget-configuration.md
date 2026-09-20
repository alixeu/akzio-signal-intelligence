# Agent Budget 配置

Budget 是资源治理上限，`max_input_tokens` 表示单个 Task 内多次 LLM 请求的累计 input token 上限，跨 Draft、Submit、Retry 和 Recovery 共享同一任务账本，不是 Provider 的 context window。设置 `1000000` 表示该 Task 的累计 input 上限为 1M，不表示整个 Run 只有 1M 总额度，也不表示 Provider 支持 1M 单请求上下文或消除了重复上下文。本功能不修改 ContextManifest、Transcript、ToolResult 或 Context Tool 的材料处理和容量策略。

## TOML schema

```toml
[agent.budget.default]
max_input_tokens = 1000000
max_tool_calls = "unlimited"

# 可选：给一个角色设置有限工具次数及其他覆盖项。
# [agent.budget.analyst]
# max_tool_calls = 8
# max_output_tokens = 12000
# timeout_seconds = 180
```

支持 `[agent.budget.analyst]`、`[agent.budget.critic]`、`[agent.budget.synthesizer]` 和 `[agent.budget.outcome_worker]`。四个字段 `max_input_tokens`、`max_output_tokens`、`max_tool_calls`、`timeout_seconds` 均可选。配置按 Role，不按模型名。CLI 的 `--config` 和 Core 启动使用同一套解析逻辑。

`research.proposal_reviewer` 继承 Critic 的有效模型 route 与预算；每个提案 revision 是独立冻结节点，费用进入同一 Run。新增 `[agent.research] max_proposal_revisions = 2` 配置初稿之外的修订次数（0 只审初稿，当前最多 5），不改变格式修复额度；超过固定 32 节点上限在创建 Run 前拒绝。

每个字段独立按 **Role override → default → 该 Role 的代码默认值** 解析。省略整个 `agent` 段或任一字段均有效。只覆盖 Analyst 不影响 Critic。显式设置 `default.max_output_tokens = 6000` 则会影响所有没有覆盖此字段的 Role；它不同于完全不写 `default`。

| Role | Task input | Task output | read tool calls | 单 Attempt timeout 秒 |
|---|---:|---:|---:|---:|
| analyst | 1000000 | 1000000 | unlimited | 180 |
| critic | 1000000 | 1000000 | unlimited | 180 |
| synthesizer | 1000000 | 1000000 | unlimited | 180 |
| outcome_worker | 1000000 | 4000 | unlimited | 180 |

output 为 `1..=1000000` 的整数；input/timeout 保留既有 `1..=4294967295` 范围；read tool calls 为 `0..=65535` 或字符串 `"unlimited"`，默认不限制次数。`unlimited` 同时移除由工具额度派生的模型调用次数上限，累计 token 和 timeout 仍生效。0 个只读工具是合法配置，不禁止提交阶段的 `submit_result`。负数、溢出、小数、类型不符、未知字段/Role、0 token 或 0 timeout 均在启动时明确失败，包括被其他 Role 覆盖的非法 default。较小的正预算可合法启动，但 Runtime 会在无法容纳必要的 Draft/Submit 请求时拒绝调用。

## 输出与单请求边界

`max_output_tokens` 是 Task 的累计输出治理预算，跨 Draft、Submit、Retry 和 Recovery 共享，包含 Provider 计入 output 的 reasoning 用量；它不是目标生成长度，也不是整个 Run 的输出总额度。Contract 49 起，研究角色的单请求上限由剩余冻结 output、应用 1000000 上限和 Provider 声明输出能力取最小值，不再按 Draft/Submit 阶段隐式减半或限制为 4000/900。Provider 未提供能力上限不等于已证明支持；协议拒绝、截断、超时与 usage 缺失仍是独立失败。较低角色覆盖照常生效，单次调用、阶段切换、重试或恢复均不会重新获得一份 token 额度。Outcome Worker 保留原阶段分配语义；旧研究版本只可读取和审计，不可恢复执行。研究协议只提供 `submit_result`，预算中的工具上限不授予读取工具。

`timeout_seconds` 是独立的单 Attempt 墙钟上限：研究角色默认 180 秒，Outcome Worker 默认 180 秒。同一 Attempt 的 Draft、Submit 和恢复使用该 Attempt 的既有起点；后续 Retry 可以创建新的 Attempt，但不会因此清零 Task 已累计的 input/output 用量。将输出配置为 1M 不会延长这项墙钟上限。

## 冻结与执行

- 新 Run 默认值定义在 `akzio-domain::budget::default_agent_budget`。当前研究 Contract 67 / PromptBundle 37 / candidate 68 沿用既有单任务预算，终稿 Reviewer 继承 Critic 的有效配置；每个预编译修订节点分别冻结预算，并共同计入 Run 成本；Outcome 保持 Contract 63 / PromptBundle 35。`versioned_contract_budget` 构造当前 Contract，`legacy_contract_budget` 只为历史解码、完整性校验和历史边界测试保留；不改写已存 Contract/Workflow 哈希。
- Rust 创建 Run 的 WorkflowGraph 时写入 `agent_budgets`，包含四个原有 Role 和继承 Critic 的 proposal_reviewer resolved budget。每个任务节点的 `budget` 和 Store 的 `budget_json` 保存实际预算；配置中的 `timeout_seconds` 对应既有持久化字段 `max_wall_time_secs`。
- 创建 Attempt 时从持久化任务取得预算。AgentRuntime 校验传入预算与持久化任务一致；之后仅使用冻结值执行 Task 累计 input/output、只读工具次数及单 Attempt 墙钟时限检查。发起请求前检查累计输入和剩余输出，Draft、Submit、Retry 和 Recovery 共用 Task token 额度，失败调用仍按既有规则记账。
- 恢复继续执行原有累计用量恢复协议，不因配置重载、阶段切换或恢复重新分配预算。Outcome 各期限保留现有独立额度机制，额度上限来自原 Run 快照。
- 延迟创建的 Outcome Worker、原 Run 的叙事修复均采用原 Run 的冻结预算。Workflow revision 不允许变更此快照。配置变更后重启 Core，新创建 Run 使用新配置；已有 Run/Attempt 不受影响。
- 旧 WorkflowGraph 没有此可选字段时，继续使用原任务和 Contract 默认预算；不改写旧 CAS、Contract 或 Attempt。
- Prompt 明示 resolved budget 及 cumulative input 语义。各次 AgentTurn 审计记录包含 `resolved_budget` 和 `budget_usage`，并通过现有 origin 关联 Run、Task、Attempt；保留独立的模型价格/成本 `budget_policy`。

## Inspect

连接已经运行的隔离 Debug Core，使用它的配置文件：

```bash
cargo run -p akzio-cli -- --config config/debug-controller-fixture.toml debug inspect RUN_ID
cargo run -p akzio-cli -- --config config/debug-controller-fixture.toml debug inspect RUN_ID --task TASK_ID --attempt ATTEMPT_ID
```

输出 `nodes[].budget.resolved`（保留兼容字段 `limits`）为持久化任务实际预算；有 Runtime 观察时，`last_runtime_observation` 包含：

- `resolved`：Runtime 实际采用的四项上限。
- `input_tokens_used` / `input_tokens_remaining`。
- `output_tokens_used` / `output_tokens_remaining`。
- `tool_calls_used` / `tool_calls_remaining`。
- `elapsed_millis` / `wall_time_remaining_millis`。
- 派生的模型调用次数及既有成本信息。

不限制工具次数时，`resolved.max_tool_calls` 显示 `"unlimited"`，`tool_calls_remaining` 与派生的 `model_calls_remaining` 为 `null`；`tool_calls_used` 继续累计。这时 `null` 表示无次数上限，可通过 resolved 值与未知用量区分。

观察带有时间戳和 Attempt ID，表示最后一个已持久化执行边界的用量，不是连续计时器。`--attempt` 筛选相应 Attempt 的观察记录。尚未开始时使用零用量与完整剩余额度；若只有历史调用记录但缺乏精确的当前阶段观察，不猜测剩余额度，保留 `null`。AgentTurn payload 的 `resolved_budget` 可用于核对调用 provenance。
