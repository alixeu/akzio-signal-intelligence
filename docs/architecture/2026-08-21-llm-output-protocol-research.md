# Akzio v2 LLM 输出协议调研：Structured Output、自然语言与 Tool Calling

日期：2026-08-21

用途：判断 Akzio v2 是否应把目前以 strict JSON 为主的模型最终输出，改成自然语言正文加 tool/function call，并给出符合 Rust-owned policy、`V2Store`、provenance 和 Paper-only 安全边界的建议。本文只做调研与设计判断，不授权生产代码修改、真实 Alpaca Paper 操作或 canonical learning 变更。

外部资料仅使用供应商官方文档、MCP 规范和原始论文；统一查阅于 2026-08-21。仓库现状来自当日 on-disk 工作树，其中相关文件存在未提交改动，所以这里只描述当前形态，不把它当成已经合并的长期契约。

## 结论

**不建议把所有 strict JSON 一律改成原始自然语言。建议采用双通道协议：自然语言负责解释，严格 tool call 或严格结构化输出负责机器契约，Rust 负责验证、授权、构造领域对象和持久化。**

核心边界是：

1. 研究分析、批判、综合说明、引用与不确定性表达，可以保留自由度较高的自然语言。
2. 任何要被 Rust 消费、改变状态、触发读取、形成 claim/decision/action 或进入 `V2Store` 的内容，必须通过小而严格、用途明确的 schema 进入系统。
3. tool call 只是模型提出的请求，不是执行权。Rust 必须继续执行 permit/grant、source closure、domain validation、budget、lease/epoch、idempotency 和 gate 检查。
4. 不应提供 `save_json`、`write_store`、`persist_anything` 这类通用写工具。更安全的形态是 `propose_*` / `submit_*`，由 Rust 验证后构造 canonical artifact；模型不能直接写 Store。
5. 纯抽取、分类、固定 UI payload、planner DAG 这类天然 machine-first 的任务，继续使用直接 Structured Output 通常更简单。复杂研究类任务才值得试验“自然语言解释 + 结构化提交”。

换句话说，用户观察到的方向有一半是对的：**不应强迫模型把所有分析表达都塞进一个庞大的 JSON；但也不应让 Rust 去解析一整段自由文本。**

## 1. 先区分四种东西

| 层 | 形态 | 适合承担 | 不应承担 |
| --- | --- | --- | --- |
| Assistant text | 自然语言正文 | 人类可读研究报告、解释、引用、不确定性、反方观点 | 机器授权、状态转换、订单或 canonical 字段 |
| Structured response | `text.format` / JSON Schema | 抽取、分类、固定数据产品、无副作用的最终 payload | 自动证明字段真实、来源闭包或业务合法 |
| Tool/function call | 工具名 + strict JSON arguments | 请求外部能力、提交 typed proposal、把意图交给 host | 绕过 Rust gate 或直接获得 Store/broker 权限 |
| Domain artifact | Rust 类型 + provenance + Store transaction | 系统事实、耐久状态、决策/执行/学习记录 | 由模型自行宣告成立 |

Tool call 的 arguments 本身仍然是结构化生成。把直接 JSON 改成 tool call，并不会自动提升模型的推理质量；它主要改变的是协议语义：从“这是最终答案”变成“请 host 尝试执行这个明确动作”。OpenAI 官方也明确把两者分开：连接应用功能、数据和工具时使用 function calling；需要让面向用户的回答遵循结构时使用 structured response format。[1][2]

## 2. 一手资料支持什么

### 2.1 OpenAI：Structured Output 和 function calling 解决不同问题

OpenAI 当前指南把 Structured Outputs 分成两个入口：function calling 用于让模型连接应用里的工具、函数和数据；`text.format` 用于让模型给用户的最终回答遵循 schema。Function calling 的标准循环是模型返回 call，应用执行，再按 `call_id` 返回 tool output；API 不会替应用执行函数。[1][2]

`strict: true` 能显著提高 JSON Schema 形状的可靠性。OpenAI 2024 年发布材料报告，其复杂 schema eval 上，新模型的 strict Structured Outputs 达到 100% schema adherence，而旧式 prompting 低于 40%。但同一官方材料也强调：schema 正确不代表字段值正确；模型仍可能在不兼容输入上产生错误值，拒答和截断也需要调用方显式处理。[1][3]

Structured Outputs 与 strict function calls 底层都依赖约束解码/grammar。它们减少的是解析和形状错误，不是事实错误、权限错误或领域错误。首次使用新 schema 还可能有 grammar 编译延迟。[1][3]

Responses API 原生把 assistant message、reasoning item、function call 和 function-call output 表示为不同 item，并用 `call_id` 串联调用结果。这说明“人类可读文本 + 机器可执行动作”本来就可以是两个通道，不必揉成一个大 JSON。[4]

OpenAI 的 agent 安全指南进一步建议：不要让不可信自然语言直接驱动高权限行为；节点之间应提取并传递经过验证的结构化字段，同时保留 tool approvals、输入校验和明确的授权边界。[5] 这与 Akzio 的 Rust-owned gates 一致，也意味着“返回纯自然语言再由 Rust 猜字段”是倒退。

### 2.2 Anthropic：可以组合，但不要依赖 tool call 周围的自然语言

Anthropic 同样把 JSON outputs 定位为抽取、分类、结构化报告和 API response，把 strict tool use 定位为 agent workflow 中 schema-valid 的工具参数；两者可以组合使用。[6]

Anthropic 的 tool-use 文档明确指出，模型可能在 tool call 前后附带自然语言，但应用代码不应依赖这段文本的存在或固定格式。若强制 `tool_choice: any` 或指定某个工具，Claude 通常不会在调用前生成自然语言说明；而把 tool call 作为结构化输出技巧使用时，完整循环还会增加一次往返。[7] 因此，系统正确性不能建立在“一次响应一定同时含正文和 tool call”上。

对研究型任务还有一个实际取舍：Anthropic 当前 Structured Outputs 文档说明，其 JSON output format 与 citations 功能不兼容；新 schema 也会有 grammar 编译和缓存成本。[6] 这支持把带引用的研究正文保留为自然语言，同时把最小可执行字段单独结构化。

### 2.3 MCP：模型选择工具，host 仍负责控制、验证和审计

截至 2026-08-21，MCP 当前工具规范版本为 2026-07-28。规范把 tools 定义为 model-controlled，但要求保留用户拒绝/确认能力；工具可声明 `inputSchema` 和 `outputSchema`，server 必须校验输入并执行访问控制，client 应校验结构化结果。规范还明确区分 MCP tool result 的 `structuredContent` 与 LLM provider 的 Structured Outputs，两者不是同一层。[8]

MCP 的安全条款要求 server 做输入校验、访问控制、限流和输出清理，client 侧做 tool-result 校验、超时和审计记录。[8] 对 Akzio 的直接含义是：模型生成了 schema-valid arguments，也只能证明“形状能解析”；Rust host 仍是最终 authority。

### 2.4 原始研究：格式约束可能影响推理，但不能推导出“JSON 永远更差”

Tam 等人在 EMNLP Industry 2024 的实验中比较了五个 LLM 在自然语言、JSON、XML 等格式限制下的表现。论文报告：推理任务在更严格格式下经常退化，而先自由生成自然语言、再转成目标格式的 NL-to-Format 方法在多数设置中接近自由文本；但分类任务并不遵循同一规律，有些结构格式反而更好。[9]

这是一项支持“分析表达和格式提交分离”的方向性证据，但有严格边界：它研究的是 2024 年模型与特定任务，不等价于当前 provider 的 strict grammar、reasoning models 或 Akzio 的领域 schema。不能据此进行全量迁移，必须在当前模型、当前 Context 和当前 Rust validators 上做 A/B eval。

JSONSchemaBench 也提醒，结构化输出系统不能只看 schema pass rate，还要同时评估 schema coverage、约束解码效率和生成质量。[10] 对 Akzio，真正有意义的指标还必须包括 semantic reject rate、source closure、provenance 完整性和 gate 结果。

## 3. 当前 Akzio v2 已经具备的能力

当前工作树不是“只有 JSON、没有工具”的单一路径，而是已经同时具备两种机制：

- [`ModelRequest`](../../crates/akzio-model/src/lib.rs#L140-L165) 同时包含可选 output schema 和 function tools；[`ModelResponse`](../../crates/akzio-model/src/lib.rs#L213-L220) 同时保留 `output_text`、`tool_calls`、原始 provider response 和去凭据 request body。
- [`responses_request_body`](../../crates/akzio-model/src/responses.rs#L198-L245) 在有 output schema 时发送 strict `json_schema`，在有 tools 时发送 strict function declarations；两者可并存。
- [`ModelClientAdapter`](../../crates/akzio-research/src/agent_v2.rs#L721-L793) 当前把最终 `output_text` 解析为 JSON `Value`，同时把 provider tool calls 转成 `AgentToolCall`；debug 模式可保留 provider request/response trace。
- [`AgentRuntime`](../../crates/akzio-research/src/agent_v2.rs#L1117-L1142) 先执行受预算约束的 tool-call loop，tool calls 结束后要求一个最终结构化 output。
- [`ToolSpec`](../../crates/akzio-domain/src/contract.rs#L232-L255) 已绑定 Rust `ToolKind`、input schema 和 strict 标志；contract 不能自行声明未获准的任意函数。

这条主干已经符合“模型提议，Rust 执行”的安全方向。真正要研究的不是推倒 Structured Outputs，而是：**哪些角色的最终 payload 应继续直接 JSON，哪些角色应把人类可读叙述从机器字段中拆出来。**

### 3.1 当前更应优先检查：tool loop 的 reasoning continuation

这里有一个与“最终输出是不是 JSON”正交、但更可能影响多步研究质量的问题：[`responses_request_body`](../../crates/akzio-model/src/responses.rs#L198-L245) 使用 `store: false` 并请求 reasoning encrypted content；但 [`ModelClientAdapter`](../../crates/akzio-research/src/agent_v2.rs#L715-L793) 把 provider response 收缩为最终文本、tool calls 和可选 debug trace。下一回合由 [`AgentRuntime`](../../crates/akzio-research/src/agent_v2.rs#L919-L939) 重新构造一个全新请求，只把 Rust tool result 放进 `prior_tool_results` JSON，而没有按 Responses API 的原生 item 协议回放上一回合的 reasoning item、function call 和对应 `function_call_output`。

OpenAI 对 reasoning model 的 function-calling 循环明确要求把 response 中的 reasoning items 与 tool output 一起传回；在 stateless 场景中，应回放完整 `response.output`，再追加带同一 `call_id` 的 function-call output。[2][4] 因此，Akzio 当前更值得先验证的是“provider continuation 是否丢失”，不是先删除最终 Structured Output。建议把 provider continuation 作为 `akzio-model` seam 内的 opaque value，由 `AgentTurn` 经 `V2Store` 留存并在下一工具回合回放；不要把它暴露成可依赖的 chain-of-thought，也不要让 provider-specific item 进入领域决策接口。

## 4. 对各类任务的建议

| Akzio 场景 | 建议输出协议 | 原因 |
| --- | --- | --- |
| Planner / workflow proposal | 保留 strict Structured Output | DAG、recipe、预算和依赖是 machine-first；自然语言不应成为 lowering 输入 |
| Evidence read / 补证据请求 | 保留或强化 strict tool call | 这是外部能力请求，需要 grant、scope、budget 和可审计 call/result |
| Analyst research | 试验自然语言正文 + `submit_claims` typed proposal | 正文适合保留论证、引用和不确定性；claim、stance、evidence refs 必须结构化 |
| Critic | 试验自然语言 critique + `submit_critique` typed proposal | 允许更自然地说明反例，但 target claim、counter-ground、gap 和 refs 必须闭包 |
| Synthesizer / decision candidate | 人类可读摘要 + strict candidate submission | UI/审计需要摘要；DecisionGate 只消费 Rust 验证后的 typed candidate |
| Classification / extraction / fixed UI payload | 保留 direct Structured Output | 无副作用且 shape 稳定，额外 tool loop 只会增加复杂度和延迟 |
| Execution、Paper submit/retry、learning promotion | 不授予模型提交权 | 必须继续由 Rust scheduler/gates/Store 状态机决定 |

`submit_*` 只是概念上的协议名称，不是本报告要求立即新增的 API。若实施，工具应表达最小领域意图，而不是通用持久化能力。

## 5. 推荐的目标边界

```text
optional assistant narrative
  - research explanation / citations / uncertainty
  - auditable but non-authoritative
                    |
                    | same turn/request/contract refs
                    v
strict propose_* / submit_* tool arguments
                    |
                    v
Rust serde decode
  -> schema + domain validation
  -> permit / grant / source closure
  -> budget / lease / epoch / idempotency
  -> decision / execution / learning gates
                    |
                    v
V2Store transaction + tool_result / rejection reason
```

有两个可行的文本承载方式：

1. **优先方式：严格 envelope 内保留自由文本字段。** 例如 tool arguments 中保留 `narrative_markdown`，其余 claim/ref/action 字段严格结构化。它仍是 JSON，但模型不必把每段论证拆成脆弱的嵌套对象；正文与机器字段也天然绑定同一 call。
2. **可选方式：独立 assistant text + tool call。** 正文只用于人类阅读；即便缺失、顺序变化或格式改变，也不影响 Rust 处理。必须用 request hash、turn artifact、call ID 和 contract hash 关联，不能靠文本位置关联。

若 provider 的 forced tool choice 会抑制自然语言，或者同一 turn 同时生成 message 和 tool call 不稳定，应使用第一种方式，或明确拆成两个 turn。两 turn 方案要额外评估延迟、token、正文与提交字段不一致的风险。

## 6. 不建议的方案

- 只返回长自然语言，然后由 Rust 用正则、JSON 提取提示或启发式解析 canonical 字段。
- 给模型一个通用 `save`/`write` 工具，把 schema-valid 当成 authorization-valid。
- 把自然语言 explanation 当作隐藏 chain-of-thought，或要求系统依赖完整思维链。只需要可审计的结论、依据、反例和不确定性。
- 因为 strict schema pass rate 很高，就跳过 Rust semantic validation、provenance、source closure 或 gate。
- 为了“自然”而把 planner、execution plan、order intent、policy transition 等 machine-first 对象改成自由文本。
- 把 fixture/Debug 的协议实验结果当作真实 Paper、Outcome 或 canonical-learning 证明。

## 7. 建议的离线验证

在任何生产改动前，固定同一模型版本、contract、ContextManifest、evidence 集和 task budget，至少比较：

- A：当前 strict final JSON。
- B：strict submit tool，arguments 中含自由文本 `narrative_markdown`。
- C：可选 assistant narrative + strict submit tool；若需要则使用第二 turn。

指标不能只有 JSON parse/schema pass：

- Rust semantic validation reject rate。
- `source_refs` closure、引用正确率与 evidence completeness。
- narrative 与 structured fields 的矛盾率。
- material conflict / abstain / blocker 质量。
- refusal、truncation、tool retry 和 missing-final-output 比例。
- 输入/输出 token、端到端延迟、provider schema 编译冷启动。
- 对相同 fixture 的任务正确率和回归稳定性。

该实验只应运行在 fixture/Debug 或明确的非 canonical shadow 环境；不触发 broker，不推动 topology/memory promotion。只有当 B/C 在语义质量上有稳定收益，且没有破坏权限、closure、延迟和 replay 语义时，才值得按角色逐步迁移。

## 8. 最终判断

Akzio v2 最合适的目标不是“自然语言替代 JSON”，而是：

> **自然语言负责表达；严格 schema 负责传输；Rust 负责决定什么是真的、允许什么发生、保存什么。**

优先保持 planner、抽取和固定 payload 的 direct Structured Output；先验证并修复 stateless tool loop 的 provider continuation，再选择 Analyst、Critic 或 Synthesizer 做小范围协议实验。无论使用 structured response 还是 strict tool call，模型输出都只是 proposal，不能成为 authorization、execution 或 durable truth。

## 9. 2026-08-21 implementation decision

The implementation decision intentionally goes beyond the conservative research recommendation: Akzio v4 migrates all five canonical research agents to a two-phase protocol. Draft produces an auditable natural-language memo and may use granted read tools; Submit replays the complete provider continuation and forces the zero-side-effect `submit_result` tool with the existing output envelope schema. Rust remains the only validator, authority and `V2Store` writer. Native direct Structured Output and its fixture path are removed rather than retained as a fallback.

## 一手来源

1. OpenAI, [Structured model outputs](https://developers.openai.com/api/docs/guides/structured-outputs), accessed 2026-08-21.
2. OpenAI, [Function calling](https://developers.openai.com/api/docs/guides/function-calling), accessed 2026-08-21.
3. OpenAI, [Introducing Structured Outputs in the API](https://openai.com/index/introducing-structured-outputs-in-the-api/), 2024-08-06, accessed 2026-08-21.
4. OpenAI, [Migrate to the Responses API](https://developers.openai.com/api/docs/guides/migrate-to-responses), accessed 2026-08-21.
5. OpenAI, [Safety in building agents](https://developers.openai.com/api/docs/guides/agent-builder-safety), accessed 2026-08-21.
6. Anthropic, [Structured outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs), accessed 2026-08-21.
7. Anthropic, [How to implement tool use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/how-tool-use-works), accessed 2026-08-21.
8. Model Context Protocol, [Specification 2026-07-28: Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools), 2026-07-28, accessed 2026-08-21.
9. Zhi Rui Tam et al., [Let Me Speak Freely? A Study on the Impact of Format Restrictions on Performance of Large Language Models](https://aclanthology.org/2024.emnlp-industry.91/), EMNLP Industry 2024.
10. Saibo Geng et al., [JSONSchemaBench: A Rigorous Benchmark of Structured Outputs for Language Models](https://arxiv.org/abs/2501.10868), arXiv:2501.10868, 2025.
