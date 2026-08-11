# Akzio v2 Prompt 与 Multi-Agent Debate 调研结论及 R11–R15 计划

日期：2026-08-11

用途：完成 `todo.md` 的调研项，并给出衔接当前 v2 工作树的最小实现计划。本文是设计与验证计划，不授权提交、推送、部署或真实 Alpaca Paper 操作。

## 结论

1. 采用混合 Prompt：共享治理规则与角色 Prompt 分开、都进入 immutable `AgentContract` 的组合 hash；运行时 objective、`ContextManifest`、tool result 不进入 Contract hash，而以 request fingerprint 和 durable refs 重放。
2. 工具名、描述、input schema 与 strict 标志都是模型可见行为，必须和 output schema 一样进入 Contract hash，并由 Rust 二次校验。动态 evidence 始终作为 data/tool result，不得升级为 instruction。
3. 不实现默认自由式多轮 Debate。保留独立 Analyst，再在冲突、证据缺口或执行相关高风险时触发一次 evidence-bound Critique；必要时只允许一次受治理的补证据/复核。
4. 不把 consensus、相似 reasoning、persona 名称、temperature 多样性或 Paper 单次盈亏当作正确性证明。未解决的 material conflict 必须保留并由 Rust `DecisionGate` 产出 blocker/`NoOrder`，模型无“commit”权。
5. Debate 只能作为 capability-bounded candidate topology。它需在相同 Context、风险门、执行引擎和 outcome horizon 下，与不含 Critique 的基线进行 paired Shadow/Canary 评估；只有 sealed canonical Paper outcome 可影响 learning policy。

## 来源与适用边界

| 来源 | 对 Akzio 可用的结论 | 不可外推的部分 |
| --- | --- | --- |
| [MAD survey](https://arxiv.org/abs/2607.26212) | 将 participants、communication、agreement、round/token/latency 作为显式、可评估的系统参数；不要默认全连接广播或以 persona 制造异质性。 | 没有统一最佳轮数或通用最优拓扑。 |
| [Consistency Illusion](https://arxiv.org/abs/2606.08457) | `Claim + Ground + stance + counter-ground` 的结构化反驳优于自由聊天的可审计性；保留独立初稿。 | reasoning alignment 不等于正确性，也不是金融验证。 |
| [Biased Consensus](https://arxiv.org/abs/2608.02827) 与 [DEAR](https://arxiv.org/abs/2608.03648) | 通信密度、轮数和从众是风险变量；应只暴露相关反方结果、隔离少数意见。 | 温度或语义相似度不能替代 provenance、Rust gate 或真实证据。 |
| [PROClaim](https://arxiv.org/abs/2603.28488) | 原子 claim、支持/反对证据、source-bound counterclaim、evidence-gap 驱动补证据和 plateau stop 是有价值的模式。 | 其十轮、重检索、约 11 倍 token 流程不能作为默认路径；模型不能直接检索。 |
| [Macro Economists](https://arxiv.org/abs/2606.08283) | 同一数据/组合引擎、按分歧触发、限制轮数、按 regime/成本评估是正确比较方式。 | 商品 ETF 短样本没有证明 Debate 稳定优于最佳单 Agent，不能外推到 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。 |
| [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/multi_agent/) 与 [Anthropic tool use](https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/implement-tool-use) | 代码编排适合确定性的串并行与 evaluator loop；tool definition、描述和 schema 是模型上下文的一部分，tool result 必须按 call id 完整配对。 | SDK 的 handoff/agents-as-tools 不能绕过 Akzio 的 Task、permit、manifest、budget 或 durable provenance。 |

## 目标设计

### Prompt 组成

```text
Contract hash = governance blob + role blob + tool specs + output schema
Request fingerprint = Contract hash + objective + Manifest/Grant refs + prior tool-result refs
```

- **治理层**：Paper-only、权限/预算/生命周期、仅 Manifest/Grant 读取、禁止网络/文件系统/凭据/订单。它是 Rust enforcement 的冗余说明，不是安全边界。
- **角色层**：Planner、Analyst、Critic、Synthesizer 的职责与 strict 输出要求。不能承载可变 source scope 或执行权限。
- **运行时层**：objective、已选择的 artifacts、当前 evidence gap、prior tool result。仅在 request 中出现；Context repair 生成新 artifact/manifest，不能静默替换。

现有工作树中的 `PromptBundle`、`ToolSpec`、strict Responses function tools 和 request/tool trace 正在覆盖上述 R11 基础；不应另起 prompt registry、模板 DSL 或 agent handoff 框架。

### Debate 协议

```text
independent Analyst claims
  -> Rust trigger? --no--> Synthesizer
                     --yes--> one Critic with claim + grounds/gaps
                                   -> optional governed EvidenceNeed + one re-check
                                   -> Synthesizer
  -> unresolved material conflict => DecisionGate blocker / NoOrder
```

- Analyst 的异质性来自问题分片、证据集合、假设/分析方法，不是只改 persona 文案或 temperature。
- Critic 必须指向一条 Claim，并携带 counter-ground 或明确的 `EvidenceGap`；只有“不同意”不是有效 Critique。
- Rust 触发条件：方向相反的 material claim、关键证据缺口、Critic/Decision 相关 blocker，或预算内的高影响不确定性。模型不能自行增加轮数、参与者或 source scope。
- R13 初版最多一次 Critique 和一次补证据后的复核；不做自由 peer transcript 广播，不强制 consensus。
- “Disagree-or-Commit”在 Akzio 改为“Disagree → evidence-bound critique → resolve or abstain”；只有 Rust ExecutionRuntime 能 Paper commit。

## R11–R15 计划

| 阶段 | 目标与最小改动 | 完成标准 |
| --- | --- | --- |
| **R11：Contract-bound Prompt**（当前 WIP） | 固化 `PromptBundle`、`ToolSpec`、output schema 的 canonical hash；将动态 input 留在 request fingerprint；严格 function schema + Rust validator；保存 request、hash、原始 tool args。 | 改治理/角色 prompt、tool 描述/schema、output schema 中任一项都会改变 Contract hash；仅改 Manifest/objective 只改变 request fingerprint。 |
| **R12：Grounded arguments**（先收口当前 WIP） | 让 `ResearchClaim` 直接闭包到 evidence，`ResearchCritique` 闭包到 Claim + counter-ground/gap，拒绝 Manifest 外 ref；修复仍产出旧宽松 Claim JSON 的 fixture，而不是放宽 validator。 | Claim/Critique/Resolution 的 schema、source closure、unknown/extra field、Manifest escape 都有窄测试；daemon fixture 与全 workspace 离线回归转绿。 |
| **R13：Triggered structured Critique** | 在 `akzio-runtime` 的工作流 lowering 中加入 Rust-owned Critique trigger 与一次上限；复用现有 `research.analyst`、`research.critic`、`research.synthesizer`。将 unresolved material conflict 传给既有 DecisionGate。 | 独立初稿先持久化；低价值任务不产生 Critic；每个 Critique 指向一条 Claim 并有 ground/gap；超轮/越权增 task/source 被拒绝；未决冲突无法得到 Accepted decision。 |
| **R14：可重放失败轨迹与受治理补证据** | 对每个 model tool call 持久化 call/result/error 的配对记录，包括 strict-argument、grant、closure、expiry 失败；以 `EvidenceNeed -> adapter -> new detail/manifest` 完成补证据。第一版保持工具串行、稳定排序，不为假设的吞吐量添加并发调度。 | 任意失败 call id 都有 trace；authority/closure 失败 fail-closed，只有明确可修正/transport 类错误走 Contract retry；补证据不暴露 HTTP、filesystem、raw CAS 或凭据给模型。 |
| **R15：金融验证与候选化 rollout** | 将“Critique enabled”与基线拓扑作为 candidate 对比。固定 Context、risk gate、execution engine 与 outcome horizon，分 regime 记录风险召回、证据完整度、校准、相对 QQQ utility、交易成本、token、P95 latency、tool/retry failure。 | Debug/Replay/Dry Run/Shadow 不直接 promotion；每个 paired result 绑定 canonical parent Decision/ExecutionContext 和 sealed outcome；风险召回或证据完整度下降立即 rollback，达到最小样本和 canary 门槛前不启用为 Active。 |

## 执行顺序与明确不做项

1. 先使 R12 的 daemon fixture 与窄测试转绿，再做 R13；当前 R12 红灯不是放松 source closure 的理由。
2. R13 只接入条件化 Critique，不新增默认第五个 `research.resolver`、通用 debate engine、自由 transcript storage 或 LLM handoff。现有 Synthesizer 和 DecisionGate 足够表达接受、保留冲突或 abstain。
3. R14 再补全失败 audit 与 evidence repair；当前仅 `read_artifact` 工具时不实现 parallel tool scheduler。
4. R15 先跑 deterministic fixture/replay evaluation，再进入由 scheduler 拥有的 Paper Shadow/Canary；不把单次回测或 Paper 盈亏解释为 Debate 的有效性。

## 验证顺序

每阶段先跑所涉及 crate 的窄测试；R12/R13/R14 增加 fixture：schema/hash mutation、Manifest escape、ungranted tool、tool error pairing、one-critique cap、unresolved conflict blocker、evidence repair closure。R15 增加同一 Context/plan-hash 的 baseline/candidate pair 与 rollback fixture。最后按 `AGENTS.md` 离线执行 `fmt`、`check`、`clippy -D warnings`、workspace tests、fresh Store Root `fixture-debug` 和 `store doctor`。

