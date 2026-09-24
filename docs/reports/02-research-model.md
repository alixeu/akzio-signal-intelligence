# 02 — 当前工作树研究 Agent 与模型协议：只读源码核查

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

日期：2026-09-29（Asia/Singapore）。源码定位基线 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`；以**当前工作树内容**而不是纯 HEAD 内容为准。

## 0. 结论、核查范围与证据等级

**当前研究主干是 Rust 固定编译的三期限 Analyst/Critic → 一轮共享补采与受影响重跑 → N+1 组 Synthesizer/ProposalReviewer，不是 Planner 自由扩图，也不是所有角色通用 Draft→Submit。当前研究 Contract=69、PromptBundle=38、freshness candidate=70；Outcome 保持 63/35。四个研究角色均 Submit-only。**【源码：`crates/akzio-research/src/agent/catalogue.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`】

- 已先读根 AGENTS、运行时契约第 3/5/6/11/12 节（并补读相关权威边界）、prompt ownership、budget configuration、retirement。遵守只读边界；仅新建本报告，不改源码、配置、Store，不执行测试、模型、HTTP API、capability probe、采集或 Broker。开始时工作树已有 AGENTS、README、CI、daemon、store/debug_bundle、若干 docs 和 run_core 脚本等改动，均保留；研究/model/domain/application 目标范围当时无 Git diff。
- 本文证据等级是 **source-inspected / implemented-in-source**；没有本轮 `offline-verified`、`real-Paper-verified` 或 `outcome/learning-verified` 证据。读取测试只能证明存在相应断言，不能宣称本轮测试通过。
- 未打开任何 SQLite/Store；“冻结”的下文有两种含义，必须区分：**源码构造出的 canonical Contract/新 Run 默认快照规则**已核；**某个实际 Run 的持久化 Contract、模型请求、usage、输出和调用时间**未核。不能由本文给出某 Run 真正使用的模型或成功率。
- 调度/Runtime/Store 主事务、Evidence acquisition/source verifier 的完整事实校验、DecisionPolicy/ExecutionGate、Outcome 数值和学习逻辑交给主助手/其他 workers；本文只追到必要交界点。规则出处：`AGENTS.md`。

## 1. 可用于中文全流程说明的流程图

```text
Rust 编译并冻结 NodeSpec / Contract / resolved TaskBudget
        │
EvidenceGate 已完成的授权证据候选（本报告不展开采集）
        ├─ Analyst(t1, round0) ─→ Critic(t1, round0) ─┐
        ├─ Analyst(t3, round0) ─→ Critic(t3, round0) ─┼─→ research.supplement
        └─ Analyst(t5, round0) ─→ Critic(t5, round0) ─┘       │
                   三条路径之间可并行；同一对内有先后        │
                                         全 Run 一轮 / ≤8 个去重资源
                                                         │
                仅新增合格事实影响的期限：Analyst(round1) → Critic(round1)
                其他 refined 节点显式 Skipped，旧 CAS 留存
                                                         │
                Rust 按 horizon 选择有效 Claim + 指向该 Claim 的 Critique
                                                         │
                Synthesizer(revision0) → ProposalReviewer(revision0)
                       │拒绝                            │全17项通过
                       └→ Synthesizer(revision1) → Review …    └→跳过剩余修订
                            至多 N 次业务修订；连续相同拒绝可提前停止
                                                         │
                精确 DecisionProposal + 精确 ProposalReview → Rust Decision 交界
```

**“三期限并行”不是六个模型同时启动。**首次三个 Analyst 无相互依赖，各 Critic 只依赖对应 Analyst；supplement 等全部首轮路径；每次 Synthesizer 等全部原/重跑路径及上一 Review。lowering 再添加 EvidenceGate 依赖。实际同时运行数量/排队时间需调度与运行证据，不能从 DAG 断言。【源码：`crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`】

`max_proposal_revisions=N` 表示**初稿之外**的修订次数，默认 2；`21+2N<=32` 限制 N 最大 5。研究部分为 6 个初轮任务+1 supplement+6 refined+2(N+1) 个终稿任务；PositionPlan 总节点 `17+2N`，Paper `21+2N`（默认 21/25）。默认图有 18 个潜在研究模型任务节点，不等于真的调用 18 次：未受影响 rerun、已通过后的修订、部分 Critic 会跳过；provider/schema retry 又可能增加单任务调用数。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；文档汇总：`docs/agent-runtime-contract.md`】

## 2. 冻结身份、实际模型路由、预算：三者不能混为一谈

### 2.1 当前常量与历史身份

| 项目 | 当前源码事实 | 来源文件 |
|---|---|---|
| 研究角色版本 | `ACTIVE_CONTRACT_VERSION=69`；`ACTIVE_PROMPT_BUNDLE_VERSION=38` | `crates/akzio-research/src/agent/catalogue.rs` |
| freshness candidate | 版本 70；克隆 active Analyst，附加 freshness 指导、重新算 hash、安装为 candidate，不自动替换 active。candidate 的 `prompt.version` 也设为 70，不能称候选仍是 bundle38 | `crates/akzio-research/src/agent/catalogue.rs` |
| Outcome | 构造分支显式 `prompt.version=35`、`contract.version=63`，保留读工具，Required deliberation | `crates/akzio-research/src/agent/errors_catalogue.rs` |
| 版本阈值 | structured eligibility 起点57；Submit-only 起点65；reviewed research起点67；typed review issues起点69；这些是兼容门槛，不是当前 active 版本 | `crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-domain/src/research_review.rs` |
| 安装/读取权威 | catalogue 检查退休和 canonical upgrade，从 Store active heads 恢复；按安装 hash 查询，不从 candidate/本地默认猜测 | `crates/akzio-research/src/agent/catalogue.rs` |

Outcome 文档还写 Contract hash `c9556a7ca9000cd06b96e385876013e3ce06a330fb4d01a473a43ef2db067ddf`（`docs/research-protocol-retirement.md`）。**本文只确认 63/35 构造分支，不把文档 hash 当作本轮重算或实际 Store 验证结果**；未执行会 stage CAS 的构造器或 hash 回归。

### 2.2 实际模型路由不是角色名硬编码

1. 生产模型 adapter 仅实现 `OpenAIResponses`；配置包括全局 model/reasoning/language 与 `routes`，route model/reasoning 覆盖全局，release_date/knowledge_cutoff 在 route 缺失时继承全局。字段可选不等于校准资格已足够；本文不推断模型真实发布日期/知识截止日。【源码：`crates/akzio-model/src/lib.rs`】
2. Daemon 从配置逐条构造 stage model，并要求匹配 capability snapshot；`AgentSession.run` 按 task recipe 调用 `model_for`，后者把 `research.proposal_reviewer` **强制映射到 `research.critic`**，其他 purpose 取同名 stage route，找不到就用全局 model。正式 Reviewer 不是独立选择某个模型名，也不读取独立 reviewer route。【源码：`crates/akzio-daemon/src/orchestration/bootstrap.rs`；`crates/akzio-daemon/src/application/agent_session.rs`；`crates/akzio-daemon/src/lib.rs`】
3. 研究请求 `reasoning_effort=None`，provider wire 回退到选中 route 的 reasoning；**只有 Outcome Submit 显式改为 low**，Outcome Draft 仍走配置。不要把 Outcome 的 formatting override 讲成当前研究角色的第二阶段降 reasoning。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-model/src/responses.rs`】
4. 文档表宣称 Analyst/Synth=Luna high、Critic=Sol high、Outcome=Luna medium，但仓库提交模板只有全局 `gpt-5.6-luna` / `low`，无逐角色 Sol route。故**可说明文档目标路由，不可声称当前代码强制该分工或当前运行必然如此**。实际本地配置、冻结 Runtime identity、AgentTurn requested_model/actual_model 需主助手另核。【文档：`docs/agent-runtime-contract.md`；模板：`config/akzio.toml`；`config/akzio.observatory.toml`；telemetry：`crates/akzio-research/src/agent/model_types.rs`】
5. 注意旁路差异：`debug verify-research-quality` 实验入口会先尝试显式 reviewer route，再回退 critic；这**不是**正式 Daemon 的 model_for 语义，不能用实验路由解释正式 Run。【源码：`crates/akzio-cli/src/cli/main.rs`；`crates/akzio-daemon/src/lib.rs`】

### 2.3 immutable Contract budget 与新 Run resolved TaskBudget

| 角色 | Contract 构造基线：input/output/read tools/wall | 新 Run 默认有效预算：input/output/read tools/Attempt wall |
|---|---|---|
| Analyst | 48,000 / 1,000,000 / 4 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| Critic | 48,000 / 1,000,000 / 4 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| Synthesizer | 48,000 / 1,000,000 / 2 / 120s | 1,000,000 / 1,000,000 / unlimited / 180s |
| ProposalReviewer | versioned budget 映射 Critic | resolved budget 映射 Critic（含覆盖） |
| OutcomeWorker | 12,000 / 4,000 / 2 / 180s | 1,000,000 / 4,000 / unlimited / 180s |

表中两列**有意不同，不应把 Contract 内 48k/120s 错报为新 Run 有效额度，也不能把新 Run 1M 写成 Contract hash 内预算**。`versioned_contract_budget` 负责前列，`default_agent_budget`/`AgentBudgetConfig.resolve` 负责后列；覆盖顺序为 role→default→代码默认。Reviewer resolved 时先映射到 critic，因此得到180s；不要直接拿 `default_agent_budget("research.proposal_reviewer")` 绕开正常 resolve 链来解释有效预算。【源码：`crates/akzio-domain/src/budget.rs`；Contract消费：`crates/akzio-research/src/agent/errors_catalogue.rs`】

- Rust 创建图时保存 resolved `agent_budgets`；node 要与图中的 budget 对应；AgentRuntime 再核对 permit、持久化 node、传入预算、retry、failure policy。修改配置不为既有 Task 充值。【源码：`crates/akzio-runtime/src/runtime.rs`；`crates/akzio-runtime/src/runtime/compilation/evidence.rs`；`crates/akzio-runtime/src/runtime/compilation/validation.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- input/output 是 **Task 累计**，跨 provider retry、schema repair 和 Retry/Recovery lineage；wall 是当前 Attempt 的持久化起点。新的 proposal revision/refined 是**不同冻结任务、各自预算**，费用可累加到 Run，但不存在“整个 Run 只有1M”。【源码：`crates/akzio-research/src/agent/recovery.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；文档：`docs/agent-budget-configuration.md`】
- 研究单请求 output cap=`min(Task剩余, 1,000,000, provider声明上限)`，再受可选硬成本上限约束；不是目标生成长度，不保证 provider 支持1M。有限 read-tool limit 派生 `max_model_calls=retry.max_attempts*(limit+3)`；unlimited 取消此派生次数上限，**不**开放读工具，不取消 token/wall。`submit_result` 不按 read tool 次数收费。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】
- Context 独立限制仍为24项；Synth 192KiB、其他128KiB；研究 Contract 的 Context max_tokens 基线48k，不等于累计输入1M；max_source_bytes 为对应字节上限×4，Raw reread=false。实际模型仅得到投影，不因较大累计额度而拿到原文/SQL/联网权。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`】

配置值还受启动校验：input/timeout为非零u32，output为1..=1,000,000；read工具次数为0..=65535或`"unlimited"`，0只读工具不禁止terminal submit。未知字段/角色由serde deny_unknown_fields拒绝，各default/role覆盖逐项验证，不能用有效override遮住非法default。【源码：`crates/akzio-domain/src/budget.rs`】

## 3. Prompt 的真实消费者：正文不是直接随文件热加载

1. `prompts/mod.rs` 用 `include_str!` 编译嵌入共享治理及完整角色正文；安装 canonical Contract 时把两份正文和 envelope schema stage 到原 CAS，再计算 Contract hash。运行时从 **installed Contract 的 governance/role/schema blob**读，不直接读取本地 Markdown，不把 AGENTS/Skills作为运行时指令。【源码：`crates/akzio-research/src/agent/prompts/mod.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；边界说明：`docs/prompt-ownership.md`】
2. 当前请求由 `structured_request_prompt(governance,role,language,ledger,output_kind,budget,version)` 拼接；阶段协议是 Rust 常量；Analyst/ Critic/ Synth 分别取得单Claim/精确核验/12预测配置规则，Reviewer 则靠自己的完整 role 文本与 Review schema。NodeSpec 把 horizon/round/revision 渲染为 objective marker，不由模型自行决定期限。【源码：`crates/akzio-research/src/agent/prompts/phases.rs`；`crates/akzio-domain/src/workflow_definition.rs`】
3. 请求还追加 Analyst ground 去重、Critic supported/no-conflict 的短指导；授权 materialization、引用 ledger、动态 Schema 与角色正文共同组成实际输入。故“检查 role.md”不足以证明实际 Prompt；本文已追到 assembler 和 adapter。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/prompts/phases.rs`】
4. **Wire ArtifactRef 只有 artifact_id，不带 kind**：`bind_reference_schema` 从当前 Manifest 按原字段允许 kind 收窄 ID enum，并删除 kind property/required；返回后 Rust 从 immutable ledger 精确回填。不会修复错误ID、替换证据或偷偷去重；allocation refs只排序，重复保留给校验拒绝。【源码：`crates/akzio-research/src/agent/helpers.rs`】
5. Provider 层把 terminal tool 设为 `strict=true`、`RequiredFunction("submit_result")`。但是 `provider_schema` 会移除本地数值/长度/数组数量/unique等约束，并把所有 properties 写为 required；所以 **provider strict 并不是完整 Rust schema 已验证**。完整的 bound Schema 留在 Rust 返回路径再校验。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/schema.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
6. Prompt registry 记录静态ID、路径和内容；component hash 包括多个请求/校验源码和 registry，不能把“角色文字没改”推成 RuntimeIdentity 未变。【源码：`crates/akzio-research/src/prompt_registry.rs`；`crates/akzio-research/src/lib.rs`】

## 4. Analyst：每期限一个 Claim，而非每资产一个 Agent

**输入**：本 Run EvidenceGate 祖先产物中的 `NormalizedEvidence/SemanticDetail` 候选，经过 canonical evidence 检查、ContextPolicy/Manifest/ReadGrant 和投影。初轮不接收其他 Analyst 结论；refined 只在该 horizon受新增事实影响时运行，按 resource 以新证据替换旧候选，原CAS不改。角色 Contract 只准这两类输入，不准 Claim/Review/Lesson/RawEvidence。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_loop.rs`】

**输出**：一个 `ResearchClaim`，含 schema_version/topic/statement/horizon/stance/materiality_ppm/confidence_ppm/grounds/evidence_gaps；stance只有 bullish/bearish/neutral；单次只有一个horizon，且必须等于Rust NodeSpec；不是4份独立 stance。grounds 1–12、每个精确证据引用只出现一次；gap最多2。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】

**实际角色指导**要求使用给定投影的精确整数和单位、不凭心算误转ppm/百分比，不把未采用当未提供；区分“采集不可用、已采未选、投影省略、提供未采用”；官方 research:*材料不是近期新闻，未source-verified新闻只能作背景；模型不能自行联网补齐。【Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

### confidence 与 uncertainty 的精确关系

- Claim 自己的 `result.confidence_ppm` 与 `materiality_ppm` 都只在0..1M；没有在 `ResearchClaim.validate()` 中强制它与 wrapper 的 `deliberation.confidence_ppm` 相等。**不确定性守恒是针对 deliberation.confidence_ppm，不是自动针对 Claim.confidence_ppm。**【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-domain/src/contract.rs`】
- 所有角色 envelope 的 deliberation 有 selected_path、alternatives≤3、对应 alternative_match_ppm、uncertainties≤3、对应 uncertainty_weight_ppm、basis IDs≤8、confidence。当前研究 wire再要求至少1个 basis ID。两组分数数组长度须与文本一一对应；每个分数0..1M；uncertainty weights恰好合计 `1,000,000 - deliberation.confidence_ppm`。alternative_match 不要求合计1M；空 uncertainty array因此只有confidence=1M时守恒成立。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-domain/src/contract.rs`】
- Rust加 `assessment_source="model_assessed"`、用独立DeliberationNote保存；Artifact provenance confidence固定1M，**这只是Rust元数据，不是研究置信度100%**。自评不得提高后续Context选择优先级；deliberation不是隐藏思维链，也不能补正式 grounds。【源码：`crates/akzio-research/src/agent/runtime_helpers.rs`；`crates/akzio-research/src/agent/prompts/phases.rs`】

## 5. Critic：对本期限 Claim 做独立但受控的核验

**输入**：仅选择同horizon有效Claim；自身Contract还允许NormalizedEvidence、SemanticDetail、DeliberationNote。Context必须纳入Claim正式grounds闭包；当前69还必须纳入Critique引用闭包。其投影可以有Analyst未选中的额外证据；不能把“我的Context有”误判成“Analyst声称它在整个采集中不存在”。Prompt明确使用 producer_context_scope 标识区分。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/critic.md`】

**是否调用**：`should_run_structured_critique` 要求至少一Claim materiality≥500000、非neutral，或同topic/horizon多Claim方向冲突；当前单Claimhorizon路径中，中性且低materiality可跳过。69 bounded loop返回 **Skipped**；旧通用路径的“NoOutput”不应直接套到69。Claim仍保留，Synth依赖并引用它，未有资格的slot必须中性。【源码：`crates/akzio-runtime/src/runtime.rs`；`crates/akzio-runtime/src/runtime/compilation/helpers.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；旧路径对照：`crates/akzio-daemon/src/application/research_run.rs`】

**输出**：target Claim ref、topic、severity(low/medium/high)、blocker、rationale、grounds≤12、gaps≤2、verification_status、supporting_refs≤12、conflicting_refs≤12。实际JSON枚举小写 `supported/contradicted/not_enough_information`，Prompt大写是描述，不是wire值。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-domain/src/research.rs`】

关键硬检查：

- grounds与gaps不能同时为空；supporting/conflicting refs必须同时在Critique自己的grounds内，且不得重复。SUPPORTED必须有支持、无反证，每条支持都 `authority != unrated` 且 `temporal_validity=valid_at_decision_cutoff`；CONTRADICTED必须有反证。【源码：`crates/akzio-domain/src/research.rs`】
- 有blocking gap时blocker必须true。反过来blocker=true不必“一票否掉所有资产”：若其有scope blocking gaps则按slot命中；没有blocking gaps的blocker是Claim-wide。【源码：`crates/akzio-domain/src/research.rs`】
- Critic grounds不得超目标Claim grounds资产；gap scope可含目标Claim已声明gap资产（>=67），horizon不得越过目标Claim；SUPPORTED至少验证Claim的一条正式ground。Critic新证据/说明可以提供反证，但不能替Claim补缺失的price/macro formal grounds。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`】
- supporting_refs中的news额外读实际payload检查 `source_verified=true`。模型填写authority不构成来源验证；canonical schema也不能证明引用文字真的蕴含Claim。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；Prompt边界：`crates/akzio-research/src/agent/prompts/roles/critic.md`】

## 6. grounds、来源权限与 directional qualification

### 6.1 从授权到正式 ground 的检查链

- Context先拒RawEvidence、ExPost RegimeSnapshot，核对Contract允许kind/source family及精确内部producer；普通RunScoped材料还要同Run，只有既有canary父证据/学习overlay例外。**开发助手能读文件/工具可用，不等于运行时Agent有此能力。**【源码：`crates/akzio-context/src/context_broker/policy.rs`；治理正文：`crates/akzio-research/src/agent/prompts/shared.md`】
- wire Schema根据实际Manifest证据scope按相同scope聚合ID，构造anyOf：bars/news仅命名的单资产，受支持series宏观可覆盖四资产；未知scope或未verifiednews分支在当前contract只准descriptive、空assets、null domain。不能因关联ETF或同一指数而跨资产扩写。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- Rust返回路径再次验证同一个bound wire，然后正式ground逐项检查Manifest精确ID+kind、payload resource→domain/scope；Directional必须NormalizedEvidence、citations_complete=true、有domain/已知scope、非空且subset assets。news必须另有source_verified；`model_reviewed`或citation完整不能替代。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 官方research:*持仓/机制、corporate actions、calendar、options等不能自动当news方向事实。源代码仍有历史leveraged_terms→FundamentalsSemiconductor映射，但其asset scope为unknown，当前boundwire要求descriptive+null；不可借保留词汇复活方向能力。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/structured.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

### 6.2 单Claim资格，而不是把碎片拼成一个“支持”

`claim_slot_eligible(ref,claim,critiques,asset,horizon)`必须同时满足：

1. Claim合法、同horizon、非neutral、无命中asset×horizon的blocking gap；
2. **某一个**精确target该Claim的合法SUPPORTED Critique，没有该slot blocker；
3. **同一个Claim**正式grounds同时有price_market_structure和macro，均Directional且含该asset；
4. 该Critique自己的grounds逐个匹配这些ground的evidence、role、domain、asset，并有对应当前权威supporting_ref。

不能跨Claim拼price+macro，也不能拿Critic新增ground或deliberation代替Claim缺项。新闻不是当前最小方向资格的必需domain；news缺口是warning时不直接阻断，有实质blocking gap则照常阻断。【源码：`crates/akzio-domain/src/decision.rs`；gap规则：`crates/akzio-domain/src/research.rs`】

Context给Synth的`coverage_verification_matrix version3`直接调用同一资格函数，并列 eligible_claims及stance；提交校验也调用该函数链。**不是模型读矩阵后自行解释一套资格。** 非中性forecast还要求stance与expected_return符号一致；return=0时才用probability相对500000决定方向，允许均值与上涨概率因偏态分布符号不同。【源码：`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-domain/src/decision.rs`；Submit消费者：`crates/akzio-research/src/agent/errors_catalogue.rs`】

**资格的限度**：`is_current_authoritative()`检验的是typed authority/temporal标签；source/scope/citation/news_verified有Rust额外门，但这些函数不是自然语言蕴含证明器或通用数字重算器。完整上游timestamp/provider来源核验交给Evidence worker；不要把此处布尔通过说成“事实被独立数学证明”。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】

## 7. 一次正常 submit_result 的构建、校验、持久化

### 7.1 调用前

校验TaskWritePermit/task/Contract/hash/node与预算 → Context组装与materialize → 读冻结Prompt/schema → 收窄reference/ground/allocation wire → 选择Submit-only和terminal → 恢复原预算/continuation → 检查capabilities、input估算、output reservation和wall → **先写AgentTurnStarted，再poll模型Future**。【源码：`crates/akzio-research/src/agent/runtime_run.rs`】

能力检查要求 verified probe、stateless continuation，以及有工具时支持tool calls；即使首轮是Fresh/Submit-only，现实现仍要求stateless continuation，以支持受控repair。未知能力不能按“端点兼容”放行。【源码：`crates/akzio-research/src/agent/validation.rs`；未probe快照：`crates/akzio-model/src/model_client/client_setup.rs`】

### 7.2 Provider请求与返回

`ModelClientAdapter`的Fresh input包含objective、context_manifest、context；Continue用既有transcript+tool outputs+instruction。Responses发送`POST {base_url}/responses`，`store=false`、`stream=true`、`reasoning.summary=auto`、请求encrypted continuation；普通研究只暴露submit_result，没有native web工具，即使model crate有web能力也不会自动开给研究Agent。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/responses.rs`；研究工具面：`crates/akzio-research/src/agent/runtime_run.rs`】

SSE必须有状态匹配的completed/incomplete terminal；[DONE]或EOF不是业务完成证明。incomplete/refusal/空输出先失败；成功continuation保留实际历史input+全部response output，而不是依赖provider服务端会话。reasoning stream是provider摘要观察，不是正式DeliberationNote或成功提交。【源码：`crates/akzio-model/src/responses.rs`】

### 7.3 返回后的顺序不能倒置

1. 先持久化AgentTurn（包括原始terminal arguments），累计实际usage；超request cap、input/output超限或usage不一致则失败，不因得到JSON而修复/继续。
2. Submit不准assistant_text或其他tool calls；必须唯一terminal submission，多次submit也不择一接受。
3. 校验bound wire → 回填kind → Synth绑定Rust日历、Reviewer绑定精确身份 → 冻结canonical envelope schema → extract/validate deliberation → canonical result schema → Claim/Outcome horizon → 角色业务语义、引用闭包、directional与allocation资格。
4. 返回前写DeliberationNote，并stage正式result blob、构造`agent.<purpose>`、RunScoped output；source_refs含Manifest、AgentTurn/ToolResult/DeliberationNote trace与正式研究依据。**最终output Artifact此处返回给daemon；不是在这一函数里就发布成功Attempt输出。**主助手须在Runtime/Store整合时追TaskCompletion成功提交事务，不能把persist_stage当TaskSucceeded。

【源码：`crates/akzio-research/src/agent/runtime_run.rs`；adapter多submit拒绝：`crates/akzio-research/src/agent/model_types.rs`；返回TaskCompletion：`crates/akzio-daemon/src/application/research_loop.rs`】

## 8. Retry、schema repair、deliberation-only repair 与 usage fail-closed

### 8.1 四种“再来一次”必须分开

| 类型 | 边界/额度 | 证据 |
|---|---|---|
| Provider turn retry | Contract max_attempts=2、initial_backoff=250ms，transport/rate_limit/invalid_output flags=true；实际还必须满足usage与时间 | `crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs` |
| Rust InvalidOutput的schema/字段repair | 初提交之外至多一次；必须剩余至少当前phase deadline的50%；同Task同预算，feedback保留call_id | `crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/helpers.rs` |
| Scheduler Retry/Recovery | 新Attempt可能有新Attempt wall起点，但沿同Run/Task的Retry/Recovery历史恢复既花token/调用及失败状态；不是新Task预算 | `crates/akzio-research/src/agent/recovery.rs`；`crates/akzio-research/src/agent/runtime_type.rs` |
| Proposal业务revision | Reviewer拒绝后进入预冻结的另一个Synth/Reviewer节点，N为额外修订数，每节点独立Task budget；与格式repair不共用计数 | `crates/akzio-runtime/src/runtime/workflow.rs`；`crates/akzio-domain/src/research_review.rs` |

**retryable flag不保证真的发生第二次网络调用。** 当前generic provider失败没有闭合usage会`record_failed_turn`置`output_usage_unknown=true`；下一次`authorize_model_call`立即ProviderUsageUnknown（有硬cost还可能先CostUsageUnknown）。所以transport/timeout/429等即便被标为will_retry，也可能只记录意图后被账本阻断。`will_retry`不是已重试证据；需要下一条Started/实际request。ProviderIncomplete/ProviderUsageMissing不在普通自动retry允许列表。【源码：`crates/akzio-research/src/agent/helpers.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】

### 8.2 deliberation-only 是真正的结果冻结，不是“让模型重写一份差不多的”

- 判定目前是feedback.message包含字符串`deliberation`，不是完整typed路径分类器。Runtime从已持久化AgentTurn逆序恢复原始result与最近deliberation；使用仅含deliberation的terminal schema，清continuation/tool outputs，Fresh context只放旧deliberation、validation feedback、frozen_result_hash，外加immutable ledger/治理指导。模型看不到整份result，不能再次提交result。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- 修复响应若自己带result，即使内容相同，也被`bind_frozen_result`拒绝；缺result时由Rust把旧result补回，随后完整验证。记录前后result语义JSON hash/changed paths；仅deliberation错误却改result直接拒绝。普通non-deliberation repair仍可改变result，靠重跑全部校验；**没有通用Rust机制逐个锁住所有“前轮已合法字段”**，不能把Prompt“只改错误字段”夸大为全部字段的机器级锁定。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/runtime_run.rs`】
- **冻结不代表result已经业务通过。** 校验顺序中deliberation在角色语义/方向资格之前，第一次若只先暴露deliberation错误，后面的原result仍可能在修复后被拒绝。Rust不能通过修deliberation自动把未审result升级成合法产物。【源码：`crates/akzio-research/src/agent/runtime_run.rs`】

### 8.3 用量与恢复

- 真实OpenAIResponses缺input或output totals直接ProviderUsageMissing；只能fixture使用可见文本估算，防止遗漏隐藏reasoning。provider usage别名归一化，缺失保持None；reasoning属于output细分、cached属于input细分，不能大于total，也不再次叠加到token total。【源码：`crates/akzio-research/src/agent/model_types.rs`；`crates/akzio-model/src/responses.rs`；`crates/akzio-research/src/agent/runtime_type.rs`】
- 已发生usage先入累计账，再返回超限错误；输出reservation先放开再计实际，超单请求output也不“退款”。input缺失且output已知会留已知值/估算但调用仍失败，恢复仍保留Missing状态，不能推断合规。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/runtime_type.rs`；`crates/akzio-research/src/agent/recovery.rs`】
- Started在I/O前持久化；恢复按attempt+start cursor闭合。Started无terminal、历史不可解析、hash/phase不匹配但曾有provider work，返回Unknown而非fresh budget；guard核对Contract、Manifest、read-grant/materialization identity、request hash、capability、budget-policy、工具集合和continuation。metadata-only有独立tool hash且必须对应deliberation反馈，不是任意工具变更例外。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/recovery.rs`】

### 8.4 审计事实与覆盖范围

AgentTurn总有domain_request/request、request_hash、Contract/Context、ReadGrant观察快照、resolved_budget/budget_usage、capability与tool/budget policy hashes；成功/失败分别持久化。实际provider request/raw只在debug启用时附model_debug，不能说每个Run都保存真实wire；telemetry另有provider_request_id/response_id/requested_model/actual_model/latency/tokens。【源码：`crates/akzio-research/src/agent/runtime_helpers.rs`；`crates/akzio-research/src/agent/model_types.rs`】

`PipelineLatency`记录provider / parse_validate / persist_stage，phase=structured/draft/submit，revision参数实际为model_turn。**此StageAcceptance只在存在DebugSession时写**；结构修复hash/diff审计同样debug_session条件。`SubmitRejected`则所有Run都写，供恢复精确重建反馈。审计`Pass`仅表“测量/记录成功”，其中succeeded可能false，不代表模型业务通过。【源码：`crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/structured.rs`】

## 9. 全Run一轮补采与受影响重跑

### 9.1 模型只提交类型化意图

当前69 wire替换旧`SupplementalNeed/ResearchIntent`：gap里是`supplemental_requests:[{kind,assets,series,query}]`，不接受resource/date/window字段。kind=news/price/macro，assets非空最多4且不重复，series最多5且不重复；macro支持DFF/DFII10/VIXCLS/DGS2/DGS10并要求非空series，news/price要求series空；query非空。每gap最多8请求、Claim/Critique最多2gap。【源码：`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-domain/src/research_review.rs`；`crates/akzio-domain/src/research.rs`】

retriable+blocks_directional_forecast必须带请求；空gap assets=所有资产、空horizons=继承Claim horizon。**retriable是分类，不是自动兑现一次采集**；warning或不可重试请求仍可形成skip disposition，只有实质blocking进入候选采集。Synthesizer没有此协调入口，requests不是模型的联网工具。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/analyst.md`】

提交阶段typed意图校验与采集阶段资源绑定是两道门：`ResearchClaim/Critique.validate`调用intent.validate；当前旧`validate_supplemental_resources`只遍历historical supplemental_needs，不是当前typed请求已被完整绑定。真正资源展开在Rust协调节点，随后同adapter的GovernedResource parser解析。【源码：`crates/akzio-domain/src/research.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`】

### 9.2 Rust控制实际资源、时间与额度

- 图必须恰有一个supplement节点；冻结EvidenceNeed取自graph node inputs。news/price按asset前缀命中同Run冻结resource；四资产news展开4条，不算一条。macro可从冻结series模板换成受支持序列，保留后缀窗口，再parse与validate。**当前`expand_intent`没有把intent.query写入need.query，返回克隆冻结Need（macro只改resource）；query是意图说明，不是自由网络搜索原文指令。**【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- 原cutoff从祖先NormalizedEvidence的`time_basis.decision_clock.decision_cutoff`取最小可解析值，缺则拒绝；不使用重试时的现在替换。gap与request assets/horizon不得越界；请求稳定排序：horizon→非series优先→asset/series→kind，再以resource/requester/index打破平局。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- 本task历史读取started/disposition；distinct resource最多8。先检查done复用→started但未知完成则unknown_after_crash不重发→额度→**持久化started在EvidenceNeed创建和外部I/O之前**。原始CAS保留，恢复不重置额度。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】
- dispositions含skipped_impact、not_retriable、invalid_protocol、deduplicated、unknown_after_crash、budget_exhausted、collection_failed、no_new_facts、accepted；最终round汇总所有处置，即使没有新事实协调任务也可Succeeded，不能等同补采事实成功。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】

### 9.3 何时重跑

新引用必须citations_complete、news额外source_verified、available_at≤原cutoff，而且和同resource原证据的`fact_value`不同。news比较reviewed_facts+verified；bars比较bars/feed/adjustment；series比较observations/units，忽略retrieved/provider wrapper变更。因此“再抓一次换了retrieval时间”不算新增事实；这里是程序定义的新增合格事实判定，不是独立事实真值证明。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`】

只有有新evidence的请求horizon写入affected_horizons（dedup复用同样传播受影响期限）；refined pair按此决定run/skip。之后每horizon选research_round最高Claim，Critique仅取target该有效Claim者；不会把旧Claim与新Critique拼配；不再创建第二轮supplement，refined请求即使仍存在也只保留缺口。【源码：`crates/akzio-daemon/src/application/research_supplement.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；固定唯一协调节点：`crates/akzio-runtime/src/runtime/workflow.rs`】

## 10. Synthesizer：12预测、四资产+现金、17份估计依据

**输入**：有效Claim/Critique及正式grounds闭包、受控Evidence/SemanticDetail、允许且合格的Lesson/Experience/CandidatePolicy、DeliberationNote，以及修订时上一ProposalReview及其绑定旧提案。Context必需集合优先于optional background，>=67包含Proposal/Review、numeric_basis inputs、allocation refs、补采结果；24项/字节/token上限放不下则MissingRequiredInput，不以删反证/弱化闭包“尽量继续”。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-context/src/context_broker/manifest.rs`】

**输出完整DecisionProposal（Rust类型alias DecisionDraft）**：summary、confidence_ppm、12 forecasts、research_allocation、claims/critiques/evidence、material_conflicts、hard_blockers、soft_warnings，另numeric_basis及可选学习应用/拒绝refs。12行必须完整4资产×T1/T3/T5，概率0..1M，expected_return为有符号整数ppm；thesis有exit_condition和≥1 invalidation_conditions。ForecastThesis文字不是自动下单规则。【源码：`crates/akzio-research/src/agent/schemas.rs`；`crates/akzio-domain/src/decision.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

- 无资格slot必须 `probability=500000 && expected_return=0`。这里Rust拒绝非法非中性提交，**不是在Submit函数里偷偷把数字改为中性**；模型在正常输出/有界repair时应主动给出neutral。支持某Claim也不要求必须配非零仓位。【源码：`crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 四资产各唯一一行；整数非负weight≤1M，四行加cash严格1M。每行rationale；0行必须非空abstention_reason；非零行abstention_reason=null，supporting_horizons与evidence_refs非空，horizons/ref排序去重。非零多头至少有一个positive expected-return supporting horizon、同资产Bullish且eligible Claim并精确引用Claim/匹配Critique/方向grounds。Bearish不是买入机会，全部cash可合法。【源码：`crates/akzio-domain/src/decision.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- **17 numeric_basis**：`forecast.<asset>.<t1|t3|t5>`12项、`allocation.<asset>`4项、`allocation.cash`1项；exact set不得缺失/重复。每项inputs非空且精确选中，kind限定Claim/Critique/NormalizedEvidence/SemanticDetail；units/method/assumptions/uncertainty非空。Rust核对完整性与引用，不解析method自动重算收益、概率、风险折扣或重叠权重。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 所有已选Claim/Critique必须保留，包括neutral/blocked/rejected；grounds、supporting/conflicting证据必须在proposal.evidence及选中闭包中，不能因不投/中性删除血缘。numeric/allocation refs也进正式source_refs。当前wire还强制claims/critiques数组数量等于selected count且唯一。【源码：`crates/akzio-research/src/agent/structured.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- `thesis_valid_until/expected_holding_period_days`从wire移除；Rust读选中bars内forecast_session_closes，要求四资产都有日历且同日期close完全一致，选择**提交时刻之后**第1/3/5个共同close，填回expiry/holding期。模型若填timing被拒，日历是计划session元数据，不是未来价格。精确实现基准是`turn_now`，不能把Prompt“基准Session后”误说成程序直接拿固定cutoff按自然日加1/3/5。【源码：`crates/akzio-research/src/agent/structured.rs`；消费者：`crates/akzio-research/src/agent/runtime_run.rs`】

Prompt要求估计说明讲明观测、原单位、方法、假设、敏感性、资产重叠/现金理由，并明确这是模型原始估计，不能伪造校准/样本/公式。**此要求与Critic上游SUPPORTED不是“Critic核过最终12个数字”同一回事。**【Prompt：`crates/akzio-research/src/agent/prompts/roles/synthesizer.md`】

## 11. ProposalReviewer：精确提案审查、问题锁定与有界修订

### 11.1 输入/绑定/输出

- 每个Reviewer只选相同proposal_revision的DecisionProposal，沿它的正式source_refs展开Claim/Critique/Evidence闭包；Context必须恰一个完整proposal并保留必需basis。不是另一位Analyst，不设计替代组合，不做外网查证；路由/预算继承Critic。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 已检查真正的Context消费者：DecisionProposal/ProposalReview分别标为`final_proposal`/`proposal_review` must_read；`compact_governed_projection`对NormalizedEvidence压缩、对Claim/Critique等缩写叙事，但没有对DecisionProposal/ProposalReview走这些裁剪分支，因而完整提案/Review字段进入本次模型上下文。它们的证据输入仍是有界投影，不等于完整外部原文。【源码：`crates/akzio-context/src/context_broker/materialization.rs`】
- 模型wire不填proposal/proposal_hash/manifest/contract_hash；Rust取Manifest唯一proposal的实际blob.hash、当前Manifest和Reviewer Contract注入。Review绑定**精确ArtifactRef+内容hash**，同数值换正文也可能换hash，必须重审。【源码：`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-domain/src/research_review.rs`】
- 每份Review恰17 assessments，每scope accepted/rationale/evidence_refs/issues；每个拒绝项1–3 issues，通过项空issues。issue category枚举为support_missing/source_qualification/conflicting_support/temporal_mismatch/unit_error/estimate_basis_mismatch/allocation_inconsistency；field_path须等scope或scope.字段或numeric_basis.scope.字段，correction_criterion非空，refs限定已授权研究kind及Manifest。stable_id=`hash(scope,category,field_path)`，不含措辞。这里field_path是语义前缀检验，**不是验证每个字符串一定指向真实JSON字段**。【源码：`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/proposal_review.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- `accepted()`仅当基本validate有效且所有assessment accepted；`authorizes()`再比较proposal ref/hash。**这个函数本身不再次证明Manifest/Contract来源，也不调用validate_for_contract(69)**；issues由提交边界验证，其他authority交由Store/Gate整合。不要把函数名authorizes翻成“允许交易”。【源码：`crates/akzio-domain/src/research_review.rs`；提交调用：`crates/akzio-research/src/agent/errors_catalogue.rs`】

Reviewer Prompt逐项查input/units/method/assumptions/uncertainty与数值是否相称、是否忽略反证、配置折扣/资产重叠/现金是否自洽；允许有边界的模型估计，不能以缺少经验校准迫使模型编造样本，尤其不能因中性先验弃权无校准就自动拒绝。优先查可证伪的ppm/百分比、方法与数值冲突、现金叙述、时间、虚构样本。**这仍是模型审查，不是Rust算术验证或预测正确性证明。**【Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

### 11.2 拒绝后如何修，哪些锁住

- 下一Synth显式收到最新Review和它引用的旧Proposal；提交先确认旧proposal实际blob hash与review匹配，且旧proposal也在当前Manifest，再调用`validate_proposal_revision`。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`】
- 锁定所有**已通过forecast的模型字段及其numeric_basis**，比较typed scope值，不依赖数组位置；排除Rust重绑的thesis_valid_until和holding_period。已通过allocation/cash不锁，因为权重守恒与新forecast修复可联动；配置对应basis也可调整。所谓“逐字保留”在机器上是语义JSON/typed value相等，而不是原JSON空白/键顺序字节相等。【源码：`crates/akzio-domain/src/research_review.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/synthesizer.md`】
- Reviewer每轮仍审完整17项，新proposal始终是新产物；通过则后面所有bounded research修订节点Skipped；到N额度仍未过不生成伪通过。终稿到Decision的最终选择与blocker由主助手追Store/Gate，不把本报告中的Review通过当Policy/Execution许可。【源码：`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-runtime/src/runtime/workflow.rs`；Prompt：`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md`】

### 11.3 相同拒绝提前停止的精确条件

连续两份Review都在69问题规则下有效且都拒绝；把每个拒绝scope的issue stable IDs+排序去重issue refs、assessment refs、该scope的proposal value+basis生成fingerprint。两次完全相同则stagnated；rationale/correction_criterion改写本身不算进展，accepted scopes不是比较对象。**注意锁定函数剔除了Rust timing，stagnation fingerprint使用完整scope_value没有剔除timing，两者相等语义不是完全同一套。**【源码：`crates/akzio-domain/src/research_review.rs`】

下一Synth运行前按revision排序两份Review和它们精确提案，命中则产出RunScoped SemanticDetail `research.revision.stop`，reason=unchanged_rejected_scopes、previous/current review refs、issue IDs、`decision_authorized=false`。后续研究节点见stop立即skip，不再调模型；保留拒绝Review，不把停滞当通过。【源码：`crates/akzio-daemon/src/application/research_loop.rs`】

## 12. 与 Outcome 的协议边界（不展开数值逻辑）

Outcome是单独角色63/35，输入Contract允许Decision/DecisionContext/ExecutionContext/ExecutionVerdict/Commitment/Receipt/Reconciliation/OutcomeSchedule/Outcome等，输出RetrospectiveDraft；仍Draft→受控读→Submit，只有它拿5个Context只读工具。Draft最多占原总output的一半、70%墙钟，必须有非空memo才转Submit；Submit低reasoning formatting但同累计预算。模型负责质性叙述，不输出权威收益/滑点/回撤/Policy；这些由另一流程的Rust计算。【源码：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-research/src/agent/runtime_run.rs`；文档工具/叙述边界：`docs/agent-runtime-contract.md`】

当前研究首轮T1/T3/T5是“同时对不同期限预测”，不意味着等待T+1/T+3/T+5到期才运行Critic/Synth。历史Outcome时间轴与T0独立；本文没有检查任何实际Outcome成熟度、numeric metrics、calibration sample或学习晋升。【阶段Prompt：`crates/akzio-research/src/agent/prompts/phases.rs`；时间轴文档：`docs/agent-runtime-contract.md`】

## 13. 四类“正确”与授权的证据边界

| 层次 | 当前能硬验证/明确做什么 | 不能因此宣称什么 | 关键出处 |
|---|---|---|---|
| 结构合法 | schema字段/类型/枚举/数量、12slot唯一网格、17basis、引用存在且kind/scope符合Manifest、Review身份、deliberation守恒 | provider strict或JSON成功即业务成功 | `crates/akzio-research/src/agent/runtime_run.rs`；`crates/akzio-research/src/agent/validation.rs` |
| 事实/来源资格 | source/kind/producer/Run/Manifest受控；ground资产/域/citation；news source_verified；精确Claim+Critique price/macro资格 | 文本引用真的蕴含结论、未漏关键反证；模型authority标签即外部独立核验 | `crates/akzio-context/src/context_broker/policy.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-domain/src/decision.rs` |
| 数值正确/自洽 | PPM范围、weight+cash守恒、方向和非零long资格、deliberation算术由Rust；basis文本存在，Reviewer可查单位/数字/方法冲突 | Rust已从method重算出每个收益/概率/权重；Critic已审最终17项；Reviewer不会误收/误拒 | `crates/akzio-domain/src/decision.rs`；`crates/akzio-domain/src/research_review.rs`；`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md` |
| 预测准确/概率校准 | 当前只是明确有边界的原始估计、未来独立Outcome/校准对象 | 两个模型一致、confidence高、SUPPORTED或Review通过就预测准确/概率校准 | `crates/akzio-research/src/agent/prompts/roles/synthesizer.md`；`crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md` |
| 执行授权 | 本模块只提供研究Proposal与精确Review；后续Gate另管 | Review authorizes=Paper允许/订单已提交/已成交/校准已激活 | `crates/akzio-research/src/agent/prompts/shared.md`；`crates/akzio-domain/src/research_review.rs`；`AGENTS.md` |

## 14. 文档 / 注释 / 活动Prompt漂移清单

| 位置与原说法 | 当前代码对照与说明 |
|---|---|
| budget文档称当前67/37/candidate68 | 已是69/38/70；预算结构本身大体仍有效。`docs/agent-budget-configuration.md` 对照 `crates/akzio-research/src/agent/catalogue.rs` |
| retirement称研究65/36/candidate66，九节点fixture | 版本陈旧，固定研究图有supplement/refined/N+1 review pairs，默认PositionPlan21/Paper25。`docs/research-protocol-retirement.md` 对照 `crates/akzio-runtime/src/runtime/workflow.rs`、`docs/agent-runtime-contract.md` |
| runtime契约早段称仅新PositionPlan单Submit、其他purpose保留旧协议 | 后段126已修正为Paper/PositionPlan/Shadow全研究一致；实际运行按角色+version而非purpose选择。`docs/agent-runtime-contract.md` 对照 `crates/akzio-research/src/agent/runtime_run.rs` |
| role模型high/sol表 | 文档不是强制route，提交模板全局Luna low；有效冻结值需运行证据。`docs/agent-runtime-contract.md` 对照 `config/akzio.toml`、`crates/akzio-daemon/src/lib.rs` |
| 模板注释“research Attempt120s” | 当前resolved默认180s；不可用注释覆盖冻结代码。`config/akzio.toml` 对照 `crates/akzio-domain/src/budget.rs` |
| “不支持slot强制降级为neutral” | 若被理解为Rust自动改写会误导；当前validator拒绝非中性不合格值，模型再提交neutral。`docs/agent-runtime-contract.md` 对照 `crates/akzio-domain/src/decision.rs` |
| “阶段审计记录provider/parse/persist、修复hash”未标Debug条件 | PipelineLatency、SubmissionRevision汇总仅debug_session；SubmitRejected全Run。`docs/agent-runtime-contract.md` 对照 `crates/akzio-research/src/agent/runtime_run.rs`、`crates/akzio-research/src/agent/structured.rs` |
| role正文要求复制artifact_id及kind | 当前wire删kind，Rust事后回填；这不是只存在于历史文档的差异：role正文实际进入69请求。应读作保持精确身份，不能按字面输出kind。`crates/akzio-research/src/agent/prompts/roles/analyst.md`、`crates/akzio-research/src/agent/prompts/roles/synthesizer.md` 对照 `crates/akzio-research/src/agent/helpers.rs`。Outcome builder明确说wire无kind（`crates/akzio-research/src/agent/prompts/phases.rs`），当前structured builder只给ledger（同文件79-94），未同样消歧。 |
| Analyst正文还提supplemental needs.max_results、needs为空、news_web/fred/alpaca | 当前wire仅kind/assets/series/query，无max_results/source_family/window。原词汇可解释业务意图但不应作为实际输入字段。`crates/akzio-research/src/agent/prompts/roles/analyst.md` 对照 `crates/akzio-research/src/agent/proposal_review.rs` |
| Prompt预算措辞“Attempt累计” | 核心实际Task跨Retry/Recovery累计；避免以新Attempt重置input/output。`crates/akzio-research/src/agent/prompts/phases.rs` 对照 `crates/akzio-research/src/agent/recovery.rs`；权威预算文档 `docs/agent-budget-configuration.md` |
| 新版Claim/Review保持通过项“逐字” | 实现是语义scope比较，排除Rust timing，且只锁accepted forecasts；allocation/cash允许联动。`crates/akzio-research/src/agent/prompts/roles/synthesizer.md` 对照 `crates/akzio-domain/src/research_review.rs` |
| supplement文件导读称async join_all并发采集 | 当前外层for逐resource `.await acquire_supplemental_evidence`，不能凭导读断言8条并发；底层adapter是否批内并行应另核。`crates/akzio-daemon/src/application/research_supplement.rs` |
| Critic正文SUPPORTED或CONTRADICTED至少复制Claim ground | 当前精确overlap检查只针对SUPPORTED；CONTRADICTED要求非空反证且在自己grounds里、scope合法，不必同一个旧Claim evidence。不要把Prompt的较强要求写成已被硬校验。`crates/akzio-research/src/agent/prompts/roles/critic.md` 对照 `crates/akzio-research/src/agent/errors_catalogue.rs`、`crates/akzio-domain/src/research.rs` |

以上为只读差异记录，未修复任何文档/Prompt/代码；不据此改旧CAS、Contract、哈希或执行权限。

## 15. 已读测试证据（均未执行）与整合交接

- 当前structured prompt只出现一次、角色规则不串到别的角色：`crates/akzio-research/src/agent/prompts/phases.rs`。
- scope wire拒绝跨资产news/保持macro，Rust日历不允许模型覆盖，synthesis全provenance/零非零分支：`crates/akzio-research/src/agent/structured.rs`。
- delib修复重用精确result并拒模型replacement；差异路径含grounds/verdict/blocker/数值：`crates/akzio-research/src/agent/structured.rs`。
- usage未知阻断下一调用、部分usage已知保留且不能恢复未知output：`crates/akzio-research/src/agent/runtime_type.rs`。
- 恢复provider terminal必须闭合精确Attempt/start cursor、SubmitRejected反馈保留：`crates/akzio-research/src/agent/recovery.rs`。
- four-asset news展开冻结单资产resource、retrieval变化非新fact：`crates/akzio-daemon/src/application/research_supplement.rs`。

### 给主助手的整合要点

1. 调度图以`runtime/workflow.rs`为研究拓扑事实；实际并发/worker lease取主助手Runtime/Store证据，不从DAG推实际运行。
2. 正式研究入口按installed version>=67直接进入bounded_research（`crates/akzio-daemon/src/application/research_run.rs`），不要把下面旧通用候选/Analyst补采路径混进69主流程。
3. 最终模型产物是`TaskCompletion::Succeeded(vec![output])`，其正式attempt output发布要跟主助手Store事务证据接上；DeliberationNote/AgentTurn/SubmitRejected先保存不等于任务成功（`crates/akzio-daemon/src/application/research_loop.rs`；`crates/akzio-research/src/agent/runtime_run.rs`）。
4. 需要实际Run断言时补查精确Proposal/Review/hash/Manifest/Contract、所选revision、Started/terminal/usage、真实provider wire或telemetry；本文没有打开Store，不能证明哪些修订/补采/重试真的发生。
5. 执行层必须分别说明研究提案、正式Decision目标、ExecutionVerdict、订单提交、成交、Outcome；本报告没有验证任何交易或校准结果。

**交付范围完成：只读源码核查与指定报告。未执行：测试/构建/fixture、模型/API/Broker、Store读取或写入、配置修改、Git提交。**
