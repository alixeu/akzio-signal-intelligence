# Phase 2A — 真实 Agent 上下文、引用与累计预算

**指定的 PositionPlan 链路已完整运行到 Rust Decision。** 最终 Run `31ce9b678e7648ac` 的9个任务全部成功，三个期限的Analyst/Critic与Synthesizer均完成真实Draft/Submit，共14次Agent模型调用，Provider累计输入157,445、输出17,511。每个Agent Attempt各自遵守原48k输入限额；最终Run没有重试、修复调用或工具读取。不能把不同任务的总输入当成单Attempt预算。

这是缺证据条件下的受控研究与零目标计划验收。它不证明方向预测准确、不证明非零配置分支，也不包含Paper执行或跨交易日学习。T3 Critic另有一项描述性标签的提示词一致性瑕疵，详见限制。

## 1. 最终实验身份与可复核入口

- Run：`31ce9b678e7648ac`；DebugSession：`debug-31ce9b678e7648ac`。
- Store：`.akzio/agent-budget-v11-store`，identity `debug-store-f0a48d2c910c47b7`。
- Core：`http://127.0.0.1:17374`；manual / real / position_plan / broker forbidden / learning isolated；`auto_paper=false`，`outcome_processing=false`。
- Code revision：`5afdab2407ad996e8a4ecb0045db76dfacf1c253+c05644b4903b339f13f37cfab21732b920eb37d869297829a22346cf23b91fe7`。
- Contract40 / Prompt24 / freshness candidate41；Domain10、Store16及原业务预算不变。
- 真实Debug路由全部为 `openai_responses / gpt-5.6-luna / low`，actual_model与response_id逐次验证。此结果不是默认Sol/high Critic配置的验证。Provider request ID未返回，context window与价格费用未知。
- 签名App：`apps/dist/agent-budget-phase2a-v11/akzio.app`，包含Rust core；实际连接上述隔离Core与Store，并留在Decision Inspector。
- 证据目录：`.akzio/agent-budget-v11/`。`final-inspect.json`、`final-matrix.json`、`actual-calls.json`、`token-attribution.json`、`store-doctor.json`、`app-qa.md`保存导出与核对结果；正式验收通过Core API写入Store的RunScoped DebugRecord CAS，导出JSON不是第二个状态权威。

## 2. 逐阶段真实结果

| 阶段 | Task / Attempt | Provider calls | 累计 input / output | 实际结果 |
|---|---|---:|---:|---|
|Evidence |`22b2fac824f94db5` / `cb77c2b3c13544c4`|0|0 / 0|34 Need：16 available / 18 unavailable|
|Analyst t1|`7ec6da30bd324c9f` / `356a0bbe486a47e5`|2|19,591 / 2,807|QQQ价格子观察，保留方向阻断|
|Critic t1|`41d9c5cac4bb4884` / `1b87d62bf2214038`|2|22,581 / 2,044|NOT_ENOUGH_INFORMATION，blocker=true|
|Analyst t3|`54be394107f64d06` / `6dd0016c54d4482b`|2|19,345 / 2,501|QQQ中性价格子观察，保留方向阻断|
|Critic t3|`4bf6b6c15cf94978` / `76e5cfa776bb4f06`|2|22,738 / 2,270|NOT_ENOUGH_INFORMATION，blocker=true；描述性标签瑕疵见限制|
|Analyst t5|`1c3ef34180274a36` / `d9b65343f9dd4318`|2|19,323 / 2,501|QQQ谨慎偏多价格子观察，保留方向阻断|
|Critic t5|`6ff9366377ef4783` / `644e3e3a7d194b4d`|2|22,465 / 1,775|SUPPORTED仅限价格子观察，blocker=true|
|Synthesizer |`c123ac237ffd42b4` / `20198fc9c985451f`|2|31,402 / 3,613|12个中性Forecast，3Claim+3Critique完整血缘|
|Rust Decision |`e33bd9ad2762430e` / `8eb26b535cdd407e`|0|0 / 0|四ETF目标权重全部0，Run completed|

所有9个任务各1个Attempt。模型调用事件14个Started、14个Completed，无悬空调用；7份ContextManifest、7份DeliberationNote、3份Claim、3份Critique、1份DecisionProposal、1份DecisionContext、1份Decision。Evidence的18项不可用包含新闻、ETF成分与事件资料等，不能统称为18项新闻。

Synthesizer真实产物 `03cef732fb1f6766134de5c60ba3cfaba2359fd5e45a52aa286ec2a44a66cf57`：

- 精确4资产×3期限=12个唯一槽位；全部 `positive_return_probability_ppm=500000`、`expected_return_ppm=0`。
- 原三个Claim和三个Critique完整保留；所有引用来自同一不可变ReadGrant，evidence覆盖全部grounds闭包，未注入RawEvidence。
- holding period分别1/3/5；到期时间为带时区的RFC3339；所有uncertainty权重守恒。
- 缺新闻、缺完整三域支持或存在阻断Critique时保持中性。价格子结论SUPPORTED没有被提升成完整方向支持。
- applied/rejected learning refs均为空。

## 3. Rust Decision / Position Plan

Decision：`8e2e6c201066b0aac41e78792398594e5e7341194867356564c3f9e14fe027e4`。

DecisionContext：`f98359e59b5178fe9c78cd69b514d9b75f71c858fc506d4a3b12f141a853fda1`。

| 项目 | 实际值及含义 |
|---|---|
|目标权重|TQQQ=0、QQQ=0、SOXX=0、SOXL=0；权重和0|
|推导现金比例|1,000,000 ppm减目标权重和=1,000,000 ppm；这是计划残余比例，不是实际Broker余额|
|金额 / 数量|N/A；PositionPlan没有账户、最新报价和下单分配阶段|
|12个期限分项|全部neutral，included_in_target=false；每个资产的期限policy weights为333333+333333+333334=1000000|
|校准与风险|calibrated_assets=0、covariance_sample_count=0；beta/volatility/shortfall/gap_loss保持null，未把缺失风险测量填为0或满分|
|阻断|missing_evidence、unverified_claim；soft warnings含incomplete_evidence、low_confidence、correlated_consensus|
|置信度|Proposal300000→Rust250000；3名Analyst来自同一模型能力快照与高度重合价格来源，independent=false|
|终态|workflow completed；DebugSession保持manual paused，不再有运行任务|
|执行/学习|execution_evidence=not_applicable；拓扑无ExecutionGate、Commit、Reconcile、Evaluate；未生成Execution/Outcome/Lesson产物|

原DecisionGate、风控、校准缺省行为与Commitment边界未改。实际零目标由Rust产生，未人工改写模型提案或Decision。

## 4. 原始问题与真实 Token Attribution

原始 Run `3ebea13d68344156`，Attempt `925f4ab935bf412a`。三次 Provider 实报输入 10,662 / 14,112 / 16,093，累计 **40,867**。下一次 Submit repair 估算 15,987，预检 **40,867 + 15,987 = 56,854 > 48,000**，因此没有发送该请求。这个拒绝正确。

下面是只读 Store 示例 `crates/akzio-store/examples/agent_token_attribution.rs` 对完整持久化 AgentModelRequest 的测量。普通 inspect 会脱敏 opaque continuation，不能用脱敏 JSON 字节数冒充完整请求大小。示例只输出字节计数与公开 telemetry，不输出或解码 opaque 内容。

| Call | 完整 request JSON bytes | Runtime bytes/4 estimate | Provider input | Context bytes | Prompt JSON bytes | opaque history bytes |
|---|---:|---:|---:|---:|---:|---:|
|1|32,449|8,113|10,662|24,874|5,271|0|
|2|46,443|11,611|14,112|24,874|5,271|1,676|
|3|58,628|14,657|16,093|24,874|5,271|3,696|

Context 中 facts 18,009 bytes、metadata 5,536 bytes、task contract 822 bytes；governance 源文本 857 bytes、role 源文本 3,023 bytes。第二次新增 ToolResult 9,709 JSON bytes，第三次继续回放早期工具与模型输出。组件存在封装和转义，不能直接相加声称 wire 精确分区。Provider 只报告总 tokens，不报告组件 tokens；完整逐项测量见 `.akzio/agent-budget/baseline-authority-attribution.json`。早期基于脱敏 inspect 的 `baseline-attribution.json` 保留为诊断历史，不作为完整请求估算。

48k 是整个 Attempt 的累计输入治理额度，不是单请求 context window。Draft 的 ×2 是发送前为当前 Draft 与未来 Submit 留空间的检查，不是两次记账；实际 usage 才累计。上述 56,854 是 Submit repair 路径，没有 ×2。没有降低估算或删除 guard。Provider context limit 仍为 **unknown**。


最终Run的输入测量：三个Analyst初始Context都是18,610 bytes；Critic为23,035～23,144 bytes；Synth为35,599 bytes。Synth两次完整AgentModelRequest JSON为45,926 / 60,678 bytes，runtime估算11,482 / 15,170 tokens，Provider实报14,333 / 17,069，合计31,402。原始基线与最终Run的材料并非完全相同，不能把差额宣称为严格控制的A/B性能收益。

**估算限制保留：bytes/4不是精确tokenizer，实测低于Provider实际输入。** 本轮没有降低估算、取消累计guard、增加48k预算或声称使用1M context；发送前估算与收到后的实际usage治理各自保留。不能声称预测每次Provider计费tokens绝不超限。

## 5. 实现变化与边界

| 位置 | 实际变化 |
|---|---|
|`akzio-context/.../materialization.rs`|必需文档metadata去重；collection status明确采集状态不等于选中授权；Synth覆盖表按期限保存共同血缘，仍保留全部12槽位与资产方向域|
|同上 Claim producer scope|从Claim精确source_refs中的唯一Manifest校验同Run/task/attempt/contract及input_hash，仅向Critic标明当前已授权证据是否在Claim生产时选中；不披露生产者独有的未授权ID/正文，不扩大ReadGrant；缺唯一Manifest时unknown|
|`akzio-context/.../reads.rs` / research `tools.rs`|超过32KiB的全文读取仍拒绝；授权错误附正文长度、32KiB限制和完整顶层JSON值的精确byte ranges；没有随机猜切片或静默截断|
|research `helpers.rs`|Submit wire ArtifactRef使用完整artifact_id，按字段原kind与当前Manifest交集生成enum；Rust按同一Manifest补回kind并走原校验；截短/未知ID拒绝，显式错kind不静默纠正|
|research `runtime_run.rs` / `runtime_type.rs`|发送前先做输入预算授权，再写AgentTurnStarted；本地拒绝不产生悬空Started；SubmitRejected在修复前写正式验收，保留原错误与AgentTurn引用|
|Domain `research.rs`|明确news需求不得伪装Alpaca bars；Critic保留blocks_directional_forecast gap时必须blocker=true，价格子结论SUPPORTED也不能例外|
|research Prompt / schemas|要求准确ppm、证据范围、方向三域与阻断规则；thesis_valid_until明确RFC3339 timestamp pattern，和原Rust时间类型一致|
|`agent_token_attribution.rs`|只读Store中完整AgentModelRequest，输出组件bytes、估算与公开telemetry；仅计数opaque continuation，不输出或解码其内容|
|App DebugWorkflowPanel|预算标题明确为Attempt累计额度、不是Provider context window；Inspect使用唯一Task accessibility ID|

保留 `store=false` 和完整stateless transcript。受控初始投影本身已可读，工具只用于具体未解细节；没有通用history/ToolResult摘要器、没有隐式丢弃早期事实、没有previous_response_id/stateful Gateway依赖、没有alias或模糊引用修复。Context授权、原CAS、预算、原始模型输出与交易风险边界保留。

## 6. 失败历史与保留证据

每次语义代码/Prompt版本变化都使用新隔离Store与Run；没有复活终态任务、拼接旧输出或伪造跨Store CAS lineage。所有失败Store/Attempt/验收仍保留。

| 实验 | 真实问题与处理 |
|---|---|
|v1 启动|Contract21与旧freshness candidate身份冲突，启动拒绝；未覆盖旧身份|
|v2 `bee06ab5917f4384`|Critic supporting refs不在grounds，修复超4k输出；保留失败，完善闭包说明|
|v3 `f77a0b73047f468d`|把未选中TQQQ bars误报为全局不可用；语义FAIL，明确采集与选择区别|
|v4 `ee636dfa645f4e07`|Critic把Claim ID标成semantic_detail；保留拒绝，精确wire kind绑定|
|v5 `dfab691b84e548e7`|2428 ppm被写成2.428%；单位FAIL，保留整数原单位|
|v6 `8883d6ab387b4bc2`|Critic混淆生产者和自身选择；语义FAIL|
|v7 `c8de471cecfe461c`|Synth首Attempt第三次修复超时，第三次usage未知；第二Attempt已用30825，下一估算17236→48061预检拒绝。旧实现先写Started导致Doctor发现悬空调用；该历史Store仍保留原问题，未改事件修绿|
|v8 `7ba70dde05ef448f`|Critic保留方向阻断gap却blocker=false；语义FAIL，补Rust既定不变量校验|
|v9 `9b61dcb74f244a41`|CriticT3在35806input/3906output内修复Schema，但生产者选择推断仍错；语义FAIL，新增精确producer scope投影|
|v10 `3740bdd975124fb2`|T5权重求和不守恒，第一次31898/4471失败，合法新Attempt19293/2604成功。Synth日期缺时区，已用33921，下一估算18648→52569预检拒绝并fail_run；Decision取消，retry-node HTTP409拒绝。Store doctor实际ok，证明本地预算拒绝不再造成旧Started缺陷|
|v11 `31ce9b678e7648ac`|本报告最终Run；9任务成功，14模型调用，无修复/重试，真实Decision已落库|

v10 T5两个失败提交的uncertainty sum为1000000/900000，confidence300000要求700000；日志通用措辞“must be positive”不准确描述求和问题。v7/v10中的48061、52569是预检总量，不能说成已发生的Provider费用。合法新Attempt与单Attempt内预算重置是不同概念；本轮没有在同一Attempt内清空预算。

## 7. 验证命令与能力矩阵

最终源码已执行 `cargo fmt --all`、`cargo check --workspace`、`cargo clippy --workspace --all-targets`、`cargo test --workspace`；**47 passed / 22 suites**。Clippy 0 errors，保留既有 `akzio-learning/src/evaluation/outcomes.rs:97` 的too_many_arguments warning。`git diff --check`无输出。

| 测试 | 实际覆盖 | 结果 |
|---|---|---|
|stateless_continuation_retains_initial_context_and_prior_tool_results|初始 Context、早期 ToolResult、后续输出保留|PASS|
|required_document_metadata_and_facts_are_not_duplicated|必需静态材料不重复注入|PASS|
|collected_availability_is_not_context_selection|采集状态和任务选择区别|PASS|
|producer_scope_distinguishes_additional_coverage_without_disclosing_ungranted_ids|当前角色新增证据与生产者选择区别；不泄露未授权ID|PASS|
|over_budget_request_never_authorizes_a_provider_turn|本地预算拒绝不写悬空AgentTurnStarted|PASS|
|supported_price_does_not_clear_direction_blocking_gap|价格子结论SUPPORTED仍须保留方向阻断|PASS|
|oversized_full_document_is_rejected_not_silently_truncated|大文档必须 range，不静默截断|PASS|
|recommended_ranges_are_exact_json_values|UTF-8 JSON 推荐范围精确|PASS|
|comparison_uses_governed_projection_without_replaying_252_bars|工具比较不重复252条日线全文|PASS|
|exact_reference_schema_is_manifest_bound_and_rejects_truncation|精确ID绑定、截短/跨集合拒绝|PASS|
|wire_ids_are_kind_filtered_and_resolve_without_widening_authority|字段kind过滤、确定解析、不纠正显式错kind|PASS|
|news_cannot_be_acquired_as_price_bars_but_gap_may_remain_unfilled|新闻不能伪装bars；空补采合法|PASS|
|reservation_is_not_a_charge_and_repair_overflow_is_rejected|×2不扣费，40867+15987被拒绝|PASS|
|failed_provider_turn_remains_charged_before_repair|失败请求仍计入累计输入|PASS|
|t08_retry_preserves_attempt_history_and_limits|新Attempt保留历史与重试限制|PASS|
|research_deferral_does_not_admit_future_data|原未来数据保护回归|PASS|
|position_plan_commit_and_dispatch_reject_and_paper_debug_forbids_broker|目的与Broker多层拒绝回归|PASS|

|compact_coverage_preserves_all_asset_horizons_and_exact_claim_verification|压缩后12槽位、精确Claim/验证和逐资产方向域保留|PASS|
|thesis_expiry_requires_timezone_timestamp_not_calendar_date|拒绝日期，接受带Z或时区偏移的RFC3339|PASS|

`swift test --filter DebugContractChecks`曾返回no tests found，因为它是executableTarget；随后正确入口 `cd apps && swift run DebugContractChecks` 的3项检查通过。正式脚本 `scripts/update_app_and_submit_debug.sh` 配合新的 `AKZIO_APP_BUNDLE`、`AKZIO_PRESERVE_BUILD_PRODUCTS=1` 打包、嵌入Core并签名验证成功。

最终隔离Store doctor实际返回 `{"ok":true}`。没有运行会创建PaperDryRun整条执行拓扑的 `run fixture-debug`，本次真实流程严格停在PositionPlan；不把单元测试或旧Fixture声明为真实业务验收。

| 能力/分支 | 已有证据 | 限制 |
|---|---|---|
|真实三组Analyst/Critic与Synth→Decision|最终Run全部实际两阶段成功|只验证当前Luna/low Debug配置、当前证据集|
|累计48k与失败保留|单元测试；历史真实预检拒绝；v10拒绝后Doctor正常|bytes/4不精确；未知Provider费用仍unknown|
|Stateless continuation与早期证据|专用回归测试；原基线真实多轮工具链|未验证Gateway有状态恢复|
|大文档range/compare投影|针对性单元测试通过|最终Run所有read tool calls=0，没有真实触发修复后的大文档工具路径|
|工具耗尽后的阶段切换|原上限和撤工具路径保留|本轮未新增独立耗尽集成测试，最终Run未覆盖该路径|
|完整ArtifactRef/授权|精确ID、kind过滤测试；真实输出通过原Schema与授权闭包|无alias；未穷举所有跨Run/future负向组合的新测试|
|数字与原单位|实际grounds逐项ppm核对、不确定性守恒、12中性槽位、零权重和期限权重求和|非零组合分配、实际现金/数量/成交不在本轮|
|Native App一致性|实际读取身份、预算、Draft/Submit/ReadGrant/response IDs、最终Decision权重与阻断|没有把签名成功本身当UI验证|

## 8. 交付状态与下一步边界

- `implemented`：本轮代码、Prompt、Schema及审计修复已实现。
- `offline-verified`：上述47项测试、编译、Clippy、App检查与打包通过。
- 额外实际证据：`real-LLM PositionPlan verified`，指定链路真实完成到Decision。
- `real-Paper-verified`：**未验证**，没有执行、Broker写入或订单。
- `outcome/learning-verified`：**未验证**，没有OutcomeSchedule、跨交易日复盘或学习转移。

已知提示词一致性限制：T3 Critic的部分descriptive grounds带资产/域标签，未完全遵循提示词要求的空标签；原Rust允许这些描述性标签，并不会把它们计为directional支持。该输出未改写，验收记录和最终报告均明确列出。不能据此称所有提示词细节完美通过。

可以把本轮作为后续**独立、broker forbidden的执行链Debug**的研究链证据；不能从这里开启Paper写入或声称非零资金配置/交易安全正向分支已验收。Outcome/Learning仍需独立Paper与实际交易Session证据。当前PositionPlan没有执行授权或Outcome，不复用它伪造后续闭环。
