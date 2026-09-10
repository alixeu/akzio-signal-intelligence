# CLI 真实 Debug Phase 2B

本报告以本轮 CLI 从 Store 导出的 Artifact、Acceptance 和当前源码为依据。生产 Store 未打开；真实研究沿用 gpt-5.6-luna / low。基线及前半轮使用原 48k 累计输入预算；用户修复预算并要求继续后，新 Paper 在新隔离 Store 冻结新预算，详见末节。所有 Debug 都是 manual、Broker forbidden、learning isolated。

## 成功基线与零目标根因

基线 `31ce9b678e7648ac` 保持 completed，9 个 Task succeeded，14 次模型调用；四资产零目标是合法业务结果。权威产物：Decision `8e2e6c201066b0aac41e78792398594e5e7341194867356564c3f9e14fe027e4`、DecisionContext `f98359e59b5178fe9c78cd69b514d9b75f71c858fc506d4a3b12f141a853fda1`、Synth `03cef732fb1f6766134de5c60ba3cfaba2359fd5e45a52aa286ec2a44a66cf57`。

必须区分三层：16 项采集成功不代表全部进入每位 Agent 的 Manifest；选入不代表成为 directional ground；单域 ground 被 SUPPORTED 不代表完整资产/期限预测获支持。

三个 Analyst Manifest 相同：130,571 / 131,072 bytes，选入 collection status、QQQ/SOXX/SOXL 三份日线，以及 TQQQ/SOXX/QQQ 公司行动。TQQQ 日线已真实采集成功，却因原文累计容量未入选；三个 FRED series 也未入选。没有增加 128 KiB 沙箱、没有把 48k 改为模型 context window。

三个 Critic 授权了 VIXCLS；Synth 授权 VIXCLS、DFF。它们仍没有补出完整四资产/三期限的宏观 directional grounds。T3 Critic 的“缺宏观交叉验证”是研究未建立方向支持，不是 FRED adapter 失败；其描述性 ground 明确承认 VIXCLS 可用。

Claim T1 `967b7ffd36656eb646fb51ab271a2d1a619bcfe20220059eb19624f5a1001d37`：QQQ 略偏多价格子观察。
Claim T3 `bf4e39fdaeb6d595d69fb1ad86bf6c1953662dfc942396779567fafce840e084`：QQQ 中性价格子观察。
Claim T5 `a06d9dfb6ae71efb65ca91fe438d55240e00a8b34b161d98751bcdbb218a137e`：QQQ 谨慎偏多价格子观察。

| 资产/期限 | Evidence 与 Claim | Critic | Synth |
|---|---|---|---|
| TQQQ T1 | 已采集日线未入 Analyst Manifest；无 TQQQ directional ground；新闻缺失，宏观未建立支持 | T1 NOT_ENOUGH_INFORMATION，blocker=true，保留 TQQQ 价格缺口和四资产新闻缺口 | 中性 |
| TQQQ T3 | 同上；Claim T3 仅 QQQ 价格观察 | T3 NOT_ENOUGH_INFORMATION，blocker=true | 中性 |
| TQQQ T5 | 同上；Claim T5 仅 QQQ 价格观察 | T5 对 QQQ 价格 SUPPORTED，仍 blocker=true，不能外推 TQQQ | 中性 |
| QQQ T1 | 价格已选且有 ground；缺 macro/news_event 方向依据；Claim T1 未获完整验证 | NOT_ENOUGH_INFORMATION，blocker=true | 中性 |
| QQQ T3 | 价格已选且有 ground；短期反弹与 20 日负收益并存；缺跨域依据 | NOT_ENOUGH_INFORMATION，blocker=true | 中性 |
| QQQ T5 | 价格 ground 获支持；缺 macro/news_event 完整验证 | SUPPORTED 仅限价格，blocker=true | 中性 |
| SOXX T1 | 日线已选但 Claim 无 SOXX directional ground；新闻缺失，宏观支持未建立 | 没有对应资产完整 SUPPORTED 路径 | 中性 |
| SOXX T3 | 同上，不能把 QQQ Claim 外推 | 同上 | 中性 |
| SOXX T5 | 同上，QQQ T5 价格 SUPPORTED 不覆盖 SOXX | 同上 | 中性 |
| SOXL T1 | 日线已选但 Claim 无 SOXL directional ground；新闻缺失，宏观支持未建立 | 没有对应资产完整 SUPPORTED 路径 | 中性 |
| SOXL T3 | 同上，不能把 QQQ Claim 外推 | 同上 | 中性 |
| SOXL T5 | 同上，QQQ T5 价格 SUPPORTED 不覆盖 SOXL | 同上 | 中性 |

全部 12 Forecast 的 probability=500000、expected_return=0。三域和非阻断 Critique 不完整，受影响槽位必须保持中性；不是“没有新闻”单一原因。

## Calibration、Risk 与真正归零分支

配置没有 decision_policy_path；`cli/identity.rs::decision_policy_from_config` 加载 `DecisionPolicy::default()`。默认 asset_calibrations 为空、active_forecast_calibration=None、forecast_calibrations 为空、covariance sample_count=0；未做离线校准拟合，252 日行情本身不会自动变成校准政策。

`decision_gate/decide.rs` 调用 `target_with_risk`。真实置信度由 correlated consensus 从 300000 限至 250000，恰好等于默认 min_confidence_ppm=250000，因此本例没有命中“低于门槛”的提前归零分支。随后 `decision_gate.rs` 对每个资产查找 asset_calibrations，均无记录而 continue；eligible 为空，返回 `TargetPortfolio::zeroed()` 与 `PortfolioRiskAssessment::default()`。这才是本例直接将四资产权重归零的 Rust 分支。

12 个 horizon trace 均 neutral / included_in_target=false。风险 beta、ex_ante_volatility、expected_shortfall、gap_loss、risk_model_hash 均 None，calibrated_assets=0、covariance_sample_count=0。该 default 表示未知，不能称风险为零或安全。missing_evidence / unverified_claim 同时保留在 DecisionContext，阻止后续执行资格。

## 18 项 unavailable

全部是 NewsWeb source family；基线 collection status 均为 adapter_unavailable。

| # | 资源 | 基线原因 | 本轮结果 |
|---|---|---|---|
| 1 | `research:etf_holdings:QQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 2 | `news:SOXX:2026-08-26:2026-09-09:market` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 3 | `research:earnings_event_calendar:SOXX:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 4 | `research:index_metadata:QQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 5 | `news:SOXL:2026-08-26:2026-09-09:market` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 6 | `research:leveraged_etf_terms:TQQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 7 | `research:index_metadata:SOXX:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 8 | `research:etf_holdings:TQQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 9 | `news:QQQ:2026-08-26:2026-09-09:market` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 10 | `research:index_metadata:TQQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 11 | `news:TQQQ:2026-08-26:2026-09-09:market` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 12 | `research:earnings_event_calendar:QQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 13 | `research:earnings_event_calendar:TQQQ:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 14 | `research:etf_holdings:SOXX:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 15 | `research:index_metadata:SOXL:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 16 | `research:earnings_event_calendar:SOXL:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 17 | `research:leveraged_etf_terms:SOXL:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |
| 18 | `research:etf_holdings:SOXL:2026-09-09` | 未注册 NewsWeb adapter | BLOCKED：原生搜索缺 action.sources 审计字段 |

注册条件位于 `orchestration/bootstrap.rs`：native_web_tool 与 native_web_tool_verified 必须同时为真。当前 function/stateless probe 将两者固定为 false，未真正探测 native web。

本轮用现有 ModelClient / Responses transport 进行了独立真实 probe，返回 `resp_03be6a3c164669a8016aa1414657cc87d0a7852fc28d0d1ee2`、actual_model=gpt-5.6-luna、completed web_search_call 和 FRED 引用，但 action.sources 缺失。`NativeWebPolicy::validate_provider_response` 返回 NativeWebArgumentsInvalid；请求已经要求 include web_search_call.action.sources。因此目前不能合法注册 adapter，也不能根据普通 citation 推测完整来源审计通过。

已经证实的是“应用未探测/未注册 + 当前返回结构不满足受控 transport”。没有证据把这 18 项分别判为 credential 错误、HTTP transport 失败、真实无数据或时点不合法；这些下游情况尚未进入，不能猜。未改 required/optional、未伪造资料或把 unavailable 改 available。独立 native-web probe 不作为 Agent read_range 验收。

## 本轮针对性修改

- `debug.rs` / CLI：experiment 保留非 canonical 父 Purpose；独立 PositionPlan experiment 可显式请求 read_range 探测，只改变新图的 Analyst objective。普通 prepare 和预算不变。
- 旧 experiment 把 PositionPlan 降为 Debug，使 Evidence 走开始时刻截止的旧采集路径。两次真实失败 `e05e1690fa204409`、`973ed3daab0e4e51` 保留。修复后的新 experiment `fb3992c7c65f4cb2` 保持 PositionPlan 并真实完成 Evidence。
- Paper 不能绕过原 Store noncanonical experiment 规则和 Session reservation。Paper experiment 明确要求 fresh prepare，禁止偷偷改成 Debug；本轮 Paper 本就使用 prepare。
- 新增独立 CLI native-web probe 示例，仅读取现有 endpoint，不打开 Store。输出公开 transport 元数据，不输出凭据或 opaque continuation。

## 零目标 execution intent

`allocation.rs` 先验证 DecisionContext accepted、同一 broker session、market open、账户有效，再逐资产算 target_value=equity×target_weight，delta=target_value-current_position.market_value。

- 零目标且无持仓：delta=0，全部跳过，orders 为空→NoExecutableOrder。
- 零目标且有正持仓市值：delta<0，检查合法且新鲜的双边 quote 后构造 Sell intent；notional 为当前持仓市值，limit 使用原保护规则。此处是按金额 intent，不代表保证全部股数已清仓或已成交。
- 有负持仓市值则 ShortPosition 拒绝；无效报价、闭市或其他 Gate 失败仍会阻断。
- `ExecutionRuntime::evaluate` 仅在 blockers 为空时进入 allocator；本基线 missing_evidence / unverified_claim 存在，不能根据目标零声称产生了实际减仓单。

## 独立真实 ToolCall 验收

**VERIFIED / PASS**。Run `fb3992c7c65f4cb2`，Task `7c88fc2b449d4979`，Attempt `89a5cc667a444589`。该实验仅执行 Evidence 与 T1 Analyst，后续节点停在 manual paused，不影响正常 PositionPlan。

真实 QQQ 日线 Artifact `912e80e8e208d41a38ad9902116895dafc7a99dcd64899c5af1ac257f514cf28` 在 Manifest `cd9ade165c4be4c76b68af7e921283726c82b9227ac33c4a786e7ffb0b48f6c8` 与该 Attempt 的 ReadGrant 可读集合中。ReadGrant snapshot 是审计观察，真正授权由 Rust 根据持久化 Manifest 派生；没有拿 snapshot 代替授权。

- ToolCall `428a201d50bb792d2fccfd0bf1188631cc57ca7afaa0cf5f8f2e097bb01b4222`，`call_CiZnubgT8m2SFJGLq3yZ8EFa`，read_range(0,512)。
- ToolResult `1fa979595efb651cd0f3a1d3efde1b6f7b6e4789b32022ee94edc0aa240c815b`，ok=true，实际 UTF-8 长度 512 bytes，文档全长 38,080 bytes。它只是原文前缀，没有当作完整 JSON 或完整市场证据。
- 事件 cursor123 tool.called → 124 tool.completed → 125 后续 agent.turn_started → 127 turn_completed → 129 Submit turn_started → 131 turn_completed → 134 Claim committed → 135 task.succeeded。
- 后续真实请求的 tool_outputs 包含同一 call_id 和结果；原 Context 保留。最终 Claim `5198c5816769f7d842fe7b07d04230e92b87eff0835a32209d02df42c8dccbae` 成功持久化。

| 调用 | 真实 response_id | input / output |
|---|---|---|
| 发起 read_range | resp_09d9ca3c9b33aba1016aa1431c80ec87d0a16ffe51fdc9ed72 | 7,783 / 90 |
| 读取结果后 Draft memo | resp_09d9ca3c9b33aba1016aa14320d10487d09e684a75824d117e | 8,199 / 1,619 |
| Submit | resp_09d9ca3c9b33aba1016aa1433fd9d887d08eb940bd6aed9354 | 10,775 / 1,047 |

合计 26,757 input / 2,756 output，1 次只读工具，耗时 60,802 ms；原 48k/6k/120s/4 tools 未改变。所有 actual_model 都是 gpt-5.6-luna。证据导出在 `.akzio/real-debug-phase2b/real-toolcall-proof.json`，正式 Acceptance 已通过 CLI 写入 Store。

## CLI 复核入口

```bash
target/debug/akzio --help
target/debug/akzio debug --help
target/debug/akzio --config .akzio/agent-budget-v11/config.toml debug inspect 31ce9b678e7648ac
target/debug/akzio --config .akzio/agent-budget-v11/config.toml debug inspect c591319fa0ab4b44
target/debug/akzio --config .akzio/agent-budget-v11/config.toml debug inspect 55cb493ab6ea4cff
target/debug/akzio --config .akzio/real-debug-phase2b/probe-config.toml debug inspect fb3992c7c65f4cb2
```

本轮每个实际节点均保留 before / step / after / acceptance JSON；CLI step 指定 exact TaskId，没有 resume 连跑。新工具场景由 `debug experiment <parent> --reason ... --read-range-probe` 编译新图，未拼接历史输出。

## 用户预算修复后的续跑

用户随后明确表示 48k 限制已修复并要求继续。当前 `docs/agent-budget-configuration.md` / `akzio-domain::budget` 将新 Run 默认累计输入设为 1,000,000、只读工具次数 unlimited；旧 Contract/旧 Run 仍使用冻结预算，ContextManifest 容量不变。本轮不把此新预算解释成 Provider context window，也不回写历史 Attempt。

旧预算下的新 PositionPlan `c591319fa0ab4b44` 已合法完成：三份真实 Claim、T1/T5 两份真实 Critique、T3 Critic NoOutput、真实 Synth、Rust Decision。Decision `2df30e24a3d1b59ed220043f83c5a8fede9ba7ecb2017f940f251c558343a407` 四资产权重全零；12 个中性 Forecast，calibrated_assets=0，risk truth 全部保持未知，missing_evidence/unverified_claim 保留。T3 Claim 为 neutral，materiality=300000，原 `should_run_structured_critique` 没有触发模型审查；该节点 succeeded 不等于真实 Critic 调用。早期 Acceptance 的“Critique persisted”描述已通过追加更正记录明确撤销，旧 CAS 保留。

旧预算 Paper `55cb493ab6ea4cff` 的三组 Analyst/Critic 都完成真实模型调用，但 Synth 提交漏掉引用闭包，被 Rust 拒绝。其真实累计 input=36912，后续修复请求预检=57065，未发送该修复请求。保留失败，不能标 Research completed 或 ExecutionGate reached。

另一个旧预算 Paper `51efc47e82424dc1` 已完成 Evidence、尚未开始 Agent；用户预算更新后保持 paused，不在同一 Run 上换预算。新代码使用全新 `.akzio/real-debug-phase2b-budget-store` / 17376，显式冻结新配置进行新的 Paper 验证。模型仍为同一 gpt-5.6-luna / low，输出和时限沿用各角色默认值，manual / forbidden / isolated 不变。

新预算 Paper `b375e7fad1d04f97` 的三组 Analyst/Critic 与 Synth 均完成，14 次真实模型调用，Synth 两次调用 input=32744/output=3752，没有触发修复，不能将本例当作累计超过 48k 的实证。DecisionContext `47312c0295bb48c64cd3c0cc371c68b8e012ab35c26f4eab1477a8a246bc3301` 的 hard_blockers 仅有 unverified_claim，soft_warnings 为 incomplete_evidence/correlated_consensus；本轮 effective confidence=200000，低于 250000，直接命中低置信度提前归零，区别于原基线的 eligible.empty 分支。校准仍为零、风险仍未知。

Execution Task `1add206c0f424860` / Attempt `e77c00e4744d4fdf` 在 2026-09-09 12:27:27–12:27:30 UTC 实际进入 refresh，但报 `evidence crosses the decision cutoff`，没有封存 execution.snapshot 或业务 Verdict。它是刷新失败，不是 NoOrder 或 Accepted；旧失败和 BLOCKED Acceptance 保留。

根因来自即时刷新时序：`acquire_paper_need` 用 HTTP 开始时间贯穿 acquire_and_normalize_async；Alpaca adapter 则在收到 HTTP 响应后记录 observed_at。没有独立事件时间的资料，以 observed_at 作为 available_at，必然晚于开始 cutoff。局部修复将执行资料先经 authorize/acquire_validated，再以响应完成后的真实 Utc::now() materialize_validated，重新执行原时点/新鲜度验证。没有取 provider timestamp 作为截止时钟，没有修改研究历史 cutoff 或 quote 规则。回归用例模拟真实响应延迟：修复前只能触发四项账户请求，修复后六项完成；仍拒绝 ask=0 和未来时间。后续真实验证使用新隔离 Store 和新 Run，未回写历史。


## 修复后离线验证

`cargo fmt --all`、`cargo check --workspace`、`cargo clippy --workspace --all-targets`、`cargo test --workspace`、`git diff --check` 均通过；57 tests / 22 suites。Clippy 保留原 `akzio-learning/src/evaluation/outcomes.rs:97` 的 too_many_arguments 警告，0 errors。Swift `swift build` 已通过；后续局部修改仅涉及 Rust Evidence 刷新及其回归用例。

独立 `.akzio/real-debug-phase2b-fixture-store` 的 `run fixture-debug` 完成，purpose=paper_dry_run、fixture=true，只是离线证明。该一次性 Fixture 退出后没有常驻 Core/token，因此针对它的 CLI doctor 无法连接；真实 Debug Store 的 doctor 结果另行记录，不以 Fixture 代替真实验收。

## 最终真实执行结果

修复后新 Paper Run `bcf2906ade9c4f14`，Store `.akzio/real-debug-phase2b-refresh-store`，Core `127.0.0.1:17377`。三份 Analyst Claim、T3/T5 两份实际 Critique、T1 Critic 合法 NoOutput、Synth 和 Rust Decision 均成功。12 次真实模型调用，input=124632 / output=15504。Synth 两次调用 input=29307 / output=3378，全部 12 个预测中性。DecisionContext `c1629106a3f5f4d8b2e7a5e9a821b5de7674d326c961c0c0c86553fd11a68eb0` 四权重为 0，effective confidence=250000，calibrated_assets=0、risk None、missing_evidence/unverified_claim 保留，与原基线同样命中无校准 eligible 集合的归零分支。

Execution Task `6bf4533a7f1a4074` / Attempt `e51bc98f3a7e436e` 在 2026-09-09 12:41:19 UTC 失败，正式 Acceptance 的原始错误为 `invalid daemon input: budget quote.price must be positive`。`materialize_paper_acquisitions` 位于四项账户请求和 quotes/clock 两项请求全部 await 成功之后；因此这次真实运行证明六项读取路径均完成，并到达快照构造时的价格检查。它没有再次遇到请求开始/响应接收时间误判。

但是，没有完整封存 execution.snapshot.account/quotes/clock，没有 ExecutionVerdict、NoOrder 或 Accepted。此路径在任一 materialization 错误时尚未执行 write_task_artifact，因此不能从已落库产物确定具体是哪只资产、bid 还是 ask 非正；不把这个错误猜成已确认的某个 ask=0。完整快照刷新验收记 FAIL，ExecutionGate 业务推进记 BLOCKED。六项真实读取完成不等于有效快照封存完成。

最终 workflow=failed、Debug session=paused；gate.paper/gate.reconcile/gate.evaluate 均 cancelled、无 Attempt。没有 ExecutionCommitment、ExecutionPlan、OrderReceipt 或 OutcomeSchedule 产物，本轮未调度任何 Broker write，未运行 canonical Learning。Accepted 后 forbidden 实际拦截为 NOT_COVERED。当前不具备本 Run 的 Outcome/Learning Debug 起点。

最终真实 Store doctor 返回 `{ "ok": true }`。原基线最终 inspect 仍是 9 个 succeeded，历史 failed/paused Run 全部保留。新预算首个 Paper `b375e7fad1d04f97` 合计 14 calls / 147642 input / 16625 output；旧预算新 PositionPlan `c591319fa0ab4b44` 为 12 calls / 124627 input / 15053 output。各 Run 用量分别统计，不合并冒充单 Attempt 预算。

复核命令：

```bash
target/debug/akzio --config .akzio/real-debug-phase2b/refresh-config.toml debug inspect bcf2906ade9c4f14
target/debug/akzio --config .akzio/real-debug-phase2b/refresh-config.toml store doctor
```

| 能力 | 状态 | 依据 |
|---|---|---|
| 成功 PositionPlan 基线保持 | PASS | 31ce9b678e7648ac，9 succeeded |
| 零权重根因已解释 | PASS | 12 槽位表、原基线真实校准 eligible.empty 分支 |
| Research Evidence 改善 | BLOCKED | 18 项仍 unavailable；原生搜索受控 response 结构不满足要求 |
| 新真实 PositionPlan | PASS | c591319fa0ab4b44 completed；T3 Critic NoOutput 明确标注 |
| Real ToolCall path | PASS / VERIFIED | 独立 fb3992c7c65f4cb2，read_range 至 Submit 全链 |
| Calibration 状态已确认 | PASS | 无已配置校准政策，calibrated_assets=0 |
| Risk truth 状态已确认 | PASS | None 为未知，未填安全数值 |
| Execution snapshots real refresh | FAIL | 六项真实读取已完成，非正报价阻止快照封存 |
| ExecutionGate | BLOCKED | 到达刷新与 materialization；尚无业务 Verdict |
| Zero-target execution semantics | PASS | 源码确认空仓无 intent、已有正持仓可生成 Sell，仍受所有 Gate 约束 |
| Accepted 后 Broker forbidden | NOT_COVERED | 没有业务 Accepted |
| Broker writes | 0 | 未运行 PaperCommit/Reconcile，无执行产物 |
| 可以进入 Outcome/Learning Debug | NO | 未完成 Evaluate，无 OutcomeSchedule |

交付等级：局部修复 implemented / offline-verified；真实 LLM 和受控 ToolCall 已验证，Alpaca Paper 读取已到报价检查；完整 real-Paper-verified 与 outcome/learning-verified 均未达成。
