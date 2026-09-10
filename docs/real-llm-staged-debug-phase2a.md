# Real LLM Staged Debug — Phase 2A

实际执行日期：2026-09-09。**状态：BLOCKED，第二阶段 A 未完成；不可进入真实 Alpaca Paper 或 Outcome/Learning Debug。**

真实 Responses Preflight 成功。正式 Paper EvidenceGate 实际执行三次，定位并修复采集时间冻结及失败可观察性问题，继续复验后确认本机系统时间落后约 17.84 秒。Broker 时钟因此成为未来数据，被原业务规则正确阻止。没有执行任何研究 Agent，没有 Broker write。不能把此次 Preflight 当成 Analyst/Critic/Synthesizer 的验收。

## 1. Execution Identity

| 项目 | 最终诊断 Run |
|---|---|
| RunId | `c75059ff00d84cfc` |
| DebugSessionId | `debug-c75059ff00d84cfc` |
| Store identity | `debug-store-6d87ec14ab9e4a7c` |
| Store root | `.akzio/phase2-diagnostics-store` |
| Core | `http://127.0.0.1:17354` |
| 执行 binary | `.akzio/phase2-evidence/akzio-core-diagnostics` |
| Code revision | `5afdab2407ad996e8a4ecb0045db76dfacf1c253+4fa9f88b87e31f4e4c72b0873ebdb1bb92ded5f0a4af6343a1fc9b6e7098d0d4` |
| Runtime identity | `9ea02e0676a0876c5cb8416b8fdc148ce41e0a163713ce59b2518ac4caa0f275` |
| Run purpose / mode | Paper / manual / Paused |
| LLM | real；OpenAI Responses；默认 gpt-5.6-luna / low |
| Broker policy | forbidden |
| Learning | isolated |
| 自动调度 | auto_paper=false；outcome_processing=false |
| Session key | 2026-09-08，检查时最近已结束的美股交易日 |
| 当前 Evidence Attempt | `080937ce79964f1c`，FAILED |

配置从仓库 `config/akzio.observatory.toml` 派生，使用现有环境变量凭据；没有修改用户级配置、生产 Store、审批、校准或风险参数。保留原 paper-engineering、IEX、100 ppm transaction cost / 50 ppm slippage。

发现 `~/.akzio/config.toml` 的 endpoint 字段疑似误填凭据格式，未使用该值。报告和新增文件不包含凭据。当前实际配置无角色 routes，也无 release_date / knowledge_cutoff；这些元数据是 unknown，不能把 AGENTS 文档中的角色模型表当成实际路由，更不能声称 canonical research model identity 已合格。

历史失败运行均原样保留：

| Run | Store | 结果 |
|---|---|---|
| dfb3f6bc1c3d4eb1 | `.akzio/phase2-store` | 原实现真实 Evidence 失败；用于同 binary / Store 重启及 App Stale/reconnect |
| c70ff9b47109447b | `.akzio/phase2-fixed-store` | 批次 cutoff 修复后的真实复验；失败 Acceptance 在 App 可见 |
| c75059ff00d84cfc | `.akzio/phase2-diagnostics-store` | 40 needs 全量诊断；最终环境时间阻断 |

三者都是独立新 Run，没有重写旧 Attempt、复用失败输出或强制 requeue。由于修改影响证据时间语义，且原 gate max_attempts=1，不能在原 Run 上合法 retry。现有 `debug experiment` 会创建非 Paper purpose，故为保持本次要求的正式 Paper 身份，在新隔离 Store 创建新 Run。此处的实验关联记录在本报告，**没有声称建立了跨 Store CAS fork lineage**。

## 2. Real LLM Preflight

新增 `debug preflight`，复用既有 `probe_capabilities` 的生产 Responses adapter、SSE parser、required function、stateless continuation 和 usage parser。没有创建虚构 Task/Attempt，没有调用独立 LLM HTTP 客户端。

| 实际 route | Provider | Requested / Actual model | Provider request ID | Response ID | Usage in/out | Latency | Result |
|---|---|---|---|---|---:|---:|---|
| default，首次 required function | openai_responses | gpt-5.6-luna / gpt-5.6-luna | unknown | resp_0e2bc3debe7ab546016aa0eef356e487d0befcee764f6ed6d8 | 360 / 17 | 3651 ms | PASS |
| default，stateless continuation | openai_responses | gpt-5.6-luna / gpt-5.6-luna | unknown | resp_0e2bc3debe7ab546016aa0eef4d02087d0aea09363435e3c7f | 401 / 18 | 1770 ms | PASS |

两个请求均 stream=true、缓存/推理 tokens=0。Provider 未提供 x-request-id，未生成假 ID。工具分别为既有 `akzio_capability_probe`、`akzio_capability_probe_complete`。完整白名单审计在 `.akzio/phase2-evidence/preflight.json`，同时作为 Preflight Acceptance 实际内容持久化进最终 Store。

Analyst、Critic、Synthesizer、Outcome 均继承 default，并不存在四条独立配置。Core 启动/重启也会执行原有两次真实能力调用；上述仅是有完整导出审计的两次，不是所有启动调用的完整账单。失败 Preflight 的部分响应审计尚未增加。

## 3. Stage Matrix

P00：环境 FAIL（本机时钟落后）；P01：正式图 PASS；P03：NOT_RUN，未产生任何研究 ContextManifest / ReadGrant。

| Stage | TaskId | Attempt count | Business Result | Test Result | LLM | Fixes |
|---|---|---:|---|---|---|---|
| P02 EvidenceGate | `a30f0a52bde74369` | 1 | Failed: temporal contamination | BLOCKED | N/A | 时间冻结、逐项诊断、失败持久化 |
| P04 Analyst T1 | `585d84229a5c4f97` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P05 Critic T1 | `7ee3361cb03c4703` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P06 Analyst T3 | `30eb622b9c5942ec` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P07 Critic T3 | `8882080ac8b24299` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P08 Analyst T5 | `5e79bb5621c94aa8` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P09 Critic T5 | `eb0f1eadc5c24ff3` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P10 Synthesizer | `8b298521ab43418d` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | NOT_COVERED | — |
| P11 DecisionGate | `ce6bb095570549b1` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | N/A | — |
| P12 ExecutionGate | `e264a438dfdb4ba7` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | N/A | — |
| P13 PaperCommit | `9225aaef99ac483b` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | N/A | — |
| P14 Reconcile | `110dd4ef7e734c2d` | 0 | Not executed; cancelled by upstream failure | NOT_RUN | N/A | — |

所有 12 个下游节点均为 0 Attempt。没有合法 Critic NoOutput 路径被执行，因此不能标注 LEGITIMATE NOOUTPUT。Outcome/Evaluate 尚未执行。

## 4. 每阶段详细验收

### P00 环境

- Store、Debug mode、broker forbidden、learning isolated、执行 revision：PASS。
- 真实 transport：PASS，仅指 Preflight。
- 系统时间：FAIL。项目 Alpaca Evidence adapter 只读获取 `paper.clock`：本机接收 `2026-09-09T05:36:38.232467Z`，Provider `2026-09-09T01:36:55.953710181-04:00` 即 UTC `05:36:55.953710181Z`，领先本机 17.721243714 秒。
- 独立 `sntp -t 3 time.apple.com`（无设置时间参数）测得 `+17.840211 +/- 0.060383` 秒，佐证是本机时间落后。没有修改系统时钟或容差。
- 证据：`clock-probe.json`、`time-check.log`、P00 Acceptance。

### P01 Workflow

实际 `debug nodes` 验证 13 个唯一 TaskId，无 Planner，三个 horizon 各一对 Analyst/Critic，Synthesizer 依赖六个研究任务及 EvidenceGate。初始全部 0 Attempt / Paused；只放行 Evidence Task。P01 Acceptance PASS。

### P02 Evidence

实际 40 needs：17 available，23 unavailable。分类：18 adapter_unavailable、4 transport、1 temporal_contamination。

- available：四资产各 252 根 OHLC 日线、四项 corporate actions、三项 FRED series、一项 release calendar，以及 account/positions/open_orders/fills/quotes。
- 日线独立核对 symbol、至少 252 根、OHLC 全正，四项均 PASS。量化特征保留原 price_micros / ppm 定义，未修改单位转换。
- account 的 USD、价格与股数等原始字段保持原标准化内容。未执行 Decision，因此没有把本次检查声称为组合金额、杠杆风险或分配独立复算完成。
- 17 项 normalized payload 的 ArtifactId、blob hash、schema、resource、time_basis、contamination certificate 保存在 `normalized-evidence-audit.json`；Store doctor PASS。
- 18 项新闻/持仓研究/指数元数据/事件日历/杠杆条款缺口来自现有配置没有提供所需 adapter，没有造资料。四个 option_chain transport 错误仍需后续在环境时间恢复后进一步诊断；没有把它们认定为成功数据。
- `paper.clock` 是 execution_safety need。Provider clock 比实际接收时间晚，原 `EvidenceTimeBasis` 规则拒绝该时间关系；即便其他输入成功，整个 Gate 仍 FAILED。
- `evidence.collection_status` CAS 包含全部 40 项；成功材料作为失败 Attempt 的审计资料保存，**没有发布成成功 attempt outputs**。
- P02 business_result=RejectedTemporalContamination；test_result=BLOCKED。`evidence.no_future_data` 检查 PASS 只证明正确拒绝，并非 EvidenceGate 整体验收 PASS。

### P03–P14

NOT_RUN。无真实研究 Context、ReadGrant、Draft、Submit、Agent usage、Claim、Critique、DecisionProposal、Decision、Execution 或 Commitment。跨 Run/错误 kind/future Outcome 的真实本次 Context 负向验收未运行。未执行的阶段没有伪造 Acceptance Attempt 绑定。

环境/图/Preflight 是 Run 级观察，但现有 Acceptance 要求真实 Task+Attempt；本轮将它们明确附在首个真实 Evidence Attempt 上记录，未创建假节点。最终 Store 有六条 Acceptance（含两个自动边界/失败记录），UI 检查计数为 PASS 20 / FAIL 1 / BLOCKED 2。

## 5. 实际 LLM 调用覆盖

- REAL VERIFIED：默认 route 的 required function、真实 stream、usage、stateless continuation。
- NOT_COVERED：Analyst T1/T3/T5、Critic T1/T3/T5、Synthesizer、Outcome narrative 的真实业务调用。
- LEGITIMATE NOOUTPUT：本轮没有。
- 未改 Prompt、Contract、Tool schema、模型路由，未用 fixture LLM 冒充生产结果。

## 6. Bug List 与工程取舍

| 问题 | 根因 / 源码 | 修复 | 回归 / 真实复验 |
|---|---|---|---|
| 实时采集时间天然晚于请求前 cutoff | `crates/akzio-daemon/src/evidence.rs:26` | 实时 Paper 先 acquire，随后以同一 batch cutoff materialize；原历史/fixture 路径及时间不变量保留 | 时间边界测试 PASS；实际 account 等 17 项标准化成功。Broker clock 真实未来时间仍被拒绝 |
| TemporalContamination 提前 return 丢失逐项诊断 | `crates/akzio-daemon/src/evidence.rs:85` | 记录该 need 为 temporal_contamination；保持 fatal，汇总全部状态并保存成功审计资料后失败 | 实际 40 项状态完整可读，下游 0 Attempt |
| 普通 Stage 失败只在进程日志，App 无具体原因 | `crates/akzio-daemon/src/dispatch.rs:36` | 原 Attempt 存活时写现有 StageAcceptance，Inspector 按已有脱敏策略投影 | 新 Run App 显示原错误，CLI/Store 一致 |
| Preflight 只有能力布尔值，没有响应审计 | `crates/akzio-model/src/model_client/capability_probe.rs:47`；CLI `debug_commands.rs:97` | 复用同一 probe，导出白名单 metadata、usage、latency、tools；拒绝 fixture client | 新审计单测及两次真实调用 PASS |
| 本机系统时间落后 | Broker read / NTP 独立证据 | 未改系统设置，未放宽 future-data 规则 | 外部环境 BLOCKED |

补充只读诊断入口 `crates/akzio-ingest/examples/clock_probe.rs`，只通过生产 Alpaca Evidence adapter GET clock，输出白名单时间字段，不提供下单能力。

未修复/覆盖：Execution refresh 的原始请求前 cutoff 路径尚未实际到达；采集耗时导致 quote stale 的后续情况也未到达。当前修复集中于实际失败的初始 EvidenceGate，不能据此宣称 Execution 时间问题全部解决。没有实现 until、Agent 内部暂停点或 production migration rehearsal。

## 7. Paper Safety

**本次隔离 Debug 执行的真实 Broker write count=0。** 证据范围是这三个隔离 Core/Run，不是其他进程或整个账户：

1. 每个 session 的不可变 broker policy 为 forbidden；auto_paper=false、outcome_processing=false。
2. 仅三个 EvidenceGate 各执行一次，所有 PaperCommit / Reconcile / Execution Task 均 0 Attempt。
3. 最终 inspect 中 Commitment / Receipt artifact 为 0，没有 dispatch/effect 路径被执行。Evidence adapter 使用 GET；LLM `/responses` POST 是模型调用，不是 Broker write。
4. 没有启用 paper_allowed，未创建审批、校准或执行权限。

**没有到达 Accepted Commitment → Reconcile debug-policy block 分支，因此该分支 NOT_COVERED。** 不能把本次 Evidence 的时间 BLOCKED 写成 Broker Debug Safety Boundary 的成功验证。

## 8. App QA

实际启动 Phase1 App 检查早期两个 Run，最终启动新构建签名的 `apps/dist/phase2a-debug/akzio.app`，连接最终 Core 17354。

- 环境条、RunId、Store identity、purpose、revision、REAL LLM / BROKER WRITE DISABLED：实际核对。
- Evidence Task / Attempt / Failed；下游 0 Attempt；原错误和 Acceptance expected/actual：实际核对。
- 最终 Acceptance PASS 20 / FAIL 1 / BLOCKED 2，P00 EnvironmentBlocked / FAIL，P01 Prepared / PASS：实际核对。
- 原 Core 在 Paused failed Run 上停止，App 显示 Stale、最后更新时间、控件禁用；同 binary/Store 重启后自动 Connected，历史 Attempt/Acceptance 保留。
- 此重启只覆盖失败 Evidence Run，**不是已完成真实 Agent checkpoint 的恢复证明**。
- Timeline 显示本 Run 及现有入口；多历史 Outcome 时间轴本轮未执行。
- 关键研究阶段 UI、Draft、Tools、Submit、输出、预算联验 NOT_COVERED。
- 原生 accessibility QA 记录：`.akzio/phase2-evidence/app-qa.md`。没有生成 mock 截图。

## 9. Commands / Tests

| 实际命令 | 结果 / 证据 |
|---|---|
| cargo fmt --all；cargo fmt --all -- --check | PASS；fmt.log |
| cargo check --workspace | PASS；workspace-check.log |
| cargo clippy --workspace --all-targets | PASS，1 条原有 too_many_arguments warning：learning/evaluation/outcomes.rs:97；clippy.log |
| cargo test --workspace | PASS；workspace-tests.log；这是离线回归，不是业务真实 LLM 覆盖 |
| cargo test -p akzio-model -p akzio-ingest -p akzio-daemon | PASS；targeted-tests.log。首次新增测试构造器写错导致编译失败，修正后通过；保留原失败日志 |
| audit_preserves_provider_ids_and_unknown_usage_without_raw_response | PASS：真实 ID 保留、unknown usage 不伪造、不输出 raw secret |
| receipt_time_requires_completed_snapshot_and_future_data_stays_blocked | PASS：请求前 cutoff 拒绝、完成后合法时间通过、未来事件仍拒绝 |
| swift run --package-path apps DebugContractChecks | PASS：3 checks |
| swift run --package-path apps DebugContractChecks .akzio/phase2-evidence/final-inspect.json | PASS：实际 c75059ff00d84cfc / 13 nodes / paused 解码 |
| scripts/update_app_and_submit_debug.sh | PASS：preserve 模式、新 bundle 路径；包含 Rust core；签名 valid on disk；app-build.log |
| store doctor，最终 Acceptance 写入后再次检查 | PASS：final-doctor.json ok=true |
| sntp -t 3 time.apple.com | 只读测量 +17.840211 秒；没有设置系统时间 |

可复查实际保留 Run（当前正确动作是 inspect，不是 retry）：

```bash
.akzio/phase2-evidence/akzio-core-diagnostics --config .akzio/phase2-evidence/config-diagnostics.toml debug nodes c75059ff00d84cfc
.akzio/phase2-evidence/akzio-core-diagnostics --config .akzio/phase2-evidence/config-diagnostics.toml debug inspect c75059ff00d84cfc --task a30f0a52bde74369
```

实际 Preflight 命令（会发生真实模型调用）：

```bash
.akzio/phase2-evidence/akzio-core-diagnostics --config .akzio/phase2-evidence/config-diagnostics.toml debug preflight
```

## 10. 下一阶段准备矩阵

这里的 FAIL 表示本轮尚未达到该项验收，不等同于已经发现该模块业务实现必然错误。

| 能力 | 状态 |
|---|---|
| Real Analyst T1 | NOT_COVERED |
| Real Critic T1 | NOT_COVERED |
| Real Analyst T3 | NOT_COVERED |
| Real Critic T3 | NOT_COVERED |
| Real Analyst T5 | NOT_COVERED |
| Real Critic T5 | NOT_COVERED |
| Real Synthesizer | FAIL：NOT_RUN |
| Evidence | FAIL：环境时间 BLOCKED |
| Context/ReadGrant | FAIL：NOT_RUN |
| Decision | FAIL：NOT_RUN |
| ExecutionGate | FAIL：NOT_RUN |
| PaperCommit | NOT_REACHED |
| Broker Safety Boundary | FAIL：Accepted dispatch 分支未到达；本次 write=0 |
| UI Real-Run Inspection | FAIL：Evidence/环境已联验，研究阶段尚未覆盖 |
| Restart/Resume real run | FAIL：失败 Evidence Run 重启已验证，成功 Agent 恢复未覆盖 |
| 可以进入真实 Alpaca Paper Debug | NO |
| 可以进入 Outcome/Learning Debug | NO |

继续前需要先恢复本机准确系统时间并重复只读 clock 检查；禁止用重写 provider timestamp 或增加 future-data 容差代替。还需明确实际角色 routing/model identity、补齐所需真实研究资料 adapter，并诊断 option-chain transport。当前失败 Run 不可重置为 Ready；环境恢复后应创建新的合法正式隔离 Run，保留本次记录。实际 Paper、跨日 Outcome、Learning 一律 NOT_RUN。
