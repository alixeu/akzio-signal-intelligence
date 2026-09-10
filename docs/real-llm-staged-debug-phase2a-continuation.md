# Phase 2A continuation — 真实环境恢复与第二次 Evidence 执行

2026-09-09。本记录追加上一轮事实，不替代 `real-llm-staged-debug-phase2a.md`。

## 结论

Host clock 已恢复；Broker clock 的生产时间校验通过。期权分页和 Provider IV 字段兼容问题已修复并经过四资产真实采集。新的正式 Paper Run 实际执行了 Evidence，22 项 available，18 项研究资料 unavailable。随后真实 SOXX IEX 报价 ask=0 被 QuoteSnapshot 校验拒绝。属于外部行情条件下的合法安全阻断。未执行任何研究 Agent，未到达正向 Broker dispatch boundary，不能宣称完整真实 LLM 链路通过。

## Execution identity

- run_id: `fb9cba7de5e444f1`
- debug_session_id: `debug-fb9cba7de5e444f1`
- store_identity: `debug-store-dcaf4b8c91cc4645`
- run_purpose: `paper`
- code_revision: `5afdab2407ad996e8a4ecb0045db76dfacf1c253+c8c339133902e1fcdf8d7393de12a71643a09512594c2bf0f7fc91bfc3ddd827`
- runtime_identity: `ace632576d9c7ecb101310122ac232979fd30e97935b2981aeadfa8319bf0429`
- llm_mode: `real`
- broker_write_policy: `forbidden`
- learning_scope: `isolated`
- Store: `.akzio/phase2-continuation-store`
- Core: `http://127.0.0.1:17355`
- 固定运行二进制：`.akzio/phase2-evidence/continuation/akzio-core-run`
- 配置：`continuation/config.toml`（环境变量引用；不记录 credential 值）。完整 identity/dataset/contract hashes 在 `prepare.json`。
- 旧 Run `c75059ff00d84cfc`、旧 Attempt `080937ce79964f1c` 未修改。新失败 Run 也保留，不恢复 Ready、不重置预算、不覆盖历史验收。

## 环境和模型

原始 Apple NTP +17.847~17.849 秒，当前用户无非交互管理员权限。用户通过 macOS 系统设置完成校时后，五次测量跨约45秒，offset 为 +0.902、+3.423、−3.204、−0.198、+1.083 ms。测量误差区间约60~68ms。证据 `clock-after-sync.json`。没有修改 future-data 规则。

`broker-clock-validation.json`：Provider timestamp `2026-09-09T02:26:25.187174685-04:00`，接收 `06:26:25.305363Z`，生产共享 temporal validator `Ok`。系统设置的自动校时开关本身仍需要管理员读取；这里验证的是实际时间同步结果。

`debug preflight --resolve-only` 按运行时 route fallback 解析 Analyst/Critic/Synthesizer/Outcome narrative：全部 `openai_responses` / `gpt-5.6-luna` / low，shared default route by design。没有为 Critic 修改模型。此命令只解析，不调用模型；actual_model=null/unknown。上一轮真实 Responses preflight actual_model=gpt-5.6-luna 的证据保留；本轮没有新的角色 Attempt，不能将启动 capability probe 当作 Agent 已覆盖。

## 修复与工程取舍

1. `akzio-ingest/src/adapters.rs`：原 option chain 遇 next_page_token 就拒绝，四资产均受影响。改为最多16页的有界 GET 分页，拒绝重复 cursor/contract，保留原始分页响应；不发布部分链。Provider 使用 `impliedVolatility`，旧代码只读 `implied_volatility`，增加兼容解析，继续要求两个有效 IV expiration buckets。真实完整链 QQQ 6462、SOXL 2492、SOXX 2424、TQQQ 1120 contracts，全部通过。证据 `final-evidence-preflight.jsonl`，对应单元测试 `merges_complete_chain_and_rejects_duplicate_contracts`、`provider_camel_case_iv_passes_without_weakening_bucket_requirement`。
2. `akzio-ingest/src/session_bars.rs`：只读 GET 的 connect/timeout 增加与其他 Alpaca 路径一致的有界重试；不重试鉴权/永久 HTTP 错误，不改变数据范围。QQQ 单独预检持续传输失败，根因未证明；正式 Run 中同一需求实际成功。不能把重试后偶然成功归因于已证明网络根因修复。
3. `materialize_normalized.rs`：提取公开共享 acquisition/time validator，正式 materialization 与只读 probe 使用同一函数，无 Gate 放宽。
4. 新 `examples/evidence_preflight.rs`、扩展 `clock_probe.rs`：通过生产 adapter 获取数据，不创建 Run/Artifact。发现“acquisition/time 校验通过并不等于 QuoteSnapshot 合法”，补充调用正式 quote decoder + validate 输出实际买卖价，防止后续将 HTTP 成功当 Gate PASS。
5. `debug_commands.rs`：新增 `preflight --resolve-only`，显示真正 route/default 解析；未建立第二套控制器。

18 个 missing need 不是18个独立 adapter：4新闻、4持仓组成、4指数定义、4事件日历、2杠杆条款，共用 NewsWeb adapter。`workflow.rs` 的 criticality 为 directional_research；daemon 对此保留缺口并中和相关 slot，不把它们改成可选。生产注册要求 native_web_tool_verified，当前未取得证明，故未注册。没有虚构 native web capability，也没有伪造新闻。六项 paper.* 是 execution_safety，仍 fail closed。

## 全部 EvidenceNeed 现场

下表 available 表示采集状态，不意味着后续 typed snapshot / freshness / execution Gate 通过。全部40个 Need 原样保留。

| Need | Criticality | Actual | Diagnostic |
|---|---|---|---|
| paper.open_orders | execution_safety | available | none |
| research:earnings_event_calendar:QQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| option_chain:TQQQ:2026-09-09:2027-01-07 | directional_research | available | none |
| research:etf_holdings:SOXL:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| paper.fills:2026-09-09 | execution_safety | available | none |
| corporate_actions:SOXL:2025-09-08:2026-09-09 | directional_research | available | none |
| corporate_actions:QQQ:2025-09-08:2026-09-09 | directional_research | available | none |
| research:etf_holdings:TQQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:index_metadata:SOXX:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:etf_holdings:SOXX:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| option_chain:SOXL:2026-09-09:2027-01-07 | directional_research | available | none |
| research:leveraged_etf_terms:SOXL:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:earnings_event_calendar:SOXL:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:leveraged_etf_terms:TQQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:index_metadata:SOXL:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:index_metadata:QQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| research:etf_holdings:QQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| paper.account | execution_safety | available | none |
| research:index_metadata:TQQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| bars:QQQ:1d:2025-08-05:252 | directional_research | available | none |
| option_chain:SOXX:2026-09-09:2027-01-07 | directional_research | available | none |
| option_chain:QQQ:2026-09-09:2027-01-07 | directional_research | available | none |
| bars:SOXX:1d:2025-08-05:252 | directional_research | available | none |
| bars:TQQQ:1d:2025-08-05:252 | directional_research | available | none |
| research:earnings_event_calendar:SOXX:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| news:SOXX:2026-08-26:2026-09-09:market | directional_research | unavailable | adapter_unavailable |
| bars:SOXL:1d:2025-08-05:252 | directional_research | available | none |
| news:TQQQ:2026-08-26:2026-09-09:market | directional_research | unavailable | adapter_unavailable |
| corporate_actions:SOXX:2025-09-08:2026-09-09 | directional_research | available | none |
| series:VIXCLS:2025-09-08:2026-09-09:2026-09-08 | directional_research | available | none |
| paper.clock | execution_safety | available | none |
| release_calendar:2026-09-09:2026-10-24:2026-09-08 | directional_research | available | none |
| series:DFII10:2025-09-08:2026-09-09:2026-09-08 | directional_research | available | none |
| corporate_actions:TQQQ:2025-09-08:2026-09-09 | directional_research | available | none |
| news:QQQ:2026-08-26:2026-09-09:market | directional_research | unavailable | adapter_unavailable |
| paper.positions | execution_safety | available | none |
| news:SOXL:2026-08-26:2026-09-09:market | directional_research | unavailable | adapter_unavailable |
| research:earnings_event_calendar:TQQQ:2026-09-09 | directional_research | unavailable | adapter_unavailable |
| paper.quotes | execution_safety | available | none |
| series:DFF:2025-09-08:2026-09-09:2026-09-08 | directional_research | available | none |

## 新 Run 的真实阻断

Evidence Task `757d727b1a294ab8`，Attempt `73345fff90824647`，开始 `06:33:28.988616Z`，失败 `06:33:45.558464Z`。原始错误 `invalid daemon input: budget quote.price must be positive`。

独立生产 adapter 复取 `quote-probe.jsonl`：SOXX bid=514.05、ask=0、ask size=0，quote timestamp=`2026-09-08T20:00:04.020886505Z`。`decode_paper_quotes` 按 bp/ap 解析无误；Domain `Quote::validate` 要求 bid>0 且 ask>bid。外部数据不满足；未改为单边价、中间价、日线价或陈旧价兜底。其他报价也来自上一交易日，不能宣称执行新鲜度已通过。

采集器状态 available 与 typed snapshot 失败分别保留。这次 preflight 的遗漏已修：现在同时输出 `quote_validation.Err`。不会因此创建第三个明知必失败的 Run。

## Acceptance / App QA

通过正式 `debug acceptance` 写入新 Run 的 P00-P03 记录，绑定实际 Evidence Attempt；环境时间、隔离、40需求保留、期权、无下游、UI 等通过，报价阻断为 BLOCKED。文件 `acceptance-environment.json` 和 `acceptance-write.json`。不改旧失败验收。

实际现有 macOS App 连接17355，banner显示 REAL LLM / BROKER WRITE DISABLED / isolated，Store和revision一致。Evidence failed、Attempts1，所有下游cancelled/Attempts0。选中Evidence后Acceptance显示 PASS8 / FAIL0 / BLOCKED2（包含Core自动记录与人工验收）。Refresh会切回默认节点，再选Evidence可看到其验收。没有mock回退。

研究阶段 UI QA、成功 Agent 后 restart/resume：NOT_COVERED，因没有成功 Agent；不复用上一轮失败场景的恢复测试冒充本轮通过。

## Stage matrix

| Stage | TaskId | Attempt | Business result | Test result | Real LLM |
|---|---|---|---|---|---|
| Host clock | N/A | N/A | Synchronized | VERIFIED | N/A |
| Broker clock | N/A | N/A | Valid temporal basis | VERIFIED | N/A |
| Evidence | 757d727b1a294ab8 | 73345fff90824647 | Invalid SOXX quote | EXPECTED_BLOCK | NO |
| gate.paper  | 0a2f8c5686a14887 | 0 | Not reached | NOT_COVERED | NO |
| gate.evaluate  | 0d70d96bbe2346ee | 0 | Not reached | NOT_COVERED | NO |
| gate.decision  | 61654f7db8724c7c | 0 | Not reached | NOT_COVERED | NO |
| gate.execution  | 66b3c21aeaa54d39 | 0 | Not reached | NOT_COVERED | NO |
| research.analyst t5 | 682605bb336b4f86 | 0 | Not reached | NOT_COVERED | NO |
| research.analyst t1 | 7af49fb8f2ea4c1c | 0 | Not reached | NOT_COVERED | NO |
| gate.reconcile  | 8943217c52c34afb | 0 | Not reached | NOT_COVERED | NO |
| research.critic t1 | 969ca03be4284624 | 0 | Not reached | NOT_COVERED | NO |
| research.critic t5 | a080d50031694b1d | 0 | Not reached | NOT_COVERED | NO |
| research.critic t3 | d843f8f9503d424c | 0 | Not reached | NOT_COVERED | NO |
| research.analyst t3 | f86790388c6c4c57 | 0 | Not reached | NOT_COVERED | NO |
| research.synthesizer  | fc0e2899955042a3 | 0 | Not reached | NOT_COVERED | NO |

## Broker safety

本轮 broker write=0：执行路径只授权 Evidence，Paper/Reconcile 等全部0 Attempt，未产生本轮 Commitment 或 effect dispatch。配置始终 forbidden；Reconcile、PaperDispatchRuntime（`assert_debug_broker_write`）和 Store effect intent guard 未移除。该结论针对本次隔离 Core/Run；没有全机器网络抓包，不声称证明其他进程没有网络活动。正向 business-ready → debug-policy-block 分支 NOT_COVERED。

Actual Alpaca Paper、Outcome T1/T3/T5、Learning 全部 NOT_RUN。未启用自动T0或Outcome，不写 canonical learning。

## 实际操作

```bash
.akzio/phase2-evidence/continuation/akzio-core-run --config .akzio/phase2-evidence/continuation/config.toml debug prepare --session 2026-09-09
.akzio/phase2-evidence/continuation/akzio-core-run --config .akzio/phase2-evidence/continuation/config.toml debug nodes fb9cba7de5e444f1
.akzio/phase2-evidence/continuation/akzio-core-run --config .akzio/phase2-evidence/continuation/config.toml debug step fb9cba7de5e444f1 --task 757d727b1a294ab8 --wait-seconds 300
.akzio/phase2-evidence/continuation/akzio-core-run --config .akzio/phase2-evidence/continuation/config.toml debug inspect fb9cba7de5e444f1 --task 757d727b1a294ab8
cargo run -p akzio-ingest --example evidence_preflight -- 2026-09-09 paper.quotes
```

不要再次 prepare 同 Session 来复用历史失败。以后在实际双边报价恢复后先通过只读 quote probe，再建立合法新实验身份。

## 下一阶段准备

| 能力 | 状态 |
|---|---|
| Host time trustworthy | PASS |
| Full required Evidence | FAIL — execution quote invalid |
| Real Analyst all horizons | FAIL / NOT_COVERED |
| Real Critic coverage | FAIL / NOT_COVERED |
| Real Synthesizer | FAIL / NOT_COVERED |
| Decision | FAIL / NOT_COVERED |
| Execution | FAIL / NOT_COVERED |
| Broker safety | PASS — no writes; positive boundary NOT_COVERED |
| Real successful Agent restart | FAIL / NOT_COVERED |
| App full research QA | FAIL / only Evidence verified |
| 可以进入 Alpaca Paper 正向 Debug | NO |
| 可以进入 Outcome/Learning Debug | NO |

## Commands / tests 实际结果

| 验证 | 结果 | 原始记录 |
|---|---|---|
| cargo fmt --all | PASS | 已执行；git diff --check 无输出 |
| cargo check --workspace | PASS，exit0 | check-final.log |
| cargo clippy --workspace --all-targets | PASS，0 errors；1既有too_many_arguments警告（learning/evaluation/outcomes.rs:97） | clippy-final.log |
| cargo test --workspace | PASS，exit0 | tests-final.log |
| cargo build -p akzio-cli --release | PASS | core-final-build.log |
| swift test --package-path apps | NOT_APPLICABLE，exit1 no tests found | swift-tests.log |
| swift run --package-path apps DebugContractChecks | PASS，3 checks | swift-checks.log |
| scripts/update_app_and_submit_debug.sh | PASS，保留构建目录、使用全新Bundle路径，内嵌Core签名验证通过 | app-build.log |

新包 `apps/dist/phase2a-continuation/akzio.app`。实际 UI 联验使用之前已构建且连接本轮外部 Core 的 `phase2a-debug` App；新包本轮完成构建和签名验证，未将新包启动结果冒充为已观察 UI。所有上述日志位于 `.akzio/phase2-evidence/continuation/`。config与实际运行binary的SHA256见 `config-identity.json`，不公开凭据值。

本轮没有运行 fixture-debug，也没有为回归验证触发真实 Broker POST。生产 Store migration、until、Agent内部断点均未改动。最终仍未满足用户完整阶段目标，外部报价恢复前禁止继续下游。

## 后续研究/执行边界修正

后续已修正“执行报价在研究之前阻断”的职责问题；本报告中的旧失败事实仍成立，但“等待有效报价才能继续研究”不再是最终架构要求。现有PositionPlan与Paper共享研究拓扑，Paper执行资料延迟至ExecutionGate刷新。新的实现、真实Agent尝试、续传/预算问题及最终未完成范围见 [研究执行边界报告](research-execution-boundary-phase2a.md)。
