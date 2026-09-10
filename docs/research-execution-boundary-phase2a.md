# Phase 2A：现有 PositionPlan / Paper 研究执行边界修复

本报告保留此前真实失败事实；此前“等待可执行报价才能继续研究”的结论适用于旧实现，不能作为最终产品语义。Quote 校验正确，前置研究 Gate 强制采集执行状态的时点错误。

## 已实现边界

- 不新增 RunMode 或 RunPurpose。PositionPlan 输出研究与 Decision，终止于此；Paper 复用同一研究 proposal，再追加 Execution/Paper/Reconcile/Evaluate。Debug manual/pause/broker policy 是调度维度。
- `crates/akzio-daemon/src/evidence.rs`：Paper 40 个 scheduler Need 不变。六项 paper.account/positions/open_orders/fills/quotes/clock 在采集前分类，记录 `deferred_to_execution`，不调用 adapter。不作为 unavailable。研究失败原有降级、future-data 和 provenance 错误仍保持。
- `orchestration/workers.rs::prepare_position_plan`：34 个研究 Need，共享 `approved_research_proposal`；不占 Paper session slot。
- `runtime/workflow.rs::approved_research_proposal`：原正式三组 T1/T3/T5 Analyst/Critic + Synth；保留 approved_paper_proposal 委托兼容入口。Planner 保留给原其他目的。
- `application/paper_execution.rs::execution_gate`：继续先调用原 `refresh_execution_snapshots`，并使用刷新后的当前时间执行原 Gate。没有改 quote validity/freshness、approval、allocation、risk、freeze、Paper purpose 和学习保护。
- `application/research_run.rs`：补充研究仍受冻结 Session 日期限制，不把 market-open 检查用于研究。
- Store Debug projection / SwiftUI：PositionPlan 显示 NO EXECUTION/N/A；Paper 执行资料显示 Deferred/Refreshing/Pass/Blocked。readiness 由 Core 提供。

## 真实 Debug 新发现

现场保留 `.akzio/research-boundary*` 中所有 Run。旧预算失败不覆盖。

1. 全量 compare_sources 将多份252日原文重复带入下一轮模型输入。当前工作树复用原有显式 compact_governed_projection，保留时间、特征、遗漏声明和全文授权入口。
2. 实际新 Run `043b9c74ec2a411c` 的 T1 `ad772308871f4b62` / `ddf4a9f4816b463c`：真实模型返回4个 read_document 调用，整份日线造成累计78093输入预算，原48000限制正确阻止。修复为超过32KiB明确返回 DocumentRequiresRange；不静默截断，不增预算。该错误持久化后作为有界工具错误反馈，其他授权错误仍维持原失败路径。
3. Run `3e8e0d8807e2425a` 真实执行暴露心跳/Store队列互等。TaskRuntime 原 select 心跳分支内部 await Store，可能阻止 Agent future 接收已分配的队列许可，timeout也不再轮询。改为持续并行轮询心跳 monitor future，保持取消、租约与超时规则。回归 heartbeat_store_contention_keeps_handler_and_observer_progressing 通过。旧故障 Core 在正常关闭无进展后结束，仅作为故障处理，绝非单步机制；Store保留在途记录，未伪造成功。

新工具行为/代码身份均用新隔离 Store/Run验证，不在旧成功或失败任务上改合同、重置预算或覆盖产物。跨Store关联由本报告记录，不声称形成CAS fork lineage。

## 离线验证

- research_gate_defers_all_execution_safety_and_readies_analysts：零卖价不在前置采集，6 deferred、3 Analyst ready。
- position_plan_shares_research_contracts_and_has_no_execution_requirements：共享研究 Contract/预算/依赖，34研究Need，无执行链。
- execution_refresh_acquires_six_new_snapshots_and_rejects_zero_ask：真实生产刷新函数调用6次，零卖价拒绝，合法报价生成typed snapshots。此测试不冒充完整Accepted订单分支。
- research_deferral_does_not_admit_future_data：时间污染继续失败。
- position_plan_commit_and_dispatch_reject_and_paper_debug_forbids_broker：直接非法调用仍拒绝，Broker spy不触达。
- oversized_full_document_is_rejected_not_silently_truncated：显式范围读取错误，原文不改。
- heartbeat_store_contention_keeps_handler_and_observer_progressing：Store竞争下任务与观察者继续。

完整workspace tests、check、clippy日志位于 `.akzio/research-boundary-v5/`。真实阶段结果随后追加；未完成项不得引用单元测试当作真实模型通过。

## 续传真实性修复（后续现场发现）

`responses.rs::openai_response_from_raw` 原先只把当前 response.output 存入 continuation；`store=false` 的下一请求没有初始 Context 或更早的 ToolResult。这解释了新 T1 最终将未重读的价格/宏观误认为不存在。修复为实际 request.input + 当前 output 的完整 transcript；原预算继续按实际重复输入累计，未放大额度。测试 stateless_continuation_retains_initial_context_and_prior_tool_results 验证初始上下文、旧工具结果和新增输出均保留。

因此 v5/v6 的成功 Claim 只能证明提交/持久化路径，不代表全部语义验收合格。旧记录不改写，最终报告以此后发现的限制为准。

真实成功 Agent 重启：v5 Run10834c3f2b554a5d 的 T5 task c6ef8f9d17384ead / attempt55f4d2c10f5a4ddf 在07:56:03Z成功。随后正常停止Core，App显示Stale；同一binary/Store重启，AgentTurn/Claim/Tool IDs完全一致，继续执行CriticT5（之后被原4k输出预算阻断）。恢复路径验证与研究语义质量分开。

## 本轮真实结果（最终状态，不以历史 PASS 掩盖后续发现）

| Store / Run | Purpose | 实际结果 |
|---|---|---|
| v3 / 043b9c74ec2a411c | PositionPlan | Evidence成功；T1输入预算78093/48000失败 |
| v4 / 3e8e0d8807e2425a | PositionPlan | Evidence成功；T1在途遇队列互等，故障现场保留 |
| v5 / 10834c3f2b554a5d | PositionPlan | Evidence成功；T1 Draft时限、T3工具额度失败；T5提交成功并完成重启；CriticT5输出4692/4000失败 |
| v5 / c4c67b4c65c5452c | Paper | Evidence真实成功，6 execution safety deferred；三个Analyst Ready，未自动执行 |
| v6 / cb75b6bba15c4121 | PositionPlan | Evidence成功；T1提交成功但语义复核发现续传丢Context；CriticT1合法NoOutput（没有真实Critic调用证明） |
| v7 / 3ebea13d68344156 | PositionPlan | 完整续传后T1原预算阻断56854/48000；终态等待问题导致Core退出，原binary重启后Attempt记abandoned并恢复pending，Run paused。未手写Ready或清除预算 |

v7 Evidence task e52c61f64f6841ff / attempt8131a3bf6abb4af2；T1 task c2e312b36bd044fa / attempt925f4ab935bf412a。终态future释放修复已通过最终离线检查，尚未用其重跑完整真实Agent。

所有上述 Store 路径为 `.akzio/research-boundary[-vN]-store`；对应诊断目录 `.akzio/research-boundary[-vN]/`。每个prepare.json保存Run/DebugSession、code_revision、runtime_identity、contract hashes、dataset和隔离策略。v5 Core17361、Store debug-store-38c1fb86a4b040b4；v7 Core17363；所有broker forbidden、learning isolated、auto_paper=false。

### 模型调用与内容验收

全部实际角色route为openai_responses/gpt-5.6-luna，Provider actual_model也为gpt-5.6-luna。Provider request id缺失则unknown；response id使用真实返回值。以下调用清单只取已持久化AgentTurn，不包含启动能力探测和未返回usage的失败网络调用，不冒充完整账单。

| Run/Stage | Response ID | Input / Output | Latency ms |
|---|---|---:|---:|
| v3/analyst-t1 | resp_07ae431d32cdb685016aa10d1a52a487d0a8f849f5e7117796 | 10813 / 325 | 7229 |
| v5/analyst-t1 | resp_0fa18eee5a026bdc016aa10fbebfb487d083a5efd23defe5dd | 11068 / 518 | 11557 |
| v5/analyst-t3 | resp_01207e78e472bb24016aa1104dce7487d0899c3f9c3ea719ab | 11134 / 319 | 8215 |
| v5/analyst-t3 | resp_01207e78e472bb24016aa1105a796487d09137ab887c88ed44 | 5286 / 244 | 5645 |
| v5/analyst-t5 | resp_03468be5b581c1b4016aa110be762487d0a95281885068c9c2 | 11121 / 367 | 10316 |
| v5/analyst-t5 | resp_03468be5b581c1b4016aa110c8d43087d09e223b22da8a42f4 | 5329 / 1124 | 22432 |
| v5/analyst-t5 | resp_03468be5b581c1b4016aa110e048e887d0b6f60e060c581fbf | 3133 / 1645 | 31019 |
| v5/analyst-t5 | resp_03468be5b581c1b4016aa11103ce0487d08e55a893ad9daf22 | 3658 / 740 | 16812 |
| v6/analyst-t1 | resp_0ef205b24a1b38a4016aa111333f5c87d082dea8c039086f8a | 11137 / 313 | 7117 |
| v6/analyst-t1 | resp_0ef205b24a1b38a4016aa1113b9b0887d09066beb91ec9f252 | 8410 / 251 | 6064 |
| v6/analyst-t1 | resp_0ef205b24a1b38a4016aa11141fb8c87d0ad4b63ec2c1188eb | 2856 / 654 | 13457 |
| v6/analyst-t1 | resp_0ef205b24a1b38a4016aa1114f4e4487d08edce558716f7209 | 2649 / 852 | 17048 |
| v6/analyst-t1 | resp_0ef205b24a1b38a4016aa11160744887d09d5bac1c596f7d17 | 2841 / 741 | 15163 |
| v7/analyst-t1 | resp_05d67e2d75abf218016aa1126106f887d0acc5764e41764b6c | 10662 / 290 | 7247 |
| v7/analyst-t1 | resp_05d67e2d75abf218016aa11268eb2c87d0a01dbacf123f8e65 | 14112 / 1478 | 27962 |
| v7/analyst-t1 | resp_05d67e2d75abf218016aa11289dfb887d0a83cb55a6d05593f | 16093 / 1569 | 30136 |

Schema/Store成功和语义验收分开：v5 T5的中性t5 Claim持久化成功，v6 T1也通过Rust校验；随后发现Context续传缺陷，因此不宣布完整Agent质量通过。CriticT1 NoOutput只覆盖规则跳过。Synthesizer、Decision、真实Execution刷新、PaperCommit均未到达。没有将T5成功输出拼接到另一个Run来绕过失败依赖。

### Acceptance

正式debug acceptance已写入v5研究Evidence、Paper研究Evidence、AnalystT1/T3失败、T5提交结果；v7研究Evidence和完整续传预算阻断。输入文件及返回ArtifactRef保存在各诊断目录。Core自动失败/边界记录同时保留。后续发现的语义限制在本报告中明确修正，未覆盖旧CAS验收。

### App QA

实际现有App显示PositionPlan / NO EXECUTION / Execution N/A，Paper / Research → Execution / Execution Evidence DEFERRED。同一Core、Store、Run一致。失败Analyst显示Attempts1，其Critic/Synth未抢跑；T5成功后Core正常退出，App显示Stale、禁用控制；重启同binary/Store后恢复连接，历史产物ID不变。证据 `.akzio/research-boundary-v5/app-qa.md` 与 `t5-after-restart.json`。未执行阶段的输入、输出、Acceptance不标为UI通过。

### Broker安全

真实Broker writes=0（本任务隔离Run范围）：所有Run只授权研究节点，Paper的Execution/PaperCommit/Reconcile无Attempt，没有Commitment或dispatch。单元测试继续验证PositionPlan直接调用拒绝、Paper forbidden在Dispatch/Store拒绝。没有全机器抓包，不声称其他进程网络状态。业务Accepted后的真实policy boundary为NOT_COVERED。

## 最终能力矩阵

PASS注明离线时只表示代码与测试，不代表完整真实交易闭环。

| 能力 | 状态 |
|---|---|
| Existing PositionPlan preserved | PASS |
| Existing Paper preserved | PASS |
| No new redundant RunMode | PASS |
| Research/Execution boundary separated | PASS |
| ExecutionSafety deferred before research | PASS，含真实Paper Evidence |
| Research quote no longer execution blocker | PASS，真实Paper Evidence成功 |
| Execution snapshots refreshed just-in-time | PASS（源码/针对性测试）；真实阶段未到达 |
| ask=0 still blocks execution | PASS（刷新函数测试），规则未改 |
| Research Evidence safety unchanged | PASS（未来数据/缺口测试） |
| PositionPlan no-execution invariant | PASS |
| Paper execution safety unchanged | PASS（离线） |
| PositionPlan/Paper research topology shared | PASS |
| Real Analyst T1 | FAIL，提交曾成功但完整Context语义复验预算阻断 |
| Real Critic T1 | NOOUTPUT，真实模型路径未覆盖 |
| Real Analyst T3 | FAIL，工具预算阻断 |
| Real Critic T3 | FAIL，未到达 |
| Real Analyst T5 | FAIL（完整语义验收）；提交/持久化成功 |
| Real Critic T5 | FAIL，原输出预算阻断 |
| Real Synthesizer | FAIL，未到达 |
| Real Decision | FAIL，未到达 |
| Successful Agent restart | PASS，持久化意义；不等同语义质量通过 |
| App QA | PASS（Purpose/Deferred/失败/重连）；全研究链未覆盖 |
| Broker positive safety boundary | NOT_COVERED |
| 可以进入下一阶段 Outcome/Learning Debug | NO |

剩余问题是完整授权Context下，模型工具/Draft/Submit能否在原48k/120s等Contract预算内稳定形成语义正确结果。不能通过丢Context、扩大预算、缩减EvidenceNeeds或伪造输出解决。本文交付是边界修复及真实失败定位，完整Phase2A仍未完成。

## 最终检查与源码定位

| 命令 | 实际结果 |
|---|---|
| cargo fmt --all / git diff --check | PASS |
| cargo check --workspace | PASS |
| cargo clippy --workspace --all-targets | PASS，0 errors；既有learning/evaluation/outcomes.rs:97参数数量warning |
| cargo test --workspace | PASS；最后一次日志tests-final.log |
| swift run --package-path apps DebugContractChecks | PASS，3 checks |
| scripts/update_app_and_submit_debug.sh | PASS；preserve模式，新包research-boundary-final-v2/akzio.app，内嵌Core及签名验证通过 |

App最后新包完成构建签名；上述实际UI观察来自research-boundary现有App连接外部隔离Core。没有把新包未运行的界面当作已QA。

源码入口（当前行号）：
- `crates/akzio-daemon/src/evidence.rs:23`：研究采集分流；`:31` Deferred；`:971` 原执行刷新。
- `crates/akzio-daemon/src/application/paper_execution.rs:50`：执行前真实刷新调用。
- `crates/akzio-runtime/src/runtime/workflow.rs:258`：共享研究proposal。
- `crates/akzio-daemon/src/orchestration/workers.rs:184`：PositionPlan准备。
- `crates/akzio-store/src/store/debug.rs:392`：执行资料状态投影。
- `crates/akzio-context/src/context_broker/reads.rs:5`：显式全文大小边界。
- `crates/akzio-runtime/src/runtime/task.rs:142`：并行心跳与结束前释放future。
- `crates/akzio-model/src/responses.rs:360`：完整stateless transcript。

所有历史Store、失败Attempt和CAS保留。未提交Git、未修改生产Store、未改IEX/SIP或任何交易阈值。无需先恢复SOXX报价才能研究；只有实际Execution阶段需要合法当前报价。

## 最后一次预算核对（完整续传实验）

v7 Analyst T1 的三次已持久化真实 Provider 输入分别为 10,662、14,112、16,093 tokens，总计 **40,867**；输出分别为 290、1,478、1,569，总计 3,337。**56,854 不是已经向 Provider 消耗的输入总量**，而是继续执行修复请求时触发的预算检查值。原 48,000 上限未改变。

这次 Submit 包含截短的 basis Artifact ID，并把新闻/宏观补充请求表达为 alpaca/bars。不能把该输出作为业务合格 Claim。模型已经完成真实 Draft/Submit，但成功提交及完整语义验收仍未通过；继续修复所需预算不足时维持阻断，不重置 Attempt 预算。原始证据见 `.akzio/research-boundary-v7/plan-research-analyst-t1-after.json`。当前 Run 保持 paused，恢复后的 Attempt 状态按 Store 记录为 abandoned，Task pending；没有手工修改终态。
