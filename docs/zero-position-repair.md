# 持续零目标仓位：当前修复与验证报告

本报告对应当前工作区的 Debug/CAS/export 修复，不把历史聊天或旧 Run 的结论当作当前运行事实。所有真实运行均使用新的隔离 Store，`broker_write_policy=forbidden`、`learning_scope=isolated`、`auto_paper=false`。

## 当前成功的完整研究 Run

```text
run_id:             7cf9253b18bc4fea
purpose:            position_plan
model:              gpt-5.6-luna
reasoning_effort:   low
policy status:      unconfigured
decision capable:   false
workflow:           completed
broker writes:      0
```

Task 结果为 EvidenceGate、T1/T3/T5 Analyst、T1/T3/T5 Critic、Synthesizer、DecisionGate 全部 succeeded。该 Run 的 7 个研究 Agent 均实际完成 Draft/Submit，共 14 次真实模型调用；不能沿用其他历史 Run 的 Rust-owned NoOutput 描述。

DecisionGate 输出：

```text
DecisionContext: da7a6aaac592531f954f68be8104502eb993cfacd8d599644d86f30b09733991
Decision:        725e0e4ae9711b577fc6a472701fc7d76fe598bae147111af8b0c1c97f3064ed
```

目标权重仍为：

```text
TQQQ = 0 ppm
QQQ  = 0 ppm
SOXX = 0 ppm
SOXL = 0 ppm
```

这次零目标仍然是合法的 fail-closed 结果，不是被强制改成非零，也不是 Broker cash 证明。

实际 DecisionContext runtime trace 已记录：

```text
first_zeroing_branch = decision.eligible_set.empty
TQQQ first exclusion = decision.calibration.missing
QQQ  first exclusion = decision.calibration.missing
SOXX first exclusion = decision.calibration.missing
SOXL first exclusion = decision.calibration.missing
```

同时保留：

```text
hard blockers: missing_evidence, unverified_claim
soft warnings: low_confidence, incomplete_evidence, correlated_consensus
calibrated_assets: 0
covariance_sample_count: 0
risk metrics: unknown/null
```

## 已实施的修复

### Policy readiness

- 显式 `decision_policy_path` 使用严格 provenance-bearing Artifact envelope；裸旧 `DecisionPolicy` JSON 只能通过兼容 decode，不再被新 loader 或 calibration validate/inspect 报为标准 frozen policy。
- policy 在正常 daemon 启动中只加载一次，再将同一个 loaded policy 同时用于 `DaemonConfig`、runtime identity 和 DebugSession policy audit，避免文件在启动期间变更造成 split-brain。
- `decision_capable()` 现在要求：
  - active forecast calibration scope；
  - 四资产 risk calibration；
  - 每资产 T1/T3/T5 forecast calibration；
  - sample/Brier 条件；
  - portfolio risk sample/covariance。
- `DaemonHealth` / `ready` 暴露 `decision_policy_status`、policy hash、policy input hash 和 `decision_capable`。Process ready 不再被错误解释成 calibration ready。
- fixture runtime 若显式提供 `decision_policy_path` 会明确拒绝，不再静默忽略。

### Context source/projection 分离

- `ContextPolicy` 新增有界 `max_source_bytes`；`max_bytes` 表示 compact projection budget。
- `ContextSelection.projected_bytes` 和 `ContextManifestPayload.projected_bytes` 持久化分离预算。
- Analyst/Synth/Child Manifest selection 使用实际 compact projection bytes/tokens 进行模型预算，原始 CAS 文档仍受独立 source cap 和既有 range-read 权限约束。
- DecisionGate 的 Manifest closure validator 同步接受新 source/projection 语义，避免把 projected token 与 raw blob bytes 错误比较。
- `read_range` 在原始 logical JSON 与重新序列化 Value 字节不一致时不再给出可能错误的 field offsets；新增 JSON key-order 回归测试。

### Runtime Decision trace

DecisionGate 新增可选 Rust-owned `runtime_trace`，在实际 predicate 执行时记录：

- `first_zeroing_branch`；
- 每资产 `first_exclusion`；
- rule id/version；
- source location；
- inputs/operator/threshold/result/reason；
- short-circuit 语义。

现有 DecisionContext/Decision JSON 仍兼容读取；旧 payload 缺少 trace 时导出继续标记 `not_recorded`，不会由 exporter 重算。

### Export/audit

当前 POSIX 脚本和 Store exporter 还新增：

- `context_coverage.json`；
- `draft_submit_coverage.json`；
- `stage_acceptance.json`；
- source/projection byte 和 token 记录；
- 实际 policy status/hash；
- Rust runtime trace；
- recursive share-safe redaction。

## 当前完整真实包

- [manifest.json](/Users/alixeu/project/akzio-signal-intelligence/target/final-audit-v4-bundles/akzio-debug-7cf9253b18bc4fea-20260910T124427Z-95407/manifest.json)
- [context_coverage.json](/Users/alixeu/project/akzio-signal-intelligence/target/final-audit-v4-bundles/akzio-debug-7cf9253b18bc4fea-20260910T124427Z-95407/context_coverage.json)
- [rust_decisions.jsonl](/Users/alixeu/project/akzio-signal-intelligence/target/final-audit-v4-bundles/akzio-debug-7cf9253b18bc4fea-20260910T124427Z-95407/rust_decisions.jsonl)
- [policies_and_risk.json](/Users/alixeu/project/akzio-signal-intelligence/target/final-audit-v4-bundles/akzio-debug-7cf9253b18bc4fea-20260910T124427Z-95407/policies_and_risk.json)
- [完整 tar.gz](/Users/alixeu/project/akzio-signal-intelligence/target/final-audit-v4-bundles/akzio-debug-7cf9253b18bc4fea-20260910T124427Z-95407.tar.gz)

该包的实际统计：

```text
Artifacts:       152
Events:          185
LLM calls:        14
Rust records:    107
Tool records:      0
Integrity:      complete
```

Context coverage 已记录 source/projection 分离；该 Run 的 policy status 是 `unconfigured`，因此最终零目标仍应被解释为“研究完成但校准/风险政策未就绪”，不能解释为信号已经证明为空。

## 仍未完成或受外部条件阻断

- Allocation now treats `p=500000` with `expected_return_ppm=0` as an
  explicit abstention. A positive calibration bin cannot resurrect that slot;
  the runtime trace records `decision.forecast.abstention`.
- Scoped Critique blockers are evaluated per asset and horizon. A blocking gap
  for TQQQ/T1 does not discard a separately grounded QQQ/T1 slot, while a
  blocker without a directional scope remains Claim-wide.
- Native hosted web probing uses Responses `tool_choice=required` and records
  whether the route did not call search, returned no verifiable sources,
  failed source validation, rejected the tool, or hit a provider/transport
  error. An optional `evidence.news_web` route is supported; no search model is
  substituted automatically.
- Oversized option chains are represented by a bounded Rust-owned
  `evidence.option_projection` SemanticDetail artifact. It keeps the original
  CAS artifact, hash, logical byte count, time basis, available/missing fields,
  and source lineage while exposing only a small feature summary to the Agent.
- The model-visible task contract now carries both the immutable Contract
  budget and the resolved cumulative Attempt budget, so the configured 1M
  input/unlimited-tool setting is not shown as a misleading 48k runtime limit.
- `akzio calibration export` reads canonical sealed Paper outcomes through the
  read-only Store API and emits real forecast/return samples plus a quality
  report. It blocks on immature outcomes, missing point-in-time labels,
  identity mismatch, price conflicts, or insufficient samples; it does not
  manufacture a policy.

- Native hosted `web_search` 是否可用仍必须由当前网关真实 capability probe 和 `action.sources` 验证；普通 Responses/Luna 成功不能替代这一层。若网关返回 502/unknown provider model，状态应继续是 BLOCKED，不把 unavailable 改成 available。
- 当前完整 Run 没有 Context Tool call；上下文是 Rust 预物化的，不能把 `tools.jsonl=0` 说成工具往返已验证。
- 没有有效历史 Luna Forecast + 到期 realized return 数据，因此没有把默认空政策伪装成已校准政策，也没有把合成数据写入真实 Run 或正式 policy。
- 当前 Run 是 PositionPlan；没有 ExecutionGate、PaperCommit、Reconcile、Outcome 或 T+1/T+3/T+5，所以 `real-Paper-verified` 和 `outcome/learning-verified` 仍是 NOT_REACHED。
- 旧 Run 缺少 runtime trace 的部分继续是 `not_recorded`，不会回填。

## 验证状态

```text
code_implemented                    PASS
offline_regression_verified         PASS
real_luna_transport_verified        PASS（当前完整 PositionPlan Run）
real_native_web_verified            BLOCKED/未形成当前 Run 的 hosted search 证据
context_projection_separation       PASS（离线 + 当前新 Run Manifest）
structured_research_coverage        PASS（3 Analyst + 3 Critic + Synth + Decision）
calibration_data_ready              BLOCKED（无合格真实历史样本）
calibration_policy_loaded           NOT_RUN / unconfigured_fail_closed
risk_model_ready                    BLOCKED（当前 policy 未配置）
real_position_plan_verified         PASS（目标计划已由 Rust DecisionGate 生成）
runtime_rule_trace_verified         PASS（当前新 DecisionContext 含 trace）
business_stage_acceptance_verified  PARTIAL（controller boundary 有记录，非所有业务阶段适用）
export_bundle_verified               PASS
real_paper_verified                 NOT_REACHED
outcome_learning_verified           NOT_REACHED
```

## 本轮最终离线复核

以下结果对应当前工作区最后一版代码，而不是历史 Run：

```text
cargo fmt --all -- --check                         PASS
git diff --check                                   PASS
cargo check --workspace                            PASS
cargo clippy --workspace --all-targets -- -D warnings PASS
cargo test --workspace                             PASS（74 个单元/集成测试；无失败）
swift build（apps）                                PASS
```

新建隔离 Store 后执行 `run fixture-debug` 也完成：Run `0684c927207d4f52`、`purpose=paper_dry_run`、`evidence=fixture/offline`。该命令在临时 daemon 关闭前调用 Store Doctor 并成功返回；daemon 退出后再单独执行认证 `store doctor` 会因没有 token/服务而失败，这是连接生命周期的预期结果，不是 Store 完整性结论。

该 fixture 仍然只证明离线控制器、Store 生命周期和固定响应路径；它不增加真实 LLM、hosted Native Web、Alpaca Paper、跨交易日 Outcome 或 T+5 Learning 证据。
