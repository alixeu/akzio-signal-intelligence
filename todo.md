# Akzio Signal Intelligence v2 — 全部 Wave TODO

更新时间：2026-08-18（Asia/Singapore）  
范围：放弃旧 Paper Run 后，从新 Store 重新完成全部 Wave。

## 当前结论

代码实现和离线验收已基本完成；尚未完成的部分主要确实是因为新的 Paper Run 和后续真实交易日数据还没有产生。

当前没有新的 canonical Paper Run，因此以下链路不能提前完成：

```text
新 Paper Canary
  → OutcomeSchedule
  → 真实 T+1
  → 真实 T+3
  → 真实 T+5 sealed Outcome
  → Experience/Evaluation
  → 人工审批
```

旧 Run `77395cfd-8d03-405d-9b47-ca99b19525f1` 已放弃，不恢复、不执行其 Outcome worker，也不把它当作新 Wave 的证据。

旧 bundle 已从工作区移除；当前不修改 `config/akzio.local.toml`，不删除其他 bundle。

## Wave 状态

- [x] CQ1–CQ4 代码质量整改
- [x] CQ5 测试布局
- [x] CQ5 Legacy 清理
- [x] R0–R10 实现
- [x] R0–R10 离线窄验收和 workspace 验证
- [x] Wave H SQLite-only 存储实现
- [x] Wave H fresh Store 离线验收
- [ ] 新 Wave B Paper Canary
- [ ] Wave C 真实 T+1 Outcome
- [ ] Wave D 真实 T+3 Outcome
- [ ] Wave E 真实 T+5 Outcome
- [ ] Wave F Outcome-backed Learning
- [ ] Wave G 最终人工审批

## 已完成的证据

- `cargo fmt --all -- --check`
- `cargo check --workspace`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace`
- `zsh -n scripts/paper_canary_run.zsh`
- `zsh -n scripts/debug_goal_run.zsh`
- fresh Store 的 Doctor、Replay、SQLite-only export、backup/restore、引用闭包检查
- SQLite-only Store：payload 存储在 `rebuild_blobs`，不恢复文件 CAS
- Store schema 保持 11，Domain/Artifact schema 保持 10
- Artifact ID、blob SHA-256、ExecutionPlan serde、plan hash、Paper gate、事务边界、provenance/source_refs 和 learning policy 未改变

## Wave B — 新 Paper Canary

### 入口条件

- [ ] 确认 daemon、scheduler 和相关端口状态正常
- [ ] 使用 scheduler-owned Paper 路径，不能直接 POST 或手工 retry
- [ ] Alpaca endpoint 精确为 Paper
- [ ] account 未 blocked，且 `trading_blocked = false`
- [ ] 记录运行前 account、positions、open orders、market clock 和四资产 quotes
- [ ] 历史 Paper 持仓、filled orders、盈亏不作为阻断条件
- [ ] open orders 只作为诊断；若发现相同 client-order ID 的非终态订单或明确 duplicate/conflict，必须 fail-closed
- [ ] 四资产闭集、Paper approval、session slot、lease/epoch、非零 transaction cost/slippage 均通过

### 执行

```bash
scripts/paper_canary_run.zsh config/akzio.local.toml
```

必须生成新的时间戳 bundle、Store、Run ID 和 broker session key。不能复用旧 Run、旧 task IDs、旧 commitment 或旧 client-order IDs。

### 出口条件

若产生 accepted plan：

- [ ] 所有 broker receipts 进入终态
- [ ] Reconciliation 为 `complete`
- [ ] 创建一个 `OutcomeSchedule`
- [ ] Doctor 通过
- [ ] Replay 通过
- [ ] SQLite-only export 通过
- [ ] 立即创建 backup，并在新目录 restore 后重新运行 Doctor/Replay
- [ ] secret scan 通过

若产生合法 NoOrder：

- [ ] 记录真实 Paper 安全结果
- [ ] 不为了制造订单而修改 Decision 或强迫模型交易
- [ ] 若没有 `OutcomeSchedule`，不能进入 Outcome Wave

## Wave C — 真实 T+1 Outcome

前置条件：新 Paper Run 已有合法 `OutcomeSchedule`。

- [ ] 由 scheduler-owned outcome worker 执行
- [ ] 使用 Alpaca broker calendar 计算交易日，不手工假设日期
- [ ] 获取真实 T+1 日线/账户/执行相关数据
- [ ] 创建 RunScoped partial Outcome/Retrospective
- [ ] 写入 durable defer，等待 T+3/T+5
- [ ] 不创建 sealed Outcome、Experience 或 Evaluation
- [ ] 验证 task/attempt、event cursor、Artifact kinds、source_refs closure
- [ ] Doctor、Replay、backup、SQLite export 通过
- [ ] 重跑验证幂等，不重复创建 canonical 产物

如果新 Run 在 2026-08-18 成交，T+1 通常不早于 2026-08-19；最终以 Alpaca calendar 为准。

## Wave D — 真实 T+3 Outcome

- [ ] 复用 Wave C 的同一 Store、Run、workflow plan、task IDs 和 execution lineage
- [ ] 获取真实 T+3 observation
- [ ] 创建 T+3 partial Outcome/Retrospective
- [ ] 引用并保留 T+1 Retrospective/source_refs
- [ ] durable defer 到 T+5
- [ ] 验证 Doctor、Replay、closure、backup、SQLite export 和幂等重跑

如果新 Run 在 2026-08-18 成交，T+3 通常不早于 2026-08-21；最终以 Alpaca calendar 为准。

## Wave E — 真实 T+5 Outcome

- [ ] 获取第五个真实后续交易日数据
- [ ] 校验 T+1/T+3/T+5 observation 的完整性
- [ ] 校验 Decision、ExecutionContext、Commitment、Receipt、Reconciliation lineage
- [ ] 创建 sealed canonical Outcome
- [ ] 创建 complete T+5 Retrospective
- [ ] worker committed，不能继续 durable defer
- [ ] Doctor、Replay、引用闭包、backup、SQLite export 和幂等重跑全部通过

如果新 Run 在 2026-08-18 成交，T+5 通常不早于 2026-08-25；最终以 Alpaca calendar 为准。

## Wave F — Outcome-backed Learning

前置条件：T+5 Outcome sealed，Retrospective complete，且 lineage 和引用闭包通过。

- [ ] 基于真实 Outcome 创建 `Experience`
- [ ] 创建 `Evaluation`
- [ ] 验证 learning 输入只来自 canonical Paper 和真实 outcome
- [ ] 验证 Doctor、Replay、去重和幂等
- [ ] 只有存在合法 candidate、baseline、fresh shadow pairs 和 canary gate 时才创建 `CandidatePolicy`
- [ ] 只有 promotion 条件真实满足时才创建 `PolicyTransition`
- [ ] 没有合法策略变化时，零 `PolicyTransition` 是正确结果

绝对禁止从以下来源推动 canonical learning：

- Debug
- Dry Run
- Replay
- Shadow-only 数据
- 当前预测
- mock、伪造或回填 Outcome

## Wave G — 最终人工审批

必须在所有前置 Wave 完成并有证据后单独进行。

- [ ] CQ1–CQ5 复核
- [ ] R0–R10 复核
- [ ] Wave H SQLite-only 复核
- [ ] 新 Paper Canary 复核
- [ ] T+1/T+3/T+5 复核
- [ ] Outcome-backed Learning 复核
- [ ] Doctor、Replay、export、backup/restore 和告警复核
- [ ] 无未对账 commitment
- [ ] 无未经验证的 policy promotion
- [ ] 明确记录人工批准人、时间、证据路径和批准范围
- [ ] 不将人工审批解释为开启 Live Trading；Live Trading 始终不支持

## 本轮禁止事项

- [ ] 不执行旧 Run 的 Outcome worker
- [ ] 不创建虚假 Run 来替代 scheduler-owned worker
- [ ] 不手工 POST、retry 或绕过 Paper gate
- [ ] 不修改原始 plan、plan hash、commitment、task IDs 或 client-order IDs
- [ ] 不绕过 V2Store 直接写 SQLite
- [ ] 不恢复文件 CAS、平行 JSON 状态或旁路数据库
- [ ] 不删除新的 Store、bundle 或 `config/akzio.local.toml`
- [ ] 不把 stale `summary.json` 当作最终业务状态

## 全部 Wave 完成判定

只有以下条件全部满足，才能将本计划标记为完成：

- [ ] 新 Paper Run 有最终状态和完整 execution/reconciliation 证据
- [ ] 同一 Run 完成真实 T+1、T+3、T+5
- [ ] T+5 Outcome sealed，Retrospective complete
- [ ] Experience/Evaluation 完成，或有明确的合法零结果说明
- [ ] CandidatePolicy/PolicyTransition 结果符合真实 gate（可为零）
- [ ] Doctor、Replay、export、backup/restore 和引用闭包通过
- [ ] 最终人工审批已明确记录

