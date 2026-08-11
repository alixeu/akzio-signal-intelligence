# Akzio v2 Paper 运行手册

本手册只覆盖当前 v2 的 Paper-only 路径。Live Trading、旧 orchestrator、旧 Store Root、旧 outputs、Unix JSON-line fallback 和旧 Prompt 兼容层均不在恢复范围内。

## 运行边界

- 可执行资产严格为 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。
- Rust 负责状态、权限、Contract、预算、Workflow Gate、学习迁移和执行策略。
- `V2Store` 是唯一耐久状态权威；Agent 只能通过 `ContextManifest` 与 task/attempt-bound `ReadGrant` 读取资料。
- `daemon.auto_paper=true` 只允许通过 Alpaca Paper market clock、`StorePaperWorkflowSource`、scheduler-owned snapshots 和 `CommittedPaperBroker`。
- Alpaca 凭据只从 `ALPACA_API_KEY`、`ALPACA_API_SECRET` 环境注入；`ALPACA_PAPER_BASE_URL` 为空或必须为 `https://paper-api.alpaca.markets`。
- `auto_paper=true` 时必须配置经过审批的非零 `transaction_cost_ppm` 或 `slippage_ppm`；零成本只可用于 fixture/离线验证。

## 启动前检查

```bash
cargo run --offline -p akzio-cli -- store doctor
cargo run --offline -p akzio-cli -- store metrics
cargo run --offline -p akzio-cli -- store alerts
cargo run --offline -p akzio-cli -- run fixture-debug
```

检查结果必须明确区分 fixture/离线证据与 Paper sandbox 证据。`fixture-debug` 成功只能证明当前 Store、权限、契约和工作流边界，不证明 Alpaca 或真实市场可用。

当前 fixture workflow 可能在 evidence gate 进入 `Failed`；`run paper-dry-run` 的验收条件是 `canonical_learning_events == 0`，不能把 `status: Failed` 或命令返回当成 Paper 成功。当前 Store 只持久化 `task.failed`，具体失败原因若未在 artifact/event 中出现，保持“待验证”。

## 日常控制

```bash
cargo run --offline -p akzio-cli -- daemon health
cargo run --offline -p akzio-cli -- daemon freeze "operator reason"
cargo run --offline -p akzio-cli -- daemon unfreeze "approved recovery"
cargo run --offline -p akzio-cli -- run replay <run-id>
cargo run --offline -p akzio-cli -- run events <run-id>
```

冻结期间不得产生新的 Paper commitment。解除冻结前必须完成 Store Doctor、replay、告警处置和人工审批。

## 证据等级

| 证据等级 | 可证明 | 不可证明 |
| --- | --- | --- |
| fixture/mock | schema、hash、权限、幂等、离线故障状态机 | Alpaca、真实市场、生产成功 |
| 离线测试 | Rust gate、Store 事务、Replay、质量门、学习边界 | provider 可用性、市场时钟正确性 |
| Paper sandbox | Paper endpoint、market clock、client-order 幂等、回执、reconciliation、重启恢复 | Live Trading、真实资本风险 |
| 真实市场/生产 | 仅在 Paper sandbox 与人工审批均完成后，证明受控 Paper 运行 | 不得由 fixture、Dry Run 或 Replay 推断 |

## 故障处理

### 进程强杀与恢复

1. 记录退出时间、`daemon health`、最近的 run/task/attempt event cursor。
2. 使用同一 `Store Root` 重启，不删除 SQLite 或 CAS。
3. 检查 stale attempt recovery、scheduler lease epoch、Paper session slot 和原冻结 workflow plan。
4. 已存在的 commitment 只能走 scheduler-owned reconciliation；不得手工重建 plan 或 client order id。
5. 在解除冻结前运行 `store doctor`、`run replay`，并完成 Paper sandbox 对账。

### 网络分区、超时和重复回执

- Evidence provider 超时、空响应、来源越权、引用缺失或 schema 不可验证：fail-closed，禁止用猜测数据填补。
- Paper broker 超时：保留 durable commitment，等待 scheduler-owned reconciliation；禁止直接 retry submit。
- 重复回执：按 plan hash、client order id、broker order id 对账，不创建第二份 commitment。
- lease epoch 变化：旧 owner 的写入、mark 和 broker commit 全部停止，进入 recovery。

## Outcome worker

`OutcomeSchedule` 只在 Paper terminal chain 完成后由 scheduler-owned worker 处理。worker 必须同时得到：

- Paper/Alpaca 未来日 bars；
- baseline quote snapshot；
- 对齐的 T+1、T+3、T+5 交易日；
- 原始 URI、dedupe key、provenance 和可验证引用的 Evidence。

不足五个共同交易日、baseline quote 缺失、bar 重复/缺失、公司行动调整无法确认或证据质量不足时，worker 只重试或保持 NoOrder 边界，不封存 canonical Outcome，不触发 canonical learning。

## 备份与恢复

```bash
cargo run --offline -p akzio-cli -- store backup <new-backup-root>
cargo run --offline -p akzio-cli -- store restore <backup-root> <new-store-root>
cargo run --offline -p akzio-cli -- store doctor
cargo run --offline -p akzio-cli -- run replay <run-id>
```

备份使用 SQLite 一致性快照并复制 CAS。恢复目标必须不存在且不能位于活动 Store Root 内；恢复后自动运行 Store Doctor，运维仍需核对 manifest hash/count/version 与 Paper commitment 一致性。

## 离线故障演练

```text
cargo run --offline -p akzio-cli -- test crash-recovery
cargo run --offline -p akzio-cli -- test concurrent-runs
cargo run --offline -p akzio-cli -- test evidence-integrity
cargo run --offline -p akzio-cli -- test learning-transitions
cargo run --offline -p akzio-cli -- test frozen-evidence
cargo run --offline -p akzio-cli -- test store-corruption
cargo run --offline -p akzio-cli -- test freeze-recovery
cargo run --offline -p akzio-cli -- test lease-takeover
```

这些命令输出会标记 `fixture: true` 或 `evidence: offline/...`，不能替代真实 Alpaca Paper、真实市场数据、网络分区或生产故障演练记录。

## Paper 试运行审批门

只有以下条件全部满足，才可宣布 Paper 生产试运行：

1. `cargo fmt --all -- --check`、`cargo check --offline --workspace`、`cargo clippy --offline --workspace --all-targets -- -D warnings` 和 `cargo test --offline --workspace` 全部通过。
2. fresh Store Root 的 `fixture-debug`、`store doctor`、metrics、alerts、replay 和离线故障演练全部通过。
3. 独立 Paper sandbox 完成 market clock、account/quote、一次性 durable commitment、client-order 幂等、broker reconciliation、freeze/unfreeze 和重启恢复，并由人工留存记录。
4. 真实 Paper Outcome worker 完成 T+1/T+3/T+5 sealing，成本/滑点配置经过审批，且无未验证 candidate promotion。
5. 无 Hard Blocker、长期冻结、Store Doctor 失败、过期快照、未对账 commitment 或 provider 证据质量告警。

在第 3、4 项尚无真实人工证据前，只能宣布“离线实现与验证完成”，不能宣布 Paper 生产就绪。
