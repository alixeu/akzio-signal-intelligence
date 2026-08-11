# Akzio v2 Paper 运行手册

本文只覆盖 Rust/V2Store 受控的 Paper-only 运行。Live Trading、旧 orchestrator、旧 Store Root、旧 outputs 和 Unix JSON-line fallback 均不属于恢复路径。

## 启动前审批

- 只允许 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。
- `ALPACA_PAPER_BASE_URL` 必须为空或 `https://paper-api.alpaca.markets`。
- `ALPACA_API_KEY`、`ALPACA_API_SECRET` 只从进程环境注入，不写配置文件、不写 Store。
- `daemon.auto_paper=true` 时必须使用 broker market clock、`StorePaperWorkflowSource` 和 scheduler-owned broker。
- `cargo run -p akzio-cli -- store doctor` 必须返回 `{"ok":true}`。
- `health.metrics` 中不能存在遗留 `running`/`leased` attempt；冻结状态必须由人工确认。

## 证据等级

| 结果 | 可证明内容 | 不可证明内容 |
| --- | --- | --- |
| fixture/mock | schema、hash、权限、幂等、故障状态机 | Alpaca、真实市场、生产成功 |
| 离线测试 | Rust gate、Store 事务、Replay、质量门 | provider 可用性、市场时钟正确性 |
| Paper sandbox | Paper endpoint、client-order 幂等、回执和 reconciliation | 真实资本、Live 交易 |
| 真实市场/生产 | 仅由 Paper sandbox 与人工审批共同确认 | 不得由 fixture 或 Dry Run 推断 |

## 故障处理

### 冻结

```bash
cargo run -p akzio-cli -- daemon freeze "operator reason"
cargo run -p akzio-cli -- health
cargo run -p akzio-cli -- store doctor
```

冻结后不得产生新的 Paper commitment。调查完成并取得人工批准后：

```bash
cargo run -p akzio-cli -- daemon unfreeze "approved recovery"
```

### 强杀恢复

1. 记录进程退出时间、`health` JSON、最近的 run/task/attempt event cursor。
2. 重新启动同一 Store Root；不要删除 SQLite 或 CAS blob。
3. 检查 stale attempt recovery、scheduler lease epoch 和 Paper session slot。
4. 对已存在 commitment 只执行 reconciliation；禁止人工重建 plan 或 client order id。
5. `store doctor`、`replay` 和人工 Paper sandbox 对账全部通过后才可解除冻结。

### 网络分区、超时和重复回执

- Evidence provider 超时、空响应、来源越权或引用缺失：保持任务失败关闭，不填补猜测数据。
- Paper broker 超时：保留 durable commitment，等待 scheduler-owned reconciliation；不得直接 retry submit。
- 重复回执：按 `plan_hash`、`client_order_id` 和 broker order id 对账；不创建第二份 commitment。
- lease epoch 变化：旧 owner 的写入、mark、broker commit 全部停止并转入 recovery。

## Outcome worker

`OutcomeSchedule` 在 Paper terminal chain 完成时与 scheduler-owned learning task 同一事务入库。worker 只接受：

- broker/Alpaca Paper 未来日 bars；
- baseline quote snapshot；
- 对齐的 T+1、T+3、T+5 交易日；
- 具备 provenance、原始 URI、dedupe key 和 normalized payload 的 Evidence。

不足五个共同交易日、baseline quote 缺失、bar 重复/缺失或证据质量不足时，worker 保持重试/NoOrder 边界，不封存 canonical Outcome。

## 发布门

只有以下条件全部满足，才可以宣布 Paper 生产试运行：

- workspace fmt/check/clippy/test 全部通过；
- fresh Store Root 的 `fixture-debug` 和 `store doctor` 通过；
- 独立 Paper sandbox 完成 market clock、一次性 commitment、client-order 幂等、reconciliation、freeze/unfreeze、重启恢复；
- 至少一轮 T+1/T+3/T+5 Outcome worker 由真实 Paper bars 完成并通过 EvaluationRuntime；
- 强杀、Store 损坏、lease takeover、网络分区和重复回执演练有人工记录；
- 仍无 Live Trading 声明、无真实资本成功声明。

## P2 本地运维命令

以下命令只操作当前配置指向的 V2Store；`AKZIO_STORE_ROOT` 可用于把演练隔离到临时 Store Root：

```text
cargo run --offline -p akzio-cli -- store metrics
cargo run --offline -p akzio-cli -- store alerts
cargo run --offline -p akzio-cli -- store backup <new-backup-root>
cargo run --offline -p akzio-cli -- store restore <backup-root> <new-store-root>
```

`store backup` 使用 SQLite 一致性快照并复制 CAS；目标目录必须不存在且不能位于活动 Store Root 内。`store restore` 拒绝覆盖既有目录，并在返回前自动运行 Store Doctor。

离线故障演练命令：

```text
cargo run --offline -p akzio-cli -- test crash-recovery
cargo run --offline -p akzio-cli -- test store-corruption
cargo run --offline -p akzio-cli -- test freeze-recovery
cargo run --offline -p akzio-cli -- test lease-takeover
```

这些命令的输出均标记 `fixture: true` 或 `evidence: offline/...`，不能替代真实 Alpaca Paper、真实市场数据或生产故障演练记录。
