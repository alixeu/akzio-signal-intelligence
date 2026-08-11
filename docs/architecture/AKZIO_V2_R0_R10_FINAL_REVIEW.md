# Akzio v2 R0–R10 Final Offline Review

日期：2026-08-11

结论：R0–R10 的当前源码、删除边界和离线验收均已通过。结论仅覆盖本地 fixture、fake broker/model 与静态源码；不代表真实市场、模型、Alpaca Paper 或生产验证。

## Final command evidence

在不联网的当前 tree 上实际完成：

```text
cargo fmt --all -- --check                              passed
cargo check --offline --workspace                       passed
cargo clippy --offline --workspace --all-targets -D warnings  passed
cargo test --offline --workspace                        181 passed, 22 suites
cargo run --offline -p akzio-cli -- --config <fresh> run fixture-debug  passed
cargo run --offline -p akzio-cli -- --config <fresh> store doctor       {"ok":true}
```

最终 fixture 使用新建的临时 Store Root；没有读取、迁移或修改旧 Store Root，也没有访问网络、broker 或模型服务。

## R0–R10 review

| Phase | Final review result | Primary local evidence |
| --- | --- | --- |
| R0 | 目标不变量、测试矩阵与删除图冻结 | `AKZIO_V2_INVARIANTS.md`、`AKZIO_V2_TEST_MATRIX.md`、更新后的 deletion graph |
| R1 | 四资产闭集、canonical schema/hash、artifact provenance/source closure 与 permit vocabulary 落地 | `akzio-domain` tests and workspace suite |
| R2 | `V2Store` 是唯一 CAS/SQLite/event/lease authority；workflow、attempt、artifact/event、slot/policy writes 均经事务 | Store atomicity, lease/epoch, session-slot and Doctor tests |
| R3 | Evidence Raw/Normalized/Detail 经过 broker；Agent read 受 manifest/grant/expiry/closure 限制 | Context grant, repair, raw-read and ingest provenance tests |
| R4 | Contract catalogue、capability ceiling、schema-validated Agent turn 与 retry 均由 Rust 定义 | Research catalogue/authority/turn tests |
| R5 | Planner 仅提出 proposal；runtime 注入不可绕过 Evidence/Decision/Execution/Paper/Reconcile/Evaluate gates，并支持 durable replay/recovery | Runtime gate, patch, retry/cancel and replay-divergence tests |
| R6 | Canonical learning 限 sealed Paper Outcome；shadow pair/cursor/canary/promotion/rollback 不可变 | Learning noncanonical, shadow, lifecycle and rollback tests |
| R7 | Decision/Execution gates、NoOrder、plan hash、Paper endpoint、commit/reconcile 均 fail closed | Execution endpoint, commitment, freeze, idempotency and reconciliation tests |
| R8 | Scheduler lease/epoch fencing、single session slot、freeze、HTTP auth/SSE cursor 与 worker dispatch 由 daemon 协调 | Daemon stale-leader/session, worker, freeze, HTTP/SSE tests |
| R9 | CLI/config 只走 loopback HTTP/SSE；旧 Unix setting 被拒绝；CLI 不能直提 Paper/retry | CLI config/help tests and daemon direct-submit rejection tests |
| R10 | 新增只读 replay report；删除 `legacy.rs`、`rebuild.rs`、`Rebuild*` public names、dead modules/dependencies；完成 fresh-root fixture/Doctor 和全仓验证 | Source inventory, replay route test and final command evidence above |

## Static boundary review

- 活跃 Rust 源码无 `legacy.rs`、`rebuild.rs`、`Rebuild*`、`UnixStream`、`serve_unix`、`DaemonCommand`、`RunPurpose::Live`、`orchestrator`、`FileStore` 或旧 Store Root 路径。
- `unix_socket` 仅留在 CLI 的拒绝旧配置测试；旧 SQLite 文件名仅留在 Store 的 fail-closed incompatibility fixture。
- `rusqlite` 不在 `akzio-store` 外使用；`akzio-research` 和 `akzio-context` 未直接使用 filesystem、raw HTTP 或 socket APIs。
- 该树没有 `cargo-deny` 或 workspace deny policy；没有安装或联网获取额外审计工具。

## Plan precedence correction

本地 `PLAN.md` 仍含 TQQQ-only、QQQ/SOXX 仅研究和 Unix transport 的历史建议。它与当前 `AGENTS.md`、当前源码/测试和计划-续冲突，故没有采纳。最终边界是四资产 `TQQQ`、`QQQ`、`SOXX`、`SOXL` 与唯一 loopback HTTP/SSE 控制面。

## Residual verification boundary

Crash/recovery 由 expired-attempt recovery 与 stale-leader lease/epoch takeover 的本地 fixture 覆盖；它不是操作系统级进程杀死或真实 broker 故障演练。所有 Paper 行为仍仅是 fake/fixture 或 noncanonical Dry Run，不能提升为真实 Paper 订单验证。
