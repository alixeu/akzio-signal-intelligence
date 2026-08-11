# Akzio v2 Test Matrix

状态：R0 verified。所有命令仅使用本地 workspace 与已缓存依赖；fixture、Dry Run 和静态检查不得描述为真实 broker、市场或模型验证。

## 阶段 gate

| 阶段 | 必测行为 | owner | 退出证据 |
| --- | --- | --- | --- |
| R0 | workspace metadata、当前 v2 Store/Runtime 窄基线、legacy inventory | workspace / store / runtime | 基线命令成功；每条 invariant、删除项均有 owner；全 workspace/CLI 留给相应阶段重新认证 |
| R1 | canonical hash、serde、asset 闭集、生命周期/provenance、graph/budget | `akzio-domain` | domain 窄测 + workspace check |
| R2 | CAS hash、atomic workflow commit、TaskWritePermit、failure injection、lease/epoch、Doctor | `akzio-store` | store 窄测 + stale leader cannot mutate |
| R3 | Raw/Normalized/Detail、manifest closure、grant scope/expiry、repair、evidence integrity | `akzio-context` / `akzio-ingest` | context/evidence 窄测 |
| R4 | Contract canonical hash、catalogue install、schema failure、tool/grant denial、turn retry | `akzio-research` | AgentRuntime 窄测 |
| R5 | proposal lowering、mandatory terminal gates、DAG/retry/cancel/recovery/replay | `akzio-runtime` | runtime 窄测 |
| R6 | sealed outcome、memory transition、shadow idempotency、canary promotion/rollback | `akzio-learning` | learning 窄测 |
| R7 | policy gates、Paper URL fail-closed、idempotency/reconcile、Dry Run noncanonical | `akzio-execution` | execution 窄测 |
| R8 | process crash/recovery、multi-run、single-run lease、epoch fence、freeze, HTTP/SSE | `akzio-daemon` | daemon integration harness |
| R9 | loopback config validation、HTTP-only CLI、old Store Root rejection、no direct Paper submit/retry | `akzio-cli` | CLI/config tests and help inventory |
| R10 | end-to-end fixture/replay/Doctor, all invariant closure, dead-code/legacy zero inventory | workspace | final command matrix below |

## Required security and durability cases

| Case | First phase | Must prove |
| --- | --- | --- |
| Evidence reference integrity | R3 | unknown kind, invalid source closure and removed blob are rejected by Store/Doctor |
| Context grant overreach | R3 | wrong attempt, expired grant, out-of-manifest and raw access fail closed |
| Atomic write failure | R2 | no partial task completion/event/artifact graph after injected failure |
| Epoch fencing | R2 / R8 | stale leader cannot reserve, mark, submit or overwrite slot |
| Scheduler singleton | R0 / R8 | one session gives one durable plan/run identity despite retry/takeover |
| Canonical learning boundary | R6 | Debug, Replay, Shadow and Paper Dry Run cannot promote state |
| Shadow pair idempotency | R6 | repeated completion including identical timestamps has one outcome |
| Paper endpoint boundary | R7 | malformed, live or lookalike URL fails before transport |
| Automatic Paper Dry Run | R7 | policy path produces noncanonical dry-run record only |
| Daemon recovery/concurrency | R8 | crash recovery reuses frozen plan/task IDs and respects lease ownership |

## R0 executable baseline

Run from the workspace root with no network access:

```bash
cargo metadata --offline --format-version 1 --no-deps
cargo test --offline -p akzio-store workflow_commit_accepts_out_of_order_nodes_and_preserves_dependencies
cargo test --offline -p akzio-runtime planner_graph_gets_non_bypassable_terminal_gates
git diff --check
```

历史 R0 基线中的 daemon scheduler 测试命令已不再是当前 Daemon 的可运行入口，故不再用于通过声明。当前 `store_v2::tests::session_slot_is_fenced_and_reuses_the_frozen_workflow` 属于 R2/R8 回归项；它已在 2026-08-11 refresh 中通过，但不构成 R0 或 R8 的完整验收。全 workspace、`fixture-debug` 与 `store doctor` 仍由 R7/R8 修复后在 R10 fresh Store Root 下实际运行。临时 Store Roots 始终只构成本地 fixture 证据。

## Final offline acceptance

After R10, the actual terminal evidence must be:

```bash
cargo fmt --all -- --check
cargo check --workspace --offline
cargo clippy --workspace --all-targets --offline
cargo test --workspace --offline
cargo run -p akzio-cli --offline -- run fixture-debug
cargo run -p akzio-cli --offline -- store doctor
```

In addition, R10 must retain repeatable harnesses for the ten required security/durability cases above and a static inventory showing no active v1/Phase/FileStore/old Store Root/Unix business protocol/direct Paper submission compatibility path.
