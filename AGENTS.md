# Akzio v2 项目规则

## 项目定位

- 本仓库是 Rust 2021 workspace，构建本地、仅支持 Alpaca Paper 的多智能体研究系统。
- 项目只维护 v2；不要恢复旧 `orchestrator-*` crates、Phase 0–8、FileStore、旧 prompts 或 `outputs/store` 兼容路径。
- Live Trading 不受支持；`AlpacaPaper::new` 必须在任何 HTTP I/O 前拒绝非 Paper endpoint。

## 权威与数据边界

- Rust 是状态、授权、contracts、任务预算、workflow gates、持久化、学习迁移和执行策略的唯一权威。
- `V2Store` 是唯一持久化权威。不要增加改变语义的并行 JSON 状态、缓存，或在 `akzio-store` 之外直接写 SQLite。
- Evidence、Claim、Decision、Execution 和 Memory 必须保留 provenance 与有效 `source_refs`。
- `akzio-context` 是 agent task 获取文档的唯一通道；模型代码不得获得任意文件系统、raw evidence、SQLite 或交易凭据访问权。
- 生成的 Store Root、BLOB、socket、报告、凭据和本地配置覆盖不得进入 Git。

## 模块边界

| Crate | 职责 |
| --- | --- |
| `akzio-domain` | 稳定 schema 与验证；无 I/O |
| `akzio-store` | SQLite-embedded CAS BLOB、事件日志、task/daemon lease、Doctor |
| `akzio-context` | 文档、manifest 与受控上下文访问 |
| `akzio-runtime` | workflow 编译、planner patch、task 生命周期与恢复 |
| `akzio-research` | agent contracts 与 model-mediated research |
| `akzio-execution` | Rust 决策/执行 gates 与 Paper broker protocol |
| `akzio-learning` | outcome evaluation 与有界 policy state transition |
| `akzio-daemon` | 进程领导权、调度、transport 与 task dispatch |

不要把 policy 或 durable invariant 堆进 `akzio-daemon` dispatch：policy 放在所属 domain/runtime crate，持久化不变量同时由 `akzio-store` 强制执行。

## Paper 调度与执行边界

- Paper run 由 scheduler 独占管理；不得恢复直接 CLI/API Paper submit 或 retry 路径。
- scheduler 使用 Alpaca Paper market clock；每个 broker session date 最多创建一个 durable session slot。
- session slot 必须在创建 run 前保存完整 workflow plan；恢复时复用该 plan 及其 task IDs。
- 所有 scheduler 写入必须校验当前 daemon lease owner 与 epoch；stale leader 不得提交、标记或覆盖 slot。
- broker submission 前必须由 Rust 校验 account、quotes、allocation、turnover、blockers、plan hash 与 idempotency。

## 学习与拓扑边界

- canonical learning 只能来自 sealed Paper outcomes。Debug、Replay、Paper Dry Run、当前预测和未封存市场数据均不得提升 Memory 或 Topology。
- Memory 与 Topology 是不可变文档历史，不得原地修改旧记录。
- shadow pair 必须引用 parent Decision、ExecutionContext 与 candidate Decision；即使 timestamp 冲突，完成操作也必须保持幂等。
- promotion 在每个 canary level 都需要 fresh paired outcomes；risk recall 或 evidence completeness 下降时回滚 candidate。
- 真实 Paper、T+1/T+3/T+5 outcome、learning transition 与最终人工批准是不同证据层级，不得相互替代。

## 验证与完成标准

- 代码修改先运行最窄的相关测试；宣称 workspace 级完成前运行：

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace
cargo run -p akzio-cli -- run fixture-debug
cargo run -p akzio-cli -- store doctor
```

- refactor 或 storage change 必须先证明行为等价；不要顺带改变 schema version、`ExecutionPlan` serde/hash、Paper gate、transaction boundary 或 learning policy。
- 交付时分别报告：implemented、offline-verified、real-Paper-verified、outcome/learning-verified。

## Observatory App

- 需要生成可分发 macOS App 时使用 `scripts/update_app_and_submit_debug.sh`。
- 该脚本只负责构建、签名、清理构建中间物并保留 `apps/AkzioMac/dist/Akzio Observatory.app`；它不会启动 App、daemon 或 run，也不构成运行时、Paper 或 learning 验证。
- App bundle 必须包含 `Contents/MacOS/akzio-core`。启动时 `RustCoreSupervisor` 以 `daemon serve` 运行该 core。
- 持久配置来自 `~/.akzio/config.toml`，core Store root 通过 `AKZIO_STORE_ROOT` 固定为 `~/.akzio/store`。
- `Paper scheduler waiting: broker market is closed` 表示 scheduler 正在等待可用 Paper session，不等同于 `Rust core unavailable`。
