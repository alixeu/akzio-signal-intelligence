# Akzio v2 最大力度重构：完整聊天背景与交接包

> 用途：将本文件与 `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md` 一起交给后续 Codex/ChatGPT 工作者。它记录了任务意图、已验证事实、外部协作状态、基线和未验证风险；不应把它当作代码已经实施的证明。

## 1. 用户目标（不可改变）

用户要求的是 **最大力度重构**，项目尚未上线、没有生产用户、真实资金、历史兼容或稳定性包袱。允许大规模重写 Rust workspace、删旧代码/旧类型/旧 CLI/旧 Store、重做 Phase 0–8 内部结构、Agent Contract、Prompt、Context、Memory、FileStore/Session/Event、Daemon、Planner 和 Paper execution；明确不要求兼容 v1 run、旧 API、旧文件格式、旧 Prompt、旧 Agent 数量或旧 outputs。

目标不是增加更多 Agent，而是获得可解释、可学习、可持续运行、安全执行的 Multi-Agent Research System。

不变量：

- Rust 是 state、authorization、contract、budget、workflow gate、persistence、learning transition、execution policy 的唯一权威。
- v2-only：不读取、迁移或兼容旧 `orchestrator-*`、Phase 0–8、FileStore、Prompt 或旧 `outputs/store`。
- assets 只能是 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。
- Live Trading 本阶段不支持；`AlpacaPaper::new` 必须在任何 HTTP I/O 前拒绝非 Paper endpoint。
- Debug、Replay、Paper Dry Run 非 canonical，不能提升 Memory 或 Topology。
- `V2Store` 是唯一持久化 authority；Agent 仅可经 `akzio-context` 读取获授文档。
- Paper 由 scheduler 拥有：一个 broker session 一次 durable slot；不允许 direct CLI/API Paper submit/retry。
- 不允许 Git commit、push、PR、deploy、生产配置修改、真实资金或真实用户数据操作。

## 2. 参考文章与已经吸收的启发

用户最初要求阅读并映射八篇 OpenAI 文章：Harness engineering、Inside OpenAI’s in-house data agent、Unrolling the Codex agent loop、Building self-improving tax agents with Codex、Symphony、The next evolution of the Agents SDK、Responses API computer environment、Responses API WebSockets。

在本计划中，它们的作用已收敛为：

| 启发 | Akzio 采用方式 |
| --- | --- |
| Harness | Contract Catalogue、职责/工具/预算/终止条件与 failure memory。 |
| Context engineering | Raw/Normalized/Detail/Claim/Decision/Memory 分层；Manifest/Grant/closure。 |
| Agent loop | Agent、Task、Workflow、Tool、Context、Eval、Execution Runtime 分离。 |
| 自我改进 | immutable Experience、sealed Outcome、Shadow、有限状态 promotion/rollback。 |
| 动态编排 | Planner proposal 在 Rust gate 中 lower 为 DAG；不再固定 Phase。 |
| Runtime/event | durable task/event/lease/recovery/control plane。 |
| computer environment | Agent 不直连环境；Evidence adapter、sandbox/permission 完全由 Rust 控制。 |
| streaming | loopback HTTP + SSE event replay；不是把 WebSocket 当作业务真相。 |

## 3. 当前 Git / 工作树基线

- 目录：`/Users/alixeu/project/akzio-signal-intelligence`
- 分支：`master`
- 当前 HEAD：`24e512e2f0c09b54bebfa04480f95cd27c0675b3`。
- 2026 年 8 月 7 日工作期间，branch 在本 agent 未执行 commit 的情况下从 `fa6986cb534b428ddb7e3be7415aa849d977d7b1` 前进到 `24e512e`。`24e512e` 将此前 dirty 的五个 `rebuild.rs` 原型、五个导出改动和两份原始审计/计划文档正式纳入了 tree。
- 本 agent 没有执行 Git commit、push、reset、stash、checkout 或修改 Rust 源码。

在上传 ZIP 时（HEAD 仍标为 `fa6986c`）已存在的 dirty work 如下；它们后来被外部提交 `24e512e` 纳入版本控制，当前不再是未提交修改：

```text
M  crates/akzio-context/src/lib.rs
M  crates/akzio-domain/src/lib.rs
M  crates/akzio-research/src/lib.rs
M  crates/akzio-runtime/src/lib.rs
M  crates/akzio-store/src/lib.rs
?? crates/akzio-context/src/rebuild.rs
?? crates/akzio-domain/src/rebuild.rs
?? crates/akzio-research/src/rebuild.rs
?? crates/akzio-runtime/src/rebuild.rs
?? crates/akzio-store/src/rebuild.rs
?? docs/architecture/2026-08-06-v2-rebuild-audit.md
?? docs/architecture/AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN.md
```

这些 `rebuild.rs` 是已提交但未接线原型，不能当作 active runtime 或已验收实现。当前未提交修改只应是此次新增的两份交接文档：

- `docs/architecture/AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md`
- `docs/architecture/AKZIO_V2_REFACTOR_HANDOFF.md`

## 4. 已读项目约束与布局

已读取根 `AGENTS.md`、`README.md`、根 `Cargo.toml`、`config/akzio.toml` 和现有 `docs/architecture/*`。未发现根 `CLAUDE.md` 或 `package.json`；这是 Rust 2021 workspace，不应假设存在 Node build 流程。

workspace crates：`akzio-domain`, `akzio-store`, `akzio-context`, `akzio-ingest`, `akzio-runtime`, `akzio-execution`, `akzio-learning`, `akzio-model`, `akzio-research`, `akzio-daemon`, `akzio-cli`。

AGENTS 要求未来代码改动至少执行：

```bash
rtk cargo fmt --all
rtk cargo check --workspace
rtk cargo clippy --workspace --all-targets
rtk cargo test --workspace
rtk cargo run -p akzio-cli -- run fixture-debug
rtk cargo run -p akzio-cli -- store doctor
```

## 5. 真实源码审计结论

已核对的核心路径：

1. `AgentRole` 在 `crates/akzio-research/src/lib.rs:35` 是硬编码闭集；现有 contract registry 仍以 role 为中心。
2. `WorkflowCompiler` 在 `crates/akzio-runtime/src/lib.rs:281`；Planner patch 只能有限增删研究任务，终止生命周期仍固定。
3. `WorkflowRuntime::submit` 在 `crates/akzio-runtime/src/lib.rs:372` 做多次 Store 写入，因此 run/plan/task/dependency/event 间存在 crash window。
4. `V2Store::register_document` 在 `crates/akzio-store/src/lib.rs:260` 不要求 task attempt/lease/epoch/contract permit；`DocumentRecord::validate` 只校验 envelope，未做 kind payload/source closure 语义校验。
5. `ContextBroker::record_json*` 能创建文档；research path 会从 `documents_for_run` 收集允许文档，导致 run-wide context expansion。
6. `akzio-daemon` 同时有 HTTP/SSE（`serve_http`）与 Unix JSON-line（`serve_unix`）；`akzio-cli` 仍使用 `UnixStream`。
7. scheduler 已有 lease、epoch、session slot 的初步实现，但 current path 还不是全程原子/permit/fencing model；不应只做 patch。
8. current execution 有 Paper adapter、reconcile 和 `DecisionGatePolicy`，但 typed blocker/freshness/conflict/exposure/commitment authority 没有作为一个完整不可绕过输入闭环。
9. current learning/topology 已有 canary/Shadow 概念，但 canonicality、Experience identity、sealed outcome 和 Dry Run 隔离未统一到单一 Rust policy。

历史 `exec/mod.rs` 属于旧 v1 文档/遗留资料；当前 active v2 tree 不存在该文件。不要为了“重构 exec/mod.rs”而重新创建它。

## 6. 已采用的架构默认决定

此前需要用户确认的五个关键分叉尚未收到单独回答；由于用户已授权普通实现选择自主决定，续篇计划默认采用：

1. governed multi-source evidence：Alpaca + SEC EDGAR + FRED + configured News/Web Adapter；Agent 无 raw HTTP。
2. 每 broker session 一个 Paper commitment。
3. risk-first lexicographic objective，T+1/T+3/T+5 Paper outcome。
4. 自动仅能调 Prompt/budget/topology；不得扩权 source/tool/execution。
5. Rust auto-freeze；仅 loopback operator HTTP/CLI 能 unfreeze。

若未来产品要改动这些决定，应创建 ADR，而不是在 Prompt 或 daemon dispatch 中例外处理。

## 7. ChatGPT Pro 协作记录

用户已授权使用 Codex 内置浏览器、与登录的 ChatGPT Pro 通讯、上传脱敏源码 ZIP，并要求 Codex 独立验收外部结论。

### 7.1 对话链接

- [架构审查对话](https://chatgpt.com/c/6a7476f8-b2f4-83ee-b6a0-97e67284de53)
- [Daemon / Paper 审查对话](https://chatgpt.com/c/6a74775d-8d58-83ee-aeb6-72d0299b3aa1)

### 7.2 已向 ChatGPT Pro 提交的最新版包

```text
commit label at archive time: fa6986cb534b428ddb7e3be7415aa849d977d7b1 plus the then-uncommitted v2 prototype/docs listed above
archive: /tmp/akzio-v2-pro-review-fa6986c-20260807T024036Z-clean.zip
size: 289,836 bytes
entries: 98
SHA-256: d1ba2cd9a40e4ac6f52b8f02f94501bbd2d9453c5d6e39f04b8c2e84bccd8353
```

打包排除 `.git`、`target`、`outputs`、`node_modules`、缓存、database/runtime/socket/log、`.env`、常见私钥/凭据文件；使用 `gitleaks` 不可用时的模式扫描，结果为 clean。该扫描不是完整安全审计。ZIP 是工作树快照，不能独立证明其文件树等于 Git commit；它与稍后出现的 `24e512e` 有相同的原型/文档路径集合，但仍应通过内容 hash 而不是该推断认定身份。

ChatGPT Pro 获得的任务明确要求：不写补丁、不运行 Cargo、不连网络/broker/LLM、不把 rebuild 原型当作接线能力；输出必须区分源码已验证、合理推断、未验证。

### 7.3 外部结果状态

较早的两条外部对话已产出静态审查和 test matrix；其中明确声明未验证 Cargo、SQLite crash durability、真实并发、broker/LLM HTTP、CLI/UI 或 Git object identity。

2026-08-07 已将最新 ZIP 与“续审”任务提交给两条对话。它们产生了以下中间发现：

- rebuild 原型虽由部分 crate 导出，但 daemon/CLI/execution 没有已验证调用点；可编译性未证明。
- current production chain 仍有 direct Paper path，执行 snapshot 不是权威输入；prototype 未覆盖 lease/slot 治理；Experience identity/attribution 需重做。
- current code 同时保留 HTTP 与 Unix 控制面；workflow 建图/失败收尾不原子；epoch 未约束 broker commitment；PaperDryRun 仍走真实 ingest；Eval 不能只按 Paper purpose 判定 canonical。

外部 UI 在输出中间调查项后多次无增量而仍显示生成。已按用户指定的恢复规则停止并要求它们直接成稿；截至本交接文件生成时，没有新的完整续审报告。后续工作者可以打开上述链接继续，但不得把未成稿内容当作最终权威。所有结论仍需要以本地源码和测试验收。

## 8. 当前验证状态与风险

- 先前已观察 `cargo test -p akzio-research` 通过 6 个测试，`akzio-cli store doctor` 返回 `ok: true`；这是历史局部证据，后续应在重构树重跑。
- 本轮尝试完整 `cargo test --workspace` 时，发现工作区已有多条长期卡在 `akzio_daemon` test binary 的 cargo/test process。为避免干扰，只停止了本轮自身启动的重复测试；没有杀死其他并行进程。
- 因此：**不得声称当前 workspace 全量测试已通过**。应先隔离并诊断 daemon test hang，再把它纳入 R10 的 regression/crash harness。
- 本轮未改 Rust 代码、未执行 Git commit/push/PR/deploy、未访问真实 broker、未执行 Live 或真实资金操作。

## 9. 后续执行协议

在用户确认 `AKZIO_V2_MAX_REFACTOR_EXECUTION_PLAN_CONTINUATION.md` 后：

1. 创建实现前基线与隔离 Store Root；保留现有 dirty work，不 reset/stash/checkout。
2. R0-R2 先行，只有 domain/store interface 稳定后才并行 R3-R7。
3. 每个 R 阶段完成立即运行本 crate 测试 + `cargo fmt/check`，不等到末尾。
4. 任何外部建议都需要代码定位、接口审查、锁文件/依赖审查和测试证据后才采纳。
5. 发生计划错误时更新本文与续篇计划，优先修正 architecture，不为最小 diff 保留错误抽象。
6. 完成 R10 后才可运行完整终验命令；fixture、Dry Run、actual Paper 明确分开报告。
