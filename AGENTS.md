# Akzio 开发协作指南

本地常驻、仅支持 Alpaca Paper 的 Rust 多智能体投研系统（`TQQQ`、`QQQ`、`SOXX`、`SOXL`；固定工具链 `Rust 1.96.0` / `2021 edition`）。模型只生成受限研究或叙事，不拥有状态、资金、工具或执行权限。

## 1. 不可妥协红线

- **Paper-only**：永不实现 Live Trading；非 Paper endpoint 必须在 HTTP I/O 前拒绝。不存在本地模拟账户/时钟/成交适配器，不恢复 `-fakerOnline` / `--faker-online`。
- **Rust / Store 单一权威**：只维护 v2（不恢复 `orchestrator-*`、`FileStore`、`outputs/store`、Planner/DryRun/独立 Debug 创建）；`akzio-store` 是唯一持久化入口，研究 Agent 只能经 `akzio-context` 获取授权 projections。
- **Fail-Closed 与幂等**：缺 active `DecisionPolicy` 时正式目标全零；Paper 写入须先通过审批与全部 Gate 再持久化确定性 Commitment；`unknown_after_post` 必须先查原 Run/Session，CLI 与 HTTP 均不得自动重发。
- **日志与历史不可变**：正式私有日志位于 `~/.akzio/log/YYYYMMDD/`；不静默改写历史 CAS、Contract、Commitment 或迁移边界；Store、BLOB、socket、凭据和本地临时报告不提交 Git。

## 2. 机械化防线（优先于自然语言）

- **编译与 Clippy 硬门禁**：[Cargo.toml](Cargo.toml) `[workspace.lints]` + [clippy.toml](clippy.toml)（所有 crate 通过 `[lints] workspace = true` 继承）。禁止通过新增 `#[allow(...)]` 或 `#[ignore]` 绕过检查。
- **架构与红线测试**：[architecture_guardrails.rs](crates/akzio-daemon/tests/architecture_guardrails.rs) 在 `cargo test` 中自动校验模块依赖 DAG、`akzio-domain` 纯净性、`rusqlite` 独占 `akzio-store`、`unsafe` 隔离、Live endpoint 禁用、`#[allow]`/`#[ignore]` 棘轮、`include!` 格式棘轮与本文件体积上限（`<= 6 KB`）。
- **Agent 防乱写 Hook**：`.agents/hooks.json` 与 `.agents/loop/bin/hook_guard.py` 拦截在工作区擅自新建 `.md` 文档（`outputs/loop/{tmp,reports}/` 除外），并在循环模式隔离主工作区与 `~/.akzio`。

## 3. 协作规范与文档索引

- 默认中文，先给结果与验证证据；开始改动前保留工作区无关变更，**不得顺带全量重排历史未格式化 `include!` 文件**。仅影响业务语义、范围、授权或不可逆操作时才询问。
- 隔离循环子智能体（`.agents/agents/`）以 `[LOOP-DESIGN]` 白名单、源码和 `.agents/loop/agent-lessons.md` 为直接依据，无需通读长篇设计文档。

| 任务涉及 | 先核对权威文档 |
|---|---|
| 领域模型、存储 DDL、T0/Outcome 协议 | [运行时契约](docs/agent-runtime-contract.md)、[CONTEXT](CONTEXT.md) |
| Prompt / 预算 / 退役旧链边界 | [提示词所有权](docs/prompt-ownership.md)、[研究协议与退役清单](docs/research-protocol-retirement.md) |
| 正式日志、校准流程、离线与 CI 检查 | [开发 Workflow](docs/development-workflow.md)、[运行日志](docs/run-log.md)、[ci.yml](.github/workflows/ci.yml) |

## 4. 运行、检查与证据口径

- **正式单次触发**（默认 `~/.akzio/config.toml`，可在模式前加 `--config FILE`）：`bash scripts/run_core.sh position-plan` / `bash scripts/run_core.sh paper`。
- **离线全量门禁**（不新增根 `tests/` 或额外 `.sh`/`.py` 脚本）：

```bash
bash -n scripts/run_core.sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

- **当前冻结版本**：Domain 10 / Store 18 / Workflow 1；研究 Contract 69 / PromptBundle 38 / freshness 70；Outcome 63 / PromptBundle 35 / metric v3。
- **交付证据标签**：严格区分 `implemented`、`offline-verified`、`real-Paper-verified`、`outcome/learning-verified`。
