# 研究协议统一与旧链路退休

活动研究主干为 Evidence → T1/T3/T5 Analyst → T1/T3/T5 Critic → Synthesizer → Decision。PositionPlan 在 Decision 后结束；Paper 继续既有 ExecutionGate、PaperCommit、Reconcile、OutcomeSchedule 和 Outcome 流程。Shadow 的研究角色使用相同提交协议。

## 协议与冻结身份

- 研究 Contract **65**、PromptBundle **36**、Analyst freshness candidate **66**。研究请求从 Submit 开始，仅提供授权 projections 和 `submit_result`，输出 `result + deliberation`。
- Outcome 保持 Contract **63**、PromptBundle **35**；Contract 哈希为 `c9556a7ca9000cd06b96e385876013e3ce06a330fb4d01a473a43ef2db067ddf`。Draft、受控读取、Submit、阶段恢复和累计预算继续保留。
- 引用闭包、方向资格、补采约束、Rust 日历绑定及冻结 result 的 deliberation 修复继续由 Rust 管理。研究角色的旧 Draft 指导、分阶段预算、reasoning 覆盖和整段压缩重提分支已删除。
- t1/t3/t5 明确表示基准 Session 后第 1/3/5 个共同完成的交易 Session。研究反馈报告 deliberation 数组长度和权重总和的实际错误；Synthesizer wire Schema 要求保留全部已选 Claim/Critique，包括受限期限，并区分零仓位理由与非零仓位支持条件。重试恢复重建与原请求相同的拒绝反馈，不改变累计预算或历史记录。
- Policy 与 Synthesizer Contract 的身份必须匹配。新 Contract 不复制、改写或激活旧 Policy；既有 preflight、显式校准与激活流程继续生效。

实现见 [运行时协议选择](../crates/akzio-research/src/agent/runtime_run.rs)、[活动 Contract](../crates/akzio-research/src/agent/catalogue.rs)、[请求构建](../crates/akzio-research/src/agent/prompts/phases.rs) 和 [Policy preflight](../crates/akzio-cli/src/cli/calibration.rs)。

## 删除与迁移清单

| 范围 | 删除或替换 | 保留职责 |
|---|---|---|
| CLI | `run fixture-debug`、`run paper-dry-run`、`run submit`、完整 `debug` 命令组、`evidence market-audit` / `evidence preflight` 及旧 `read-range-probe` 实验参数 | 正式 PositionPlan/Paper、只读 replay/export |
| HTTP | 通用 POST `/runs` 创建路由、`SubmitRequest`、`fixture_controller` 与 `read_range_probe` 字段 | 正式 Paper/PositionPlan 创建与控制；未知字段明确拒绝 |
| App | 新运行的 fixture 开关、Planner 模型路由、当前演示的 Planner 节点与角色卡 | 历史 Planner / PaperDryRun 标签与布局解码 |
| 活动注册 | Planner Contract、recipe、catalogue 必选项、模型路由及预算配置 | Analyst、Critic、Synthesizer、Outcome |
| Prompt / Schema | Planner 角色正文、Planner Draft 与 EvidenceNeed 输出 Schema、旧研究 Draft 拼接 | 研究结构化提交、Outcome 两阶段、当前补采请求 Schema |
| Workflow | Planner 启动 bootstrap、模型 Proposal 扩图、Store workflow patch 写入口、旧空图 retry | Rust 固定提案编译；PositionPlan retry 经正式准备流程 |
| 模块 | 原 `runtime/planner/` 模块 | 共享 lowering、校验和证据编译迁至 `runtime/compilation/` |
| 资源 | 裸 `quote` / `bars` 的 `LegacyFixture` 解析 | 明确资产、日期和数量的受治理资源 |
| Fixture | Planner 模型模板、旧 fixture 图、可操作的 `debug serve-fixture` / `debug verify-fixture` | 正式图的注入式 Cargo 离线测试 |
| 测试和文档 | 旧图控制测试、旧创建命令和过期的通用两阶段说明 | 正式九节点控制测试、Paper NoOrder 后续链、历史只读夹具 |

固定编译见 [compilation](../crates/akzio-runtime/src/runtime/compilation.rs)，验证入口见[开发 Workflow](development-workflow.md)。

## 历史兼容与执行阻断

历史 CAS、事件、Contract、哈希、Commitment、Policy 激活记录及必要 wire 字段不改写。`RunPurpose::Debug` 与 `PaperDryRun`、Planner Artifact/事件/角色类型、旧预算值及旧 Contract 能力边界只供历史解码、展示、只读导出与完整性校验；不得创建新的 Debug-purpose 证据图。

退休身份按旧 purpose、Planner 节点或研究 Contract 版本识别。启动升级预检列出未完成 Run/Task 与有效 lease，不取消、不改状态、不释放 lease；任务领取、resume、retry、step 和 fork 返回 `legacy_workflow_retired`。检查视图不会把退休节点标为可执行。

历史 replay 从持久化事件、图修订和冻结 Contract 校验，不依赖活动 Planner recipe。旧版本的特殊能力迁移规则只在历史完整性检查中使用，不再用于当前激活入口。

实现见 [Store 退休判定](../crates/akzio-store/src/store/workflow/helpers.rs)、[升级预检](../crates/akzio-store/src/store/workflow/contracts.rs)、[历史 replay](../crates/akzio-runtime/src/runtime/replay.rs)。冻结的脱敏离线旧运行及出处见 [历史夹具说明](../crates/akzio-store/src/store/fixtures/retired-history.md)。

## 离线验收

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
git diff --check
```

正式拓扑的离线测试由 Cargo 测试和明确注入的 fixture adapters 驱动；不启动 Debug 控制 HTTP、真实模型或 Broker。详情见[开发 Workflow](development-workflow.md)。

协议回归覆盖 Paper、PositionPlan、Shadow 的 Submit 起始、工具限制，以及 Outcome 冻结哈希和恢复。Paper 离线图验证既有 ExecutionGate → NoOrder → Reconcile → OutcomeSchedule。Policy 回归单独使用旧 Synthesizer 冻结身份和显式合成校准数据，验证身份不匹配阻断且不激活 Policy。

真实模型与行情的原生 Paper 入口为 `bash scripts/run_core.sh paper`（Rust `run start --purpose paper --auto-serve`）。它复用已有正式 Core，或在端口明确无服务时自动启动同配置 Core，在 canonical Store 中复用同一 Paper 图与 scheduler；原审批和全部 Gate 通过后才可能向 Alpaca Paper 提交订单。没有本地模拟时钟、账户或成交，也没有恢复 Planner 或 PaperDryRun。旧 `-fakerOnline` / `--faker-online` 参数和配置被拒绝，`simulated_only` 只保留历史解码。缺 Policy 时仍形成正式 NoOrder，提交确认、运行完成、订单提交与 Paper 成交分开核对。完整操作和按需导出边界见 [开发 Workflow](development-workflow.md)。

这些验收仅提供离线证据，不代表真实模型调用、Policy 激活、Paper 下单或跨交易日 Outcome 验收。
