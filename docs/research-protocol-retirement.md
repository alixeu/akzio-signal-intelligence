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
| CLI | `run fixture-debug`、`run paper-dry-run`、`run submit`、`debug prepare --fixture-controller`、旧 `read-range-probe` 实验参数 | 正式 Debug prepare/control、只读 replay/export |
| HTTP | 通用 POST `/runs` 创建路由、`SubmitRequest`、`fixture_controller` 与 `read_range_probe` 字段 | 正式 Paper/PositionPlan 创建与控制；未知字段明确拒绝 |
| App | 新运行的 fixture 开关、Planner 模型路由、当前演示的 Planner 节点与角色卡 | 历史 Planner / PaperDryRun 标签与布局解码 |
| 活动注册 | Planner Contract、recipe、catalogue 必选项、模型路由及预算配置 | Analyst、Critic、Synthesizer、Outcome |
| Prompt / Schema | Planner 角色正文、Planner Draft 与 EvidenceNeed 输出 Schema、旧研究 Draft 拼接 | 研究结构化提交、Outcome 两阶段、当前补采请求 Schema |
| Workflow | Planner 启动 bootstrap、模型 Proposal 扩图、Store workflow patch 写入口、旧空图 retry | Rust 固定提案编译；PositionPlan retry 经正式准备流程 |
| 模块 | 原 `runtime/planner/` 模块 | 共享 lowering、校验和证据编译迁至 `runtime/compilation/` |
| 资源 | 裸 `quote` / `bars` 的 `LegacyFixture` 解析 | 明确资产、日期和数量的受治理资源 |
| Fixture | Planner 模型模板、旧 fixture 图、`config/task2-fixture.toml` | 正式图的明确 fixture adapters、`debug serve-fixture`、`debug verify-fixture` |
| 测试和文档 | 旧图控制测试、旧创建命令和过期的通用两阶段说明 | 正式九节点控制测试、Paper NoOrder 后续链、历史只读夹具 |

固定编译见 [compilation](../crates/akzio-runtime/src/runtime/compilation.rs)，验证入口见 [Debug CLI](../crates/akzio-cli/src/cli/debug_commands.rs)。

## 历史兼容与执行阻断

历史 CAS、事件、Contract、哈希、Commitment、Policy 激活记录及必要 wire 字段不改写。`RunPurpose::Debug` 继续支持证据审计；`PaperDryRun`、Planner Artifact/事件/角色类型、旧预算值及旧 Contract 能力边界只供历史解码、展示与完整性校验。

退休身份按旧 purpose、Planner 节点或研究 Contract 版本识别。启动升级预检列出未完成 Run/Task 与有效 lease，不取消、不改状态、不释放 lease；任务领取、resume、retry、step 和 fork 返回 `legacy_workflow_retired`。检查视图不会把退休节点标为可执行。

历史 replay 从持久化事件、图修订和冻结 Contract 校验，不依赖活动 Planner recipe。旧版本的特殊能力迁移规则只在历史完整性检查中使用，不再用于当前激活入口。

实现见 [Store 退休判定](../crates/akzio-store/src/store/workflow/helpers.rs)、[升级预检](../crates/akzio-store/src/store/workflow/contracts.rs)、[历史 replay](../crates/akzio-runtime/src/runtime/replay.rs)。冻结的脱敏离线旧运行及出处见 [历史夹具说明](../crates/akzio-store/src/store/fixtures/retired-history.md)。

## 离线验收

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
bash scripts/check_markdown_links.sh
cargo run --locked -p akzio-cli -- debug verify-fixture
```

`debug verify-fixture` 自动创建 `.akzio/verify-fixture-*/store`，完成正式 PositionPlan 九节点，检查节点结果、Store Doctor 和证据导出。输出 Run ID、证据目录和 `passed`，失败返回非零；Broker 始终 forbidden。详情及独立 HTTP 控制方式见 [开发 Workflow](development-workflow.md)。

协议回归覆盖 Paper、PositionPlan、Shadow 的 Submit 起始、工具限制，以及 Outcome 冻结哈希和恢复。Paper 离线图验证既有 ExecutionGate → NoOrder → Reconcile → OutcomeSchedule。Policy 回归单独使用旧 Synthesizer 冻结身份和显式合成校准数据，验证身份不匹配阻断且不激活 Policy。

真实模型与行情的原生 Paper 入口为 `python3 scripts/position_plan_run.py --paper`。它使用同一正式 Paper 图与隔离 Store，在原审批和全部 Gate 通过后向 Alpaca Paper 提交订单；没有本地模拟时钟、账户或成交，也没有恢复 Planner 或 PaperDryRun。旧 `-fakerOnline` / `--faker-online` 参数和配置被拒绝，`simulated_only` 只保留历史解码。缺 Policy 时仍形成正式 NoOrder，运行完成、订单提交与 Paper 成交分开报告。完整操作和归档边界见 [开发 Workflow](development-workflow.md)。

这些验收仅提供离线证据，不代表真实模型调用、Policy 激活、Paper 下单或跨交易日 Outcome 验收。
