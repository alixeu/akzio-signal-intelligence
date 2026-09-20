# Akzio 开发 Workflow

本文件统一开发助手的任务推进、验证和交付流程。应用的 Paper Workflow、受控模型工具和运行时审批仍按 [运行时契约](agent-runtime-contract.md) 执行。

## 1. 定位与推进

1. 依据当前请求和已有决策确定交付，检查 `git status --short`，定位相关源码、调用方和测试。只读取 [AGENTS.md](../AGENTS.md) 索引中与任务有关的章节。
2. 在现有模块边界内实现直接、完整的方案，连同必要的调用方和校验修改。普通细节自行决定；缺失事实先查，只有影响结果或权限的未决选择才提问。
3. 独立只读操作可批量并行；修改、依赖验证、授权和副作用按顺序处理。等待未完成操作的结果后再重试。获准委派时交给子任务独立工作，主任务负责整合到最终交付。
4. 按下面的分支验证。保留仍适用的结果；只有代码变化、失败或新疑点才重跑或扩大范围。不要为常规修改追加与行为无关的测试、强制访谈或额外审阅轮次。
5. 完成已授权的准备工作，再为确需批准的具体操作请求批准。待批准或外部依赖阻塞时继续其他必要工作，并给出可审阅结果和准确的剩余项。

## 2. 文档、指令与 Skill

只改 Markdown、AGENTS 或 Skill 指令时，检查实际差异、引用路径、触发条件与正文的一致性，以及是否改变明确的审批或业务约束。运行适用的文档检查，不因此启动 Rust/Core、API、Paper 或 T+5 流程。

```bash
git diff --check
bash scripts/check_markdown_links.sh
```

该链接脚本扫描本地 Markdown；若本地生成资料导致失败，区分本次改动与已有文件，并对本次文件给出可核验的检查结果，不将全量失败表述为通过。

修改 Skill 时使用其现有验证器检查 frontmatter；检查相关引用文件和 UI 元数据是否存在矛盾。现有扩展字段与验证器不兼容时，对照修改前结果，不通过删除调用参数或放宽验证器来伪造通过。指令静态检查不等于真实模型行为验证。

## 3. 代码修改的离线检查

先运行所修改 crate 的具体测试，再完成项目原有的 workspace 检查。不要只测实现细节或用实现本身计算预期结果；验证受影响的外部行为。用户已确认的测试接口可复用；新增接口仍遵循适用 Skill 的明确确认要求。

CI 的实际配置见 [ci.yml](../.github/workflows/ci.yml)。与 CI 对齐的命令如下，按当前任务有效的已有结果可复用：

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

格式问题只修本任务相关内容，再检查；不得为了通过 `cargo fmt --all` 改动无关的脏工作区。独立的既有失败要说明，不顺带改掉或掩盖。CI 中的安全审计和 SBOM jobs 保留原定义。

正式拓扑的离线验收统一使用：

```bash
cargo run --locked -p akzio-cli -- debug verify-fixture
```

命令自动在 `.akzio/` 下创建新隔离 Store，使用明确的 fixture adapters 完成默认 21 节点的 PositionPlan：Evidence、首轮三个 Analyst/Critic 对、共享补采、三个可跳过的重跑对、三组 Synthesizer/ProposalReviewer 与 Decision。初稿审查通过的 fixture 实际执行八次研究模型调用，其余重跑与修订节点显式 skipped。它检查每个节点、Store Doctor 和证据导出，输出 Run ID、证据目录和结果；任一检查失败返回非零。Broker 写入始终 forbidden，不读取模型或 Broker 凭据。这是正式拓扑的离线验证，不是实盘数据或模型预测验收。源码入口见 [debug_commands.rs](../crates/akzio-cli/src/cli/debug_commands.rs)。

需要测试暂停、单步、恢复和 HTTP 控制时，继续用 `debug serve-fixture` 与 [隔离 fixture 配置](../config/debug-controller-fixture.toml)，通过 `debug prepare --purpose position-plan --session YYYY-MM-DD` 创建正式图。旧 Planner / PaperDryRun 创建命令及 `--fixture-controller` 参数已删除，旧字段请求被拒绝。

独立 `store doctor` 是认证 HTTP 客户端，需要与所选配置一致且已经获准运行的隔离 daemon。执行前先确认配置、endpoint、Store 和该服务的状态，再使用显式配置调用，并检查返回报告：

```text
cargo run --locked -p akzio-cli -- --config <对应隔离配置路径> store doctor
```

fixture 退出后不能假设 daemon 仍在。缺少适用服务时报告独立 Doctor 验证受阻，继续其他检查；不得为凑齐结果改连正式 Store、启用 auto_paper 或额外调用模型。具体控制与恢复边界见 [Debug 控制](debug-control.md)。

## 4. Observatory App

日常 App 的运行入口只提供 PositionPlan（仅生成仓位计划）和 Paper（完整模式）。认证原生客户端通过 `POST /runs` 提交 purpose；PositionPlan 原子发布正式研究图，在 Decision 后结束；Paper 请求现有 scheduler tick，沿用真实 Clock、审批绑定、Session 排他预约与全部 Gate，同 Session 返回已有 Run。休市或审批等前置未满足时返回明确拒绝，不伪造已开始。

`daemon.manual_paper=true` 仅配置手动 Paper 运行能力，初始化与自动 Paper 相同的 Broker、runtime identity 和启动校验；`auto_paper=false` 时不会自动创建新 T0。内置 App 模板默认采用这组配置。隔离 Debug Core 拒绝日常启动接口，不能通过切换界面解除 Store 隔离或 Broker 限制。已有日常配置需要显式迁移后生效。


SwiftUI 或 App/Core 接口修改还需运行与改动相关的 Swift 检查。当前可用的检查入口为 `swift run --package-path apps DebugContractChecks`；完整 Bundle 构建不能替代实际 UI 验收。

macOS 分发版本统一通过 `scripts/update_app_and_submit_debug.sh` 编译、打包与签名。Bundle 必须包含 `Contents/MacOS/akzio-core`。脚本始终保留构建产物，并拒绝覆盖已有 Bundle；使用工作区内新的 Bundle 目标，例如：

```bash
AKZIO_APP_BUNDLE="$PWD/apps/dist/akzio-review-$(date -u +%Y%m%dT%H%M%SZ).app" bash scripts/update_app_and_submit_debug.sh
```

已有目标不可覆盖；清理构建目录、删除旧 Bundle 仍须明确授权。源码入口见 [打包脚本](../scripts/update_app_and_submit_debug.sh) 和 [构建脚本](../apps/Scripts/build_app.sh)。

`apps/Scripts/create_dmg.sh` 接受同一个 `AKZIO_APP_BUNDLE`，可用 `AKZIO_DMG_PATH` 指定新的 DMG 路径；已有 DMG 不会被覆盖。

脚本离线回归入口：

```bash
python3 scripts/tests/check_markdown_scope.py
python3 scripts/tests/check_position_plan_isolation.py
python3 scripts/tests/check_position_plan_redirect.py
python3 scripts/tests/check_shell_scripts.py
swift run --package-path apps DebugContractChecks
python3 scripts/tests/check_observer_redirect.py "$(swift build --package-path apps --show-bin-path)/DebugContractChecks"
```

真实 PositionPlan 的现行入口是 `python3 scripts/position_plan_run.py`：默认读取 `~/.akzio/config.toml`，也可追加配置路径覆盖。脚本复制配置并创建新的 `.akzio/` 隔离 Store，不让 Debug Core 打开 `~/.akzio/store`；它只从原配置的 canonical SQL Store 复制 active DecisionPolicy 的精确 CAS Artifact。canonical Store 数据库尚不存在时由 Rust Store 初始化完整 schema；已有数据库始终只读打开。数据库无法读取、版本不兼容或完整性失败时直接阻断。DecisionPolicy 的风险限制、dataset 和候选 policy 全部保存为 SQL CAS Artifact；旧文件配置与 JSON 文件输入命令已移除。operator 使用 `calibration activate --store <canonical-store> --policy <artifact-id>` 选择已存候选版本，启动流程不会自动激活。随后 Rust calibration preflight 分别报告 research_capable 和 decision_capable：没有 active head 时默认在 LLM 前停止；显式 `--research-only` 才进入已标记 incomplete 的研究，并在终稿审查结束后停止，Decision 不运行；即使 Policy 已就绪，`--research-only` 也保持此研究边界，审查拒绝与缺 Policy 分别报告；已安装 policy 的 Artifact ID、input hash、模型/版本和 Synthesizer Contract 校验失败时，在 daemon/模型探测前停止并输出诊断 ZIP。运行使用受控 `debug prepare/resume`，Session Identity 固定同一 policy Artifact，调用真实模型但禁止 Broker 写入。结束时自动生成 `.akzio/position-plan-*.zip` 并输出 `ZIP=<绝对路径>`，包含导出 bundle 和脱敏后的运行日志、状态 JSON，不包含配置文件、Store 或认证 Token。`EXPORT_STATUS` 表示导出完整性：`complete`、`partial` 或尚无 bundle 的 `unavailable`；异常中止时 ZIP 可能只有诊断信息。退出码 0 表示运行完成且导出完整，1 表示运行失败、预检阻断或显式 research-only incomplete，2 表示运行完成但导出不完整；具体状态以 FINAL_STATUS 和 summary.json 为准。旧 `debug_goal_run.zsh` 和 `paper_canary_run.zsh` 已移除，不再使用其临时验收流程。

未校准的 canonical Paper 冷启动使用默认 fail-closed DecisionPolicy：Decision 的执行目标全为 0；正式 scheduler 在 SQL active policy 缺失时，允许在真实开市 Session 创建不绑定交易 approval 的 canonical Paper run；有 active policy 时仍要求原审批。无 approval 时 pre-trade safety 不作安全断言，ExecutionGate 保留 `UnqualifiedRuntime` 并持久化 `NoOrder`，不生成 ExecutionPlan，不获取执行 lease，也不写 Broker。PaperCommit 与 Reconcile 返回 NoOutput，Evaluate 仍生成带 `NoOrder` lineage 的 OutcomeSchedule。此链路不改变 PositionPlan 的预检或研究模式边界。

显式使用真实模型、行情和 Alpaca Paper 时使用：

```bash
python3 scripts/position_plan_run.py --paper
```

`--paper` 创建 `.akzio/paper-*` 新隔离 Store，使用正式 Paper 图及统一研究协议。它只在隔离配置中移除退休 Planner 路由/预算并记录 `config-migration.json`，不修改原配置或恢复旧链路。旧 `-fakerOnline` / `--faker-online` 参数及 `daemon.faker_online` 配置已删除，不静默转换旧的禁止发单权限。

Broker 身份为 `paper_allowed`，唯一交易环境是 Alpaca Paper。该身份仍不等于 PaperLaunchApproval，也不自动复制或生成审批。没有 active Policy 时走原来的零目标 / NoOrder；Policy、Approval、Decision Validity、Quote Freshness、Risk、Capacity、Compliance、Turnover 和 Exposure 检查均保留。不存在本地模拟 Broker、账户、成交或假开市。

TradingSession 由真实 Clock、Alpaca Calendar 和美东时区共同确定：PreMarket 04:00 至当日开盘，Regular 按交易日历开收盘，AfterHours 收盘至 20:00，Overnight 为下一交易日之前的 20:00 至 04:00，其余为 Closed。正常日开收盘是 09:30 / 16:00；周末、节假日、提前收盘与夏令时以日历及时区为准。`clock.is_open=false` 不再单独表示不能交易。隔夜 trade date 是其后的交易日，不能用 UTC 日期或夜间自然日代替；启动器通过现有 Observer 只读接口获取 Rust 的 broker_session，无法获取时停止准备，不用本机日期兜底。[Alpaca 24/5 规则](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api)。

Regular 订单沿用原逻辑；其余可交易时段使用 `limit + day + extended_hours=true`。Overnight 检查资产当次 API 返回的 `overnight_tradable` / `overnight_halted`（兼容 `attributes` 表达），并在提交前再次确认。原 SIP 配置夜间使用 `boats`，原 IEX 配置夜间使用 `overnight` 实时指示行情，不回退到 IEX、不改写报价时间。权限失败会报告失败；指示行情仍接受原有数据质量 Gate，选对 feed 不保证获得执行资格。[最新报价接口](https://docs.alpaca.markets/us/reference/stocklatestquotes-1)。

Closed 会持久化延期任务，等下次可交易时段或 Decision 到期。再次领取时重新刷新 Account / Quote / Clock，再运行原 ExecutionGate；Decision 过期会被拒绝。提交成功的 `accepted/new/partially_filled` 不是最终成交，短轮询超时只保存待成交进度并等待下一次 Reconcile。扩展时段订单不因原来的 60 秒阈值撤改，Regular 行为不变。最终成交、取消、过期或拒绝以 Alpaca 回执为准。

该模式关闭自动创建 Paper run，启用现有 Outcome worker，不能与 `--research-only` 混用。Paper 隔离 Store 始终保留，即使没有传 `--keep-artifacts`，以便继续对账和等待真实 T+1/T+3/T+5。观察窗口结束后暂停 Run 并停止本次 Core；不会伪装成交或永久后台运行。用输出的 `RETAINED_ROOT` 和 `RUN_ID` 恢复同一 Store：

```text
target/debug/akzio --config <RETAINED_ROOT>/runtime.toml daemon serve
target/debug/akzio --config <RETAINED_ROOT>/runtime.toml debug resume <RUN_ID>
```

第二条在另一个终端执行，原审批、冻结运行身份与恢复检查继续有效；已完成的 Run 只需 Core 的 Outcome worker，无须 resume。不要另建 Store 代替待成交订单的原始 Commitment。归档仍不包含配置、凭据或 Store；订单效果与成交证据读取原 Store 和脱敏 bundle，不把缺少统计写成零 Broker 请求。

全部节点完成或完成并形成 NoOrder、且导出完整时返回 0；失败返回 1；完成但导出不完整返回 2；仍有休市等待或待成交任务时返回 3。`FINAL_STATUS`、`EXPORT_STATUS`、Paper 回执和 Outcome 状态分别报告，流程完成至 OutcomeSchedule 不代表真实成交或跨交易日评估通过。

只读缺口报告（已有 Store，不创建或修改 Store）：

```bash
target/debug/akzio calibration readiness --store ~/.akzio/store --min-samples 30
```

报告扫描最近最多 500 个 Decision，按 Paper run 列出 `sealed`、`pending`、`blocked`、`no_schedule`；只有 canonical 且完整四资产 × T1/T3/T5 标签的成熟 run 计入 gap 和 12 个槽位。pending 报告 baseline 交易日及尚未持久化的期限；缺冻结 account/quote baseline 的 run 标记 `baseline_snapshot_missing`，等待不能补回，应在开市时段重新运行并取得完整快照。缺快照本身不能证明原因一定是休市。报告只依据持久化 Outcome windows，不把自然日流逝当成真实交易日推进。

`store_scope` 和 `calibration_eligible` 表示 Store 是否具备参与正式校准的资格，不表示样本足够或 Policy 可激活。隔离 Debug Store（包括 `--paper`）报告 `isolated_debug_store`，其中的运行不计入成熟样本，并提示 `use_canonical_store`；等待或完成隔离 Outcome 都不会改变资格。此边界与 `collect` 拒绝隔离 Store 一致。

Outcome worker 必须从真实 Alpaca bars 等待四资产共同完成的 T+1/T+3/T+5 交易 Session 后才能密封。成熟样本够数时 readiness 提示 collect；collect 仍检查模型/Contract、训练窗口、价格序列和 risk limits，通过后把完整 dataset 保存为 SQL Artifact，之后才可以 build。随后 `inspect → validate → activate` 仍由 operator 显式执行，readiness 和 preflight 不自动激活 policy。离线回归只证明 NoOrder 调度链路，不能替代真实 Outcome 密封或 collect/build/activate 的端到端验收。

准备前先核对当前配置与 CLI：`research.planner` 路由及 Planner 预算已退役，启动器只清理隔离副本，不会修改 `~/.akzio/config.toml`。处理原配置时沿用已有授权和工作区外写入规则，先备份，再删除明确退役的段落，保留有效角色的路由和预算。不要通过恢复旧配置解析或 Planner 注册来让启动通过。

`collect` 要求有效 Synthesizer 路由具有 `release_date` 和 `knowledge_cutoff`（可继承共享模型配置），这些值应来自模型提供方的真实版本资料，不能据模型别名猜测。readiness 只检查持久化事实与适用身份，不替代完整配置、模型元数据和 collect 校验；`store_active_head_missing` 也不表示存在一个只待激活的候选。若没有 canonical 历史，应先通过获准的正式冷启动积累真实 baseline 与 Outcome，而不是反复运行隔离 Paper、复制其样本或降低样本要求来凑数。

SQL 校准操作顺序（参数中的 ID 都是 Store Artifact ID，不是文件路径）：

```text
akzio calibration set-risk-limits --store <store> <显式风险限制参数>
akzio calibration inspect --store <store> --artifact <risk-limits-id>
akzio --config <config> calibration collect --store <store> --risk-limits <risk-limits-id> --min-samples 30
akzio calibration build --store <store> --dataset <dataset-id>
akzio calibration inspect --store <store> --artifact <policy-id>
akzio calibration validate --store <store> --policy <policy-id>
akzio --config <config> calibration activate --store <store> --policy <policy-id>
```

`set-risk-limits --help` 列出所有必填参数，数值必须由 operator 明确给定，没有伪造的默认校准样本或自动批准的风险配置。collect 只接受本 Store 的 canonical Decision/Outcome，风险限制与来源 Artifact 形成持久化引用；不成熟时仅报告 BLOCKED，不生成 dataset。build 从 SQL 读取完整 dataset 并持久化候选 DecisionPolicy；没有 active head 时，build 完成后 head 仍为空。activate 校验模型和已存 active Synthesizer Contract 后，激活同一个 policy Artifact，保留既有激活历史。新 `CalibrationRiskLimits` / `CalibrationDataset` 复用原 CAS 表，不改写历史 Artifact 或增设并行配置权威。

首次安装的 canonical Paper 冷启动与 PositionPlan 是两条不同入口：前者用于积累真实 Outcome，必须由用户启用正式 scheduler 并满足真实行情/模型配置；后者仍保留既有无 policy 时的显式 research-only 行为。代码修复不自动修改本机 `auto_paper`，不启动真实付费模型或订单，也不免除 policy 激活后的 Paper 交易审批。

普通 Observatory Core 使用 `~/.akzio/store` 与 `~/.akzio/config.toml`；Debug Core 必须使用新隔离 Store。`Paper scheduler waiting: broker market is closed` 是正常非交易状态，不视为服务异常。

非 Paper 的 PositionPlan 脚本指定 `--keep-artifacts` 时保留隔离运行目录用于复核；未指定时，确认 Core 已停止并成功写入、校验 ZIP 后，删除本次临时运行目录，只保留同名 ZIP；适用于运行成功、失败和部分导出。临时配置、隔离 Store、散落日志及 bundle 目录随之清理，原始配置、canonical Store 和历史运行不受影响。ZIP 写入或校验失败、Core 退出未确认时保留本次目录并报告原因。ZIP 仍沿用脱敏导出规则，不包含原始配置和 Store；删除后不能从该临时 Store 恢复运行。

在 policy 未就绪时，真实行情仍可独立验证：`target/debug/akzio --config ~/.akzio/config.toml evidence market-audit --output .akzio/market-audit-new --option-feed indicative`。输出目录必须不存在；该命令只使用真实 Rust Alpaca adapter 的 GET 请求，在输出目录的新 Store 中通过 EvidenceRuntime 和任务许可提交 RawEvidence/NormalizedEvidence，并运行 Store 完整性检查。报告记录每项持久化结果、覆盖率和错误；进程正常退出不代表每种 feed 都有权限。使用另一新目录和 `--option-feed opra` 分别核验权限，不自动回退。该证据专用审计图没有模型、Decision 或交易节点，不能充当真实 LLM PositionPlan 验收。

## 5. 完成与证据

Context / 终稿评审 / Lesson 修改还可使用[研究质量验证](research-quality.md)的 24 项稳定目录和受限真实模型实验。真实模型实验需要已有调用授权；40 次预算由同一隔离 Store 记录，不能通过换 phase 或进程重置。离线目录、真实模型语义评测、正式 Paper 和 Outcome 分别报告。

完成当前任务约定的交付和适用强制检查后收尾；不以继续执行无关检查代替交付。若必要验证缺失，明确写出已完成、被阻塞的步骤及其依赖，不宣称全部完成。

| 状态 | 可作出的结论 |
|---|---|
| `implemented` | 指定修改已经写入 |
| `offline-verified` | 实际运行的本地检查通过，注明覆盖范围 |
| `real-Paper-verified` | 本次获准的真实 Alpaca Paper 验证通过 |
| `outcome/learning-verified` | 本次实际跨交易日 Outcome/学习验收通过 |

真实 LLM 证据应另行注明调用、Run、配置和范围，不与 fixture 或 Paper 订单验证混为一谈。四级状态是证据标签，不要求文档或普通修复任务无条件执行到 T+5；模型推荐也不能替代原运行时审批、冻结预算或 Gate。
