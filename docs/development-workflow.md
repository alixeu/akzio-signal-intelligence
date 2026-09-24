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

长期文档与生成的架构 HTML 使用仓库相对文件路径、模块和命名符号定位源码，不固化易漂移的数字位置或定位锚点。文件级引用只帮助导航，不保证更新后的实现仍符合旧描述；涉及行为变化时须重新追调用链、Gate 和持久化写入点。

```bash
git diff --check
```

核对本次变更的本地 Markdown 引用路径；CI 在 [ci.yml](../.github/workflows/ci.yml) 内联扫描跟踪及未跟踪 Markdown 的相对链接，不依赖额外的本地脚本。若本地生成资料导致失败，区分本次改动与已有文件，不将全量失败表述为通过。

修改 Skill 时使用其现有验证器检查 frontmatter；检查相关引用文件和 UI 元数据是否存在矛盾。现有扩展字段与验证器不兼容时，对照修改前结果，不通过删除调用参数或放宽验证器来伪造通过。指令静态检查不等于真实模型行为验证。

## 3. 代码修改的离线检查

先运行所修改 crate 的具体测试，再完成项目原有的 workspace 检查。不要只测实现细节或用实现本身计算预期结果；验证受影响的外部行为。用户已确认的测试接口可复用；新增接口仍遵循适用 Skill 的明确确认要求。

CI 的实际配置见 [ci.yml](../.github/workflows/ci.yml)。与 CI 对齐的命令如下，按当前任务有效的已有结果可复用：

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
python3 -m unittest discover -s scripts -p 'test_run_core.py'
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


SwiftUI 或 App/Core 接口修改还需运行与改动相关的 Swift 检查；完整 Bundle 构建不能替代实际 UI 验收。

macOS 自用分发版本统一通过 `scripts/build_app_and_core.sh` 编译、打包与 ad-hoc 签名。它从当前源码构建 Swift App 和 release Rust CLI：分别得到新 `.app` 内的 `Contents/MacOS/akzio-core` 和独立的 `target/release/akzio`；脚本输出 `APP_BUNDLE`、`RUST_BINARY`、`EMBEDDED_CORE` 的路径。默认给每次构建选择新的时间戳 Bundle，保留产物且拒绝覆盖已有目标。运行：

```bash
bash scripts/build_app_and_core.sh
```

若需指定目标，可设 `AKZIO_APP_BUNDLE=<尚不存在的路径>`。已有目标不可覆盖；清理构建目录、删除旧 Bundle 仍须明确授权。源码入口见 [打包入口](../scripts/build_app_and_core.sh) 和 [内部构建脚本](../apps/Scripts/build_app.sh)。

`apps/Scripts/create_dmg.sh` 接受同一个 `AKZIO_APP_BUNDLE`，可用 `AKZIO_DMG_PATH` 指定新的 DMG 路径；已有 DMG 不会被覆盖。

真实运行脚本 `python3 scripts/run_core.py --mode position-plan`（默认）和 `python3 scripts/run_core.py --mode paper` 是**已有正式 Core 的客户端**，不是 Debug 启动器。默认读取 `~/.akzio/config.toml`，可用位置参数指定另一个正式配置；Store Root 必须为绝对路径。先从 App 或 `akzio --config <config> daemon serve` 启动 Core；脚本只读原配置和该 Store 的 token，限制回环地址、拒绝代理和认证重定向，通过 `/ready` 核对 canonical Store 与受支持的分享安全导出接口，再向正式 `POST /runs` 提交一次显式 purpose。Core 不可用或版本过旧时在创建 Run 前失败；不复制或修改配置、Store、Policy，不启动/停止 daemon，也不把 PositionPlan 自动切换为 Paper。PositionPlan 响应不确定时不自动重试，必须先在正式 Store 查询，避免重复创建 Run。`--research-only` 已从此正式入口移除；隔离研究实验使用独立 Debug 控制入口。

`/ready` 新增 `store_scope` 和 `formal_run_bundle_supported` 供客户端在提交前核对；旧版 Core 须更新并重启。认证的 `POST /control/store/export-run-bundle` 只导出 canonical Paper/PositionPlan 的分享安全快照到新目录，不回写 Store，也不向隔离 Debug Run 开放；既有 Debug bundle 入口仍单独保留。

缺 active DecisionPolicy 时，正式 PositionPlan 仍通过 Rust DecisionGate 形成可审查的研究分配与**全零正式目标仓位**，在 Decision 后结束，不产生 ExecutionVerdict 或 Outcome；证据不足时研究分配也可能为现金/阻断。已安装 Policy 的模型与 Contract 不匹配仍由正式 Core 拒绝或由原 Gate fail closed，脚本不补造或激活 Policy。`--mode paper` 的冷启动则由正式 scheduler 处理。脚本按 Run ID 读取 Rust 持久状态，分别报告研究提案、目标、ExecutionVerdict、订单提交、回执中的成交量和 OutcomeSchedule；Observer 有界投影缺失时显示 `not_observed`，不把缺失解释为零订单或真实成交。

每次请求可生成 `.akzio/position-plan-*.zip` 或 `.akzio/paper-*.zip`：只有本地脱敏 summary 与 Core 生成的分享安全 Run bundle，不包含 Store、配置、token 或未经授权的 RawEvidence/模型原文。`EXPORT_STATUS=partial` 可能仅表示授权范围外的 payload 被省略；它和 Run 完成状态独立报告。退出码 0 表示本次 T0 流程完成且分享安全导出无意外缺失（即使按权限部分省略），1 表示未启动/运行失败/提交结果不确定，2 表示 Run 完成但导出异常或观察投影缺失，3 表示 Run 仍在运行或等待。本地报告 ZIP 校验后默认删除本次临时报告目录；`--keep-artifacts` 只保留该报告目录，正式 Store 中的 Run 始终保留。旧 `debug_goal_run.zsh` 和 `paper_canary_run.zsh` 已移除。

未校准的 canonical Paper 冷启动使用默认 fail-closed DecisionPolicy：Decision 的执行目标全为 0；正式 scheduler 在 SQL active policy 缺失时，允许在真实可交易 Session 创建不绑定交易 approval 的 canonical Paper run；有 active policy 时仍要求原审批。无 approval 时 pre-trade safety 不作安全断言，ExecutionGate 保留 `UnqualifiedRuntime` 并持久化 `NoOrder`，不生成 ExecutionPlan，不获取执行 lease，也不写 Broker。PaperCommit 与 Reconcile 返回 NoOutput，Evaluate 仍生成带 `NoOrder` lineage 的 OutcomeSchedule。只有该正式 Paper 入口的成熟 Outcome 才可能供后续校准；PositionPlan 没有 Outcome，不作为校准样本。

显式使用真实模型、行情和 Alpaca Paper 时使用：

```bash
python3 scripts/run_core.py --mode paper
```

`--mode paper` 只在用户显式选择时调用正式 Core 的 Paper scheduler，不修改 `auto_paper`、`manual_paper` 或 Outcome worker 配置。若正式 Core 未配置 Paper 能力、休市或审批/资格条件未满足，服务端给出拒绝或等待原因，不伪造已启动的 Run。旧 `-fakerOnline` / `--faker-online` 与本地模拟 Broker 仍被拒绝。

Paper purpose 不等于 PaperLaunchApproval。没有 active Policy 时走零目标 / NoOrder；Policy、Approval、Decision Validity、Quote Freshness、Risk、Capacity、Compliance、Turnover 和 Exposure 检查均保留。只有原审批和全部 Gate 通过后才可能向 Alpaca Paper 发单。不存在本地模拟 Broker、账户、成交或假开市。

TradingSession 由真实 Clock、Alpaca Calendar 和美东时区共同确定：PreMarket 04:00 至当日开盘，Regular 按交易日历开收盘，AfterHours 收盘至 20:00，Overnight 为下一交易日之前的 20:00 至 04:00，其余为 Closed。正常日开收盘是 09:30 / 16:00；周末、节假日、提前收盘与夏令时以日历及时区为准。`clock.is_open=false` 不再单独表示不能交易。隔夜 trade date 是其后的交易日，不能用 UTC 日期或夜间自然日代替；服务端 scheduler 从真实 Broker Clock/Calendar 获取 broker_session；脚本不自行推断交易日期。[Alpaca 24/5 规则](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api)。

Regular 订单沿用原逻辑；其余可交易时段使用 `limit + day + extended_hours=true`。Overnight 检查资产当次 API 返回的 `overnight_tradable` / `overnight_halted`（兼容 `attributes` 表达），并在提交前再次确认。原 SIP 配置夜间使用 `boats`，原 IEX 配置夜间使用 `overnight` 实时指示行情，不回退到 IEX、不改写报价时间。权限失败会报告失败；指示行情仍接受原有数据质量 Gate，选对 feed 不保证获得执行资格。[最新报价接口](https://docs.alpaca.markets/us/reference/stocklatestquotes-1)。

Closed 会持久化延期任务，等下次可交易时段或 Decision 到期。再次领取时重新刷新 Account / Quote / Clock，再运行原 ExecutionGate；Decision 过期会被拒绝。提交成功的 `accepted/new/partially_filled` 不是最终成交，短轮询超时只保存待成交进度并等待下一次 Reconcile。扩展时段订单不因原来的 60 秒阈值撤改，Regular 行为不变。最终成交、取消、过期或拒绝以 Alpaca 回执为准。

脚本只连接运行中的 Core，不暂停 Run、不关闭进程。T0 结束、订单待成交或观察窗口超时后，正式 Core 与同一 Store 继续承担对账及未来 Outcome；再次检查使用返回的 `RUN_ID` 调用正式 `run inspect` / `run replay`，不能重发 PositionPlan POST 来代替恢复。`--mode paper` 同交易 Session 的复用由 Rust session slot 负责。报告中 `OutcomeSchedule` 仅表示未来任务已安排，不等于订单成交、T+5 密封或 Policy 激活。

`FINAL_STATUS`、`EXPORT_STATUS`、Paper 回执中的订单提交/成交、Outcome 状态分别报告；accepted/new/partially_filled 仍不是最终成交。超时只报告 pending 并保留 Core 中的任务，不请求取消或复制 Store。

只读缺口报告（已有 Store，不创建或修改 Store）：

```bash
target/debug/akzio calibration readiness --store ~/.akzio/store --min-samples 30
```

报告扫描最近最多 500 个 Decision，按 Paper run 列出 `sealed`、`pending`、`blocked`、`no_schedule`；只有 canonical 且完整四资产 × T1/T3/T5 标签的成熟 run 计入 gap 和 12 个槽位。pending 报告 baseline 交易日及尚未持久化的期限；缺冻结 account/quote baseline 的 run 标记 `baseline_snapshot_missing`，等待不能补回，应在开市时段重新运行并取得完整快照。缺快照本身不能证明原因一定是休市。报告只依据持久化 Outcome windows，不把自然日流逝当成真实交易日推进。

`store_scope` 和 `calibration_eligible` 表示 Store 是否具备参与正式校准的资格，不表示样本足够或 Policy 可激活。隔离 Debug Store 报告 `isolated_debug_store`，其中的运行不计入成熟样本，并提示 `use_canonical_store`；等待或完成隔离 Outcome 都不会改变资格。此边界与 `collect` 拒绝隔离 Store 一致。

Outcome worker 必须从真实 Alpaca bars 等待四资产共同完成的 T+1/T+3/T+5 交易 Session 后才能密封。成熟样本够数时 readiness 提示 collect；collect 仍检查模型/Contract、训练窗口、价格序列和 risk limits，通过后把完整 dataset 保存为 SQL Artifact，之后才可以 build。随后 `inspect → validate → activate` 仍由 operator 显式执行，readiness 和 preflight 不自动激活 policy。离线回归只证明 NoOrder 调度链路，不能替代真实 Outcome 密封或 collect/build/activate 的端到端验收。

原配置中的 `research.planner` 路由及 Planner 预算已退役；正式脚本不再生成隔离配置副本或自动迁移它们。Core 配置不合法时需按既有授权单独修正原配置，不通过恢复 Planner 注册放行。

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

首次安装的 canonical Paper 冷启动与 PositionPlan 是两条不同入口：前者用于积累真实 Outcome，须显式选择 `--mode paper` 并满足真实行情/模型配置；后者可在缺 Policy 时产出零正式目标和研究分配，但不产生校准样本。脚本不修改 `auto_paper` 或 Paper 审批，也不自动激活 Policy。

普通 Observatory Core 使用 `~/.akzio/store` 与 `~/.akzio/config.toml`；Debug Core 必须使用新隔离 Store。`Paper scheduler waiting: broker market is closed` 是正常非交易状态，不视为服务异常。

脚本的 `--keep-artifacts` 只保留本地报告暂存目录。默认在 ZIP 写入和完整性校验成功后删除该暂存目录；归档失败则保留并说明原因。任何选项都不删除正式 Core、Store、Run、Commitment 或历史状态；ZIP 不包含配置、Store 和凭据。

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
