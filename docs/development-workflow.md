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
bash -n scripts/run_core.sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

上述是开发检查命令，不是日常运行模式。Shell 的双模式分派与正式客户端 HTTP 边界回归位于 `akzio-cli`，由 Cargo 测试运行；没有单独的 Python 测试脚本。

格式问题只修本任务相关内容，再检查；不得为了通过 `cargo fmt --all` 改动无关的脏工作区。独立的既有失败要说明，不顺带改掉或掩盖。CI 中的安全审计和 SBOM jobs 保留原定义。

正式拓扑的离线验收使用 `cargo test --workspace --locked` 中注入 fixture adapters 的 Rust 测试；不再启动可操作的 fixture Core，也不再运行 `debug verify-fixture`。旧 Debug CLI、HTTP 控制和独立配置已退役。需要单独运行 `store doctor` 时，仍必须确认目标配置、endpoint、Store 和已获准的服务；不得为了验证改连正式 Store 或启动模型/Paper。运行归档见[私有阶段日志](run-log.md)。

## 4. 正式运行与 Core 生命周期

日常正式客户端只提供 PositionPlan（仅生成仓位计划）和 Paper（完整模式）。认证原生客户端通过 `POST /runs` 提交 purpose；PositionPlan 原子发布正式研究图，在 Decision 后结束；Paper 请求现有 scheduler tick，沿用真实 Clock、审批绑定、Session 排他预约与全部 Gate，同 Session 返回已有 Run。休市或审批等前置未满足时返回明确拒绝，不伪造已开始。

`daemon.manual_paper=true` 仅配置手动 Paper 运行能力，初始化与自动 Paper 相同的 Broker、runtime identity 和启动校验；`auto_paper=false` 时不会自动创建新 T0。内置配置模板默认采用这组配置。旧隔离 Debug Store 始终拒绝作为正式 Core 打开，已有日常配置需要显式迁移后生效。

日常脚本只保留 `bash scripts/run_core.sh position-plan` 和 `bash scripts/run_core.sh paper`，无参数或 `--help` 仅显示简短帮助，不依赖 Python/curl/jq。两者分别调用 Rust `run start --purpose position-plan|paper --auto-serve`，与正式 HTTP 客户端使用同一个 `POST /runs`。若所选回环端口已有服务，必须核验配置 Store 的 token、canonical `/ready` 和导出能力，只复用该服务；仅连接明确被拒绝时，才用同一当前源码的 `akzio --config <config> daemon serve` 启动正式 Core，等待 ready 后提交。认证、隔离 Store、旧版 Core 或非连接拒绝错误不能触发额外服务。默认读取 `~/.akzio/config.toml`，仅可在模式前指定 `--config FILE`（相对路径按调用者目录解析）；CLI 通过 `cargo run --locked` 从当前源码构建，不自动选旧二进制。脚本不提供开发/运维模式、`--dry-run` 或任意 CLI 透传，日常说明见 [AGENTS](../AGENTS.md)。

已有 Core 时不重复启动；没有服务时自动启动的 Core 将在脚本返回后独立运行，以便处理未来 Outcome。默认配置、Store/token 和新启动日志都在 `~/.akzio/`：分别为 `config.toml`、`store/` 与 `logs/core-*.log`。日志目录设为仅用户可进入，日志文件仅用户可读写；每次启动使用新日志，不覆盖已有 `logs/core.log`，不再写仓库 `.akzio/`。若显式指定另一正式配置，日志跟随其 Store Root 的父目录下的 `logs/`。PID 和日志路径从 `CORE_STARTED_PID`、`CORE_LOG` 输出。若启动前模型能力探测、凭据、Policy/Contract 或 Paper 配置失败，会在 ready 前退出并提示查看日志，不会提交 Run；启动等待有界（最多五分钟），超时后 Core 可能仍在启动，不能盲目重发。脚本自启的 Core 应用同配置的原生 `run replay/detail` 查询。正式客户端只读原配置和指定 Store 的 token，要求绝对 Store Root、回环地址及一致的 `AKZIO_STORE_ROOT`，禁止代理、认证重定向及 HTTP 自动重试；通过 `/ready` 核对 canonical Store、私有日志可用性和分享安全导出能力，再向 `POST /runs` 提交一次 purpose。客户端不复制/修改配置、Store 或 Policy，不把 PositionPlan 自动切换为 Paper。若 `auto_paper=true` 且尚无 Core，PositionPlan 拒绝自动启动以避免间接开启 Paper scheduler；脚本不改此配置。`unknown_after_post` 表示可能已经创建 Run，必须先查询原 Run/Session，不能直接重试。隔离 Debug 实验命令已退役，不再通过其他入口恢复。

`/ready` 提供 `store_scope`、`run_log_ready` 和 `formal_run_bundle_supported` 供客户端在提交前核对；旧版 Core 须更新并重启。认证的 `POST /control/store/export-run-bundle` 只导出 canonical Paper/PositionPlan 的分享安全快照到新目录，不回写 Store，也不向隔离 Debug Run 开放；历史 Debug bundle 的只读导出仍单独保留。

缺 active DecisionPolicy 时，正式 PositionPlan 仍通过 Rust DecisionGate 形成可审查的研究分配与**全零正式目标仓位**，在 Decision 后结束，不产生 ExecutionVerdict 或 Outcome；证据不足时研究分配也可能为现金/阻断。已安装 Policy 的模型与 Contract 不匹配仍由正式 Core 拒绝或由原 Gate fail closed，客户端不补造或激活 Policy。`paper` 的冷启动由正式 scheduler 自动判断，无需单独模式。两者在 T0 都包含 T1/T3/T5 预测，只有 Paper 安排后续跨交易日评估。日常通过原生 `akzio --config <config> run detail RUN_ID` 查看有界 Observer JSON；开发诊断可用 `run replay` 读取持久化生命周期，`run retrospectives` 读取已保存复盘。研究提案、正式目标、ExecutionVerdict、订单提交、成交与 OutcomeSchedule 仍分别核对；投影缺失不等于零订单或真实成交。

提交命令只打印 JSON `run_id`、`purpose`、`submission_status=submitted_or_reused`，不等待 T0、不自动导出或清理。返回 0 仅表示该次提交/复用被确认，不能解释为流程完成、订单成交或 Outcome 密封；传输/解码/HTTP 错误返回非零，提交不确定时明确标为 `unknown_after_post`。这与已删除的 Python 入口的 `FINAL_STATUS`、0/1/2/3 业务退出码、自动 ZIP 和 `--keep-artifacts` 不兼容。参数错误返回非零；Shell 保留底层命令退出码。

开发诊断需要导出时，使用原生 `akzio --config <config> run export-bundle RUN_ID /NEW/ABS/DIR` 调用 Rust 分享安全接口，生成含 manifest、checksums 的新目录，拒绝覆盖已有目录/符号链接；不包含 Store、配置、token 或未经授权的原文。不自动压 ZIP，也不删除目录或历史状态。返回 manifest 的 `integrity.status=partial` 可能仅表示授权范围外 payload 被省略；缺失/损坏等异常保持非零退出，原 Run 状态独立存在。该工具不属于日常 Shell 模式；实现见 [formal_run.rs](../crates/akzio-cli/src/formal_run.rs) 和 [Store bundle](../crates/akzio-store/src/store/debug_bundle.rs)。

未校准的 canonical Paper 冷启动使用默认 fail-closed DecisionPolicy：Decision 的执行目标全为 0；正式 scheduler 在 SQL active policy 缺失时，允许在真实可交易 Session 创建不绑定交易 approval 的 canonical Paper run；有 active policy 时仍要求原审批。无 approval 时 pre-trade safety 不作安全断言，ExecutionGate 保留 `UnqualifiedRuntime` 并持久化 `NoOrder`，不生成 ExecutionPlan，不获取执行 lease，也不写 Broker。PaperCommit 与 Reconcile 返回 NoOutput，Evaluate 仍生成带 `NoOrder` lineage 的 OutcomeSchedule。只有该正式 Paper 入口的成熟 Outcome 才可能供后续校准；PositionPlan 没有 Outcome，不作为校准样本。

显式使用真实模型、行情和 Alpaca Paper 时使用：

```bash
bash scripts/run_core.sh paper
```

`paper` 只在用户显式选择时调用正式 Core 的 Paper scheduler，不修改 `auto_paper`、`manual_paper` 或 Outcome worker 配置。若正式 Core 未配置 Paper 能力、休市或审批/资格条件未满足，服务端给出拒绝或等待原因，不伪造已启动的 Run。旧 `-fakerOnline` / `--faker-online` 与本地模拟 Broker 仍被拒绝。

Paper purpose 不等于 PaperLaunchApproval。没有 active Policy 时走零目标 / NoOrder；Policy、Approval、Decision Validity、Quote Freshness、Risk、Capacity、Compliance、Turnover 和 Exposure 检查均保留。只有原审批和全部 Gate 通过后才可能向 Alpaca Paper 发单。不存在本地模拟 Broker、账户、成交或假开市。

TradingSession 由真实 Clock、Alpaca Calendar 和美东时区共同确定：PreMarket 04:00 至当日开盘，Regular 按交易日历开收盘，AfterHours 收盘至 20:00，Overnight 为下一交易日之前的 20:00 至 04:00，其余为 Closed。正常日开收盘是 09:30 / 16:00；周末、节假日、提前收盘与夏令时以日历及时区为准。`clock.is_open=false` 不再单独表示不能交易。隔夜 trade date 是其后的交易日，不能用 UTC 日期或夜间自然日代替；服务端 scheduler 从真实 Broker Clock/Calendar 获取 broker_session；脚本不自行推断交易日期。[Alpaca 24/5 规则](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api)。

Regular 订单沿用原逻辑；其余可交易时段使用 `limit + day + extended_hours=true`。Overnight 检查资产当次 API 返回的 `overnight_tradable` / `overnight_halted`（兼容 `attributes` 表达），并在提交前再次确认。原 SIP 配置夜间使用 `boats`，原 IEX 配置夜间使用 `overnight` 实时指示行情，不回退到 IEX、不改写报价时间。权限失败会报告失败；指示行情仍接受原有数据质量 Gate，选对 feed 不保证获得执行资格。[最新报价接口](https://docs.alpaca.markets/us/reference/stocklatestquotes-1)。

Closed 会持久化延期任务，等下次可交易时段或 Decision 到期。再次领取时重新刷新 Account / Quote / Clock，再运行原 ExecutionGate；Decision 过期会被拒绝。提交成功的 `accepted/new/partially_filled` 不是最终成交，短轮询超时只保存待成交进度并等待下一次 Reconcile。扩展时段订单不因原来的 60 秒阈值撤改，Regular 行为不变。最终成交、取消、过期或拒绝以 Alpaca 回执为准。

日常双模式脚本复用或启动正式 Core，不暂停 Run、不关闭进程。T0 结束或订单待成交时，正式 Core 与同一 Store 继续承担对账及未来 Outcome；再次检查用原生 `run inspect` / `run replay`，不能重发 PositionPlan POST 来代替恢复。`paper` 同交易 Session 的复用由 Rust session slot 负责。`OutcomeSchedule` 仅表示未来任务已安排，不等于订单成交、T+5 密封或 Policy 激活。Core 退出后需在同一 Store 恢复服务才能继续工作。

提交确认、`replay` 运行状态、bundle 导出完整性、Paper 回执中的订单提交/成交与 Outcome 状态分别核对；accepted/new/partially_filled 仍不是最终成交。查询错误不取消原 Run，不复制 Store。预测分数来自 T0 的逐资产/期限 forecasts，组合指标来自冻结实际敞口，不把 research_plan 建议权重当作已成交持仓。

开发诊断的只读缺口报告（已有 Store，不创建/修改 Store，也不是每次日常运行的前置步骤）：

```bash
cargo run --locked -p akzio-cli -- --config "$HOME/.akzio/config.toml" calibration readiness --store "$HOME/.akzio/store" --min-samples 30
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

首次安装的 canonical Paper 冷启动与 PositionPlan 是两条不同入口：前者用于积累真实 Outcome，须显式选择 `paper` 并满足真实行情/模型配置；后者可在缺 Policy 时产出零正式目标和研究分配，但不产生校准样本。脚本不修改 `auto_paper` 或 Paper 审批，也不自动激活 Policy。`auto_paper=false`、`manual_paper=true`、`outcome_processing=true` 可只手动创建 T0 而持续处理已安排的 Outcome；配置仍须 operator 明确维护。

普通正式 Core 使用 `~/.akzio/store` 与 `~/.akzio/config.toml`；旧隔离 Store 只保留历史读取，不得改作正式运行。`Paper scheduler waiting: broker market is closed` 是正常非交易状态，不视为服务异常。

Shell 的两个提交模式及原生查询/分享安全导出不会删除正式 Core、Store、Run、Commitment、报告或历史状态。需要独立备份/恢复时使用显式的 `store backup/restore` 原生命令，沿用原审批，不把分享安全 bundle 当成数据库备份。

旧 `evidence market-audit` 以 Debug purpose 创建证据图，已与 `evidence preflight` 一并退役，不能绕过私有 Run 日志生成无日志的真实采集或模型请求。离线采集与协议验证使用注入测试 adapters 的 Cargo 测试；真实证据只在获准的正式 PositionPlan/Paper 中取得，不以测试替代真实市场验证。

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
