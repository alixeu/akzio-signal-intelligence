# 06｜用户入口、Observatory、CLI、Core、Debug 与导出操作面

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

核查日期：2026-09-29。对象：`/Users/alixeu/project/akzio-signal-intelligence` **当前工作树**，不是只看 HEAD。HEAD 为 `e4292f09acf5b3798bf16de26718ceb85046a190`。

## 0. 证据与执行边界

- 本次只做源码、文档、Git 状态和文件存在性核查；唯一写入为本报告。没有启动/停止 Core，没有打开 UI，没有执行 CLI 业务命令、HTTP GET/POST、Broker、模型、Doctor 或 fixture，没有读取实际 token、Keychain 或 canonical DB，没有修改源码/配置，也没有提交 Git。下文命令是接口说明，不是执行记录。
- 不标记 `implemented`、`offline-verified`、`real-Paper-verified` 或 `outcome/learning-verified`；准确标签是 **current-worktree source-inspected**。测试代码存在不等于测试已通过。证据标签定义：`docs/development-workflow.md`。
- 已先读 AGENTS、运行时契约第 12 节以及 Debug/Workflow/开发/退休文档；调度器、worker lease、事务恢复只描述操作接口含义，不深入主线负责的实现。约束：`AGENTS.md`；`docs/agent-runtime-contract.md`。
- 核查开始和报告前 `git status --short` 一致：修改了 `.github/workflows/ci.yml`、`AGENTS.md`、`README.md`、`crates/akzio-daemon/src/{http.rs,http_launch.rs,lib.rs,orchestration/health_canary.rs}`、`crates/akzio-store/src/store/debug_bundle.rs`、`docs/{agent-runtime-contract,debug-control,development-workflow,research-protocol-retirement}.md`、`scripts/run_core.py`；另有未跟踪的 `scripts/test_run_core.py`。这些不是本次修改。
- 重点工作树文件 SHA-256：`scripts/run_core.py` = `3abaa1e6b5f1239b92437804bbcd1484ef22340837605387b309f1b116b90f83`；`crates/akzio-daemon/src/http_launch.rs` = `fcb676ff23c3cc6e4bc8e5979a94855405cdf672e16bed419430e9b15ad7346f`；`crates/akzio-daemon/src/http.rs` = `e03735d4638c37974c76a38312cbb222896fe6cf1f6852583b1c3e8d5e6eb6c7`；`crates/akzio-store/src/store/debug_bundle.rs` = `c0e397aa54621df287d80f9e84e2549bd4f2e7df0656c59dcaea42bb7edd48c8`。这些指纹只对应当时读取的工作树快照；后续变化须重新核对源码行为。

## 1. 全流程总图：入口不是权威，显示不是执行

```text
原生 App                         正式运行 Python 客户端
  配置/启动受管 Core                 不启动 Core、不改配置
  └─POST /runs {purpose}────────────┘
                 │
        loopback + token + native 来源检查
                 │
      ┌──────────┴─────────────────────────┐
 PositionPlan                           Paper
 prepare + commit 正式研究图              请求已有 Paper scheduler tick
 Evidence / Research / Reviewer           真实 TradingSession / session slot
 Rust Decision                           同一研究主干 → Decision
 结束；无 Execution/Outcome               ExecutionGate → Commitment/提交
                                       Reconcile → OutcomeSchedule
                                       历史到期 Outcome 独立推进
                 │
           Rust / V2Store 持久权威
                 │
       Observer snapshot/detail + SSE invalidation
                 │
         Swift live projections → 页面

另一条独立入口：
新隔离 Debug Core → prepare（Paused）→ inspect → step / resume
                       ↑ pause / retry-node / abort（受身份和 revision 约束）
                       └─fork / experiment → 新研究身份，不复制 Paper 执行权

旁路只读：
Blueprint / inspection / checkpoint / journal / replay / doctor / export
不产生业务成功，不授予模型、Decision 或 Broker 权限。
```

对应入口证据：`crates/akzio-daemon/src/http_launch.rs`；`scripts/run_core.py`；`crates/akzio-daemon/src/debug.rs`；业务结束边界：`docs/agent-runtime-contract.md`。

## 2. App / Core 启动、配置、凭据与鉴权

### 2.1 正常 App 启动与受管 Core

1. `ObservatoryLauncher.main → ObservatoryApp → AppShell`；主窗口和独立 Run Detail 窗口由 SwiftUI 管理，AppShell `.task` 调用 `bootstrapCore()`。普通构造自动走 live；capture 构造明确 `autoStartsCore:false`，属于 Mock/离屏展示。证据：`apps/Sources/ObservatoryKit/App/ObservatoryLauncher.swift`；`apps/Sources/ObservatoryKit/App/AppShell.swift`。
2. Supervisor 先解析必填配置；缺配置时进入 `needsConfiguration`。二进制优先 `AKZIO_CORE_EXECUTABLE`，再查 Bundle 内 `Contents/MacOS/akzio-core` 和工作区 debug/release 候选。配置优先 `AKZIO_CORE_CONFIG`，否则 `AKZIO_HOME/config.toml` 或默认用户 home 下 `.akzio/config.toml`；Store 为同一 home 下 `store`。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`。
3. 默认配置不存在时，App 调 Rust `observatory-config --config ... init --template ... --store-root ...`，并不是 Swift 自己拼完整 TOML。模板为仓库 `config/akzio.observatory.toml` 或 Bundle 资源。已有默认配置不会自动迁移为新模板。证据：`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`；`crates/akzio-cli/src/cli/observatory_config.rs`。
4. Supervisor 固定使用 `http://127.0.0.1:7342`；端口已响应就报占用，**不会自动附着到现有正式 Core**。随后执行 `akzio-core --config <path> daemon serve`，工作目录为配置父目录，注入 Store 路径、stdin EOF 退出标志、模型与 Paper 凭据等环境；Paper URL 固定为原生 Paper endpoint。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`。因此任意改 TOML `http_addr` 不意味着 App supervisor 会随之改端口。
5. 启动后最多等待 300 秒，200ms 间隔轮询认证 `/ready`，再成功读取 snapshot 才置 `ready`。这是连接就绪，不是 Policy/订单就绪。App 退出调用共享 supervisor `stop()`；受管进程先关闭 stdin/SIGTERM，最多等五秒，再 SIGKILL。重启/保存配置/清理凭据同样属于真实生命周期操作，不能在只读核查时点击。证据：`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryLauncher.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。

### 2.2 凭据不是当前实现中的 Keychain

- **当前读取路径是本地 TOML + 环境占位符，不是 Keychain。** Swift `CoreCredentialStore` 调 Rust CLI `get/set`，`get` 的编辑投影含模型 API key、Alpaca key/secret 等；`set` 将其写回 TOML。源码扫描未发现 `SecItem`/Keychain 存取调用。`import Security` 实际用于 `SecRandomCopyBytes` 生成 daemon token，不能据此讲成 Keychain 存储。证据：`apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`；`crates/akzio-cli/src/cli/observatory_config.rs`；`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`。
- 文件权限隔离不等于加密：home/config 父目录 0700，配置临时文件 0600 后 rename；token 文件为 Store Root 的 `.daemon-token`，App 校验非空、无换行并收紧 0600，首次创建使用 32 字节安全随机数。Rust daemon 首次创建用独占临时文件和 hard-link 发布；**CLI 观察客户端只读已有 token，不创建、不 chmod**。证据：`crates/akzio-cli/src/cli/identity.rs`；`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-cli/src/http_client.rs`。
- `observatory-config get` 不能当成安全的公开诊断输出；它会返回编辑凭据。本次未调用。Core 日志路径与权限管理见 `apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift`、`apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；不能自动把日志/配置加入分享 ZIP。

### 2.3 loopback / token / Origin / CORS 的真实范围

- Core bind 和已有 listener 均校验 `is_loopback()`；通用 `authorize()` 精确匹配 `x-akzio-token`，失败 401。`/health`、`/ready`、Observer、SSE、inspection 等也需要认证，不是公开健康页。证据：`crates/akzio-daemon/src/http.rs`。
- 正式 `POST /runs` 和 Debug mutation 额外拒绝任意 `Origin`，以及 `sec-fetch-site != none`；Debug mutation 还要求 Debug enabled。**不是所有 handler 都有 Origin 检查**：一般 GET 及旧 control 路由主要只调用 token `authorize()`。当前 Router 未安装允许跨源的 CORS layer、未注册统一 OPTIONS 放行。不能讲成“所有接口做同一 Origin 校验”，也不能把 CORS 代替认证。证据：`crates/akzio-daemon/src/http_launch.rs`；`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-daemon/src/http.rs`；`crates/akzio-daemon/src/http_runtime.rs`。
- Rust CLI 和 Python 明确禁用代理及重定向；Swift 为 ephemeral URLSession + 拒绝重定向，初始 endpoint 限定 HTTP loopback，但没有同样显式设置 `no_proxy`。不要把 Rust/Python 的代理策略移植成 Swift 的已证实行为。证据：`crates/akzio-cli/src/http_client.rs`；`scripts/run_core.py`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`。

### 2.4 CLI Core 启动的副作用前置

- `daemon serve` 先加载 Policy、校验 Debug policy 前置，再调用真实模型 capability probes，构造 runtime identity 和 Daemon；**“仅启动 Core”也可能调用模型，不能用于本次只读验收。**生产 worker 默认是 CLI 的 **4**，不是 WorkerPool 类型的默认 2。`auto_paper=true` 以 **30 秒**间隔启动 scheduler；否则只启动 HTTP + workers。证据：`crates/akzio-cli/src/cli/run_commands.rs`。
- `manual_paper=true` 与 auto 共用 Broker/runtime identity/成本/feed/profile 启动校验，但不自动创建新 T0。App 模板 `auto_paper=false, manual_paper=true`。`/ready` 的 `store_scope` 来自 Store 隔离标记，`formal_run_bundle_supported` 仅 canonical 非 Debug 为 true；`ready()` 检查的是当前服务能力，不证明任何 Run 成功。证据：`config/akzio.observatory.toml`；`crates/akzio-cli/src/cli/identity.rs`；`crates/akzio-daemon/src/orchestration/health_canary.rs`。

## 3. 正式 `POST /runs`：PositionPlan / Paper / auto_paper

| 入口/条件 | 实际含义和输出 | 来源文件 |
|---|---|---|
| App 模式选择 | 用户入口仅 `.positionPlan`、`.paper`；请求只编码 purpose，返回 Run ID 后转 Workflow；按钮“已受理”不是完成。 | `apps/Sources/ObservatoryKit/PresentationModels/DomainVocabulary.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift` |
| 请求协议 | `RunLaunchRequest { purpose }` 且 `deny_unknown_fields`；旧通用 workflow/fixture 字段不能混入。非 PositionPlan/Paper 拒绝。 | `crates/akzio-daemon/src/http_launch.rs` |
| 正式 PositionPlan | 服务端取当前美东日期作研究 session，`prepare_position_plan → commit_position_plan`；不使用 Paper slot，也不是让客户端选任意图。每次成功 POST 创建新研究 Run，应避免丢响应后重发。 | `crates/akzio-daemon/src/http_launch.rs`；`scripts/run_core.py` |
| 正式 Paper | 要有 Paper observer 和 runtime identity；用 `AlpacaPaperSessionClock` 调已有 scheduler tick，返回 reservation 对应 Run ID；未获 slot 返回拒绝原因。手动请求不会修改 auto_paper。 | `crates/akzio-daemon/src/http_launch.rs` |
| TradingSession | clock adapter 使用真实 `market_clock` 的 `session.kind != Closed` 和 `session_date`；不是只允许 Regular/`clock.is_open=true`，不是 Python 用本机日期猜交易日。 | `crates/akzio-daemon/src/scheduler.rs` |
| auto_paper | 自动发现新 T0 的独立开关；手动 Paper 使用同一 scheduler/session slot 规则，不能把按钮与 auto 设置等同。 | `crates/akzio-cli/src/cli/run_commands.rs`；`docs/development-workflow.md` |
| Debug Core | 正式 `/runs` 在 Debug enabled 时 409，必须走隔离 prepare；不能切 UI 模式解除 Store 隔离。 | `crates/akzio-daemon/src/http_launch.rs` |
| 缺 active Policy 的正式 PositionPlan | 仍可有 `research_plan.raw/validated`，正式 `Decision.targets` 全零并结束；无 ExecutionVerdict/Commitment/Outcome。 | `docs/agent-runtime-contract.md`；工作树测试断言 `crates/akzio-daemon/src/http_launch.rs`（本次未执行） |
| 缺 Policy 的正式 Paper 冷启动 | 独立的零目标/NoOrder/OutcomeSchedule 路径；不是 PositionPlan 自动切 Paper，不代表具备下单授权或已成交。 | `docs/development-workflow.md`；工作树测试 `crates/akzio-daemon/src/http_launch.rs`（本次未执行） |

时段关闭后的延期、恢复刷新快照和重新 Gate、accepted/new/partially_filled 与最终成交的区别，按运行时契约第 12 节向主线衔接即可，不在此展开执行引擎：`docs/agent-runtime-contract.md`。

## 4. `scripts/run_core.py`：现有 canonical Core 的一次运行客户端

### 4.1 参数与前置检查

- 公开参数只有可选正式配置路径、`--mode position-plan|paper`（默认 position-plan）、`--keep-artifacts`。没有 `--research-only`、`--faker-online`、`-fakerOnline`、启动 daemon、关闭 daemon 或拷贝 Store 的参数。`run()` 内部有默认 1800 秒观察窗口、10 秒轮询，但不是公开 CLI 参数。证据：`scripts/run_core.py`。
- 默认配置为用户 home 下 `.akzio/config.toml`；拒绝 `debug_control=true`、非回环 IP/无端口/URL userinfo 等，要求 `store_root` 为绝对路径；当前进程 `AKZIO_STORE_ROOT` 若与文件不一致也拒绝。只读取既有 token，缺失时提示先启动 Core。证据：`scripts/run_core.py`。
- 创建报告目录和发 Run 前先 GET `/ready`，核对 `status=ok`、`store_scope=canonical`、`formal_run_bundle_supported=true`。Paper 还检查原配置启用了 manual 或 auto。旧 Core/隔离 Core/未 ready 都在付费研究创建前失败。**这不是对远端 Store 路径做独立证明**：依赖配置 token 绑定和 server 返回的 scope，并未比对 server 返还的绝对 Store 路径。证据：`scripts/run_core.py`；`crates/akzio-daemon/src/orchestration/health_canary.rs`。

### 4.2 单次提交、观察和不确定结果

- 仅发送一次 `POST /runs`。5xx、断连、超时、JSON 不完整或缺 Run ID 都可能“已提交但响应丢失”，报告 `unknown_after_post`，**不自动重试**。尤其 PositionPlan 不是客户端幂等请求；先用正式 Run 查询确认。Paper 同 Session 复用属于 Rust，不属于 Python 的重试算法。证据：`scripts/run_core.py`。
- 轮询 `/runs/{id}/replay`，核对 run_id/purpose。PositionPlan 遇 `decision_completed` 或合法终态即可；Paper 遇 Outcome 已 scheduled 时可以结束 T0 观察并报告 `t0_completed_outcome_pending`。Paper 单到 Decision 不算完整；PositionPlan 的 execution-rejection 状态反而视为不匹配。窗口到期报告 pending，不暂停、取消、停止或复制 Core。证据：`scripts/run_core.py`。
- 再读取 Observer detail；观察失败/有界投影漏 artifact 与“没有订单/已完成”严格分开。summary 和终端输出分别列 proposal 审查状态与引用、validated research allocation、正式 Decision targets、ExecutionVerdict、receipt 中的 state/filled_quantity、OutcomeSchedule、numeric seal 和 retrospective status；不会把 receipt_observed 标为 filled。证据：`scripts/run_core.py`。
- 当前 summary 的 `paper_commitment` 在 Observer 白名单限制下可能一直 `not_observed_in_bounded_projection`（详见第 9 节）；它不读 canonical DB 补结论。脚本同样没有把 raw 研究配置完整打印出来：summary 仅摘要 validated，raw/adjustments 应在授权 bundle 的 Decision/DecisionContext 中查。证据：`scripts/run_core.py`；`crates/akzio-daemon/src/observer/broker.rs`。

### 4.3 报告、ZIP 和退出码

- 在仓库 `.akzio/` 下新建 `position-plan-<UTC timestamp>-*` 或 `paper-*` 临时报告目录；umask 077。通过认证 POST `/control/store/export-run-bundle` 让 Core 写 `bundle/`，再核对文件 `EXPORT_STATUS` 与返回 manifest；本地摘要另做秘密值替换。**这是导出文件副作用，不是 Store mutation，也仍属于 POST；本次未调用。**证据：`scripts/run_core.py`。
- ZIP 只收报告根的 JSON 和 Core bundle 文件，拒绝 symlink，校验 checksum 清单的路径、完整文件集合和每个 SHA-256，再以 exclusive 模式建 ZIP 并 `testzip()`。Core bundle 字节不二次替换，否则破坏 hash；不要宣称 Python 又独立做了一遍 Rust bundle 内容脱敏。证据：`scripts/run_core.py`。
- `EXPORT_STATUS=partial` 可能只是按权限省略 raw payload；`_export_issue` 将 missing/corruption/dropped/unknown_after_crash 视为异常，而不是把所有 uncaptured 都当 Run 失败。**T0 完成、导出完整、订单提交、成交、Outcome 成熟为五个不同结论。**证据：`scripts/run_core.py`。

| 退出码 | 当前判断 |
|---|---|
| 0 | 本次 T0 完成，Observer 可用，PositionPlan 的 Decision/research_plan 观察满足要求，导出无异常缺失；允许因授权省略而 partial。 |
| 1 | 未启动、运行失败/取消或提交/观察出错；提交不确定不能读成“未创建 Run”。 |
| 2 | 已完成但观察不全/导出异常，或原拟返回 0 的归档失败。 |
| 3 | Run 仍 pending/等待，脚本不请求取消。 |

退出码证据：`scripts/run_core.py`。ZIP 成功后默认只删除本次临时报告目录，`--keep-artifacts` 保留该目录；归档失败保留目录。任何选项都不删除正式 Run/Store/Core。证据：同文件 `440-451`。

## 5. Observer / SSE / 页面：投影的来源、范围与误读风险

### 5.1 Rust 返回什么，Swift 如何续接

- snapshot schema 2 包括 generated_at、event_cursor、Core/approval、current_run、recent_runs、portfolio、outcome、learning。最近 Run 上限 20，Run events 100，trajectory 200，learning 100；current_run 是最近 workflows 的第一项，**不保证是刚点击启动的 Run**。证据：`crates/akzio-daemon/src/observer.rs`；`crates/akzio-daemon/src/observer/snapshot.rs`。
- Run detail 的 artifacts 从有界 trajectory 提取并按类型白名单反序列化，包含 Claim/Critique/DecisionProposal/DecisionContext/Decision/ExecutionContext/Verdict/Plan/OrderReceipt/Reconciliation/Outcome 等；并非任意 CAS 浏览器，不输出 RawEvidence 或原始 AgentTurn/Tool payload。证据：`crates/akzio-daemon/src/observer/broker.rs`。
- `ObserverSection` 的 available/pending/unavailable、reason、observed_at/data 要一起读；空数据不等于数值 0。尤其 snapshot/portfolio-history 虽为 GET，**服务端可能真实调用 Broker GET 与行情 adapter**，不是纯本地 DB 读；本次禁止 Broker，因此也未请求这些 Observer GET。证据：`crates/akzio-daemon/src/observer.rs`；`crates/akzio-daemon/src/observer/broker.rs`；`crates/akzio-daemon/src/observer/snapshot.rs`。
- 全局 `/v1/observer/events?after=` 每 500ms 看 Store cursor，发送 `invalidate`（带 id/cursor），另发无 durable cursor 的 reasoning-start/delta/end 广播；它不是全局 durable 事件正文流。单 Run `/runs/{id}/events?after=` 每 200ms 分页输出持久事件 `event:akzio`，附带该 Run 的 reasoning 广播。cursor 是观察水位，不是队列 ack/执行许可；服务端看 query `after`，不是 Last-Event-ID。证据：`crates/akzio-daemon/src/http.rs`。
- Swift 每轮先取 snapshot、Debug listing、曲线，再从 snapshot cursor 订阅 SSE；invalidate 重新拉 snapshot，reasoning 仅合并本地临时记录；出错时无旧投影→offline，有旧投影→stale，退避 1→2→4…最多 15 秒。重连不重发 Run POST；取消 SSE 不取消 Run。证据：`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`。
- **没有完整无损重放保证**：全局 invalidation 合并变化，reasoning 广播可以 lag/drop；Swift 未单独处理 `event:error`，落入忽略分支。snapshot 的业务读取与末尾 cursor 读取分处两次 StoreExecutor 调用，中间还读 Broker，不能把它讲成“内容与 watermark 同一 SQLite snapshot”。精确事件追溯用 journal/单 Run SSE/导出。证据：`crates/akzio-daemon/src/http.rs`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift`；`crates/akzio-daemon/src/observer/snapshot.rs`。这是静态机制限制，未复现实例。
- CLI `run events --after` 打开一次流，只解析/打印 data；没有 App 的自动重连循环或自动保存 cursor。有限分页是 `run journal`，不是 `run events`。证据：`crates/akzio-cli/src/http_client.rs`；`crates/akzio-cli/src/cli/dispatch.rs`。

### 5.2 七个页面的讲解地图

页面定义和顺序为 Overview、Workflow、Intelligence、Portfolio、Outcome、Learning、Run Archive；Settings 为覆盖层。证据：`apps/Sources/ObservatoryKit/App/AppRoute.swift`。

| 页面 | 应讲什么 / 不应推断什么 | 来源文件 |
|---|---|---|
| Overview | 当前 Run、agents、health 和最近事件；服务 ready、Policy capable、approval 状态分开。live 无数据不自动退回 Mock。 | `apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift` |
| Workflow | 任务 DAG、task 状态、role/horizon、stage inspector、research audit；另有 Runtime Inspector。纯 Rust 节点显示 Rust/N/A，不虚构模型。普通 inspector 的“模型输出”只列指定模型 artifact，JSON 限 80 行/6000 字符，不等于完整审计导出。 | `apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift` |
| Intelligence | 基于同一 Run 的 trajectory、已验证 artifact、模型公开返回的 deliberation、工具生命周期和临时 reasoning；confidence/alternative score 为模型说明，不是经校准正确率。 | `apps/Sources/ObservatoryKit/LiveData/LiveIntelligenceProjection.swift` |
| Portfolio | Broker 当前账户/持仓；PositionPlan 的 target 来自 `DecisionContext.research_plan.validated`，副标题明确 research target / execution N/A；Paper target 来自 ExecutionPlan。Orders 来自持久 receipt，Fills 来自 Broker activity 投影，两者不同。 | `apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift` |
| Outcome | 显示 T1/T3/T5 已有窗口与完成 session 进度，不用自然日推算；聚合选已有 Outcome 中窗口最多/最近的一份，可能属于历史 Run，而非页头 current_run。服务端 `actual_account_nav_available=false`，比较路径不能叫真实账户 NAV。 | `crates/akzio-daemon/src/observer/runs.rs`；`apps/Sources/ObservatoryKit/LiveData/LiveOutcomeProjection.swift` |
| Learning | 汇总 Outcome/Retrospective/Experience/Evaluation 和 transition，保留 pending/unavailable；候选/impact 展示不表示 Active Lesson 或 DecisionPolicy 已激活。独立数值 seal、叙事有效性、学习资格要分开。 | `crates/akzio-daemon/src/observer/runs.rs`；`apps/Sources/ObservatoryKit/LiveData/LiveLearningProjection.swift`；`CONTEXT.md` |
| Run Archive | 列表是有界 recent_runs 的展示，可选行再 fetch detail；完成率包含合法终态，非盈利率。复制 ID/模型、查看详情、独立窗口不是“重新运行/导出”。 | `apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`；`apps/Sources/ObservatoryKit/Features/Archive/RunPreviewPanel.swift` |

### 5.3 必须逐级区分的业务数据

1. **研究 raw**：`ResearchPlanReview.raw` 是模型原始研究配置，**不是 RawEvidence、模型 raw transport 或隐式思维链**。
2. **研究 validated**：Rust 研究层检查后保留/调整的配置；有 adjustments/status/execution_status/execution_blockers，非零不表示可执行。
3. **正式 targets**：`Decision.targets` / `DecisionContext.target` 是执行侧目标，可在研究 validated 非零时仍为零。
4. **ExecutionVerdict**：Rust wire 为 `{"verdict":"accepted",...}` 或 `{"verdict":"no_order","no_order":...}`；是 Gate 结果，不是订单。
5. **Commitment / submit / receipt**：Commitment 的持久化先于请求；receipt observed 仅证明有回执，要看 state/filled_quantity。
6. **Fill / Reconcile**：accepted/new/partial 不等于最终 filled；只读账户/成交 activity 也不能替代 Store 中正式 Reconcile 状态。
7. **OutcomeSchedule / sealed Outcome / narrative / learning**：分别是安排、成熟数值窗口、模型叙事和学习处理；T0 结束可以仍等未来 session。

字段证据：`crates/akzio-domain/src/decision.rs`；`crates/akzio-domain/src/execution/effects.rs`；提交与后评估边界：`docs/agent-runtime-contract.md`。页面并未为以上每一层提供独立完整面板；精确 raw/validated/targets 应看授权 artifact/bundle，不能只看图标。

## 6. 独立 Debug：准备、检查、控制、重试与新实验

### 6.1 隔离和 Policy 前置

- 操作规范要求新 Store 位于仓库 `.akzio/`，`debug_control=true, auto_paper=false`；不能打开用户 canonical Store。代码在 Store 层拒绝 `$HOME/.akzio/store`，首次标记须 Run 数为零，隔离标记永久，普通 Core 重开拒绝。**“必须放仓库 .akzio”是操作规则；当前 Store guard 不是全路径必须属于仓库 .akzio 的校验器。**证据：`docs/debug-control.md`；`crates/akzio-store/src/store/debug.rs`；`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- App 通过同时设置 `AKZIO_DEBUG_ENDPOINT` 和 `AKZIO_DEBUG_STORE_ROOT` 连接已有隔离 Core；只读取所指 token，查 Debug listing 要求 enabled 和 store_identity，直接进入 Workflow，不启动默认受管 Core。连接失败保留 offline。证据：`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。
- 真实 `daemon serve` 在模型 probe 前校验冻结 Policy；有 Policy 时须 decision-capable/status/input hash/artifact 和 Synthesizer Contract 匹配。没有 active head 的非 auto 研究模式有例外。针对**真实 Debug PositionPlan**，无 Policy 的身份 `research_only_without_policy()` 只允许手动研究，禁止 resume 或直接 step Decision。不能把这个限制外推成“所有正式 PositionPlan 都不能 Decision”，也不能把它无差别表述成所有 Debug purpose 的同一判定。证据：`crates/akzio-cli/src/cli/run_commands.rs`；`crates/akzio-domain/src/debug.rs`；`crates/akzio-daemon/src/debug.rs`。
- 三种 preflight 不同：`debug preflight` 默认真实 capability probe，`--resolve-only` 仅解析路由；`calibration preflight --scratch` 没有模型调用，但会 `Store::open(scratch)`，不是零文件写入的纯只读命令；`calibration readiness --store` 用既有 Store 只读检查。不能根据 preflight 名称就执行。本次均未调用。证据：`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-cli/src/cli/calibration.rs`。

### 6.2 prepare → inspect

- `POST /v1/debug/runs` 接受 session_key/purpose/paper_allowed，拒绝未知字段；purpose 默认 Paper，CLI 为 `paper|position-plan`，JSON 为 `paper|position_plan`。PositionPlan 不得 `paper_allowed=true`。prepare 发布图、Need、冻结身份和 paused 控制 head，**不执行首节点**。Paper 同 session 已存在时校验 runtime identity/Broker policy，一致才返回原 session；PositionPlan 不占 Paper slot。证据：`crates/akzio-daemon/src/debug.rs`；`docs/debug-control.md`。
- GET listing 返回 enabled/store_identity 和最近 100 workflows 中的 Debug sessions；GET inspect 可带 task/attempt，展示节点、尝试、预算、产物引用、事件、Acceptance、inspection 和 allowed_actions。局部 task/attempt 查询不会假装完整研究进度。inspect 不重跑节点；RawEvidence 被排除，其他 JSON 递归脱敏，损坏 JSON 可能显示 Null。证据：`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-store/src/store/debug.rs`。
- 新身份包含 store identity、run purpose、real/fixture、broker policy、learning scope、code/runtime identity、policy hash/artifact、contracts、dataset 与 parent lineage；界面不能就地修改这些授权维度。App prepare 总是 `paper_allowed:false`，仅 CLI 有显式 `--paper-allowed`。证据：`crates/akzio-daemon/src/debug.rs`；`apps/Sources/ObservatoryKit/LiveData/DebugPayloads.swift`；`crates/akzio-cli/src/cli/debug_commands.rs`。

### 6.3 控制矩阵（不是新增执行引擎）

共用 `POST /v1/debug/runs/{id}/control`，请求为 `{action, expected_revision, task_id?}`；token/native/Debug 校验在 HTTP，revision 与身份在 Store。普通 canonical Run 不因此获得 Debug 权。证据：`crates/akzio-daemon/src/http_debug.rs`；`crates/akzio-domain/src/debug.rs`。

| 操作 | 前置/结果 | 当前用户入口 |
|---|---|---|
| step | Paused、无在途任务、精确 TaskId、依赖/due/identity 合法，授权一个调度单位；成功/失败/Deferred 后暂停，不自动跑 sibling 或无限重试。 | CLI `debug step ... --task ...`；App 节点单步按钮。 |
| pause | 阻止新 claim，清未消费 permit；在途自然收尾，Running→PauseRequested→Paused，不杀执行 future。 | CLI 和 App。 |
| resume | 同身份、Paused→Running/continuous；缺 Policy 的真实 Debug PositionPlan 拒绝；不是清空成功产物/预算。 | CLI 和 App。 |
| retry-node | Paused 且原重试策略仍允许、Task 已可领取/due；原 Task 新 attempt，不把 succeeded 节点重跑，不扩充预算。 | CLI 和 App。 |
| abort | 必须先 pause 并排空，仍有 running 返回 `pause_and_drain_before_abort`；控制状态 Aborted，不替代订单撤销/业务恢复。 | **领域 enum + HTTP API 有；CLI 无 `debug abort`，App 无按钮。** |
| run_until / run-until | 当前 DebugAction、CLI、App 均没有；resume 是 continuous，不是“精确运行至某节点”。 | **不存在，不能编造命令。** |

动作代码：`crates/akzio-store/src/store/debug.rs`；调度单位结束语义：`docs/debug-control.md`；CLI 全枚举：`crates/akzio-cli/src/cli/debug_commands.rs`；App 按钮：`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。

- CLI 先 inspect 取 revision，再只 POST 一次；step/retry 默认等 300 秒、最多 3600 秒，以 250ms 轮询 inspect。超时不撤回 permit、不再授予。App `debugBusy` 串行化请求，CAS 冲突后只刷新、不猜 revision。证据：`crates/akzio-cli/src/cli/debug_commands.rs`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。
- runtime identity 改变仍可 inspect，只保留 pause/abort；相关节点不可 step/retry。Outcome processing 关闭/adapter 不可用时相关 step/retry 返回不可用；这些不是“模型失败”的同義词。证据：`crates/akzio-daemon/src/debug.rs`。

### 6.4 fork / experiment / acceptance / Broker

- `debug fork --task <successful_task>` 要有真实成功 attempt/output；`debug experiment` 不带 task，用父 WorkflowGraph 作 lineage。都调用 fork API，要求非空 reason，新 experiment_id；相同 ID、父/Task/reason 一致可返回既有结果，冲突拒绝。重采 governed evidence，不把父成功结果复制成新成功。**Paper parent 明确拒绝 fork/experiment，须 fresh prepare 与 Session reservation。**证据：`crates/akzio-daemon/src/debug.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`。
- App 显示“成功阶段 fork / new experiment”，但按钮只按连接/busy/reason 禁用，并没有完全复制服务端 succeeded/Paper 检查；可见按钮不是可执行授权。证据：`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。
- Acceptance POST 只追加 typed checks/evidence，path Run ID 必须一致且为 DebugSession；记录成功不改变 workflow/Gate。UI 单独列 Business / Test / NOT_RUN，不能用业务 succeeded 自动生成 PASS。证据：`crates/akzio-daemon/src/http_debug.rs`；`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`。
- Broker 默认 Forbidden，显式 PaperAllowed 只是解除 Debug 额外限制，仍需审批/全部 Gate/Commitment；SimulatedOnly 仅历史解码，执行禁止。学习 scope 默认 Isolated，不能激活 canonical policy/Active Lesson。证据：`crates/akzio-daemon/src/debug.rs`；`crates/akzio-domain/src/debug.rs`；`crates/akzio-execution/src/paper_dispatch.rs`；`docs/debug-control.md`。

## 7. Blueprint / inspection / checkpoint / journal / replay / Doctor

| 能力 | 入口与可见内容 | 边界与证据 |
|---|---|---|
| Blueprint 预览 | GET `/v1/workflows/blueprint?purpose=position_plan|paper|shadow`；CLI `workflow blueprint --purpose ... --format json|mermaid`。 | 当前已安装定义的同一编译器输出，不创建 Run/Need/模型。`crates/akzio-daemon/src/http_runtime.rs`；`crates/akzio-cli/src/main.rs` |
| Run inspection | GET `/v1/observer/runs/{id}/inspection`；CLI `run inspect`。 | 持久 graph、blueprint、共享 control、checkpoint、recovery、allowed_actions；普通 Run 的动作列表为空。`crates/akzio-store/src/store/run_control.rs` |
| checkpoint | CLI `run checkpoint` 实际复用 inspection，仅打印 checkpoint 字段；无独立 checkpoint POST/恢复命令。 | 不是进程 dump，不能脱离 Store 重启副作用，也不保证 external exactly-once。`crates/akzio-cli/src/cli/dispatch.rs`；`docs/workflow-runtime.md` |
| journal | GET `/v1/observer/runs/{id}/journal?after=0&limit=100`，支持 task_id/attempt_id；after≥0，limit 1..500。 | 返回 events、next_cursor、has_more，以及 artifact/source refs；没有原始模型/工具 payload。`crates/akzio-daemon/src/http_runtime.rs`；`crates/akzio-store/src/store/run_control.rs` |
| Runtime Inspector UI | Workflow 面板区分“本次冻结定义”与“当前预览”，展示 recovery/checkpoint，手动每页 50 条 journal。 | 切 Run 清本地 cursor，结果按 Run ID 防过期写回；读取不推进控制。`apps/Sources/ObservatoryKit/Features/Workflow/RuntimeInspectorPanel.swift`；`apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift` |
| replay | GET `/runs/{id}/replay` / CLI `run replay`。 | 重建/核验持久图和 lifecycle，返回 status/revision/task_count/terminal_task_count/cursor/cancel_requested；terminal 包括 Failed/Cancelled/Skipped，不能当成功节点数。不是重新调用模型。`crates/akzio-daemon/src/orchestration/control.rs` |
| trajectory / retrospectives | GET `/runs/{id}/trajectory`、`/retrospectives`；对应 CLI。 | 读取持久审计和回顾，不安排补跑；不是 raw-model dump。`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/cli/dispatch.rs` |
| Doctor | GET `/control/store/doctor` / CLI `store doctor`。 | 认证 HTTP maintenance；检查 SQLite/BLOB/hash/外键/事件/来源/控制 checkpoint，不修复、不证明预测正确或 Paper 成交。`crates/akzio-daemon/src/http.rs`；`crates/akzio-store/src/store/doctor.rs` |

Blueprint definition hash 仅标识控制结构，不保证模型逐字重现；完整 provenance 还需 graph CAS、RuntimeManifest、Contract/Prompt、模型记录与证据快照。历史 Run 可无 checkpoint，不伪造回填。证据：`docs/workflow-runtime.md`。

## 8. 导出与其余 CLI / HTTP 操作面

### 8.1 三种“导出”不能混为一个安全等级

| 类别 | 当前入口 | 输出/权限 |
|---|---|---|
| 正式分享安全 Run bundle | 新 POST `/control/store/export-run-bundle`；当前 Python 使用它。 | 只允许非隔离、非 DebugSession 的 Paper/PositionPlan；formal raw_access 强制 false，不授予 raw 权限。HTTP 新入口尚无独立 `store export-run-bundle` CLI 子命令。 |
| Debug 诊断 bundle | POST `/control/store/export-debug-bundle`；CLI `debug export-bundle <id> --out <new-dir>`，可 `--store <root>` 离线。 | 隔离 session/store/run/purpose/learning-scope 匹配才可包含脱敏 provider detail；历史 RunPurpose::Debug 无 session 有显式兼容读取分支。普通 formal Run 经这一 exporter 仍识别为 formal-safe，不因此获得 raw 权限。 |
| 旧 `store export-run` | POST `/control/store/export-run`；CLI `store export-run <id> <target> [--include-raw-model]`。 | 导出到独立 SQLite payload 库与清单；raw-model flag 有身份检查，但默认隐藏的只有 AgentTurn/ToolCall/ToolResult，**不能等同新分享安全 bundle**，可能包含 RawEvidence/embedded blobs。不要放入正式脚本 ZIP。 |

证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-store/src/store/debug_bundle.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-cli/src/main.rs`；`crates/akzio-store/src/store/blob.rs`；`crates/akzio-store/src/store/free_paper_checks.rs`。

- Debug 离线 `--store` 用 `Store::open_existing`，不启 daemon/模型/迁移/修复；但 CLI main 仍先加载常规配置再 dispatch，因此**不是完全 config-free**；本次不能选 canonical Store。证据：`crates/akzio-cli/src/cli/main.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`。
- 分享 bundle 从单一 SQLite Deferred 读事务提取事实，释放事务/连接后写目标文件；目标须不存在且不能位于 Store Root 内，允许创建父目录，后段错误可能留下部分目录。导出不调用模型、不抓新证据、不重跑 Decision。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- 主要文件：workflow/tasks_attempts/model_routes；timeline/llm_calls/tools/rust_decisions JSONL；llm_transcript/rust_decisions/SUMMARY/README Markdown；evidence_status/research_review/context_coverage/draft_submit_coverage/stage_acceptance/decision_matrix/policies_and_risk/failures_and_missing JSON；artifacts/ 和 artifact_index；manifest、EXPORT_STATUS、checksums.sha256。文件名 `draft_submit_coverage` 仍可用于历史/Outcome，不意味着新研究恢复 Draft。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- 各 artifact 分开记录 source hash 与 export payload hash；脱敏后两者可能不同。raw/跨 Run 未授权 payload 写 omitted marker，不能补造；跨 Run payload 须匹配授权 dataset/parent refs。导出对嵌套敏感字段、headers/cookie/key/secret、encrypted continuation 和可识别 credential text 递归替换；它不是密码学上保证发现任意秘密字符串的检测器。证据：`crates/akzio-store/src/store/debug_bundle.rs`。
- `partial` 可来自授权省略，也可来自 crash unknown、缺 blob/记录，必须看 manifest 分类。未配对 AgentTurnStarted 记 unknown_after_crash，不假定失败/成功。transcript 只含已持久化 provider-visible 材料，不含 hidden chain-of-thought。证据：`crates/akzio-store/src/store/debug_bundle.rs`。

### 8.2 CLI 和 HTTP 操作清单（均未执行）

- CLI 默认配置是仓库相对 `config/akzio.toml`，**不是** App/Python 的用户配置；讲解运行命令时应显式指定目标配置。CLI 主命令含 Workflow/Debug/ObservatoryConfig/Daemon/Run/Store/Canary/ModelQualification/Calibration/Evidence；部分本地命令在主函数先分派。证据：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/main.rs`。
- **只读 HTTP 操作**：health/ready；observer snapshot/run/history/events；blueprint/inspection/journal；run replay/trajectory/retrospectives/events；store doctor/inventory/metrics/executor/alerts/session/release-evidence/events/lessons；canary status；debug list/inspect。注意 snapshot/history 可能 Broker GET，Doctor 为完整 Store 检查；HTTP-only `/control/store/executor`、`/control/store/events/{id}` 不代表已有同名 CLI。证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/main.rs`。
- **状态变更 HTTP 操作**：正式 run launch、run cancel/retry/repair-narrative、freeze/unfreeze、paper-approval、canary stage/resume、lesson add/transition、Debug prepare/control/fork/acceptance。**文件操作 POST**：backup/restore/三类 export；restore 尤其不是观察。证据：同一 Router `crates/akzio-daemon/src/http.rs`，handlers `499-590,760-846,877-884,1209-1226`。
- 普通 `run retry` 是为合法终态 PositionPlan 创建**新 Run**，不是 Debug retry-node/resume；Paper 拒绝 operator retry，Debug Core 要求专用 Debug 命令，退休 workflow 先拒绝。`run cancel` 是业务取消请求；不等于 Debug abort 或撤销已完成订单。证据：`crates/akzio-daemon/src/orchestration/control.rs`。
- `run repair-narrative` 只入队 sealed T5 narrative repair，返回 task_id，不代表 repair 完成或 Policy 激活。`store approve-paper` 参数含 session/operator/reason/notional/valid_hours/qualification_report，还会真实模型 capability probe；purpose=paper 不替代此批准。证据：`crates/akzio-daemon/src/http.rs`；`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/observatory_config.rs`。
- Calibration readiness/inspect 与 collect/build/activate 不同，build 候选不自动激活；`set-risk-limits` 要 operator 明确输入。Evidence market-audit 会创建新审计输出和调用行情 GET，debug verify-fixture 会创建隔离 Store 并执行离线图，均不是本次允许的“只读源码核查”动作。证据：`docs/development-workflow.md`；`crates/akzio-cli/src/cli/main.rs`。

## 9. 文档漂移、已删入口与当前显示缺口

以下是**本次静态核查发现**，不是 UI 复现或安全漏洞实测；不在本任务中修复。

| 项目 | 当前证据与讲解修正 |
|---|---|
| CONTEXT 版本过旧 | `CONTEXT.md` 仍写 Store17 / Contract67 / Prompt37 / candidate68；当前 `crates/akzio-store/src/store/prelude.rs` 为 Store18，`crates/akzio-research/src/agent/catalogue.rs` 为研究69 / bundle38 / candidate70。运行时契约已有当前版本：`docs/agent-runtime-contract.md`。 |
| 退休清单版本更旧 | `docs/research-protocol-retirement.md` 仍写65/36/66；这是历史协议整合节点，不是当前活动版本。当前版本同上，不改写历史冻结身份。 |
| CONTEXT 12k input 描述过旧 | `CONTEXT.md` 写模型调用仍限12k。`versioned_contract_budget` 的 Outcome 历史默认仍12k，但新 Run 的 `default_agent_budget` 把 input 设为1,000,000，并有配置覆盖；不能混用历史 Contract budget、新 Run budget、Context grant 和 provider 单请求上限。证据 `crates/akzio-domain/src/budget.rs`。 |
| “九节点”漂移 | `docs/research-protocol-retirement.md`、`README.md`、CLI help 注释 `crates/akzio-cli/src/cli/debug_commands.rs` 仍称九节点；当前 fixture 成功条件明确 `nodes.len()==21`（同文件392-394）；开发 workflow 47 行写默认21，runtime 文档7行写 PositionPlan21/Paper25，修订配置可改变数量。 |
| README 旧 fixture-controller | `README.md` 仍建议加 `--fixture-controller`；当前 CLI 无该参数，且专门有拒绝旧参数测试 `crates/akzio-cli/src/cli/debug_commands.rs`。不要照旧命令执行。 |
| `/runs` 退休文字易误读 | `docs/research-protocol-retirement.md` 的“通用 POST /runs 创建路由删除”只指旧通用提交协议；当前 Router96行和 `http_launch.rs` 明确保留正式 purpose-only `/runs`。 |
| “所有 HTTP 同一 Origin 检查”过度概括 | `docs/workflow-runtime.md` 的表述不能理解成每个 GET 都验 Origin。`crates/akzio-daemon/src/http_runtime.rs` 只调用 token authorize；native 拒绝集中在正式 launch 与 Debug mutation。 |
| preflight “只读”有文件边界例外 | `crates/akzio-cli/src/cli/calibration.rs` 注释称只读，但 `Store::open(scratch)` 是可创建/迁移路径；没有模型调用不等于零写盘。应选 readiness 的 open_existing 做真正既有 Store 读取；本次不调用任何一种。 |
| App 当前没有 Keychain 存储路径 | 当前 TOML get/set 和 `.daemon-token` 机制见第2节。不能因文件名 CoreCredentials 或 import Security 宣称 Keychain。 |
| App 不是通用 canonical attach 客户端 | Supervisor 固定7342且已占用就拒绝；只有显式 Debug endpoint/root 走 attach 分支。Python 才按配置连接已有 canonical Core。证据 `apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift`；`apps/Sources/ObservatoryKit/App/ObservatoryStore.swift`。 |
| Portfolio Accepted 映射不匹配 | `apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift` 检查 JSON 顶层是否含 key `"accepted"`；Rust 当前是内部标签 `verdict:"accepted"`，见 `crates/akzio-domain/src/execution/effects.rs`。按该 typed payload 形状推导，Accepted 会落 `.noOrder`；缺 verdict 也落 `.noOrder`。这是源码可确定的映射缺口，不能把该 UI 标签当 Gate 权威。 |
| 空订单表文案过强 | `apps/Sources/ObservatoryKit/Features/Portfolio/OrdersFillsTable.swift` 仅凭 orders.isEmpty 就显示“No executable order was produced”；它可能是尚未回执、bounded projection 缺失或 N/A。且 fills 表嵌套在有 orders 的分支。讲解必须回到具体 verdict/receipt/availability。 |
| 脚本 Commitment 可见性缺口 | `scripts/run_core.py` 试图从 Observer artifacts 找 ExecutionCommitment；当前白名单 `crates/akzio-daemon/src/observer/broker.rs` 没有该 kind。因此正常 detail 不会提供它；summary 的 not_observed 不是“未持久化”。应在授权 bundle 的相应 artifact/journal 引用查证。 |
| Outcome/Learning 不必与当前 Run 同一条 | `crates/akzio-daemon/src/observer/snapshot.rs` current_run 取最新；`crates/akzio-daemon/src/observer/runs.rs` 独立跨近期 artifact 选择。页头 T0 与历史 T+N 并列是允许的，不应给后者强加前者 Run ID。 |
| Debug 文档与 UI/CLI覆盖不同 | `docs/debug-control.md` 称 Abort 为 Core 内部动作；当前 HTTP 反序列化支持 Abort（domain/debug.rs118-131 + http_debug.rs112-129），但没有 CLI/App 对应按钮。run_until 完全不在当前接口中；不要把用户期望列成已实现。 |
| 旧导出不是新安全 bundle | `store export-run` 可写 SQLite payload，默认过滤不含 RawEvidence；新 formal bundle 才强制 raw access=false/敏感脱敏。见第8节，不得因同有“export”字样而混用。 |

### 9.1 已删除/不可活动创建的入口

- CLI `run fixture-debug`、`run paper-dry-run`、`run submit`、`debug prepare --fixture-controller` 不在活动命令中，退休解析测试覆盖拒绝；`read_range_probe`、`fixture_controller` 不在严格 Debug prepare/fork 请求结构。证据：`crates/akzio-cli/src/main.rs`；`crates/akzio-cli/src/cli/debug_commands.rs`；`crates/akzio-daemon/src/debug.rs`。
- 当前脚本参数拒绝 `--research-only`、`-fakerOnline`、`--faker-online`；文件存在性检查确认旧 `scripts/debug_goal_run.zsh`、`scripts/paper_canary_run.zsh`、`config/task2-fixture.toml` 不存在。已删除文件没有现行源码可定位；以现有参数定义 `scripts/run_core.py` 和删除说明 `docs/development-workflow.md`、`docs/research-protocol-retirement.md` 为现行证据。
- 历史 Planner/PaperDryRun/旧 AgentTurn/旧预算类型继续读、显示、导出、replay/doctor，不恢复执行。Store 退休判定包括 paper_dry_run、research.planner、研究 Contract<65；返回 `legacy_workflow_retired`。这是退休识别阈值，不是“当前活动 Contract 为65”。证据：`crates/akzio-store/src/store/workflow/helpers.rs`；`docs/research-protocol-retirement.md`。
- App `PaperDryRun`/Planner/SimulatedOnly 标签仍有历史解码用途，不在 userLaunchModes；Mock 界面仍存在且有明确演示 banner，**Mock UI 不等于已删除的 faker Broker 模拟**。证据：`apps/Sources/ObservatoryKit/PresentationModels/DomainVocabulary.swift`；`apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift`；`apps/Sources/ObservatoryKit/App/AppShell.swift`。

## 10. 可供主线直接使用的中文讲解顺序

1. **先分客户端和权威**：App 管受管进程、Python 连接现有 Core、CLI 提交控制/检查；Rust/V2Store 决定状态与权限。（第1–4节）
2. **说明启动不是无副作用**：配置/文件 token、loopback/native 检查、模型 probe；只读调查不能为了截图启动真实 App/Core。（第2节）
3. **再分正式两模式**：PositionPlan 到 Decision；Paper 加执行与未来 Outcome；manual 是能力，auto 是新 T0 自动调度开关；Paper purpose 不等于交易 approval。（第3节）
4. **逐级解释结果**：raw 研究配置→validated 研究配置→正式 targets→ExecutionVerdict→Commitment/提交→fill/reconcile→OutcomeSchedule→数值 seal→叙事/学习。（第5.3节）
5. **带着证据看 UI**：snapshot 是有界投影，SSE 是观察/失效通知；看 Run ID、artifact、available/pending/unavailable、cursor 和 lifecycle，不只看 badge/空表。（第5节、第9节）
6. **最后讲隔离 Debug 与导出**：prepare 暂停、inspect、精确 step、pause/resume/retry、非 Paper fork；说明不存在 run_until 和未暴露的 abort；分享安全 bundle 与旧 SQLite 导出分开。（第6–8节）

### 交付状态

本报告完成了当前请求的入口/显示/操作面只读映射。不存在本次真实 Run ID、模型调用、Paper 提交/成交、Outcome 或 UI 验收结果。没有运行构建/测试/fixture/Doctor，因而不宣称它们通过；源码测试仅作为预期行为的旁证。后续若主线引用本报告，应保留“源码行为”“静态映射缺口”“实际运行事实”的三层区分。

文档静态检查包括 `git diff --check`、来源文件存在性与链接检查；这些检查不证明源码行为在更新后仍相同。原始报告曾存于忽略目录；当前 `docs/reports/` 副本需独立做格式与链接检查。原始核查时四个重点源文件 SHA-256 曾复查一致。这些检查只证明报告定位与工作树边界，不构成程序行为验证。
