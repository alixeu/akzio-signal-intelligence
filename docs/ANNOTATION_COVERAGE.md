# 中文注释覆盖登记

> 这是一次已完成任务的历史清单与当时的验证记录；其中旧 `scripts/` 路径不代表当前入口。现行构建、运行和文档检查命令以[开发 Workflow](development-workflow.md)为准，下方历史证据不回写。

## 记录规则

本表以任务开始时的 Git 清单为基线：开始时工作区 clean，共 **473 个跟踪路径**；忽略目录 `target/`、`apps/.build/` 不纳入。`docs/RUST_LEARNING_GUIDE.md` 与本表在任务开始前已经存在于 Git，本任务对其进行更新，并不把它们计作新文件。每个跟踪路径在下表单独登记；主 Agent 负责清单、跨模块语义、专用文档、差异审计和最终验证。

状态含义（仅使用本次要求的状态类别）：

- **已完成**：文件已完整阅读，函数/方法/trait 默认实现/宏/闭包/异步任务/测试或配置分支已复核，文件内具备符合本任务的合法普通中文说明。记录会区分“本次新增/完善”与“基线已有充分注释、逐项复核后避免重复”。
- **受限文件已补外部说明**：文件是运行时 Prompt、模板、SQL、历史 fixture 或不支持安全注释的结构化输入；原文保持不变，本表记录用途、消费方、关联入口和不能修改的原因。
- **排除**：第三方、二进制、纯生成文件、锁文件、视觉资源、版本控制元数据或普通说明文档；排除项不计入已注释数量。
- **未完成**：尚未达到上述标准；最终报告必须保留原因。

本仓库在任务开始前已有逐文件注释覆盖表，所以逐文件行区分“本次新增/完善”与“基线已有充分注释”。对后者，全文/函数逐项复核不是唯一依据：状态还取决于该文件当前是否保有足够的中文导读和内部注释。源码身份哈希输入清单高于逐文件表中的历史注释标记：其中路径本次一律属于“受限文件已补外部说明”，不得因早期行写了“已完成”而新增源码字节。

普通注释不得改变表达式、控制流、签名、类型、依赖、SQL、Prompt、fixture、配置值、日志或测试断言；不新增或改写 `///`、`//!`、`#[doc]`。任务中的“基线已有充分注释”不是本次新增数量；本表另行列出源码身份哈希、嵌入资源及固定输入等受限路径。

## 基线与结果摘要

| 范围 | 基线数量 | 结果 |
| --- | ---: | --- |
| Rust 源文件（含 inline tests、examples、build.rs） | 268 | 40 个显式 Prompt/Contract/Topology hash source 按专项表受限；其余 228 路径全部全文/函数复核并补注或保留基线充分注释 |
| Swift 源及 `apps/Package.swift` | 123 | 122 个 App Swift 源 + 1 个 SwiftPM manifest，全部逐文件复核；存在的充分基线注释未重复堆砌 |
| Shell/Python 脚本 | 10 | 8 个 Shell + 2 个 Python，均全文复核并补充/完善合法中文注释 |
| TOML/JSON/YAML/Plist 模板 | 23 | 16 TOML 与 4 YAML 已审注释；2 JSON 和 1 Plist 模板受限并外部说明 |
| Markdown | 26 | 两份专用文档更新；7 个运行时 Prompt + 1 个历史 fixture 受限；16 个普通说明/规则文档排除 |
| 图片/SVG/ICNS | 19 | 视觉/二进制资源，按其实际用途登记为排除，不计入注释覆盖 |
| 其他元数据与 SQL fixture | 4 | `.gitattributes`、`.gitignore`、`Cargo.lock` 排除；受限 SQL fixture 外部说明 |
| **基线跟踪路径总计** | **473** | 两份专用文档已在基线内；忽略的构建产物不计 |

与仓库中既有登记表逐项核对时发现 26 条历史路径不在本次 Git 基线：19 个旧 Rust 测试文件、2 个旧 Swift 测试文件和 5 个旧 `scripts/tests` 文件。它们没有被本任务删除或恢复，只从当前“逐文件基线清单”中移除，且不计入覆盖数。当前清单与 `git ls-files` 双向核对无缺失。

## 负责人和批次（按时间交接；并行写入互斥）

| 负责人 | 主要范围 | 实际复核重点 |
| --- | --- | --- |
| 本次负责人 | 不重叠写入范围 | 主要复核 |
| 主 Agent | 两份专用文档、根 Cargo/toolchain、config、CI/skill YAML、顶层脚本、完整清单与最终验收 | 工作区保护、跨 crate 语义、注释-only 差异、哈希/模板/结构化输入边界 |
| Worker A / Faraday | `akzio-domain`、`akzio-context` | Schema、授权来源闭包、Manifest/ReadGrant/projection |
| Worker B / Chandrasekhar | `akzio-store`（接续并复核先前 Newton 流中断时留下的进度） | SQLite/CAS、事务、lease、workflow、学习与历史兼容；59 个可安全注释文件已完成，2 个历史 fixture 外部登记 |
| Worker C / Ampere | `akzio-execution`、`akzio-learning` | Decision/Execution/Paper、Commitment/Reconcile、Outcome/资格 |
| Worker D / Sagan | `akzio-ingest`、`akzio-model`、`akzio-research`、`akzio-runtime` | Evidence/Responses、Contract/Prompt、Future/恢复；编辑 safe adapter/model 文件，identity-hash 与 PromptSource inputs 外部说明 |
| 补充批次 / Sartre | `akzio-research/src/agent/{projection,structured}.rs`、`fixture.rs`、`quality.rs` | 四个非显式 Prompt/Contract/topology byte-input 源文件；include!/fixture/实验恢复边界 |
| Worker E / James | `akzio-daemon`、`akzio-cli` | HTTP/SSE、Scheduler/Lease、Debug、Worker、CLI/build.rs |
| UI-1 / Dirac | App 入口、App Shell、LiveData | SwiftUI/Observation、认证只读投影、App 状态流 |
| UI-2 / Schrödinger | DesignSystem、Motion | token、Environment/Binding、Canvas/Shape 与动画策略 |
| UI-3 / Dalton | Features | 各页面输入投影、空值/状态分支、用户动作与显示边界 |
| UI-4 / Laplace | MockData、PresentationModels、SwiftPM manifest、App 构建/签名脚本 | fixture 与真实行情区别、投影转换、脚本副作用和退出码 |

初始阶段最多五个 worker 并行，Rust/Swift 文件分批互斥。`Sartre` 是 `Sagan` 结束后的顺序补充批次，只修改 research 中四个非显式身份-hash 输入、且此前缺少完整文件导读的路径；两者没有同时写同一文件。`Newton` 的 Store 流在完成前断开，`Chandrasekhar` 在旧 worker 退出后接续并复核已留下的部分进度；没有同时写 Store 文件。逐文件表中的 Worker 字母/旧作者名是先前基线登记标签，本次实际负责人按上方目录范围和这些顺序交接规则确定。

## 特殊文件边界

- `.github/workflows/ci.yml` 与 `.agents/skills/*/agents/openai.yaml` 可用独立 YAML `#` 注释，已补中文说明；这四份 YAML 不再登记为受限文件。
- `config/research-quality-cases.json` 由 `scripts/verify_research_quality.py` 读取稳定 case ID；`apps/Resources/AppIcon.icon/icon.json` 是 Icon Composer 生成的 `.icon` bundle 内部元数据，仓库打包脚本复制整个 bundle 而不解析该 JSON。JSON 不支持注释，均保持字节不变并由本表外部说明。
- `apps/Resources/Info.plist.in` 是 `apps/Scripts/build_app.sh` 渲染的 Plist 模板。尽管 XML 语法支持注释，模板会被打包脚本直接消费；本任务保守保持模板逐字节不变，以免影响生成输出。
- `crates/akzio-research/src/agent/prompts/**` 与 `crates/akzio-ingest/src/prompts/source_verifier.md` 是 `include_str!`/Prompt registry 读取的运行时文本；不得把 Markdown 当普通文档改写。
- `crates/akzio-store/src/store/fixtures/retired-history.md` 是 `docs/research-protocol-retirement.md` 引用的冻结离线历史来源说明；`retired_paper_dry_run.sql` 的文件头标明只供 frozen offline history、生产代码不加载。二者逐文件阅读，但不改正文、SQL、哈希输入或 BLOB 字节。
- `Cargo.lock`、`.gitattributes`、`.gitignore`、PNG/SVG/ICNS 是锁文件、版本控制元数据、视觉或二进制资源，不插入注释。
- README、AGENTS、CONTEXT、普通 `docs/*.md` 与技能说明是说明性文档，按规则排除；仅两份专用文档在本任务中更新。

图标资源的逐组用途按 `apps/Resources/AppIcon/README.md` 核实：`apps/Resources/AppIcon.icns` 是当前手工打包链复制并由 Info.plist 关联的兼容图标；`apps/Resources/AppIcon.icon/Assets/01-primary-mark.svg` 与 `02-signal-accent.svg` 是 Icon Composer 多层 bundle 的 SVG 图层，`icon.json` 由 Icon Composer 生成，不手写；`apps/Resources/AppIcon/sources/layers/*.svg` 是导入 Icon Composer 的两个源图层；`sources/candidates/*.svg` 仅供设计候选评审；`previews/*.svg`、`previews/*.png` 与 `previews/sizes/*.png` 是视觉/小尺寸检查样本。它们是绘图数据/预览，不是可安全插入源码注释的程序；`.icon` 内部结构和严格 JSON 均保持原样。

### 源码身份哈希输入（受限文件逐项清单）

`akzio-research::prompt_component_hash`、`contract_component_hash`、`akzio-runtime::topology_component_hash` 对明确列出的源码/Prompt bytes 计算稳定身份。普通注释也会改变这些输入字节，因此这些路径**只读并通过本表提供外部导读，不在源码中插注释**。这些限制不代表历史 CAS/Contract 被改写；本任务没有运行 Core 或持久化新的身份。根 `crates/akzio-cli/build.rs::git_revision` 还会把整个工作树 diff 纳入下一次构建的 `code_revision`，所以后续 Core 会看到新的源码修订身份；本次不生成或持久化 RuntimeIdentity。

| 路径 | 身份消费关系 | 受限原因与外部说明 |
| --- | --- | --- |
| `crates/akzio-domain/src/contract.rs` | `contract_component_hash` | 静态授权/Contract bytes；注释会改变输入哈希。职责与字段关系见本表“领域/Contract”逐文件行及学习指南 §7、§12。 |
| `crates/akzio-domain/src/research_review.rs` | `contract_component_hash` | 评审与 revision 绑定 schema bytes；注释会改变输入哈希。职责与 provenance 见本表对应行及运行时契约 §5。 |
| `crates/akzio-domain/src/workflow_definition.rs` | `topology_component_hash` | 冻结 NodeSpec/WorkflowBlueprint 输入 bytes；注释会改变拓扑身份。职责与 lowering 边界见本表对应行及运行时契约 §4。 |
| `crates/akzio-research/src/agent.rs` | Prompt 与 Contract component | Agent 协议总装配输入；源码注释会改变对应组件身份。外部索引见学习指南 §4、§6、§11。 |
| `crates/akzio-research/src/agent/budget.rs` | Prompt 与 Contract component | Agent 调用预算与策略输入；身份需保持原始 bytes。外部索引见学习指南 §6、§8、§11。 |
| `crates/akzio-research/src/agent/catalogue.rs` | Prompt 与 Contract component | 冻结 Contract/catalogue 组装输入；外部索引见运行时契约 §5 与学习指南 §7。 |
| `crates/akzio-research/src/agent/errors_catalogue.rs` | Prompt 与 Contract component | 输出错误反馈/拒绝语义输入；外部索引见学习指南 §8、§11。 |
| `crates/akzio-research/src/agent/helpers.rs` | Prompt 与 Contract component | 研究提交辅助规则输入；外部索引见运行时契约 §5 与本表对应行。 |
| `crates/akzio-research/src/agent/model_types.rs` | Prompt 与 Contract component | 模型 turn/telemetry 类型及转换输入；外部索引见学习指南 §3、§6、§11。 |
| `crates/akzio-research/src/agent/recovery.rs` | Prompt component | 恢复事件与状态折叠输入；外部索引见学习指南 §6、§11。 |
| `crates/akzio-research/src/agent/runtime_core.rs` | Prompt 与 Contract component | AgentRuntime 运行/Submit 协议输入；外部索引见运行时契约 §5、§11。 |
| `crates/akzio-research/src/agent/runtime_helpers.rs` | Prompt 与 Contract component | Store/Context 辅助流程输入；外部索引见学习指南 §2、§6。 |
| `crates/akzio-research/src/agent/runtime_run.rs` | Prompt 与 Contract component | 请求、预算、验证、持久化阶段协议输入；外部索引见学习指南 §6、§8、§11。 |
| `crates/akzio-research/src/agent/runtime_type.rs` | Prompt 与 Contract component | AgentRuntime 共享状态与错误类型输入；外部索引见学习指南 §5、§6。 |
| `crates/akzio-research/src/agent/schemas.rs` | Prompt 与 Contract component，且 PromptSource 登记 | wire Schema bytes；外部索引见运行时契约 §5 与学习指南 §4、§7。 |
| `crates/akzio-research/src/agent/tools.rs` | Prompt 与 Contract component，且 PromptSource 登记 | 工具 contract/guidance bytes；外部索引见运行时契约 §5 与学习指南 §6、§7。 |
| `crates/akzio-research/src/agent/validation.rs` | Prompt 与 Contract component | Rust 结构化提交验证输入；外部索引见运行时契约 §5 与学习指南 §4、§8。 |
| `crates/akzio-research/src/lib.rs` | Prompt 与 Contract component | component hash 算法和 crate 哈希汇总入口本身；只在本表解释其路径/字节序列。 |
| `crates/akzio-research/src/prompt_registry.rs` | Prompt 与 Contract component | PromptSource ID/path/bytes 清单；外部说明见本表运行时 Prompt 边界及学习指南 §7。 |
| `crates/akzio-research/src/agent/proposal_review.rs` | Contract component 与 PromptSource | Review Schema/绑定代码作为受控身份输入；外部索引见运行时契约 §5、§7。 |
| `crates/akzio-research/src/agent/prompts/mod.rs` | PromptSource `research.renderer` | role 文档选择器的源码 bytes 参与 Prompt identity；外部说明见运行时 Prompt 消费者记录。 |
| `crates/akzio-research/src/agent/prompts/phases.rs` | PromptSource `research.phases` | Draft/Submit renderer 源码 bytes 参与 Prompt identity；职责见运行时契约 §5 与学习指南 §7。 |
| `crates/akzio-context/src/context_broker/guidance.rs` | PromptSource `context.guidance` | Context 解释文本源码 bytes 被纳入 Prompt identity；本次撤销新增注释，使用外部说明。 |
| `crates/akzio-ingest/src/prompts.rs` | PromptSource `ingest.acquisition` | governed acquisition 意图的静态 guidance bytes；外部索引见本表 Context/Prompt 边界。 |
| `crates/akzio-ingest/src/prompts/source_verifier.md` | PromptSource + `include_str!` | 来源核验运行时 Prompt；正文不可编辑，调用入口见 `crates/akzio-ingest/src/news.rs`。 |
| `crates/akzio-ingest/examples/native_web_preflight.rs` | PromptSource `ingest.preflight_example` | 该受限 preflight 示例源码 bytes 是 Prompt 身份组件；不为注释变化重写固定输入。 |
| `crates/akzio-model/src/model_client/probe_prompts.rs` | PromptSource `model.probes` | model capability probe 的固定提示文本 bytes；消费者见 `capability_probe.rs`。 |
| `crates/akzio-research/src/agent/prompts/shared.md` | PromptSource `research.governance` | 模型可见的共享治理 Prompt，正文逐字节保持。 |
| `crates/akzio-research/src/agent/prompts/roles/analyst.md` | PromptSource `research.analyst` | Analyst 角色运行时 Prompt，正文逐字节保持。 |
| `crates/akzio-research/src/agent/prompts/roles/critic.md` | PromptSource `research.critic` | Critic 角色运行时 Prompt，正文逐字节保持。 |
| `crates/akzio-research/src/agent/prompts/roles/synthesizer.md` | PromptSource `research.synthesizer` | Synthesizer 角色运行时 Prompt，正文逐字节保持。 |
| `crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md` | PromptSource `research.proposal_reviewer` | Reviewer 角色运行时 Prompt，正文逐字节保持。 |
| `crates/akzio-research/src/agent/prompts/roles/outcome.md` | PromptSource `research.outcome` | Outcome 两阶段协议 Prompt，正文与冻结哈希保持。 |
| `crates/akzio-runtime/src/lib.rs` | `topology_component_hash` | Runtime 拓扑哈希算法/组件清单本身；外部索引见运行时契约 §4、§11。 |
| `crates/akzio-runtime/src/runtime.rs` | `topology_component_hash` | Runtime 顶层状态/类型实现 bytes；外部索引见学习指南 §1、§6。 |
| `crates/akzio-runtime/src/runtime/catalogue.rs` | `topology_component_hash` | 节点 recipe catalogue bytes；外部索引见运行时契约 §4、§5。 |
| `crates/akzio-runtime/src/runtime/compilation.rs` | `topology_component_hash` | 拓扑编译入口 bytes；外部索引见运行时契约 §4。 |
| `crates/akzio-runtime/src/runtime/compilation/evidence.rs` | `topology_component_hash` | Evidence 节点 lowering/检查 bytes；外部索引见运行时契约 §6。 |
| `crates/akzio-runtime/src/runtime/compilation/helpers.rs` | `topology_component_hash` | 编译辅助规则 bytes；外部索引见运行时契约 §4。 |
| `crates/akzio-runtime/src/runtime/compilation/lowering.rs` | `topology_component_hash` | WorkflowProposal 到冻结图的转换 bytes；职责见运行时契约 §4、§5。 |
| `crates/akzio-runtime/src/runtime/compilation/validation.rs` | `topology_component_hash` | 编译后图约束与依赖验证 bytes；职责见运行时契约 §4。 |
| `crates/akzio-runtime/src/runtime/node.rs` | `topology_component_hash` | NodeExecutor 边界 bytes；Rust `Future` 语义索引见学习指南 §3、§5、§6。 |
| `crates/akzio-runtime/src/runtime/reducer.rs` | `topology_component_hash` | 事件归约/状态恢复 bytes；外部索引见学习指南 §6。 |
| `crates/akzio-runtime/src/runtime/replay.rs` | `topology_component_hash` | Replay 与历史投影校验 bytes；外部索引见运行时契约 §11。 |
| `crates/akzio-runtime/src/runtime/store_executor.rs` | `topology_component_hash` | 同步 Store 调度/并发上限 bytes；Rust 并发索引见学习指南 §5、§6。 |
| `crates/akzio-runtime/src/runtime/task.rs` | `topology_component_hash` | claim/lease/heartbeat/cancel/finish 协议 bytes；外部索引见学习指南 §6。 |
| `crates/akzio-runtime/src/runtime/workflow.rs` | `topology_component_hash` | Run/Workflow 持久化与查询入口 bytes；外部索引见运行时契约 §4、§8。 |

## 发现但未修改的行为/疑点

以下是代码复核发现的可验证行为或待确认风险，不作为本任务修复项；没有因此改表达式、策略、UI 字符串或并发实现。

| 路径/符号 | 代码中可确认的行为 | 边界/后续核验建议 |
| --- | --- | --- |
| `crates/akzio-learning/src/evaluation/runtime_setup.rs::evaluate`、`evaluate_with_lease`；`evaluation/materialization.rs::evaluate_frozen` | 两个便捷入口向统一实现传 `None` draft；当前冻结 T+5 materializer 要求有效 `RetrospectiveDraft`，所以该路径返回 `InvalidMaterialization`。Daemon 有 draft 时走另一入口，无 draft 时走 Rust-only seal 分支。 | 搜索范围内未发现外部调用；需判断便捷入口是否应弃用/给出明确错误，而不是本次修改。 |
| `crates/akzio-execution/src/decision_gate/decide.rs::decide` | selected Artifact 的 `Store::artifact` 重读错误经 `.ok()` 转成缺项；后续仅取不晚于 `input.now` 的最大 `observed_at`，空结果回退到 `input.now`。 | 记录的是 best-effort 过滤行为；读取失败是否应传播属于决策 cutoff 风险，未改。 |
| `crates/akzio-execution/src/calibration.rs::fit_forecast_calibration` | 每个概率 bin 累加 `probability_sum` 与 `brier_sum` 后显式丢弃；最终 asset/horizon Brier 再遍历全部样本计算。 | 当前输出不使用这两个 bin 局部和；仅记录可见的重复计算/死数据流，未判断是否为缺陷。 |
| `crates/akzio-execution/src/paper.rs::PaperCredentials`、`AlpacaPaper` | 两者派生 `Debug`；`AlpacaPaper` 持有含 `secret_key` 的 credentials。 | 若未来把整个值格式化到日志会暴露凭据；本次对范围调用点搜索未找到实际格式化使用。 |
| `apps/Sources/ObservatoryKit/Features/Portfolio/OrdersFillsTable.swift` | 空回执表分支可显示 “No executable order”，而 Accepted verdict 与后续回执投影是分开的状态。 | 不应仅凭表格为空断言 Rust 已给出 `NoOrder`；保留现有 UI 行为。 |
| `apps/Sources/ObservatoryKit/DesignSystem/Components/GaugeRing.swift` | `RiskGauge` 的数值显示条件调用 `PpmFormatter.signPrefix(0)`；该 helper 对零返回 `±`，所以当前条件分支总选择格式化数值。 | 记录恒真条件；未更改表达式/文案。`ProgressRing` 也在每次新 `progress >= 1` 时递增 `completionTick`，重复完成值会重复触发。 |
| `apps/Sources/ObservatoryKit/Features/Workflow/DagLayout.swift`、`DesignSystem/MaterialTokens.swift` | 两处 `nonisolated(unsafe)` 静态 Set/Dictionary 没有自身锁或原子保护。 | 当前检索到的访问路径未做并发压力验证；只记录共享可变状态边界。 |
| `apps/Sources/ObservatoryKit/MockData/SeededGenerator.swift`、`MockData/LearningFixtures.swift` | double range 实现用 `[0,1)` 缩放 `ClosedRange`；fixture session date 仅跳过周末，且日期构造使用强制解包。 | 属于可复现 UI fixture，不是交易日历或真实市场事实；上界/节假日/trap 语义未改。 |
| `apps/Sources/ObservatoryKit/App/ObservatoryStore.swift::connectObserver`、`LiveData/RustCoreSupervisor.swift::start`、`LiveData/ObserverClient.swift` 的 URL 构造 | Observer 循环在 `guard let self` 后可能强持有至循环退出；start 在 `await` 处让出 MainActor，多个调用可能重叠预检；固定 endpoint 的 `URLComponents` 有强制解包。 | 只记录取消/并发/trap 边界；未运行 App 并发验证，也未改代码。 |
| `crates/akzio-research/src/quality.rs::verify`、修订路径 `CountedModel` | 报告的 real 标记在 verify 入口按 `client.is_some()` 决定；一个修订 wrapper 内部固定 `real: true`。CLI candidate 使用 real client，但直接调用 API 时传 fixture client 可能仍记入 real-call ledger。 | API 调用者的 client 来源是否需单独类型化/校验，留待独立审查；未改实验状态逻辑。 |
| `crates/akzio-research/src/agent/structured.rs::semantic_changed_paths` | 对象递归比较使用 `Null` 作为缺失键的比较默认值，因而“字段缺失”与“字段存在且为 JSON null”可能被视为相同。 | 仅记录修订差异审计语义，未改 comparator。 |
| `crates/akzio-daemon/src/worker.rs`、`scheduler/serve.rs`、`evidence.rs` | shutdown 由服务循环在当前 handler/tick 返回后观察；EvidenceNeed 多项逐条写入，不是跨资源原子事务，先完成的写入不会因后续失败自动回滚。 | handler 与已提交 evidence 的部分成功必须按 Store 记录审计；本任务没有执行 daemon 或修改事务边界。 |
| `crates/akzio-store/src/store/blob.rs::export_run`、`backup_to`、`restore_from` | 导出/备份目标的 containment 检查使用路径前缀逻辑但未 canonicalize symlink；restore 的前置判断侧重目标是否已存在。 | 这段实现尚未证明符号链接解析后的路径仍在预期根目录内；只记录为路径安全核验项，不据此断言可利用或修改实现。 |

## 逐文件登记（按任务开始时 Git 基线复核）

| 路径 | 职责/入口 | 历史责任标签（本次 owner 按上方路径范围） | 全文/函数复核 | 文件级导读 | 内部关键逻辑与知识 | 状态/原因（哈希输入专项表优先） |
| --- | --- | --- | --- | --- | --- | --- |
| .agents/skills/code-review/SKILL.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| .agents/skills/code-review/agents/openai.yaml | Code Review skill UI 元数据 | 主 Agent | 全文读取；字段/消费界面已核对 | 已补文件职责 `#` 注释 | 说明 display name 与描述只影响 skill UI metadata | **已完成**：只加独立 YAML 注释，字段未变 |
| .agents/skills/deep-research/SKILL.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| .agents/skills/deep-research/agents/openai.yaml | Deep Research skill UI 元数据 | 主 Agent | 全文读取；默认 Prompt/界面字段已核对 | 已补文件职责 `#` 注释 | 说明 default_prompt 是配置文本，不是业务输入 | **已完成**：只加独立 YAML 注释，字段未变 |
| .agents/skills/diagnosing-bugs/SKILL.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| .agents/skills/diagnosing-bugs/agents/openai.yaml | Diagnosing Bugs skill UI 元数据 | 主 Agent | 全文读取；界面字段已核对 | 已补文件职责 `#` 注释 | 说明 UI metadata 与 Skill 正文的边界 | **已完成**：只加独立 YAML 注释，字段未变 |
| .agents/skills/diagnosing-bugs/scripts/hitl-loop.template.sh | 人工参与复现脚本模板：收集步骤和用户回答，不启动业务 | 主 Agent | 全文阅读；脚本入口/参数/交互函数已复核 | 已补合法文件导读 | 已补脚本输入、退出和非业务证明边界 | **已完成**：只新增 shell 普通注释 |
| .gitattributes | 仓库元数据或结构化输入 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| .github/workflows/ci.yml | GitHub Actions CI 自动化配置 | 主 Agent | 全文读取；各 job 的实际命令已核对 | 已补文件导读 `#` 注释 | 说明 rust、audit、sbom 三类工作流分工 | **已完成**：只加独立 YAML 注释，job/step 字段未变 |
| .gitignore | 仓库元数据或结构化输入 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| AGENTS.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| CONTEXT.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| Cargo.lock | 依赖锁定文件 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| Cargo.toml | Cargo workspace/package manifest | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| README.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| apps/Package.swift | SwiftPM package topology | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Resources/AppIcon.icns | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon.icon/Assets/01-primary-mark.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon.icon/Assets/02-signal-accent.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon.icon/icon.json | Icon Composer 保存的 `.icon` bundle 内部元数据 | 主 Agent | 全文读取；两组 layer 资源和平台范围已核对 | 外部说明 | 仓库脚本只复制完整 `.icon` bundle，不直接解析该 JSON；JSON 本身无注释语法 | **受限文件已补外部说明**：不手改工具生成结构，保持 JSON 字节 |
| apps/Resources/AppIcon/README.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| apps/Resources/AppIcon/previews/AppIcon-appearances.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/AppIcon-appearances.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/AppIcon-candidates.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/AppIcon-candidates.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/AppIcon-flat.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/AppIcon-flat.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/sizes/1024.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/sizes/128.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/sizes/16.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/sizes/32.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/previews/sizes/64.png | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/sources/candidates/01-signal-confluence.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/sources/candidates/02-decision-aperture.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/sources/candidates/03-clarity-prism.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/sources/layers/01-primary-mark.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/AppIcon/sources/layers/02-signal-accent.svg | 视觉/二进制资源 | 主 Agent | 不适用 | 不适用 | 不适用 | **排除**：锁文件、视觉资源、二进制或版本控制元数据，不安全插入注释 |
| apps/Resources/Info.plist.in | macOS App 的 Info.plist 生成模板 | 主 Agent | 全文读取；已追踪 `apps/Scripts/build_app.sh` 的模板消费 | 外部说明 | `sed` 替换 `__VERSION__`/`__BUILD__` 后写入 Bundle；不在生成模板中插注释 | **受限文件已补外部说明**：模板会进入打包输出，原字节保持不变 |
| apps/Scripts/build_app.sh | App 构建/截图/签名脚本：build_app.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Scripts/capture_screens.sh | App 构建/截图/签名脚本：capture_screens.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Scripts/create_dmg.sh | App 构建/截图/签名脚本：create_dmg.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Scripts/sign_app.sh | App 构建/截图/签名脚本：sign_app.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/AkzioObservatory/main.swift | Observatory：main.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/AppRoute.swift | Observatory：App/AppRoute.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/AppShell.swift | Observatory：App/AppShell.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/CaptureCommand.swift | Observatory：App/CaptureCommand.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/ObservatoryLauncher.swift | Observatory：App/ObservatoryLauncher.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/ObservatoryStore.swift | Observatory：App/ObservatoryStore.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/PageSidebar.swift | Observatory：App/PageSidebar.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/RunStatusBar.swift | Observatory：App/RunStatusBar.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/App/WindowChromeConfigurator.swift | Observatory：App/WindowChromeConfigurator.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/ColorTokens.swift | Observatory：DesignSystem/ColorTokens.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/Chip.swift | Observatory：DesignSystem/Components/Chip.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/GaugeRing.swift | Observatory：DesignSystem/Components/GaugeRing.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/MetricCard.swift | Observatory：DesignSystem/Components/MetricCard.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/MiniSparkline.swift | Observatory：DesignSystem/Components/MiniSparkline.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/PressableStyle.swift | Observatory：DesignSystem/Components/PressableStyle.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/StatusBadge.swift | Observatory：DesignSystem/Components/StatusBadge.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Components/TooltipPopover.swift | Observatory：DesignSystem/Components/TooltipPopover.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/LayoutTokens.swift | Observatory：DesignSystem/LayoutTokens.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/Localization.swift | Observatory：DesignSystem/Localization.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/MaterialTokens.swift | Observatory：DesignSystem/MaterialTokens.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/PpmFormatter.swift | Observatory：DesignSystem/PpmFormatter.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/StatusSemantics.swift | Observatory：DesignSystem/StatusSemantics.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/DesignSystem/TypographyTokens.swift | Observatory：DesignSystem/TypographyTokens.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Archive/ArchiveFilterBar.swift | Observatory：Features/Archive/ArchiveFilterBar.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Archive/DetachedRunWindow.swift | Observatory：Features/Archive/DetachedRunWindow.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Archive/RunArchivePage.swift | Observatory：Features/Archive/RunArchivePage.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Archive/RunPreviewPanel.swift | Observatory：Features/Archive/RunPreviewPanel.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Archive/RunTable.swift | Observatory：Features/Archive/RunTable.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/CollapsibleInspector.swift | Observatory：Features/CollapsibleInspector.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Intelligence/IntelligencePage.swift | Observatory：Features/Intelligence/IntelligencePage.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Intelligence/IntensityOrbitCanvas.swift | Observatory：Features/Intelligence/IntensityOrbitCanvas.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Learning/ExperienceTimelineCanvas.swift | Observatory：Features/Learning/ExperienceTimelineCanvas.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Learning/ImpactSummaryCard.swift | Observatory：Features/Learning/ImpactSummaryCard.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Learning/LearningPage.swift | Observatory：Features/Learning/LearningPage.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Learning/PolicyTransitionTrack.swift | Observatory：Features/Learning/PolicyTransitionTrack.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Learning/RetrospectiveCardStack.swift | Observatory：Features/Learning/RetrospectiveCardStack.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/LiveUnavailablePage.swift | Observatory：Features/LiveUnavailablePage.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Outcome/HorizonRingView.swift | Observatory：Features/Outcome/HorizonRingView.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Outcome/OutcomeMetricGrid.swift | Observatory：Features/Outcome/OutcomeMetricGrid.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Outcome/OutcomePage.swift | Observatory：Features/Outcome/OutcomePage.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Outcome/OutcomeSummaryCard.swift | Observatory：Features/Outcome/OutcomeSummaryCard.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/ActiveAgentsList.swift | Observatory：Features/Overview/ActiveAgentsList.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/HealthSnapshotView.swift | Observatory：Features/Overview/HealthSnapshotView.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/KpiStripView.swift | Observatory：Features/Overview/KpiStripView.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/LatestEventCard.swift | Observatory：Features/Overview/LatestEventCard.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/OverviewPage.swift | Observatory：Features/Overview/OverviewPage.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/SignalUniverseCanvas.swift | Observatory：Features/Overview/SignalUniverseCanvas.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Overview/SignalUniverseLayout.swift | Observatory：Features/Overview/SignalUniverseLayout.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/PageScaffold.swift | Observatory：Features/PageScaffold.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/PageScroll.swift | Observatory：Features/PageScroll.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/AllocationBars.swift | Observatory：Features/Portfolio/AllocationBars.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/AllocationFlowCanvas.swift | Observatory：Features/Portfolio/AllocationFlowCanvas.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/EquityCurveChart.swift | Observatory：Features/Portfolio/EquityCurveChart.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/OrdersFillsTable.swift | Observatory：Features/Portfolio/OrdersFillsTable.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/PortfolioPage.swift | Observatory：Features/Portfolio/PortfolioPage.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/PositionCardView.swift | Observatory：Features/Portfolio/PositionCardView.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Portfolio/RiskPanel.swift | Observatory：Features/Portfolio/RiskPanel.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/AccessibilitySection.swift | Observatory：Features/Settings/AccessibilitySection.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/AppearanceSection.swift | Observatory：Features/Settings/AppearanceSection.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/CoreSettingsSection.swift | Observatory：Features/Settings/CoreSettingsSection.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/ModelDisplaySection.swift | Observatory：Features/Settings/ModelDisplaySection.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/MotionSection.swift | Observatory：Features/Settings/MotionSection.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/SettingsControls.swift | Observatory：Features/Settings/SettingsControls.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Settings/SettingsLayer.swift | Observatory：Features/Settings/SettingsLayer.swift | Worker 1 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/CanvasToolbar.swift | Observatory：Features/Workflow/CanvasToolbar.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/DagLayout.swift | Observatory：Features/Workflow/DagLayout.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/DebugWorkflowPanel.swift | Observatory：Features/Workflow/DebugWorkflowPanel.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/ObservedMarkdown.swift | Observatory：Features/Workflow/ObservedMarkdown.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/RuntimeInspectorPanel.swift | Observatory：Features/Workflow/RuntimeInspectorPanel.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/StageInspectorPanel.swift | Observatory：Features/Workflow/StageInspectorPanel.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/WorkflowDagCanvas.swift | Observatory：Features/Workflow/WorkflowDagCanvas.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/WorkflowPage.swift | Observatory：Features/Workflow/WorkflowPage.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Features/Workflow/WorkflowProgressStrip.swift | Observatory：Features/Workflow/WorkflowProgressStrip.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/CoreCredentials.swift | Observatory：LiveData/CoreCredentials.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/DebugPayloads.swift | Observatory：LiveData/DebugPayloads.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/JSONValue.swift | Observatory：LiveData/JSONValue.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LiveIntelligenceProjection.swift | Observatory：LiveData/LiveIntelligenceProjection.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LiveLearningProjection.swift | Observatory：LiveData/LiveLearningProjection.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LiveOutcomeProjection.swift | Observatory：LiveData/LiveOutcomeProjection.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LivePortfolioProjection.swift | Observatory：LiveData/LivePortfolioProjection.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LiveProjection.swift | Observatory：LiveData/LiveProjection.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/LiveProjectionHelpers.swift | Observatory：LiveData/LiveProjectionHelpers.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/ObserverClient.swift | Observatory：LiveData/ObserverClient.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/ObserverCorePayloads.swift | Observatory：LiveData/ObserverCorePayloads.swift | Worker 2 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/ObserverOutcomePayloads.swift | Observatory：LiveData/ObserverOutcomePayloads.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/ResearchAuditPayload.swift | Observatory：LiveData/ResearchAuditPayload.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/RuntimeInspectionPayloads.swift | Observatory：LiveData/RuntimeInspectionPayloads.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/LiveData/RustCoreSupervisor.swift | Observatory：LiveData/RustCoreSupervisor.swift | Worker 4 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/ArchiveFixtures.swift | Observatory：MockData/ArchiveFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/CouncilFixtures.swift | Observatory：MockData/CouncilFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/CurveFixtures.swift | Observatory：MockData/CurveFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/EventFixtures.swift | Observatory：MockData/EventFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/LearningFixtures.swift | Observatory：MockData/LearningFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/MockScenario.swift | Observatory：MockData/MockScenario.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/OutcomeFixtures.swift | Observatory：MockData/OutcomeFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/PortfolioFixtures.swift | Observatory：MockData/PortfolioFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/ScenarioLibrary.swift | Observatory：MockData/ScenarioLibrary.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/SeededGenerator.swift | Observatory：MockData/SeededGenerator.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/MockData/WorkflowFixtures.swift | Observatory：MockData/WorkflowFixtures.swift | Worker 5 | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/CanvasRenderPolicy.swift | Observatory：Motion/CanvasRenderPolicy.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/MotionTokens.swift | Observatory：Motion/MotionTokens.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/NodeAnimation.swift | Observatory：Motion/NodeAnimation.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/RouteTransitionTable.swift | Observatory：Motion/RouteTransitionTable.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/SharedElementID.swift | Observatory：Motion/SharedElementID.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/TransitionCoordinator.swift | Observatory：Motion/TransitionCoordinator.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/Motion/ValueAnimation.swift | Observatory：Motion/ValueAnimation.swift | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/AgentPresentation.swift | Observatory：PresentationModels/AgentPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/AgentVocabulary.swift | Observatory：PresentationModels/AgentVocabulary.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/DomainVocabulary.swift | Observatory：PresentationModels/DomainVocabulary.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/ExecutionVocabulary.swift | Observatory：PresentationModels/ExecutionVocabulary.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/LearningModels.swift | Observatory：PresentationModels/LearningModels.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/LearningPresentation.swift | Observatory：PresentationModels/LearningPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/LearningVocabulary.swift | Observatory：PresentationModels/LearningVocabulary.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/ObservatorySnapshot.swift | Observatory：PresentationModels/ObservatorySnapshot.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/OutcomePresentation.swift | Observatory：PresentationModels/OutcomePresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/PortfolioModels.swift | Observatory：PresentationModels/PortfolioModels.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/PortfolioPresentation.swift | Observatory：PresentationModels/PortfolioPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/RunPresentation.swift | Observatory：PresentationModels/RunPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/SettingsPresentation.swift | Observatory：PresentationModels/SettingsPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/WorkflowDisplay.swift | Observatory：PresentationModels/WorkflowDisplay.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/WorkflowLayout.swift | Observatory：PresentationModels/WorkflowLayout.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| apps/Sources/ObservatoryKit/PresentationModels/WorkflowPresentation.swift | Observatory：PresentationModels/WorkflowPresentation.swift | Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| config/akzio.observatory.toml | 运行配置或固定 fixture：akzio.observatory.toml | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| config/akzio.toml | 运行配置或固定 fixture：akzio.toml | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| config/debug-controller-fixture.toml | 运行配置或固定 fixture：debug-controller-fixture.toml | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| config/research-quality-cases.json | 离线研究质量验收的稳定 case catalog | 主 Agent | 全文读取；`scripts/verify_research_quality.py` 按 id/crate/test 消费 | 外部说明 | 严格 JSON 无注释；稳定 ID、测试名及 case 数是验收输入 | **受限文件已补外部说明**：不插入 JSON 不支持的注释，也不改 fixture |
| crates/akzio-cli/Cargo.toml | akzio-cli：Cargo.toml | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/build.rs | akzio-cli：build.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/calibration.rs | akzio-cli：src/cli/calibration.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/debug_commands.rs | akzio-cli：src/cli/debug_commands.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/dispatch.rs | akzio-cli：src/cli/dispatch.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/identity.rs | akzio-cli：src/cli/identity.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/main.rs | akzio-cli：src/cli/main.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/model_qualification.rs | akzio-cli：src/cli/model_qualification.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/observatory_config.rs | akzio-cli：src/cli/observatory_config.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/cli/run_commands.rs | akzio-cli：src/cli/run_commands.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/http_client.rs | akzio-cli：src/http_client.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/lesson.rs | akzio-cli：src/lesson.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-cli/src/main.rs | akzio-cli：src/main.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/Cargo.toml | akzio-context：Cargo.toml | Worker A | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明 Context/Store 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-context/src/broker.rs | akzio-context：src/broker.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/coverage.rs | akzio-context：src/context_broker/coverage.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/grants.rs | akzio-context：src/context_broker/grants.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/guidance.rs | Context projection 的模型可见提示常量 | Worker A / Faraday | 全文阅读；PromptSource 消费与身份哈希已核对 | 外部说明；原有注释保留 | 按角色拼接提示，不授予读取权；注释变更会改变 `prompt_component_hash` | **受限文件已补外部说明**：PromptSource `context.guidance` 输入字节固定 |
| crates/akzio-context/src/context_broker/helpers.rs | akzio-context：src/context_broker/helpers.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/manifest.rs | akzio-context：src/context_broker/manifest.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/materialization.rs | akzio-context：src/context_broker/materialization.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/policy.rs | akzio-context：src/context_broker/policy.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/reads.rs | akzio-context：src/context_broker/reads.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/context_broker/selection.rs | akzio-context：src/context_broker/selection.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/lib.rs | akzio-context：src/lib.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-context/src/selection.rs | akzio-context：src/selection.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/Cargo.toml | akzio-daemon：Cargo.toml | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/agent_session.rs | akzio-daemon：src/application/agent_session.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/evidence_acquisition.rs | akzio-daemon：src/application/evidence_acquisition.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/maintenance.rs | akzio-daemon：src/application/maintenance.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/mod.rs | akzio-daemon：src/application/mod.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/outcome_sealing.rs | akzio-daemon：src/application/outcome_sealing.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/paper_execution.rs | akzio-daemon：src/application/paper_execution.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/research_loop.rs | akzio-daemon：src/application/research_loop.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/research_run.rs | akzio-daemon：src/application/research_run.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/application/research_supplement.rs | akzio-daemon：src/application/research_supplement.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/debug.rs | akzio-daemon：src/debug.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/dispatch.rs | akzio-daemon：src/dispatch.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/evidence.rs | akzio-daemon：src/evidence.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/http.rs | akzio-daemon：src/http.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/http_debug.rs | akzio-daemon：src/http_debug.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/http_launch.rs | akzio-daemon：src/http_launch.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/http_runtime.rs | akzio-daemon：src/http_runtime.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/lib.rs | akzio-daemon：src/lib.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/market_audit.rs | akzio-daemon：src/market_audit.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer.rs | akzio-daemon：src/observer.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer/broker.rs | akzio-daemon：src/observer/broker.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer/helpers.rs | akzio-daemon：src/observer/helpers.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer/learning.rs | akzio-daemon：src/observer/learning.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer/runs.rs | akzio-daemon：src/observer/runs.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer/snapshot.rs | akzio-daemon：src/observer/snapshot.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/observer_analytics.rs | akzio-daemon：src/observer_analytics.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/orchestration.rs | akzio-daemon：src/orchestration.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/orchestration/bootstrap.rs | akzio-daemon：src/orchestration/bootstrap.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/orchestration/control.rs | akzio-daemon：src/orchestration/control.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/orchestration/health_canary.rs | akzio-daemon：src/orchestration/health_canary.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/orchestration/workers.rs | akzio-daemon：src/orchestration/workers.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome.rs | akzio-daemon：src/outcome.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/canary.rs | akzio-daemon：src/outcome/canary.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/collection.rs | akzio-daemon：src/outcome/collection.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/helpers.rs | akzio-daemon：src/outcome/helpers.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/materialization.rs | akzio-daemon：src/outcome/materialization.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/narrative_repair.rs | akzio-daemon：src/outcome/narrative_repair.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/shadow.rs | akzio-daemon：src/outcome/shadow.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/outcome/worker.rs | akzio-daemon：src/outcome/worker.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler.rs | akzio-daemon：src/scheduler.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler/canary.rs | akzio-daemon：src/scheduler/canary.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler/lease.rs | akzio-daemon：src/scheduler/lease.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler/scheduler_core.rs | akzio-daemon：src/scheduler/scheduler_core.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler/scheduler_tick.rs | akzio-daemon：src/scheduler/scheduler_tick.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/scheduler/serve.rs | akzio-daemon：src/scheduler/serve.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-daemon/src/worker.rs | akzio-daemon：src/worker.rs | Worker E / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/Cargo.toml | akzio-domain：Cargo.toml | Worker A | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明纯领域/I/O 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-domain/src/artifact.rs | akzio-domain：src/artifact.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/behavior.rs | akzio-domain：src/behavior.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/budget.rs | akzio-domain：src/budget.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/canary.rs | akzio-domain：src/canary.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/context.rs | akzio-domain：src/context.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/context_scope.rs | akzio-domain：src/context_scope.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/contract.rs | Agent Contract 静态权限上限及校验 | Worker A / Faraday | 全文阅读；函数复核完成；Contract hash 输入已核对 | 外部说明；保留基线导读 | `contract_component_hash` 包含本文件 bytes，详细入口见本表哈希清单与学习指南 §7 | **受限文件已补外部说明**：不改冻结 Contract 身份输入 |
| crates/akzio-domain/src/core.rs | akzio-domain：src/core.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/debug.rs | akzio-domain：src/debug.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/decision.rs | akzio-domain：src/decision.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/evaluation.rs | akzio-domain：src/evaluation.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/evaluation/outcome.rs | akzio-domain：src/evaluation/outcome.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/evaluation/policy.rs | akzio-domain：src/evaluation/policy.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/evaluation/risk_ground_truth.rs | akzio-domain：src/evaluation/risk_ground_truth.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/event.rs | akzio-domain：src/event.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/execution.rs | akzio-domain：src/execution.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/execution/effects.rs | akzio-domain：src/execution/effects.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/execution/plan.rs | akzio-domain：src/execution/plan.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/execution/session.rs | akzio-domain：src/execution/session.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/execution/snapshots.rs | akzio-domain：src/execution/snapshots.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/experiment.rs | akzio-domain：src/experiment.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/ids.rs | akzio-domain：src/ids.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/instrument_evidence.rs | akzio-domain：src/instrument_evidence.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/lesson.rs | akzio-domain：src/lesson.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/lib.rs | akzio-domain：src/lib.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/longitudinal.rs | akzio-domain：src/longitudinal.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/market_safety.rs | akzio-domain：src/market_safety.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/qualification.rs | akzio-domain：src/qualification.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/regime.rs | akzio-domain：src/regime.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/release.rs | akzio-domain：src/release.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/research.rs | akzio-domain：src/research.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/research_review.rs | ProposalReview、受限修订与补采意图领域校验 | Worker A / Faraday | 全文阅读；函数复核完成；Contract hash 输入已核对 | 外部说明；保留基线导读 | `contract_component_hash` 包含本文件 bytes；业务阶段边界见运行时契约 §5/§7 | **受限文件已补外部说明**：不改冻结 Review/Contract 身份输入 |
| crates/akzio-domain/src/runtime_manifest.rs | akzio-domain：src/runtime_manifest.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/schema.rs | akzio-domain：src/schema.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/workflow.rs | akzio-domain：src/workflow.rs | Worker A | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-domain/src/workflow_definition.rs | 冻结 NodeSpec、WorkflowBlueprint 与历史节点适配 | Worker A / Faraday | 全文阅读；函数复核完成；topology hash 输入已核对 | 外部说明；保留基线导读 | `topology_component_hash` 包含本文件 bytes；执行图边界见运行时契约 §4 | **受限文件已补外部说明**：不改 Workflow 身份输入 |
| crates/akzio-execution/Cargo.toml | akzio-execution：Cargo.toml | Worker C / Pascal | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明 Gate/Paper/Commitment 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-execution/src/allocation.rs | akzio-execution：src/allocation.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/calibration.rs | akzio-execution：src/calibration.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/decision_gate.rs | akzio-execution：src/decision_gate.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/decision_gate/commit.rs | akzio-execution：src/decision_gate/commit.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/decision_gate/decide.rs | akzio-execution：src/decision_gate/decide.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/decision_gate/helpers.rs | akzio-execution：src/decision_gate/helpers.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/decision_gate/validate.rs | akzio-execution：src/decision_gate/validate.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/execution_gate.rs | akzio-execution：src/execution_gate.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/execution_gate/core.rs | akzio-execution：src/execution_gate/core.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/execution_gate/helpers.rs | akzio-execution：src/execution_gate/helpers.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/execution_gate/snapshots.rs | akzio-execution：src/execution_gate/snapshots.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/execution_gate/validation.rs | akzio-execution：src/execution_gate/validation.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/lib.rs | akzio-execution：src/lib.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper.rs | akzio-execution：src/paper.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/broker.rs | akzio-execution：src/paper/broker.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/execute.rs | akzio-execution：src/paper/execute.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/protocol.rs | akzio-execution：src/paper/protocol.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/reconcile.rs | akzio-execution：src/paper/reconcile.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/submission_authorization.rs | akzio-execution：src/paper/submission_authorization.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper/transport.rs | akzio-execution：src/paper/transport.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper_commitment.rs | akzio-execution：src/paper_commitment.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/paper_dispatch.rs | akzio-execution：src/paper_dispatch.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/policy.rs | akzio-execution：src/policy.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/pretrade_safety.rs | akzio-execution：src/pretrade_safety.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/reconciliation.rs | akzio-execution：src/reconciliation.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-execution/src/snapshot.rs | akzio-execution：src/snapshot.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/Cargo.toml | akzio-ingest：Cargo.toml | Worker C / Pascal | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明证据 adapter 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-ingest/examples/clock_probe.rs | akzio-ingest：examples/clock_probe.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/examples/evidence_preflight.rs | akzio-ingest：examples/evidence_preflight.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/examples/native_web_preflight.rs | native-web 预检演示与 PromptSource 示例身份输入 | Worker D / Sagan | 全文读取；编译/运行边界已复核 | 外部说明；原文/字节不改 | 该示例源码作为 `ingest.preflight_example` 纳入 Prompt identity；不调用就代表正式研究/Paper | **受限文件已补外部说明**：修改注释也会改变固定 PromptSource bytes |
| crates/akzio-ingest/src/adapters.rs | akzio-ingest：src/adapters.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/direct.rs | akzio-ingest：src/direct.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/financial_content.rs | akzio-ingest：src/financial_content.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/lib.rs | akzio-ingest：src/lib.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/market_capture.rs | akzio-ingest：src/market_capture.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/materialization/materialize_normalized.rs | akzio-ingest：src/materialization/materialize_normalized.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/materialization/materialize_raw.rs | akzio-ingest：src/materialization/materialize_raw.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/news.rs | akzio-ingest：src/news.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/official.rs | akzio-ingest：src/official.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/paper_decode.rs | akzio-ingest：src/paper_decode.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/paper_session.rs | akzio-ingest：src/paper_session.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/prompts.rs | GovernedResource 到采集意图文本的映射 | Worker D / Sagan | 全文读取；adapter 消费路径已核对 | 外部说明；源码字节不改 | 作为 `ingest.acquisition` 纳入 Prompt component hash；解析失败仍经 `EvidenceAdapterError::Policy` 传播 | **受限文件已补外部说明**：注释会改变 PromptSource identity |
| crates/akzio-ingest/src/prompts/source_verifier.md | akzio-ingest：src/prompts/source_verifier.md | Worker C / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-ingest/src/quant_features.rs | akzio-ingest：src/quant_features.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/runtime.rs | akzio-ingest：src/runtime.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-ingest/src/session_bars.rs | akzio-ingest：src/session_bars.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/Cargo.toml | akzio-learning：Cargo.toml | Worker C / Pascal | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明 Outcome/Learning 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-learning/src/campaign.rs | akzio-learning：src/campaign.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation.rs | akzio-learning：src/evaluation.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/benchmarks.rs | akzio-learning：src/evaluation/benchmarks.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/materialization.rs | akzio-learning：src/evaluation/materialization.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/materialize_outcome.rs | akzio-learning：src/evaluation/materialize_outcome.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/materialize_partial.rs | akzio-learning：src/evaluation/materialize_partial.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/outcomes.rs | akzio-learning：src/evaluation/outcomes.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/policy_learning.rs | akzio-learning：src/evaluation/policy_learning.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/risk_ground_truth.rs | akzio-learning：src/evaluation/risk_ground_truth.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/evaluation/runtime_setup.rs | akzio-learning：src/evaluation/runtime_setup.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/lesson_evidence.rs | akzio-learning：src/lesson_evidence.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/lib.rs | akzio-learning：src/lib.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/metrics.rs | akzio-learning：src/metrics.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/outcome_schedule.rs | akzio-learning：src/outcome_schedule.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-learning/src/qualification.rs | akzio-learning：src/qualification.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/Cargo.toml | akzio-model：Cargo.toml | Worker C / Pascal | 配置字段和依赖已复核 | 已补合法 TOML 导读 | 已说明模型协议/fixture 依赖边界 | **已完成**：只新增 TOML 注释 |
| crates/akzio-model/src/fixture.rs | akzio-model：src/fixture.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/lib.rs | akzio-model：src/lib.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/model_client/capability_probe.rs | akzio-model：src/model_client/capability_probe.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/model_client/client_response.rs | akzio-model：src/model_client/client_response.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/model_client/client_setup.rs | akzio-model：src/model_client/client_setup.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/model_client/probe_prompts.rs | 模型 capability probe 的固定提示文本 | Worker D / Sagan | 全文读取；probe 构造调用已核对 | 外部说明；Prompt bytes 不改 | `capability_probe.rs` 读取本文件文本，PromptSource `model.probes` 纳入 identity hash | **受限文件已补外部说明**：保持固定 capability probe Prompt 字节 |
| crates/akzio-model/src/native_web.rs | akzio-model：src/native_web.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/responses.rs | akzio-model：src/responses.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-model/src/schema.rs | akzio-model：src/schema.rs | Worker C / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/Cargo.toml | akzio-research：Cargo.toml | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent.rs | akzio-research：src/agent.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/budget.rs | akzio-research：src/agent/budget.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/catalogue.rs | akzio-research：src/agent/catalogue.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/errors_catalogue.rs | akzio-research：src/agent/errors_catalogue.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/helpers.rs | akzio-research：src/agent/helpers.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/model_types.rs | akzio-research：src/agent/model_types.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/projection.rs | akzio-research：src/agent/projection.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/prompts/mod.rs | akzio-research：src/agent/prompts/mod.rs | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/phases.rs | akzio-research：src/agent/prompts/phases.rs | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/roles/analyst.md | akzio-research：src/agent/prompts/roles/analyst.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/roles/critic.md | akzio-research：src/agent/prompts/roles/critic.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/roles/outcome.md | akzio-research：src/agent/prompts/roles/outcome.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md | akzio-research：src/agent/prompts/roles/proposal_reviewer.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/roles/synthesizer.md | akzio-research：src/agent/prompts/roles/synthesizer.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/prompts/shared.md | akzio-research：src/agent/prompts/shared.md | Worker D / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-research/src/agent/proposal_review.rs | akzio-research：src/agent/proposal_review.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/recovery.rs | akzio-research：src/agent/recovery.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/runtime_core.rs | akzio-research：src/agent/runtime_core.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/runtime_helpers.rs | akzio-research：src/agent/runtime_helpers.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/runtime_run.rs | akzio-research：src/agent/runtime_run.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/runtime_type.rs | akzio-research：src/agent/runtime_type.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/schemas.rs | akzio-research：src/agent/schemas.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/structured.rs | akzio-research：src/agent/structured.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/tools.rs | akzio-research：src/agent/tools.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/agent/validation.rs | akzio-research：src/agent/validation.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/fixture.rs | akzio-research：src/fixture.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/lib.rs | akzio-research：src/lib.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/prompt_registry.rs | akzio-research：src/prompt_registry.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-research/src/quality.rs | akzio-research：src/quality.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/Cargo.toml | akzio-runtime：Cargo.toml | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/lib.rs | akzio-runtime：src/lib.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime.rs | akzio-runtime：src/runtime.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/catalogue.rs | akzio-runtime：src/runtime/catalogue.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/compilation.rs | akzio-runtime：src/runtime/compilation.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/compilation/evidence.rs | akzio-runtime：src/runtime/compilation/evidence.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/compilation/helpers.rs | akzio-runtime：src/runtime/compilation/helpers.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/compilation/lowering.rs | akzio-runtime：src/runtime/compilation/lowering.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/compilation/validation.rs | akzio-runtime：src/runtime/compilation/validation.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/node.rs | akzio-runtime：src/runtime/node.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/reducer.rs | akzio-runtime：src/runtime/reducer.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/replay.rs | akzio-runtime：src/runtime/replay.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/store_executor.rs | akzio-runtime：src/runtime/store_executor.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/task.rs | akzio-runtime：src/runtime/task.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-runtime/src/runtime/workflow.rs | akzio-runtime：src/runtime/workflow.rs | Worker D / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/Cargo.toml | akzio-store：Cargo.toml | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/examples/agent_token_attribution.rs | akzio-store：examples/agent_token_attribution.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/lib.rs | akzio-store：src/lib.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store.rs | akzio-store：src/store.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/attempt.rs | akzio-store：src/store/attempt.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/blob.rs | akzio-store：src/store/blob.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary.rs | akzio-store：src/store/canary.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary/cohort.rs | akzio-store：src/store/canary/cohort.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary/helpers.rs | akzio-store：src/store/canary/helpers.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary/history.rs | akzio-store：src/store/canary/history.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary/reservation.rs | akzio-store：src/store/canary/reservation.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/canary/stage.rs | akzio-store：src/store/canary/stage.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/debug.rs | akzio-store：src/store/debug.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/debug_bundle.rs | akzio-store：src/store/debug_bundle.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/decision_policy.rs | akzio-store：src/store/decision_policy.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/doctor.rs | akzio-store：src/store/doctor.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/execution.rs | akzio-store：src/store/execution.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/experiment.rs | akzio-store：src/store/experiment.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/fixtures/retired-history.md | akzio-store：src/store/fixtures/retired-history.md | Worker B / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-store/src/store/fixtures/retired_paper_dry_run.sql | akzio-store：src/store/fixtures/retired_paper_dry_run.sql | Worker B / Pascal | 用途/消费方已核对；原文保持不变 | 本表外部说明 | 原文不改；记录调用/限制 | **受限文件已补外部说明**：运行时 Prompt、模板、SQL 或历史 fixture；原文不能改写，消费方/用途/限制由本表说明 |
| crates/akzio-store/src/store/free_events.rs | akzio-store：src/store/free_events.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_lifecycle.rs | akzio-store：src/store/free_lifecycle.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_paper_checks.rs | akzio-store：src/store/free_paper_checks.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_policy_helpers.rs | akzio-store：src/store/free_policy_helpers.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_policy_reads.rs | akzio-store：src/store/free_policy_reads.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_reads.rs | akzio-store：src/store/free_reads.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_trajectory.rs | akzio-store：src/store/free_trajectory.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/free_validation.rs | akzio-store：src/store/free_validation.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_attempt.rs | akzio-store：src/store/impl_attempt.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_core.rs | akzio-store：src/store/impl_core.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_history.rs | akzio-store：src/store/impl_history.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_learning.rs | akzio-store：src/store/impl_learning.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_queries.rs | akzio-store：src/store/impl_queries.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/impl_workflow.rs | akzio-store：src/store/impl_workflow.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/learning.rs | akzio-store：src/store/learning.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/learning/history.rs | akzio-store：src/store/learning/history.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/learning/outcome.rs | akzio-store：src/store/learning/outcome.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/learning/policy.rs | akzio-store：src/store/learning/policy.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/learning/shadow.rs | akzio-store：src/store/learning/shadow.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/lease.rs | akzio-store：src/store/lease.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/lesson.rs | akzio-store：src/store/lesson.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/lesson/helpers.rs | akzio-store：src/store/lesson/helpers.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/lesson/queries_verify.rs | akzio-store：src/store/lesson/queries_verify.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/lesson/write.rs | akzio-store：src/store/lesson/write.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/maintenance.rs | akzio-store：src/store/maintenance.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/migration.rs | akzio-store：src/store/migration.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/prelude.rs | akzio-store：src/store/prelude.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/public_types.rs | akzio-store：src/store/public_types.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/release.rs | akzio-store：src/store/release.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/research_quality.rs | akzio-store：src/store/research_quality.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/research_review.rs | akzio-store：src/store/research_review.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/run_control.rs | akzio-store：src/store/run_control.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/schema.rs | akzio-store：src/store/schema.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/trajectory.rs | akzio-store：src/store/trajectory.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow.rs | akzio-store：src/store/workflow.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/commits.rs | akzio-store：src/store/workflow/commits.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/contracts.rs | akzio-store：src/store/workflow/contracts.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/helpers.rs | akzio-store：src/store/workflow/helpers.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/outputs.rs | akzio-store：src/store/workflow/outputs.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/queries.rs | akzio-store：src/store/workflow/queries.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| crates/akzio-store/src/store/workflow/tasks.rs | akzio-store：src/store/workflow/tasks.rs | Worker B / Pascal | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| docs/agent-budget-configuration.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/agent-runtime-contract.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/debug-control.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/development-workflow.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/news-evidence.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/prompt-ownership.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/research-protocol-retirement.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/research-quality.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| docs/workflow-runtime.md | 项目规则、设计/运行契约或说明文档 | 主 Agent | 已读取至任务所需范围；不作为可注释源码 | 外部说明 | 不适用 | **排除**：项目规则/说明文档；不把普通文档改写冒充代码注释覆盖 |
| rust-toolchain.toml | Rust 工具链固定配置 | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| scripts/check_markdown_links.sh | 离线检查/归档/隔离运行脚本：check_markdown_links.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| scripts/export_debug_bundle.sh | 离线检查/归档/隔离运行脚本：export_debug_bundle.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| scripts/position_plan_run.py | 离线检查/归档/隔离运行脚本：position_plan_run.py | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| scripts/update_app_and_submit_debug.sh | 离线检查/归档/隔离运行脚本：update_app_and_submit_debug.sh | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| scripts/verify_research_quality.py | 离线检查/归档/隔离运行脚本：verify_research_quality.py | 主 Agent | 全文阅读；函数/方法/宏/闭包/异步任务/测试或配置分支已复核 | 已补合法文件导读 | 已补关键数据流、分支、错误/副作用及语言机制说明 | **已完成**：当前 diff 仅含普通注释和必要空白；已纳入全文/函数复核及非注释审计 |
| docs/RUST_LEARNING_GUIDE.md | 初级读者 Rust 学习导航；只串联源码中的真实机制 | 主 Agent | 全文重读并核验示例/实际路径 | 已更新文件导读、层级结构 | 覆盖实际用法与未使用概念，示例区分项目实现/独立教学 | **已完成**：本任务专用文档，任务开始前已存在；仅文档更新，不作为运行时输入 |
| docs/ANNOTATION_COVERAGE.md | 跟踪文件清单、阅读/函数复核、注释状态与限制记录 | 主 Agent | 按任务开始 Git 清单逐路径核对 | 以本文记录职责/入口 | 记录各批调用链、Rust知识、hash-sensitive/模板/Prompt/SQL边界 | **已完成**：本任务专用文档，任务开始前已存在；仅记录，不作为运行时输入 |

## 主 Agent 最终复核

- 任务开始时 `git status --short` 为空；本任务未使用 `reset`、`checkout`、`git clean`、删除或提交。任务末 `git ls-files` 与逐文件清单双向对照无缺失；忽略的 target/App build 产物未登记。
- `git diff --check` 通过。注释-only 差异审计对 `.rs`、`.swift`、脚本及 TOML/YAML 没有发现普通注释以外的代码/配置行变化，也没有新增或改写 `///`、`//!`、`#[doc]`。发现一次 `official.rs::parse_csv` 引号分支格式差异后已恢复原始表达式，仅保留就近普通注释。
- 任务末 `git ls-files --others --exclude-standard` 无输出，仓库内没有因本任务生成的 `.akzio` Store/报告；`target/` 是任务前已存在的 ignored 构建目录，本任务未清理。
- 显式 Prompt/Contract/Topology `include_bytes!` 身份输入未改动；`guidance.rs` 及领域 `contract.rs`、`research_review.rs`、`workflow_definition.rs` 中试加的普通注释已撤回并登记为受限。源文件内容/历史哈希未重写。注释与本表/学习指南差异会由 `build.rs::git_revision` 影响下次编译出的 `code_revision`；本任务没有执行 daemon 或生成 RuntimeIdentity。
- 实际检查：
  - `cargo fmt --all -- --check`：通过。
  - `cargo check --workspace --locked`：通过。
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`：通过。
  - `cargo test --locked -p akzio-domain`：25 passed。
  - worker C：`cargo test --locked -p akzio-execution`，9 passed；worker D：`cargo test --locked -p akzio-ingest -p akzio-model -p akzio-research -p akzio-runtime`，101 passed、2 ignored（忽略项需 FRED/Alpaca 凭据和网络）。
  - `swiftc -frontend -parse $(git ls-files '*.swift')`：通过语法解析；不是 SwiftPM 类型检查或 UI 验收。
  - 所有跟踪 Shell 文件逐个 `bash -n`：通过；两份 Python 脚本 `python3 -m ast`：通过。
  - `bash scripts/check_markdown_links.sh`：通过。
- 未运行 `cargo test --workspace`：源内测试包含固定 `.akzio` 路径的 Store/报告写入（例如 `akzio-cli` calibration tests 与 `akzio-daemon` scheduler lease test），不为注释任务创建/覆盖这些持久化 fixture；另两项需要真实 FRED/Alpaca 的测试保持 ignored。未运行 `debug verify-fixture`、Store Doctor、Core/daemon、App build/sign、真实模型、Broker/Paper 或任何 Outcome/学习流程。
- 证据范围仅为 `implemented` 与上述离线静态/编译检查；未声明 `offline-verified`、`real-Paper-verified` 或 `outcome/learning-verified`。任何研究提案、Decision、ExecutionVerdict、提交/成交与 Outcome 均没有在本次任务中执行。
