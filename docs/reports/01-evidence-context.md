# 数据采集与 Context 全流程：当前工作树只读核查

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

- 核查日期：2026-09-29（Asia/Singapore）。对象：当前工作树文件，不是 Git HEAD、已部署二进制或某次 Run 的实际结果。
- 唯一写入：本报告。没有修改源码、配置、Store；没有执行 cargo、测试、真实模型、Broker 或外部 API；没有读取用户凭据或数据库内容。
- 证据标签：**source-inspected（当前源码已查）**。不是 offline-verified、real-Paper-verified 或 outcome/learning-verified。
- 启动和结束的 `git status --short` 均显示他人既有改动：CI、根 AGENTS、README、daemon HTTP/launch/lib/health_canary、Store debug_bundle、运行时/Debug/开发/退役文档、run_core.py，另有未跟踪 test_run_core.py。均按当前内容读取且未处理。
- 已先读根 AGENTS、运行时契约 3/5/6/11/12 节及 news-evidence 文档。本报告只覆盖采集和 Context；调度/Runtime/Store 仅列跨模块接口，不复述其状态机；研究执行、修订及 Gate 业务裁决交其他 worker。
- 下文以仓库相对文件路径及职责/符号说明定位来源；源码移动后须重核行为，不能把静态阅读当作联网或测试通过。

## 1. 给主助手的核心结论

1. **40 项是 Need 数，不是 40 次 HTTP，更不是每个模型能看 40 件材料。** Paper 固定 40 个去重 EvidenceNeed：6 ExecutionSafety + 34 DirectionalResearch；PositionPlan 去掉六项执行安全 Need，保留 34 项。当前固定表中 Enhancement=0。六项前置只标记 deferred_to_execution，不采集；执行阶段才重新获取。依据：`crates/akzio-domain/src/workflow.rs`；`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-daemon/src/evidence.rs`。
2. **EvidenceGate 是受治理输入采集/封存边界，不是事实真值或交易许可。** 已知来源失败可保留缺口，成功项继续；哪怕所有研究来源不可用，collection-status Artifact 仍可能使门面返回 Succeeded。时间污染阻断整个 Gate；身份、引用、策略、Store 错误不得降级成普通覆盖缺口。依据：`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/evidence_acquisition.rs`。
3. **新闻当前主路径为 discovery + 独立模型 source review，而非 Rust 独立抓网页。** `model_reviewed`、`citations_complete=true`、URL 绑定都不等于 `source_verified=true`。可用事实和失败项允许共存；空事实、审阅失败也可能形成合法 NormalizedEvidence 并在 Gate 被记 available。依据：`crates/akzio-ingest/src/news.rs`；`crates/akzio-daemon/src/evidence.rs`。
4. **时间边界并非“下载新鲜即内容新鲜”。** 日线严格按美东交易所 close+20m、宏观按前一自然日 vintage、期权保留各字段时间和缺失。但通用 `StaleEvidence` 实际比较 `now - acquired.observed_at`，生产适配器通常把 observed_at 设为抓取完成时间，未统一用 `available_at` 做 max_age。须报告此处文档/实现差异。依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`docs/agent-runtime-contract.md`。
5. **Context 是精确引用授权，不是一个目录/来源族的泛化读取权。** Manifest 固化选择；ReadGrant 再绑定 Run/Task/Attempt/Lease/epoch/Contract、精确 readable 和过期时间；读取重验持久化闭包和当前 permit。RawEvidence 不进 Manifest，当前所有活动 Contract 均 `allow_raw_reread=false`。依据：`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
6. **预算要讲清完整来源与投影分离。** 24 件上限；普通角色投影128 KiB、来源512 KiB；Synthesizer投影192 KiB、来源768 KiB。必需闭包先于可选背景；过大必需输入直接拒绝。原始期权 NormalizedEvidence 在条件满足时被替换为可引用 SemanticDetail 投影，保留原 source_ref，但不自动授予原对象读取权。依据：`crates/akzio-research/src/agent/errors_catalogue.rs`；`crates/akzio-context/src/context_broker/manifest.rs`。
7. **召回、选中、内联正文、工具读取是四件不同事。** 当前 Lesson 召回只向允许 Lesson 的 Contract 开放，默认研究链只有 Synthesizer；但通用 selection reason 为 lesson，must_read_class 无 Lesson 分支，因此默认研究 materialization 只列 metadata，没有 Lesson 正文，且研究无读工具。这是静态路径推导，不是运行复现。依据：`crates/akzio-context/src/selection.rs`；`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。

## 2. 全链路图与数据对象

```text
冻结 EvidenceNeed（Rust 的 resource/source/max_age）
  -> daemon 核对完整固定集合、producer、Run、去重
  -> ExecutionSafety 六项：仅 collection-status deferred_to_execution
  -> 其余 Need：EvidenceRequest + 指定 adapter（可并发，逐项有界）
       Alpaca direct / FRED direct / SEC direct（未进固定40项）
       news_web router：issuer direct 或 native discovery + source review
  -> AcquiredEvidence { raw, normalized JSON, provenance, quality }
  -> EvidenceRuntime：请求授权 + URI/citation/quality + 时间污染检查
  -> RawEvidence + NormalizedEvidence（Normalized source_refs = Raw + Need）
  -> evidence.collection_status（各 Need 的状态与诊断）
  -> EvidenceGate task result（不等于研究/方向/执行通过）
  -> 调用方显式 candidate refs
  -> ContextBroker：Contract/来源/Run/overlay/隔离过滤
       + 允许时 Lesson/Experience 召回
       + 期权 source -> 受治理 SemanticDetail 投影
       + 必需闭包 -> 平衡背景优先级 -> 件数/来源/投影/token预算
  -> ContextManifest（CAS）+ 当前 Attempt 的 ReadGrant（内存授权）
  -> materialize_for_agent_with_budget
       研究：已授权 projections + metadata；无资料读取工具
       Outcome：独立 projection + 五个受控读工具
```

对象证据：`crates/akzio-ingest/src/runtime.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。

注意 RawEvidence 并非统一“文章原始HTML”：直接 API 是响应 bytes；市场捕获是多请求 JSON 账本；ModelReviewed 新闻是 discovery provider envelope 加 review 审计的 NDJSON；历史 VerifiedSource 模式可拼接来源正文。其含义是适配器保留的原始获取材料，不是统一媒体类型。依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-ingest/src/adapters.rs`。

## 3. 40 / 34 项完整清单

### 3.1 表中符号和计数

- `S` = Run 已冻结的 session 日期，不是本机今天；`B=S-400自然日`、`N=S-14日`、`M=S-366日`、`V=S-1日`、`O=S+120日`、`C=S+45日`。
- E=ExecutionSafety；D=DirectionalResearch。**下表 1–6 仅 Paper；7–40 同时属于 Paper 和 PositionPlan**。每行一个唯一 Need；不按 registry requirement 出现次数重复计数。
- 基础17项 = 6执行 + 4日线 + 4新闻 + 3宏观。Instrument registry 另外贡献24个唯一Need，其中 paper.quotes 与基础表重复1个：17+24-1=40。它的4个 LiquiditySpread requirement 共用同一 paper.quotes，不能算成4次需求。
- 源码按 BTreeSet 排序，本表为了说明按业务分组，**不是生产顺序或实际请求顺序**。

清单依据：`crates/akzio-domain/src/workflow.rs`（基础）；`crates/akzio-domain/src/instrument_evidence.rs`（扩展）；分类依据 `crates/akzio-domain/src/workflow.rs`；过滤依据 `crates/akzio-daemon/src/evidence.rs`。

| # | source_family | 完整 resource 模板 | max_age_secs | 分类/用途 | 清单来源 |
|---:|---|---|---:|---|---|
|1|alpaca|`paper.account`|300|E 账户|基础|
|2|alpaca|`paper.positions`|300|E 持仓|基础|
|3|alpaca|`paper.open_orders`|300|E 挂单|基础|
|4|alpaca|`paper.fills:S`|300|E 当session成交活动|基础|
|5|alpaca|`paper.quotes`|300|E 四资产报价/流动性共享|基础；扩展|
|6|alpaca|`paper.clock`|300|E 市场时钟|基础|
|7|alpaca|`bars:TQQQ:1d:B:252`|604800|D 价格|基础|
|8|alpaca|`bars:QQQ:1d:B:252`|604800|D 价格|基础|
|9|alpaca|`bars:SOXX:1d:B:252`|604800|D 价格|基础|
|10|alpaca|`bars:SOXL:1d:B:252`|604800|D 价格|基础|
|11|news_web|`news:TQQQ:N:S:market`|604800|D 近期新闻|基础|
|12|news_web|`news:QQQ:N:S:market`|604800|D 近期新闻|基础|
|13|news_web|`news:SOXX:N:S:market`|604800|D 近期新闻|基础|
|14|news_web|`news:SOXL:N:S:market`|604800|D 近期新闻|基础|
|15|fred|`series:DFF:M:S:V`|604800|D 宏观|基础|
|16|fred|`series:DFII10:M:S:V`|604800|D 宏观|基础|
|17|fred|`series:VIXCLS:M:S:V`|604800|D 宏观|基础|
|18|news_web|`research:etf_holdings:TQQQ:S`|86400|D 官方持仓|扩展|
|19|news_web|`research:etf_holdings:QQQ:S`|86400|D 官方持仓|扩展|
|20|news_web|`research:etf_holdings:SOXX:S`|86400|D 官方持仓|扩展|
|21|news_web|`research:etf_holdings:SOXL:S`|86400|D 官方持仓|扩展|
|22|news_web|`research:index_metadata:TQQQ:S`|86400|D 指数元数据|扩展|
|23|news_web|`research:index_metadata:QQQ:S`|86400|D 指数元数据|扩展|
|24|news_web|`research:index_metadata:SOXX:S`|86400|D 指数元数据|扩展|
|25|news_web|`research:index_metadata:SOXL:S`|86400|D 指数元数据，当前adapter明确缺口|扩展|
|26|news_web|`research:earnings_event_calendar:TQQQ:S`|86400|D 成分公司事件日历|扩展|
|27|news_web|`research:earnings_event_calendar:QQQ:S`|86400|D 成分公司事件日历|扩展|
|28|news_web|`research:earnings_event_calendar:SOXX:S`|86400|D 成分公司事件日历|扩展|
|29|news_web|`research:earnings_event_calendar:SOXL:S`|86400|D 成分公司事件日历|扩展|
|30|news_web|`research:leveraged_etf_terms:TQQQ:S`|604800|D 3倍每日重置条款|扩展|
|31|news_web|`research:leveraged_etf_terms:SOXL:S`|604800|D 3倍每日重置条款，当前adapter明确缺口|扩展|
|32|alpaca|`corporate_actions:TQQQ:M:S`|86400|D 公司行动|扩展|
|33|alpaca|`corporate_actions:QQQ:M:S`|86400|D 公司行动|扩展|
|34|alpaca|`corporate_actions:SOXX:M:S`|86400|D 公司行动|扩展|
|35|alpaca|`corporate_actions:SOXL:M:S`|86400|D 公司行动|扩展|
|36|alpaca|`option_chain:TQQQ:S:O`|300|D 期权链/IV背景|扩展|
|37|alpaca|`option_chain:QQQ:S:O`|300|D 期权链/IV背景|扩展|
|38|alpaca|`option_chain:SOXX:S:O`|300|D 期权链/IV背景|扩展|
|39|alpaca|`option_chain:SOXL:S:O`|300|D 期权链/IV背景|扩展|
|40|fred|`release_calendar:S:C:V`|86400|D 共享宏观发布日历|扩展|

其中“基础”完整路径为 `crates/akzio-domain/src/workflow.rs`；“扩展”完整路径为 `crates/akzio-domain/src/instrument_evidence.rs`。`EvidenceNeed` 全字段参与去重（schema/source/resource/max_age），不是仅resource去重：`crates/akzio-domain/src/workflow.rs`。

**关键解释：** PositionPlan 不含 paper.quotes Need，但研究日线/期权的底层 `capture_stock` 仍会 GET 资产、clock、calendar、snapshot/quote/trade。故“六项执行 Need 不采集”不等于“研究时完全不访问 Paper只读端点/不获取任何quote”。两者用途和 Artifact 身份不同。依据：`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-ingest/src/session_bars.rs`。

## 4. 输入源、资源语法和授权限制

### 4.1 Rust-owned 三道请求约束

1. `EvidenceNeed::validate`：schema正确、source/resource非空、resource最多2048字符、max_age 1..604800秒、来源仅 alpaca/sec_edgar/fred/news_web。`crates/akzio-domain/src/workflow.rs`。
2. Paper/PositionPlan 的 daemon 在I/O前核对**整个Need集合完全等于冻结session对应政策**；每个Artifact为EvidenceNeed、producer=`scheduler.paper_snapshot`、RunScoped且同Run，拒绝多/少/重复项。Paper session从已持久化slot取；PositionPlan从冻结option_chain Need取得唯一session，不要求当前市场开放。`crates/akzio-daemon/src/evidence.rs`。
3. `EvidenceRuntime::authorize_request` 从Store重读Need，核对kind、同Run、source/resource/max_age与request完全一致，source在当前runtime allowlist且adapter.source一致；`EvidenceRequest::validate`再调用GovernedResource解析器。它不是任意URL代理。`crates/akzio-ingest/src/materialization/materialize_raw.rs`；`crates/akzio-ingest/src/runtime.rs`。

资源语法范围：四资产quotes/bars、公司行动跨度≤366日、option区间≤180日；普通bars limit≤252；FRED series名称≤64且仅ASCII字母数字/`._-`、窗口≤366日；release_calendar跨度≤90日且vintage<start；news近期窗口≤31日，topic仅market/rates/semiconductor/regulation/earnings/geopolitics；research类别只有holdings/index/leveraged terms/earnings calendar。SEC CIK/accession/文档名有独立格式验证。**通用GovernedResource仍存在NewsWeb query分支，不能笼统说底层完全没有自由查询；正式固定Need政策及当前补采接口并不让研究模型直接调用该分支。** `crates/akzio-ingest/src/runtime.rs`。

### 4.2 实际适配器注册与外部地址

- Daemon从环境构造Alpaca、SEC、FRED；构造失败不注册对应source，实际采集报告adapter不可用；news_web统一使用configured router，不因一次capability probe结果永久禁止采集。这里没有核验本地实际配置是否满足。`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- Alpaca仅接受精确Paper base URL（可尾随 `/`），行情host固定data.alpaca.markets；凭据为空/非Paper URL在I/O前拒绝；HTTP不follow redirect。股票必须显式IEX/SIP；期权默认Indicative，显式配置可改OPRA，不自动fallback。`crates/akzio-ingest/src/adapters.rs`。
- SEC直连data.sec.gov / www.sec.gov，环境键SEC_USER_AGENT；FRED直连api.stlouisfed.org，环境键FRED_API_KEY。客户端不follow redirect；SEC rate gate125ms、FRED250ms。FRED key仅放实际请求URL，provenance用无key public_url。SEC存在实现但**不属于固定40/34项**。`crates/akzio-ingest/src/direct.rs`。
- issuer direct按完整host允许：dng-api.invesco.com、www.invesco.com、accounts.profunds.com、www.proshares.com、www.ishares.com、www.direxion.com；HTTPS、无自动redirect、正文≤4MiB。`crates/akzio-ingest/src/official.rs`。
- native NewsWeb域名组：Reuters、AP、ETFChannel（含m子域）、ETF.com、Invesco、ProShares、iShares、BlackRock、Direxion、Nasdaq、NYSE；具体列表是代码，不是默认NativeWebPolicy那四个域名。host可为受限域合法子域，不允许后缀伪装；HTTPS、不带user/password/端口。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-model/src/native_web.rs`。
- 通用provenance URI检查禁止认证、fragment、token/secret/password/api_key/key/authorization等query，但其函数自身没有强制HTTPS或source-host匹配；这些靠各生产adapter/policy约束，不能把它描述成单函数完整SSRF边界。`crates/akzio-ingest/src/runtime.rs`。

## 5. 原始采集 → 规范化 → 逐项状态 → EvidenceGate

### 5.1 时间、质量、溯源的实际检查顺序

- 异步`acquire_validated_async`先authorize，再`adapter.acquire_at(request, now)`，再基本`validate_acquisition`（无CAS写入）；`materialize_validated`重新authorize，然后完整materialize。注意方法名“validated”不意味着完整时间基准已在第一次验证完成；`time_basis`及污染证书是在materialize阶段建立。`crates/akzio-ingest/src/materialization/materialize_raw.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- raw必须非空、media/source URI非空；provenance顶层source_uri/observed_at必须和AcquiredEvidence相等；dedupe_key非空；每条citation的原始byte slice必须精确等于quote；quality completeness≤1M且normalized=true，但**并不要求citations_complete=true**。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/runtime.rs`。
- Normalized payload保存schema/source/resource/Need ref/Raw ref/observed_at/time_basis/contamination_certificate/quant_features/financial_content/value/provenance/quality。Artifact为RunScoped、producer=`akzio.ingest.<source>.normalized`、source_refs精确Raw+Need；Raw producer是相应`.raw`。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- 异步materialize将completeness_ppm作为Normalized provenance.confidence_ppm；这是覆盖度元数据，不是预测置信度或语义正确率。同步fixture路径传入1M，所以两条入口的confidence构造不同。`crates/akzio-ingest/src/materialization/materialize_raw.rs`。

### 5.2 EvidenceGate的并发和部分成功

- 正式Paper/PositionPlan研究源用join_all；非fixture每项采集allowance=`max(max_wall_time_secs-15,1)`秒，余量留给验证/持久化。采完后用host `Utc::now()`冻结统一materialization cutoff；fixture沿用传入cutoff。`crates/akzio-daemon/src/evidence.rs`。
- 每项状态为available / unavailable / deferred_to_execution，并记录need ref、resource、criticality、diagnostic；collection-status写成SemanticDetail、producer=`evidence.collection_status`、source_family=`akzio.ingest`，缺方向材料的约定为neutralize_affected_slots。它不是ReadGrant。`crates/akzio-daemon/src/evidence.rs`。
- 普通覆盖错误类别：authorization、rate_limited、pending、adapter_unavailable、transport、permanent_provider_error、native-web细分类、data_quality、stale_content、acquisition_timeout；unknown/internal/invalid provenance/citation/need/policy/Store不降级。时间污染单独记unavailable/temporal_contamination，先保留状态与成功证据诊断，再返回Err。`crates/akzio-daemon/src/evidence.rs`。
- collection-status是先单独写入；成功Raw/Normalized一般随task结果返回。不能说“所有证据和状态是一次全有/全无事务”。门面只看返回Artifact是否为空决定NoOutput/Succeeded。`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/evidence_acquisition.rs`。
- registered Shadow canary在门面等待父证据（无则DeferredUntil+1秒），只将父成功材料引用连同精确collection-status包为`canary.evidence_snapshot`；不是重新获取一套市场证据。父成功/授权资格由Store接口负责，未在此报告重验Store实现。`crates/akzio-daemon/src/application/evidence_acquisition.rs`。

### 5.3 freshness / cutoff的准确含义

| 时间维度 | 实现 | 边界 |
|---|---|---|
| `event_time` | Alpaca从bars或snapshots的t/timestamp取最晚值；FRED取observations日期；SEC/news为None | 新闻逐事实event_date另验，非统一event_time |
| `available_at` | FRED=vintage日23:59:59 UTC；news/SEC=published_at，否则observed_at；Alpaca优先content_available_at，否则event_time/observed_at | 不能把所有来源一概称为精确发布时间 |
| `retrieved_at` | time_basis使用acquired.observed_at | Artifact.provenance.retrieved_at另用materialization now，二者不要混写 |
| 污染 | available/event/release/vintage≤DecisionClock；release≤availability、retrieval≥availability、event≤release（有值时） | 缺event/release时跳过相应比较，不是验证了一个不存在的时间 |
| stale | `now - acquired.observed_at > max_age` | 没有统一 `now - available_at` stale判断；新下载旧内容并不由此通用检查必然拒绝 |

表依据：`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/runtime.rs`。

**尤其不能夸大：** EvidenceTimeBasis未显式要求retrieved_at≤cutoff。无可信published_at时available_at回退retrieval，历史晚抓会被挡；有过去published_at/vintage时，晚抓本身不必然违反此函数。证书五个true只表明上述实现条件通过（可选值为空的条件视为通过），不代表全部来源真的提供了原始时间字段。`crates/akzio-ingest/src/runtime.rs`。

## 6. 各资料族的获取和事实边界

### 6.1 新闻：discovery、model_reviewed、source_verified必须拆开

**策略与路由**

- policy version=4；非news_web直接VerifiedSource；`research:*`除earnings_event_calendar外也VerifiedSource；其余news_web为ModelReviewed。当前函数忽略RunPurpose参数，因此不是Paper才加强、Debug自动降级。旧DiscoveryOnly/VerifiedSource枚举和adapter路径仍存在，不代表当前news Need采用它。`crates/akzio-domain/src/workflow.rs`。
- discovery路由evidence.news_web，reviewer路由research.critic，各自缺route回退默认。router将holdings/index/terms发给official；recent news及earnings calendar发native。`crates/akzio-ingest/src/news.rs`。

**第一步 discovery**

- 一次`respond`，required native web，输出上限2000；prompt请求近期news最多三篇，但Rust NativeWebPolicy允许最多32个citation，review只取排序后前8个URL。三篇是文字指导，不是Rust硬上限。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/prompts.rs`；`crates/akzio-model/src/native_web.rs`；`crates/akzio-ingest/src/news.rs`。
- 必须有provider raw中的completed web_search_call和允许的search/open_page/find_in_page action；query/action.sources可缺，annotations等原生来源可参与引用提取；无真实hosted action的文字“我搜过”不能通过。`crates/akzio-model/src/native_web.rs`。
- DiscoveryOnly只证明URL存在于保留的provider envelope字节：citation.quote就是URL本身，不是文章摘录。status=provider_attributed_unverified、verified_source_count=0、fetch_count=0、exact_quote_count=0、citations_complete=false、completeness=250000。`crates/akzio-ingest/src/adapters.rs`。

**第二步独立 source review**

- reviewer不复用前次结论作为真值，而以URL集合+candidate_summary做独立required native-web请求，上限6000输出tokens。允许域从实际discovery provider_request回读，不另扩源。Prompt要求候选摘要和网页视为不可信数据。`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/prompts/source_verifier.md`。
- 返回SourceStatus四值supported/contradicted/unverifiable/irrelevant。每条事实有statement、可空published_at、event_date、date_basis；非supported不能贡献事实；supported须在review provider citations出现（只移除固定utm_source/medium/campaign/term/content做URL身份比较，不follow redirect，不合并业务URL）。`crates/akzio-ingest/src/news.rs`。
- 当前按来源/逐事实分区：重复/遗漏来源、身份歧义、未请求来源、无citation、无事实、窗口外事实进入validation_failures；合法事实仍可保留。因此不要把较早文档“遗漏/重复均拒绝”理解成任一问题必定整包丢弃。`crates/akzio-ingest/src/news.rs`。
- RecentNews的event_date必须在resource[start,end]且不晚于review完成UTC日期，published_at不得晚于now。事件日历不进入RecentNews窗口分支，所以未来计划可保留；具体计划是否已公布、是否属于该ETF/成分公司，主要由模型审阅和Prompt承担，Rust没有逐条证明公告时间/事实含义，也没有对该日历添加固定未来天数区间。`crates/akzio-ingest/src/news.rs`。
- `parse_review_envelope`容忍第一个完整JSON后的尾随bytes、顶层未知key；source/fact保留deny_unknown_fields；非supported source省略facts默认空。截断JSON仍解析失败。`crates/akzio-ingest/src/news.rs`。

**结果如何表达**

- accepted事实成为reviewed_facts及output_text；始终source_verified=false、verified_source_count=0，另计model_reviewed_source_count；有合法事实时news_evidence_status=model_reviewed，即使validation_failures非空；否则fetch_failed/malformed_json/facts_outside_window/facts_empty/source_unverified。`crates/akzio-ingest/src/news.rs`。
- citations_complete=是否至少有可用事实；completeness=“被选最多8个来源中支持来源数/选中来源数”，不是全新闻覆盖率、事实正确率。若所有事实都有published_at则包级取其最大值，否则None，再用实际review读取完成时间约束可用性；不猜时间。`crates/akzio-ingest/src/news.rs`。
- Raw追加完整review request/response/audit，Normalized移除provider_result/provider_request/discovery_output_text及大型review audit，只保留精简事实、失败记录和审阅来源摘要；human_review=not_performed、investment_inference=not_verified。`crates/akzio-ingest/src/news.rs`。
- 新闻策略身份不匹配会使daemon报InvalidInput；完全缺身份只warn并保持未验证，citations不完整同样warn而非返回Err。因此Gate层available意味着材料形成，不意味着有合格事实。`crates/akzio-daemon/src/evidence.rs`。

**第三种历史/显式模式 VerifiedSource**

- 仅requires_independent_fetch=true时，按canonical URL聚合并逐一Rust fetch。来源body≤2MiB、无redirect、允许text/JSON/xhtml；每个归属摘录须为16..4096 bytes且逐字落在body；每个来源全部摘录精确绑定才计verified_sources，所有来源完整才source_verified=true。Raw slice、hash、quote和citation再在materialization二次校验。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- 这证明“所引原文快照/字节闭包”，不证明报道真、投资方向正确或仓位安全；当前default新闻ModelReviewed不走这条Rust fetch路径。两模式的URL规范化不同：历史canonical_source_url只去fragment，不去UTM；review url_identity才去列明UTM。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/news.rs`。
- 数据侧交给研究的关键约束：当前news:未source_verified只可描述性ground，不得作为supporting_ref。只给消费接口定位，不在本报告展开研究验证逻辑：`crates/akzio-research/src/agent/errors_catalogue.rs`。

### 6.2 日线/市场快照/量化特征

- `capture_stock`先验资产symbol、active、tradable，读真实clock、近14日calendar，并从显式feed分别取snapshot/latest quote/latest trade。各组件按自己t/timestamp做cutoff过滤；缺时间也移除，但仅明确未来才计rejected。closed标closed_last_available_data，不伪称实时。`crates/akzio-ingest/src/market_capture.rs`。
- 研究bars窗口默认400日，calendar从start到cutoff美东日+30天；future close仅存forecast_session_closes元数据；已完成session必须close+20m≤cutoff。请求adjustment=all、sort=desc、limit=252，最多16页，每个body≤8MiB；重复bar/token、未完成session、非法OHLCV拒绝；最近完成session缺失或不足252根为Pending。`crates/akzio-ingest/src/session_bars.rs`；`crates/akzio-ingest/src/runtime.rs`。
- 最终只取最新252根、按日期升序；availability是所选session最晚close+20m。故“至少252根”是获取合格条件，最终研究payload不是无限长度一年历史。`crates/akzio-ingest/src/session_bars.rs`。
- 特征公式akzio.quant.daily.v1，含1/3/5/20/60/252日收益、20/60日年化已实现波动、ATR14、最大回撤、20日平均美元成交额、最新gap，保留adjustment/feed/sample/available_at；字段不足为None。**252日收益需要253根bar，所以固定252根payload的return_252d_ppm为空是源码规定，不应宣称已计算全年252期收益。** `crates/akzio-ingest/src/quant_features.rs`。
- raw_prices路径专供另类价格窗口/Outcome接口：不生成上述adjusted研究特征，还读取公司行动；不在此解释Outcome收益算法。`crates/akzio-ingest/src/materialization/materialize_normalized.rs`；`crates/akzio-ingest/src/session_bars.rs`。

### 6.3 FRED宏观与发布日历

- DFF/DFII10/VIXCLS窗口366自然日，vintage=S-1自然日，防同日revision穿越decision；请求realtime_start=realtime_end=vintage，响应两字段必须精确匹配。`crates/akzio-domain/src/workflow.rs`；`crates/akzio-ingest/src/direct.rs`。
- observations、vintage_dates、release_dates检查对应数组存在；此adapter并未对每条observation逐项证明经济发布时间、非空/数值完整或期待样本覆盖。空数组在shape门可通过；FRED availability由vintage日末确定，而非每项公布时刻。`crates/akzio-ingest/src/direct.rs`；`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。
- release_calendar Need为[S,S+45]、前日vintage。实际/fred/releases/dates请求include_release_dates_with_no_data=true、limit1000、按release_date降序；窗口start/end不是专门发送的发布日期过滤参数，而在取得响应后retain。当前没有release-calendar分页循环或覆盖完整性证明。未来已公布日历是计划元数据，不是未来经济观测值。`crates/akzio-ingest/src/direct.rs`。

### 6.4 期权链：四种上限、时间独立、未知不能补成0

- Need声明到S+120日，资源解析允许≤180日；**实际capture把结束截到min(request.end,start+30日)**。先取得cutoff-valid标的latest_trade.p或完整dailyBar.c，strike只取其90%..110%。`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-ingest/src/runtime.rs`；`crates/akzio-ingest/src/market_capture.rs`。
- contracts API及snapshot chain各最多4页、page size128、contracts最多512；按完整OCC symbol关联；contract必须active、underlying/expiry/strike满足过滤，重复contract/token报错。快照只接纳在contracts map里的symbol。feed在整次capture固定，不自动切OPRA。`crates/akzio-ingest/src/market_capture.rs`。
- 缺quote才补latest option quotes，每批100、最多6次；已遭snapshot权限拒绝则不补，补采中401/403则停止。后续cutoff过滤会移除未来/无时间quote，**被过滤的quote不在此函数启动第二轮补采**。`crates/akzio-ingest/src/market_capture.rs`。
- OI由contracts的open_interest/open_interest_date按OCC合并，只有日期严格早于cutoff美东日且值存在才接受；不把chain默认值当OI。IV/Greeks数值可以保留，但iv_timestamp、greeks_timestamp显式null，status=provider_does_not_supply_timestamp_unknown，不能借quote/retrieval时间。`crates/akzio-ingest/src/market_capture.rs`。
- stale_contracts诊断阈值900秒（缺quote也计stale）；这是绝对年龄统计，**不同于Need max_age=300秒，也不自动拒绝整条期权证据**。coverage包含requested/returned/trade/bidask/iv/greeks/OI、缺quote数、未来剔除、权限、错误、分页完整性及supplement次数；collection_status可permission_denied/provider_error/no_market_data/bounded_partial/available。只要返回AcquiredEvidence，quality仍为default(1M,true,true)，故不能将quality默认满值误说为期权覆盖满分。`crates/akzio-ingest/src/market_capture.rs`。
- Raw保存请求URL、开始/完成时间、响应/错误；Normalized保留精简requests与coverage/bounds。若函数早期`?`直接Err（如asset/contracts失败），内存ledger并不会自动形成Raw Artifact，不能保证“所有失败HTTP完整原文均已入Store”。`crates/akzio-ingest/src/market_capture.rs`。

### 6.5 官方ETF持仓/指数/每日杠杆条款

- QQQ=Invesco JSON、TQQQ=ProShares全基金CSV中对应基金、SOXX=iShares CSV、SOXL=Direxion CSV；完整source snapshot保留。effective_as_of不得晚于requested_as_of或cutoff日期。`crates/akzio-ingest/src/official.rs`。
- QQQ/TQQQ绑定Nasdaq-100，SOXX/SOXL静态registry为NYSE Semiconductor Index；TQQQ/SOXL属于DailyResetLeveragedEtf、daily_leverage_multiplier=3。静态profile是要求/身份注册，不是当前外部条款的已采事实。`crates/akzio-domain/src/instrument_evidence.rs`。
- 当前官方index获取实现：QQQ同时读details+page证明基金/指数字符串；TQQQ解析ProShares产品版本；SOXX从page找身份/指数/as-of。**SOXL index_metadata直接返回NotConfigured**（代码reason为browser challenge），不把holdings CSV冒充指数说明。`crates/akzio-ingest/src/official.rs`。
- TQQQ leverage_terms校验TQQQ/daily investment results/3x/Nasdaq-100等字符串，输出daily_reset、multiple、benchmark、effective日期和terms_text_sha256；**没有结构化费用/损耗率估计，也没有完整条款正文进入这个Normalized输出**。SOXL terms直接NotConfigured，reason为未有issuer page/PDF text parser；本次没有实测网站挑战/PDF状况。`crates/akzio-ingest/src/official.rs`。
- 官方source_document标official_direct、verified_source、policy身份、source_closure=complete、required/verified count=1、raw hash/revision；不走模型新闻quote逐字绑定。provenance.published_at取HTTP Last-Modified/Date（部分产品版本parser另处理），若无则effective日期UTC午夜；它不是一律发行人精确发布时间。`crates/akzio-ingest/src/official.rs`。

### 6.6 执行数据侧接口（不扩展到ExecutionGate裁决）

`refresh_execution_snapshots`重找六个scheduler Need，先并发account/positions/open_orders/fills，再并发quotes/clock，生成execution.snapshot.account/quotes/clock，返回`ExecutionSnapshotRefresh { account, quotes, clock, quote_error }`；账户/clock缺失报错，quotes可以通过quote_error保留失败信息供下游fail closed。旧研究快照不充当本次refresh成功。`crates/akzio-daemon/src/evidence.rs`。

clock/quotes adapter根据真实clock+calendar判断TradingSession，Overnight额外检查四资产资格；SIP映射boats，基础IEX配置映射overnight，夜间不退回iex；其余时段继续显式股票feed。这里的GET成功不代表ExecutionVerdict、订单提交或成交。`crates/akzio-ingest/src/paper_session.rs`。

## 7. Context候选、精确授权、必需闭包和预算

### 7.1 从候选到Manifest

- `assemble`只接受调用方显式candidates，再按Contract允许召回Lesson/Experience；不是全Store自动供模型检索。Synthesizer可从Critique合法源Manifest闭合被审Claim，但须同Run、源Attempt/Contract和实际被源Manifest选择。`crates/akzio-context/src/context_broker/manifest.rs`。
- 拒绝Raw、未允许kind/source、错误内部producer；同Run或受控跨Run overlay/canary资格另验。精确内部配对包括：akzio.ingest+SemanticDetail仅collection_status/canary.evidence_snapshot/evidence.option_projection/research.supplement.result；akzio.agent按Claim/Critique/DecisionProposal/ProposalReview对应固定producer；执行和学习类型各有固定配对。**不是akzio.*通配；但外部source fallback自身只检查NormalizedEvidence/SemanticDetail/RegimeSnapshot kind，不能说该函数对所有外部producer也做相同枚举校验。** `crates/akzio-context/src/context_broker/policy.rs`。
- 外部NormalizedEvidence和SemanticDetail统一UntrustedEvidence；先扫描instruction-like strings，再按FinancialContentPolicy隔离高风险金融内容，quarantined只留refs/reason/indicators，不进入readable。金融内容评估中ModelReviewed只扫描reviewed_facts，official_direct扫描结构化normalized，其余合并raw+normalized；这是启发式治理，不是语义正确证明。`crates/akzio-context/src/selection.rs`；`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-ingest/src/financial_content.rs`。

### 7.2 必需闭包不是“选几份够用即可”

- Critic/Synthesizer/ProposalReviewer：候选中的Claim/Critique本体必需；Claim.source_refs为grounds，Critique.source_refs含target、grounds和核验引用。引用必须存在于同一候选集合且kind匹配；当前reviewed版本把二者引用都加入required。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/research.rs`。
- reviewed版还把DecisionProposal、ProposalReview、research.supplement.result列必需；proposal claims/critiques、numeric_basis.inputs、allocation.evidence_refs必须全部在候选中并入闭包；ProposalReviewer须恰好一份最终提案。此处只说明数据输入，非审阅业务判定。`crates/akzio-context/src/context_broker/manifest.rs`。
- Outcome另有Decision/DecisionContext/ExecutionContext/OutcomeSchedule、当前stage packet或sealed Outcome；DecisionContext引用的claims/critiques以及已有Retrospective列必需。`crates/akzio-context/src/context_broker/manifest.rs`。
- `required_role_inputs`在候选排序前构集合，预算循环必需超限报MissingRequiredInput；mint前用实际selected集合再验闭包。Critic还有Claim+NormalizedEvidence最小结构要求。**Analyst没有这类强制四资产/四新闻/宏观全齐的required集合**，其balanced bundle是尽量覆盖，不是强制完全覆盖。`crates/akzio-context/src/context_broker/manifest.rs`。

### 7.3 24件、字节与token精确预算

| 默认活动Contract | 顶层件数 | 模型投影max_bytes | 选中source总max_source_bytes | Context估算token |
|---|---:|---:|---:|---|
|Analyst / Critic / ProposalReviewer|24|131072（128KiB）|524288（512KiB）|角色definition.budget.max_input_tokens|
|Synthesizer|24|196608（192KiB）|786432（768KiB）|角色definition.budget.max_input_tokens|
|Outcome|24|131072（128KiB）|524288（512KiB）|32768（32*1024）|

依据：`crates/akzio-research/src/agent/errors_catalogue.rs`。`ContextPolicy.max_source_bytes=None`时回退max_bytes，历史Manifest.projected_bytes=None时也用total_bytes；因此不能把新表强套旧冻结Contract。`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/selection.rs`。

每件projection序列化后按ceil(bytes/4)、至少1token估算，source字节和projection字节分别累计；达到件数/source_bytes/projection_bytes/tokens任一上限跳过可选项、required报错。此估算不是provider tokenizer的精确计数；外层实际完整模型请求及累计输入预算由研究Runtime负责，未在本报告复述。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/lib.rs`。

**可选背景优先顺序**：Analyst和Synthesizer的balanced selector先collection status，再按资产分组bars/news，再DFF/DFII10/VIXCLS，再earnings_event_calendar，最后option_chain；组内按confidence降序、source bytes、ArtifactId稳定排序。它只优先级，不绕过预算；随后required再次置顶。其他角色不调用该balanced selector，而按purpose_rank/直接引用等排序。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/selection.rs`。

### 7.4 Manifest与ReadGrant不是一回事

- Manifest payload固化ordered selections、quarantined、total_bytes/projected_bytes/estimated_tokens、contract_hash、input_hash；input_hash只哈希有序ID/kind，不含reason或预算metadata。CAS Artifact仍绑定完整payload、producer、origin和source_refs。`crates/akzio-domain/src/context.rs`；`crates/akzio-context/src/context_broker/manifest.rs`。
- 先持久化Manifest，再mint内存ReadGrant：精确manifest_id/run/task/attempt/lease/epoch/contract/readable/raw_source_closure/expiry；TTL不能绕过lease或当前Attempt权限。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-domain/src/context.rs`。
- 每次读重载persisted Manifest，检查Artifact与payload、Contract、Run/Task/Attempt、producer、kind、source_refs、预算和input_hash；readable必须正好等于selected IDs。普通读取拒绝Raw与AgentTurn/ToolCall/ToolResult。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/grants.rs`。
- `read_raw`/`read_raw_document`是库中存在的独立接口，不等于模型有Raw权限；只有policy.allow_raw_reread时沿source_refs构raw closure，且只能显式读取；当前活动Contract该标志全false、读工具表不含read_raw。`crates/akzio-context/src/context_broker/grants.rs`；`crates/akzio-context/src/context_broker/policy.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
- `read_authority_document`只读当前Contract声明的精确governance/role/output schema/tool schema BlobRef，不接受任意文件或Blob。`crates/akzio-context/src/context_broker/grants.rs`。

### 7.5 父子与恢复（数据侧）

- child Context只可来自同Run当前成功父Attempt的已选文档或其真正输出，且source lineage闭合到父Manifest；child policy/预算再收缩，raw closure须为parent子集；不额外Lesson召回。`crates/akzio-context/src/context_broker/selection.rs`。
- success proof恢复父Manifest仅供历史验证；synthetic grant expires_at=now，不能以普通live读取复活父权限；真正child再mint当前身份。`crates/akzio-context/src/context_broker/grants.rs`。
- option projection复用只沿同Run同Task的Retry/Recovery祖先，要求source_ref、Blob、Contract、producer、RunScoped、origin完全匹配；相同内容可复用Artifact ID，但外层必须签发新Attempt Manifest/ReadGrant。改变source、Contract或projection内容不复用。`crates/akzio-context/src/context_broker/materialization.rs`。
- 实际消费者以ContextQueryScope::for_node生成scope，有parent走proof，其他走assemble；随后materialize+model_context，没有从这条接口直接绕过Context读全部Store。只核查此调用口，不展开Runtime状态机。`crates/akzio-research/src/agent/runtime_run.rs`。

## 8. Projection、遗漏与只读工具

### 8.1 模型到底得到什么

- materialize按Manifest逐项授权读取。每项先进入metadata ledger（id/kind/source/observed_at/tokens/reason/grant identity），只有must_read_class识别的对象才附内联value。当前research version>=65声明read_tools=[]，task_contract可见授权种类、来源及各预算。`crates/akzio-context/src/context_broker/materialization.rs`。
- research外层ledger projection_version=3；NormalizedEvidence投影version=1；option version=3；collection-status投影version=4；Outcome外层version=2。不要把这些不同对象的projection_version混成全系统一个版本。`crates/akzio-context/src/context_broker/materialization.rs`。
- 普通NormalizedEvidence投影只保留source/resource/time_basis/quality/quant_features/value_summary及full_document提示；bars/observations只保留最后5行并保留original_count；移除session_closes，但calendar source/forecast_session_closes等其他summary字段仍在。**Need/Raw/provenance/financial_content等不在该通用顶层投影中**，完整CAS不改写。`crates/akzio-context/src/context_broker/materialization.rs`。
- holdings识别rows或Invesco holdings数组，保留全部列，用发行人权重列优先排序，取前12行，保留精确数值、original/retained/omitted_count/ranked_by；找不到权重列保持源序。排序优先weight/holdingspercent/percentageoftotalnetassets，再exposure/notional，不擅自把名义金额当权重。`crates/akzio-context/src/context_broker/materialization.rs`。
- 所有Normalized投影以及Claim/Critique/Decision/Retrospective内的长字符串（函数递归，**不只字段名为narrative者**）超过600字符裁成前600+明确truncated提示；所以长statement、date_basis等也可能裁剪。数字不改。`crates/akzio-context/src/context_broker/materialization.rs`。
- collection-status按status分组仅暴露resource/criticality/diagnostic，省略Need Artifact refs，明确“Collection status不是ReadGrant；available但未选中不是不可用”。`crates/akzio-context/src/context_broker/materialization.rs`；`crates/akzio-context/src/context_broker/guidance.rs`。

### 8.2 期权投影的两层含义

- 聚合遍历完整已采snapshots计算IV min/max/mean、contracts/quote/OI/expiry计数；保留coverage/feed/bounds/decision_cutoff/time_basis/quality/provenance，缺失字段列missing_items。expiry最多列32、available_fields最多64；仅前两个OCC排序样例，显式sample_contracts_omitted及“非代表分布”说明。**聚合完整只指已采部分，不代表全市场/全120天完整**。`crates/akzio-context/src/context_broker/materialization.rs`。
- assembler条件满足时把每个option NormalizedEvidence都转SemanticDetail，代码**没有先判断它是否“过大”**，与函数注释“when too large”不同。新Artifact.source_refs=[原Normalized]，payload.source_artifact记录原id/kind/blob_hash/logical_bytes/producer/source_refs。Context预算算选中的投影Artifact自身字节，而不是连原链一起算；原链仍在CAS，但只有source_ref不是授予其readable。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。
- 对无读工具research，projection_only_guidance把**顶层**full_document替换为“本轮仅提供投影，原文未开放”。它不递归：holdings嵌套full_document和通用truncated marker仍可能说read original，应由上层实际read_tools=[]解释，不能把提示文字当权限。`crates/akzio-context/src/context_broker/materialization.rs`。

### 8.3 五个工具实际限制（当前仅Outcome有工具授权）

| 工具 | 读取范围/限制 | 源码 |
|---|---|---|
|read_document|精确selected文档；序列化响应value>32KiB返回DocumentRequiresRange，不静默截断|`crates/akzio-context/src/context_broker/reads.rs`|
|read_range|精确blob半开byte区间、单段≤32KiB、必须UTF-8边界；返回total_bytes|`crates/akzio-context/src/context_broker/reads.rs`|
|search_context|query≤256字符、1..16结果、仅当前selections，大小写不敏感子串；offset基于document_value_text，不是直接所有CAS bytes|`crates/akzio-context/src/context_broker/reads.rs`|
|read_claim_evidence|先读Claim，再去重展开每个ground，每个ref必须已授权且kind一致；不自动扩grant|`crates/akzio-context/src/context_broker/reads.rs`|
|compare_sources|2..4个不同已授权Artifact，返回source/time和compact projection，不判定谁正确|`crates/akzio-context/src/context_broker/reads.rs`|

补充`document_range_metadata`只在已授权后给可证的完整JSON顶层value范围，不能代替grant。32KiB硬检查在read_document_result和read_range中，不应宣称所有五工具组合响应各自都由本文件硬卡32KiB；read_claim_evidence在此未做合并响应32KiB检查，仍需外层累计输入预算。`crates/akzio-context/src/context_broker/reads.rs`。

## 9. Lesson查询与实际消费边界

1. Scope由已核验node生成：四资产全集、recipe完整ID；Synthesizer/ProposalReviewer全部T1/T3/T5，其余取execution_spec.horizon；未知recipe空scope。空query维度不是任意匹配，只匹配该维度未限定的Lesson。`crates/akzio-domain/src/context_scope.rs`。
2. `learning_query_scope`先清空caller regimes，仅从当前候选中通过kind/source/policy/Run/type校验的DecisionTime RegimeSnapshot重建，不取正文tag，不取ExPost。当前默认Synthesizer permitted_kinds并不含RegimeSnapshot，所以**默认路径下不能假定有regime标签**。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-research/src/agent/errors_catalogue.rs`。
3. `learning_candidates`只在Contract允许Lesson时扫描`active_lessons_snapshot()`全部Active heads；按scope、usage、governance、source、overlay筛选，再相关性regime/stage/asset/horizon、updated_at、lesson_id排序；去相同规范化内容/范围指纹但保留显式冲突。`crates/akzio-context/src/context_broker/manifest.rs`。
4. max4是**四条Lesson**，不是四组无限大小冲突组；冲突关系在eligible集合构无向连通组，整组放不下则不选。超过4条的大冲突组无法进入本轮候选。`crates/akzio-context/src/context_broker/manifest.rs`。后续assemble还会逐Artifact应用总体预算且Lesson非required，因此“冲突组最终必定完整送模型”不能只由这一召回算法证明。
5. governance要求无quarantine、valid_from≤now、有效期未过、使用次数未到上限、contradiction/post-use-failure为0、regime兼容；有compatibility但scope.regimes为空则不允许。`crates/akzio-domain/src/lesson.rs`。
6. 召回审计保存`learning.retrieval.audit`，含最终query scope及每项scope_mismatch/governance_ineligible/eligible/exact_duplicate/rank_or_conflict_capacity等记录。此处selected表示召回候选；后续总Context预算仍可能不选，需再看Manifest/context.coverage。`crates/akzio-context/src/context_broker/manifest.rs`。
7. 发现Lesson旧来源对应同source/resource的新Normalized、更晚于last_revalidated且blob不同，只生成RunScoped `learning.revalidation.suggestion`，lifecycle_changed=false，不认定矛盾、更不自动激活/失效Lesson。`crates/akzio-context/src/context_broker/coverage.rs`。
8. Experience另扫描最近100项，最多4条，须Canonical+canonical learning Run、learning_eligible、记录的influence subject一致、policy head状态允许。没有把隔离Debug/非合格Outcome经验自动送进正式上下文。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/policy.rs`。
9. **静态消费缺口：** 当前普通assemble给Lesson selection.reason=lesson，must_read_class没有Lesson/Experience/CandidatePolicy分支，默认只进入ledger metadata；metadata本身没有Lesson statement/recommended_behavior。research无工具，通用model_context也未读取额外正文。因此应表述为“具备受治理Lesson候选召回、授权与审计”，不能无保留说“默认Synthesizer已利用Lesson正文”。若主助手发现其他专门注入路径，应以额外源码/运行request证据修正此条；本报告看到的默认路径见 `crates/akzio-context/src/selection.rs`、`crates/akzio-context/src/context_broker/materialization.rs`、`crates/akzio-research/src/agent/runtime_run.rs`。

## 10. 缺口、过期、截断、恢复、补采数据接口

### 10.1 当前有界补采（仅数据侧，不展开角色重跑/修订）

- 公共门面`EvidenceAcquisition::prepare_supplemental / supplemental / note_abandoned`委托daemon；旧prepare接口收ResearchIntent并生成EvidenceNeed，当前shared research节点另走类型化SupplementalIntent→冻结Need绑定，不应混成模型自由写资源。`crates/akzio-daemon/src/application/evidence_acquisition.rs`；`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 当前SupplementalIntent只含kind(news/price/macro)、assets、series、query；唯一资产/序列、非空query、Macro序列仅DFF/DFII10/VIXCLS/DGS2/DGS10。query用于目的，不透传任意URL/日期；News/Price命中冻结同类Need，Macro可用冻结series模板替换受支持序列、保留窗口，重新走adapter同一个GovernedResource parser。`crates/akzio-domain/src/research_review.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 全run单轮最多8个distinct resources；四资产news展开4条。每项started先持久化再Need及I/O；恢复已done复用处置，started无done标unknown_after_crash并不重发，预算不重置。此条只定位数据侧审计接口，不接管协调逻辑。`crates/akzio-daemon/src/application/research_supplement.rs`。
- 当前shared节点取原Normalized的最早decision_cutoff，调用acquire_supplemental_evidence时传该原cutoff；底层recipe=research.supplement时materialize也用原cutoff，而旧其他调用者用新Utc::now。**不能笼统写“每次补采扩大cutoff到抓取完成时刻”。** `crates/akzio-daemon/src/application/research_supplement.rs`；`crates/akzio-daemon/src/evidence.rs`。
- 新事实条件=quality.citations_complete、news:还须source_verified=true、available_at≤原cutoff、同resource的事实内容不同；对比忽略retrieval/provider包装变化，bars比较bars/feed/adjustment，FRED比较observations/units，news比较reviewed_facts/verified。`crates/akzio-daemon/src/application/research_supplement.rs`。
- **重要边界推导：** 当前news默认ModelReviewed始终source_verified=false，因此即使新闻二次审阅发现新描述性事实，也不能通过该补采“新增合格事实”的news准入条件；可能记录no_new_facts，或因无发布时间导致晚于原cutoff而采集失败。不是自动切换VerifiedSource，也不是可用文字等于允许重跑。`crates/akzio-domain/src/workflow.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。
- 旧validate_supplemental_need仅bars、五个宏观序列、六种news topic，SEC补采明确不启用；它与当前typed binding都不是运行时Agent联网工具。底层补采逐项写Raw/Normalized，未提供跨所有resource原子事务；shared节点调用一项一处置。`crates/akzio-daemon/src/evidence.rs`。

### 10.2 用户可理解的故障层级

| 情况 | 数据侧动作 | 不能推出的结论 |
|---|---|---|
|adapter未配置/权限/限流/暂未发布|逐Need unavailable+分类诊断，保留其他成功项|不是所有研究必定失败，也不是缺口已补齐|
|future-data污染|整Gate Err，状态与可保存成功项仍留诊断|不能说可选数据污染可以忽略|
|provenance/citation/policy/Store错误|保留原错误，fail closed|不是普通“新闻没搜到”|
|新闻审阅无事实|可保存news_evidence_status失败、零coverage；Gate可能available|available不等于facts_ready/source_verified|
|日线不足252/最新完成session缺失|Pending，不拿旧不完整历史冒充最新完整payload|下载成功不等于价格可用于研究|
|期权权限/分页/缺字段|coverage/bounds/unknown/truncated明确保留，可能仍有Normalized|quality默认1M不代表完整链|
|Context可选项超预算|不选并记录exclusion reason|未选中不等于未采到、不存在或内容被证伪|
|Context必需闭包放不下|MissingRequiredInput；不返回完整可用Context|不能靠只保留摘要引用绕过闭包|
|旧ReadGrant/lease过期|read校验拒绝；新Attempt须新grant|CAS对象存在不代表当前有权限|
|补采started后崩溃无结果|unknown_after_crash，不重发|不能猜provider没收到请求|

对应实现：`crates/akzio-daemon/src/evidence.rs`；`crates/akzio-ingest/src/news.rs`；`crates/akzio-ingest/src/session_bars.rs`；`crates/akzio-ingest/src/market_capture.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/grants.rs`；`crates/akzio-daemon/src/application/research_supplement.rs`。

## 11. 文档 / 注释 / 代码差异与写总说明时的注意点

| # | 表述或容易误读处 | 当前实现结论与证据 |
|---:|---|---|
|1|契约6节写“14天新闻VerifiedSource”|与当前policy4/news router不符，当前为ModelReviewed。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-domain/src/workflow.rs`。news-evidence较新说明与代码一致：`docs/news-evidence.md`。|
|2|“新鲜度看内容时间，不看下载”|内容cutoff确有独立检查，但通用stale还是observed_at。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-ingest/src/materialization/materialize_normalized.rs`。|
|3|普通Context128KiB / Synth192KiB及Outcome原文128KiB|这些是projection max_bytes；活动构造max_source_bytes一律4倍，Outcome也是512KiB。文档`docs/agent-runtime-contract.md`；代码`crates/akzio-research/src/agent/errors_catalogue.rs`。|
|4|40项全部前置采集|六项只deferred，PositionPlan没有这六Need；研究底层仍可能读clock/quote。`docs/agent-runtime-contract.md`较明确；代码`crates/akzio-daemon/src/evidence.rs`、`crates/akzio-ingest/src/market_capture.rs`。|
|5|120天期权需求等于120天覆盖|registry+120、adapter+30且最多4页512合约；文档当前规则19行已记30天，但需总说明同时提请求/实际差异。`crates/akzio-domain/src/instrument_evidence.rs`；`crates/akzio-ingest/src/market_capture.rs`。|
|6|有杠杆条款Need即实现完整损耗/费用事实|SOXL terms/index NotConfigured；TQQQ只校验/输出有限daily_reset/3x/benchmark/hash等，无损耗率或费用结构。`crates/akzio-ingest/src/official.rs`。|
|7|news文档“遗漏/重复/窗口外均拒绝”可读为全包拒绝|当前逐事实分区，合法和失败共存，usable优先model_reviewed。`docs/news-evidence.md`；`crates/akzio-ingest/src/news.rs`。|
|8|“只在期权原文过大时做SemanticDetail”注释|assembler条件无大小判断，凡符合kind/source许可的option Normalized都转换。`crates/akzio-context/src/context_broker/materialization.rs`对比`crates/akzio-context/src/context_broker/manifest.rs`。|
|9|召回Lesson即模型收到正文 / 冲突组总能完整保留|默认Lesson仅metadata，无research读取工具；冲突分组是候选召回层，最终预算逐项再过滤。`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。|
|10|原始网页从不进入研究Context|活动ModelReviewed移除provider transcript且research只投影；但库中历史DiscoveryOnly/VerifiedSource可保留provider_result/provider_request于Normalized，不能把新路径描述无条件套旧对象。`crates/akzio-ingest/src/adapters.rs`；`crates/akzio-ingest/src/news.rs`。|
|11|投影提示总能指导read original|当前研究无工具；只有顶层full_document被替换，嵌套和truncated提示仍有读原文用语。`crates/akzio-context/src/context_broker/materialization.rs`。|
|12|所有Context组装路径在mint前都走同样required算法|普通assemble确实先required/mint重验；assemble_child直接预算/建Manifest，无同样required_role_inputs调用，后续materialize中的validate_manifest_closure才检查当前selected闭包。应分别描述，不把普通路径排序保证套给child。`crates/akzio-context/src/context_broker/selection.rs`；`crates/akzio-context/src/context_broker/manifest.rs`；`crates/akzio-context/src/context_broker/materialization.rs`。|

另外契约21行仍有“其他purpose保持原协议”，126行已明确Paper/PositionPlan/Shadow统一单次结构化；当前活动Contract没有研究读工具，Outcome单独有。属于同文档滚动更新留下的范围说明不一致，由角色worker详细处理，本报告不展开。`docs/agent-runtime-contract.md`；`crates/akzio-research/src/agent/errors_catalogue.rs`。

## 12. 给主助手优先引用的18个源码入口

1. `crates/akzio-domain/src/workflow.rs` — 固定基础Need与E/D分类。
2. `crates/akzio-domain/src/instrument_evidence.rs` — 40项扩展清单/日期/去重。
3. `crates/akzio-daemon/src/evidence.rs` — 精确政策核对、deferred、逐项状态、部分成功/污染。
4. `crates/akzio-daemon/src/application/evidence_acquisition.rs` — Gate完成边界、Shadow父证据接口。
5. `crates/akzio-ingest/src/materialization/materialize_raw.rs` — Need/request/source授权。
6. `crates/akzio-ingest/src/materialization/materialize_normalized.rs` — Raw/Normalized血缘、time basis、stale实际公式。
7. `crates/akzio-ingest/src/news.rs` — discovery/review、事实分区、source_verified=false、Raw审计。
8. `crates/akzio-ingest/src/adapters.rs` — discovery与独立snapshot差异、source白名单。
9. `crates/akzio-ingest/src/session_bars.rs` — 交易calendar、252根、close+20m。
10. `crates/akzio-ingest/src/market_capture.rs` — 期权界限/权限/截断/时间未知。
11. `crates/akzio-ingest/src/direct.rs` — FRED vintage/发布calendar及校验边界。
12. `crates/akzio-ingest/src/official.rs` — 官方持仓、SOXL缺口、杠杆条款和来源身份。
13. `crates/akzio-research/src/agent/errors_catalogue.rs` — 当前24件/128–192KiB/4倍来源预算、无research读取工具。
14. `crates/akzio-context/src/context_broker/manifest.rs` — required闭包、精确校验、预算拒绝。
15. `crates/akzio-domain/src/context.rs` 与 `crates/akzio-context/src/context_broker/grants.rs` — 当前Attempt精确ReadGrant。
16. `crates/akzio-context/src/context_broker/materialization.rs` — 内联/metadata区别、投影/期权恢复。
17. `crates/akzio-context/src/context_broker/manifest.rs` 与 `crates/akzio-domain/src/context_scope.rs` — Lesson scope/全Active召回/四条冲突限制。
18. `crates/akzio-daemon/src/application/research_supplement.rs` — 补采绑定、原cutoff、started恢复、合格新事实。

## 13. 未核验边界与交付建议

- **未运行验证：** 无build/test/fixture；因此不证明当前dirty工作树可编译、单测通过或以上分支在某个Run中发生过。测试源码里的assert均未当作验收。
- **未联网：** 不确认当前provider可用性、账号IEX/SIP/OPRA资格、网站是否仍browser challenge、实际获取条数、新闻review质量、FRED样本完整度或实际报价新鲜度。
- **未读Store/配置：** 不确认某个Run冻结的是哪版Contract/policy、是否有active Lessons、实际Manifest选中多少件、哪个source被quarantine、哪个请求恢复后unknown。文中默认Contract参数来自构造代码，不代表历史Run或当前部署身份。
- **不覆盖其他worker职责：** 不判定Scheduler触发、Store事务/CAS权限实现、研究角色修订流程、Decision目标、ExecutionVerdict、Paper提交、Paper成交或Outcome完成。仅把接口边界交给主助手衔接。
- **静态推导须保留限定语：** Lesson仅metadata、候选冲突组可能被最终预算裁开、当前ModelReviewed新闻不能满足补采source_verified新事实门、期权无大小条件替换、stale基于retrieval，均由已列默认代码路径推导，未做针对性运行复现。
- 主说明建议固定使用“已采集 / 有可用事实 / 已source-verified / 已选入Context / 已给模型内联内容 / 已工具读取 / 已获方向使用资格”这些分开的状态，不能互相替代。本报告各节给出这些边界的源码实现。

## 14. Memory使用记录（只定位，不作为当前事实）

只查询了注册表以定位本仓库架构和证据边界，全部正文事实重新从当前文件核查；没有据旧memory宣称当前运行结果。

- `/Users/alixeu/.codex/memories/MEMORY.md`：仓库定位、源码核查与运行验证需区分、基础关键词。
- 对应历史rollout id：`01a0e619-cd21-7130-8b8a-f56a1adb918c`。未打开rollout文件，不引用其内容。
