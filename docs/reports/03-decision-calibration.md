# 03 — DecisionGate 与 DecisionPolicy SQL 校准全过程（当前工作树只读核查）

> **来源维护**：源码入口仅标仓库相对文件与命名职责，不绑定随编辑移动的数字位置。本文结论仍受文首源码快照与验证范围约束；本次导航格式调整没有重跑 Core、Paper 或跨日 Outcome。

## 0. 核查口径与结论

- 日期：2026-09-29（会话本地时区 Asia/Singapore）。核查对象为 `/Users/alixeu/project/akzio-signal-intelligence` **当前工作树**，不是仅 HEAD。核查时 HEAD：`e4292f09acf5b3798bf16de26718ceb85046a190`。
- 已先读根 AGENTS、运行时契约 3/7/8/11/12（并补读相关 1/2/4、研究协议）、开发 Workflow readiness/SQL 校准及相关 CONTEXT、Debug 控制。记忆仅作定位线索；以下业务结论均重新查当前源码。
- **唯一写入为本报告及其必要父目录**；没有修改源码、配置、Store、历史 CAS 或内存规则，没有读取实际配置/密钥/认证 token，没有打开实际 Store，没有模型、网络、Broker 请求。未启动 daemon、CLI 校准命令、测试或构建。读取到的测试代码不是本次通过的测试。
- 用户许可 worker；当前可用工具没有独立 subagent/worker 派发接口，因此由本核查直接完成，没有擅自创建新用户 chat，也没有接管其他 worker 的订单执行或 Outcome 数值学习任务。
- 初始与收尾期间既有改动包括 `.github/workflows/ci.yml`、`AGENTS.md`、`README.md`、daemon 的 `http.rs/http_launch.rs/lib.rs/orchestration/health_canary.rs`、Store `debug_bundle.rs`、四份 docs、`scripts/run_core.py` 和未跟踪 `scripts/test_run_core.py`。均未触碰；此处事实只对应当时读取的工作树，后续变化须重新核查。

**结果先行：**

1. `DecisionProposal` 的 Rust wire 类型仍是 `DecisionDraft` 别名。模型给 12 forecasts、四资产+现金的研究意图、引用、numeric_basis 等；Rust 独立保留 `research_plan.raw/validated`，再用 **forecasts + 冻结校准 + 风险模型** 另算 `DecisionContext.target` 与同值的 `Decision.targets`。不是“照抄 allocation 后下单”。[^D01][^D02][^D03]
2. 缺 active Policy 的 canonical PositionPlan 可以完成合法 Decision，研究配置可非零、targets 全零、无执行/Outcome；canonical Paper 冷启动另走无交易 approval 的研究/NoOrder 入口以积累未来真实标签。缺 Policy 的真实隔离 Debug PositionPlan 只允许手动研究、禁止 resume/step Decision。[^D04][^D05][^D06][^D07]
3. **拒绝提案、正式目标归零、Execution NoOrder 是三种不同结果。** provenance/12 slots/精确方向资格/Reviewer 失败会在 Decision commit 前返回错误；已通过研究检查但校准/信号不足才可能形成研究非零、targets 零。研究过程质量等 hard blocker 也可能与非零 targets 同存，由 ExecutionGate 阻断。[^D02][^D08][^D09]
4. SQL 链为 `readiness → set-risk-limits → collect → build → inspect → validate → activate`。readiness 的成熟度不是 candidate 存在性；build 不激活；validate 的结构合法不是 decision-capable；activate 才更新 installation/history/head。[^C01][^C02][^C03][^S01][^S02]
5. **发现实际边界/漂移：** canonical daemon 未显式重做 active Policy 与当前 Synthesizer Contract 的比较；Policy 启动后是内存快照而非逐 Decision 热加载；collect 的历史 model version hash 是从当前配置写入样本，不是逐 AgentTurn 比对；preflight 非严格 Store 只读；asset_eligibility 摘要仍要求 NewsEvent 而实际 slot 谓词只强制 price+macro。详见第 10 节。以上为静态源码结论，不是已经复现的生产事故。[^I01][^I02][^I03][^I04][^C04][^D10]

## 1. 角色与名词不混用

| 对象 | 输入/权威 | 输出及边界 |
|---|---|---|
| 研究 proposal / allocation | Synthesizer 已授权 projections；Rust 提交校验 | 模型意图，不是订单；`target_weight_ppm` 字段名也不赋予执行权 |
| `ResearchPlanReview` | raw allocation、forecasts、Claim/Critique、静态 gross cap | raw/validated、adjustments、研究状态、execution_status |
| `DecisionPolicy` | Rust execution crate 定义；SQL active Artifact 装载 | 概率校准、风险参数与目标计算限制，不是模型生成的批准 |
| `DecisionContext` / `Decision` | Rust DecisionRuntime | Context 的风险、provenance、blockers、trace；Decision 的 forecasts 和正式 targets |
| domain `PolicySubject/PolicyState/CandidatePolicy` | Memory / Contract / Topology 生命周期 | 属于学习/候选治理 namespace，**不是**上述资金配置 DecisionPolicy |
| domain `QualificationStageReceipt` | behavior bundle、qualification key、场景、双侧输出、评估者哈希 | Replay/Adversarial/ExecutionSimulation/Shadow/Canary 的不可变 receipt；不替代校准或 Paper approval |

`QualificationVerdict` 只有 Pass 是可晋级；receipt 的 identity hash 覆盖输入身份、metrics、verdict、source_refs、时间；本报告不展开这些阶段的执行和学习算法。`DecisionRuntime` 对应用的 Experience/CandidatePolicy 另查 Canonical Paper 来源及 subject active head，不能把经验 active 与 DecisionPolicy active 混为一个 head。[^D01][^D11][^D12][^Q01][^Q02]

## 2. 从研究提案到持久化 Decision

```text
成功研究提交 → RunScoped DecisionProposal（DecisionDraft wire）
  ├─ proposal / 唯一 Synthesizer Manifest / permit / Contract provenance
  ├─ draft.validate：12 slots、研究配置守恒、引用 kind 等
  ├─ Contract >= 67：numeric_basis 17项 + final ProposalReview 精确授权
  ├─ Claim/Critique 当前版本语义与方向资格（>=57 单 Claim 闭合规则）
  ├─ review_research_plan(raw) → raw + validated + adjustments + statuses
  ├─ critical Claim / conflicts / consensus / 过程质量 → hard/soft blockers
  ├─ DecisionPolicy(forecasts, effective_confidence, now)
  │     → target + PortfolioRiskAssessment + runtime/horizon trace
  └─ DecisionContext(target, policy_hash, validity, research_plan, blockers...)
        + Decision(targets=同一target, forecasts=原draft.forecasts, research_plan)
          → 单次 Store::commit_attempt，两 Artifact + TaskStatus::Succeeded
```

- daemon 先从 Decision task 的成功依赖链找 final review：accepted 则选择该 Review 绑定的 proposal；有拒绝 review 则直接失败，**不回退旧 proposal**；无 review 才取 terminal proposal，随后当前 Contract 的 DecisionGate 仍会要求 review。[^D13]
- `DecisionRuntime::new` 只校验并按值持有 Policy。`decide` 先校验 TaskWritePermit，再重读 Store 中 proposal 和闭包；不会在此拟合或更换 Policy。[^D02]
- `DecisionContext` 额外保留 applied/rejected learning refs、material conflicts、policy hash、portfolio risk、有效期、过程质量、每资产资格投影、数值分支 trace；`behavior_bundle_hash` 在此构造为 `None`。`Decision` 保存有效共识置信度和原 forecasts，没有用 calibration 覆写原 forecast 数字。[^D03]
- Context source_refs 为 proposal + 唯一 Manifest + selected；Decision sources 为 Context + proposal。Paper 按 purpose 生成 Canonical Decision artifacts，其余为 RunScoped；**这不能替代 Store 隔离资格**，隔离 Debug Paper 即使 payload lifecycle 为 Canonical，collect 仍拒绝整个 Store。[^D03][^C04]
- 原子发布在两个 payload 都 validate 后；任何前置 Err 都不发布半份 Decision。不可把“原 proposal 已存”说成“Decision 已生成”。[^D03]

## 3. Provenance、闭包、12 slots 与 ground 精确资格

### 3.1 Proposal 与 Manifest

Proposal 必须：RunScoped；producer=`agent.research.synthesizer`；source_family=`akzio.agent`；origin Run 等于 permit Run；task/attempt 存在；provenance producer_contract_hash 等于 origin Contract。它必须恰好引用一个 ContextManifest。[^D14]

Synthesizer Manifest 必须：RunScoped；producer=`context.research.synthesizer`；source_family=`akzio.context`；同 Run、同 proposal task/attempt/Contract；payload schema 与 Contract 相符，selections 非空，每项 reason 非空、estimated_tokens 非零，NormalizedEvidence/SemanticDetail 信任级为 UntrustedEvidence。[^D15]

递归闭包核查：selected/quarantined 无重复且互斥；manifest.source_refs **精确等于** selected ∪ quarantined ∪ ancestor manifests；input_hash 重算匹配；quarantine kind 与 indicators 合法；重算 total/projected bytes 和 estimated tokens；拒绝 selected RawEvidence/AgentTurn/ToolCall/ToolResult；祖先来源需同 Run、有完整 origin，并防环。这里不能说所有祖先必须和当前 Synthesizer 同一 Contract——每个祖先绑定自己的 Contract。[^D15]

Draft claims、critiques、evidence、allocation evidence_refs、applied/rejected learning refs、conflict refs 都需在 selected 集。selected Lesson/Experience 必须明确 applied 或 rejected。**numeric_basis.inputs 的 selected 检查在研究提交层；Decision 的 validate_draft_closure 未再次遍历 numeric_basis.inputs**。Decision 只重做 numeric basis 格式校验及 Review 匹配，不宜把“上游检查”说成这个函数逐项重算。[^D16][^R01]

### 3.2 完整网格及中性

- 恰好 `TQQQ/QQQ/SOXX/SOXL × T1/T3/T5 = 12` 个唯一 `(asset,horizon)`，缺失或重复均 Err。
- 概率范围 `0..=1_000_000`；中性精确定义为 `expected_return_ppm == 0 && probability == 500_000`，不是单独概率 0.5。
- 方向先按 expected_return 的符号；只有 return 为零才用 probability 相对 500000 的方向。故“均值为正但正收益概率低于一半”不自动结构非法。
- Decision 要求 thesis 存在；valid_until 为 `min(now + maximum_execution_delay_ms, 12 forecasts最早thesis_valid_until)`；evidence_cutoff 为 selected 中不晚于 now 的最大 observed_at，缺项/读取失败在该统计处 best-effort 过滤，空结果回退 now，不应误称逐条数据新鲜度证明。[^D17][^D18]

### 3.3 新协议的单 Claim 精确资格

源码最低方向 Contract=47；单 Claim 精确 slot Contract=57；终稿 review Contract=67；structured issues=69；当前 catalogue=69 / PromptBundle=38 / freshness candidate=70。历史合同按安装版本选 validator，不重写历史 CAS。[^D02][^D17][^R02][^V01]

`claim_slot_eligible(reference, claim, critiques, asset, horizon)` 精确要求：

1. Claim horizon 相同，Claim.validate 成功，stance 非 Neutral，无命中该 slot 的 blocking EvidenceGap。
2. **存在同一份** target=该 ClaimRef 的合法 Supported Critique，且不阻断该 slot。
3. 对 PriceMarketStructure 与 Macro **两域各自**：在**同一个 Claim**找到 Directional、覆盖该 asset 的 ground；在**上述同一个 Critique**找到同 evidence ArtifactRef、同 role、同 domain、包含该 asset 的 reviewed ground；且 supporting_refs 对同 evidence 有 current-authoritative 核验。
4. current-authoritative 的代码含义是 authority != Unrated 且 temporal_validity=ValidAtDecisionCutoff。因此 EstablishedSecondary 也能满足，不是仅 Primary/Official；这个枚举判断本身不是现场网络来源验证。
5. Forecast 非中性还要该 Claim 已列入 draft.claims，且 stance 匹配方向。不能用不同 Claim 拼 price/macro，不能用 Critic 新证据或 deliberation 补 Analyst 正式 grounds。[^D17][^R03]

Gap 的空 assets 表示所有资产，空 horizons 继承 Claim horizon。Critique blocker=false 不挡；blocker=true 且无 directional blocking gaps 表示 Claim-wide；有 scoped gaps 则只挡命中的资产/期限。Supported 还要求非空 supporting_refs、无 conflicting_refs，且所有 supporting refs 当前权威。[^R03]

**ground 真正资源范围的验证发生在上游提交：**从 Manifest 精确 ArtifactRef 读规范化 payload，验证 domain 与 resource、资产 subset、Directional 必须 NormalizedEvidence 且 citations_complete=true、scope/domain 已知；未 source_verified 的 news（>=63）只能 descriptive、assets=[]、domain=null，不能入 supporting_refs。Decision 的纯 domain 谓词不重新联网、不独立证明来源真实或预测正确；不要把它扩写成更强保证。[^R04]

### 3.4 allocation 的研究资格与 final Review

- 研究计划恰好四资产一行加显式 cash，权重总和严格 1,000,000。每行有 rationale；horizons、refs 排序去重。非零行需非空 supporting_horizons/evidence_refs 且 abstention_reason=None；零行需非空 abstention_reason。[^D01]
- 新结构化 allocation gate：每个非零行，至少一个所列 horizon 有同资产正 expected_return forecast、Bullish 且 `claim_slot_eligible` 的 Claim，且行引用关联到该 Claim、其该资产 ground，或其 Critique。只要一个非零行完全不合格，整个 Decision 前置 Err；不是先写“blocked research”来兜底全部非法输入。[^D17]
- 17 个 numeric basis scopes =12 forecasts+4 allocations+cash；每项 inputs 非空且 kind 受限，units/method/assumptions/uncertainty 非空。这是估计说明完整度，不是代数证明、事实正确率或实证校准。[^R02]
- `Store::final_proposal_review` 只取 Decision 依赖祖先的 succeeded task 正式输出；冻结 revision 排序取最后，重复 revision 为歧义 Err；校验 Review producer、origin Run/Contract、Task Contract、source_refs，proposal 哈希、同 Run、成功 Synthesizer 任务且其正式输出含 proposal，Review Manifest 与 Review 同 origin、其 input_hash 与 Contract 一致且选中该 proposal。**Review.manifest 是 Reviewer Manifest，不是 Synthesizer Manifest。**[^R05]
- Contract >=69：拒绝 assessment 要 1–3 issues，通过项 issues 为空，scope/field_path、稳定 ID 去重和证据 kind 受约束。`accepted` 要基础结构合法且17项全通过；`authorizes` 再要求精确 proposal ref+blob hash。失败对应 `ProposalReviewRequired` 或 Store/daemon error，不会产生“已正式决策的零目标”来代替。[^R02][^R05][^D02]

## 4. raw → validated research plan 与 blockers 的真实影响

`review_research_plan` clone raw，不改 proposal；逐非零行过滤 supporting_horizons：有同资产正收益 forecast，且本地 `research_slot_supported` 找到 Bullish Claim、price+macro grounds、Supported 未阻断 Critique及关联引用。剩余期限空则该行归零并记理由。再以 `policy.max_gross_weight` 等比例缩放超限研究配置，整数向下取整，cash 重算为 `1_000_000 - Σweights`。默认研究 gross cap=500000，所以 raw 80% 风险资产不等于 validated 80%。[^D19]

注意：前置新协议 validator 使用更严格的 `claim_slot_eligible`，本地 review helper 自身**没有**重做每个 ground 的 current-authoritative 精确匹配。它是后续整理，不是上游强规则的替代品。当前合法路径已先通过严格门，但不要声称每个辅助函数都独立证明同样强的性质。[^D17][^D19]

研究状态：

- validated 仍有非零行 → `qualified_recommendation`；
- raw 有非零、validated 全零 → `blocked_by_research`（包含 static cap 裁至全零的情况）；
- raw 已全现金 → `explicit_cash`。

execution_status 的**先后顺序**：先研究 blocked → `blocked/research_plan_blocked`；否则 PositionPlan → `not_applicable/position_plan_does_not_enter_execution`；否则 Policy 不 decision-capable → `blocked/decision_policy_not_ready`；否则 `pending_execution_gate/execution_gate_not_run`。因此“PositionPlan 一律 N/A”需排除研究 blocked 的分支；此字段也不是 ExecutionVerdict。[^D19]

**allocation 不约束正式 targets 的数值：**target 函数没有 allocation 参数，raw/validated 权重没有进入其 sizing 公式；包括显式现金，也没有一个 allocation-cash veto 分支。由代码可推得：若研究允许非中性 forecasts 却选择现金，当前 target 计算仍可能从 forecasts 得出非零目标；不能宣称正式目标必小于等于 validated allocation。这里是数据流结论，不是本次真实 Run 的观察。[^D03][^D08]

其他 blocker 与置信度：

- 从 draft.hard_blockers 开始；materiality>=500000 的 active Claim 要**恰一份**匹配 Supported Critique且 slot 不阻断，否则加入 UnverifiedClaim；active Claim 的 material conflict 加 MaterialConflict；校准 horizon 冲突加 HorizonConflict。[^D09][^D20]
- consensus 按 Claim task 分组、capability snapshot hash、evidence content_similarity_cluster（缺失退 Artifact ID）计数；最大 pairwise Jaccard overlap 门槛500000。多参与者非 independent 时 `effective_confidence=min(raw,min_confidence)`，**等于最低门槛仍通过 `< min` 的判断**，不是必然归零；相关性加 soft warning。其他部分重叠分支按 cluster/participant 比例缩放并夹在原置信度与最低门槛之间。[^D20][^Q03]
- 研究过程质量为 grounding/premise/temporal/logic 四项最小值；任一 None 则 floor=None。门槛默认900000，缺失或不足加入 UnverifiedClaim。执行的 mandate/portfolio/action 三项此时 None，不伪造分数；也不因这些 blocker 在 Decision 代码内重新把 target 改成零。[^D09][^D21]
- `DecisionContext::accepted()` 只判断 hard_blockers 和 material_conflicts 空；不能用 targets 非零、asset_eligibility.eligible 或 ResearchPlanStatus 替代 accepted。[^D22]

## 5. DecisionPolicy 的合法、capable、当次可用是三层

### 5.1 缺失与加载失败

`load_decision_policy_from_store` 只从 active head 读取，不挑“最新 candidate”。真的缺 head 返回合法 `DecisionPolicy::default()`，status=`store_active_head_missing`，input_hash/contract_hash/artifact_id 全 None。已有 active Artifact 但 envelope、hash、身份、Policy validate 失败，则返回 Err，**不 fallback default**。模型比较用有效 Synthesizer route。[^I01]

严格 decode 要完整 provenance-bearing `DecisionPolicyArtifact`，不能裸旧 Policy。validate 核查 schema=1、元数据非空、assets/horizons固定、sample_count/source_runs非空、provider/route/Contract存在，重算 policy output_hash 和 risk_model_hash。**单独 decode/validate 不重新读取原 dataset 重算 provenance.input_hash，也不是重做 collect。**[^C05]

### 5.2 实际默认值（不是推荐的 operator 风险限制）

| 字段 | 缺 Policy 默认值 |
|---|---:|
| min_confidence_ppm | 250000（25%） |
| max_gross_weight | 500000（50%） |
| horizon weights T1/T3/T5 | 333333 / 333333 / 333334 |
| maximum_execution_delay_ms | 300000（5分钟） |
| minimum_process_quality_ppm | 900000 |
| min_probability_edge_ppm | 50000（概率偏离中点5个百分点） |
| min_calibration_samples | **u32::MAX = 4294967295**，不是30 |
| max_brier_score_ppm | 250000 |
| active_forecast_calibration | None |
| forecast_calibrations / asset_calibrations | 空 |
| target_annualized_volatility_ppm | 150000 |
| max_portfolio_beta_ppm | 500000 |
| risk model version | audit-unapproved-v1 |
| covariance_sample_count / covariance | 0 / 空 |
| max_expected_shortfall_ppm / max_gap_loss_ppm | 0 / 0 |
| max_leveraged_holding_days | 1 |

默认值可通过结构 validate，因为空 asset_calibrations 对应未配置风险模型允许为空；但不 decision-capable，正常 target 资产查找均缺校准而归零。不能把 `min_calibration_samples` 默认理解为“已经有4294967295样本”，也不能把0尾部限制理解为批准的安全模型。[^D23][^D24]

### 5.3 validate 与 decision_capable

- Policy.validate：confidence/process/Brier/gross/horizon weights在ppm范围；execution delay>0；edge<=500000；min_samples>0；target vol在1..=1000000；beta在1..=3000000；三个 horizon 键齐全且总权重1000000；校准 `(scope,asset,horizon)` 唯一。
- Asset风险：vol/beta/ES/gap在1..=3000000；liquidity<=max_capital<=1000000；decay<=1000000，非TQQQ/SOXL的decay必须0。资产 sample_count 的足量性不是该子类型 validate 的结构条件，而在 capable/当次计算检查。
- 有资产风险时：risk model版本非空，samples>0且>=min_samples，持有日1..=5，ES/gap limits在1..=3000000；协方差全 pair 存在、对称、幅度不超过两资产vol乘积、对角>0。没有全矩阵正半定证明；实际 `wᵀΣw < 0` 再 Err。
- decision_capable：有active scope、4份资产风险、4行covariance、risk samples足量；各资产samples/Brier合格；**12面**匹配scope的forecast calibration总样本足量/Brier合格/bins非空。
- capable 本身不检查“当前预测所在 bin 样本足量”、frozen_at<=本次now或最终alpha/edge；也不直接检查当前Config/Contract。它需要配合结构校验及入口身份核验。[^D24][^D25]

关键限制：`target_with_risk_traced` 并没有 `if !decision_capable {全局归零}`。它逐资产逐参与forecast检查；正常 CLI activation 会拒绝 incapable candidate，但不能推广成任意结构合法的局部 Policy 在函数层都全局归零。[^D08][^I05]

## 6. 正式 targets 的计算与风险裁剪

令 `S=1_000_000`，所有下述除法为代码中的整数除法/向下量化（有符号项按Rust整数规则截断）。[^D08]

### 6.1 先筛资格

1. effective_confidence < min_confidence → 全零，risk=Default（数值None而非测得0），first_zeroing_branch=`decision.confidence.minimum`。
2. horizon_trace 先按**校准概率**分类：p>=500000+edge 为 Bullish；p+edge<=500000 为 Bearish；其余Neutral；原始中性永远Neutral；无校准为Uncalibrated。同资产任两horizon一多一空产生冲突，该资产排除，Context另加全局HorizonConflict blocker。
3. 缺资产risk calibration；样本<min；Brier>max → 排除资产。
4. 原始中性 forecast 不参加任何正向bin“复活”。TQQQ/SOXL只纳入 horizon_days<=max_leveraged_holding_days 的非中性预测，QQQ/SOXX不做此持有期限过滤。
5. 对每个参加的forecast：匹配active `(model_id,model_version_hash,regime)`+asset+horizon；frozen_at<=decision_at、forecast calibration总样本>=min、Brier<=max、**命中bin自身count>=min**。任一参加horizon缺校准，整个资产不使用其余部分信号。

注意 horizon_trace 的冲突扫描在上述杠杆持有期过滤**之前**且包含全部forecasts：一个随后被持有期过滤的T5仍可能引发冲突。`included_in_target` 在trace中初始仅指calibrated.is_some再扣冲突，不等于该sleeve最后真的带来非零仓位。[^D08][^D25][^D26]

### 6.2 信号与单资产资本上限

对参与期限，权重h取Policy.horizon_weights：

```text
E = [Σ ((calibrated_p - 500000) * h / S)] * S / Σh
A = [Σ (calibrated_expected_alpha * h / S)] * S / Σh
合格条件：看到了参与forecast，校准完整，E >= min_probability_edge，A > daily_reset_decay
signal = clamp(2*E + A - daily_reset_decay, 0, S)
vol_cap  = target_annualized_volatility * S / asset_vol
beta_cap = max_portfolio_beta * S / asset_beta
cap = min(vol_cap, beta_cap, max_capital_weight, liquidity_weight_cap)
w = cap * signal / S
```

少部分期限因中性/杠杆持有期被排除时，按**实际参与权重**重新归一；缺校准不是这种可忽略排除。`confidence` 达最低门槛后不再连续乘进仓位。当前raw expected_return主要用于研究方向/中性；正式A来自历史bin实现收益均值，并非模型raw预期收益。liquidity cap 是operator静态额度，不是该函数实测实时订单簿。[^D08][^C06]

### 6.3 组合层

先以max_gross等比例缩放。然后：

- variance = Σ(w_i*w_j*cov_ij / S²)，vol = integer_sqrt(variance)；非零资产缺cov或最终variance负 → Err。
- beta / ES / gap = Σ(w_i*各资产metric)/S；这是线性聚合风险指标，不是新做组合尾部情景模拟。
- 取vol、beta、ES、gap各超限项目 `limit*S/measured` 的**最小**比例统一缩放所有weights（不扩大）；重算最终assessment。
- 若初始eligible集合空 → `decision.eligible_set.empty`；若曾有eligible但静态额度/signal/整数缩放把最终组合压为零 → `decision.post_scale.zero`。保留各资产首个排除分支；runtime_trace主要记录排除/归零，不是每个成功计算的完整逐式日志。
- accepted且非零的DecisionContext必须有risk_model_hash及四项风险指标；缺项validate报错。未接受的非零Context可以保留为审计候选，但不能经Allocator放行。[^D08][^D27][^D22]

**不是严格金融保证：**风险模型的对称/幅度约束不等于PSD证明；组合裁剪后虽重算风险但没有在本函数内循环直到每项再次低于限值的单独断言。这里描述实现，而非宣称已经实证覆盖全部数值极端值。[^D24][^D27]

## 7. 何时研究非零但 targets 全零；为何不等于强制清仓

| 情况 | 研究计划 | Decision / targets |
|---|---|---|
| 合格proposal + canonical缺active Policy | validated可非零（受gross cap） | 默认Policy产生零目标，Decision可成功 |
| effective confidence不足 | 不按该门槛删除研究建议 | 全局零目标 |
| 所有资产缺风险/forecast/bin校准、样本不足或Brier差 | 研究证据仍可合格 | eligible集合空，零目标 |
| 各资产被校准方向冲突或参与期限限制排除 | raw/validated研究仍保留 | 资产零；若全部排除则组合零 |
| 校准edge不足，或alpha<=decay | 研究正收益估计仍保留 | 相应资产零；全部失败则组合零 |
| max_gross/cap/signal/risk缩放及整数取整到0 | raw保留；validated可能非零也可能被其独立gross cap裁零 | post_scale.zero |
| proposal/Manifest/12slots/精确ground/Reviewer不合法 | proposal可能已存在 | **Decision Err，不是已完成的零目标Decision** |
| hard blocker/过程质量缺失 | 研究可非零 | **不保证targets归零**；Context不accepted、Execution阻断 |
| 真实隔离Debug PositionPlan缺Policy | 手动研究可存在 | 控制层禁止Decision，不能报告targets=0已生成 |

依据为两条独立数据流与实际分支，不是本次Run统计。[^D02][^D08][^D09][^D19][^D06]

**targets=0是目标权重，不是卖出指令：**ExecutionGate先继承Decision blockers，再校验purpose、Paper approval/qualification、有效期、freeze、账户/quote/clock等，blockers为空才调用Allocator；其后仍有Gate检查。缺approval明确加UnqualifiedRuntime，跳过allocation。[^E01]

Allocator还要求DecisionContext.accepted、快照session一致、市场tradable；它计算 `delta = account.equity * target_weight/S - current_market_value`。当前空仓且零目标→没有delta→NoExecutableOrder；当前有多头且零目标→只有经过前述门后才**可能**生成sell意图；后续执行Gate、Commitment及Broker结果仍决定能否提交/成交。NoOrder意味着没授权新计划，不能解释为替用户清仓。这里停止在接口含义，不接管执行实现。[^E02]

## 8. canonical、隔离 Debug 与 Policy 状态矩阵

| 路径/状态 | 当前代码行为 | 不可推出 |
|---|---|---|
| canonical PositionPlan，无active head | 正式`POST /runs`准备34研究Need、正式研究图；Decision用启动时默认Policy；在Decision结束 | 不产生ExecutionVerdict/Outcome，不提供校准标签 |
| canonical Paper，无active head | scheduler当次SQL检查cold_start，binding=None（即使Store有旧approval）；正常研究/Decision后由原ExecutionGate挡单 | Paper purpose不是approval；等待真实标签≠已经成熟 |
| 已有active、当前model/version不匹配 | strict loader Err，serve在能力probe前停止 | 不是缺Policy fallback；不能伪造日期消除hash差异 |
| 已有active、仅当前Synthesizer Contract不匹配 | activate拒绝；readiness可报contract_mismatch；真实Debug ready preflight拒绝；**canonical serve/decide缺对应显式比较，见第10节** | 不能依据文档声称所有canonical入口必然拒绝 |
| active envelope/hash/数值非法 | loader/constructor返回错误；readiness捕获部分decode身份错误投影为invalid_store_policy_or_identity | 不是所有Store错误都被转为该JSON状态；active读取本身的Err可直接传播 |
| 结构合法但校准面不足 | 状态validated_but_no_asset_calibration或validated_but_insufficient_samples；正常activate拒绝 | validate=valid不意味着capable；也别泛化任意部分Policy都会全局归零 |
| 隔离真实Debug PositionPlan缺Policy | 允许prepare/手动研究；冻结identity为PositionPlan+非Fixture+缺policy artifact，阻止resume和直达gate.decision | 重启/等Outcome不会变canonical；不是正式PositionPlan的零目标完成语义 |
| 隔离fixture | 明确fixture例外可验证Decision流程 | 不是真实校准、模型或Paper成交验收 |

证据：正式入口/拓扑/冷启动[^D04][^D05][^D07]；loader和启动[^I01][^I02]；Debug冻结身份与控制[^D06]；状态投影[^C01][^C03]。

**启动快照特别提示：**serve加载Policy，Daemon bootstrap clone进DecisionRuntime，此后`decision_gate`调用该实例；未见逐次active head刷新。scheduler却每tick读取SQL active head判断是否cold_start。CLI activate成功不等于已有daemon内存Policy即时换新；需核对实际运行实例policy_hash，不能自动给已有进程授予“已切换策略”的结论。报告不启动/重启进程，也不建议绕过原RuntimeIdentity/审批。[^I02][^I03][^D13][^D05]

## 9. readiness → 风险限制 → collect → build → inspect → validate → activate

### 9.1 readiness：只读成熟度，不是候选生成

定义：`calibration readiness --store <既有Store> --min-samples 30`；30是readiness/collect CLI默认值而不是默认Policy样本门槛。代码拒绝min_samples=0。使用`Store::open_existing`：SQLite READ_ONLY、要求当前schema18；不创建主库或迁移，但有当前连接TEMP staging准备。不存在/旧版本Store会报错，不等同于“空样本”。[^C01][^C02][^S03]

扫描最近最多500份Decision（先限额后筛Paper），每Run去重；非Paper不进入行。每种risk/dataset/policy仅展示最近20个Artifact，因此列表为空/截断也不是全库不存在证明。隔离Store报告store_scope=isolated_debug、calibration_eligible=false、reason=isolated_debug_store、next_step=use_canonical_store，成熟数不计入。[^C01]

canonical Run沿Decision→Schedule/ExecutionContext→Outcome与bars检查：

- Decision必须Canonical且validate；Schedule如存在必须指向该Decision。
- sealed Outcome必须Canonical、validate_sealed、绑定相同Schedule、有可用四资产Alpaca bars及完整12标签，才记sealed。
- **分类优先sealed-label结果**，再baseline missing，再no_schedule，再pending。
- 有ExecutionContext但account_snapshot或quote_snapshot缺失→baseline_snapshot_missing；如果ExecutionContext本身完全缺失，这个is_some_and布尔不是true，应按后续分类解释，不能泛化成所有缺基线必同一reason。
- pending仅列尚未持久化的T1/T3/T5 windows与baseline_trading_day，不以自然日推断成熟；自然日过去不补写历史基线。
- gap = min_samples - sealed Run数（饱和到0），12 slots各写同一成熟Run数；够数只提示collect。该阶段**不按当前模型/Contract过滤每一个成熟Run**，不检验完整价格训练面和operator limits，因此会被collect进一步筛掉。[^C07]

若有active head，readiness读取配置身份和**已存**active Synthesizer Contract；绝不调用可能安装catalogue的helper。active缺失时直接status=store_active_head_missing，不说明有待激活candidate。[^C01]

### 9.2 operator 显式风险限制

`set-risk-limits` 必填14个参数，均无CLI默认：min-confidence-ppm、max-gross-weight-ppm、maximum-execution-delay-ms、minimum-process-quality-ppm、min-probability-edge-ppm、max-brier-score-ppm、target-annualized-volatility-ppm、max-portfolio-beta-ppm、max-expected-shortfall-ppm、max-gap-loss-ppm、max-capital-weight-ppm、liquidity-weight-cap-ppm、max-leveraged-holding-days、daily-reset-decay-ppm。**本报告不提供替operator选择的数值。**[^C02]

限制：capital与liquidity均>0，liquidity<=capital<=1000000；ES/gap limit在1..=3000000；holding1..=5；decay<=1000000；其余复用Policy.validate。落为Canonical CalibrationRiskLimits CAS，无origin/source_refs。Store拒绝隔离写入。可inspect后再collect；这只是保存限制，不批准交易。[^C08][^S01]

### 9.3 collect：正式Store中的成熟标签与身份

1. 读取完整config，要求有效Synthesizer route有release_date、knowledge_cutoff（可继承共享）；模型ID/版本hash按当前配置计算。**load_config也解析credential配置/环境占位符，所以本次没有运行collect或调用它来“只看状态”。**日期真实性由operator提供方资料负责，代码不网络核实。[^C04][^I01][^I06]
2. 只读打开源Store，拒绝debug_environment；risk-limits ID必须是本Store的Canonical CalibrationRiskLimits，重新validate；训练边界可选RFC3339，start<=end。
3. 最近500 Decisions筛Paper+Canonical、Decision/Context可解码validate、Canonical Outcome存在且sealed、Schedule精确绑定Decision。
4. 沿同Run且created_at<=Decision时刻的DAG找最近 research.synthesizer AgentTurn，最多256 visited；provider来自capability_snapshot，model优先response.telemetry.actual_model、无则capability.model_id；Contract优先turn顶层、无则request。对齐当前provider与model；缺失/mismatch跳过。
5. 候选按RunId排序，取**首个候选的Contract hash**为训练组，其他Contract全部skip。它不是“自动挑当前active Contract”或“挑最多样本组”。模型变更后旧组可能collect/build成功、最终activate因Contract不符被拒。
6. 仅从Outcome.market_evidence中NormalizedEvidence、payload.source=Alpaca、resource前缀bars:读取价格；四资产都需有series，同资产同日不同close会阻断。不重新请求行情。
7. 对每个forecast找到T1/T3/T5 Outcome window；必须有schedule baseline date与window observed date价格、realized时刻晚于Decision。当前标签是每资产价格 `(future-base)*S/base`，**不是成交收益、组合Outcome净收益或学习评分**；NoOrder也可贡献forecast标签。[^C04][^C09]

**时间精度/身份边界必须保留：**

- collect用`session_close_time(date)=当日23:59:59 UTC`作为日级realized_at；它不是交易所实际close，不重跑共同交易Session选择。共同Session真实性依赖已冻结Outcome的上游生产链；本报告不接管该算法。
- 未显式training_start/end时，start=所选Decision最早created_at；end=所选Outcome windows日期对应日末的最晚值。
- 一个Run要12条forecast全部落入窗口才加入，不能只收其部分horizon。panel按日期裁窗、冲突检测，再取四资产日期交集；每资产至少min_samples+1个共同price points（默认31），即至少30个returns。每slot至少30条、总至少360条；这不等于360个独立Run或独立市场状态。
- 原始中性forecasts没有在collect中过滤；它们的预测概率和后续真实方向也参加Brier/bin统计。运行时仍禁止把新的中性forecast复活。
- readiness sealed数与collect实际选中样本数是不同指标；collect尚未做完整builder的risk值/Brier/capable检查。
- collect样本provenance.model_version_hash **直接填写当前configured_synthesizer_identity计算的hash**。历史identity helper只返回provider/model/Contract，不返回历史版本hash。当前配置hash包含base_url（去末尾斜线）、release_date、knowledge_cutoff、reasoning、language等，但这里没有逐历史turn核对这些字段；仅“hash相等”是在后续builder里验证本次组装的字段自洽，不能宣称历史日期/route参数已全部独立核实。
- collect/build没有把model release/cutoff作为字段与每个历史forecast_at逐样本比较；不能从日期字段存在推出完全无训练污染。禁止用猜测日期制造身份匹配。[^C04][^C09][^I01][^C10]

不足时输出status=BLOCKED、counts、price_conflicts、skipped_runs，不写dataset、不build、不activate；成功才写`StoredCalibrationDataset { input, report }`，source_refs包含同Store risk limits + 选中Decisions + 对应Outcomes，之后verify_integrity。input包含schema1、policy_version=`offline_historical_v2`、algorithm_version=`offline_historical_risk_v1`、route=`research.synthesizer`、regime=`all`。[^C10][^S01]

### 9.4 build：离线纯拟合 + SQL candidate

CLI用可写Store打开指定dataset，要求Canonical+kind匹配；纯函数`build_offline_decision_policy(input,Utc::now())`不访问模型/行情/Store；成功后persist candidate并verify_integrity，输出activated=false。[^C03]

纯输入校验：training_start<=training_end<=frozen_at、min_samples>0、source_runs非空唯一；四资产价格序列恰好一次、日期严格递增、正价格且在训练窗内；每预测`forecast_at < realized_at <= training_end`且forecast_at>=training_start、cutoff=forecast_at、provider/model/version/route/Contract与input一致。source_decision/context只校验kind，不在纯函数重新打开Store。该函数未额外验证 `(source_run,asset,horizon)` 样本唯一；正式数据可信度依赖collect及Store合法生产路径，不应把任意构造input视为真实Paper样本。[^C05][^C11]

概率拟合：

- 每asset×horizon独立，总样本<min时Err；10等宽概率bins：0..99999，100000..199999，…，900000..1000000。
- calibrated_probability=正实现收益样本数*S/count；实现收益=0算非正。
- calibrated_expected_alpha=该bin realized_return_ppm均值；名字alpha**没有减去benchmark**，不是raw预期收益回归。
- 空bin存p=500000、alpha=0、count=0；不是虚构观测，运行时bin count门仍拒绝。
- forecast面与资产级Brier都取原始p对实际二元方向的平方误差均值，ppm单位；不是“拟合后校准概率在独立holdout上的Brier”。本builder没有独立holdout/rolling out-of-sample评估步骤。
- 每面fit_dataset_hash绑定全部input.forecasts，trained_through=input.training_end、frozen_at=build时刻。[^C06]

风险拟合（只说明DecisionPolicy数值，不扩展Outcome学习）：

- 四资产共同日期，相邻close计算ppm returns；样本variance/covariance分母n-1，年化252。
- vol=round(sample_std_ppm*sqrt252)；beta=`abs(cov(asset,QQQ)/var(QQQ))*S`，benchmark固定QQQ；无QQQ方差、非正vol/beta或没有负收益尾部则RiskUnavailable。
- ES为最坏`ceil(returns总数*0.05)`个负收益幅度均值（至少1、最多实际loss数），gap为最大**日close-to-close损失**，不是实测overnight gap。
- liquidity/capital/decay来自operator，decay只给TQQQ/SOXL，不从这些bars拟合。
- risk.sample_count=共同returns数；asset.sample_count=该资产所有horizon预测数（完整30 Run时90）；forecast面sample_count=30，bin可能远小于30，需分开解释。
- covariance×252；对角用舍入vol²，非对角限制在±vol_i*vol_j；风险浮点必须有限、>0且<=3000000；cov绝对值<=9000000000000。
- build应用显式risk limits和input.min_samples；未给的horizon权重沿用默认三等分。最后Policy.validate与envelope.validate；**不强制decision_capable=true才保存candidate**，Brier差的合法候选可能build成功而不能activate。[^C12][^C13][^C14]

### 9.5 inspect、validate、activate与CAS哈希

- inspect：既有只读Store，按Artifact ID读元数据+payload，允许3种calibration kind。代码只检查kind，未像calibration_artifact helper那样检查Canonical，注释“canonical检查”比实际分支强。它不验证拟合也不激活。[^C03]
- validate：既有只读Store，要求Canonical DecisionPolicy；decode_strict，输出status=valid与decision_capable布尔；**不读config、不匹配当前Contract、不要求capable=true**。[^C03]
- activate：读当前模型config并resolve；可写Store；拒绝隔离；读同一Canonical policy Artifact、strict decode + 当前Synthesizer模型身份检查；读**已存active Synthesizer Contract**，无则Err，hash不同则Err；helper再次strict decode且要求decision_capable；生成descriptor并调用Store activation事务。[^C03][^I05]
- 文档要求operator显式inspect→validate→activate；源码没有“必须先持久化inspect/validate操作receipt”的状态机，activate自身重新校验即能执行，不能谎称SQL强制记录了这两步审批历史。[^C03][^S02]

| 名称 | 实际含义 |
|---|---|
| dataset `input_hash` / policy provenance.input_hash | 完整OfflineCalibrationInput的canonical JSON hash；不含dataset report |
| forecast `fit_dataset_hash` | input.forecasts序列的canonical JSON hash |
| provenance.output_hash / descriptor.policy_hash / DecisionContext.decision_policy_hash | 完整DecisionPolicy正文hash（非整个envelope） |
| provenance.risk_model_hash / assessment.risk_model_hash | PortfolioRiskModel正文hash |
| descriptor.envelope_hash | 整个Policy Artifact blob hash |
| loader的`input_hash`，Debug/部分CLI展示`decision_policy_input_hash` | **envelope bytes hash**，不是上面的训练input_hash |
| Artifact ID | Store Artifact身份；CLI要求的--dataset/--policy/--artifact是它，不是文件路径或任选policy hash |

build生成的Policy Artifact **source_refs为空**，Store descriptor也要求无origin且无source_refs；dataset才有risk/Decision/Outcome引用DAG。Policy回溯dataset通过provenance.input_hash等字段，不存在policy→dataset ArtifactRef边，不能把“完整血缘”误报成直接全DAG闭合。[^C03][^C05][^C11][^I01][^S01][^S02]

Store层：

1. `write_calibration_artifact`禁止Debug和错误lifecycle/origin；dataset至少有RiskLimits、Decision、Outcome三类引用；Immediate事务重读每个来源的kind+Canonical并insert Artifact/refs；不写Policy installation/head。
2. `activate_decision_policy`校验descriptor，Immediate事务：同policy_hash已有记录必须descriptor+Artifact完全相同，否则冲突；未安装则写Artifact+installation。若不是当前hash，检查激活时间不倒退，append(previous_policy_hash,policy_hash,activated_at)，更新singleton_id=1 head；同当前hash重放不append历史。
3. 这里CAS指内容寻址存储；head更新是串行Immediate事务的upsert，接口没有由调用方提供expected_previous_hash的比较交换参数。不要混淆为额外operator版本CAS批准。
4. Doctor重建安装、activation previous链、时间单调与head最后一条一致；本次没有运行Doctor。
5. `bootstrap_active_decision_policy_from`只复制源active immutable Policy bytes+descriptor，**会在目标Store调用activate**；源无active则None。它不复制Run/Outcome/凭据、不解除隔离样本资格；不是collect/build，也不是“只读复制”。底层activate没有Debug拒绝，由CLI普通activate拒绝，而bootstrap可服务隔离策略快照复用。[^S01][^S02][^S04]

## 10. 文档漂移、实现不对称与不能夸大的保证

### A. canonical当前Contract绑定存在显式检查缺口（优先关注）

文档说“已安装policy仍须严格匹配当前模型及Synthesizer Contract”。实际loader检查model/version和descriptor↔envelope Contract自洽，**不读当前active Contract**；serve只有`debug_control && !uncalibrated_research`分支调用当前catalogue hash比较；canonical普通serve只把内嵌DecisionPolicy交给Daemon。DecisionRuntime只收到Policy正文（没有envelope.provenance.contract_hash），decide验证proposal producer Contract已安装/版本够，但不比较它与校准Contract。

因此activate当时的Contract绑定、readiness警告、Debug preflight，不构成canonical以后每次使用时绑定未漂移的证明。特别是catalogue升级或现有active Policy沿用时，不能按文档推断“必拒绝/targets必零”。这是已核实的数据流缺口；没有运行构造或攻击，不能断言当前Store已发生错配，更不能据此推断Broker已下单。[^V02][^I01][^I02][^I03][^D02]

### B. SQL active head不等于正在运行的Policy

canonical readiness读SQL+配置+已存Contract；health读DecisionRuntime.policy，非Debug常用capable映射ready/unconfigured/insufficient，而不是逐次做上述身份检查。独立activate不会自动替换runtime内存；SQL、health和Run Context应分开看。[^I04][^I02][^I03][^D05]

### C. preflight并非严格只读

calibration.rs文件导读/分派注释说readiness/preflight只读；实际Preflight调用`Store::open(scratch)`，可创建/初始化/升级；eligible时调用`canonical_synthesizer_contract_hash`，内部`ActiveResearchCatalogue::install`可写Contract。它不发模型调用不等于不写Store。readiness明确避免这个helper。故本次未运行Preflight。[^C03][^I07][^S03][^C01]

### D. 资格摘要与真正授权谓词不一致

实际`claim_slot_eligible`强制price+macro；`build_asset_eligibility.directional_evidence`仍强制Price+Macro+NewsEvent，且聚合逻辑较粗；`eligible`只是返回的诊断字段，未用于回写target或Context.accepted。因此可能targets有值但摘要MissingEvidence/eligible=false；不能用摘要解释某次真实首个归零分支，应看runtime_trace和target实际分支。`horizon_trace.included_in_target`也不计杠杆持有期和最终risk/edge全部条件。[^D17][^D10][^D26][^D22]

### E. 模型时间身份并非历史turn全字段复核

collect严格要求当前配置有日期，却只从历史turn核provider/model/Contract；版本hash用当前配置赋予样本。builder验证这些已组装字段一致，而不验证日期真实性或模型发布/知识时间相对历史预测的关系。故“模型Contract绑定”必须分别说清当前加载、历史收集和当次proposal三个边界。[^C04][^C09][^C11][^I01]

### F. CONTEXT版本与术语落后

CONTEXT.md 的版本说明仍为Store17 / research67 / Prompt37 / freshness68；当前源码Store18 / research69 / Prompt38 / freshness70，与运行时契约当前版本一致。CONTEXT Canonical Run 专指scheduler-owned Paper；当前日常PositionPlan使用canonical Store/正式Core但Decision仍RunScoped。必须区分“canonical Store/正式入口”与“Canonical Paper artifact/样本”。[^V01][^V03][^D03]

### G. 注释的其他过强表述

- `decision_gate.rs` 的模块导读仍提model DecisionDraft，实际public vocabulary已是DecisionProposal别名；不代表旧Draft模型阶段复活。[^D01][^D28]
- Store active读取注释说“head存在但安装不完整Err”，实际read_decision_policy缺installation row返回None，active_decision_policy直接转交该Option；正常FK/Doctor应保障一致性，但仅这个函数不能声称覆盖所有损坏情况。[^S04]
- readiness的“sealed”只表示其持久化标签资格检查成功；不能推导独立holdout表现、足够bin count、risk可测、当前Contract组足样本、candidate存在或已激活。
- risk字段gap_loss的实现是最坏日收盘收益；calibrated_expected_alpha是bin原始实现收益均值；不要在报告中改叫实际隔夜跳空损失或超额收益验证。[^C01][^C06][^C12]

这些问题均未修复，符合只读范围。也没有通过恢复旧流程、伪造policy/模型元数据、降低样本门槛或开启交易来绕过。

## 11. 验证边界与交接

本次证据级别：**source-reviewed（源码/文档交叉核查）**；报告已写入。没有业务implemented变更；不标记运行时offline-verified、real-Paper-verified或outcome/learning-verified。

原始核查使用本地`git status/rev-parse`、限定目录`rg`与源码阅读；当前版本按仓库相对文件定位并复核链接，未因此验证运行时。未运行cargo、Python仓库测试、fixture、readiness、preflight、collect、build、inspect/validate CLI、activate、Doctor，也未读取真实样本数、已安装Policy ID或当前进程hash。没有证据说明当前用户机器Policy缺失/已错配，只报告代码对这些条件的处理。

静态交付需复核来源文件、脚注定义、代码围栏与行末空白；`git diff --check`只覆盖 Git 记录的差异。收尾`git status --short`与初始既有改动列表一致；原始报告曾存于忽略目录；当前 `docs/reports/` 副本是未跟踪文档，需独立做格式与链接检查。这里的通过仅覆盖文档交付与静态工作树检查，不升级为业务运行时验证。

读取到但未运行的测试包括：默认Policy不capable、低confidence与empty eligible不同归零分支、中性forecast不被正bin复活；offline candidate可出非零targets、359/360样本不足、strict decode拒裸旧policy；CLI隔离readiness和preflight匹配测试；Store activation幂等/历史保留。这些是测试设计证据，不是本次执行成功。[^T01]

给执行worker的最小交接：以Context.accepted/hard_blockers、validity、target、原approval和ExecutionVerdict解释下单资格，不把research weights或targets=0直接翻译为订单；本文只核到Allocator delta接口。给Outcome/学习worker的最小交接：collect消费Canonical sealed Outcome的windows/market_evidence作为标签来源，模型日期、隔离Store、同Contract训练组与sample/bin统计问题见第9节；不把数值密封自动等同叙事/学习晋级。[^E01][^E02][^C04][^C09]

## 12. 源码文件与职责索引

以下为本次工作树的仓库相对源码文件索引；脚注按职责说明定位，负面断言需结合实际参数、分支和调用链重新核查。

[^D01]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（proposal alias、研究配置与schema）。

[^D02]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（构造、permit、Contract、Review和slot先验）。

[^D03]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（Context/Decision同target及原子commit）。

[^D04]: [crates/akzio-daemon/src/http_launch.rs](../../crates/akzio-daemon/src/http_launch.rs)（正式Run入口）；[crates/akzio-daemon/src/orchestration/workers.rs](../../crates/akzio-daemon/src/orchestration/workers.rs)（34Need PositionPlan图）。

[^D05]: [crates/akzio-daemon/src/scheduler/scheduler_tick.rs](../../crates/akzio-daemon/src/scheduler/scheduler_tick.rs)（SQL active head与cold-start approval）。

[^D06]: [crates/akzio-domain/src/debug.rs](../../crates/akzio-domain/src/debug.rs)（冻结research-only身份）；[crates/akzio-daemon/src/debug.rs](../../crates/akzio-daemon/src/debug.rs)（prepare资格）；[crates/akzio-daemon/src/debug.rs](../../crates/akzio-daemon/src/debug.rs)（禁止resume和Decision）。

[^D07]: [crates/akzio-runtime/src/runtime/compilation/evidence.rs](../../crates/akzio-runtime/src/runtime/compilation/evidence.rs)（PositionPlan终止于Decision）；[docs/development-workflow.md](../development-workflow.md)（canonical Paper NoOrder与Outcome边界）。

[^D08]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（target函数完整资格与sizing分支）。

[^D09]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（hard blockers与confidence）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（质量只追加blocker）。

[^D10]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（asset eligibility诊断投影）。

[^D11]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（ResearchPlanReview字段与校验）。

[^D12]: [crates/akzio-execution/src/decision_gate/commit.rs](../../crates/akzio-execution/src/decision_gate/commit.rs)（learning influence边界）。

[^D13]: [crates/akzio-daemon/src/application/paper_execution.rs](../../crates/akzio-daemon/src/application/paper_execution.rs)（final proposal选择和调用DecisionRuntime）。

[^D14]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（proposal provenance）；[crates/akzio-execution/src/decision_gate/helpers.rs](../../crates/akzio-execution/src/decision_gate/helpers.rs)（唯一Manifest）。

[^D15]: [crates/akzio-execution/src/decision_gate/validate.rs](../../crates/akzio-execution/src/decision_gate/validate.rs)（Manifest/递归闭包/bytes与hash）。

[^D16]: [crates/akzio-execution/src/decision_gate/validate.rs](../../crates/akzio-execution/src/decision_gate/validate.rs)（draft引用集合和learning归因）。

[^D17]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（概率/中性/方向）；[crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（12slots和精确qualification）；[crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（方向Contract47）。

[^D18]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（cutoff与有效期）。

[^D19]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（raw/validated处理与状态优先级）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（本地research slot helper）。

[^D20]: [crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（consensus构建和confidence）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（关键Claim唯一review）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（materiality与overlap常量）。

[^D21]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（研究质量None与floor）；[crates/akzio-execution/src/decision_gate/decide.rs](../../crates/akzio-execution/src/decision_gate/decide.rs)（四项质量的计数口径）。

[^D22]: [crates/akzio-domain/src/decision.rs](../../crates/akzio-domain/src/decision.rs)（accepted、target/risk字段验证）。

[^D23]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（全部默认值）。

[^D24]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（bin连续覆盖与总数守恒）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（asset与risk模型校验）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（Policy.validate）。

[^D25]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（decision_capable谓词）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（calibrated_forecast的point-in-time/bin门）。

[^D26]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（horizon方向冲突与included标记）。

[^D27]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（组合risk计算）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（比例缩放与整数平方根）；[crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（排除分支trace）。

[^D28]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（模块标题与历史wire措辞）。

[^R01]: [crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（numeric refs selected、全Claim/Critique闭包）。

[^R02]: [crates/akzio-domain/src/research_review.rs](../../crates/akzio-domain/src/research_review.rs)（版本与17scope numeric basis）；[crates/akzio-domain/src/research_review.rs](../../crates/akzio-domain/src/research_review.rs)（Review issues/accepted/authorizes）。

[^R03]: [crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（authority/current定义）；[crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（gap scope）；[crates/akzio-domain/src/research.rs](../../crates/akzio-domain/src/research.rs)（Critique blocker/Supported）。

[^R04]: [crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（news supporting refs与scope）；[crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（ground与payload精确范围）；[crates/akzio-research/src/agent/errors_catalogue.rs](../../crates/akzio-research/src/agent/errors_catalogue.rs)（news source_verified）。

[^R05]: [crates/akzio-store/src/store/research_review.rs](../../crates/akzio-store/src/store/research_review.rs)（final成功依赖review、revision、provenance与hash）。

[^Q01]: [crates/akzio-domain/src/qualification.rs](../../crates/akzio-domain/src/qualification.rs)（资格阶段/verdict）；[crates/akzio-domain/src/qualification.rs](../../crates/akzio-domain/src/qualification.rs)（receipt身份与校验）。

[^Q02]: [crates/akzio-domain/src/evaluation/policy.rs](../../crates/akzio-domain/src/evaluation/policy.rs)（CandidatePolicy和PolicySubject）；[crates/akzio-domain/src/evaluation/policy.rs](../../crates/akzio-domain/src/evaluation/policy.rs)（允许influence的状态）。

[^Q03]: [crates/akzio-domain/src/market_safety.rs](../../crates/akzio-domain/src/market_safety.rs)（consensus independent与Jaccard）。

[^C01]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（只读readiness扫描、状态、隔离与limits）。

[^C02]: [crates/akzio-cli/src/main.rs](../../crates/akzio-cli/src/main.rs)（全部calibration CLI定义与显式风险参数）。

[^C03]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（命令分派、build/validate/activate/preflight）。

[^C04]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（collect Store资格、metadata、成熟样本和Contract选择）。

[^C05]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（Policy envelope和严格decode）。

[^C06]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（10bins、p/alpha、Brier及fit hash）。

[^C07]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（分类和gap）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（Run readiness lineage/labels）。

[^C08]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（OfflineRiskLimits校验）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（Canonical helper和CAS写入）。

[^C09]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（时间与每个sample组装）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（日末时间、historical identity和Alpaca labels）。

[^C10]: [crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（价格交集、count、BLOCKED和dataset持久化）。

[^C11]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（builder、input时间/身份/价格验证）。

[^C12]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（QQQ benchmark、vol/beta/ES/gap/decay）。

[^C13]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（covariance、共同returns与sample variance）。

[^C14]: [crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（有限值/数值范围）。

[^I01]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（配置模型hash、SQL加载、envelope身份与缺head）。

[^I02]: [crates/akzio-cli/src/cli/run_commands.rs](../../crates/akzio-cli/src/cli/run_commands.rs)（serve只对Debug做额外Contract比较）；[crates/akzio-cli/src/cli/run_commands.rs](../../crates/akzio-cli/src/cli/run_commands.rs)（传入DaemonConfig内存Policy）。

[^I03]: [crates/akzio-daemon/src/orchestration/bootstrap.rs](../../crates/akzio-daemon/src/orchestration/bootstrap.rs)（catalogue安装及DecisionRuntime快照）。

[^I04]: [crates/akzio-daemon/src/orchestration/health_canary.rs](../../crates/akzio-daemon/src/orchestration/health_canary.rs)（health用runtime Policy的投影）。

[^I05]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（激活要求capable并生成descriptor）。

[^I06]: [crates/akzio-cli/src/cli/identity.rs](../../crates/akzio-cli/src/cli/identity.rs)（load_config解析凭据）；[docs/development-workflow.md](../development-workflow.md)（模型日期真实资料与SQL顺序）。

[^I07]: [crates/akzio-daemon/src/lib.rs](../../crates/akzio-daemon/src/lib.rs)（所谓canonical hash helper会install catalogue）。

[^S01]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（calibration CAS write seam）。

[^S02]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（descriptor）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（activation完整事务）。

[^S03]: [crates/akzio-store/src/store/schema.rs](../../crates/akzio-store/src/store/schema.rs)（read-only与可写打开语义）。

[^S04]: [crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（active read、bootstrap、缺row分支）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（history完整性）。

[^E01]: [crates/akzio-execution/src/execution_gate/core.rs](../../crates/akzio-execution/src/execution_gate/core.rs)（blockers和有条件Allocator入口）；[crates/akzio-execution/src/execution_gate/core.rs](../../crates/akzio-execution/src/execution_gate/core.rs)（Accepted与NoOrder）。

[^E02]: [crates/akzio-execution/src/allocation.rs](../../crates/akzio-execution/src/allocation.rs)（Decision接受态与session）；[crates/akzio-execution/src/allocation.rs](../../crates/akzio-execution/src/allocation.rs)（delta方向与无单）。

[^V01]: [crates/akzio-research/src/agent/catalogue.rs](../../crates/akzio-research/src/agent/catalogue.rs)（当前研究版本）；[crates/akzio-store/src/store/prelude.rs](../../crates/akzio-store/src/store/prelude.rs)（Store18）；[crates/akzio-domain/src/schema.rs](../../crates/akzio-domain/src/schema.rs)（Domain10）。

[^V02]: [docs/agent-runtime-contract.md](../agent-runtime-contract.md)（文档policy当前身份与正式/Debug区别）；[docs/agent-runtime-contract.md](../agent-runtime-contract.md)（双Gate与SQL边界）。

[^V03]: [CONTEXT.md](../../CONTEXT.md)（Canonical术语）；[CONTEXT.md](../../CONTEXT.md)（过期版本段落）；[docs/agent-runtime-contract.md](../agent-runtime-contract.md)（契约当前版本）。

[^T01]: [crates/akzio-execution/src/decision_gate.rs](../../crates/akzio-execution/src/decision_gate.rs)（未运行Decision测试）；[crates/akzio-execution/src/calibration.rs](../../crates/akzio-execution/src/calibration.rs)（未运行calibration测试）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（未运行隔离readiness测试）；[crates/akzio-cli/src/cli/calibration.rs](../../crates/akzio-cli/src/cli/calibration.rs)（未运行preflight Contract测试）；[crates/akzio-store/src/store/decision_policy.rs](../../crates/akzio-store/src/store/decision_policy.rs)（未运行Store激活测试）。
