# Akzio Signal Intelligence

Akzio is a local, Paper-only multi-agent research system whose durable state and authority are owned by Rust. This glossary names the learning and execution concepts shared across its domain boundaries.

## Language

**Canonical Run**:
A scheduler-owned Paper run. Its sealed outcomes may influence durable learning only after independent research, narrative, risk-ground-truth and policy eligibility checks. T0 completion does not imply Outcome or learning completion.
_Avoid_: production run, live run

**Noncanonical Run**:
A Debug, Replay, Shadow, or Paper Dry Run whose artifacts may support diagnostics or comparison but may never directly promote memory, contracts, or topology.
_Avoid_: test run when the exact purpose matters

**Outcome Schedule**:
An immutable commitment to evaluate one canonical Paper decision, whether it ended in a durable NoOrder verdict or a reconciled commitment, at specified future trading-session horizons.
_Avoid_: immediate evaluation, evaluation timer

**Sealed Paper Outcome**:
An immutable outcome derived from a canonical Paper decision with durable terminal execution lineage and complete governed market evidence for its horizon.
_Avoid_: result, caller-supplied metrics

**Policy Subject**:
The typed memory, contract version, or topology version whose learning lifecycle may change.
_Avoid_: subject string, policy key

**Policy Influence**:
An Active or Proven learning artifact that was actually included in the context of a later decision.
_Avoid_: memory hint, hidden prior

**Shadow Pair**:
An immutable comparison between a canonical parent Paper decision and one noncanonical candidate decision evaluated over the same outcome horizon.
_Avoid_: A/B test, timestamp pair

**Candidate Policy**:
An immutable proposed contract or research-topology version progressing through bounded canary states without gaining new data-source, tool, or execution authority.
_Avoid_: active policy, permission expansion

**Paper Commitment**:
The single scheduler-owned broker commitment permitted for one broker session after all Rust execution gates accept it.
_Avoid_: live order, model order


**Common Session / Outcome Stage**:
T1/T3/T5 are the first, third and fifth completed exchange sessions shared by all four assets after the baseline. Rust uses exchange-calendar close plus provider availability lag. A late T1 evaluation receives only T1-cutoff facts, never T3/T5 observations. A persisted stage packet is an immutable projection with source lineage, not a second state authority.

**Prepared Output / Committed Reference**:
An Agent output initially contains a staged blob. Rust reads and validates that blob, then performs a fenced Artifact commit. Only the committed Artifact can be loaded by ID or referenced downstream. Partial-stage commits do not complete the long-lived Outcome task.

**Research Coverage**:
The canonical Paper graph has three bounded Analyst/Critic pairs, one per horizon, feeding one Synthesizer. Each nonneutral research slot requires verified directional price and macro grounds plus a matching critique; NewsWeb is retained as explicit coverage and execution-risk context, but its unavailability alone does not erase a scoped research recommendation. MissingEvidence carries scope; unverified price/macro slots are neutral. This is independent of four successful market-price downloads.

**Context Authorization**:
Contract, Manifest and attempt-bound read grants agree on exact source/kind/producer eligibility. Same-Run lineage or qualified learning-overlay rules still apply. Mandatory bounded content accompanies the manifest. Tool access does not expand into RawEvidence or arbitrary I/O.

**Frozen Post-Execution Exposure**:
Outcome metric basis v3 reconstructs T0 quantities and cash from unique reconciled fills, then holds that exposure fixed over market windows. It is not actual subsequent account NAV. Corporate actions that cannot be reconciled to the raw baseline make numeric evaluation unavailable. Legacy outcomes without a metric basis remain unknown.

**Decision Production Cost / Retrospective Cost / Lifecycle Cost**:
The first is reconstructed from the Decision-producing task dependency closure, including its retries. Retrospective cost belongs to Outcome tasks on the same Run. Lifecycle cost includes all Run model usage. Future T1/T3/T5 turns never change T0 production cost.

**Producer Identity / Evaluator Identity**:
Experience evaluation context v1 records the original Run, research Contract set and Workflow artifact/revision separately from the retrospective Contract. PolicySubject names what is evaluated; it is not a substitute for either identity. DecisionContext and artifact lineage retain policy and model provenance.

**Numeric Sealing / Narrative Validity / Learning Eligibility**:
These are separate statuses. A valid numeric T5 can coexist with ModelUnavailable narrative and blocked learning. Bounded narrative repair appends a linked Retrospective revision to the original Run, preserves the sealed numeric Outcome and does not perform automatic policy promotion.

**Scoped Lesson Proposal**:
A bounded proposal includes assets, horizons, recommended behavior, exclusion conditions and evidence references. It produces Draft/quarantine material only. Legacy free text is retained for reading but is not promoted into an all-assets lesson.

**Versions and Verification**:
Domain schema 10; Store schema 17; research Contract 67 / Prompt bundle 37 / freshness candidate 68; Outcome Contract 63 / Prompt bundle 35; Outcome metric basis v3; evaluation context v1; benchmark definition v1 unchanged. See [runtime contract](docs/agent-runtime-contract.md) for compatibility and [development workflow](docs/development-workflow.md) for verification boundaries.

Follow-up invariants: claims/critiques accept at most 12 grounds (four single-asset price, four single-asset news, one shared macro is a legal minimum). Context minimum sets precede optional allocation. Contract 67 research, including final proposal review, uses direct structured submission for Paper, PositionPlan and Shadow; only Outcome retains two-phase submission with a persisted Draft memo. Per-stage retries reset only at committed retrospective events; fenced lease release also runs on cancellation. Narrative repair reuses the sealed Outcome and enters the ordinary eligibility/consumption transaction.

Outcome projection v2 keeps aggregate numeric facts and all twelve forecasts, explicitly identifies omitted detail, and retains the original documents in the 128 KiB / 24-artifact grant (32k estimated tokens). The model invocation remains limited to 12k input tokens including tool reads. Registered Canary Shadows reuse only their parent's committed T0 normalized evidence and frozen baseline execution lineage. Incremental Canary evaluations stay in the durable evaluation ledger; an interrupted Attempt is never indexed as a succeeded output.

Debug uses the formal Paper builder plus durable Store execution control: exact TaskId grants are revision-CAS protected and consumed in the claim transaction. A new isolated Store is mandatory; normal Core cannot reopen it. Broker writes default to forbidden at Reconcile/Dispatch/effect intent. Outcome processing is independent of auto_paper while retaining Paper-purpose eligibility; isolated experiments cannot activate canonical policy or Lessons. CLI and native App use the same authenticated Observer API. Commands, recovery and safety boundaries: [Debug control](docs/debug-control.md).

研究与执行边界：现有 `RunPurpose::PositionPlan` 表示 research + Decision / target position plan，无 ExecutionGate、PaperCommit、Reconcile 或 Evaluate；`RunPurpose::Paper` 使用同一 `approved_research_proposal` 三组 T1/T3/T5 Analyst/Critic 和 Synthesizer，再进入原执行链。没有新增 RunMode。Paper 的 account / positions / open_orders / fills / quotes / clock Need 保留 scheduler identity 与 provenance，前置只记录 Deferred，执行时才刷新。PositionPlan 执行显示 N/A，Debug manual/continuous、Broker policy、learning scope 仍是独立控制维度。补充研究保留冻结 Session 日期及原 future-data/cutoff 校验，不再要求市场当前开放。NewsWeb acquisition purpose mapping 和 policy hash 未改，缺失或未验证的方向证据不得变成有效 grounds。

未校准 Paper 冷启动仍可生成零执行目标 Decision、持久化 NoOrder 与 OutcomeSchedule；缺 approval 的 pre-trade safety 不作断言，原 blocker 保持 Broker 零写入，真实 Outcome 仍等待完整 baseline 和四资产共同的 T+1/T+3/T+5 交易日，policy 激活始终由 operator 显式完成。

DecisionPolicy 校准全程以 SQL Store 为权威：显式风险限制、由 canonical Outcome 收集的 dataset 和候选 policy 均为不可变 CAS Artifact；collect/build 不激活，operator 按 Artifact ID inspect/validate/activate。首次 scheduler 在无 active policy 时创建无交易 approval 的 Paper 研究 Run，Gate 保持 NoOrder；不会为了冷启动伪造校准数据或放开交易。
