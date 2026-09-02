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
The canonical Paper graph has three bounded Analyst/Critic pairs, one per horizon, feeding one Synthesizer. Each nonneutral asset/horizon slot requires verified directional price, macro and news grounds. MissingEvidence carries scope; unverified slots are neutral. This is independent of four successful market-price downloads.

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
Domain schema 10; Store schema 15; canonical Contract 20; Prompt bundle 14; freshness candidate 21; Outcome metric basis v3; evaluation context v1; benchmark definition v1 unchanged. These name current code semantics. See [implementation handoff](docs/task1-repair-handoff.md) and [Task 2 debug entrypoints](docs/task2-debug.md) for compatibility and the distinction between implementation checks and complete runtime evidence.

Follow-up invariants: claims/critiques accept at most 12 grounds (four single-asset price, four single-asset news, one shared macro is a legal minimum). Context minimum sets precede optional allocation. Submit requires a persisted Draft memo. Per-stage retries reset only at committed retrospective events; fenced lease release also runs on cancellation. Narrative repair reuses the sealed Outcome and enters the ordinary eligibility/consumption transaction.

Outcome projection v2 keeps aggregate numeric facts and all twelve forecasts, explicitly identifies omitted detail, and retains the original documents in the 128 KiB / 24-artifact grant (32k estimated tokens). The model invocation remains limited to 12k input tokens including tool reads. Registered Canary Shadows reuse only their parent's committed T0 normalized evidence and frozen baseline execution lineage. Incremental Canary evaluations stay in the durable evaluation ledger; an interrupted Attempt is never indexed as a succeeded output. Executed offline scenarios and remaining verification limits are recorded in [Task 2 offline debug](docs/task2-offline-debug.md).
