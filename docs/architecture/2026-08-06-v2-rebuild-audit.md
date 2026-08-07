# Akzio v2 rebuild audit

## Scope and baseline

- Baseline commit: `fa6986cb534b428ddb7e3be7415aa849d977d7b1` (`V2`).
- This is a v2-only, local, Paper-only system. The executable universe remains exactly
  `TQQQ`, `QQQ`, `SOXX`, and `SOXL`; Live Trading is not part of the rebuild.
- Existing v2 run data, the current store schema, Unix-socket control protocol, and
  v1/Phase compatibility are deliberately not migration targets. The rebuilt system
  gets a new Store Root and rejects an incompatible old database at open time.

## What is worth preserving as a principle

| Existing area | Keep the principle | Do not preserve the implementation |
| --- | --- | --- |
| `akzio-domain` | integer portfolio arithmetic and a closed executable asset universe | the broad, string-heavy mixed domain vocabulary |
| `akzio-store` | CAS blobs, immutable document references, SQLite durability, leases, and Store Doctor | UUID document identity as the primary integrity anchor, split-write workflow submission, and permissive artifact writes |
| `akzio-context` | agents receive brokered documents rather than filesystem access | run-wide implicit context expansion and raw reread by arbitrary durable ID |
| `akzio-runtime` | worker leases, retries, cancellation, and a Rust-owned non-bypassable execution path | a fixed lifecycle graph encoded as task kinds and compiler special cases |
| `akzio-execution` | deterministic allocation/order planning, Paper-only adapter, idempotency, reconciliation | an execution input that does not consume decision blockers, evidence status, or factor exposure |
| `akzio-learning` | immutable outcome-backed history and paired Shadow comparison | memory-as-summary and the two-topology baseline-versus-no-challenger experiment |
| `akzio-daemon` | durable queue, epoch fencing, automatic Paper scheduling, event replay | business orchestration spread across daemon dispatch and duplicate control protocols |

## Source-confirmed rebuild drivers

1. `AgentRole` is a closed four-value enum; Planner proposals can only choose
   Investigator or Challenger. The installed contract registry is role-indexed,
   so it cannot express a candidate role graph or versioned capability catalogue.
2. `WorkflowCompiler` requires singleton Ingest, MemoryOverlay, Plan, Synthesize,
   DecisionGate, ExecutionGate, Reconcile, Evaluate, and Shadow lifecycle nodes in
   a hard-coded sequence. A dynamic Planner currently fills two slots inside a
   static graph rather than planning a workflow.
3. Agent context starts from task inputs and then adds every allowed document in the
   run. Tool rereads take a durable document ID and check kind/source only; they do
   not prove the document belongs to the calling manifest or its source closure.
4. `register_document` checks document shape and reference existence but has no
   write permit bound to the active task attempt, lease, epoch, run, or contract.
   An obsolete worker can therefore create an orphan artifact after its lease is
   superseded.
5. Workflow submission writes the run, plan, tasks, dependencies, and submitted
   event through separate Store calls. A crash can leave a partially materialized
   workflow that recovery must infer.
6. `AgentContract::validate` does not recompute the self-declared contract hash.
   Contract identity, prompt, schema, budget, and grants are not a single
   canonical-content commitment.
7. `PaperDryRun` participates in topology selection/initialization even though it
   must be noncanonical. This demonstrates that canonical-learning classification
   is not owned by one policy gate.
8. Current execution planning accepts targets, account, quotes, turnover, and time
   but not typed blockers, evidence freshness verdicts, claim conflicts, or factor
   exposure. The decision's free-form blockers therefore cannot be a durable
   rejection condition.
9. The daemon exposes both HTTP/SSE and a Unix JSON-line control path. The CLI uses
   the latter; socket startup removes a same-path file. This gives two divergent
   command surfaces and no single API contract for a future local UI.

## Default architecture decisions

These defaults are used unless a later product decision explicitly replaces them.

1. **Evidence**: model code never performs network access. A model may emit a typed
   `EvidenceNeed`; allowlisted Rust adapters acquire and seal the source into CAS.
   The initial concrete adapter remains Alpaca, while the interface permits
   allowlisted news, macro, filings, and options/flow adapters without granting an
   agent raw HTTP access.
2. **Contract evolution**: active contracts are immutable and executable; candidate
   contracts/topologies are declarative, capability-bounded, and evaluated through
   replay and paired Shadow. Policy may auto-promote a candidate but cannot promote
   a contract that expands tool authority or execution authority.
3. **Cadence**: evidence acquisition and research are event-driven; Paper execution
   is constrained to one scheduler-owned portfolio commit per broker session. A
   research refresh may update the next commit but cannot create intraday churn.
4. **Safety**: execution is automatic only after a Rust-owned HardBlocker gate,
   evidence freshness/completeness threshold, factor exposure, account, quote,
   turnover, plan hash, and reconciliation conditions pass. Soft warnings are
   traceable decision inputs; unresolved material contradictions become hard
   blockers. A local durable `freeze` switch is mandatory and does not require
   per-order approval.
5. **Control plane**: localhost HTTP plus replayable SSE is the only business
   control protocol. CLI and any future UI use the same API. SQLite lease/epoch
   fencing, not a Unix socket path, owns singleton leadership.

## Target seams

The rebuild should create deep modules with narrow interfaces:

- **Store**: atomically commits a run graph, artifact, event, and task transition
  through typed write permits; callers never build SQL fragments or partial plans.
- **Contract catalogue**: returns a canonical, hashed `AgentContract` and a
  capability-bounded `TaskRecipe`; it is the only source for prompts, schemas,
  limits, tools, termination, and retry policy.
- **Workflow runtime**: lowers a Planner proposal into a valid graph, owns task
  lifecycle/recovery, and exposes fixed Rust gates as edges that cannot be removed.
- **Context broker**: returns a manifest plus read grants; every tool read checks
  the manifest closure, lifecycle, byte limit, and source policy.
- **Evaluation engine**: derives Experience, outcome observations, calibration,
  ablations, and candidate transitions from immutable references; it is the only
  authority allowed to activate research policy.
- **Execution runtime**: converts an accepted Decision Context into either a
  `NoOrder` explanation or an idempotent Paper commitment. It has no model
  dependency.

## Mandatory deletion set

- Fixed `AgentRole`/`PlannedResearchRole` topology encoding and the
  baseline-versus-investigator-only topology special case.
- Lifecycle task special casing used to encode the former Phase-like graph.
- Any compatibility readers or schema adapters for an old Store Root.
- Unix JSON-line daemon business protocol and CLI client.
- Run-wide implicit context selection and document-ID-only tool rereads.
- Memory/topology paths reachable from Debug or Paper Dry Run.

## Verification baseline

Before changing source, the workspace test suite and `akzio store doctor` were run
against the baseline. The existing output Store is retained only as a diagnostic
artifact; it is not a migration input or proof that the rebuilt runtime works.

