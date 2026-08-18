@/Users/alixeu/.codex/RTK.md

# Akzio v2 agent instructions

This repository is a Rust 2021 workspace for a local, Paper-only Multi-Agent Research System. It is v2-only: do not recreate compatibility code for old `orchestrator-*` crates, Phase 0–8, FileStore layouts, prompts or `outputs/store`.

## Source of truth

- Rust owns state, authorization, contracts, task budgets, workflow gates, persistence, learning transitions and execution policy.
- `V2Store` is the only persistence authority. Do not add parallel JSON state, caches that change semantics, or direct SQLite writes outside `akzio-store`.
- Evidence, claims, decisions, execution and memory must retain provenance and valid `source_refs`.
- `akzio-context` is the only route by which agent tasks obtain documents. Do not give model code arbitrary filesystem or raw-evidence access.
- Live Trading is unsupported. `AlpacaPaper::new` must reject non-Paper endpoints.
- Debug and Paper Dry Run are noncanonical: they must not promote memory or topology.

## Architecture boundaries

| Area | Owns |
| --- | --- |
| `akzio-domain` | Stable schemas and validation, no I/O |
| `akzio-store` | SQLite-embedded CAS BLOBs, event log, task/daemon leases, Doctor |
| `akzio-runtime` | Workflow compilation, planner patches, task lifecycle/recovery |
| `akzio-research` | Agent contracts and model-mediated research only |
| `akzio-execution` | Rust decision/execution gates and Paper broker protocol |
| `akzio-learning` | Outcome evaluation and bounded policy state transitions |
| `akzio-daemon` | Process leadership, scheduling, transport and task dispatch |

Do not combine these concerns in `akzio-daemon` dispatch code. If a change is a policy, put it in its owning domain/runtime crate; if it is a durable invariant, enforce it in `akzio-store` too.

## Paper scheduling and safety

- Paper runs are scheduler-owned. Never restore a direct CLI/API `Paper` submit or retry path.
- The scheduler uses Alpaca Paper's market clock; it must create no more than one durable session slot per broker session date.
- A session slot stores the exact workflow plan before run creation. Recovery must reuse that plan and its task IDs.
- Every scheduler write must validate the active daemon lease owner and epoch. Stale leaders may not submit, mark or overwrite a slot.
- Execution remains Rust-gated: validate account, quotes, allocation, turnover, blockers, plan hash and idempotency before broker submission.

## Learning and topology

- Canonical learning is Paper-only and outcome-backed. Never learn from Debug, Dry Run, current predictions or unsealed market data.
- Memory and topology state are immutable-document histories; do not mutate a prior record in place.
- Shadow pairs must reference parent Decision, ExecutionContext and candidate Decision. Pair completion must remain idempotent even when timestamps collide.
- Promotion requires fresh paired outcomes at each canary level. Lower risk recall or evidence completeness rolls a candidate back.

## Execution waves and code-generation discipline

Follow the plan in order. Do not reopen a completed wave unless current evidence shows a regression.

- **R0–R10 implementation wave:** domain, Store, Context/Evidence, contracts, workflow/replay, learning, execution, daemon/HTTP, CLI and final offline cleanup. Treat these as complete only when the current tree passes their exit evidence; historical prose is not proof.
- **Paper canary wave:** a real Alpaca Paper run may validate broker/session/receipt/reconciliation behavior, but only that run and its durable evidence are canonical proof. Fixture, Debug, Dry Run and Replay are never real-Paper proof.
- **Outcome wave:** after a real Paper run, the scheduler must wait for real T+1, T+3 and T+5 sessions. Only sealed Paper outcomes may create Retrospective, Experience, Evaluation or Policy Transition. Never fabricate, backfill or mock these artifacts.
- **Approval wave:** final human launch approval remains separate from code, offline tests and a Paper canary.

Current checkpoint (2026-08-18; refresh from Store before relying on it):

- Run `77395cfd-8d03-405d-9b47-ca99b19525f1` completed a real Paper canary on 2026-08-17; four orders filled and reconciliation completed.
- Its `learning.outcome_worker` is still queued for `2026-08-18T22:00:00Z`; only `OutcomeSchedule` exists so far.
- T+1/T+3/T+5 sealing, Retrospective, Experience, Policy Evaluation/Transition and final human approval are not complete.

For every code-generation task:

1. Start with a narrow inventory and identify the current wave and owner crate.
2. Reuse existing types/helpers; make the smallest change that satisfies the wave exit gate.
3. Keep policy in its owner crate and durable invariants in `akzio-store`; do not introduce speculative abstractions, parallel state, or compatibility layers.
4. Preserve serialization, hashes, provenance, source closure, lifecycle, transaction boundaries, leases, gates and replay semantics unless the plan explicitly changes them.
5. Run the narrowest relevant test immediately, then the required workspace checks before declaring the wave complete.
6. Report four separate states: implemented, offline-verified, real-Paper-verified and outcome/learning-verified. Never collapse them into one “done”.

When a task is only a refactor or storage change, prove behavioral equivalence first; do not mix it with schema-version, `ExecutionPlan` serde/hash, Paper gate, transaction-boundary or learning-policy changes.

## Code navigation

Use CodeGraph for structural questions only when its index reflects the v2 tree. If it only contains deleted v1 files, use current filesystem/Cargo/tests as truth and rebuild the index before relying on it.

Use `rg` for literal text and `rg --files` for discovery. Start nontrivial work with:

```bash
rtk git status --short --untracked-files=all
```

## Verification

For code changes, run the narrow crate tests while iterating, then:

```bash
rtk cargo fmt --all
rtk cargo check --workspace
rtk cargo clippy --workspace --all-targets
rtk cargo test --workspace
rtk cargo run -p akzio-cli -- run fixture-debug
rtk cargo run -p akzio-cli -- store doctor
```

Keep generated Store Roots, blobs, sockets, reports, credentials and local config overrides out of Git. Preserve unrelated dirty work; do not reset, clean or checkout the workspace.
