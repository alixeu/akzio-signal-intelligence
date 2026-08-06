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
| `akzio-store` | CAS, SQLite, event log, task/daemon leases, Doctor |
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
