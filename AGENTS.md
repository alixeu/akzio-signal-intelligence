@/Users/alixeu/.codex/RTK.md

# Agent Instructions

This repository is a Rust workspace for AI-assisted market-signal research and TQQQ-oriented report workflows.

## Project Snapshot

- Language: Rust 2021.
- Workspace crates:
  - `orchestrator-core`: shared config, paths, ticker parsing, prompt helpers, and artifact validation.
  - `orchestrator-sql`: SQLite schema, imports, scoped messages, and read-context commands.
  - `orchestrator-llm`: OpenAI Responses API execution and mock role artifacts.
  - `orchestrator-cli`: CLI binaries and workflow orchestration.
- Prompt templates live under `prompts/` and are owned by their runtime phase:
  - `phase_summary`: completed-phase compression.
  - `phase1`: technical and news/macro research.
  - `phase2`: Topic Generator, Bull/Bear debate, Topic Controller, and steer messages.
  - `phase3`: Research Manager probability decision.
  - `phase4`: Trader conversion.
  - `phase5`: aggressive, neutral, and conservative risk reviewers.
  - `phase6`: Portfolio Manager final decision.
  - `common`: reusable contracts/components; `system`: agent-loop messages.
- Prompt components are role-scoped. Topic Generator and Research Manager use
  the analytical trace; Trader and Portfolio Manager use the execution trace;
  Phase Summary uses the summary trace. Bull/Bear packets, Topic Controller,
  and Phase 5 risk reviewers keep their compact packet/constraint audit data.
- Phase 2 starts Topic Generator, Bull warm-up, and Bear warm-up concurrently.
  Each selected topic forks Bull/Bear from their warm-up turns and its Topic
  Controller from the Topic Generator turn. Debate reduction remains Rust-owned.
- Phase 0 historical scoring/task selection, Phase 7 allocation, and Phase 8
  decision snapshot/archive are Rust-owned stages. Phase 0 uses a dedicated
  historical-reflector prompt for causal analysis.
- A non-mock workflow scores predictions after three stored trading bars and
  promotes qualified historical experience for retrieval on later runs.
- Generated run outputs live under `outputs/` and should not be committed.
- Runtime defaults live in `config/config.yaml`.
- Live agent runs use strict SQLite input by default.

## Commands

Use these checks before handing off code changes:

```bash
rtk cargo fmt --all
rtk cargo test
rtk cargo clippy --workspace --all-targets
```

Common local runs:

```bash
rtk cargo run -p orchestrator-cli --bin orchestrator-exec -- --mock
rtk cargo run -p orchestrator-cli --bin run-daily-tqqq-report -- --mock --skip-send
```

## CodeGraph

This project has a CodeGraph MCP server (`codegraph_*` tools) configured. CodeGraph is a tree-sitter-parsed knowledge graph of every symbol, edge, and file.

Use CodeGraph for structural questions:

| Question | Tool |
| --- | --- |
| Where is a symbol defined? | `codegraph_search` |
| What calls a symbol? | `codegraph_callers` |
| What does a symbol call? | `codegraph_callees` |
| How does one symbol reach another? | `codegraph_trace` |
| What would a change affect? | `codegraph_impact` |
| Show signature/source/docstring | `codegraph_node` |
| Get task-area context | `codegraph_context` |
| Explore related source | `codegraph_explore` |
| Browse indexed files | `codegraph_files` |

Prefer `codegraph_context` first for architecture, feature, or bug-context questions. Use native `rg` only for literal text queries, generated files, or after a specific file is already identified.

## Coding Rules

- Keep changes scoped and aligned with the existing crate boundaries.
- Prefer existing helpers in `orchestrator-core` and `orchestrator-sql` before adding new utilities.
- Validate inputs at CLI and system boundaries.
- Do not hardcode secrets; use environment variables.
- Preserve mock paths for local development without `LLM_GATEWAY_API_KEY`.
- Do not make live `orchestrator-exec` read network, CSV, or external JSON directly. Import those sources into SQLite first.
- Keep prompt paths configured under `orchestrator.prompts` and fail early if a configured prompt file is missing.
- Keep `mediator.topic` evidence-only: it may use the Phase 1 index and prior
  phase summaries, while Rust owns the topic artifact runtime envelope and
  deterministic fallback.
- Do not create a cross-phase prompt bucket such as `phase25`; move a role prompt
  with its executing phase and update config defaults, `include_str!` paths,
  prompt lint role inference, golden render tests, README, and this file together.
- Keep the three Phase 5 reviewers on distinct prompt paths. Shared constraints
  belong in `prompts/phase5/risk_analyst.md`, while stance-specific behavior
  remains in `prompts/phase5/{aggressive,neutral,conservative}.md`.
- Do not describe YouTube or Reddit/X as active inputs until ingestion, SQLite
  context readers, role registration, prompts, and scheduling are all configured.
- Keep reflection outcome-backed and historical: never learn from mock runs,
  unscored predictions, or the current prediction. Candidate distillation must
  remain idempotent, and reflection failures must not invalidate a completed
  investment decision.
- Avoid committing local config, SQLite databases, build output, or report artifacts.

## Documentation Rules

- Update `README.md` when commands, setup steps, environment variables, or crate responsibilities change.
- Put durable project knowledge in existing docs or module-level comments only when it helps future maintainers.
- Do not create new top-level docs unless the task explicitly needs them.
