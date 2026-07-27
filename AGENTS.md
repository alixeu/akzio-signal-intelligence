@/Users/alixeu/.codex/RTK.md

# Agent Instructions

This repository is a Rust workspace for AI-assisted market-signal research and TQQQ-oriented report workflows.

## Project Snapshot

- Language: Rust 2021.
- Workspace crates:
  - `orchestrator-core`: shared config, paths, ticker parsing, prompt helpers, and artifact validation.
  - `orchestrator-store`: atomic FileStore persistence, manifests, sessions, drafts, indexes, and execution recovery.
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
- Phase 2 builds one shared Bull/Bear warm-up checkpoint and runs Topic
  Generator independently. Each topic's Bull/Bear conversations fork from the
  warm-up checkpoint, while Topic Controller forks from Topic Generator.
  Debate reduction remains Rust-owned.
- Phase 0 historical scoring/task selection, Phase 7 allocation, and Phase 8
  decision snapshot/archive are Rust-owned stages. Phase 0 uses a dedicated
  historical-reflector prompt for causal analysis.
- A non-mock workflow records outcome-backed historical cases as Experience
  Index/Detail entries for later retrieval.
- Phase Summary is the only cross-phase semantic interface for model roles in
  Phases 2–6. Roles list summaries before expanding details; Rust enforces
  role-specific source-phase, pagination, detail-budget, and evidence-ID policy.
- Phase 5 reviewers run independently in parallel. Portfolio Manager combines
  their separately compressed Phase 5 summaries.
- Generated run outputs live under `outputs/` and should not be committed.
- Runtime defaults live in `config/config.yaml`.
- Live agent runs use run-sealed FileStore input snapshots by default.

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
rtk cargo run -p orchestrator-cli --bin report-email -- --help
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
- Prefer existing helpers in `orchestrator-core` and `orchestrator-store` before adding new utilities.
- Validate inputs at CLI and system boundaries.
- Do not hardcode secrets; use environment variables.
- Preserve mock paths for local development without `LLM_GATEWAY_API_KEY`.
- Do not let live `orchestrator-exec` consume mutable market inputs directly; seal atomic Technical/Jin10 input snapshots in the run FileStore first.
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
- Do not describe YouTube or Reddit/X as active inputs until ingestion, FileStore
  context readers, role registration, prompts, and scheduling are all configured.
- Keep reflection outcome-backed and historical: never learn from mock runs,
  unscored predictions, or the current prediction. Experience Index writes must
  remain idempotent, and reflection failures must not invalidate a completed
  investment decision.
- Avoid committing local config, FileStore data, build output, or report artifacts.

## Documentation Rules

- Update `README.md` when commands, setup steps, environment variables, or crate responsibilities change.
- Put durable project knowledge in existing docs or module-level comments only when it helps future maintainers.
- Do not create new top-level docs unless the task explicitly needs them.
