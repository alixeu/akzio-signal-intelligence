@/Users/alixeu/.codex/RTK.md

# Agent Instructions

This repository is a Rust workspace for AI-assisted market-signal research and TQQQ-oriented report workflows.

## Project Snapshot

- Language: Rust 2021.
- Workspace crates:
  - `orchestrator-core`: shared config, paths, ticker parsing, prompt helpers, and artifact validation.
  - `orchestrator-sql`: SQLite schema, imports, scoped messages, and read-context commands.
  - `orchestrator-llm`: Rig/OpenAI execution and mock role artifacts.
  - `orchestrator-cli`: CLI binaries and workflow orchestration.
- Prompt templates live under `prompts/`.
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
- Avoid committing local config, SQLite databases, build output, or report artifacts.

## Documentation Rules

- Update `README.md` when commands, setup steps, environment variables, or crate responsibilities change.
- Put durable project knowledge in existing docs or module-level comments only when it helps future maintainers.
- Do not create new top-level docs unless the task explicitly needs them.
