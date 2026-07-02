# Akzio Signal Intelligence

Rust workspace for running AI-assisted market-signal research workflows. The project orchestrates analyst prompts, LLM-backed role execution, SQLite-backed run state, technical-data imports, and daily TQQQ-oriented report generation.

## What Is Included

- Multi-crate Rust workspace with shared core, SQL, LLM, and CLI crates.
- Prompt packs for analysts, researchers, mediators, managers, and risk profiles.
- CLI binaries for orchestrator runs, SQL context tools, transcript fetching, Jin10 flash data, Twelve Data technical indicators, and report email delivery.
- Explicit mock execution paths for local development without live provider API keys.

## Workspace Layout

| Path | Purpose |
| --- | --- |
| `crates/orchestrator-core` | Shared config loading, project paths, ticker parsing, prompt helpers, and artifact validation. |
| `crates/orchestrator-sql` | SQLite schema, imports, scoped agent messages, and read-context commands. |
| `crates/orchestrator-llm` | Rig LLM integration, OpenAI-compatible Responses routing, tool registration, and mock agent artifacts. |
| `crates/orchestrator-cli` | Binary entry points and orchestration commands. |
| `prompts/` | Markdown prompt templates for analyst, researcher, mediator, manager, meta, and risk roles. |

## Requirements

- Rust toolchain with Cargo.
- Live provider API keys configured locally when a selected provider requires them.
- Optional `curl` command for SMTP report delivery.

## Quick Start

```bash
cargo test
cargo build
```

Run a mock market research workflow:

```bash
cargo run -p orchestrator-cli --bin orchestrator-exec -- TQQQ --mock
```

Phase 1 defaults to `technical,news`. Social analysts remain available by
explicit opt-in, for example `--phase1-agents technical,news,reddit` after
their source context has been imported into SQLite.

Run only Phase 1:

```bash
cargo run -p orchestrator-cli --bin orchestrator-exec -- TQQQ --mock --to-phase 1
```

Build a report payload/HTML from the latest run output:

```bash
cargo run -p orchestrator-cli --bin report-email -- --mode build
```

Send the built report by email:

```bash
cargo run -p orchestrator-cli --bin report-email -- --mode send
```

`report-email` sends the first report each day. After that, it sends again only
when the direction reverses between long and short and the new direction
probability is at least `report.email.probability_threshold`.

## Main CLI Binaries

| Binary | Purpose |
| --- | --- |
| `orchestrator-exec` | Run the phased stock-analysis orchestrator. |
| `orchestrator-sql` | Read/write run context in SQLite for agent tools. |
| `report-email` | Build and send the daily report email. |
| `fetch-jin10-flash` | Fetch Jin10 flash/news context. |
| `fetch-last30days-context` | Fetch Reddit, X, or YouTube social context. |
| `fetch-youtube-transcript` | Fetch YouTube transcript data. |
| `fetch-wayinvideo-transcript` | Fetch transcript data through WayinVideo. |
| `run-technical-indicators` | Fetch Twelve Data bars and compute local technical indicators. |

## Configuration

Runtime configuration is loaded from `config/config.yaml`. This file is the default source for role-level LLM settings, prompt paths, output paths, analyst weights, SQLite requirements, and data-ingestion defaults. CLI flags still override config values when provided.

Live orchestrator runs use a strict SQLite data-source policy by default. Network, CSV, and file-based collectors should run before the orchestrator and import their outputs into SQLite. The orchestrator then reads context from SQLite only.

`orchestrator.db_path` is the shared runtime SQLite database, defaulting to `outputs/orchestrator.sqlite`. Direct `orchestrator-exec`, the Rust daily flow, and technical imports use this same database unless a CLI `--db-path` is provided.

### Workflow Stages And Reducers

The orchestrator is moving toward a `Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact` execution model. Agent workers produce role-specific artifacts; reducers compress those artifacts into durable state briefs that downstream stages can consume without rereading every raw message.

`orchestrator.workflow` controls the implemented runtime knobs. Missing keys use conservative defaults in the Rust orchestrator:

- `phase1.parallelism` or `parallel.max_worker_concurrency` controls Phase 1 worker fan-out.
- `agent_timeout_sec` or `timeouts.worker_sec` controls worker timeout seconds.
- `reducer_timeout_sec` or `timeouts.reducer_sec` controls LLM controller timeout seconds.
- `critical_roles.phase1` lists roles that must complete for a stage to proceed.
- `late_evidence.enabled` controls whether delayed worker/source outputs are appended to state artifacts and marked as late.

Roles not listed under `critical_roles` are treated as noncritical. A noncritical role failure should degrade the relevant state artifact with an explicit evidence gap instead of blocking the whole workflow. Critical role failure should block the affected stage unless the runtime explicitly overrides that policy.

Reducer state artifacts are built deterministically in Rust. Phase 1.5 writes the evidence state artifact from existing analyst artifacts. Phase 2 first runs `mediator.topic` to generate topic candidates from that Phase 1.5 artifact. Each topic then runs sequential bull/bear micro-turns with a per-topic `mediator.topic_controller` Phase 2.5a artifact after every micro-turn. Phase 2.5b writes the final debate state brief from topic controller artifacts.

Standalone `fundamental` analyst execution was removed. Fundamental company facts belong inside `analyst.news_macro` and are treated as a news sub-signal rather than an independent vote.

### Agent Loop And ReAct Runtime

The runtime now has an explicit Turn loop instead of relying only on one prompt returning one final artifact. A Turn is the lifecycle unit for a role request and records ordered items:

- `user_message`
- `assistant_message`
- `reasoning_summary`
- `tool_call`
- `tool_result`
- `system_context`
- `developer_context`
- `compact_summary`
- `injected_context`

Current gap from the old path: the underlying model library already had an internal tool loop, but that loop hid tool calls, tool results, follow-up decisions, and conversation history from this project. The project runtime now owns those pieces. Every live role call enters `run_rig_agent_loop`, which builds a Turn, asks the model for a structured next action, executes any requested tool through the runtime, appends the tool result to session history, and continues until the model produces a final assistant message or a stop condition is hit.

Turn history is append-only in SQLite:

- `agent_turns` stores lifecycle state such as `turn_id`, `session_id`, `user_input`, `model_context`, `needs_follow_up`, and `end_reason`.
- `agent_turn_items` stores ordered context items, tool calls, tool results, compact summaries, and injected context.

The first implementation focuses on the minimal closed loop:

1. Build model input from session history, user input, pending steer input, and tool results.
2. Ask the model to return the loop action fields: `assistant_message`, `reasoning_summary`, `tool_calls`, and `end_turn`.
3. Validate tool names and arguments in the runtime.
4. Execute blocking tools and append `tool_result`.
5. Continue while there are tool calls, tool results, pending steer inputs, or `end_turn=false`.
6. End with `completed`, `max_loops`, or an explicit error reason.

### Role-Level LLM Settings

Every business role must have an entry under `orchestrator.llm.roles`. Common provider, model, reasoning, turn-limit, key, and tool settings can live under `orchestrator.llm.defaults`; each role inherits those defaults and only needs to define fields that differ. Missing roles, unknown tools, missing gateway fields, or missing direct API keys fail during startup.

Defaults and each role support:

| Field | Purpose |
| --- | --- |
| `route` | Use `responses` for `${base_url}/responses`. |
| `model` | Provider model name for that role. |
| `base_url` | Gateway API root, usually ending in `/v1`; the runtime appends `/responses`. |
| `api_key` | Direct local API key value for the configured provider. |
| `preamble` | Optional Rig agent preamble for role-level steering. It is omitted by default and should not be used as the structured-output enforcement mechanism. |
| `max_turns` | Optional agent-loop turn cap. Set `null` or omit it for no role-level max-turn cap; set a positive number on a role to override. |
| `reasoning_effort` | Optional Responses reasoning effort, injected as `additional_params.reasoning.effort` when set to a value other than `none`. |
| `reasoning_summary` | Optional Responses reasoning summary level: `auto`, `concise`, or `detailed`. |
| `preserve_reasoning_state` | When `true`, requests include `reasoning.encrypted_content`, set `store: false`, persist encrypted reasoning state, and replay it on the next model iteration. |
| `transport` | Use `http` by default. Use `ws` for Responses WebSocket mode. |
| `think_tool` | Registers Rig `ThinkTool` for that role when `true`. |
| `tools` | Names of external tools available to that role. Use `all` in defaults to expose every registered project tool, then override per role when needed. |
| `native_web_search` | Set `true` when the gateway/model supports provider-native web search. When enabled and `orchestrator.web_search.mode` is `live`, the request uses hosted `web_search` and does not expose the configured `web.run` fallback. |

Responses routing behavior:

- `route: responses` uses Rig's OpenAI Responses client with the role's `base_url`; the final request path is `${base_url}/responses`, so set `base_url` to the gateway API root ending in `/v1`.
- `transport: ws` enables Responses WebSocket mode for tool-aware event handling.
- Reasoning params are injected for Responses routes only. With `preserve_reasoning_state: true`, the runtime stores OpenAI encrypted reasoning state locally and replays it as a typed Responses `reasoning` input item on the next iteration.
- Web search follows role capability: when `orchestrator.web_search.mode` is `live`, roles with `native_web_search: true` use provider-native web search; all other roles receive the `web.run` agent tool backed by Exa MCP. Other modes send no web search tool.
- `manager.research` uses Rig typed structured output for `ResearchArtifact`; on Responses routes, the runtime uses provider-native Responses structured output when the gateway/model supports it, then validates probabilities and ticker payloads.
- Other JsonArtifact roles still parse JSON text from their prompt/contracts and validate those artifacts after the model response.

`--model` overrides only the role model names. `--reasoning-effort` overrides only role Responses reasoning effort. `--mock` bypasses live LLM calls; live daily runs no longer silently downgrade to mock output when provider keys are missing.

Prompts now receive only run-boundary values such as ticker, date, role, phase, round, and topic id. Agents read current-run data through the structured `read_run_context` tool instead of having analyst reports, debate history, or large context packets injected into the prompt.

Unified `/v1/responses` gateway example:

```yaml
orchestrator:
  llm:
    gateway:
      base_url: &llm_gateway_base_url "https://your-unified-llm-gateway.example.com/v1"
      api_key: &llm_gateway_api_key "your-local-gateway-key"
    defaults:
      route: responses
      model: gpt-5.5
      base_url: *llm_gateway_base_url
      api_key: *llm_gateway_api_key
      native_web_search: true
      max_turns: null
      reasoning_effort: low
      reasoning_summary: auto
      preserve_reasoning_state: true
      transport: http
      think_tool: false
      tools: []
    roles:
      manager.research:
        tools:
          - read_run_context
```

Web search fallback example:

```yaml
orchestrator:
  web_search:
    mode: live
    base_url: "https://mcp.exa.ai/mcp"
    api_key: ""
    context_size: medium
    max_result_chars: 12000
  llm:
    gateway:
      base_url: &llm_gateway_base_url "https://your-unified-llm-gateway.example.com/v1"
      api_key: &llm_gateway_api_key "your-local-gateway-key"
    defaults:
      route: responses
      model: gpt-5.5
      base_url: *llm_gateway_base_url
      api_key: *llm_gateway_api_key
      native_web_search: true
      max_turns: null
      transport: http
      tools: []
    roles:
      analyst.news_macro: {}
      analyst.reddit:
        native_web_search: false
```

The `analyst.news_macro` role uses provider-hosted `web_search` through the Responses gateway. Social analyst roles are opt-in and default to reading imported SQLite context only.

Keep real gateway and provider keys in local config or environment variables only. Do not commit real provider keys.

| Variable | Purpose |
| --- | --- |
| `CODEX_PROJECT_ROOT` | Overrides automatic project-root detection. |
| `CODEX_ORCH_DIR_SLUG` | Output directory slug override. |
| `CODEX_ORCH_RUN_DIR` | Explicit run output directory. |
| `CODEX_ORCH_LOG` | Background log path override. |
| `ORCH_DB_PATH` | SQLite path for SQL context commands. |
| `ORCH_RUN_ID` | Run ID for SQL context commands. |
| `ORCH_TICKER` / `ORCH_TICKERS` | Active ticker context for SQL commands. |
| `ORCH_PHASE` / `ORCH_ROLE` | Active phase/role context for SQL commands. |

## Prompt Flow

Prompt templates are configured under `orchestrator.prompts` in `config/config.yaml`.

Phase 2 uses four separate templates:

| Template | Purpose |
| --- | --- |
| `bull_initial` | Bull-side initial analysis and thesis generation. |
| `bull_interaction` | Bull-side research and response to Bear arguments. |
| `bear_initial` | Bear-side initial analysis and thesis generation. |
| `bear_interaction` | Bear-side research and response to Bull arguments. |
| `bull_initial_monitor` / `bear_initial_monitor` | Monitor-mode initial prompt selected by the runtime when `--mode monitor` is active. |
| `phase2.topic_generation` / `mediator.topic` | Phase 2 topic generator that forks debate topics from Phase 1.5 evidence. |
| `mediator.topic_controller` | Phase 2.5a per-topic controller that tracks claim ledger, repeats, unverifiable claims, and next agenda. |

All prompt paths are validated at startup. Missing prompt files fail before live LLM execution.

## SQLite Data Preparation

The SQLite schema is initialized automatically by `orchestrator_sql::connect`. Required context is controlled by `orchestrator.data_source.required_contexts`.

By default, runtime data and imports are written to `orchestrator.db_path` in `config/config.yaml` (`outputs/orchestrator.sqlite`). Use `--db-path` only when intentionally isolating a run.

Useful import commands:

```bash
cargo run -p orchestrator-cli --bin run-technical-indicators -- --symbols QQQ,VIX,SOXX --days 60 --intervals 1d,3h,20min
cargo run -p orchestrator-cli --bin fetch-jin10-flash
cargo run -p orchestrator-cli --bin fetch-last30days-context -- --source youtube --ticker QQQ
```

With `strict_sqlite: true`, live `orchestrator-exec` fails when required SQLite contexts are empty. `--mock` runs skip this check for local development.

## Development

Use standard Cargo commands:

```bash
cargo fmt --all
cargo test
cargo clippy --workspace --all-targets
```

Generated artifacts are written under `outputs/` by default and are intentionally ignored by Git.

## License

MIT, as declared by the workspace package metadata.
