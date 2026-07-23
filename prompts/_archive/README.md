# Archived prompts

Prompts in this directory are **not** loaded by the runtime.

Use this folder for retired revisions that should remain in git history for
reference. Active role prompts live under:

- `prompts/phase_summary/`
- `prompts/phase1/`
- `prompts/phase2/`
- `prompts/phase3/`
- `prompts/phase4/`
- `prompts/phase5/`
- `prompts/phase6/`
- `prompts/common/` (shared includes)
- `prompts/common/components/` (plugin components)
- `prompts/system/` (agent-loop and runtime messages)

Phase 2 topic generation uses an evidence-only LLM prompt, while Rust owns its
runtime envelope, validation, deterministic fallback, and final debate
reduction. Phase 7 allocation and Phase 8 reflection/archive are Rust-owned
stages without active role prompt files.

## Active phase-2 steer_room set

| File | Role |
|------|------|
| `phase2/topic_generator.md` | evidence-only topic generation |
| Rust conflict detector | topic validation, fallback, and debate gate |
| `phase2/researcher/warmup.md` / `seed.md` / `debate.md` | kind-specific Bull/Bear turns; `side_bull.md` and `side_bear.md` supply the strategy delta |
| `phase2/topic_controller.md` | mediator.topic_controller |
