# Archived prompts

Prompts in this directory are **not** loaded by the runtime.

Use this folder for retired revisions that should remain in git history for
reference. Active role prompts live under:

- `prompts/analysts/`
- `prompts/researchers/` (including `*_monitor.md` for `--mode monitor`)
- `prompts/mediators/`
- `prompts/managers/`
- `prompts/risk/`
- `prompts/traders/`
- `prompts/allocation/`
- `prompts/common/` (shared includes)
- `prompts/components/` (plugin components)

## Active phase-2 steer_room set

| File | Role |
|------|------|
| `mediators/topic_generation.md` | mediator.topic |
| `researchers/bull.md` | researcher.bull.{warmup,initial,interaction} 长会话 |
| `researchers/bear.md` | researcher.bear.{warmup,initial,interaction} 长会话 |
| `mediators/topic_controller.md` | mediator.topic_controller |
| `researchers/*_initial_monitor.md` | only when `--mode monitor` |

