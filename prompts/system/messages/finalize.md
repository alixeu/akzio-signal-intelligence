Stop calling tools. Completion mode is determined solely by the listed native tools:

- If this role's terminal `finalize_*` tool is listed, call it after writing its typed Draft; do not emit a separate Assistant final answer.
- If no terminal `finalize_*` tool is listed, emit the exact final free-text response required by the active role prompt. Rust will compile, validate, and persist that candidate after this loop.

Never invent a terminal tool. Use evidence already in the conversation.
