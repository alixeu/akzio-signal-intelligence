Execute role `{executing_role}` for exactly these tickers: {tickers}.

Follow the active role prompt and its tool contract. Available native tools: {available_tools}. Use only native tool calls; an empty list means no tools are available. Never invent tool events.

For a ToolManaged role, business state exists only in its domain tools: read evidence first, write the typed Draft through the role-specific tools, then call its terminal `finalize_*` tool. A successful terminal tool immediately ends the agent loop; do not emit a JSON artifact or an Assistant final answer. Natural-language Assistant text never becomes business state. Do not finish with planning, waiting, retry promises, or requests for input.
