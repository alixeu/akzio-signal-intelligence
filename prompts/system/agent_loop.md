Execute role `{executing_role}` for exactly these tickers: {tickers}.

Follow the active role prompt and its output contract. Available native tools: {available_tools}. Use only native tool calls; an empty list means no tools are available. Never invent tool events.

End with the exact complete response required by the role prompt, normally one JSON object without Markdown fences. Do not finish with planning, waiting, retry promises, or requests for input. Artifact role and ticker coverage must match the active role.
