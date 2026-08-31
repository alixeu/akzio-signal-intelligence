# Akzio Agent Recovery Failpoint Matrix

：2026-08-31。

 deterministic fake provider、 fake tools V2Store 。、 Provider Paper 。

| Failpoint | durable  |  |  |
| --- | --- | --- | --- |
| Provider | `AgentTurnStarted`， Provider  | exactly-once Provider  |  0 ， |
| Provider 、AgentTurn | Provider ， durable AgentTurn | at-least-once； Provider  |  1 Provider  |
| AgentTurn | 、 pending tool Draft AgentTurn | exactly-once Provider turn | Submit， Draft |
| ToolCall | tool call AgentTurn durable，ToolCall durable | at-least-once Provider turn； | Provider turn， ToolCall |
| ToolCall | ToolCall durable，ToolResult  | at-least-once； tool batch  | Provider turn ToolCall  |
| ToolResult | ，ToolResult durable | ； | Provider turn，canonical state  |
| ToolResult | tool batch durable | exactly-once tool result consumption | ToolCall ， Draft/Submit |
| final submission | terminal AgentTurn durable， canonical artifact | at-least-once Provider trajectory | Provider Draft/Submit ； terminal exactly-once |
| final output | output Artifact CAS payload， task artifact/canonical commit | at-least-once Provider trajectory；canonical commit | ， task commit |
| task commit | handler artifacts，attempt Running | handler at-least-once；commit exactly-once | lease recovery handler， succeeded output |
| task commit 、 | `commit_attempt`  | exactly-once canonical commit | handler，`TaskSucceeded` output  |
| task finish | handler NoOutput，attempt Running | handler at-least-once；terminal transition exactly-once | recovery handler， Succeeded transition |
| task finish 、 | terminal transaction  | exactly-once terminal transition | handler，`TaskSucceeded`  |
| heartbeat  | lease 、attempt  terminal | lease recovery； permit fenced | recovery=1， recovery=0， permit `StalePermit` |

## 

- AgentTurn、ToolCall ToolResult Store event artifact durable 。
- pending tools AgentTurn ToolResult batch durable continuation exactly-once。
- Provider usage、tool-call count、reasoning/cached token cost ledger durable checkpoint ，。
- context manifest、ReadGrant scope、materialization、tool set、route capability、budget policy pricing identity fail closed。
- `commit_attempt` terminal `finish_task` ； ack canonical output terminal event。
- lease recovery attempt/epoch， permit 。

## 

Provider 、 AgentTurn Store ，Rust Provider continuation usage 。 Provider， at-least-once， Token 。 Provider、， Provider exactly-once。

## 

- failpoint ，。
- Provider ID、usage、cached/reasoning tokens 。
- AgentTurn、ToolCall、ToolResult terminal submission kill/restart。
- `commit_attempt`/`finish_task` kill/restart， canonical output、event attempt proof。
- heartbeat， lease ， worker fencing recovery。
- context、tool set、route pricing ， fail closed。
