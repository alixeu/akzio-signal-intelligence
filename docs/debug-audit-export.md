# Debug 审计记录与导出

本功能把一次 Run 的持久化事实导出为可分享的目录和 `tar.gz`。它复用 `V2Store` 的 CAS、事件 cursor、Attempt、AgentTurn、ToolCall/ToolResult 和 DebugSession；导出物是只读派生物，不是新的状态权威。

## 入口

推荐使用仓库根目录下的 POSIX `sh` 脚本：

```sh
sh scripts/export_debug_bundle.sh \
  --config /path/to/isolated-debug.toml \
  --run-id RUN_ID \
  --out /path/to/debug-exports
```

Core 已运行时，脚本通过配置中的 loopback endpoint 和已有 `.daemon-token` 调用认证的只读 Store operation。Core 已退出时，显式指定离线 Store Root：

```sh
sh scripts/export_debug_bundle.sh \
  --config /path/to/isolated-debug.toml \
  --store /path/to/isolated-store \
  --run-id RUN_ID \
  --out "/path with spaces/debug-exports"
```

离线路径只使用 `Store::open_existing` 的 SQLite 只读连接，不启动 daemon、worker、capability probe、model、Evidence、Broker、migration 或 doctor repair。在线路径也不会触发外部 Provider/Broker；HTTP 只把目标目录交给 Store 的普通 executor read operation，不走会延长 live lease 的 maintenance operation。

脚本不会隐式选择 `latest`，也不会覆盖已有目录或压缩包。退出码：`0`=完整包，`2`=已生成但资料不完整，`1`=无法导出。脚本输出唯一的绝对目录和 tar.gz 路径。

## 目录内容

```text
README.md
manifest.json
SUMMARY.md
timeline.jsonl
workflow.json
tasks_attempts.json
model_routes.json
llm_calls.jsonl
llm_transcript.md
tools.jsonl
rust_decisions.jsonl
rust_decisions.md
evidence_status.json
context_coverage.json
draft_submit_coverage.json
stage_acceptance.json
decision_matrix.json
policies_and_risk.json
failures_and_missing.json
artifact_index.json
artifacts/<artifact_id>.json
EXPORT_STATUS
checksums.sha256
```

`timeline.jsonl` 使用 SQLite `event_id/cursor` 作为因果顺序，不依赖秒级时间排序。并发节点保留自己的 Run/Task/Attempt 关联；导出器不会把并发事件重写成虚假的全局串行顺序。

`llm_calls.jsonl` 每行对应一个持久化 AgentTurn，或一个只有 `agent.turn_started` 而没有 terminal 的未闭合调用。状态严格区分 `completed`、`failed`、`unknown_after_crash` 和 payload 中的 `will_retry`。未闭合的 start 不会被推断为失败，也不会自动重发。

Provider request/result、Draft memo、Submit JSON、continuation、tool outputs、usage 和 model/capability 字段来自 CAS 中当时保存的事实；没有返回的字段为 `null` 或带 `not_returned`/`not_recorded` 原因。

`tools.jsonl` 同时保留“模型请求了工具”和“Rust 执行了工具”的边界。`tool.failed` 不自动等同于 Task 失败，例如 `document_requires_range` 是模型可继续处理的工具结果，必须结合后续 cursor 判断。

`rust_decisions.jsonl` 优先展示持久化 DecisionContext、ExecutionContext、ExecutionVerdict、Evidence 和 Outcome 等真实 payload。旧记录没有 `rule_id`、函数位置或 first-zeroing checkpoint 时，字段明确为 `not_recorded`；导出器不会重新运行 DecisionGate、重新读取行情或事后编写权威解释。`reconstruction` 只表示可读投影，不是 Rust runtime trace。

`decision_matrix.json` 固定保留 4 个资产 × 3 个 horizon 的槽位。Raw forecast、Claim/Critique 引用、DecisionContext eligibility、calibration/risk 字段分别保留，不把单资产权重和 horizon 预测合成一个数；整数单位写在 `units` 中。

## 一致性与水位

Store exporter 在同一个 SQLite `Deferred` read transaction 中读取 Run、事件、Task/Attempt、artifact closure 和 payload，并记录 `snapshot_cursor`。事件水位之后的新事件不属于本包。

它不调用 `verify_integrity()`，因为失败 Run、取消 Run、半途崩溃 Run 和缺损 blob 也必须尽量导出。缺失或损坏的 blob 会进入 `failures_and_missing.json`，对应 artifact 文件只写 omission marker，不会在导出时修复 Store。

目标目录必须位于 Store Root 外，父目录会做 canonical path 检查；Bundle 内只创建普通文件和目录，拒绝 symlink。`checksums.sha256` 使用 SHA-256 内容指纹；`source_artifact_hash` 是原 CAS 对象 hash，`export_payload_hash` 是脱敏后导出 payload hash，二者不能互相冒充。

## raw model 权限

旧 `store export-run --include-raw-model` 已修正为：

1. 历史 `RunPurpose::Debug` 继续兼容；
2. `Paper`/`PositionPlan` 只有在同一隔离 Store 中存在匹配的 `DebugSessionIdentity`、Run purpose、Store identity 和 `learning_scope=isolated` 时才允许 provider detail；
3. 普通 Paper/PositionPlan 不能通过 CLI flag 提升权限；
4. 新 Debug bundle 不接受 caller 自己传“放行”开关，而是由 Store 持久化身份决定。

即使授权，默认包仍是 share-safe：递归清理 Authorization、API key、Cookie、daemon token、token-bearing URL、secret/header、opaque `encrypted_content` 和嵌套 ToolResult。被删字段保留类型、原始字节长度和安全指纹；不尝试解密 opaque continuation。没有授权的 provider request/result 为 `not_authorized`，不是“Provider 未调用”。

## 现有记录与差距

已复用：

- `AgentTurn` 的 domain request/response、Draft/Submit phase、Manifest/ReadGrant 观察、budget/capability snapshot、telemetry、continuation 和 Debug `ModelCallTrace`；
- `ToolCall`/`ToolResult` 的 call_id、参数、结果/错误和 source refs；
- Store cursor、Task/Attempt、Artifact source closure、Decision/Execution/Outcome CAS；
- DebugSession 的 isolation、runtime identity 和 purpose 绑定。

本次修正：

- raw 导出不再把 `RunPurpose` 当作 Debug isolation 的替代品；
- 在线导出不再走会 defer live lease 的 maintenance seam；
- 新增按角色、horizon、Attempt、phase、turn 的可读 transcript 和机器 JSONL；
- 导出使用单读事务水位，支持全量事件而非最近 100/500 条；
- 默认递归脱敏，并区分 source hash/export hash；
- 对 pending/failed/missing/cancelled/not-returned 状态显式标记。
- 跨 Run source refs 只在当前 DebugSession 的 `dataset`/`parent_artifacts` 显式登记时导出 payload；否则保留引用元数据并标记 `cross_run_source_not_authorized`。

仍然缺失且不会被本包补造：

- 旧 Run 未启用 Debug 时没有保存的 provider wire request/result；
- 现有旧 AgentTurn 没有 SSE 每个 envelope 的原始序号/partial stream；
- Provider 未返回的 reasoning/usage/response ID；
- 没有在当时持久化的 Rust rule_id/source location/first-zeroing checkpoint；
- capability probe 若只以 snapshot/hash 保存，不能从导出反推完整 probe response；
- 未到期的 T+1/T+3/T+5 和未执行的 Paper/Outcome/Learning 节点。

这些边界由 `manifest.json`、`failures_and_missing.json` 和 `SUMMARY.md` 直接说明。导出器不会运行 LLM 来补写它们。

## 验证分级

- `implemented`：Store exporter、权限检查、CLI/HTTP 路径、POSIX 脚本和文档已实现；
- `offline-verified`：Rust focused tests、`sh -n`、workspace check/test、隔离 Store 导出和 tar 解包检查通过；
- `real-LLM-export-verified`：必须是采集点启用后新建的隔离 Luna Debug Run，且包中实际存在对应 request/result/Tool/Decision refs；
- `real-LLM-export-verification=BLOCKED`：网关或额度阻断时使用此标签，不能用历史导出或 fixture 冒充新采集验证；
- `real-Paper-verified` 与 `outcome/learning-verified`：本包本身不授予也不声称这两个状态。
