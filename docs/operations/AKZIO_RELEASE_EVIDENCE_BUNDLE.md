# Akzio Release Evidence Bundle

状态日期：2026-08-31。

`ReleaseEvidenceBundle` 是从 `V2Store` canonical state 只读生成的版本化批准证据投影。它不是模型输出，不接受模型提供字段，也不创建并行状态文件。

## Schema

Bundle v1 包含：

- repository commit 和 dirty-worktree 标记；
- runtime config、prompt、contract、topology hashes；
- frozen workflow graph reference/hash；
- task contract、tool-set、context-manifest hashes；
- provider/model route capability snapshots；
- canonical evidence/source snapshot identities；
- broker account fingerprint，不包含 account ID 或凭据；
- Paper session slot 和 scheduler epoch；
- daemon owner/epoch；
- execution plan hash、commitment、client/broker order IDs；
- reconciliation reference 和 receipt references；
- sealed T+1/T+3/T+5 outcomes；
- learning transition 和关联 canary campaign；
- human Paper approval identity、时间和 approval hash；
- typed completeness/integrity issues、status 和 deterministic bundle hash。

序列化只使用有序集合和 canonical JSON hash。重复读取相同 Store 状态会得到相同 `bundle_hash`。

## 状态规则

`Approvable` 要求：

- canonical `Paper` run；
- clean repository state；
- runtime、workflow、contract/tool/context、provider、source、session、lease、execution、reconciliation、全部 outcome、learning 和 human approval 完整；
- config/workflow/account/daemon epoch 检查一致；
- broker evidence 标记为 real；
- human approval 为 approved。

`Incomplete` 表示缺少必要 evidence，例如 reconciliation、T+3 outcome 或 learning transition。

`NotApprovable` 表示存在硬阻断，例如：

- Debug、Replay、Shadow 或 Paper Dry Run；
- dirty worktree；
- offline fixture 或 fake broker evidence；
- config/workflow hash 漂移；
- broker account mismatch；
- stale daemon owner/epoch；
- human approval 非 approved。

fixture bundle 必须同时保留 `offline_fixture` 环境和 `fake_broker_evidence` issue，不能升级为真实 release evidence。

## CLI

查看：

```bash
cargo run -p akzio-cli -- store release-evidence <run-id>
```

导出到一个尚不存在的文件：

```bash
cargo run -p akzio-cli -- store release-evidence <run-id> --target /path/to/bundle.json
```

导出拒绝覆盖已有文件。HTTP 控制面为认证的
`GET /control/store/release-evidence/{run_id}`。

## 真实验证所需证据

最终真实 bundle 生成前必须确认：

1. release commit 与工作区 clean 状态；
2. 当前配置文件 hash 和 RuntimeManifest config hash；
3. frozen workflow graph/hash；
4. 实际 prompt、contract、tool-set、context-manifest hashes；
5. real OpenAI Responses provider/model/capability snapshot；
6. 每个 canonical source snapshot 的 artifact/blob identity；
7. Alpaca Paper account fingerprint 与批准账户一致；
8. session slot、当前 scheduler owner/epoch；
9. execution plan/commitment hash；
10. Alpaca broker/client order IDs 和真实回执；
11. complete reconciliation；
12. sealed T+1/T+3/T+5 outcomes；
13. learning transition 和 canary state；
14. 最终 human approval identity/time/hash。

离线 fixture、mock receipt 和 local server 结果不能替代以上证据。
