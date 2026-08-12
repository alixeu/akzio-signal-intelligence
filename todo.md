# Akzio v2 生产化 TODO

当前目标：Paper-only；可执行资产仅为 `TQQQ`、`QQQ`、`SOXX`、`SOXL`。

状态判断以当前 Rust 源码、Cargo 配置、测试和 `V2Store` 持久化事实为准。

## P0：生产阻断项

- [x] `AgentModelTurn` 测试构造器已修复；fmt、check、clippy 和 workspace tests 已通过。
- [x] Rust-owned Evidence 路径、allowlist、超时、失败关闭、provenance、`ContextManifest`/`ReadGrant` 已落地；真实 provider 待验证。
- [x] `alpaca`、`sec_edgar`、`fred`、`news_web` typed resource schema 与错误分类已落地；真实来源回执待验证。
- [x] `ResearchIntent` / Run 输入已由 Rust 校验，资产范围固定为四 ETF。
- [x] scheduler-owned Paper 入口、Paper clock、`PaperWorkflowSource`、`CommittedPaperBroker`、原子 session provisioning 和跨 Run evidence 拒绝已落地；真实 sandbox 待验证。
- [x] Outcome worker 的 `outcome.need`、evidence 和 canonical evaluation 均在 Store 事务内重新验证 outcome-worker lease；旧 lease 不得写 learning。
- [ ] 独立 Paper sandbox 端到端验证：clock、account、quotes、durable commitment、client-order 幂等、reconciliation、freeze/unfreeze、重启恢复。

## P1：研究与风险质量

- [x] 市场数据 Schema/质量门：OHLCV、报价、交易时段、时区、公司行动、重复/缺失 bar、异常价格/点差和过期数据；真实 provider 分布待验证。
- [x] SEC、FRED、新闻证据追溯字段：标识、发布时间、观察时间、修订版本、原始 URI、去重键和引用片段；真实来源回执待验证。
- [x] Planner 研究分片：价格/市场结构、宏观、基本面/半导体链条、新闻事件按条件选择。
- [x] 冻结证据集离线评测覆盖 Planner、Claim、Critique、DecisionProposal，并记录模型版本、Prompt/Contract hash、成本、延迟、Schema 成功率和 blocker 召回率。
- [ ] 真实 Paper T+1/T+3/T+5 Outcome：收益、相对 QQQ、成本、滑点、校准度、证据完整度、风险召回率和 regime 分层。
- [x] scheduler-owned Outcome worker、受治理未来观测、Outcome sealing、`EvaluationRuntime` 和原子 Policy Evaluation 已落地；真实 Paper worker 待验证。
- [x] NoOrder/降级规则已落地：限流、超时、空响应、来源延迟、provider 中断和部分来源不可用不得用猜测数据填补。
- [x] 补充专门的 scheduler snapshot 跨 Run 回归证据。
- [x] 补充同一 `OutcomeSchedule`/permit 重复调度不得生成第二个 worker 的回归证据。

## P2：运维、恢复与治理

- [x] Run/Task/Attempt 指标、耗时、token、模型/工具错误、来源时效、Gate blocker、Paper commitment 和 reconciliation 已落地；长期基线待验证。
- [x] 告警和运行手册已覆盖模型/证据源不可用、lease 抖动、快照过期、订单拒绝、对账延迟、Doctor 失败和长期冻结。
- [x] 离线等价演练已通过：crash recovery、SQLite/CAS 损坏、lease takeover、freeze/unfreeze、backup/restore、replay 和 Doctor。
- [ ] 人工故障演练：进程强杀、网络分区、Alpaca timeout/重复回执、时钟边界、真实 lease takeover 和 Paper commitment 一致性核对。
- [x] Store Root 备份/恢复/保留边界已定义；离线 backup/restore、Doctor/replay 已通过，真实密钥轮换待验证。
- [ ] 人工完成上线审批：仅 Paper endpoint、四 ETF、真实适配器健康、sandbox 回归、freeze/unfreeze 演练、无 Hard Blocker、无未验证 candidate promotion。

## 完成定义

- [ ] 所有 P0 完成，且真实 Paper sandbox 与真实 T+1/T+3/T+5 Outcome worker 验收完成。
- [ ] P2 人工演练和上线审批完成前，不得宣布 Paper 生产试运行或生产就绪。
- [x] Debug、Replay、Shadow、Paper Dry Run 和 fixture 结果不得直接驱动 canonical learning、拓扑晋升或真实 Paper 成功声明。
