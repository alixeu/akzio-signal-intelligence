# Akzio v2 生产化 TODO

> 当前目标：Paper-only；可执行资产仅为 TQQQ、QQQ、SOXX、SOXL。不得恢复 Live Trading、旧 orchestrator、旧 Store/Prompt 兼容层，也不得让 Agent 直接访问网络、文件系统、数据库或凭据。

## P0：生产阻断项

- [x] 修复当前工作树编译失败：补齐所有 `AgentModelTurn` 测试构造器的 `model_debug` 字段。
- [x] 重新通过 `cargo fmt --all -- --check`、`cargo check --offline --workspace`、`cargo clippy --offline --workspace --all-targets -- -D warnings` 和 `cargo test --offline --workspace`。
- [x] 确定并实现生产 Evidence 路径：通过 Responses API/模型原生联网能力完成受治理的证据检索；Rust 仍必须拥有来源 allowlist、查询/资源约束、超时与失败关闭、证据归档、provenance、`ContextManifest`/`ReadGrant` 和 canonical learning 边界。不得给 Agent 任意 HTTP、文件系统、数据库或凭据访问；`FixtureEvidenceAdapter` 仅用于 Debug/Replay。（源码与离线测试完成；真实 provider 可用性待验证。）
- [x] 为模型原生联网补齐受治理 tool contract：限定可用联网能力、记录模型请求/原始结果及引用来源，验证返回证据可规范化为 `RawEvidence`/`NormalizedEvidence`，并在能力缺失、来源越权、引用不完整或结果不可验证时 fail-closed。（源码与离线测试完成。）
- [x] 为 `alpaca`、`sec_edgar`、`fred`、`news_web` 定义 typed resource schema、参数上限、分页/时间窗规则、URI 规范化和错误分类。（源码与离线测试完成。）
- [x] 增加 Rust 校验的 `ResearchIntent` / Run 输入：明确资产范围、研究问题、预测期限、数据新鲜度和允许来源，替换泛化 Planner objective。（源码与离线测试完成。）
- [x] 组装 scheduler-owned Paper 入口：注入 Alpaca Paper session clock、PaperWorkflowSource、CommittedPaperBroker 和 scheduler-owned snapshots。（源码与离线测试完成；真实 Paper sandbox 待验证。）
- [ ] 在独立 Paper sandbox 完成端到端验证：市场时钟、账户、报价、一次性 durable commitment、client-order 幂等、broker reconciliation、freeze/unfreeze 和重启恢复。

## P1：上线前的研究与风险质量

- [x] 为标准化市场数据建立严格 Schema 与质量门：OHLCV/报价、交易时段、时区、公司行动与拆分、重复/缺失 bar、异常价格/点差和过期数据。（当前源码覆盖 OHLCV、RFC3339/工作日、重复日期、`adjustment=all`、Quote/Execution spread-age gate；真实 provider 数据分布待验证。）
- [x] 为 SEC、FRED、新闻证据建立可追溯结构：文档/系列标识、发布时间、观察时间、修订版本、原始 URI、去重键和引用片段。（源码与离线测试完成；真实来源回执待验证。）
- [x] 为 Planner 增加受约束的研究分片规则：价格/市场结构、宏观、公司基本面/半导体链条、新闻事件按条件选择。（源码与离线测试完成。）
- [x] 建立冻结证据集离线评测：覆盖 Planner、Claim、Critique、DecisionProposal，记录模型版本、Prompt/Contract hash、成本、延迟、Schema 成功率和 blocker 召回率。（`test frozen-evidence` fixture/offline 完成。）
- [ ] 用真实 Paper Outcome 完成 T+1/T+3/T+5 评估：收益、相对 QQQ、交易成本、滑点、校准度、证据完整度、风险召回率和 regime 分层。（OutcomeCostModel 与离线 materialization 已完成；真实 Paper bars/Outcome sealing 待验证。）
- [x] 实现 scheduler-owned 到期 Outcome worker：扫描 `OutcomeSchedule`，获取受治理的未来价格/风险观测，封存 Outcome，调用 EvaluationRuntime，并原子记录 Policy Evaluation。（源码与离线测试完成；真实 Paper worker 运行待验证。）
- [x] 对模型和证据供应故障建立 NoOrder / 降级规则：限流、超时、空响应、来源延迟、provider 中断和部分来源不可用都不能用猜测数据填补。（源码与离线测试完成。）

## P2：可运维性、恢复与治理

- [x] 增加 Run/Task/Attempt 的结构化指标：耗时、token、模型/工具错误、来源时效、Gate blocker、Paper commitment 和 reconciliation。（当前 Store metrics/health alerts 已覆盖可持久化指标；真实长期运行基线待验证。）
- [x] 增加告警和运行手册：模型不可用、证据源不可用、scheduler lease 抖动、快照过期、订单拒绝、对账延迟、Store Doctor 失败和长期冻结。（运行手册与 `store alerts` 已完成。）
- [x] 演练进程强杀、SQLite/CAS 损坏、网络分区、Alpaca 超时/重复回执、时钟边界和 lease takeover，验证不会重复下单或错误晋升学习状态。（强杀/Store/lease/冻结为离线 fixture；真实网络分区、Alpaca timeout/重复回执和人工强杀待验证。）
- [x] 定义 Store Root 备份、恢复、保留和密钥轮换方案；恢复后必须运行 Store Doctor、replay 和 Paper commitment 一致性核对。（SQLite/CAS backup/restore 与 Doctor/replay 边界已完成；真实密钥轮换和 Paper commitment 核对待验证。）
- [x] 增加上线审批清单：仅 Paper endpoint、四 ETF universe、真实适配器健康、sandbox 回归、operator freeze/unfreeze 演练、无 Hard Blocker、无未验证 candidate promotion。（运行手册已列出审批门；人工审批尚未完成。）

## 完成定义

- [ ] 所有 P0 完成，P1 的数据质量、离线评测、真实 Paper sandbox 和 Outcome worker 验收完成。（真实 Paper sandbox 与真实 Outcome 仍待人工验证。）
- [x] P2 的最低运行手册、告警和离线恢复演练可复现；真实人工演练完成前不得声明“生产可试运行”。
- [x] Debug、Replay、Shadow、Paper Dry Run 和 fixture 结果永远不能直接驱动 canonical learning、拓扑晋升或真实 Paper 成功声明。（源码边界、`test learning-transitions` 和 `paper-dry-run` 的 `canonical_learning_events: 0` 已验证。）
