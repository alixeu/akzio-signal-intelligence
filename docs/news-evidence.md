# 新闻采集与来源核验

新闻和成分公司事件日历通过模型原生搜索获取，官方持仓、指数元数据和杠杆条款继续通过发行人适配器获取。两条路径由 `configured_news_evidence_transport` 按资源分发；Daemon、CLI preflight 和真实集成测试共用该入口。

采集使用 `evidence.news_web` 模型路由，没有该路由时使用默认模型。来源核验使用 `research.critic` 路由，没有该路由时使用默认模型。来源核验是独立请求，重新使用原生联网读取来源，不复用采集调用的结论作为已知事实。相同模型的两次调用仍可能共享错误，因此状态称为 `model_reviewed`，不称为人工确认或真值。

原生工具请求使用 `web_search`、`filters.allowed_domains` 和原生来源输出。支持完成的 search/open_page/find_in_page 调用；query 和 action.sources 不再被视为总会存在的字段，URL annotations 也可以建立来源关联。只有模型文字声称“已搜索”不能通过。域名匹配允许受限域的子域，不允许后缀伪装。

新建 EvidenceGate 任务的有限执行时限为 180 秒，覆盖采集和核验两次 hosted model 调用。真实隔离运行中原来的 30 秒时限会在核验完成前取消任务。此修改不改写旧 Run 已冻结的预算，也不改变 ExecutionGate 时限。

## 证据与状态

- 采集策略版本 4 引入 `model_reviewed`，新 Run 的新闻与事件日历使用此策略；直接数据保持 `verified_source`。旧 discovery/独立抓取模式仍可解析，历史 BLOB 不改写。旧 Run 的策略身份与新策略不符时仍会被拒绝，不自动升级历史证据。
- 新路径不执行 Rust 独立网页抓取，也不要求模型摘录逐字存在于 HTML 字节。
- 核验逐来源记录 supported、contradicted、unverifiable 或 irrelevant，以及具体原因。只有 supported 来源可贡献 `reviewed_facts`；遗漏、重复、未在核验调用出现的来源、窗口外事件均拒绝。
- 每条事实包含 URL、事实陈述、事件日期和日期依据。发布时间无法确定时保留 null，整包以实际读取时间界定可用性，不猜测精确时间。事件日历可描述未来已公布的计划，近期新闻不能使用窗口外事件。
- `citations_complete=true` 仅表示被接纳事实的引用闭合；`completeness_ppm` 表示选中来源中获得支持的比例，不是新闻覆盖率或语义正确率。原始发现的其他来源数单独保留。
- 核验原始请求/响应与采集原始响应进入 RawEvidence；研究 Context 只接收精简事实和核验结果。原始 URL 引用字节绑定仍由 Rust 检查。模型请求指令和协议字段不作为新闻正文扫描，暴露给研究的事实仍通过金融内容检查。
- `human_review=not_performed`、`investment_inference=not_verified` 明确保留。当前实现选择独立 LLM 核验方案，没有创建人工审批流程，也没有将新闻核验转成 Paper 授权。

## 测试

普通回归测试不联网：

```sh
cargo test --locked -p akzio-model -p akzio-ingest --lib
cargo test --workspace --locked
```

真实测试显式 opt-in，读取实际部署文件，复用 CLI/Daemon 的模型环境变量与路由解析。输出目录必须是新的工作区内目录：

```sh
AKZIO_REAL_LLM_CONFIG="$HOME/.akzio/config.toml" \
AKZIO_REAL_LLM_OUTPUT="$PWD/.akzio/real-news-run-01" \
cargo test --locked -p akzio-cli real_news_uses_deployment_routes \
  -- --ignored --nocapture
```

测试断言：实际新闻路由能获取资料；来源核验产生可用事实；核验模型匹配配置；无独立 HTTP fetch；取得的证据通过正式时间/引用验证。没有可用事实时测试失败，保存来源拒绝原因，不能把 HTTP 成功替代业务通过。它不测试预测收益、Paper 成交或完整研究结论。

输出含脱敏配置身份、原始模型响应、精简事实、质量与时间校验产物。不得提交这些输出或配置凭据。`base_url` 必须是请求地址，`api_key` 必须是密钥。测试保留显式的 `AKZIO_REAL_LLM_SWAP_ENDPOINT_FIELDS=1` 诊断选项，只用于已确认填反字段的临时内存修正；正常部署不应设置它。

协议依据：[OpenAI Web search](https://developers.openai.com/api/docs/guides/tools-web-search)。来源存在、事件陈述准确、投资方向正确、仓位合理及执行授权始终是不同的判断。
