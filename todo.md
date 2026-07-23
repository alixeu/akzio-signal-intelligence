# Agent 监控网站 TODO

> 状态：仅规划，暂不执行。
>
> 目标：为 Rust 股票多 Agent 投资研究系统增加 `-web` / `--web` 监控入口，构建“蒸汽机械铁路王国”风格的实时 3D 工作流看板，并将监控事件持久化到 SQLite 供实时展示和历史回放。

## 已确认的实现原则

- [ ] `orchestrator-exec` 支持 `-web`、标准参数 `--web` 和短参数 `-w`。
- [ ] Web 默认绑定 `127.0.0.1:8787`，启动后尝试打开浏览器。
- [ ] 工作流完成或失败后保留网站，直到用户按 `Ctrl+C`。
- [ ] 监控信息写入独立 SQLite 事件表，不写入投资反思使用的 `memory_items` / `memory_versions`。
- [ ] 监控记录默认随所有工作流运行产生；`--web` 只负责启动网站。
- [ ] 网站只读，不能从前端改变 Agent 决策或工作流状态。
- [ ] 前端资源预编译并嵌入 Rust 二进制，运行时不依赖 Node。
- [ ] 第一版仅本机访问，不加入公网部署、用户系统或远程控制。

## 1. CLI 与 Web 服务

- [ ] 增加 `--web`、`-w`，并兼容字面参数 `-web`。
- [ ] 增加 `--web-port <PORT>`，默认 `8787`。
- [ ] 在工作流开始前检查端口；端口不可用时直接报错，避免运行开始后看不到监控。
- [ ] 使用 Axum 提供静态前端、JSON API 和 SSE 实时事件流。
- [ ] 浏览器打开失败时只打印 URL，不中断工作流。
- [ ] 为工作流增加统一失败边界：写入失败状态和错误摘要后，Web 服务继续运行。
- [ ] 更新 README 中的启动、前端开发、端口和退出方式。

## 2. SQLite 监控事件

- [ ] 将数据库 schema 升级到 v4，并保留现有迁移备份与事务约束。
- [ ] 新增追加式 `monitor_events` 表，至少保存：
  - `id`
  - `run_id`
  - `occurred_at_ms`
  - `event_kind`
  - `phase`
  - `role`
  - `status`
  - `payload_json`
- [ ] 为 run/event 顺序和 run/role/时间建立索引，run 删除时级联清理事件。
- [ ] 事件载荷统一记录：ticker 列表、任务、topic、round、LLM/Tool/总耗时、重试数、错误摘要、Artifact 状态、输入/输出证据数。
- [ ] 未知数据保存为 `null`，不能用 `0` 或模拟值冒充真实指标。
- [ ] 不把 Prompt、推理文本、密钥、完整 Tool 参数或完整 Tool 输出写入监控事件。
- [ ] 错误摘要限制长度并清理可能出现的凭据。
- [ ] 监控写入失败只让监控进入 degraded，不阻断投资工作流。

### 事件类型

- [ ] `run.started/completed/failed`
- [ ] `phase.started/completed/degraded/skipped`
- [ ] `source.started/loaded/degraded/failed`
- [ ] `agent.started/retry/completed/degraded/failed`
- [ ] `llm.completed`
- [ ] `tool.started/completed/failed`
- [ ] `artifact.persisted/rejected`
- [ ] `dependency.blocked/released`

## 3. 工作流埋点

- [ ] 在 Yahoo、Jin10 和 SQLite 预检/导入边界记录数据源事件。
- [ ] 在每个 Phase 开始、完成、跳过和降级处记录事件。
- [ ] 在每个 Role job 的尝试、重试、超时、失败和完成处记录事件。
- [ ] 扩展现有 Agent event sink，记录 LLM iteration 耗时和 Tool 起止事件；不保存流式文本 delta。
- [ ] 通过 `call_id` 配对 Tool 起止事件，得到真实调用耗时。
- [ ] 为 `RoleJobResult` 增加真实 attempt/retry 计数和安全错误摘要。
- [ ] Artifact 只有通过校验并成功持久化后才显示 usable/completed。
- [ ] 从已知的 evidence/context 字段计算并去重证据数量；不能可靠识别时显示未知。
- [ ] 对依赖等待、PhaseSummary 压缩、桥梁解锁条件和异步完成通知记录 dependency 事件。

## 4. 只读监控 API

- [ ] `GET /healthz`
- [ ] `GET /api/runs`
- [ ] `GET /api/runs/{run_id}/events?after=<event_id>`
- [ ] `GET /api/runs/{run_id}/details`
- [ ] `GET /api/runs/{run_id}/stream?after=<event_id>`
- [ ] SSE 每约 250ms 检查新增事件，并发送 15 秒 keep-alive。
- [ ] 支持 `Last-Event-ID` 和 `after`，确保断线重连不遗漏或重复应用事件。
- [ ] 旧 run 没有监控事件时，从 `runs` 和 `role_turn_summaries` 提供粗粒度终态，不伪造历史动画。

## 5. 前端技术与构建

- [ ] 使用 React 19、TypeScript、Vite、Three.js、React Three Fiber 9 和 Drei。
- [ ] 使用 `motion` 处理面板、进入退出和布局动画。
- [ ] 使用 `zustand` 管理 live/replay 共享状态。
- [ ] 使用 `@number-flow/react` 展示不断变化的数字。
- [ ] 使用 `liveline` 展示 LLM 与 Tool 延迟流。
- [ ] 简单 hover、颜色和透明度使用 CSS，不额外引入动画库。
- [ ] 用程序化低多边形几何体制作建筑、列车、齿轮、蒸汽和 Agent，不依赖外部 3D 素材。
- [ ] Vite 输出固定名称的 `index.html`、`monitor.js`、`monitor.css`，由 Rust 嵌入二进制。
- [ ] 锁定依赖并检查前端构建产物没有漂移。

## 6. 3D 城市与建筑映射

- [ ] 数据装卸港：Yahoo 技术数据、Jin10 新闻和 SQLite 中央仓库。
- [ ] 技术分析工厂：`analyst.technical`，展示趋势、支撑、阻力和技术置信度。
- [ ] 新闻宏观研究院：`analyst.news_macro`，展示电报纸带、新闻分类和事件警钟。
- [ ] 市场情绪广播塔：第一版显示 `unconfigured/degraded`，不得伪造 Reddit/X/YouTube 数据。
- [ ] 多空辩论大厅：Bull、Bear 和 Topic Controller，展示轮次、证据砝码和机械天平。
- [ ] 证据压缩机：PhaseSummary 与 reducer，展示去重、冲突分流和低质量证据淘汰。
- [ ] 概率计算塔：Phase 3 research manager，展示多头、空头和 Hold/多空平衡度。
- [ ] 风险闸门：Trader、Risk Committee 和 Allocator，展示仓位、波动率、回撤与闸门状态。
- [ ] 中央决策塔：Portfolio Manager、预测归档与最终 Buy/Hold/Sell。

## 7. 铁路与机关

- [ ] 使用带高度和连接方向的网格铁路模型。
- [ ] BFS 只允许曼哈顿相邻、接口相连且高度差不超过 1 的格子。
- [ ] 高度差为 1 时自动渲染机械台阶或短轨坡道。
- [ ] 平台只绕 Y 轴按 90° 旋转；旋转后更新连接并重新寻路。
- [ ] ticker 使用固定配色和独立货箱，切换 ticker 只改变筛选和高亮。
- [ ] 错位桥只有在真实 Artifact/依赖满足后才对齐并允许列车通过。
- [ ] 升降机由对应 dependency 完成事件触发。
- [ ] 活动桥状态：completed 全展开、failed 停在半空、degraded 使用木板、blocked 完全关闭。
- [ ] 用户可以旋转/缩放镜头并选择建筑，但机关由后端真实状态自动驱动。

## 8. 状态视觉与右侧面板

- [ ] running：黄铜齿轮、蒸汽、移动 Agent 和流动货箱。
- [ ] completed：金绿灯、铁路通行；关键流程全部完成后启动塔灯和钟楼。
- [ ] failed：红灯、警钟、断轨或半展开桥梁。
- [ ] degraded：暗灯和临时木板结构，但允许非关键流程继续。
- [ ] blocked：闸门关闭并明确显示等待的依赖。
- [ ] skipped/derived：使用透明旁路并说明为何未执行 LLM 阶段。
- [ ] 右侧面板显示 run、ticker、Agent、任务、Phase、状态、LLM/Tool/总耗时、重试、错误、Artifact、证据输入输出和失败列表。
- [ ] 建筑点击后展示详情抽屉；核心视图不能退化成纯表格。
- [ ] 缺失的目标区间、风险等级或置信度显示 `—`，不得从其他字段臆造。

## 9. 历史回放与性能

- [ ] 同一个事件 reducer 同时驱动实时模式和历史回放。
- [ ] 提供时间轴、暂停、拖动和 0.5×/1×/2×/4×。
- [ ] 拖动时先重建目标时刻状态，再从下一事件继续动画。
- [ ] 限制设备像素比并使用 instanced meshes。
- [ ] 页面隐藏时暂停 3D 渲染和实时图表动画。
- [ ] 支持 `prefers-reduced-motion`，停止持续列车、蒸汽和钟摆循环。
- [ ] WebGL 不可用时提供只读二维流程降级视图。
- [ ] 目标：常见笔记本在 1440×900 下流畅运行。

## 10. 当前没有、建议以后补齐

- [ ] Reddit/X/YouTube 的真实采集器、存储结构和情绪 Agent。
- [ ] 情绪源的授权、限流、内容合规和缺失数据策略。
- [ ] 真正的上涨/下跌/震荡三分类概率契约，以及 Prompt、校验、预测和回测迁移。
- [ ] 当前先保留二分类契约；第三个仪表使用 `1 - |long_probability - short_probability|`，标注为 UI 派生的 Hold/多空平衡度，不称为模型概率。
- [ ] 局域网/公网访问所需的鉴权、TLS、访问令牌、审计和部署配置。
- [ ] 前端控制工作流、取消 Agent、重新执行 Phase 等写操作；在有明确权限模型前不实现。
- [ ] 外部 3D 模型、品牌素材和更复杂的可玩沙盘机制；程序化模型不足时再评估。

## 11. 测试与验收

- [ ] Rust：v3→v4 迁移、幂等性、外键、索引和旧数据库备份。
- [ ] Rust：事件顺序、敏感数据过滤、重试、Tool 耗时、失败 run 和 degraded 监控。
- [ ] Rust：CLI 参数、端口冲突、REST API、SSE 续传和旧 run 降级读取。
- [ ] 前端：BFS、高度限制、坡道、旋转连接、无路线和事件 reducer。
- [ ] 前端：实时/回放一致性、SSE 重连、状态视觉和 Hold 指标。
- [ ] 视觉：running、blocked、degraded、failed、completed、窄屏、减少动态效果和 WebGL fallback。
- [ ] 端到端：`orchestrator-exec --mock --web --to-phase 8` 能实时走完整流程，刷新后仍可回放。
- [ ] 执行 `npm test`、`npm run build`、TypeScript 检查和嵌入产物漂移检查。
- [ ] 执行 `rtk cargo fmt --all`、`rtk cargo test`、`rtk cargo clippy --workspace --all-targets`。

## 12. 前端详细数据字段清单

> 本节合并前端详细 TODO。所有字段必须来自 SQLite、运行事件或已校验
> Artifact；当前后端没有的数据在接口中返回 `null`，不得由前端猜测。

### 12.1 全局运行信息

- [ ] 展示 `run_id` 和任务名称。
- [ ] 展示本次分析的 `tickers`。
- [ ] 展示单 ticker / 多 ticker 运行模式。
- [ ] 展示开始时间、结束时间和总运行时长。
- [ ] 展示当前 Phase、总体进度和运行状态。
- [ ] 支持 `pending/running/completed/failed/degraded/cancelled` 状态；`cancelled` 在后端真正支持取消前仅保留类型。
- [ ] 展示关键、非关键、完成、失败和降级 Agent 数量。
- [ ] 展示累计 LLM/Tool 调用次数。
- [ ] 展示输入、输出、缓存、reasoning 和总 Token。
- [ ] 展示累计成本；没有模型价格配置时返回 `null`。
- [ ] 展示全局安全错误摘要。
- [ ] 展示当前活动 Agent 和当前 ticker。

计划中的前端类型：

```ts
type WorkflowStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "degraded"
  | "cancelled"

interface WorkflowRun {
  runId: string
  name: string
  tickers: string[]
  mode: "single" | "multi"
  status: WorkflowStatus
  currentPhase?: number
  progress: number

  startedAt?: string
  finishedAt?: string
  elapsedMs: number

  totalAgents: number
  criticalAgents: number
  completedAgents: number
  failedAgents: number
  degradedAgents: number

  llmCalls: number
  toolCalls: number
  inputTokens: number
  outputTokens: number
  cachedTokens: number
  reasoningTokens: number
  totalTokens: number
  estimatedCost?: number

  currentAgent?: string
  currentTicker?: string
  errorSummary?: string
}
```

### 12.2 数据源通用字段

- [ ] 数据源 ID、名称、类型和图标。
- [ ] 状态、是否关键、当前 ticker。
- [ ] 请求开始/结束时间、耗时和请求次数。
- [ ] 成功、失败、重试次数。
- [ ] 最后成功/错误时间和安全错误摘要。
- [ ] 获取、有效、丢弃和去重后的数据条数。
- [ ] 数据时间范围、新鲜度、过期状态、完整度和质量评分。
- [ ] 缓存命中和 degraded 状态/原因。

#### Yahoo Finance

- [ ] ticker 和日线、3 小时、20 分钟等 interval。
- [ ] K 线数量、数据起止时间和最新价格。
- [ ] 涨跌幅、成交量和 OHLCV 完整度。
- [ ] 技术指标是否预计算、缺失指标数和重算状态。
- [ ] 指标计算耗时。

#### Jin10

- [ ] 新闻总数、ticker 相关新闻数和宏观新闻数。
- [ ] 高影响事件数和新闻时间范围。
- [ ] 去重前后数量和无效新闻数。
- [ ] 进入 SQLite 的 attention-scored 新闻数量。

#### SQLite

- [ ] 连接、WAL 和 schema 版本状态。
- [ ] 当前数据库、WAL 文件大小。
- [ ] 本次读写条数和最近写入时间。
- [ ] 查询、写入和锁等待耗时。
- [ ] 数据库安全错误摘要。

#### Reddit / X / YouTube（future-only）

- [ ] 未实现采集前统一展示 `unconfigured/degraded`。
- [ ] 实现后展示抓取、有效和相关内容数量。
- [ ] 展示正面、负面、中性数量和情绪分数。
- [ ] 展示热度、参与度、讨论量变化和数据新鲜度。
- [ ] 展示不可用或降级原因。

### 12.3 Agent 通用字段

- [ ] Agent ID、名称、角色、权重和是否关键。
- [ ] 状态、ticker、任务、步骤和执行进度。
- [ ] 开始、结束、总耗时、LLM、Tool 和等待耗时。
- [ ] `loop_index`、最大循环数、LLM/Tool 调用数。
- [ ] 输入、输出、缓存、reasoning 和总 Token。
- [ ] 模型、Provider、Responses/Chat Completions 路由和流式状态。
- [ ] 当前输出安全摘要；不向监控 API 暴露 reasoning 或完整 Prompt。
- [ ] 最终 Artifact 类型、校验状态和修复次数。
- [ ] 错误、重试和降级原因。

### 12.4 Phase 1 — 多源研究

#### 技术分析 Agent

- [ ] ticker、当前 interval 和已加载 K 线/指标数量。
- [ ] 缺失指标及是否调用重算工具。
- [ ] `bullish/bearish/neutral` 趋势和趋势强度。
- [ ] 技术置信度、当前价格、支撑位和阻力位。
- [ ] RSI、MACD、均线、成交量和波动率状态。
- [ ] 技术、看涨、看跌和冲突证据数量。
- [ ] 技术结论摘要。

#### 新闻宏观 Agent

- [ ] ticker、已分析新闻和宏观事件数量。
- [ ] 高影响、利多、利空和中性新闻数量。
- [ ] 新闻情绪、宏观方向、宏观风险和事件影响等级。
- [ ] 新闻时间范围、来源数和去重后证据数。
- [ ] 核心利多/利空事件、结论摘要和置信度。

#### 社交/视频 Agent（future-only）

- [ ] Reddit、X、YouTube role 未注册前不得出现在已完成 Agent 统计中。
- [ ] 实现后展示内容来源、分析/有效内容数和情绪分布。
- [ ] 展示热度趋势、讨论变化、关键意见领袖和噪声数量。
- [ ] 展示情绪方向、置信度、核心观点和降级原因。

### 12.5 Phase 1 汇总与 PhaseSummary 压缩

- [ ] 展示输入、成功、缺失 Agent 数量。
- [ ] 展示缺失关键角色和降级非关键角色数量。
- [ ] 展示 `actionable/partial/insufficient` 证据质量。
- [ ] 展示是否允许继续和阻塞原因。
- [ ] 展示可执行/不可执行 ticker 数量。
- [ ] 展示跨 ticker 备注和延迟证据数量。
- [ ] 每个 ticker 展示可用、看涨、看跌、中性证据数。
- [ ] 分开展示技术、新闻和已配置情绪证据数。
- [ ] 展示重复信号、冲突证据、跨 Agent 冲突和决策关键点。
- [ ] 展示可用来源角色、缺失关键角色和质量原因。
- [ ] 展示 PhaseSummary 压缩输入、去重、保留和丢弃数量。

### 12.6 Phase 2 — Topic Generator 与多空辩论

#### 辩论总览

- [ ] ticker、辩论状态、当前/总轮次和当前辩题。
- [ ] 已完成/剩余辩题数量。
- [ ] Bull/Bear 分数和当前领先方。
- [ ] 未解决冲突、开始时间、总耗时和当前发言 Agent。
- [ ] Topic Controller 当前状态。

#### Topic Generator

- [ ] 生成、过滤和重复辩题数量。
- [ ] 辩题列表、优先级、来源证据和关联 ticker。
- [ ] 每个辩题的关联证据数和生成耗时。
- [ ] 显示 Topic Generator turn 与分叉关系。

#### Bull Researcher

- [ ] 当前辩题、观点、引用证据 ID 和看涨证据数。
- [ ] 上涨催化剂、潜在上涨空间和观点置信度。
- [ ] 反驳次数、未回答问题、得分和当前发言摘要。
- [ ] 显示从 Bull warm-up turn 分叉的关系。

#### Bear Researcher

- [ ] 当前辩题、观点、引用证据 ID 和看跌证据数。
- [ ] 下跌风险、潜在下跌空间和观点置信度。
- [ ] 反驳次数、未回答问题、得分和当前发言摘要。
- [ ] 显示从 Bear warm-up turn 分叉的关系。

#### Topic Controller

- [ ] 当前辩题、轮次和 Bull/Bear 评分。
- [ ] 证据相关性、可信度和逻辑完整性评分。
- [ ] 是否要求证据、进入下一轮或结束辩题。
- [ ] 裁判结论、胜出方、原因和未解决问题。
- [ ] 显示从 Topic Generator turn 分叉的关系。

#### Rust Debate Reducer

- [ ] 明确标记为 Rust-owned，不显示为 LLM Agent。
- [ ] 输入、重复、压缩和保留观点数。
- [ ] 已解决/未解决冲突数量。
- [ ] Bull/Bear 最强证据、最终倾向、置信度和摘要。

### 12.7 Phase 3 — Research Manager 概率决策

- [ ] 每个 ticker 展示校准前/后的上涨与下跌概率。
- [ ] 展示概率调整幅度、角色权重和历史校准影响。
- [ ] 展示数据完整度修正、模型置信依据、校准方法和原因。
- [ ] 展示概率是否归一化、异常检查和 Artifact 校验。
- [ ] 使用双方向概率仪表和校准前后对比。
- [ ] 第三个仪表只显示 UI 派生的 Hold/多空平衡度。
- [ ] 不显示不存在的独立“震荡概率”字段。
- [ ] 展示低置信度和证据不足警告。

### 12.8 Phase 4 — Trader 转换

- [ ] 展示 research rating 到交易动作的转换。
- [ ] 展示 ticker、动作、执行状态、入场条件和持有周期。
- [ ] 展示止损、止盈、关键风险和执行摘要。
- [ ] 若工作流策略使用 Rust derived 路径，明确显示 `derived` 和来源。
- [ ] 不允许 Trader 覆盖 Phase 3 的权威概率和市场观点。

### 12.9 Phase 5 — 三个风险评审 Agent

- [ ] 分开展示 aggressive、neutral、conservative Reviewer。
- [ ] 展示 ticker、风险状态和 low/medium/high/critical 等级。
- [ ] 展示当前价格、波动率、ATR、最大回撤和预期下行。
- [ ] 展示止损、止盈、风险收益比、建议/最大仓位。
- [ ] 展示组合暴露、行业/ticker 相关性和流动性风险。
- [ ] 展示事件风险、数据风险和模型不确定性。
- [ ] 展示是否通过风险门禁、阻塞原因、调整建议和摘要。
- [ ] 风险门动画支持 `checking/passed/warning/blocked`。
- [ ] `manual_review` 只在后端真正支持人工确认后启用。

### 12.10 Phase 6 — Portfolio Manager 最终决策

- [ ] 每个 ticker 展示 `strong_buy/buy/hold/reduce/sell/avoid` 或后端实际动作。
- [ ] 展示最终置信依据、上涨/下跌概率和风险等级。
- [ ] 展示建议仓位、入场区间、目标、止损和持有周期。
- [ ] 展示投资逻辑、上涨催化剂、下跌风险和关键证据。
- [ ] 展示未解决冲突、数据完整度和可执行状态/原因。
- [ ] 展示生成时间、Artifact ID 和 schema 校验结果。
- [ ] 关键 Agent 完成、风险通过、Artifact 有效后点亮塔楼。
- [ ] 非关键降级显示橙灯，关键失败时塔楼保持熄灭。
- [ ] 点击塔楼打开已持久化的完整投资报告。

### 12.11 Phase 7 — Rust Allocation

- [ ] 明确标记为 Rust-owned，不显示虚构的 LLM 请求。
- [ ] 展示 investable assets、VIX regime 和总股票暴露。
- [ ] 展示每个资产和现金/对冲权重。
- [ ] 展示资产上限、现金约束和权重合计校验。
- [ ] 展示 allocation method、约束修正和不可投资 ticker 排除原因。

### 12.12 Phase 8 — Outcome、Reflection 与 Archive

- [ ] 明确标记为 Rust-owned、post-run 阶段。
- [ ] 展示预测归档成功/失败和写入 ticker 数量。
- [ ] 展示 matured prediction 评分数量。
- [ ] 展示 candidate experience 蒸馏和合格数量。
- [ ] 展示 memory promotion 数量、幂等状态和失败摘要。
- [ ] mock run 明确显示 reflection skipped，不得生成学习记忆。
- [ ] reflection 失败显示 non-blocking degraded，不回退已完成投资决策。

## 13. Tool、LLM 与 Artifact 详细监控

### 13.1 Tool Call

- [ ] Tool Call ID、名称、Agent、Phase 和 ticker。
- [ ] `queued/running/completed/failed/timeout/cancelled` 状态。
- [ ] 只展示脱敏后的参数摘要。
- [ ] 开始、结束、耗时、阻塞和重试信息。
- [ ] 返回摘要、数据量、错误码和安全错误信息。
- [ ] 完整 Tool 输出仍只保留在既有受控存储，不复制到监控事件。

### 13.2 LLM Request

- [ ] Request ID、Turn ID、Agent、ticker 和 Phase。
- [ ] 模型、Provider、Responses/Chat Completions 路由和流式状态。
- [ ] 请求开始、首 Token、完成和总耗时；首 Token 未埋点前返回 `null`。
- [ ] 输入、输出、缓存、reasoning Token。
- [ ] Tool Call 状态和数量。
- [ ] finish reason、HTTP 状态、SSE 解析状态和 malformed event 数。
- [ ] 错误摘要、重试和估算成本。

### 13.3 Artifact

- [ ] Artifact ID、类型、生成方、Phase 和 ticker。
- [ ] schema/version、校验状态、错误数和修复次数。
- [ ] `generating/validating/repairing/valid/invalid/discarded` 状态。
- [ ] 创建/更新时间、输入 Artifact 和下游使用方。
- [ ] 内容安全摘要和是否最终 Artifact。
- [ ] Artifact 链路使用 ID/引用展示，不复制大体积正文。

## 14. 错误、重试与降级详细字段

- [ ] 错误时间、Phase、Agent、ticker、类型和等级。
- [ ] 安全错误消息、是否可重试、当前/最大重试。
- [ ] 是否恢复、恢复时间、是否降级和降级策略。
- [ ] 原始错误只允许在本机 debug 产物查看，不通过监控 API 返回。
- [ ] 分类展示数据源不可用、LLM 失败、流解析失败和 Tool 超时。
- [ ] 分类展示 Artifact 无效、数据缺失、数据库错误和最大循环耗尽。
- [ ] 分类展示风险门失败和最终结果不可执行。
- [ ] 错误恢复后保留原事件，不覆盖历史。

## 15. 3D 机关所需前端数据模型

### 15.1 节点

- [ ] node ID、类型、名称和 Phase。
- [ ] 绑定 Agent/ticker、`x/y/z`、Y 轴角度和高度。
- [ ] 状态、进度、可通行/解锁/关键标记和错误摘要。

### 15.2 路径

- [ ] edge ID、起点、终点和状态。
- [ ] 锁定/解锁条件、错位连接和当前对齐状态。
- [ ] 是否允许 BFS、路径长度和动画进度。

### 15.3 Agent 小人

- [ ] Agent ID、当前/目标节点和当前路径。
- [ ] 移动状态、速度和当前动画。
- [ ] 是否站在平台、等待机关或携带 Artifact。
- [ ] 携带的 Artifact 类型和目标建筑。

### 15.4 旋转机关

- [ ] 机关 ID、当前/目标角度和旋转状态。
- [ ] 可用角度、触发条件和旋转后的连接关系。
- [ ] 是否需要重新执行 BFS。

### 15.5 升降平台

- [ ] 平台 ID、当前/目标/最低/最高高度。
- [ ] 是否载有 Agent、状态、触发 dependency ID。
- [ ] 移动状态和进度。

计划中的统一场景类型：

```ts
interface WorkflowNode {
  id: string
  kind: string
  name: string
  phase?: number
  agentId?: string
  tickers: string[]
  position: [number, number, number]
  rotationY: 0 | 90 | 180 | 270
  height: number
  status: WorkflowStatus | "blocked" | "skipped" | "derived"
  progress: number
  passable: boolean
  unlocked: boolean
  critical: boolean
  errorSummary?: string
}

interface WorkflowEdge {
  id: string
  from: string
  to: string
  status: string
  locked: boolean
  unlockCondition?: string
  perspectiveBridge: boolean
  aligned: boolean
  bfsPassable: boolean
  length: number
  animationProgress: number
}
```

## 16. 页面区域详细 TODO

### 16.1 主工作流场景

- [ ] 实现悬浮机械城市、建筑节点和铁路。
- [ ] 实现 Agent 移动、BFS 高亮和状态颜色。
- [ ] 实现旋转、错位桥、升降台和活动桥。
- [ ] 实现最终塔楼点亮和 Agent 携带 Artifact。
- [ ] 支持缩放、旋转、重置视角和点击详情。
- [ ] 支持多 Agent 并行和多 ticker 货箱。

### 16.2 右侧详情面板

- [ ] 当前 Agent、ticker、任务和输出摘要。
- [ ] LLM 指标、Tool 列表和 Artifact 链路。
- [ ] 错误、重试、输入/输出证据。
- [ ] 数据源和 SQLite 状态。

### 16.3 底部时间线

- [ ] 展示 Agent、Phase、Tool、LLM、错误、重试和 Artifact 事件。
- [ ] 支持暂停实时滚动。
- [ ] 支持 Agent、ticker、Phase 和事件类型过滤。
- [ ] 支持历史 run 回放和速度控制。

### 16.4 顶部总览

- [ ] Run 状态、进度、Phase、Agent 和 ticker。
- [ ] 总耗时、Token、Tool 数、错误和降级数量。

## 17. SSE 统一事件契约

- [ ] 只使用 SSE，不同时维护 WebSocket。
- [ ] 定义 `workflow.started/completed/failed`。
- [ ] 定义 `phase.started/completed/skipped/degraded`。
- [ ] 定义 `agent.queued/started/progress/completed/failed/degraded/retry`。
- [ ] 定义 `tool.started/completed/failed`。
- [ ] 定义 `llm.started/completed/failed`；不持久化 token/delta 文本。
- [ ] 定义 `artifact.created/validating/repairing/valid/invalid`。
- [ ] 定义 `debate.round.started/argument.created/round.completed`。
- [ ] 定义 `risk.blocked/passed` 和 `decision.created`。
- [ ] 定义 node/edge unlock、mechanism rotate 和 platform move。

```ts
interface WorkflowEvent<T = unknown> {
  eventId: number
  eventType: string
  runId: string
  phase?: number
  agentId?: string
  ticker?: string
  timestamp: string
  sequence: number
  payload: T
}
```

- [ ] `eventId/sequence` 与 SQLite 自增 ID 保持一致。
- [ ] 客户端按 `(runId, sequence)` 幂等应用事件。
- [ ] reconnect 从最后确认的 sequence 继续。
- [ ] 事件乱序、重复或间断时显示连接 degraded，并通过 REST 补齐。

## 18. 分阶段优先级

### P0：真实数据链

- [ ] Run、Phase、Agent 和 ticker 状态。
- [ ] LLM/Tool 耗时、Token、错误和重试。
- [ ] Artifact 校验和最终投资结论。
- [ ] SSE 实时更新、断线补齐和节点点击详情。
- [ ] Agent 基础路径移动和最终塔楼点亮。
- [ ] 当前实际 Phase 1–8 映射准确。

### P1：研究过程

- [ ] Bull/Bear/Topic Controller 辩论动画。
- [ ] Phase 3 概率变化和 Phase 5 风险门动画。
- [ ] 多 ticker 切换、执行时间线和历史回放。
- [ ] Tool/LLM 详情、Artifact 链路和 degraded 路径。
- [ ] Phase 7 allocation 与 Phase 8 reflection/archive 展示。

### P2：机关与视觉增强

- [ ] BFS 实际寻路动画和建筑 90° 旋转。
- [ ] 视觉错位桥和不可能连接。
- [ ] 曲柄/远程触发升降平台。
- [ ] 数据粒子、Artifact 携带和多 Agent 同屏。
- [ ] 星际传送门仅作为纯视觉效果，不映射虚构工作流能力。

第一版先打通 **Run → Phase → Agent → Tool → Artifact → Decision** 的真实
事件链，再逐步增加复杂机关；任何动画都必须由真实状态驱动。
