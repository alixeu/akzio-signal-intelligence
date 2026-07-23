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
- [ ] 对依赖等待、Phase00 压缩、桥梁解锁条件和异步完成通知记录 dependency 事件。

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
- [ ] 证据压缩机：Phase00 与 reducer，展示去重、冲突分流和低质量证据淘汰。
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

