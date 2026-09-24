# 历史 Debug 控制退役

独立 Debug Core、CLI `debug` 命令组、`/v1/debug/runs` 控制路由、App 调试入口、隔离 Core 配置以及以 Debug purpose 创建证据图的 `evidence market-audit` 已退役；不得通过旧配置或旧 Run 恢复 prepare、step、resume、fork 或实验写入。

历史 DebugSession、RunControl、checkpoint、CAS 和隔离 Store 标记继续供只读 replay、inspection、journal、完整性检查及分享安全导出解码。Store 的任务领取、permit 写入、过期恢复和 Broker effect 边界拒绝旧 Debug 身份；隔离 Store 不得作为正式 Core 启动，其历史运行不取得 canonical 校准/学习/交易资格。既有冻结 Contract、Commitment、策略身份和有效 lease 不改写。

正式 PositionPlan/Paper 入口、私有阶段日志和独立离线测试分别见[开发 Workflow](development-workflow.md)与[运行日志](run-log.md)。离线 fixture 测试不是实际模型、Paper 成交或跨交易日 Outcome 证据。
