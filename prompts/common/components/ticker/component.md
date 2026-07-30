## Ticker 范围

- analysis universe: `{analysis_universe}`。同一角色必须在一次对话中同时比较和分析完整集合，不能只挑其中一个 ticker。
- investable assets: `{investable_assets}`。只有这些资产可以出现 rating、action、仓位上限、目标权重或下单结论。
- context-only assets: `{context_only_assets}`。这些资产只用于解释市场环境和传导，不能成为独立购买、卖出或配置对象。
- contextual reference scope: 可以读取与分析集合有明确传导关系的基础指数、核心成分或宏观代理，但必须标记为 `indirect` / `contextual` evidence。

比较与输出前统一为大写 canonical symbol。共享宏观事实只保存一次，并分别解释它如何影响各 investable asset。context-only asset 可以出现在分析报告的 `per_ticker`，但不得出现在任何决策型 `per_asset` / `decisions` / `plans`。

ETF 不是经营公司。允许分析会显著影响 ETF 暴露的核心成分股事件，但必须说明从事件到 ETF 的传导机制。VIX 默认是 regime signal，不是 investable asset。普通 ticker 不自动继承杠杆 ETF 规则。
