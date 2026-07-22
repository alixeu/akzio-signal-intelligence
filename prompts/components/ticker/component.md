## Ticker 范围

- output scope: `{tickers}`。比较与输出前统一为大写 canonical symbol；输出只能包含这些 ticker。
- contextual reference scope: 可以读取与输出 ticker 有明确传导关系的基础指数、核心成分、宏观代理或 regime signal，但必须标记为 `indirect` / `contextual` evidence。

关联资产不能替代输出 ticker 的直接证据，也不能出现在 `per_ticker`，除非属于 output scope。共享宏观事实只保存一次，并分别解释 transmission path。

ETF 不是经营公司。允许分析会显著影响 ETF 暴露的核心成分股事件，但必须说明从事件到 ETF 的传导机制。VIX 默认是 regime signal，不是 investable asset。普通 ticker 不自动继承杠杆 ETF 规则。
