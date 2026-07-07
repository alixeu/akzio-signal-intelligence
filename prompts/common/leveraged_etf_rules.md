## 杠杆 ETF 通用规则

1. 只有当本次输入 ticker 包含 `TQQQ`、`SQQQ`、`UPRO`、`SOXL` 等杠杆 ETF 时，才执行本节规则；否则不要新增或替换 ticker。
2. 若 ticker 为杠杆 ETF，必须检查其对应基础指数 ETF 或行业 ETF 的价格结构 / 基本面是否同向。对应关系至少包括：
   - `TQQQ` / `SQQQ` -> `QQQ`
   - `UPRO` -> `SPY`
   - `SOXL` -> `SOXX`
3. 对 `TQQQ`，分析必须同时检查 `TQQQ`、`QQQ`、`VIX`，但只有本次输入包含 `TQQQ` 时，才在 `per_ticker` 中输出 `TQQQ`。
4. 若杠杆 ETF 自身出现看多信号，但基础指数 ETF 价格结构偏空或动量恶化，必须明确降权，不得把杠杆 ETF 自身指标孤立解读为强看多。
5. 若基础指数 ETF 上涨但 `VIX` 同时显著上行或维持异常强势，必须视为风险偏好异常或趋势质量下降的警报，而不是忽略。
6. 若杠杆 ETF、基础指数 ETF 与 `VIX` 三者方向明显冲突，`confidence` 不得高于 `0.65`。
7. 对 `TQQQ` / `SQQQ`，核心主线包括 `QQQ` / Nasdaq 100 / Mega Cap Tech / Fed / rates / `US10Y` / `VIX` / `DXY` / CPI / NFP。
8. 若 `QQQ` 风险偏好与 `US10Y`、`DXY` 或 `VIX` 冲突，应下调 `confidence` 并说明冲突。
