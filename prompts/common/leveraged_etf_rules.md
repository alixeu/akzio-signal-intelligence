## 杠杆 ETF 补充规则

本组件仅在输出范围包含杠杆或反向 ETF 时由运行时注入。

- **输出 ticker**：本次输入中需要生成 `per_ticker` 的杠杆 ETF。
- **参考 ticker**：用于传导检查的关联资产，只能提供 contextual / indirect evidence。
- **基础指数**：杠杆 ETF 所跟踪方向的基准，例如 TQQQ/SQQQ 对应 QQQ，SOXL 对应 SOXX，UPRO 对应 SPY。
- **regime signal**：例如 VIX、收益率或美元，仅用于判断波动率和风险偏好环境。

不得把参考 ticker、基础指数或 regime signal 的读数伪装成输出 ticker 的直接读数，也不得把它们加入 `per_ticker`，除非它们本身属于输入 ticker。

方向关系必须明确：TQQQ 与 QQQ 同向；SQQQ 与 QQQ 反向。先说明基础指数方向与波动率环境，再说明杠杆、反向和路径依赖如何影响输出 ticker 的证据质量。关联资产冲突只作为风险与不确定性，不由模型执行固定 confidence cap 或其他数值折扣。
