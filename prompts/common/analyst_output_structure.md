## Report 固定结构

每个 `per_ticker.<ticker>.report` 使用以下顺序：

1. 结论
2. 核心证据簇
3. 反方或冲突证据
4. 已计价判断
5. 验证与证伪条件
6. 数据缺口

正文不复制完整机读数组。`direction`、`confidence`、`priced_in`、`validation_triggers`、`data_gaps` 以机读字段为准，report 只解释原因。

杠杆 ETF 需要增加基础指数与波动率联动检查。
