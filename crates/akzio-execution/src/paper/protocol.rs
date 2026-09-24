// 文件导读：协议解析层把 Alpaca JSON 与领域 PaperOrderReceipt/MarketClock/订单请求互转。
// 解析阶段只接受明确字段、有限精度的小数和已知状态；client_order_id 由 session、plan
// hash、订单索引和 repricing 次数确定性构造，是恢复时防止重复提交的身份核心。

fn receipt_from_value(
    value: Value,
    expected_client_order_id: &str,
    reused: bool,
    reprice_count: u8,
) -> Result<PaperOrderReceipt> {
    // 读取并校验 broker identity、数量、成交价、时间和拒单/取消原因；client ID 不匹配
    // 直接按 Commitment 破坏处理，不能仅凭 symbol 或 broker order ID 关联。
    // `value` 按值移入并由本函数读取；expected ID 是只读字符串借用。required_string 会
    // 把 JSON 字段复制成 owned String，供返回的 receipt 独立持有。
    let broker_order_id = required_string(&value, "id")?;
    let symbol = required_string(&value, "symbol")?;
    let status = required_string(&value, "status")?;
    let client_order_id = required_string(&value, "client_order_id")?;
    if client_order_id != expected_client_order_id {
        return Err(PaperError::InvalidCommitment(
            "broker client order ID does not match durable commitment".to_owned(),
        ));
    }
    let requested_quantity_micros = decimal_micros(&required_string(&value, "qty")?)?;
    let filled_quantity_micros = decimal_micros(&required_string(&value, "filled_qty")?)?;
    let remaining_quantity_micros = requested_quantity_micros
        .checked_sub(filled_quantity_micros)
        .filter(|quantity| *quantity >= 0)
        .ok_or(PaperError::InvalidQuantity("filled_qty"))?;
    // 可选 filled_avg_price 的处理链先借用 JSON 字符串；map 返回 Result，transpose
    // 将 Option<Result<_>> 变为 Result<Option<_>>，因此字段缺失是 None、格式错误是 Err。
    let average_fill_price = value
        .get("filled_avg_price")
        .and_then(Value::as_str)
        .filter(|price| !price.trim().is_empty())
        .map(decimal_micros)
        .transpose()?
        .map(MoneyMicros);
    let broker_updated_at = DateTime::parse_from_rfc3339(&required_string(&value, "updated_at")?)
        .map_err(|error| PaperError::InvalidClock(error.to_string()))?
        .with_timezone(&Utc);
    // 按字段优先级找第一个字符串原因；find_map 在命中时短路，最终 trim 后复制非空值。
    let reason = ["reject_reason", "cancel_reason"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .map(ToOwned::to_owned);
    Ok(PaperOrderReceipt {
        client_order_id,
        broker_order_id,
        symbol,
        status,
        requested_quantity_micros,
        filled_quantity_micros,
        remaining_quantity_micros,
        average_fill_price,
        broker_updated_at,
        reason,
        reused,
        reprice_count,
    })
}

fn decimal_micros(value: &str) -> Result<i64> {
    // 手动把最多六位小数转换为整数 micros，避免浮点舍入；格式、溢出和负值都显式受控。
    // `&str` 仅借用输入文本；trim/strip_prefix/split_once 都产生原字符串切片，不分配新正文。
    let value = value.trim();
    let (negative, value) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 6
    {
        return Err(PaperError::InvalidQuantity("decimal"));
    }
    // 整数部分与小数部分分别解析为 owned i64；小数右补到六位后以 checked 算术组合，
    // 任一解析/乘法/加法溢出都返回 InvalidQuantity，而不是饱和或浮点舍入。
    let whole = whole
        .parse::<i64>()
        .map_err(|_| PaperError::InvalidQuantity("decimal"))?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .map_err(|_| PaperError::InvalidQuantity("decimal"))?
            .checked_mul(10_i64.pow((6 - fraction.len()) as u32))
            .ok_or(PaperError::InvalidQuantity("decimal"))?
    };
    let micros = whole
        .checked_mul(1_000_000)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or(PaperError::InvalidQuantity("decimal"))?;
    Ok(if negative { -micros } else { micros })
}

fn required_string(value: &Value, field: &'static str) -> Result<String> {
    // Broker 字段缺失立即失败，避免 serde 的默认值把“不知道”伪装成零或空字符串。
    // JSON Value 只读借用；`as_str` 的 Option 在 map 中复制成 String，None 统一变成
    // MissingField(field)，保留“缺字段”而不填空串。
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(PaperError::MissingField(field))
}

fn parse_value(body: &str) -> Value {
    // 成功响应优先解析 JSON；非 JSON 正文保留为字符串，错误诊断不在此处猜 schema。
    // `unwrap_or_else` 仅在 JSON 解析 Err 时惰性调用闭包，把原始 body 复制为字符串 Value；
    // 解析成功则不执行 fallback。
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_owned()))
}

fn reprice_count_from_client_order_id(client_order_id: &str) -> u8 {
    // 从固定的 -rN 后缀恢复替换代数，未知后缀按原始订单代数 0 处理。
    // `and_then` 只有分割成功才 parse u8；无后缀或越界数字的 Option 都由 unwrap_or(0) 归为 r0。
    client_order_id
        .rsplit_once("-r")
        .and_then(|(_, value)| value.parse::<u8>().ok())
        .unwrap_or(0)
}

fn broker_status_is_final_without_successor(status: &str) -> bool {
    // 这些状态本身终止原订单，但若存在 durable reprice intent，仍需观察 successor 才能
    // 关闭不确定窗口；调用方负责结合两者判断整体 settlement。
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "filled" | "canceled" | "expired" | "rejected" | "failed"
    )
}

fn market_clock_with_calendar(clock: &Value, calendar: &Value) -> Result<MarketClock> {
    // 以 broker timestamp 和日历构造领域 TradingSession；不以本机日期或单独 is_open 推断
    // Overnight/Closed 的业务含义。
    // 两个 JSON Value 都共享借用；clock 字段缺失用专用 PaperError 返回，calendar schema
    // 由 Domain ExchangeSession::from_alpaca 校验。
    let is_open = clock
        .get("is_open")
        .and_then(Value::as_bool)
        .ok_or(PaperError::MissingField("clock.is_open"))?;
    let timestamp = clock
        .get("timestamp")
        .and_then(Value::as_str)
        .ok_or(PaperError::MissingField("clock.timestamp"))?;
    let observed_at = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| PaperError::InvalidClock(error.to_string()))?;
    let session = akzio_domain::TradingSessionSnapshot::from_calendar(
        observed_at.with_timezone(&Utc),
        is_open,
        &akzio_domain::ExchangeSession::from_alpaca(calendar)?,
    )?;
    Ok(MarketClock {
        is_open,
        session_date: session.trade_date,
        session,
    })
}

fn side_name(side: OrderSide) -> &'static str {
    // 领域枚举到 Alpaca wire 字符串的封闭映射。
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_request(order: &OrderIntent, client_order_id: &str) -> Result<Value> {
    // 将已由 Rust 生成的 OrderIntent 编码为 limit/day 请求，保留 extended_hours 与 durable ID。
    // OrderIntent 与 ID 只读借用；quantity_string 的错误经 `?` 阻断请求体构造，直到成功
    // 才生成新 Value 交给 transport。
    Ok(serde_json::json!({
        "symbol": order.asset.symbol(), "qty": quantity_string(order)?,
        "side": side_name(order.side), "type": "limit", "time_in_force": "day",
        "limit_price": money_string(order.limit_price), "extended_hours": order.extended_hours,
        "client_order_id": client_order_id,
    }))
}

fn money_string(value: MoneyMicros) -> String {
    // 整数 micros 格式化为固定六位小数，避免二进制浮点进入订单请求。
    // MoneyMicros 是 Copy 数值，除法/取余只做整数运算；format! 分配返回的 wire 字符串。
    let whole = value.0 / 1_000_000;
    let fraction = value.0.unsigned_abs() % 1_000_000;
    format!("{whole}.{fraction:06}")
}

fn quantity_string(order: &OrderIntent) -> Result<String> {
    // 用名义金额除以限价得到 millionths 数量；零数量在 POST 前阻断。
    // `order` 共享借用；limit_price 为 0 时 checked_div 得 None 并取 0，随后统一拒绝为零数量。
    let quantity_millionths = order
        .notional
        .0
        .saturating_mul(1_000_000)
        .checked_div(order.limit_price.0)
        .unwrap_or_default();
    if quantity_millionths <= 0 {
        return Err(PaperError::ZeroQuantity);
    }
    let whole = quantity_millionths / 1_000_000;
    let fraction = quantity_millionths.unsigned_abs() % 1_000_000;
    Ok(format!("{whole}.{fraction:06}"))
}

pub fn client_order_id(
    broker_session: &str,
    plan_hash: &ContentHash,
    order_index: usize,
    reprice_count: u8,
) -> String {
    // 对 broker session、plan hash、订单位置做内容寻址，repricing 只改变显式代数后缀。
    // format! 暂时分配身份输入，再 hash 成固定长度 ContentHash；prefix 是对该局部 hash 字符串
    // 的短期切片借用，最终格式化成 owned ID，生命周期不泄漏到返回值。
    let identity =
        ContentHash::of_bytes(format!("{broker_session}\0{plan_hash}\0{order_index}").as_bytes());
    let prefix = &identity.as_str()[..16];
    format!("akzio-{prefix}-{order_index}-r{reprice_count}")
}

fn replacement_client_order_id(previous: &str) -> String {
    // 将原始 r0 ID 映射到唯一 r1 successor；不接受任意动态重试次数。
    // split/next 只借用原字符串的前缀；若无 r 后缀就保留原文，再分配唯一 r1 字符串。
    let base = previous.split("-r").next().unwrap_or(previous);
    format!("{base}-r1")
}
