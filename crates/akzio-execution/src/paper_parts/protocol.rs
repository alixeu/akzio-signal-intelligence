fn receipt_from_value(
    value: Value,
    expected_client_order_id: &str,
    reused: bool,
    reprice_count: u8,
) -> Result<PaperOrderReceipt> {
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
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(PaperError::MissingField(field))
}

fn parse_value(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_owned()))
}

fn market_clock_from_value(clock: &Value) -> Result<MarketClock> {
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
    Ok(MarketClock {
        is_open,
        session_date: observed_at.date_naive(),
    })
}

fn side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn money_string(value: MoneyMicros) -> String {
    let whole = value.0 / 1_000_000;
    let fraction = value.0.unsigned_abs() % 1_000_000;
    format!("{whole}.{fraction:06}")
}


fn quantity_string(order: &OrderIntent) -> Result<String> {
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
    let identity =
        ContentHash::of_bytes(format!("{broker_session}\0{plan_hash}\0{order_index}").as_bytes());
    let prefix = &identity.as_str()[..16];
    format!("akzio-v2-{prefix}-{order_index}-r{reprice_count}")
}

fn replacement_client_order_id(previous: &str) -> String {
    let base = previous.split("-r").next().unwrap_or(previous);
    format!("{base}-r1")
}
