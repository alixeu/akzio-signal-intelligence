//! Canonical decimal-money parsing at the daemon ingress.

use akzio_domain::MoneyMicros;
use serde_json::Value;

use crate::{DaemonError, Result};

pub(super) fn parse_money(value: &Value) -> Result<MoneyMicros> {
    let text = match value {
        Value::String(value) => value.as_str(),
        Value::Number(value) => return parse_money_text(&value.to_string()),
        _ => {
            return Err(DaemonError::InvalidInput(
                "money must be a decimal".to_owned(),
            ))
        }
    };
    parse_money_text(text)
}

fn parse_money_text(text: &str) -> Result<MoneyMicros> {
    let text = text.trim();
    let (negative, text) = match text.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let whole = whole
        .parse::<i64>()
        .map_err(|_| DaemonError::InvalidInput(format!("invalid money {text:?}")))?;
    let mut digits = fraction.chars();
    let mut micros = 0_i64;
    for _ in 0..6 {
        micros =
            micros.saturating_mul(10)
                + i64::from(
                    digits.next().unwrap_or('0').to_digit(10).ok_or_else(|| {
                        DaemonError::InvalidInput(format!("invalid money {text:?}"))
                    })?,
                );
    }
    if digits.next().is_some_and(|digit| digit >= '5') {
        micros += 1;
    }
    let value = whole.saturating_mul(1_000_000).saturating_add(micros);
    Ok(MoneyMicros(if negative { -value } else { value }))
}
