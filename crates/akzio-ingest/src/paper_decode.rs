//! Decode Paper provider payloads into domain snapshots and bar series.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    AccountSnapshot, Asset, DomainError, MarketClockSnapshot, MoneyMicros, Position, Quote,
    QuoteSnapshot, V2_DOMAIN_SCHEMA_VERSION,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PaperDecodeError {
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Domain(#[from] DomainError),
}

pub type PaperDecodeResult<T> = Result<T, PaperDecodeError>;

pub fn parse_daily_bars(
    value: &Value,
    observed_at: DateTime<Utc>,
) -> PaperDecodeResult<BTreeMap<NaiveDate, MoneyMicros>> {
    let Some(items) = value.get("bars").and_then(Value::as_array) else {
        let close = value
            .get("close")
            .and_then(parse_money_micros)
            .ok_or_else(|| {
                PaperDecodeError::Unavailable("daily bars close is missing".to_owned())
            })?;
        return Ok(BTreeMap::from([(observed_at.date_naive(), close)]));
    };
    let mut bars = BTreeMap::new();
    for item in items {
        let close = item
            .get("c")
            .or_else(|| item.get("close"))
            .and_then(parse_money_micros)
            .ok_or_else(|| {
                PaperDecodeError::Unavailable("daily bar close is invalid".to_owned())
            })?;
        let date = item
            .get("t")
            .or_else(|| item.get("timestamp"))
            .and_then(Value::as_str)
            .and_then(|timestamp| {
                DateTime::parse_from_rfc3339(timestamp)
                    .map(|value| value.date_naive())
                    .or_else(|_| NaiveDate::parse_from_str(timestamp, "%Y-%m-%d").map_err(|_| ()))
                    .ok()
            })
            .unwrap_or_else(|| observed_at.date_naive());
        if bars.insert(date, close).is_some() {
            return Err(PaperDecodeError::Unavailable(
                "daily bar date is duplicated".to_owned(),
            ));
        }
    }
    Ok(bars)
}

pub fn parse_money_micros(value: &Value) -> Option<MoneyMicros> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_number().map(ToString::to_string))?;
    let raw = raw.trim();
    let (negative, unsigned) = if let Some(value) = raw.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = raw.strip_prefix('+') {
        (false, value)
    } else {
        (false, raw)
    };
    if unsigned.is_empty() || unsigned.contains(['e', 'E']) {
        return None;
    }
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
        || fraction.len() > 6
    {
        return None;
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<i64>().ok()?
    };
    let mut fraction = fraction.to_owned();
    while fraction.len() < 6 {
        fraction.push('0');
    }
    let fraction = fraction.parse::<i64>().ok()?;
    let magnitude = whole.checked_mul(1_000_000)?.checked_add(fraction)?;
    Some(MoneyMicros(if negative {
        magnitude.checked_neg()?
    } else {
        magnitude
    }))
}

pub fn common_bar_dates(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    baseline: NaiveDate,
) -> Vec<NaiveDate> {
    let Some((_, first)) = bars_by_asset.iter().next() else {
        return Vec::new();
    };
    first
        .keys()
        .copied()
        .filter(|date| *date > baseline)
        .filter(|date| bars_by_asset.values().all(|bars| bars.get(date).is_some()))
        .collect()
}

pub fn decode_paper_account(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> PaperDecodeResult<AccountSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| PaperDecodeError::InvalidInput("Paper account status missing".to_owned()))?;
    Ok(AccountSnapshot {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        observed_at,
        equity: provider_money(value, "equity")?,
        buying_power: provider_money(value, "buying_power")?,
        day_turnover: MoneyMicros::ZERO,
        active: status.eq_ignore_ascii_case("ACTIVE"),
        trading_blocked: value
            .get("trading_blocked")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                PaperDecodeError::InvalidInput("Paper account trading_blocked missing".to_owned())
            })?,
        positions: BTreeMap::new(),
        external_positions: BTreeSet::new(),
        open_order_ids: BTreeSet::new(),
    })
}

pub fn decode_paper_account_components(
    account_value: &Value,
    positions_value: &Value,
    open_orders_value: &Value,
    fills_value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> PaperDecodeResult<AccountSnapshot> {
    let mut account = decode_paper_account(account_value, broker_session, observed_at)?;
    if account_value.get("schema_version").is_some() {
        return Ok(account);
    }

    account.positions.clear();
    account.external_positions.clear();
    for position in positions_value.as_array().ok_or_else(|| {
        PaperDecodeError::InvalidInput("Paper positions must be an array".to_owned())
    })? {
        let symbol = position
            .get("symbol")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                PaperDecodeError::InvalidInput("Paper position symbol missing".to_owned())
            })?;
        let quantity_micros = position
            .get("qty")
            .and_then(parse_money_micros)
            .map(|quantity| quantity.0)
            .ok_or_else(|| {
                PaperDecodeError::InvalidInput("Paper position qty invalid".to_owned())
            })?;
        let market_value = provider_money(position, "market_value")?;
        match Asset::try_from(symbol) {
            Ok(asset) => {
                account.positions.insert(
                    asset,
                    Position {
                        quantity_micros,
                        market_value,
                    },
                );
            }
            Err(_) => {
                account.external_positions.insert(symbol.to_owned());
            }
        }
    }

    account.open_order_ids = open_orders_value
        .as_array()
        .ok_or_else(|| {
            PaperDecodeError::InvalidInput("Paper open orders must be an array".to_owned())
        })?
        .iter()
        .map(|order| {
            order
                .get("client_order_id")
                .or_else(|| order.get("id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    PaperDecodeError::InvalidInput("Paper open order ID missing".to_owned())
                })
        })
        .collect::<PaperDecodeResult<BTreeSet<_>>>()?;

    let fills = fills_value
        .as_array()
        .ok_or_else(|| PaperDecodeError::InvalidInput("Paper fills must be an array".to_owned()))?;
    if fills.len() >= 100 {
        return Err(PaperDecodeError::InvalidInput(
            "Paper fills require pagination before execution".to_owned(),
        ));
    }
    let turnover = fills.iter().try_fold(0_i128, |sum, fill| {
        let quantity = fill
            .get("qty")
            .and_then(parse_money_micros)
            .map(|value| i128::from(value.0).abs())
            .ok_or_else(|| PaperDecodeError::InvalidInput("Paper fill qty invalid".to_owned()))?;
        let price = i128::from(provider_money(fill, "price")?.0).abs();
        Ok::<_, PaperDecodeError>(
            sum.saturating_add(quantity.saturating_mul(price).saturating_div(1_000_000)),
        )
    })?;
    account.day_turnover = MoneyMicros(i64::try_from(turnover).map_err(|_| {
        PaperDecodeError::InvalidInput("Paper day turnover exceeds i64".to_owned())
    })?);

    Ok(account)
}
pub fn decode_paper_quotes(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> PaperDecodeResult<QuoteSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    let quotes = value
        .get("quotes")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            PaperDecodeError::InvalidInput("Paper quotes payload missing quotes".to_owned())
        })?
        .iter()
        .map(|(symbol, quote)| {
            let asset = Asset::try_from(symbol.as_str()).map_err(|_| {
                PaperDecodeError::InvalidInput(format!(
                    "Paper quote asset outside v2 universe: {symbol}"
                ))
            })?;
            let quote_observed_at = quote
                .get("t")
                .map(|timestamp| provider_timestamp(timestamp, "quote.t"))
                .transpose()?
                .unwrap_or(observed_at);
            Ok((
                asset,
                Quote {
                    bid: provider_money(quote, "bp")?,
                    ask: provider_money(quote, "ap")?,
                    observed_at: quote_observed_at,
                },
            ))
        })
        .collect::<PaperDecodeResult<BTreeMap<_, _>>>()?;
    if quotes.is_empty() {
        return Err(PaperDecodeError::InvalidInput(
            "Paper quotes payload contains no executable assets".to_owned(),
        ));
    }
    Ok(QuoteSnapshot {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        observed_at,
        quotes,
    })
}

pub fn decode_paper_clock(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> PaperDecodeResult<MarketClockSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    Ok(MarketClockSnapshot {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        is_open: value
            .get("is_open")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                PaperDecodeError::InvalidInput("Paper clock is_open missing".to_owned())
            })?,
        observed_at: value
            .get("timestamp")
            .map(|timestamp| provider_timestamp(timestamp, "clock.timestamp"))
            .transpose()?
            .unwrap_or(observed_at),
    })
}

pub fn provider_money(value: &Value, field: &str) -> PaperDecodeResult<MoneyMicros> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .ok_or_else(|| {
            PaperDecodeError::InvalidInput(format!("Paper provider field {field} invalid"))
        })
}

fn provider_timestamp(value: &Value, field: &str) -> PaperDecodeResult<DateTime<Utc>> {
    let raw = value.as_str().ok_or_else(|| {
        PaperDecodeError::InvalidInput(format!("Paper provider field {field} invalid"))
    })?;
    DateTime::parse_from_rfc3339(raw)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            PaperDecodeError::InvalidInput(format!("Paper provider field {field}: {error}"))
        })
}
