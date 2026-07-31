//! Rust-owned Alpaca Paper execution.
//!
//! Model roles never receive these methods as tools. The workflow loads one
//! account snapshot, derives a deterministic order plan from the guarded Phase
//! 7 allocation, persists that plan, and only then calls the Paper API.

use anyhow::{bail, Context, Result};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{cmp::Ordering, collections::BTreeMap, time::Duration};

const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";
const MIN_ORDER_NOTIONAL_USD: f64 = 1.0;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PositionSnapshot {
    pub symbol: String,
    pub qty: f64,
    pub market_value: f64,
    pub current_price: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AccountSnapshot {
    pub status: String,
    pub source: String,
    pub simulated: bool,
    pub account_status: String,
    pub cash: f64,
    pub equity: f64,
    pub buying_power: f64,
    pub trading_blocked: bool,
    pub positions: Vec<PositionSnapshot>,
    pub current_weights: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OrderIntent {
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub target_weight: f64,
    pub current_weight: f64,
    pub target_value: f64,
    pub current_value: f64,
    pub estimated_notional: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notional: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_qty: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct SkippedOrder {
    pub symbol: String,
    pub reason: String,
    pub estimated_notional: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OrderPlan {
    pub status: String,
    pub account_equity: f64,
    pub orders: Vec<OrderIntent>,
    pub skipped: Vec<SkippedOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OrderReceipt {
    pub client_order_id: String,
    pub broker_order_id: String,
    pub symbol: String,
    pub side: String,
    pub status: String,
    pub simulated: bool,
    pub recovered_existing_order: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_qty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_avg_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ExecutionReport {
    pub status: String,
    pub simulated: bool,
    pub receipts: Vec<OrderReceipt>,
}

#[derive(Debug, Clone)]
pub(crate) struct AlpacaCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub(crate) fn credentials(
    api_key: Option<&str>,
    api_secret: Option<&str>,
) -> Result<AlpacaCredentials> {
    let api_key = api_key
        .filter(|value| !value.trim().is_empty())
        .context("orchestrator.alpaca.api_key is required for Alpaca Paper execution")?;
    let api_secret = api_secret
        .filter(|value| !value.trim().is_empty())
        .context("orchestrator.alpaca.api_secret is required for Alpaca Paper execution")?;
    Ok(AlpacaCredentials {
        api_key: api_key.to_owned(),
        api_secret: api_secret.to_owned(),
    })
}

pub(crate) fn debug_account_snapshot(
    investable_assets: &[String],
    starting_cash: f64,
) -> Result<AccountSnapshot> {
    if !starting_cash.is_finite() || starting_cash <= 0.0 {
        bail!("debug starting cash must be finite and greater than zero")
    }
    Ok(AccountSnapshot {
        status: "available".to_owned(),
        source: "debug_simulator".to_owned(),
        simulated: true,
        account_status: "ACTIVE".to_owned(),
        cash: starting_cash,
        equity: starting_cash,
        buying_power: starting_cash,
        trading_blocked: false,
        positions: Vec::new(),
        current_weights: investable_assets
            .iter()
            .map(|symbol| (symbol.to_ascii_uppercase(), 0.0))
            .collect(),
    })
}

pub(crate) async fn load_alpaca_account_snapshot(
    credentials: &AlpacaCredentials,
    investable_assets: &[String],
) -> Result<AccountSnapshot> {
    let client = http_client()?;
    let account = response_json(
        authenticated(
            client.get(format!("{PAPER_BASE_URL}/v2/account")),
            credentials,
        )
        .send()
        .await
        .context("Alpaca account request failed")?,
        "account",
    )
    .await?;
    let positions = response_json(
        authenticated(
            client.get(format!("{PAPER_BASE_URL}/v2/positions")),
            credentials,
        )
        .send()
        .await
        .context("Alpaca positions request failed")?,
        "positions",
    )
    .await?;
    parse_account_snapshot(&account.value, &positions.value, investable_assets)
}

pub(crate) fn build_order_plan(
    run_id: &str,
    allocation: &Value,
    market_snapshot: Option<&Value>,
    account: &AccountSnapshot,
) -> Result<OrderPlan> {
    if !account.equity.is_finite() || account.equity <= 0.0 {
        bail!("account equity must be finite and greater than zero")
    }
    let weights = allocation
        .get("weights")
        .and_then(Value::as_object)
        .context("Phase 7 allocation is missing weights")?;
    let positions = account
        .positions
        .iter()
        .map(|position| (position.symbol.as_str(), position))
        .collect::<BTreeMap<_, _>>();
    let mut orders = Vec::new();
    let mut skipped = Vec::new();

    for (symbol, entry) in weights {
        if symbol == "cash_hedge" {
            continue;
        }
        let target_weight = entry
            .get("weight")
            .and_then(Value::as_f64)
            .with_context(|| format!("allocation weight missing for {symbol}"))?;
        if !target_weight.is_finite() || !(0.0..=1.0).contains(&target_weight) {
            bail!("allocation weight invalid for {symbol}: {target_weight}")
        }
        let position = positions.get(symbol.as_str()).copied();
        if position.is_some_and(|position| position.qty < 0.0 || position.market_value < 0.0) {
            bail!("short position execution is not supported for {symbol}")
        }
        let current_value = position.map_or(0.0, |position| position.market_value);
        let current_weight = current_value / account.equity;
        let target_value = account.equity * target_weight;
        let delta = target_value - current_value;
        let estimated_notional = delta.abs();
        if estimated_notional < MIN_ORDER_NOTIONAL_USD {
            skipped.push(SkippedOrder {
                symbol: symbol.clone(),
                reason: "below_minimum_order_notional".to_owned(),
                estimated_notional: round_money(estimated_notional),
            });
            continue;
        }

        let side = if delta > 0.0 { "buy" } else { "sell" };
        let estimated_price = position
            .map(|position| position.current_price)
            .filter(|price| *price > 0.0)
            .or_else(|| market_price(market_snapshot, symbol));
        let estimated_qty = estimated_price
            .filter(|price| *price > 0.0)
            .map(|price| round_qty(estimated_notional / price));
        let (notional, qty) = if side == "buy" {
            (Some(round_money(estimated_notional)), None)
        } else {
            let position = position.context("sell order requires an existing long position")?;
            let price = estimated_price.context("sell order requires a positive current price")?;
            let qty = round_qty((estimated_notional / price).min(position.qty));
            if qty <= 0.0 {
                skipped.push(SkippedOrder {
                    symbol: symbol.clone(),
                    reason: "zero_sell_quantity".to_owned(),
                    estimated_notional: round_money(estimated_notional),
                });
                continue;
            }
            (None, Some(qty))
        };
        let client_order_id =
            stable_client_order_id(run_id, symbol, side, target_weight, current_weight)?;
        orders.push(OrderIntent {
            client_order_id,
            symbol: symbol.clone(),
            side: side.to_owned(),
            order_type: "market".to_owned(),
            time_in_force: "day".to_owned(),
            target_weight: round_weight(target_weight),
            current_weight: round_weight(current_weight),
            target_value: round_money(target_value),
            current_value: round_money(current_value),
            estimated_notional: round_money(estimated_notional),
            notional,
            qty,
            estimated_price,
            estimated_qty,
        });
    }
    orders.sort_by(
        |left, right| match (left.side.as_str(), right.side.as_str()) {
            ("sell", "buy") => Ordering::Less,
            ("buy", "sell") => Ordering::Greater,
            _ => left.symbol.cmp(&right.symbol),
        },
    );
    Ok(OrderPlan {
        status: if orders.is_empty() {
            "no_orders".to_owned()
        } else {
            "ready".to_owned()
        },
        account_equity: round_money(account.equity),
        orders,
        skipped,
    })
}

pub(crate) async fn submit_order_plan(
    plan: &OrderPlan,
    account: &AccountSnapshot,
    credentials: Option<&AlpacaCredentials>,
    simulated: bool,
) -> Result<ExecutionReport> {
    if plan.orders.is_empty() {
        return Ok(ExecutionReport {
            status: "no_orders".to_owned(),
            simulated,
            receipts: Vec::new(),
        });
    }
    if account.trading_blocked {
        bail!("Alpaca account is blocked from trading")
    }
    if simulated {
        return Ok(ExecutionReport {
            status: "simulated_filled".to_owned(),
            simulated: true,
            receipts: plan
                .orders
                .iter()
                .map(|order| OrderReceipt {
                    client_order_id: order.client_order_id.clone(),
                    broker_order_id: format!("simulated-{}", order.client_order_id),
                    symbol: order.symbol.clone(),
                    side: order.side.clone(),
                    status: "simulated_filled".to_owned(),
                    simulated: true,
                    recovered_existing_order: false,
                    filled_qty: order.qty.or(order.estimated_qty),
                    filled_avg_price: order.estimated_price,
                    submitted_at: None,
                    request_id: None,
                })
                .collect(),
        });
    }

    let credentials = credentials.context("Alpaca Paper credentials are required to submit")?;
    let client = http_client()?;
    let mut receipts = Vec::with_capacity(plan.orders.len());
    for order in &plan.orders {
        if let Some(existing) = find_order_by_client_id(&client, credentials, order).await? {
            receipts.push(order_receipt(order, existing, true)?);
            continue;
        }
        let body = order_request(order);
        let response = response_json(
            authenticated(
                client
                    .post(format!("{PAPER_BASE_URL}/v2/orders"))
                    .json(&body),
                credentials,
            )
            .send()
            .await
            .with_context(|| format!("Alpaca order request failed for {}", order.symbol))?,
            "order",
        )
        .await?;
        receipts.push(order_receipt(order, response, false)?);
    }
    Ok(ExecutionReport {
        status: "submitted".to_owned(),
        simulated: false,
        receipts,
    })
}

fn parse_account_snapshot(
    account: &Value,
    positions: &Value,
    investable_assets: &[String],
) -> Result<AccountSnapshot> {
    let equity = numeric_field(account, "equity")?;
    if equity <= 0.0 {
        bail!("Alpaca account equity must be greater than zero")
    }
    let position_values = positions
        .as_array()
        .context("Alpaca positions response must be an array")?;
    let mut parsed_positions = Vec::with_capacity(position_values.len());
    for position in position_values {
        let symbol = position
            .get("symbol")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("Alpaca position is missing symbol")?
            .trim()
            .to_ascii_uppercase();
        let market_value = numeric_field(position, "market_value")?;
        parsed_positions.push(PositionSnapshot {
            symbol,
            qty: numeric_field(position, "qty")?,
            market_value,
            current_price: numeric_field(position, "current_price")?,
            weight: round_weight(market_value / equity),
        });
    }
    let current_weights = investable_assets
        .iter()
        .map(|symbol| {
            let symbol = symbol.to_ascii_uppercase();
            let weight = parsed_positions
                .iter()
                .find(|position| position.symbol == symbol)
                .map_or(0.0, |position| position.weight);
            (symbol, weight)
        })
        .collect();
    Ok(AccountSnapshot {
        status: "available".to_owned(),
        source: "alpaca_paper".to_owned(),
        simulated: false,
        account_status: account
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        cash: numeric_field(account, "cash")?,
        equity,
        buying_power: numeric_field(account, "buying_power")?,
        trading_blocked: account
            .get("trading_blocked")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        positions: parsed_positions,
        current_weights,
    })
}

async fn find_order_by_client_id(
    client: &Client,
    credentials: &AlpacaCredentials,
    order: &OrderIntent,
) -> Result<Option<ApiResponse>> {
    let response = authenticated(
        client
            .get(format!("{PAPER_BASE_URL}/v2/orders:by_client_order_id"))
            .query(&[("client_order_id", order.client_order_id.as_str())]),
        credentials,
    )
    .send()
    .await
    .with_context(|| {
        format!(
            "Alpaca order recovery request failed for {}",
            order.client_order_id
        )
    })?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    response_json(response, "order recovery").await.map(Some)
}

fn order_request(order: &OrderIntent) -> Value {
    let mut body = json!({
        "symbol": order.symbol,
        "side": order.side,
        "type": order.order_type,
        "time_in_force": order.time_in_force,
        "client_order_id": order.client_order_id,
    });
    if let Some(notional) = order.notional {
        body["notional"] = json!(format!("{notional:.2}"));
    }
    if let Some(qty) = order.qty {
        body["qty"] = json!(format!("{qty:.6}"));
    }
    body
}

fn order_receipt(
    intent: &OrderIntent,
    response: ApiResponse,
    recovered_existing_order: bool,
) -> Result<OrderReceipt> {
    let returned_client_order_id = response
        .value
        .get("client_order_id")
        .and_then(Value::as_str)
        .context("Alpaca order response is missing client_order_id")?;
    if returned_client_order_id != intent.client_order_id {
        bail!(
            "Alpaca order response client_order_id mismatch for {}",
            intent.symbol
        )
    }
    for (field, expected) in [
        ("symbol", intent.symbol.as_str()),
        ("side", intent.side.as_str()),
    ] {
        if response.value.get(field).and_then(Value::as_str) != Some(expected) {
            bail!(
                "Alpaca order response {field} mismatch for {}",
                intent.symbol
            )
        }
    }
    let status = response
        .value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("accepted");
    if matches!(
        status,
        "canceled" | "expired" | "rejected" | "suspended" | "stopped"
    ) {
        bail!(
            "Alpaca order {} is in terminal failure status {status}",
            intent.client_order_id
        )
    }
    let broker_order_id = response
        .value
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("Alpaca order response is missing id")?
        .to_owned();
    Ok(OrderReceipt {
        client_order_id: intent.client_order_id.clone(),
        broker_order_id,
        symbol: response
            .value
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or(&intent.symbol)
            .to_owned(),
        side: response
            .value
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or(&intent.side)
            .to_owned(),
        status: status.to_owned(),
        simulated: false,
        recovered_existing_order,
        filled_qty: optional_numeric_field(&response.value, "filled_qty")?,
        filled_avg_price: optional_numeric_field(&response.value, "filled_avg_price")?,
        submitted_at: response
            .value
            .get("submitted_at")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        request_id: response.request_id,
    })
}

#[derive(Debug)]
struct ApiResponse {
    value: Value,
    request_id: Option<String>,
}

async fn response_json(response: Response, operation: &str) -> Result<ApiResponse> {
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let text = response
        .text()
        .await
        .with_context(|| format!("failed to read Alpaca {operation} response"))?;
    if !status.is_success() {
        bail!(
            "Alpaca {operation} returned HTTP {}{}: {}",
            status.as_u16(),
            request_id
                .as_deref()
                .map(|id| format!(" request_id={id}"))
                .unwrap_or_default(),
            truncate(&text, 2000)
        )
    }
    Ok(ApiResponse {
        value: serde_json::from_str(&text)
            .with_context(|| format!("Alpaca {operation} returned invalid JSON"))?,
        request_id,
    })
}

fn http_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to create Alpaca Paper HTTP client")
}

fn authenticated(
    request: reqwest::RequestBuilder,
    credentials: &AlpacaCredentials,
) -> reqwest::RequestBuilder {
    request
        .header("APCA-API-KEY-ID", &credentials.api_key)
        .header("APCA-API-SECRET-KEY", &credentials.api_secret)
}

fn numeric_field(value: &Value, field: &str) -> Result<f64> {
    optional_numeric_field(value, field)?
        .with_context(|| format!("Alpaca response is missing numeric field {field}"))
}

fn optional_numeric_field(value: &Value, field: &str) -> Result<Option<f64>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let parsed = raw
        .as_f64()
        .or_else(|| raw.as_str().and_then(|text| text.parse::<f64>().ok()))
        .with_context(|| format!("Alpaca field {field} is not numeric"))?;
    if !parsed.is_finite() {
        bail!("Alpaca field {field} must be finite")
    }
    Ok(Some(parsed))
}

fn stable_client_order_id(
    run_id: &str,
    symbol: &str,
    side: &str,
    target_weight: f64,
    current_weight: f64,
) -> Result<String> {
    let hash = orchestrator_store::content_hash(&json!({
        "run_id": run_id,
        "symbol": symbol,
        "side": side,
        "target_weight": round_weight(target_weight),
        "current_weight": round_weight(current_weight),
    }))?;
    Ok(format!("akzio-{}", &hash[..32.min(hash.len())]))
}

fn market_price(market_snapshot: Option<&Value>, symbol: &str) -> Option<f64> {
    market_snapshot?
        .pointer(&format!("/per_ticker/{symbol}/latest_close"))
        .and_then(Value::as_f64)
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn round_money(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round_qty(value: f64) -> f64 {
    (value * 1_000_000.0).floor() / 1_000_000.0
}

fn round_weight(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account_with_position() -> AccountSnapshot {
        AccountSnapshot {
            status: "available".to_owned(),
            source: "test".to_owned(),
            simulated: false,
            account_status: "ACTIVE".to_owned(),
            cash: 5_000.0,
            equity: 10_000.0,
            buying_power: 5_000.0,
            trading_blocked: false,
            positions: vec![PositionSnapshot {
                symbol: "QQQ".to_owned(),
                qty: 10.0,
                market_value: 5_000.0,
                current_price: 500.0,
                weight: 0.5,
            }],
            current_weights: BTreeMap::from([("QQQ".to_owned(), 0.5), ("SOXX".to_owned(), 0.0)]),
        }
    }

    #[test]
    fn debug_snapshot_is_empty_with_ten_thousand_dollars() {
        let snapshot =
            debug_account_snapshot(&["QQQ".to_owned(), "SOXX".to_owned()], 10_000.0).unwrap();
        assert_eq!(snapshot.cash, 10_000.0);
        assert_eq!(snapshot.equity, 10_000.0);
        assert_eq!(snapshot.buying_power, 10_000.0);
        assert!(snapshot.positions.is_empty());
        assert_eq!(snapshot.current_weights["QQQ"], 0.0);
        assert_eq!(snapshot.current_weights["SOXX"], 0.0);
        assert!(snapshot.simulated);
    }

    #[test]
    fn zero_position_debug_plan_uses_buy_notional() {
        let account =
            debug_account_snapshot(&["QQQ".to_owned(), "SOXX".to_owned()], 10_000.0).unwrap();
        let allocation = json!({
            "weights": {
                "QQQ": {"weight": 0.4},
                "SOXX": {"weight": 0.2},
                "cash_hedge": {"weight": 0.4}
            }
        });
        let market = json!({
            "per_ticker": {
                "QQQ": {"latest_close": 500.0},
                "SOXX": {"latest_close": 250.0}
            }
        });
        let plan = build_order_plan("run-1", &allocation, Some(&market), &account).unwrap();
        assert_eq!(plan.orders.len(), 2);
        assert_eq!(plan.orders[0].notional, Some(4_000.0));
        assert_eq!(plan.orders[0].estimated_qty, Some(8.0));
        assert_eq!(plan.orders[1].notional, Some(2_000.0));
        assert_eq!(plan.orders[1].estimated_qty, Some(8.0));
    }

    #[test]
    fn rebalance_sells_before_buying_and_never_oversells() {
        let allocation = json!({
            "weights": {
                "QQQ": {"weight": 0.2},
                "SOXX": {"weight": 0.3},
                "cash_hedge": {"weight": 0.5}
            }
        });
        let market = json!({"per_ticker": {"SOXX": {"latest_close": 250.0}}});
        let plan = build_order_plan(
            "run-2",
            &allocation,
            Some(&market),
            &account_with_position(),
        )
        .unwrap();
        assert_eq!(plan.orders[0].symbol, "QQQ");
        assert_eq!(plan.orders[0].side, "sell");
        assert_eq!(plan.orders[0].qty, Some(6.0));
        assert_eq!(plan.orders[1].symbol, "SOXX");
        assert_eq!(plan.orders[1].side, "buy");
        assert_eq!(plan.orders[1].notional, Some(3_000.0));
    }

    #[test]
    fn parses_alpaca_numeric_strings_and_weights() {
        let account = json!({
            "status": "ACTIVE",
            "cash": "4000",
            "equity": "10000",
            "buying_power": "8000",
            "trading_blocked": false
        });
        let positions = json!([{
            "symbol": "QQQ",
            "qty": "12",
            "market_value": "6000",
            "current_price": "500"
        }]);
        let snapshot =
            parse_account_snapshot(&account, &positions, &["QQQ".to_owned(), "SOXX".to_owned()])
                .unwrap();
        assert_eq!(snapshot.current_weights["QQQ"], 0.6);
        assert_eq!(snapshot.current_weights["SOXX"], 0.0);
        assert_eq!(snapshot.positions[0].qty, 12.0);
    }

    #[tokio::test]
    async fn debug_submission_returns_simulated_fills() {
        let account = debug_account_snapshot(&["QQQ".to_owned()], 10_000.0).unwrap();
        let plan = build_order_plan(
            "run-3",
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.5},
                    "cash_hedge": {"weight": 0.5}
                }
            }),
            Some(&json!({"per_ticker": {"QQQ": {"latest_close": 500.0}}})),
            &account,
        )
        .unwrap();
        let report = submit_order_plan(&plan, &account, None, true)
            .await
            .unwrap();
        assert_eq!(report.status, "simulated_filled");
        assert_eq!(report.receipts[0].filled_qty, Some(10.0));
        assert!(report.receipts[0].simulated);
    }
}
