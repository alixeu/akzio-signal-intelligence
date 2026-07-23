use super::{api_tool_name, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

pub const GET_PORTFOLIO_NAME: &str = "alpaca_get_portfolio";
pub const GET_HISTORY_NAME: &str = "alpaca_get_history";
pub const GET_PRICE_NAME: &str = "alpaca_get_price";
pub const SUBMIT_TRADE_NAME: &str = "alpaca_submit_trade";

const TRADING_BASE_URL: &str = "https://paper-api.alpaca.markets/v2";
const MARKET_DATA_BASE_URL: &str = "https://data.alpaca.markets";
pub fn get_portfolio_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(GET_PORTFOLIO_NAME),
        description: "Read the authenticated Alpaca paper account cash, buying power, open positions, and unrealized PnL. Call this before sizing or submitting a trade.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn get_history_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(GET_HISTORY_NAME),
        description: "Read the authenticated Alpaca paper account, positions, and recent fills. Phase 0 keeps attribution restricted to orders this project recorded locally. This tool is read-only.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn get_price_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(GET_PRICE_NAME),
        description: "Read the latest Alpaca market trade for a US stock or crypto symbol. Use it with account cash and the upstream position cap to calculate a valid quantity before submitting a trade.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Uppercase ticker or crypto symbol, e.g. QQQ, SOXX, TQQQ, BTC."
                },
                "market": {
                    "type": "string",
                    "enum": ["us-stock", "crypto"]
                }
            },
            "required": ["symbol", "market"],
            "additionalProperties": false
        }),
    }
}

pub fn submit_trade_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(SUBMIT_TRADE_NAME),
        description: "Submit one authenticated Alpaca paper market order. Only call after reading the portfolio and current price. Quantity must respect the Phase 4 position_size and the strictest Phase 5 position cap; never trade a Hold/wait/downgrade decision.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "market": {
                    "type": "string",
                    "enum": ["us-stock", "crypto"]
                },
                "action": {
                    "type": "string",
                    "enum": ["buy", "sell", "short", "cover"]
                },
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol. Use only an upstream investable symbol."
                },
                "price": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Reference price used for sizing and local audit. Alpaca receives a paper market order."
                },
                "quantity": {
                    "type": "number",
                    "exclusiveMinimum": 0
                },
                "content": {
                    "type": "string",
                    "description": "Concise Phase 6 rationale; do not include secrets."
                },
                "executed_at": {
                    "type": "string",
                    "description": "Use now for platform simulated execution, or an ISO 8601 timestamp for an external trade sync."
                }
            },
            "required": ["market", "action", "symbol", "price", "quantity", "executed_at"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceArgs {
    symbol: String,
    market: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeArgs {
    market: String,
    action: String,
    symbol: String,
    price: f64,
    quantity: f64,
    #[serde(default)]
    content: Option<String>,
    executed_at: String,
}

pub async fn get_portfolio(config: &ExternalToolConfig) -> Result<Value> {
    let (client, credentials) = live_client(config)?;
    let account = response_json(
        authenticated(
            client.get(format!("{TRADING_BASE_URL}/account")),
            &credentials,
        )
        .send()
        .await
        .context("Alpaca account request failed")?,
    )
    .await?;
    let positions = response_json(
        authenticated(
            client.get(format!("{TRADING_BASE_URL}/positions")),
            &credentials,
        )
        .send()
        .await
        .context("Alpaca positions request failed")?,
    )
    .await?;
    let open_positions = normalized_positions(&positions);
    let unrealized_pnl = open_positions
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|position| value_as_f64(position.get("unrealized_pl")))
        .sum::<f64>();
    let result = json!({
        "status": "success",
        "source": "alpaca",
        "cash": number_or_null(account.get("cash")),
        "buying_power": number_or_null(account.get("buying_power")),
        "equity": number_or_null(account.get("equity")),
        "last_equity": number_or_null(account.get("last_equity")),
        "unrealized_pnl": unrealized_pnl,
        "positions": open_positions
    });
    persist_account_snapshot(config, &result)?;
    Ok(result)
}

pub async fn get_history(config: &ExternalToolConfig) -> Result<Value> {
    let (client, credentials) = live_client(config)?;
    let fills = response_json(
        authenticated(
            client
                .get(format!("{TRADING_BASE_URL}/account/activities"))
                .query(&[
                    ("activity_types", "FILL"),
                    ("direction", "desc"),
                    ("page_size", "100"),
                ]),
            &credentials,
        )
        .send()
        .await
        .context("Alpaca account activities request failed")?,
    )
    .await?;
    let portfolio = get_portfolio(config).await?;
    Ok(json!({
        "status": "success",
        "source": "alpaca",
        "portfolio": portfolio,
        "fills": fills,
        "locally_imported_execution_count": 0,
        "attribution_note": "Remote Alpaca fills are observational only. Phase 0 attribution remains limited to this project's locally recorded order IDs."
    }))
}

pub async fn get_price(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args =
        serde_json::from_value::<PriceArgs>(args).context("invalid alpaca_get_price arguments")?;
    let symbol = normalized_symbol(&args.symbol)?;
    validate_allowed_symbol(&symbol, config)?;
    if !matches!(args.market.as_str(), "us-stock" | "crypto") {
        bail!("Alpaca price market must be us-stock or crypto");
    }
    let (client, credentials) = live_client(config)?;
    let response = match args.market.as_str() {
        "us-stock" => {
            response_json(
                authenticated(
                    client
                        .get(format!(
                            "{MARKET_DATA_BASE_URL}/v2/stocks/{symbol}/trades/latest"
                        ))
                        .query(&[("feed", "iex")]),
                    &credentials,
                )
                .send()
                .await
                .context("Alpaca stock price request failed")?,
            )
            .await?
        }
        "crypto" => {
            response_json(
                authenticated(
                    client
                        .get(format!(
                            "{MARKET_DATA_BASE_URL}/v1beta3/crypto/us/latest/trades"
                        ))
                        .query(&[("symbols", symbol.as_str())]),
                    &credentials,
                )
                .send()
                .await
                .context("Alpaca crypto price request failed")?,
            )
            .await?
        }
        _ => unreachable!("market was validated"),
    };
    let trade = response
        .get("trade")
        .or_else(|| {
            response
                .get("trades")
                .and_then(|trades| trades.get(&symbol))
        })
        .context("Alpaca latest-trade response is missing trade data")?;
    let price = value_as_f64(trade.get("p").or_else(|| trade.get("price")))
        .context("Alpaca latest-trade response is missing a numeric price")?;
    Ok(json!({
        "status": "success",
        "source": "alpaca",
        "symbol": symbol,
        "market": args.market,
        "price": price,
        "timestamp": trade.get("t").or_else(|| trade.get("timestamp")).cloned().unwrap_or(Value::Null)
    }))
}

pub async fn submit_trade(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let mut args = serde_json::from_value::<TradeArgs>(args)
        .context("invalid alpaca_submit_trade arguments")?;
    args.symbol = normalized_symbol(&args.symbol)?;
    validate_allowed_symbol(&args.symbol, config)?;
    validate_trade(&args)?;
    let (client, credentials) = live_client(config)?;
    let response = response_json(
        authenticated(
            client.post(format!("{TRADING_BASE_URL}/orders")),
            &credentials,
        )
        .json(&json!({
            "symbol": args.symbol,
            "qty": args.quantity,
            "side": alpaca_side(&args.action),
            "type": "market",
            "time_in_force": if args.market == "crypto" { "gtc" } else { "day" }
        }))
        .send()
        .await
        .context("Alpaca paper order request failed")?,
    )
    .await?;
    if let Some(path) = config.db_path.as_deref() {
        let conn = orchestrator_sql::connect(path)?;
        orchestrator_sql::record_exact_execution(
            &conn,
            &orchestrator_sql::ExecutionRecord {
                run_id: config.run_id.as_deref(),
                ticker: &args.symbol,
                action: &args.action,
                quantity: args.quantity,
                requested_price: args.price,
                executed_at: &args.executed_at,
                response: &response,
            },
        )?;
    }
    Ok(json!({
        "status": "submitted",
        "source": "alpaca",
        "order": response,
        "reference_price": args.price,
        "rationale": args.content
    }))
}

fn persist_account_snapshot(config: &ExternalToolConfig, payload: &Value) -> Result<()> {
    let Some(path) = config.db_path.as_deref() else {
        return Ok(());
    };
    let conn = orchestrator_sql::connect(path)?;
    orchestrator_sql::record_account_snapshot(
        &conn,
        config.run_id.as_deref(),
        config.phase.unwrap_or(0),
        payload,
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
struct Credentials {
    api_key: String,
    api_secret: String,
}

fn live_client(config: &ExternalToolConfig) -> Result<(Client, Credentials)> {
    if !config.alpaca_live {
        bail!("Alpaca tools are disabled for mock or debug execution");
    }
    let api_key = config
        .alpaca_api_key
        .clone()
        .filter(|value| !value.trim().is_empty())
        .context("orchestrator.alpaca.api_key is required for live Alpaca tools")?;
    let api_secret = config
        .alpaca_api_secret
        .clone()
        .filter(|value| !value.trim().is_empty())
        .context("orchestrator.alpaca.api_secret is required for live Alpaca tools")?;
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to create Alpaca HTTP client")?;
    Ok((
        client,
        Credentials {
            api_key,
            api_secret,
        },
    ))
}

fn authenticated(
    request: reqwest::RequestBuilder,
    credentials: &Credentials,
) -> reqwest::RequestBuilder {
    request
        .header("APCA-API-KEY-ID", &credentials.api_key)
        .header("APCA-API-SECRET-KEY", &credentials.api_secret)
}

fn normalized_symbol(symbol: &str) -> Result<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 128
        || !symbol
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':' | '/'))
    {
        bail!("invalid Alpaca symbol");
    }
    Ok(symbol)
}

fn validate_allowed_symbol(symbol: &str, config: &ExternalToolConfig) -> Result<()> {
    if config
        .tickers
        .iter()
        .any(|ticker| ticker.eq_ignore_ascii_case(symbol))
    {
        Ok(())
    } else {
        bail!("Alpaca symbol {symbol} is not in the configured investable assets")
    }
}

fn validate_trade(args: &TradeArgs) -> Result<()> {
    if !matches!(args.market.as_str(), "us-stock" | "crypto") {
        bail!("unsupported Alpaca market");
    }
    if !matches!(args.action.as_str(), "buy" | "sell" | "short" | "cover") {
        bail!("unsupported Alpaca action");
    }
    if !args.price.is_finite() || args.price < 0.0 {
        bail!("Alpaca reference price must be finite and non-negative");
    }
    if !args.quantity.is_finite() || args.quantity <= 0.0 {
        bail!("Alpaca order quantity must be finite and positive");
    }
    if args.executed_at.trim().is_empty() || args.executed_at.len() > 64 {
        bail!("Alpaca executed_at must be now or an ISO 8601 timestamp");
    }
    if args
        .content
        .as_deref()
        .is_some_and(|value| value.len() > 1000)
    {
        bail!("Alpaca trade content exceeds 1000 characters");
    }
    Ok(())
}

fn alpaca_side(action: &str) -> &'static str {
    match action {
        "buy" | "cover" => "buy",
        "sell" | "short" => "sell",
        _ => unreachable!("action was validated"),
    }
}

fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
}

fn number_or_null(value: Option<&Value>) -> Value {
    value_as_f64(value).map_or(Value::Null, Value::from)
}

fn normalized_positions(payload: &Value) -> Value {
    let positions = payload
        .as_array()
        .or_else(|| payload.get("positions").and_then(Value::as_array));
    Value::Array(
        positions
            .into_iter()
            .flatten()
            .map(|position| {
                json!({
                    "symbol": position.get("symbol").cloned().unwrap_or(Value::Null),
                    "quantity": number_or_null(position.get("qty").or_else(|| position.get("quantity"))),
                    "current_price": number_or_null(position.get("current_price")),
                    "market_value": number_or_null(position.get("market_value")),
                    "unrealized_pnl": number_or_null(position.get("unrealized_pl").or_else(|| position.get("unrealized_pnl"))),
                })
            })
            .collect(),
    )
}

async fn response_json(response: Response) -> Result<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read Alpaca response")?;
    if !status.is_success() {
        bail!(
            "Alpaca returned HTTP {}: {}",
            status.as_u16(),
            super::truncate_chars(&text, 2000)
        );
    }
    serde_json::from_str(&text).context("Alpaca returned invalid JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_gate_blocks_network_and_credentials() {
        let error = get_portfolio(&ExternalToolConfig::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mock or debug"));
    }

    #[test]
    fn live_client_requires_token_from_tool_config() {
        let config = ExternalToolConfig {
            alpaca_live: true,
            ..Default::default()
        };
        let error = live_client(&config).unwrap_err();
        assert!(error.to_string().contains("orchestrator.alpaca.api_key"));
    }

    #[test]
    fn rejects_invalid_trade_numbers() {
        let trade = TradeArgs {
            market: "us-stock".to_string(),
            action: "buy".to_string(),
            symbol: "QQQ".to_string(),
            price: 0.0,
            quantity: f64::NAN,
            content: None,
            executed_at: "now".to_string(),
        };
        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_non_investable_symbol() {
        let config = ExternalToolConfig {
            tickers: vec!["QQQ".to_string(), "SOXX".to_string()],
            ..Default::default()
        };
        assert!(validate_allowed_symbol("VIX", &config).is_err());
    }

    #[test]
    fn supports_alpaca_crypto_symbols_and_side_mapping() {
        assert_eq!(normalized_symbol("btc/usd").unwrap(), "BTC/USD");
        assert_eq!(alpaca_side("short"), "sell");
        assert_eq!(alpaca_side("cover"), "buy");
    }

    #[test]
    fn normalizes_alpaca_array_positions_and_numeric_strings() {
        let positions = normalized_positions(&json!([{
            "symbol": "QQQ",
            "qty": "3",
            "current_price": "500.25",
            "market_value": "1500.75",
            "unrealized_pl": "10.5"
        }]));
        assert_eq!(
            positions,
            json!([{
                "symbol": "QQQ",
                "quantity": 3.0,
                "current_price": 500.25,
                "market_value": 1500.75,
                "unrealized_pnl": 10.5
            }])
        );
    }
}
