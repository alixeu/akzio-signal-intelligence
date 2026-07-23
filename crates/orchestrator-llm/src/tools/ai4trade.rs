use super::{api_tool_name, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

pub const GET_PORTFOLIO_NAME: &str = "ai4trade_get_portfolio";
pub const GET_HISTORY_NAME: &str = "ai4trade_get_history";
pub const GET_PRICE_NAME: &str = "ai4trade_get_price";
pub const SUBMIT_TRADE_NAME: &str = "ai4trade_submit_trade";

const BASE_URL: &str = "https://ai4trade.ai/api";
pub fn get_portfolio_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(GET_PORTFOLIO_NAME),
        description: "Read the authenticated AI4Trade simulated account cash, points, open positions, and unrealized PnL. Call this before sizing or submitting a trade.".to_string(),
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
        description: "Read this project's authenticated AI4Trade account, current positions, and the agent's documented signal history for Phase 0 outcome attribution. This tool is read-only.".to_string(),
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
        description: "Read the current AI4Trade market price for a US stock or crypto symbol. Use it with account cash and the upstream position cap to calculate a valid quantity before submitting a trade.".to_string(),
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
        description: "Submit one authenticated AI4Trade simulated trade. Only call after reading the portfolio and current price. Quantity must respect the Phase 4 position_size and the strictest Phase 5 position cap; never trade a Hold/wait/downgrade decision.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "market": {
                    "type": "string",
                    "enum": ["us-stock", "crypto", "polymarket"]
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
                    "description": "Use 0 with executed_at=now for an AI4Trade simulated market-price execution."
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
    let (client, token) = live_client(config)?;
    let account = response_json(
        client
            .get(format!("{BASE_URL}/claw/agents/me"))
            .bearer_auth(&token)
            .send()
            .await
            .context("AI4Trade account request failed")?,
    )
    .await?;
    let positions = response_json(
        client
            .get(format!("{BASE_URL}/positions"))
            .bearer_auth(&token)
            .send()
            .await
            .context("AI4Trade positions request failed")?,
    )
    .await?;
    let open_positions = positions
        .get("positions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let unrealized_pnl = open_positions
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|position| position.get("pnl").and_then(Value::as_f64))
        .sum::<f64>();
    let result = json!({
        "status": "success",
        "source": "ai4trade",
        "cash": account.get("cash").or_else(|| positions.get("cash")).cloned().unwrap_or(Value::Null),
        "points": account.get("points").cloned().unwrap_or(Value::Null),
        "unrealized_pnl": unrealized_pnl,
        "positions": open_positions
    });
    persist_account_snapshot(config, &result)?;
    Ok(result)
}

pub async fn get_history(config: &ExternalToolConfig) -> Result<Value> {
    let (client, token) = live_client(config)?;
    let account = response_json(
        client
            .get(format!("{BASE_URL}/claw/agents/me"))
            .bearer_auth(&token)
            .send()
            .await
            .context("AI4Trade account request failed")?,
    )
    .await?;
    let agent_id = account
        .get("id")
        .and_then(|value| {
            value
                .as_i64()
                .map(|value| value.to_string())
                .or_else(|| value.as_str().map(ToString::to_string))
        })
        .context("AI4Trade account response is missing id")?;
    let signals = response_json(
        client
            .get(format!("{BASE_URL}/signals/{agent_id}"))
            .bearer_auth(&token)
            .query(&[("limit", "200")])
            .send()
            .await
            .context("AI4Trade signal history request failed")?,
    )
    .await?;
    let portfolio = get_portfolio(config).await?;
    let imported = if let Some(path) = config.db_path.as_deref() {
        let conn = orchestrator_sql::connect(path)?;
        orchestrator_sql::import_legacy_executions(&conn, &signals, config.run_id.as_deref())?
    } else {
        0
    };
    Ok(json!({
        "status": "success",
        "source": "ai4trade",
        "agent_id": agent_id,
        "portfolio": portfolio,
        "signals": signals.get("signals").cloned().unwrap_or(signals),
        "locally_imported_execution_count": imported
    }))
}

pub async fn get_price(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args = serde_json::from_value::<PriceArgs>(args)
        .context("invalid ai4trade_get_price arguments")?;
    let symbol = normalized_symbol(&args.symbol)?;
    validate_allowed_symbol(&symbol, config)?;
    if !matches!(args.market.as_str(), "us-stock" | "crypto") {
        bail!("AI4Trade price market must be us-stock or crypto");
    }
    let (client, token) = live_client(config)?;
    response_json(
        client
            .get(format!("{BASE_URL}/price"))
            .bearer_auth(token)
            .query(&[("symbol", symbol), ("market", args.market)])
            .send()
            .await
            .context("AI4Trade price request failed")?,
    )
    .await
}

pub async fn submit_trade(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let mut args = serde_json::from_value::<TradeArgs>(args)
        .context("invalid ai4trade_submit_trade arguments")?;
    args.symbol = normalized_symbol(&args.symbol)?;
    validate_allowed_symbol(&args.symbol, config)?;
    validate_trade(&args)?;
    let (client, token) = live_client(config)?;
    let response = response_json(
        client
            .post(format!("{BASE_URL}/signals/realtime"))
            .bearer_auth(token)
            .json(&json!({
                "market": args.market,
                "action": args.action,
                "symbol": args.symbol,
                "price": args.price,
                "quantity": args.quantity,
                "content": args.content,
                "executed_at": args.executed_at
            }))
            .send()
            .await
            .context("AI4Trade trade request failed")?,
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
    Ok(response)
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

fn live_client(config: &ExternalToolConfig) -> Result<(Client, String)> {
    if !config.ai4trade_live {
        bail!("AI4Trade tools are disabled for mock or debug execution");
    }
    let token = config
        .ai4trade_token
        .clone()
        .filter(|value| !value.trim().is_empty())
        .context("orchestrator.ai4trade.token is required for live AI4Trade tools")?;
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to create AI4Trade HTTP client")?;
    Ok((client, token))
}

fn normalized_symbol(symbol: &str) -> Result<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 128
        || !symbol
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
    {
        bail!("invalid AI4Trade symbol");
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
        bail!("AI4Trade symbol {symbol} is not in the configured investable assets")
    }
}

fn validate_trade(args: &TradeArgs) -> Result<()> {
    if !matches!(args.market.as_str(), "us-stock" | "crypto" | "polymarket") {
        bail!("unsupported AI4Trade market");
    }
    if !matches!(args.action.as_str(), "buy" | "sell" | "short" | "cover") {
        bail!("unsupported AI4Trade action");
    }
    if args.market == "polymarket" && !matches!(args.action.as_str(), "buy" | "sell") {
        bail!("AI4Trade polymarket trades only support buy or sell");
    }
    if !args.price.is_finite() || args.price < 0.0 {
        bail!("AI4Trade trade price must be finite and non-negative");
    }
    if !args.quantity.is_finite() || args.quantity <= 0.0 {
        bail!("AI4Trade trade quantity must be finite and positive");
    }
    if args.executed_at.trim().is_empty() || args.executed_at.len() > 64 {
        bail!("AI4Trade executed_at must be now or an ISO 8601 timestamp");
    }
    if args
        .content
        .as_deref()
        .is_some_and(|value| value.len() > 1000)
    {
        bail!("AI4Trade trade content exceeds 1000 characters");
    }
    Ok(())
}

async fn response_json(response: Response) -> Result<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read AI4Trade response")?;
    if !status.is_success() {
        bail!(
            "AI4Trade returned HTTP {}: {}",
            status.as_u16(),
            super::truncate_chars(&text, 2000)
        );
    }
    serde_json::from_str(&text).context("AI4Trade returned invalid JSON")
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
            ai4trade_live: true,
            ..Default::default()
        };
        let error = live_client(&config).unwrap_err();
        assert!(error.to_string().contains("orchestrator.ai4trade.token"));
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
}
