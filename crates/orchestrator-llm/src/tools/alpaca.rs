use super::{api_tool_name, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

pub const GET_NEWS_NAME: &str = "alpaca_get_news";

const MARKET_DATA_BASE_URL: &str = "https://data.alpaca.markets";

pub fn get_news_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(GET_NEWS_NAME),
        description: "Read authenticated Alpaca news for configured research symbols. Returns timestamped headlines, summaries, sources, URLs, and optional article content. Use it as a market-news source and verify material claims against primary sources when available.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "One or more configured research symbols, e.g. [\"QQQ\", \"SOXX\"]."
                },
                "start": {
                    "type": "string",
                    "description": "Optional inclusive RFC-3339 or YYYY-MM-DD start."
                },
                "end": {
                    "type": "string",
                    "description": "Optional inclusive RFC-3339 or YYYY-MM-DD end."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 50,
                    "default": 10
                },
                "include_content": {
                    "type": "boolean",
                    "default": false
                }
            },
            "required": ["symbols"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NewsArgs {
    symbols: Vec<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_news_limit")]
    limit: u8,
    #[serde(default)]
    include_content: bool,
}

pub async fn get_news(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args =
        serde_json::from_value::<NewsArgs>(args).context("invalid alpaca_get_news arguments")?;
    if args.symbols.is_empty() || args.limit == 0 || args.limit > 50 {
        bail!("Alpaca news requires 1-50 results and at least one symbol");
    }
    let symbols = args
        .symbols
        .iter()
        .map(|symbol| normalized_symbol(symbol))
        .collect::<Result<Vec<_>>>()?;
    for symbol in &symbols {
        validate_allowed_symbol(symbol, config)?;
    }
    for (name, value) in [
        ("start", args.start.as_deref()),
        ("end", args.end.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty() || value.len() > 64) {
            bail!("Alpaca news {name} must be RFC-3339 or YYYY-MM-DD");
        }
    }
    let (client, credentials) = market_data_client(config)?;
    let mut query = vec![
        ("symbols", symbols.join(",")),
        ("limit", args.limit.to_string()),
        ("sort", "desc".to_string()),
        ("include_content", args.include_content.to_string()),
    ];
    if let Some(start) = args.start {
        query.push(("start", start));
    }
    if let Some(end) = args.end {
        query.push(("end", end));
    }
    let response = response_json(
        authenticated(
            client
                .get(format!("{MARKET_DATA_BASE_URL}/v1beta1/news"))
                .query(&query),
            &credentials,
        )
        .send()
        .await
        .context("Alpaca news request failed")?,
    )
    .await?;
    Ok(json!({
        "status": "success",
        "source": "alpaca_news",
        "symbols": symbols,
        "news": response.get("news").cloned().unwrap_or_else(|| json!([])),
        "next_page_token": response.get("next_page_token").cloned().unwrap_or(Value::Null)
    }))
}

#[derive(Debug, Clone)]
struct Credentials {
    api_key: String,
    api_secret: String,
}

fn market_data_client(config: &ExternalToolConfig) -> Result<(Client, Credentials)> {
    if !config.alpaca_market_data {
        bail!("Alpaca market-data tools are disabled for mock or debug execution");
    }
    authenticated_client(config)
}

fn authenticated_client(config: &ExternalToolConfig) -> Result<(Client, Credentials)> {
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

const fn default_news_limit() -> u8 {
    10
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
        bail!("Alpaca symbol {symbol} is not in this role's configured symbol universe")
    }
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
    async fn market_data_gate_blocks_news_network_and_credentials() {
        let config = ExternalToolConfig {
            tickers: vec!["QQQ".to_string()],
            ..Default::default()
        };
        let error = get_news(json!({"symbols": ["QQQ"]}), &config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mock or debug"));
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
    fn supports_alpaca_symbols() {
        assert_eq!(normalized_symbol("btc/usd").unwrap(), "BTC/USD");
    }
}
