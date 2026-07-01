use anyhow::{Context, Result};
use chrono::{Duration, Local, NaiveDateTime, Timelike};
use clap::Args;
use orchestrator_core::{config_float, config_int, config_str};
use reqwest::Client;
use serde_json::{json, Value};
use std::{fs, path::PathBuf, time::Duration as StdDuration};

const API_URL: &str = "https://4a735ea38f8146198dc205d2e2d1bd28.z3c.jin10.com/flash";
const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Debug, Clone, Args, Default)]
pub struct Jin10Args {
    #[arg(long)]
    pub channel: Option<i64>,
    #[arg(long)]
    pub vip: Option<i64>,
    #[arg(long)]
    pub classify: Option<String>,
    #[arg(long)]
    pub lookback_hours: Option<f64>,
    #[arg(long)]
    pub pages: Option<usize>,
    #[arg(long)]
    pub sleep: Option<f64>,
    #[arg(long)]
    pub timeout: Option<f64>,
    #[arg(long, default_value = "")]
    pub output: String,
    #[arg(long, default_value = "")]
    pub jsonl: String,
    #[arg(long)]
    pub pretty: bool,
}

pub async fn run(args: Jin10Args) -> Result<Value> {
    let args = ResolvedJin10Args::from_args(args);
    let classify = parse_classify(&args.classify)?;
    let end_time = Local::now().naive_local().with_nanosecond(0).unwrap();
    let earliest_time =
        end_time - Duration::milliseconds((args.lookback_hours * 3_600_000.0) as i64);
    let mut cursor = end_time.format(TIME_FORMAT).to_string();
    let client = Client::builder()
        .timeout(StdDuration::from_secs_f64(args.timeout))
        .build()?;
    let mut seen = std::collections::BTreeSet::new();
    let mut collected = Vec::new();
    let jsonl_path = (!args.jsonl.is_empty()).then(|| PathBuf::from(&args.jsonl));
    if let Some(path) = &jsonl_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, "")?;
    }
    let mut pages_fetched = 0;
    for page_index in 0..args.pages {
        let items = fetch_page(&client, &args, &classify, &cursor).await?;
        if items.is_empty() {
            break;
        }
        let mut reached_end = false;
        for item in &items {
            let Some(item_time) = item
                .get("time")
                .and_then(Value::as_str)
                .and_then(parse_time)
            else {
                continue;
            };
            if item_time < earliest_time {
                reached_end = true;
                continue;
            }
            let content = item
                .pointer("/data/content")
                .and_then(Value::as_str)
                .unwrap_or("");
            if content.contains("VIP专享快讯，解锁直达") {
                continue;
            }
            let key = item
                .get("id")
                .map(Value::to_string)
                .unwrap_or_else(|| format!("{}|{}", item_time, content));
            if seen.insert(key) {
                let compact =
                    json!({"time": item_time.format(TIME_FORMAT).to_string(), "content": content});
                if let Some(path) = &jsonl_path {
                    use std::io::Write;
                    let mut file = fs::OpenOptions::new().append(true).open(path)?;
                    writeln!(file, "{}", serde_json::to_string(&compact)?)?;
                }
                collected.push(compact);
            }
        }
        pages_fetched += 1;
        if reached_end {
            break;
        }
        if let Some(next) = next_cursor(&items) {
            cursor = next;
        } else {
            break;
        }
        if args.sleep > 0.0 && page_index + 1 < args.pages {
            tokio::time::sleep(StdDuration::from_secs_f64(args.sleep)).await;
        }
    }
    collected.sort_by_key(|item| {
        item.get("time")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    });
    let result = json!({
        "status": "success",
        "channel": args.channel,
        "vip": args.vip,
        "classify": classify,
        "fetched_at": end_time.format(TIME_FORMAT).to_string(),
        "earliest_time": earliest_time.format(TIME_FORMAT).to_string(),
        "lookback_hours": args.lookback_hours,
        "pages_fetched": pages_fetched,
        "total_items": collected.len(),
        "items": collected
    });
    if !args.output.is_empty() {
        fs::write(&args.output, serde_json::to_string_pretty(&result)?)?;
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct ResolvedJin10Args {
    api_url: String,
    channel: i64,
    vip: i64,
    classify: String,
    lookback_hours: f64,
    pages: usize,
    sleep: f64,
    timeout: f64,
    output: String,
    jsonl: String,
}

impl ResolvedJin10Args {
    fn from_args(args: Jin10Args) -> Self {
        let config = crate::config::load_default_config();
        Self {
            api_url: config_str(&config, "jin10.api_url", API_URL),
            channel: args
                .channel
                .unwrap_or_else(|| config_int(&config, "jin10.channel", -8200)),
            vip: args
                .vip
                .unwrap_or_else(|| config_int(&config, "jin10.vip", 1)),
            classify: args.classify.unwrap_or_else(|| {
                config_str(
                    &config,
                    "jin10.classify",
                    "24,27,31,86,90,92,76,83,2,168,53,47,48,50,157,16",
                )
            }),
            lookback_hours: args
                .lookback_hours
                .unwrap_or_else(|| config_float(&config, "jin10.lookback_hours", 24.0)),
            pages: args
                .pages
                .unwrap_or_else(|| config_int(&config, "jin10.pages", 200) as usize),
            sleep: args
                .sleep
                .unwrap_or_else(|| config_float(&config, "jin10.sleep", 0.0)),
            timeout: args
                .timeout
                .unwrap_or_else(|| config_float(&config, "jin10.timeout", 15.0)),
            output: args.output,
            jsonl: args.jsonl,
        }
    }
}

fn parse_classify(value: &str) -> Result<Vec<i64>> {
    let items = value
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| item.trim().parse::<i64>().context("invalid classify id"))
        .collect::<Result<Vec<_>>>()?;
    if items.is_empty() {
        anyhow::bail!("classify cannot be empty");
    }
    Ok(items)
}

async fn fetch_page(
    client: &Client,
    args: &ResolvedJin10Args,
    classify: &[i64],
    cursor: &str,
) -> Result<Vec<Value>> {
    let payload: Value = client
        .get(&args.api_url)
        .header("x-app-id", "bVBF4FyRTn5NJF5n")
        .header("x-version", "1.0")
        .header("User-Agent", "Mozilla/5.0 Codex Jin10 Fetcher")
        .query(&[
            ("channel", args.channel.to_string()),
            ("vip", args.vip.to_string()),
            ("max_time", cursor.to_string()),
            ("classify", serde_json::to_string(classify)?),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if payload.get("status").and_then(Value::as_i64) != Some(200) {
        anyhow::bail!("unexpected jin10 status: {}", payload);
    }
    Ok(payload
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

fn parse_time(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, TIME_FORMAT).ok()
}

fn next_cursor(items: &[Value]) -> Option<String> {
    items
        .iter()
        .filter_map(|item| {
            item.get("time")
                .and_then(Value::as_str)
                .and_then(parse_time)
        })
        .min()
        .map(|value| {
            (value - Duration::seconds(1))
                .format(TIME_FORMAT)
                .to_string()
        })
}
