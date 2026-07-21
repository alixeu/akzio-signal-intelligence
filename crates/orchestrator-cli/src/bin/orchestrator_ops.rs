use anyhow::{bail, Context, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use orchestrator_cli::{
    init_tracing,
    memory_promote::{promote_memories, PromoteMode, PromoteOptions},
    reflection_score::{score_predictions, ScoreOptions},
    weekly_distill::{distill_weekly, DistillOptions},
};
use serde_json::{json, Value};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "orchestrator-ops", about = "Unified operations CLI")]
struct Cli {
    #[command(subcommand)]
    command: OpsCommand,
}

#[derive(Subcommand)]
enum OpsCommand {
    /// Score expired reflection predictions against stored Close prices
    ReflectionScore {
        #[arg(long)]
        db_path: Option<PathBuf>,
        #[arg(long)]
        as_of: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, default_value = "1d")]
        interval: String,
    },
    /// Distill scored reflection outcomes into candidate experiences
    WeeklyDistill {
        #[arg(long)]
        db_path: Option<PathBuf>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long, default_value_t = 3)]
        min_samples: usize,
    },
    /// Promote candidate experiences into long-term memory
    MemoryPromote {
        #[arg(long)]
        db_path: Option<PathBuf>,
        #[arg(long, default_value = "auto")]
        mode: String,
        #[arg(long, default_value_t = 0.6)]
        min_quality: f64,
        #[arg(long, default_value_t = 5)]
        min_samples: usize,
        #[arg(long, default_value_t = 0.6)]
        min_confidence: f64,
    },
    /// Probe LLM gateway strict-schema support
    ProbeStrictSchema,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        OpsCommand::ReflectionScore {
            db_path,
            as_of,
            limit,
            interval,
        } => {
            let db_path = resolve_db_path(db_path);
            let as_of = as_of.unwrap_or_else(|| Utc::now().date_naive().to_string());
            let conn = orchestrator_sql::connect(&db_path)?;
            let summary = score_predictions(
                &conn,
                &ScoreOptions {
                    as_of,
                    limit,
                    interval,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        OpsCommand::WeeklyDistill {
            db_path,
            since,
            until,
            min_samples,
        } => {
            let db_path = resolve_db_path(db_path);
            let until = until.unwrap_or_else(|| Utc::now().date_naive().to_string());
            let since =
                since.unwrap_or_else(|| (Utc::now().date_naive() - Duration::days(7)).to_string());
            let conn = orchestrator_sql::connect(&db_path)?;
            let summary = distill_weekly(
                &conn,
                &DistillOptions {
                    since,
                    until,
                    min_samples,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        OpsCommand::MemoryPromote {
            db_path,
            mode,
            min_quality,
            min_samples,
            min_confidence,
        } => {
            let db_path = resolve_db_path(db_path);
            let conn = orchestrator_sql::connect(&db_path)?;
            let summary = promote_memories(
                &conn,
                &PromoteOptions {
                    mode: PromoteMode::parse(&mode),
                    min_quality: min_quality.clamp(0.0, 1.0),
                    min_samples,
                    min_confidence: min_confidence.clamp(0.0, 1.0),
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        OpsCommand::ProbeStrictSchema => {
            run_probe_strict_schema().await?;
        }
    }
    Ok(())
}

fn resolve_db_path(explicit: Option<PathBuf>) -> PathBuf {
    explicit
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"))
}

async fn run_probe_strict_schema() -> Result<()> {
    let base_url = std::env::var("LLM_GATEWAY_BASE_URL")
        .unwrap_or_else(|_| "https://oneapi-comate.baidu-int.com/v1".to_string());
    let api_key = std::env::var("LLM_GATEWAY_API_KEY")
        .context("LLM_GATEWAY_API_KEY must be set to probe the gateway")?;
    if api_key.trim().is_empty() {
        bail!("LLM_GATEWAY_API_KEY is empty");
    }
    let model = std::env::var("LLM_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string());
    let endpoint = format!("{}/responses", base_url.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    println!("Probing gateway strict-schema support");
    println!("  endpoint: {endpoint}");
    println!("  model:    {model}\n");

    let plain = json!({
        "model": model,
        "input": "Reply with the single word: ok",
    });
    let baseline = probe_post(&client, &endpoint, &api_key, &plain).await;
    match &baseline {
        Ok((status, _)) if status.is_success() => {
            println!("[1/2] baseline plain request: OK ({status})");
        }
        Ok((status, body)) => {
            println!("[1/2] baseline plain request: FAILED ({status})");
            println!("      body: {}", truncate_str(body, 500));
            println!("\nGateway/creds/model do not work even for a plain request; fix that before judging strict-schema support.");
            return Ok(());
        }
        Err(e) => {
            println!("[1/2] baseline plain request: transport error: {e}");
            return Ok(());
        }
    }

    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "direction": {"type": "string", "enum": ["bullish", "bearish", "neutral"]},
            "confidence": {"type": "number"}
        },
        "required": ["direction", "confidence"]
    });
    let strict = json!({
        "model": model,
        "input": "Return a JSON object with direction and confidence for a neutral test.",
        "text": {
            "format": {
                "type": "json_schema",
                "name": "probe_schema",
                "strict": true,
                "schema": schema
            }
        }
    });
    match probe_post(&client, &endpoint, &api_key, &strict).await {
        Ok((status, body)) if status.is_success() => {
            println!("[2/2] strict json_schema request: ACCEPTED ({status})");
            println!("\nVERDICT: gateway appears to SUPPORT strict structured output.");
            if let Some(sample) = extract_probe_output_text(&body) {
                println!("  sample output: {}", truncate_str(&sample, 200));
            }
        }
        Ok((status, body)) => {
            println!("[2/2] strict json_schema request: REJECTED ({status})");
            println!("      body: {}", truncate_str(&body, 800));
            println!("\nVERDICT: gateway does NOT accept this strict-schema shape.");
        }
        Err(e) => {
            println!("[2/2] strict json_schema request: transport error: {e}");
        }
    }

    Ok(())
}

async fn probe_post(
    client: &reqwest::Client,
    endpoint: &str,
    api_key: &str,
    body: &Value,
) -> Result<(reqwest::StatusCode, String)> {
    let resp = client
        .post(endpoint)
        .bearer_auth(api_key)
        .json(body)
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

fn extract_probe_output_text(body: &str) -> Option<String> {
    let value: Value = serde_json::from_str(body).ok()?;
    value
        .get("output")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("content"))
        .and_then(Value::as_array)
        .and_then(|c| c.first())
        .and_then(|c| c.get("text"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn truncate_str(text: &str, max: usize) -> String {
    if text.len() <= max {
        text.to_string()
    } else {
        format!("{}…", &text[..max])
    }
}
