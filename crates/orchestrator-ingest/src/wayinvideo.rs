use anyhow::Result;
use clap::Args;
use orchestrator_core::{config_int, config_str};
use reqwest::Client;
use serde_json::{json, Value};
use std::{fs, path::PathBuf, time::Duration};

#[derive(Debug, Clone, Args)]
pub struct WayinVideoArgs {
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub title: Option<String>,
    #[arg(long)]
    pub published: Option<String>,
    #[arg(long)]
    pub task: Option<String>,
    #[arg(long)]
    pub task_id: Option<String>,
    #[arg(long)]
    pub output: Option<String>,
}

pub async fn run(args: WayinVideoArgs) -> Result<Value> {
    let args = ResolvedWayinVideoArgs::from_args(args);
    let result = if !args.task_id.is_empty() {
        fetch_existing_task(&args).await?
    } else if !args.password.is_empty() {
        start_and_fetch_task(&args).await?
    } else {
        json!({
            "status": "login_required",
            "message": "WAYINVIDEO_PASSWORD is not set; live WayinVideo fetch is disabled.",
            "url": args.url,
            "title": args.title,
            "published": args.published,
            "task": args.task
        })
    };
    if !args.output.is_empty() {
        let path = PathBuf::from(&args.output);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&result)?)?;
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct ResolvedWayinVideoArgs {
    url: String,
    title: String,
    published: String,
    task: String,
    task_id: String,
    output: String,
    #[allow(dead_code)]
    username: String,
    password: String,
    existing_task_url: String,
    timeout_sec: u64,
}

impl ResolvedWayinVideoArgs {
    fn from_args(args: WayinVideoArgs) -> Self {
        let config = crate::config::load_default_config();
        Self {
            url: args.url,
            title: args.title.unwrap_or_default(),
            published: args.published.unwrap_or_default(),
            task: args
                .task
                .unwrap_or_else(|| config_str(&config, "wayinvideo.task", "视频逐字稿")),
            task_id: args.task_id.unwrap_or_default(),
            output: args.output.unwrap_or_default(),
            existing_task_url: config_str(
                &config,
                "wayinvideo.existing_task_url",
                "https://wayinvideo-api.wayin.ai/api/highlight_moment/task",
            ),
            timeout_sec: config_int(&config, "wayinvideo.timeout_sec", 60) as u64,
            username: config_str(&config, "wayinvideo.username", ""),
            password: config_str(&config, "wayinvideo.password", ""),
        }
    }
}

async fn fetch_existing_task(args: &ResolvedWayinVideoArgs) -> Result<Value> {
    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout_sec))
        .build()?;
    let payload: Value = client
        .get(&args.existing_task_url)
        .query(&[("id", args.task_id.as_str())])
        .send()
        .await?
        .json()
        .await
        .unwrap_or_else(|_| json!({}));
    Ok(json!({
        "status": "success",
        "url": args.url,
        "title": args.title,
        "published": args.published,
        "task_id": args.task_id,
        "payload": payload
    }))
}

async fn start_and_fetch_task(args: &ResolvedWayinVideoArgs) -> Result<Value> {
    Ok(json!({
        "status": "not_implemented",
        "message": "Rust API path is available for existing task IDs; browser login fallback is intentionally live-gated.",
        "url": args.url,
        "title": args.title,
        "published": args.published,
        "task": args.task
    }))
}
