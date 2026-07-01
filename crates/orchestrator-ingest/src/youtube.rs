use anyhow::{Context, Result};
use chrono::{Duration, Local};
use clap::Args;
use orchestrator_core::{config_get, config_int, config_str};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::PathBuf, time::Duration as StdDuration};

#[derive(Debug, Clone, Args)]
pub struct YoutubeArgs {
    #[arg(long)]
    pub all: bool,
    #[arg(long)]
    pub channel: Option<String>,
    #[arg(long)]
    pub url: Option<String>,
    #[arg(long)]
    pub max_videos: Option<usize>,
    #[arg(long)]
    pub output: Option<String>,
}

pub async fn run(args: YoutubeArgs) -> Result<Value> {
    let config = crate::config::load_default_config();
    let mut channels = configured_channels(&config);
    let channel_name = args
        .channel
        .clone()
        .unwrap_or_else(|| config_str(&config, "youtube.default_channel", "rhino"));
    let max_videos = args
        .max_videos
        .unwrap_or_else(|| config_int(&config, "youtube.max_videos", 6) as usize);
    if let Some(url) = args.url.as_deref().filter(|value| !value.is_empty()) {
        channels.insert(
            channel_name.clone(),
            json!({
                "handle": channel_name,
                "display_name": channel_name,
                "source_url": url
            }),
        );
    }
    let keys: Vec<String> = if args.all {
        channels.keys().cloned().collect()
    } else {
        vec![channel_name.clone()]
    };
    let client = Client::builder()
        .timeout(StdDuration::from_secs(
            config_int(&config, "youtube.timeout_sec", 20) as u64,
        ))
        .user_agent(config_str(
            &config,
            "youtube.user_agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Codex TQQQ report",
        ))
        .build()?;
    let mut results = Vec::new();
    for key in keys {
        let channel = channels
            .get(&key)
            .with_context(|| format!("unknown channel {key}"))?;
        let url = channel
            .get("source_url")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                format!(
                    "https://www.youtube.com/@{}/videos",
                    channel
                        .get("handle")
                        .and_then(Value::as_str)
                        .unwrap_or(&key)
                )
            });
        let html = client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let videos = extract_videos(&html, max_videos);
        results.push(json!({
            "status": if videos.is_empty() { "empty" } else { "success" },
            "channel": key,
            "message": if videos.is_empty() { "no recent videos found" } else { "ok" },
            "videos": videos,
            "video": videos.first().cloned().unwrap_or(Value::Null)
        }));
    }
    let result = if args.all {
        json!({"status": "success", "channels": results})
    } else {
        results
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"status": "empty"}))
    };
    if let Some(output) = args.output.as_deref().filter(|value| !value.is_empty()) {
        let path = PathBuf::from(output);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&result)?)?;
    }
    Ok(result)
}

fn configured_channels(config: &Value) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    if let Some(channels) = config_get(config, "youtube.channels").and_then(Value::as_object) {
        for (key, value) in channels {
            map.insert(key.clone(), value.clone());
        }
    }
    if map.is_empty() {
        map.insert(
            "rhino".to_string(),
            json!({
                "handle": "rhinofinance",
                "display_name": "Rhino Finance",
                "source_url": "https://youtube.com/@rhinofinance",
                "language": ["zh-Hans", "zh-CN", "zh", "en"],
                "aliases": []
            }),
        );
    }
    map
}

fn extract_videos(html: &str, max_videos: usize) -> Vec<Value> {
    let mut videos = Vec::new();
    let id_re = Regex::new(r#""videoId":"([^"]+)""#).expect("valid regex");
    let title_re = Regex::new(r#""title":\{"runs":\[\{"text":"([^"]+)""#).expect("valid regex");
    let titles: Vec<String> = title_re
        .captures_iter(html)
        .filter_map(|cap| cap.get(1).map(|m| unescape(m.as_str())))
        .collect();
    let cutoff = Local::now() - Duration::days(3);
    let mut seen = std::collections::BTreeSet::new();
    for (index, cap) in id_re.captures_iter(html).enumerate() {
        let Some(video_id) = cap.get(1).map(|m| m.as_str().to_string()) else {
            continue;
        };
        if !seen.insert(video_id.clone()) {
            continue;
        }
        let title = titles
            .get(index)
            .cloned()
            .unwrap_or_else(|| video_id.clone());
        videos.push(json!({
            "video_id": video_id,
            "title": title,
            "published": cutoff.to_rfc3339(),
            "url": format!("https://www.youtube.com/watch?v={video_id}")
        }));
        if videos.len() >= max_videos {
            break;
        }
    }
    if videos.is_empty() {
        let doc = Html::parse_document(html);
        if let Ok(selector) = Selector::parse("a[href*=\"watch?v=\"]") {
            for node in doc.select(&selector).take(max_videos) {
                if let Some(href) = node.value().attr("href") {
                    if let Some(video_id) = href
                        .split("watch?v=")
                        .nth(1)
                        .map(|s| s.split('&').next().unwrap_or(s))
                    {
                        if seen.insert(video_id.to_string()) {
                            videos.push(json!({
                                "video_id": video_id,
                                "title": node.text().collect::<Vec<_>>().join(" ").trim(),
                                "published": cutoff.to_rfc3339(),
                                "url": format!("https://www.youtube.com/watch?v={video_id}")
                            }));
                        }
                    }
                }
            }
        }
    }
    videos
}

fn unescape(value: &str) -> String {
    value
        .replace("\\u0026", "&")
        .replace("\\\"", "\"")
        .replace("\\/", "/")
}
