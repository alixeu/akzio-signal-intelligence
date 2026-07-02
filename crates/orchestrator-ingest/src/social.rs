use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, ValueEnum};
use html_escape::decode_html_entities;
use regex::Regex;
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    time::Duration as StdDuration,
};

const REDDIT_RSS_URL: &str = "https://www.reddit.com/search.rss";
const XQUIK_SEARCH_URL: &str = "https://xquik.com/api/v1/x/tweets/search";
const YOUTUBE_SEARCH_URL: &str = "https://www.youtube.com/results";
const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Codex social last30days";

#[derive(Debug, Clone, Args, Default)]
pub struct SocialArgs {
    #[arg(long, value_enum)]
    pub source: Source,
    #[arg(long, default_value = "")]
    pub query: String,
    #[arg(long, value_delimiter = ',')]
    pub tickers: Vec<String>,
    #[arg(long, default_value_t = 7)]
    pub days: i64,
    #[arg(long, value_enum, default_value_t = Depth::Quick)]
    pub depth: Depth,
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long = "subreddit")]
    pub subreddits: Vec<String>,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum Source {
    #[default]
    Reddit,
    X,
    Youtube,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum Depth {
    Quick,
    #[default]
    #[value(alias = "default")]
    Balanced,
    Deep,
}

pub async fn run(args: SocialArgs) -> Result<Value> {
    validate_args(&args)?;
    let client = Client::builder()
        .timeout(StdDuration::from_secs(20))
        .user_agent(DEFAULT_USER_AGENT)
        .build()?;
    let result = if args.tickers.is_empty() {
        run_single(&client, &args).await?
    } else {
        run_per_ticker(&client, &args).await?
    };
    if let Some(path) = args.output.as_ref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&result)?)?;
    }
    Ok(result)
}

async fn run_single(client: &Client, args: &SocialArgs) -> Result<Value> {
    match args.source {
        Source::Reddit => run_reddit(client, args).await,
        Source::X => {
            run_x(
                client,
                args,
                std::env::var("XQUIK_API_KEY")
                    .ok()
                    .filter(|value| !value.trim().is_empty()),
            )
            .await
        }
        Source::Youtube => run_youtube(client, args).await,
    }
}

async fn run_per_ticker(client: &Client, args: &SocialArgs) -> Result<Value> {
    let mut per_ticker = serde_json::Map::new();
    let mut all_items = Vec::new();
    let mut data_gaps = Vec::new();
    for ticker in &args.tickers {
        let ticker = ticker.trim();
        if ticker.is_empty() {
            continue;
        }
        let mut ticker_args = args.clone();
        ticker_args.tickers = Vec::new();
        ticker_args.output = None;
        ticker_args.query = if args.query.trim().is_empty() {
            social_query_for_ticker(ticker)
        } else {
            args.query.replace("{ticker}", ticker)
        };
        let value = run_single(client, &ticker_args).await?;
        let items = value
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &items {
            let mut item = item.clone();
            item["ticker"] = Value::String(ticker.to_string());
            all_items.push(item);
        }
        for gap in value
            .get("data_gaps")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            data_gaps.push(format!("{ticker}: {gap}"));
        }
        per_ticker.insert(
            ticker.to_string(),
            json!({
                "status": value.get("status").cloned().unwrap_or(Value::String("unknown".to_string())),
                "query": ticker_args.query,
                "summary": summarize_items(args.source, &items),
                "items": items,
                "data_gaps": value.get("data_gaps").cloned().unwrap_or_else(|| json!([]))
            }),
        );
    }
    Ok(json!({
        "status": if all_items.is_empty() { "empty" } else { "success" },
        "source": source_name(args.source),
        "tickers": args.tickers,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "summary": summarize_items(args.source, &all_items),
        "data_gaps": data_gaps,
        "per_ticker": per_ticker,
        "items": all_items
    }))
}

fn validate_args(args: &SocialArgs) -> Result<()> {
    if args.query.trim().is_empty() && args.tickers.is_empty() {
        anyhow::bail!("either --query or --tickers is required");
    }
    if args.days <= 0 {
        anyhow::bail!("--days must be positive");
    }
    if args.limit == Some(0) {
        anyhow::bail!("--limit must be positive when provided");
    }
    Ok(())
}

async fn run_reddit(client: &Client, args: &SocialArgs) -> Result<Value> {
    let now = Utc::now();
    let limit = args.limit.unwrap_or_else(|| default_limit(args.depth));
    let rss_url = format!(
        "{REDDIT_RSS_URL}?q={}&sort=new&t=month",
        urlencoding::encode(args.query.trim())
    );
    let rss_response = match client.get(&rss_url).send().await {
        Ok(response) => response,
        Err(error) => {
            return Ok(unavailable_result(
                "reddit",
                args,
                now,
                format!("failed to fetch Reddit RSS search: {error}"),
            ));
        }
    };
    let rss_response = match rss_response.error_for_status() {
        Ok(response) => response,
        Err(error) => {
            return Ok(unavailable_result(
                "reddit",
                args,
                now,
                format!("Reddit RSS search returned an error status: {error}"),
            ));
        }
    };
    let rss_text = match rss_response.text().await {
        Ok(text) => text,
        Err(error) => {
            return Ok(unavailable_result(
                "reddit",
                args,
                now,
                format!("failed to read Reddit RSS response body: {error}"),
            ));
        }
    };
    let mut gaps = Vec::new();
    let mut items = parse_reddit_rss(&rss_text, now, args.days, &args.query);
    if items.is_empty() {
        gaps.push("reddit RSS search returned no date-filtered relevant items".to_string());
    }
    let allowed_subreddits = normalize_subreddit_filter(&args.subreddits);
    if !allowed_subreddits.is_empty() {
        items.retain(|item| {
            item.subreddit
                .as_deref()
                .map(normalize_subreddit_name)
                .is_some_and(|name| allowed_subreddits.contains(&name))
        });
    }
    items.sort_by(|a, b| {
        b.relevance
            .cmp(&a.relevance)
            .then_with(|| {
                b.score
                    .unwrap_or_default()
                    .cmp(&a.score.unwrap_or_default())
            })
            .then_with(|| {
                b.comments
                    .unwrap_or_default()
                    .cmp(&a.comments.unwrap_or_default())
            })
            .then_with(|| b.published.cmp(&a.published))
    });
    items.truncate(limit);
    let mut backfilled = 0usize;
    for item in &mut items {
        if let Some(url) = item.url.as_deref() {
            match fetch_reddit_listing_backfill(client, url).await {
                Ok(Some(backfill)) => {
                    item.apply_backfill(backfill);
                    backfilled += 1;
                }
                Ok(None) => {}
                Err(err) => gaps.push(format!(
                    "reddit listing backfill failed for {}: {}",
                    item.id.as_deref().unwrap_or(url),
                    err
                )),
            }
        }
    }
    items.sort_by(|a, b| {
        b.relevance
            .cmp(&a.relevance)
            .then_with(|| {
                b.score
                    .unwrap_or_default()
                    .cmp(&a.score.unwrap_or_default())
            })
            .then_with(|| {
                b.comments
                    .unwrap_or_default()
                    .cmp(&a.comments.unwrap_or_default())
            })
            .then_with(|| b.published.cmp(&a.published))
    });
    Ok(json!({
        "status": if items.is_empty() { "empty" } else { "success" },
        "source": "reddit",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "data_gaps": gaps,
        "rss_url": rss_url,
        "backfilled_items": backfilled,
        "items": items.into_iter().map(RedditItem::into_json).collect::<Vec<_>>()
    }))
}

async fn fetch_reddit_listing_backfill(
    client: &Client,
    url: &str,
) -> Result<Option<RedditListingBackfill>> {
    let html = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(parse_shreddit_listing(&html))
}

async fn run_x(client: &Client, args: &SocialArgs, api_key: Option<String>) -> Result<Value> {
    let now = Utc::now();
    let Some(api_key) = api_key else {
        return Ok(x_unavailable(args, now));
    };
    let limit = args.limit.unwrap_or_else(|| default_limit(args.depth));
    let response: Value = client
        .post(XQUIK_SEARCH_URL)
        .bearer_auth(&api_key)
        .header("X-API-Key", &api_key)
        .json(&json!({
            "query": args.query,
            "limit": limit,
            "days": args.days
        }))
        .send()
        .await
        .context("failed to call XQuik tweet search")?
        .error_for_status()
        .context("XQuik tweet search returned an error status")?
        .json()
        .await
        .context("failed to parse XQuik tweet search JSON")?;
    let items = normalize_x_items(&response, now, args.days, limit);
    Ok(json!({
        "status": if items.is_empty() { "empty" } else { "success" },
        "source": "x",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "data_gaps": if items.is_empty() {
            vec!["XQuik returned no date-filtered tweet items"]
        } else {
            Vec::<&str>::new()
        },
        "items": items,
        "raw": response
    }))
}

fn x_unavailable(args: &SocialArgs, now: DateTime<Utc>) -> Value {
    json!({
        "status": "unavailable",
        "source": "x",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "data_gaps": [
            "XQUIK_API_KEY is not set; X search was skipped"
        ],
        "items": []
    })
}

async fn run_youtube(client: &Client, args: &SocialArgs) -> Result<Value> {
    let now = Utc::now();
    let limit = args.limit.unwrap_or_else(|| default_limit(args.depth));
    let url = format!(
        "{YOUTUBE_SEARCH_URL}?search_query={}&sp=CAI%253D",
        urlencoding::encode(args.query.trim())
    );
    let response = match client.get(&url).send().await {
        Ok(response) => response,
        Err(error) => {
            return Ok(unavailable_result(
                "youtube",
                args,
                now,
                format!("failed to fetch YouTube candidates: {error}"),
            ));
        }
    };
    let response = match response.error_for_status() {
        Ok(response) => response,
        Err(error) => {
            return Ok(unavailable_result(
                "youtube",
                args,
                now,
                format!("YouTube search returned an error status: {error}"),
            ));
        }
    };
    let html = match response.text().await {
        Ok(text) => text,
        Err(error) => {
            return Ok(unavailable_result(
                "youtube",
                args,
                now,
                format!("failed to read YouTube search response body: {error}"),
            ));
        }
    };
    let items = extract_youtube_candidates(&html, limit, &args.query);
    Ok(json!({
        "status": if items.is_empty() { "empty" } else { "success" },
        "source": "youtube",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "candidate_search_url": url,
        "transcript_status": {
            "status": "not_attempted",
            "reason": "social YouTube subset returns candidates only; transcript extraction remains external"
        },
        "data_gaps": if items.is_empty() {
            vec!["YouTube candidate search returned no parseable video ids"]
        } else {
            Vec::<&str>::new()
        },
        "items": items
    }))
}

fn parse_reddit_rss(rss: &str, now: DateTime<Utc>, days: i64, query: &str) -> Vec<RedditItem> {
    let entry_re = Regex::new(r"(?s)<entry\b[^>]*>(.*?)</entry>").expect("valid entry regex");
    let mut by_key: BTreeMap<String, RedditItem> = BTreeMap::new();
    let cutoff = now - Duration::days(days);
    for cap in entry_re.captures_iter(rss) {
        let Some(entry) = cap.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let Some(published) = xml_tag_text(entry, "updated")
            .or_else(|| xml_tag_text(entry, "published"))
            .and_then(|value| parse_datetime(&value))
        else {
            continue;
        };
        if published < cutoff || published > now + Duration::minutes(5) {
            continue;
        }
        let title = xml_tag_text(entry, "title").unwrap_or_default();
        let content = xml_tag_text(entry, "content")
            .or_else(|| xml_tag_text(entry, "summary"))
            .unwrap_or_default();
        let url = xml_link_href(entry).or_else(|| {
            xml_tag_text(entry, "id")
                .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
        });
        let id = reddit_id_from_url(url.as_deref()).or_else(|| xml_tag_text(entry, "id"));
        let subreddit = subreddit_from_entry(entry).or_else(|| {
            url.as_deref()
                .and_then(|value| subreddit_from_url(value).map(ToString::to_string))
        });
        let relevance = relevance_score(query, &title, &content, subreddit.as_deref());
        if relevance == 0 {
            continue;
        }
        let key = id
            .clone()
            .or_else(|| url.clone())
            .unwrap_or_else(|| format!("{published}|{title}"));
        by_key
            .entry(key)
            .and_modify(|existing| {
                if existing.relevance < relevance {
                    existing.relevance = relevance;
                }
            })
            .or_insert(RedditItem {
                id,
                title,
                url,
                subreddit,
                author: author_from_entry(entry),
                published,
                summary: html_to_text(&content),
                relevance,
                score: None,
                comments: None,
            });
    }
    by_key.into_values().collect()
}

fn parse_shreddit_listing(html: &str) -> Option<RedditListingBackfill> {
    let post_re = Regex::new(r#"(?is)<shreddit-post\b([^>]*)>"#).expect("valid post regex");
    let attrs = post_re
        .captures(html)
        .and_then(|cap| cap.get(1).map(|m| m.as_str()))?;
    let score = attr_i64(attrs, &["score", "upvote-count", "post-score"]);
    let comments = attr_i64(attrs, &["comment-count", "comments-count"]);
    let subreddit = attr_value(attrs, "subreddit-prefixed-name")
        .or_else(|| attr_value(attrs, "subreddit-name"))
        .map(|value| {
            value
                .trim_start_matches("r/")
                .trim_start_matches("/r/")
                .to_string()
        });
    if score.is_none() && comments.is_none() && subreddit.is_none() {
        None
    } else {
        Some(RedditListingBackfill {
            score,
            comments,
            subreddit,
        })
    }
}

fn normalize_x_items(response: &Value, now: DateTime<Utc>, days: i64, limit: usize) -> Vec<Value> {
    let cutoff = now - Duration::days(days);
    let raw_items = response
        .get("data")
        .or_else(|| response.get("tweets"))
        .or_else(|| response.get("items"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    raw_items
        .into_iter()
        .filter_map(|item| {
            let created_at = first_string(
                &item,
                &[
                    "created_at",
                    "createdAt",
                    "timestamp",
                    "date",
                    "published_at",
                ],
            )
            .and_then(|value| parse_datetime(&value));
            if let Some(created_at) = created_at {
                if created_at < cutoff || created_at > now + Duration::minutes(5) {
                    return None;
                }
            }
            let text =
                first_string(&item, &["text", "full_text", "content", "body"]).unwrap_or_default();
            Some(json!({
                "id": first_string(&item, &["id", "tweet_id", "rest_id"]),
                "author": first_string(&item, &["author", "username", "screen_name", "user"]),
                "text": text,
                "url": first_string(&item, &["url", "tweet_url", "link"]),
                "published": created_at.map(|value| value.to_rfc3339()),
                "likes": first_i64(&item, &["likes", "favorite_count", "like_count"]),
                "reposts": first_i64(&item, &["retweets", "retweet_count", "reposts"]),
                "replies": first_i64(&item, &["replies", "reply_count"]),
                "raw": item
            }))
        })
        .take(limit)
        .collect()
}

fn extract_youtube_candidates(html: &str, limit: usize, query: &str) -> Vec<Value> {
    let id_re = Regex::new(r#""videoId":"([^"]+)""#).expect("valid video id regex");
    let title_re =
        Regex::new(r#""title":\{"runs":\[\{"text":"([^"]+)""#).expect("valid video title regex");
    let titles: Vec<String> = title_re
        .captures_iter(html)
        .filter_map(|cap| cap.get(1).map(|m| unescape_json_text(m.as_str())))
        .collect();
    let mut seen = BTreeSet::new();
    let mut items = Vec::new();
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
        let relevance = relevance_score(query, &title, "", None);
        if relevance == 0 {
            continue;
        }
        items.push(json!({
            "type": "candidate",
            "video_id": video_id,
            "title": title,
            "url": format!("https://www.youtube.com/watch?v={video_id}"),
            "relevance": relevance,
            "transcript_status": {
                "status": "not_attempted",
                "reason": "candidate search does not fetch transcripts"
            }
        }));
        if items.len() >= limit {
            break;
        }
    }
    items
}

#[derive(Debug, Clone)]
struct RedditItem {
    id: Option<String>,
    title: String,
    url: Option<String>,
    subreddit: Option<String>,
    author: Option<String>,
    published: DateTime<Utc>,
    summary: String,
    relevance: i64,
    score: Option<i64>,
    comments: Option<i64>,
}

impl RedditItem {
    fn apply_backfill(&mut self, backfill: RedditListingBackfill) {
        if self.score.is_none() {
            self.score = backfill.score;
        }
        if self.comments.is_none() {
            self.comments = backfill.comments;
        }
        if self.subreddit.is_none() {
            self.subreddit = backfill.subreddit;
        }
    }

    fn into_json(self) -> Value {
        json!({
            "id": self.id,
            "title": self.title,
            "url": self.url,
            "subreddit": self.subreddit,
            "author": self.author,
            "published": self.published.to_rfc3339(),
            "summary": self.summary,
            "relevance": self.relevance,
            "score": self.score,
            "comments": self.comments
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedditListingBackfill {
    score: Option<i64>,
    comments: Option<i64>,
    subreddit: Option<String>,
}

fn xml_tag_text(entry: &str, tag: &str) -> Option<String> {
    let re = Regex::new(&format!(r"(?s)<{tag}\b[^>]*>(.*?)</{tag}>")).ok()?;
    re.captures(entry)
        .and_then(|cap| cap.get(1))
        .map(|m| decode_html_entities(m.as_str()).to_string())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn xml_link_href(entry: &str) -> Option<String> {
    let re = Regex::new(r#"<link\b[^>]*href="([^"]+)""#).expect("valid link regex");
    re.captures(entry)
        .and_then(|cap| cap.get(1))
        .map(|m| decode_html_entities(m.as_str()).to_string())
        .filter(|value| !value.is_empty())
}

fn subreddit_from_entry(entry: &str) -> Option<String> {
    let category_re =
        Regex::new(r#"<category\b[^>]*term="([^"]+)""#).expect("valid category regex");
    let subreddit = category_re
        .captures_iter(entry)
        .filter_map(|cap| {
            cap.get(1)
                .map(|m| decode_html_entities(m.as_str()).to_string())
        })
        .find_map(|value| {
            value
                .strip_prefix("r/")
                .or_else(|| value.strip_prefix("/r/"))
                .map(ToString::to_string)
        });
    subreddit
}

fn author_from_entry(entry: &str) -> Option<String> {
    let author_re = Regex::new(r"(?s)<author\b[^>]*>.*?<name\b[^>]*>(.*?)</name>.*?</author>")
        .expect("valid author regex");
    author_re
        .captures(entry)
        .and_then(|cap| cap.get(1))
        .map(|m| decode_html_entities(m.as_str()).to_string())
        .map(|value| value.trim().trim_start_matches("/u/").to_string())
        .filter(|value| !value.is_empty())
}

fn html_to_text(value: &str) -> String {
    let tag_re = Regex::new(r"(?s)<[^>]+>").expect("valid html tag regex");
    decode_html_entities(tag_re.replace_all(value, " ").as_ref())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn reddit_id_from_url(url: Option<&str>) -> Option<String> {
    let re = Regex::new(r"/comments/([^/]+)/").expect("valid reddit id regex");
    re.captures(url?)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().to_string())
}

fn subreddit_from_url(url: &str) -> Option<&str> {
    let re = Regex::new(r"/r/([^/]+)/").expect("valid subreddit regex");
    re.captures(url)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str())
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .ok()
}

fn relevance_score(query: &str, title: &str, body: &str, subreddit: Option<&str>) -> i64 {
    let haystack = format!(
        "{} {} {}",
        title.to_lowercase(),
        body.to_lowercase(),
        subreddit.unwrap_or("").to_lowercase()
    );
    let tokens = query_tokens(query);
    if tokens.is_empty() {
        return 0;
    }
    tokens
        .iter()
        .map(|token| {
            let title_hits = title.to_lowercase().matches(token.as_str()).count() as i64;
            let haystack_hits = haystack.matches(token.as_str()).count() as i64;
            title_hits * 3 + haystack_hits
        })
        .sum()
}

fn query_tokens(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_alphanumeric())
        .map(|token| token.trim().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn attr_i64(attrs: &str, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .filter_map(|name| attr_value(attrs, name))
        .find_map(|value| compact_number(&value))
}

fn attr_value(attrs: &str, name: &str) -> Option<String> {
    let re = Regex::new(&format!(
        r#"\b{}\s*=\s*['"]([^'"]*)['"]"#,
        regex::escape(name)
    ))
    .ok()?;
    re.captures(attrs)
        .and_then(|cap| cap.get(1))
        .map(|m| decode_html_entities(m.as_str()).to_string())
        .filter(|value| !value.is_empty())
}

fn compact_number(value: &str) -> Option<i64> {
    let cleaned = value.trim().replace(',', "");
    if cleaned.is_empty() {
        return None;
    }
    let (number, multiplier) = match cleaned.chars().last()? {
        'k' | 'K' => (&cleaned[..cleaned.len() - 1], 1_000.0),
        'm' | 'M' => (&cleaned[..cleaned.len() - 1], 1_000_000.0),
        _ => (cleaned.as_str(), 1.0),
    };
    number
        .parse::<f64>()
        .ok()
        .map(|value| (value * multiplier).round() as i64)
}

fn normalize_subreddit_filter(subreddits: &[String]) -> BTreeSet<String> {
    subreddits
        .iter()
        .map(|value| normalize_subreddit_name(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn normalize_subreddit_name(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("r/")
        .trim_start_matches("/r/")
        .to_lowercase()
}

fn first_string(item: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| item.get(*key).and_then(Value::as_str))
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
}

fn first_i64(item: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        item.get(*key).and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(compact_number))
        })
    })
}

fn default_limit(depth: Depth) -> usize {
    match depth {
        Depth::Quick => 5,
        Depth::Balanced => 10,
        Depth::Deep => 25,
    }
}

fn depth_name(depth: Depth) -> &'static str {
    match depth {
        Depth::Quick => "quick",
        Depth::Balanced => "balanced",
        Depth::Deep => "deep",
    }
}

fn unavailable_result(source: &str, args: &SocialArgs, now: DateTime<Utc>, gap: String) -> Value {
    json!({
        "status": "unavailable",
        "source": source,
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "data_gaps": [gap],
        "items": []
    })
}

fn source_name(source: Source) -> &'static str {
    match source {
        Source::Reddit => "reddit",
        Source::X => "x",
        Source::Youtube => "youtube",
    }
}

fn social_query_for_ticker(ticker: &str) -> String {
    if matches!(ticker, "QQQ" | "TQQQ" | "SQQQ") {
        format!("{ticker} Nasdaq VIX risk on risk off volatility")
    } else {
        ticker.to_string()
    }
}

fn summarize_items(source: Source, items: &[Value]) -> String {
    if items.is_empty() {
        return format!("No usable {} samples were retrieved.", source_name(source));
    }
    let labels = items
        .iter()
        .take(5)
        .filter_map(|item| {
            item.get("title")
                .or_else(|| item.get("text"))
                .and_then(Value::as_str)
        })
        .map(|value| value.chars().take(120).collect::<String>())
        .collect::<Vec<_>>();
    if labels.is_empty() {
        format!("Retrieved {} {} samples.", items.len(), source_name(source))
    } else {
        format!(
            "Retrieved {} {} samples. Representative items: {}",
            items.len(),
            source_name(source),
            labels.join(" | ")
        )
    }
}

fn unescape_json_text(value: &str) -> String {
    value
        .replace("\\u0026", "&")
        .replace("\\\"", "\"")
        .replace("\\/", "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn reddit_rss_parser_dedupes_filters_dates_and_scores_relevance() {
        let rss = r#"
        <feed>
          <entry>
            <id>https://www.reddit.com/r/TQQQ/comments/abc123/tqqq_signal/</id>
            <title>TQQQ signal turns bullish</title>
            <updated>2026-06-15T10:00:00+00:00</updated>
            <author><name>/u/researcher</name></author>
            <category term="r/TQQQ"/>
            <link href="https://www.reddit.com/r/TQQQ/comments/abc123/tqqq_signal/"/>
            <content type="html">&lt;p&gt;TQQQ traders discuss Nasdaq breadth.&lt;/p&gt;</content>
          </entry>
          <entry>
            <id>https://www.reddit.com/r/TQQQ/comments/abc123/tqqq_signal/</id>
            <title>TQQQ signal duplicate</title>
            <updated>2026-06-15T11:00:00+00:00</updated>
            <link href="https://www.reddit.com/r/TQQQ/comments/abc123/tqqq_signal/"/>
            <content type="html">TQQQ duplicate</content>
          </entry>
          <entry>
            <id>https://www.reddit.com/r/ETFs/comments/old/tqqq_old/</id>
            <title>TQQQ old thread</title>
            <updated>2026-04-01T10:00:00+00:00</updated>
            <link href="https://www.reddit.com/r/ETFs/comments/old/tqqq_old/"/>
            <content type="html">TQQQ old content</content>
          </entry>
          <entry>
            <id>https://www.reddit.com/r/cooking/comments/food/pasta/</id>
            <title>Pasta ideas</title>
            <updated>2026-06-15T10:00:00+00:00</updated>
            <link href="https://www.reddit.com/r/cooking/comments/food/pasta/"/>
            <content type="html">Dinner thread</content>
          </entry>
        </feed>
        "#;

        let items = parse_reddit_rss(rss, fixed_now(), 30, "TQQQ Nasdaq");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id.as_deref(), Some("abc123"));
        assert_eq!(items[0].subreddit.as_deref(), Some("TQQQ"));
        assert_eq!(items[0].author.as_deref(), Some("researcher"));
        assert!(items[0].relevance > 0);
        assert!(items[0].summary.contains("Nasdaq breadth"));
    }

    #[test]
    fn shreddit_listing_parser_reads_score_comments_and_subreddit() {
        let html = r#"
        <html>
          <body>
            <shreddit-post
              post-id="t3_abc123"
              score="1.2k"
              comment-count="45"
              subreddit-prefixed-name="r/TQQQ">
            </shreddit-post>
          </body>
        </html>
        "#;

        let backfill = parse_shreddit_listing(html).expect("backfill");

        assert_eq!(
            backfill,
            RedditListingBackfill {
                score: Some(1200),
                comments: Some(45),
                subreddit: Some("TQQQ".to_string())
            }
        );
    }

    #[test]
    fn x_without_key_returns_unavailable_with_data_gap() {
        let args = SocialArgs {
            source: Source::X,
            query: "TQQQ".to_string(),
            tickers: Vec::new(),
            days: 30,
            depth: Depth::Quick,
            limit: Some(5),
            subreddits: Vec::new(),
            output: None,
        };

        let value = x_unavailable(&args, fixed_now());

        assert_eq!(value["status"], "unavailable");
        assert_eq!(value["source"], "x");
        assert_eq!(value["items"].as_array().unwrap().len(), 0);
        assert!(value["data_gaps"][0]
            .as_str()
            .unwrap()
            .contains("XQUIK_API_KEY"));
    }
}
