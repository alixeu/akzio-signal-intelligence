use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use clap::{Args, ValueEnum};
use decrypt_cookies::{browser::cookies::CookiesInfo, safari::SafariBuilder};
use html_escape::decode_html_entities;
use regex::Regex;
use reqwest::{cookie::Jar, Client};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration as StdDuration,
};
use twikit::{
    constants, endpoints::GqlEndpoint, utils::find_values, Client as TwikitClient, Tweet,
    TweetSearchProduct,
};

const REDDIT_RSS_URL: &str = "https://www.reddit.com/search.rss";
const REDDIT_JSON_URL: &str = "https://www.reddit.com/search.json";
const YOUTUBE_SEARCH_URL: &str = "https://www.youtube.com/results";
const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Codex social last30days";
const SOCIAL_COOKIE_HOSTS: &[&str] = &[
    "reddit.com",
    "www.reddit.com",
    "old.reddit.com",
    "x.com",
    "twitter.com",
    "api.x.com",
];
const X_SEARCH_TIMELINE_2026: GqlEndpoint = GqlEndpoint {
    query_id: "4fpceYZ6-YQCx_JSl_Cn_A",
    operation: "SearchTimeline",
};

#[derive(Debug, Clone)]
struct BrowserCookieAuth {
    enabled: bool,
    cookies_loaded: usize,
    twikit_cookies_loaded: usize,
    warnings: Vec<String>,
    browser_cookies: Value,
}

#[derive(Clone)]
struct SocialClient {
    client: Client,
    browser_cookie_auth: BrowserCookieAuth,
}

impl SocialClient {
    async fn build(source: Source) -> Result<Self> {
        let mut builder = Client::builder()
            .timeout(StdDuration::from_secs(20))
            .user_agent(DEFAULT_USER_AGENT);
        let mut browser_cookie_auth = BrowserCookieAuth {
            enabled: matches!(source, Source::Reddit | Source::X),
            cookies_loaded: 0,
            twikit_cookies_loaded: 0,
            warnings: Vec::new(),
            browser_cookies: json!({ "cookies": [] }),
        };
        if browser_cookie_auth.enabled {
            let (jar, auth) = load_browser_cookie_jar(SOCIAL_COOKIE_HOSTS).await;
            browser_cookie_auth = auth;
            if browser_cookie_auth.cookies_loaded > 0 {
                builder = builder.cookie_provider(jar);
            }
        }
        Ok(Self {
            client: builder.build()?,
            browser_cookie_auth,
        })
    }

    fn auth_json(&self) -> Value {
        json!({
            "enabled": self.browser_cookie_auth.enabled,
            "cookies_loaded": self.browser_cookie_auth.cookies_loaded,
            "twikit_cookies_loaded": self.browser_cookie_auth.twikit_cookies_loaded,
            "twikit_has_auth_token": twikit_cookie_present(&self.browser_cookie_auth.browser_cookies, "auth_token"),
            "twikit_has_ct0": twikit_cookie_present(&self.browser_cookie_auth.browser_cookies, "ct0"),
            "warnings": self.browser_cookie_auth.warnings,
        })
    }

    fn auth_gaps(&self) -> Vec<String> {
        if !self.browser_cookie_auth.enabled || self.browser_cookie_auth.cookies_loaded > 0 {
            return Vec::new();
        }
        let mut gaps = vec![
            "browser cookie auth enabled but no usable reddit/x cookies were loaded".to_string(),
        ];
        gaps.extend(
            self.browser_cookie_auth
                .warnings
                .iter()
                .map(|warning| format!("browser cookie auth: {warning}")),
        );
        gaps
    }
}

fn add_browser_cookie(jar: &Jar, cookie: &impl CookiesInfo) {
    if cookie.name().is_empty() || cookie.value().is_empty() || cookie.domain().is_empty() {
        return;
    }
    if let Ok(url) = reqwest::Url::parse(&cookie.url()) {
        jar.add_cookie_str(&cookie.set_cookie_header(), &url);
    }
}

fn twikit_cookie_json(cookie: &impl CookiesInfo) -> Option<Value> {
    if cookie.name().is_empty() || cookie.value().is_empty() {
        return None;
    }
    let domain = normalize_twikit_cookie_domain(cookie.domain())?;
    Some(json!({
        "name": cookie.name(),
        "value": cookie.value(),
        "domain": domain,
        "path": cookie.path(),
    }))
}

fn normalize_twikit_cookie_domain(domain: &str) -> Option<&'static str> {
    match domain.trim_start_matches('.').to_ascii_lowercase().as_str() {
        "x.com" | "twitter.com" => Some(".x.com"),
        _ => None,
    }
}

fn twikit_cookie_present(cookies: &Value, name: &str) -> bool {
    cookies
        .get("cookies")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|cookie| cookie.get("name").and_then(Value::as_str) == Some(name))
}

fn cookie_matches_hosts(cookie: &impl CookiesInfo, hosts: &[&str]) -> bool {
    let domain = cookie.domain().trim_start_matches('.').to_ascii_lowercase();
    hosts.iter().any(|host| {
        let host = host.trim_start_matches('.').to_ascii_lowercase();
        domain == host || domain.ends_with(&format!(".{host}"))
    })
}

async fn load_browser_cookie_jar(hosts: &[&str]) -> (Arc<Jar>, BrowserCookieAuth) {
    let jar = Arc::new(Jar::default());
    let mut cookies_loaded = 0usize;
    let mut twikit_cookies = Vec::new();
    let mut warnings = Vec::new();

    match SafariBuilder::new().build().await {
        Ok(getter) => {
            let mut seen = BTreeSet::new();
            for cookie in getter.cookies_all() {
                if !cookie_matches_hosts(cookie, hosts) {
                    continue;
                }
                let cookie_key =
                    format!("{}\t{}\t{}", cookie.domain(), cookie.path(), cookie.name());
                if !seen.insert(cookie_key) {
                    continue;
                }
                add_browser_cookie(&jar, cookie);
                if let Some(cookie) = twikit_cookie_json(cookie) {
                    twikit_cookies.push(cookie);
                }
                cookies_loaded += 1;
            }
        }
        Err(error) => warnings.push(format!("safari browser unavailable: {error}")),
    }

    (
        jar,
        BrowserCookieAuth {
            enabled: true,
            cookies_loaded,
            twikit_cookies_loaded: twikit_cookies.len(),
            warnings,
            browser_cookies: json!({ "cookies": twikit_cookies }),
        },
    )
}

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
    let client = SocialClient::build(args.source).await?;
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

async fn run_single(client: &SocialClient, args: &SocialArgs) -> Result<Value> {
    match args.source {
        Source::Reddit => run_reddit(client, args).await,
        Source::X => run_x(client, args).await,
        Source::Youtube => run_youtube(client, args).await,
    }
}

async fn run_per_ticker(client: &SocialClient, args: &SocialArgs) -> Result<Value> {
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

async fn run_reddit(client: &SocialClient, args: &SocialArgs) -> Result<Value> {
    let now = Utc::now();
    let limit = args.limit.unwrap_or_else(|| default_limit(args.depth));
    let encoded_query = urlencoding::encode(args.query.trim());
    let rss_url = format!("{REDDIT_RSS_URL}?q={encoded_query}&sort=new&t=month");
    let json_url = format!("{REDDIT_JSON_URL}?q={encoded_query}&sort=new&t=month&limit={limit}");
    let mut gaps = client.auth_gaps();
    let mut items = match fetch_reddit_rss_items(client, &rss_url, now, args).await {
        Ok(items) => items,
        Err(error) => {
            gaps.push(error);
            Vec::new()
        }
    };
    if items.is_empty() {
        match fetch_reddit_json_items(client, &json_url, now, args).await {
            Ok(json_items) if !json_items.is_empty() => {
                gaps.push("reddit RSS search failed or returned no usable items; used Reddit JSON fallback".to_string());
                items = json_items;
            }
            Ok(_) => gaps
                .push("reddit JSON fallback returned no date-filtered relevant items".to_string()),
            Err(error) => gaps.push(error),
        }
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
            match fetch_reddit_listing_backfill(&client.client, url).await {
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
        "json_fallback_url": json_url,
        "backfilled_items": backfilled,
        "browser_cookie_auth": client.auth_json(),
        "items": items.into_iter().map(RedditItem::into_json).collect::<Vec<_>>()
    }))
}

async fn fetch_reddit_rss_items(
    client: &SocialClient,
    rss_url: &str,
    now: DateTime<Utc>,
    args: &SocialArgs,
) -> std::result::Result<Vec<RedditItem>, String> {
    let response = client
        .client
        .get(rss_url)
        .send()
        .await
        .map_err(|error| format!("failed to fetch Reddit RSS search: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Reddit RSS search returned an error status: {error}"))?;
    let rss_text = response
        .text()
        .await
        .map_err(|error| format!("failed to read Reddit RSS response body: {error}"))?;
    let items = parse_reddit_rss(&rss_text, now, args.days, &args.query);
    if items.is_empty() {
        Err("reddit RSS search returned no date-filtered relevant items".to_string())
    } else {
        Ok(items)
    }
}

async fn fetch_reddit_json_items(
    client: &SocialClient,
    json_url: &str,
    now: DateTime<Utc>,
    args: &SocialArgs,
) -> std::result::Result<Vec<RedditItem>, String> {
    let response = client
        .client
        .get(json_url)
        .send()
        .await
        .map_err(|error| format!("failed to fetch Reddit JSON fallback: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Reddit JSON fallback returned an error status: {error}"))?;
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| format!("failed to parse Reddit JSON fallback body: {error}"))?;
    Ok(parse_reddit_json(&value, now, args.days, &args.query))
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

async fn run_x(client: &SocialClient, args: &SocialArgs) -> Result<Value> {
    let now = Utc::now();
    let limit = args
        .limit
        .unwrap_or_else(|| default_limit(args.depth))
        .min(20);
    let twikit_client = match TwikitClient::builder().build() {
        Ok(client) => client,
        Err(error) => {
            return Ok(x_unavailable(
                args,
                now,
                client,
                format!("failed to build twikit client: {error}"),
            ))
        }
    };
    if client.browser_cookie_auth.twikit_cookies_loaded == 0 {
        return Ok(x_unavailable(
            args,
            now,
            client,
            "twikit search requires x.com browser cookies, but no usable x.com cookies were loaded"
                .to_string(),
        ));
    }
    if let Err(error) =
        twikit_client.set_browser_cookies(client.browser_cookie_auth.browser_cookies.clone())
    {
        return Ok(x_unavailable(
            args,
            now,
            client,
            format!("failed to set browser cookies on twikit client: {error}"),
        ));
    }
    let (tweets, raw, mut data_gaps) = match twikit_client
        .search_tweet(
            args.query.clone(),
            TweetSearchProduct::Latest,
            limit as i64,
            None,
        )
        .await
    {
        Ok(timeline) => (timeline.items, timeline.raw, Vec::new()),
        Err(error) => match run_x_search_timeline_2026(&twikit_client, args, limit).await {
            Ok((tweets, raw)) => (
                tweets,
                raw,
                vec![format!(
                    "twikit default SearchTimeline failed, used 2026 fallback: {error}"
                )],
            ),
            Err(fallback_error) => {
                return Ok(x_unavailable(
                    args,
                    now,
                    client,
                    format!(
                        "twikit tweet search failed: {error}; 2026 SearchTimeline fallback failed: {fallback_error}"
                    ),
                ))
            }
        },
    };
    let items = normalize_twikit_tweets(tweets, now, args.days, limit);
    if items.is_empty() {
        data_gaps.push("twikit returned no date-filtered tweet items".to_string());
    }
    Ok(json!({
        "status": if items.is_empty() { "empty" } else { "success" },
        "source": "x",
        "provider": "twikit-rs",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "browser_cookie_auth": client.auth_json(),
        "data_gaps": data_gaps,
        "items": items,
        "raw": raw
    }))
}

async fn run_x_search_timeline_2026(
    twikit_client: &TwikitClient,
    args: &SocialArgs,
    limit: usize,
) -> Result<(Vec<Tweet>, Value)> {
    let raw = twikit_client
        .gql_get_raw(
            X_SEARCH_TIMELINE_2026,
            json!({
                "rawQuery": args.query,
                "count": limit,
                "querySource": "typed_query",
                "product": "Latest",
                "withGrokTranslatedBio": false
            }),
            Some(constants::features()),
            Some(json!({ "fieldToggles": { "withArticleRichContentState": false } })),
        )
        .await?;
    let tweets = parse_twikit_tweets_from_raw(&raw, limit);
    Ok((tweets, raw))
}

fn parse_twikit_tweets_from_raw(raw: &Value, limit: usize) -> Vec<Tweet> {
    let mut tweet_results = Vec::new();
    find_values(raw, "tweet_results", &mut tweet_results);
    let mut seen = BTreeSet::new();
    tweet_results
        .into_iter()
        .filter_map(|value| value.get("result").cloned())
        .filter_map(|value| Tweet::from_value(value).ok())
        .filter(|tweet| seen.insert(tweet.id.clone()))
        .take(limit)
        .collect()
}

fn x_unavailable(
    args: &SocialArgs,
    now: DateTime<Utc>,
    client: &SocialClient,
    reason: String,
) -> Value {
    let (failure_kind, next_action) = diagnose_x_failure(client, &reason);
    let mut data_gaps = client.auth_gaps();
    data_gaps.push(reason);
    json!({
        "status": "unavailable",
        "source": "x",
        "provider": "twikit-rs",
        "query": args.query,
        "depth": depth_name(args.depth),
        "window_days": args.days,
        "fetched_at": now.to_rfc3339(),
        "failure_kind": failure_kind,
        "next_action": next_action,
        "browser_cookie_auth": client.auth_json(),
        "data_gaps": data_gaps,
        "items": []
    })
}

fn diagnose_x_failure(client: &SocialClient, reason: &str) -> (&'static str, &'static str) {
    let lower_reason = reason.to_ascii_lowercase();
    let has_auth_token =
        twikit_cookie_present(&client.browser_cookie_auth.browser_cookies, "auth_token");
    let has_ct0 = twikit_cookie_present(&client.browser_cookie_auth.browser_cookies, "ct0");

    if lower_reason.contains("failed to build twikit client") {
        return (
            "x_client_initialization_failed",
            "Check twikit-rs dependency initialization before retrying X ingest.",
        );
    }

    if client.browser_cookie_auth.twikit_cookies_loaded == 0 || !has_auth_token || !has_ct0 {
        return (
            "x_missing_browser_session",
            "Open Safari, sign in to x.com, then rerun last30days X ingest.",
        );
    }

    if lower_reason.contains("failed to set browser cookies") {
        return (
            "x_cookie_format_rejected",
            "Refresh the Safari x.com session and verify exported cookies include auth_token and ct0.",
        );
    }

    if lower_reason.contains("status 403") && lower_reason.contains("body: ") {
        return (
            "x_blocked_by_cloudflare_or_client_fingerprint",
            "Use a browser-backed X retrieval path or the official X API; Safari cookies are present but reqwest/twikit is being rejected.",
        );
    }

    (
        "x_request_failed",
        "Retry later, then inspect the raw twikit-rs error if the failure persists.",
    )
}

async fn run_youtube(client: &SocialClient, args: &SocialArgs) -> Result<Value> {
    let now = Utc::now();
    let limit = args.limit.unwrap_or_else(|| default_limit(args.depth));
    let url = format!(
        "{YOUTUBE_SEARCH_URL}?search_query={}&sp=CAI%253D",
        urlencoding::encode(args.query.trim())
    );
    let response = match client.client.get(&url).send().await {
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

fn parse_reddit_json(value: &Value, now: DateTime<Utc>, days: i64, query: &str) -> Vec<RedditItem> {
    let cutoff = now - Duration::days(days);
    value
        .pointer("/data/children")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|child| child.get("data"))
        .filter_map(|post| {
            let created = post
                .get("created_utc")
                .and_then(Value::as_f64)
                .and_then(|value| DateTime::<Utc>::from_timestamp(value as i64, 0))?;
            if created < cutoff || created > now + Duration::minutes(5) {
                return None;
            }
            let title = first_string(post, &["title"]).unwrap_or_default();
            let summary = first_string(post, &["selftext", "body", "text"]).unwrap_or_default();
            let subreddit = first_string(post, &["subreddit"]);
            let relevance = relevance_score(query, &title, &summary, subreddit.as_deref());
            if relevance == 0 {
                return None;
            }
            let permalink =
                first_string(post, &["permalink"]).map(|value| format_reddit_url(&value));
            let url = permalink.or_else(|| first_string(post, &["url", "link"]));
            Some(RedditItem {
                id: first_string(post, &["id", "name"]),
                title,
                url,
                subreddit,
                author: first_string(post, &["author"]),
                published: created,
                summary,
                relevance,
                score: first_i64(post, &["score", "ups"]),
                comments: first_i64(post, &["num_comments", "comments"]),
            })
        })
        .collect()
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

fn normalize_twikit_tweets(
    tweets: Vec<Tweet>,
    now: DateTime<Utc>,
    days: i64,
    limit: usize,
) -> Vec<Value> {
    let cutoff = now - Duration::days(days);
    tweets
        .into_iter()
        .filter_map(|tweet| {
            let published = tweet
                .created_at_datetime()
                .and_then(|value| value.ok())
                .map(|value| value.with_timezone(&Utc));
            if let Some(published) = published {
                if published < cutoff || published > now + Duration::minutes(5) {
                    return None;
                }
            }
            let id = tweet.id;
            Some(json!({
                "id": id.clone(),
                "author": tweet.user.as_ref().and_then(|user| user.screen_name.clone()),
                "author_name": tweet.user.as_ref().and_then(|user| user.name.clone()),
                "text": tweet.full_text.or(tweet.text),
                "url": format!("https://x.com/i/web/status/{id}"),
                "published": published.map(|value| value.to_rfc3339()),
                "likes": tweet.favorite_count,
                "reposts": tweet.retweet_count,
                "replies": tweet.reply_count,
                "bookmarks": tweet.bookmark_count,
                "views": tweet.view_count,
                "raw": tweet.raw
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

fn format_reddit_url(value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else {
        format!("https://www.reddit.com/{}", value.trim_start_matches('/'))
    }
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
    fn twikit_cookie_domain_filter_accepts_only_x_root_domain() {
        assert_eq!(normalize_twikit_cookie_domain("x.com"), Some(".x.com"));
        assert_eq!(normalize_twikit_cookie_domain(".x.com"), Some(".x.com"));
        assert_eq!(
            normalize_twikit_cookie_domain("twitter.com"),
            Some(".x.com")
        );
        assert_eq!(
            normalize_twikit_cookie_domain(".twitter.com"),
            Some(".x.com")
        );
        assert_eq!(normalize_twikit_cookie_domain("api.x.com"), None);
        assert_eq!(normalize_twikit_cookie_domain("reddit.com"), None);
    }

    #[test]
    fn auth_json_reports_twikit_key_cookie_presence_without_values() {
        let client = SocialClient {
            client: Client::new(),
            browser_cookie_auth: BrowserCookieAuth {
                enabled: true,
                cookies_loaded: 2,
                twikit_cookies_loaded: 2,
                warnings: Vec::new(),
                browser_cookies: json!({
                    "cookies": [
                        {"name": "auth_token", "value": "secret"},
                        {"name": "ct0", "value": "csrf"}
                    ]
                }),
            },
        };

        let auth = client.auth_json();

        assert_eq!(auth["twikit_has_auth_token"], true);
        assert_eq!(auth["twikit_has_ct0"], true);
        assert!(!auth.to_string().contains("secret"));
        assert!(!auth.to_string().contains("csrf"));
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
    fn reddit_json_parser_reads_posts_from_fallback_shape() {
        let value = json!({
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "abc123",
                            "title": "QQQ SOXX rotation gets debated",
                            "selftext": "VIX is rising while semis lag.",
                            "permalink": "/r/stocks/comments/abc123/qqq_soxx_rotation/",
                            "subreddit": "stocks",
                            "author": "macrodesk",
                            "created_utc": 1781546400.0,
                            "score": 42,
                            "num_comments": 9
                        }
                    },
                    {
                        "data": {
                            "id": "old",
                            "title": "QQQ old",
                            "created_utc": 1777593600.0,
                            "subreddit": "stocks"
                        }
                    },
                    {
                        "data": {
                            "id": "irrelevant",
                            "title": "Gardening",
                            "created_utc": 1781546400.0,
                            "subreddit": "plants"
                        }
                    }
                ]
            }
        });

        let items = parse_reddit_json(&value, fixed_now(), 30, "QQQ SOXX VIX");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id.as_deref(), Some("abc123"));
        assert_eq!(items[0].subreddit.as_deref(), Some("stocks"));
        assert_eq!(items[0].author.as_deref(), Some("macrodesk"));
        assert_eq!(items[0].score, Some(42));
        assert_eq!(items[0].comments, Some(9));
        assert_eq!(
            items[0].url.as_deref(),
            Some("https://www.reddit.com/r/stocks/comments/abc123/qqq_soxx_rotation/")
        );
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

    fn x_args() -> SocialArgs {
        SocialArgs {
            source: Source::X,
            query: "TQQQ".to_string(),
            tickers: Vec::new(),
            days: 30,
            depth: Depth::Quick,
            limit: Some(5),
            subreddits: Vec::new(),
            output: None,
        }
    }

    #[test]
    fn x_without_browser_cookies_returns_unavailable_with_data_gap() {
        let client = SocialClient {
            client: Client::new(),
            browser_cookie_auth: BrowserCookieAuth {
                enabled: true,
                cookies_loaded: 0,
                twikit_cookies_loaded: 0,
                warnings: vec!["no browser cookies".to_string()],
                browser_cookies: json!({ "cookies": [] }),
            },
        };

        let value = x_unavailable(
            &x_args(),
            fixed_now(),
            &client,
            "request failed".to_string(),
        );

        assert_eq!(value["status"], "unavailable");
        assert_eq!(value["source"], "x");
        assert_eq!(value["items"].as_array().unwrap().len(), 0);
        assert_eq!(value["failure_kind"], "x_missing_browser_session");
        let gaps = value["data_gaps"].as_array().unwrap();
        assert!(gaps
            .iter()
            .any(|gap| gap.as_str().unwrap().contains("request failed")));
        assert!(gaps
            .iter()
            .any(|gap| gap.as_str().unwrap().contains("browser cookie auth")));
        assert_eq!(value["browser_cookie_auth"]["cookies_loaded"], 0);
    }

    #[test]
    fn x_forbidden_with_key_cookies_reports_client_fingerprint_block() {
        let client = SocialClient {
            client: Client::new(),
            browser_cookie_auth: BrowserCookieAuth {
                enabled: true,
                cookies_loaded: 2,
                twikit_cookies_loaded: 2,
                warnings: Vec::new(),
                browser_cookies: json!({
                    "cookies": [
                        {"name": "auth_token", "value": "secret"},
                        {"name": "ct0", "value": "csrf"}
                    ]
                }),
            },
        };

        let value = x_unavailable(
            &x_args(),
            fixed_now(),
            &client,
            "twikit tweet search failed: forbidden: status 403, body: ; 2026 SearchTimeline fallback failed: forbidden: status 403, body: ".to_string(),
        );

        assert_eq!(value["status"], "unavailable");
        assert_eq!(
            value["failure_kind"],
            "x_blocked_by_cloudflare_or_client_fingerprint"
        );
        assert!(value["next_action"]
            .as_str()
            .unwrap()
            .contains("browser-backed X retrieval path"));
        assert_eq!(value["browser_cookie_auth"]["twikit_has_auth_token"], true);
        assert_eq!(value["browser_cookie_auth"]["twikit_has_ct0"], true);
    }
}
