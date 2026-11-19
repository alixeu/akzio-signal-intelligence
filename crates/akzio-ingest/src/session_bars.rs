//! Session-aware, bounded Alpaca daily-bar acquisition. Calendar and each page
//! are retained in RawEvidence on successful acquisition; normalized prices
//! carry their availability basis.

// 文件导读：日线采集先抓 clock/market snapshot 与交易日历，按美东 close+20 分钟筛出已
// 完成的单资产 Session，再分页读取有界 bars。成功返回时才把请求、calendar、page、
// corporate action 响应放入 RawEvidence；四资产共同 Session 由下游另算。
// NormalizedEvidence 只暴露 cutoff 内且 OHLCV 合法的 bars。raw_prices 保留
// corporate-action 响应，Outcome 阶段另检查分页完整性及估值窗口内公司行动。
use super::*;
use akzio_domain::ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES;
use chrono::{NaiveDate, NaiveTime, TimeZone};
use chrono_tz::America::New_York;

const MAX_BAR_PAGES: usize = 16;
const MAX_PROVIDER_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarAdjustment {
    All,
    Raw,
}

impl BarAdjustment {
    const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Raw => "raw",
        }
    }

    const fn price_basis(self) -> &'static str {
        match self {
            Self::All => "adjusted_research",
            Self::Raw => "raw_requires_stage_action_check",
        }
    }
}

/// The exchange calendar and observed prices for one bounded Alpaca request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarSessionCoverage {
    pub version: u32,
    pub expected_closes: BTreeMap<NaiveDate, DateTime<Utc>>,
    pub observed_dates: Vec<NaiveDate>,
    pub missing_dates: Vec<NaiveDate>,
    pub outside_window_dates: Vec<NaiveDate>,
    pub feed: Option<AlpacaMarketDataFeed>,
    pub adjustment: BarAdjustment,
    pub provider_revision: Option<String>,
    pub calendar_source: String,
    pub cutoff: DateTime<Utc>,
    pub requested_limit: u16,
    pub provider_has_more: bool,
}

impl BarSessionCoverage {
    /// Earlier immutable evidence may have observed closes without calendar coverage.
    pub fn read(value: &Value) -> EvidenceRuntimeResult<Option<Self>> {
        let Some(field) = value.get("bar_session_coverage") else {
            return Ok(None);
        };
        let coverage: Self = serde_json::from_value(field.clone())
            .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
        let observed_closes: BTreeMap<NaiveDate, DateTime<Utc>> =
            serde_json::from_value(value["session_closes"].clone())
                .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
        let request_url = value["request_url"]
            .as_str()
            .and_then(|url| Url::parse(url).ok())
            .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
        let query = request_url.query_pairs().collect::<BTreeMap<_, _>>();
        let feed = coverage.feed.map(AlpacaMarketDataFeed::as_str);
        if coverage.version != 1
            || coverage.expected_closes.is_empty()
            || observed_closes.is_empty()
            || coverage.requested_limit == 0
            || query.get("limit").and_then(|limit| limit.parse().ok())
                != Some(coverage.requested_limit)
            || query.get("feed").map(|feed| feed.as_ref()) != feed
            || query.get("adjustment").map(|value| value.as_ref())
                != Some(coverage.adjustment.as_str())
            || value["price_basis"].as_str() != Some(coverage.adjustment.price_basis())
            || observed_closes.len() > usize::from(coverage.requested_limit)
            || value["feed"] != serde_json::json!(feed)
            || value["calendar_source"].as_str() != Some(coverage.calendar_source.as_str())
            || value["decision_cutoff"]
                .as_str()
                .and_then(|at| at.parse().ok())
                != Some(coverage.cutoff)
            || !observed_closes
                .keys()
                .copied()
                .eq(coverage.observed_dates.iter().copied())
            || observed_closes
                .iter()
                .any(|(date, close)| coverage.expected_closes.get(date) != Some(close))
            || !coverage
                .expected_closes
                .keys()
                .filter(|date| !observed_closes.contains_key(date))
                .eq(&coverage.missing_dates)
            || coverage.expected_closes.iter().any(|(date, close)| {
                close.with_timezone(&New_York).date_naive() != *date
                    || close
                        .checked_add_signed(Duration::minutes(
                            ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES,
                        ))
                        .is_none_or(|available_at| available_at > coverage.cutoff)
            })
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        Ok(Some(coverage))
    }
}

pub(crate) fn classify_evidence_response(
    response: &reqwest::Response,
) -> Result<(), EvidenceAdapterError> {
    // 只把成功、鉴权、限流、临时和永久 HTTP 状态映射为明确 adapter 结果；429 的 retry-after
    // 支持秒数或日期并有上限，避免错误响应被当作空证据。
    let status = response.status();
    match status.as_u16() {
        200..=299 => Ok(()),
        401 | 403 => Err(EvidenceAdapterError::Unauthorized(status.as_u16())),
        429 => {
            let delay = response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| {
                    s.parse::<u64>().ok().or_else(|| {
                        DateTime::parse_from_rfc2822(s).ok().map(|t| {
                            (t.with_timezone(&Utc) - Utc::now()).num_seconds().max(1) as u64
                        })
                    })
                })
                .unwrap_or(60)
                .clamp(1, 86_400);
            Err(EvidenceAdapterError::RateLimited {
                retry_after_secs: delay,
            })
        }
        408 | 500..=599 => Err(EvidenceAdapterError::Transport(format!(
            "provider HTTP {}",
            status.as_u16()
        ))),
        _ => Err(EvidenceAdapterError::Permanent(status.as_u16())),
    }
}

fn invalid(message: &str) -> EvidenceAdapterError {
    // 统一构造 provider data-quality 错误，让调用方区分数据坏、等待发布和网络失败。
    EvidenceAdapterError::DataQuality(message.to_owned())
}

/// The acquisition range finds common sessions; only this valuation range can
/// invalidate a frozen stage. Dates follow Alpaca's corporate-action models.
pub fn validate_outcome_price_window(
    normalized: &Value,
    baseline: NaiveDate,
    through: NaiveDate,
) -> Result<(), EvidenceAdapterError> {
    // 逐类读取 corporate action 的真实生效字段，并要求 pagination 完整；baseline 之后、
    // through 之内出现未建账的 split/dividend/merge 等动作时拒绝数值 Outcome。
    let response = normalized
        .get("corporate_actions_response")
        .ok_or_else(|| invalid("Outcome corporate-action evidence missing"))?;
    let actions = response
        .get("corporate_actions")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("corporate actions missing"))?;
    if response
        .get("next_page_token")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
    {
        return Err(invalid(
            "corporate-action window incomplete: pagination required",
        ));
    }
    for (kind, records) in actions {
        for action in records
            .as_array()
            .ok_or_else(|| invalid("corporate-action array malformed"))?
        {
            let field = match kind.as_str() {
                "forward_splits"
                | "reverse_splits"
                | "stock_dividends"
                | "cash_dividends"
                | "spin_offs"
                | "rights_distributions"
                | "capital_gains_distributions" => "ex_date",
                "unit_splits" | "cash_mergers" | "stock_mergers" | "stock_and_cash_mergers" => {
                    "effective_date"
                }
                "redemptions" | "name_changes" | "worthless_removals" => "process_date",
                _ => return Err(invalid("unknown corporate-action date semantics")),
            };
            let effective = action
                .get(field)
                .and_then(Value::as_str)
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                .ok_or_else(|| invalid("corporate-action effective date missing"))?;
            if baseline < effective && effective <= through {
                return Err(invalid(
                    "Outcome valuation window contains corporate action; quantity/cash adjustment ledger required",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn session_closes(
    calendar: &Value,
) -> Result<BTreeMap<NaiveDate, DateTime<Utc>>, EvidenceAdapterError> {
    // 把 Alpaca calendar 的美东 close 转成 UTC，并拒绝重复日期/模糊时区；这是可用性边界
    // 元数据，不是未来价格事实。
    let mut closes = BTreeMap::new();
    for session in calendar
        .as_array()
        .ok_or_else(|| invalid("calendar must be an array"))?
    {
        let date = session
            .get("date")
            .and_then(Value::as_str)
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            .ok_or_else(|| invalid("calendar date"))?;
        let close = session
            .get("close")
            .and_then(Value::as_str)
            .and_then(|s| {
                NaiveTime::parse_from_str(s, "%H:%M")
                    .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
                    .ok()
            })
            .ok_or_else(|| invalid("calendar close"))?;
        let close = New_York
            .from_local_datetime(&date.and_time(close))
            .single()
            .ok_or_else(|| invalid("ambiguous exchange close"))?
            .with_timezone(&Utc);
        if closes.insert(date, close).is_some() {
            return Err(invalid("duplicate calendar session"));
        }
    }
    Ok(closes)
}

impl AlpacaPaperEvidenceTransport {
    pub(super) async fn bounded_json(
        &self,
        url: &reqwest::Url,
    ) -> Result<Value, EvidenceAdapterError> {
        // 对 GET 仅在连接/超时错误下做最多四次短退避，HTTP policy/data 错误不重试；body
        // 有 8 MiB 上限后才 JSON decode，避免无限读取外部响应。
        // These are read-only GETs; use the same bounded connection retry as
        // the other Alpaca acquisition path. HTTP policy failures are not retried.
        let mut attempt = 1_u64;
        let response = loop {
            match self
                .client
                .get(url.clone())
                .header("APCA-API-KEY-ID", &self.key_id)
                .header("APCA-API-SECRET-KEY", &self.secret_key)
                .send()
                .await
            {
                Ok(response) => break response,
                Err(error) if attempt < 5 && (error.is_connect() || error.is_timeout()) => {
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                    attempt += 1;
                }
                Err(error) => {
                    return Err(EvidenceAdapterError::Transport(
                        if error.is_timeout() {
                            "provider request timeout"
                        } else if error.is_connect() {
                            "provider connection failed"
                        } else {
                            "provider request failed"
                        }
                        .to_owned(),
                    ))
                }
            }
        };
        classify_evidence_response(&response)?;
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|_| EvidenceAdapterError::Transport("provider body failed".to_owned()))?;
            if bytes.len().saturating_add(chunk.len()) > MAX_PROVIDER_BYTES {
                return Err(invalid("provider payload exceeds 8 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| invalid("provider JSON decode"))
    }

    pub(super) async fn acquire_session_bars(
        &self,
        resource: &str,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 输入→解析 bars 资源→clock/calendar cutoff→分页并去重→可选 corporate actions→
        // 生成 expected/observed coverage 再验证 OHLCV；缺失较早交易日可以保留。
        // 分页 token 重复、缺最后一根调整日线、越界 bar 都拒绝封存。
        let GovernedResource::AlpacaBars {
            asset,
            start,
            limit,
            raw_prices,
            end,
        } = GovernedResource::parse(EvidenceSource::Alpaca, resource)
            .map_err(|_| invalid("bars resource"))?
        else {
            return Err(invalid("bars resource"));
        };
        let feed = self.market_data_feed;
        if raw_prices && feed.is_none() {
            return Err(invalid("explicit equity feed required"));
        }
        let mut market_requests = Vec::new();
        let market = self
            .capture_stock(asset.symbol(), cutoff, feed, &mut market_requests)
            .await?;
        let end_date = end.unwrap_or(cutoff.with_timezone(&New_York).date_naive());
        let start_date = start.unwrap_or(end_date - Duration::days(400));
        if start_date > end_date
            || end_date > cutoff.with_timezone(&New_York).date_naive()
            || (end_date - start_date).num_days() > 730
        {
            return Err(invalid("bars window outside bounded cutoff"));
        }
        let calendar_url = reqwest::Url::parse_with_params(
            &format!("{}/v2/calendar", self.base_url),
            &[
                ("start", start_date.to_string()),
                (
                    "end",
                    (cutoff.with_timezone(&New_York).date_naive() + Duration::days(30)).to_string(),
                ),
            ],
        )
        .map_err(|_| invalid("calendar URL"))?;
        let calendar = self
            .capture_get(calendar_url.clone(), &mut market_requests)
            .await?;
        let all_closes = session_closes(&calendar)?;
        // Scheduled exchange closes are metadata, never future market observations.
        let forecast_session_closes = all_closes
            .iter()
            .filter(|(_, close)| **close > cutoff)
            .map(|(date, close)| (date.to_string(), *close))
            .collect::<BTreeMap<_, _>>();
        let closes = all_closes
            .iter()
            .filter_map(|(&date, &close)| {
                (close + Duration::minutes(ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES) <= cutoff)
                    .then_some((date, close))
            })
            .collect::<BTreeMap<_, _>>();
        let Some((&latest_session, _)) = closes.range(start_date..=end_date).next_back() else {
            return Err(EvidenceAdapterError::Pending(
                "no available completed session".to_owned(),
            ));
        };
        let adjustment = if raw_prices {
            BarAdjustment::Raw
        } else {
            BarAdjustment::All
        };
        let endpoint = format!("{}/v2/stocks/{}/bars", self.market_data_url, asset.symbol());
        let next_midnight = New_York
            .from_local_datetime(
                &latest_session
                    .succ_opt()
                    .ok_or_else(|| invalid("date overflow"))?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| invalid("midnight"))?,
            )
            .single()
            .ok_or_else(|| invalid("midnight timezone"))?
            .with_timezone(&Utc);
        let mut url = reqwest::Url::parse_with_params(
            &endpoint,
            &[
                ("timeframe", "1Day".to_owned()),
                ("limit", limit.to_string()),
                ("adjustment", adjustment.as_str().to_owned()),
                ("sort", if raw_prices { "asc" } else { "desc" }.to_owned()),
                (
                    "start",
                    New_York
                        .from_local_datetime(
                            &start_date
                                .and_hms_opt(0, 0, 0)
                                .ok_or_else(|| invalid("start time"))?,
                        )
                        .single()
                        .ok_or_else(|| invalid("start timezone"))?
                        .to_rfc3339(),
                ),
                (
                    "end",
                    (next_midnight - Duration::seconds(1))
                        .min(cutoff)
                        .to_rfc3339(),
                ),
                ("asof", end_date.to_string()),
            ],
        )
        .map_err(|_| invalid("bars URL"))?;
        if let Some(feed) = feed {
            url.query_pairs_mut().append_pair("feed", feed.as_str());
        }
        let source_uri = url.to_string();
        let mut pages = Vec::new();
        let mut bars = BTreeMap::<NaiveDate, Value>::new();
        let mut tokens = BTreeSet::new();
        let mut provider_has_more = false;
        let mut previous_date = None;
        for page_index in 0..MAX_BAR_PAGES {
            let page = self.capture_get(url.clone(), &mut market_requests).await?;
            for bar in page
                .get("bars")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("bars missing"))?
            {
                let timestamp = bar
                    .get("t")
                    .and_then(Value::as_str)
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .ok_or_else(|| invalid("bar timestamp"))?;
                let date = timestamp.with_timezone(&New_York).date_naive();
                if !closes.contains_key(&date) || date < start_date || date > latest_session {
                    return Err(invalid("bar outside completed market sessions"));
                }
                if bars.insert(date, bar.clone()).is_some() {
                    return Err(invalid("duplicate paginated bar"));
                }
                if previous_date.is_some_and(|previous| {
                    if raw_prices {
                        date < previous
                    } else {
                        date > previous
                    }
                }) {
                    return Err(invalid("bar pagination order differs from request"));
                }
                previous_date = Some(date);
            }
            let token = page
                .get("next_page_token")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            pages.push(serde_json::json!({"request_url":url.as_str(),"response":page}));
            let Some(token) = token else {
                break;
            };
            if !tokens.insert(token.clone()) {
                return Err(invalid("bar pagination exhausted or repeated token"));
            }
            if bars.len() >= usize::from(limit) {
                provider_has_more = true;
                break;
            }
            if page_index + 1 == MAX_BAR_PAGES {
                return Err(invalid("bar pagination exhausted or repeated token"));
            }
            url = reqwest::Url::parse(&source_uri).map_err(|_| invalid("bars URL"))?;
            url.query_pairs_mut().append_pair("page_token", &token);
        }
        if bars.is_empty() {
            return Err(EvidenceAdapterError::Pending(
                "completed session bars not published".to_owned(),
            ));
        }
        if !raw_prices
            && (bars.last_key_value().map(|(date, _)| *date) != Some(latest_session)
                || closes.range(start_date..=latest_session).count() < usize::from(limit))
        {
            return Err(EvidenceAdapterError::Pending(format!(
                "latest {limit} completed bars unavailable; expected through {latest_session}"
            )));
        }
        let mut corporate_actions = Value::Null;
        if raw_prices {
            let path = self.configured_path_for(&format!(
                "corporate_actions:{}:{start_date}:{end_date}",
                asset.symbol()
            ))?;
            let url = reqwest::Url::parse(&format!("{}{}", self.market_data_url, path))
                .map_err(|_| invalid("corporate actions URL"))?;
            corporate_actions = self.bounded_json(&url).await?;
        }
        let mut expected_closes = if raw_prices {
            closes
                .range(start_date..=latest_session)
                .map(|(&date, &close)| (date, close))
                .collect::<BTreeMap<_, _>>()
        } else {
            closes
                .range(start_date..=latest_session)
                .rev()
                .take(usize::from(limit))
                .map(|(&date, &close)| (date, close))
                .collect()
        };
        let mut selected = bars.into_iter().collect::<Vec<_>>();
        if raw_prices && (provider_has_more || selected.len() > usize::from(limit)) {
            let through = selected[usize::from(limit) - 1].0;
            expected_closes.retain(|date, _| *date <= through);
        }
        let outside_window_dates = selected
            .iter()
            .filter(|(date, _)| !expected_closes.contains_key(date))
            .map(|(date, _)| *date)
            .collect();
        selected.retain(|(date, _)| expected_closes.contains_key(date));
        let observed_closes = selected
            .iter()
            .map(|(date, _)| (*date, expected_closes[date]))
            .collect::<BTreeMap<_, _>>();
        // Retain all returned pages in RawEvidence, including rows outside the
        // selected calendar window. Only actually observed closes set availability.
        let content_available_at = observed_closes
            .last_key_value()
            .map(|(_, close)| *close + Duration::minutes(ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES))
            .ok_or_else(|| invalid("no selected bars"))?;
        let coverage = BarSessionCoverage {
            version: 1,
            observed_dates: observed_closes.keys().copied().collect(),
            missing_dates: expected_closes
                .keys()
                .filter(|date| !observed_closes.contains_key(date))
                .copied()
                .collect(),
            outside_window_dates,
            expected_closes,
            feed,
            adjustment,
            provider_revision: None,
            calendar_source: calendar_url.to_string(),
            cutoff,
            requested_limit: limit,
            provider_has_more,
        };
        let quality = EvidenceQuality {
            completeness_ppm: (coverage.observed_dates.len() as u32 * 1_000_000)
                / coverage.expected_closes.len() as u32,
            ..EvidenceQuality::default()
        };
        let observed_at = Utc::now();
        let mut normalized = serde_json::json!({
            "market": market, "feed": feed,
            "request_url": source_uri, "timezone": "America/New_York", "decision_cutoff": cutoff,
            "symbol": asset.symbol(), "bars": selected.iter().map(|(_, bar)| bar).collect::<Vec<_>>(),
            "price_basis": adjustment.price_basis(),
            "corporate_actions_response": corporate_actions,
            "session_closes": observed_closes,
            "bar_session_coverage": coverage,
            "content_available_at": content_available_at, "latest_completed_session": latest_session,
            "forecast_session_closes": forecast_session_closes,
            "calendar_source": calendar_url.as_str(), "availability_lag_minutes": ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES,
        });
        if !raw_prices {
            // Keep the scheduled dates even when their closes precede a late acquisition cutoff.
            normalized["forecast_calendar"] = serde_json::json!({
                "version": 1,
                "availability_lag_minutes": ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES,
                "session_closes": all_closes,
            });
        }
        validate_daily_bar_payload(&normalized).map_err(|_| invalid("OHLCV invalid"))?;
        let raw = serde_json::to_vec(&serde_json::json!({"market_requests":market_requests,"bar_request_url":source_uri,"calendar_request_url":calendar_url.as_str(),"calendar": calendar, "pages": pages, "corporate_actions": corporate_actions}))
            .map_err(|_| invalid("provider payload serialization"))?;
        Ok(AcquiredEvidence {
            provenance: EvidenceProvenance {
                document_id: Some(resource.to_owned()),
                published_at: None,
                observed_at,
                revision: None,
                source_uri: source_uri.clone(),
                dedupe_key: format!("alpaca:{}", ContentHash::of_bytes(&raw)),
                citations: vec![],
            },
            raw,
            media_type: "application/json".to_owned(),
            source_uri,
            observed_at,
            normalized,
            quality,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn acquire_fixture(
        feed: Option<AlpacaMarketDataFeed>,
        resource: &str,
        pages: Vec<Value>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut pages = pages.into_iter();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.ends_with(b"\r\n\r\n") {
                    let bytes = stream.read(&mut buffer).await.unwrap();
                    assert!(bytes > 0);
                    request.extend_from_slice(&buffer[..bytes]);
                }
                let request = std::str::from_utf8(&request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap();
                let body = match path.split('?').next().unwrap() {
                    "/v2/assets/QQQ" => {
                        serde_json::json!({"symbol":"QQQ", "status":"active", "tradable":true})
                    }
                    "/v2/calendar" => serde_json::json!([
                        {"date":"2026-11-24", "close":"16:00"},
                        {"date":"2026-11-25", "close":"16:00"},
                        {"date":"2026-11-27", "close":"13:00"},
                        {"date":"2026-11-30", "close":"16:00"}
                    ]),
                    "/v2/stocks/QQQ/bars" => {
                        let url = Url::parse(&format!("http://{address}{path}")).unwrap();
                        assert_eq!(
                            url.query_pairs()
                                .find(|(key, _)| key == "feed")
                                .map(|(_, value)| value.to_string()),
                            feed.map(|value| value.as_str().to_owned())
                        );
                        pages.next().unwrap()
                    }
                    "/v1/corporate-actions" => serde_json::json!({"corporate_actions":{}}),
                    _ => serde_json::json!({}),
                }
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let mut transport = AlpacaPaperEvidenceTransport::new(
            "https://paper-api.alpaca.markets",
            "fixture",
            "fixture",
            feed,
        )
        .unwrap();
        transport.base_url = format!("http://{address}");
        transport.market_data_url = transport.base_url.clone();
        let request = EvidenceRequest {
            source: EvidenceSource::Alpaca,
            resource: resource.to_owned(),
            max_age: Duration::days(7),
            acquisition_mode: akzio_domain::EvidenceAcquisitionMode::VerifiedSource,
        };
        let result = transport
            .acquire_at(&request, "2026-11-30T22:00:00Z".parse().unwrap())
            .await;
        server.abort();
        result
    }

    fn bar(date: &str, volume: u64) -> Value {
        serde_json::json!({"t":format!("{date}T05:00:00Z"), "o":10, "h":12, "l":9, "c":11, "v":volume})
    }

    /// WHY THIS TEST EXISTS (Task #33): The real collector can receive the requested
    /// count while the middle expected exchange session is missing.
    #[tokio::test]
    async fn governed_bars_preserve_expected_dates_even_when_older_rows_fill_limit() {
        for feed in [AlpacaMarketDataFeed::Iex, AlpacaMarketDataFeed::Sip] {
            let evidence = acquire_fixture(
                Some(feed),
                "bars:QQQ:1d:2026-11-24:3",
                vec![serde_json::json!({"bars":[
                    bar("2026-11-30", 100), bar("2026-11-25", 100), bar("2026-11-24", 100)
                ]})],
            )
            .await
            .unwrap();
            let coverage = &evidence.normalized["bar_session_coverage"];
            assert_eq!(coverage["missing_dates"], serde_json::json!(["2026-11-27"]));
            assert_eq!(coverage["feed"], feed.as_str());
            assert_eq!(
                coverage["expected_closes"]["2026-11-27"],
                "2026-11-27T18:00:00Z"
            );
            assert_eq!(
                coverage["observed_dates"],
                serde_json::json!(["2026-11-25", "2026-11-30"])
            );
            assert_eq!(
                coverage["outside_window_dates"],
                serde_json::json!(["2026-11-24"])
            );
            assert!(coverage["provider_revision"].is_null());
            assert_eq!(evidence.quality.completeness_ppm, 666_666);
            assert_eq!(evidence.normalized["bars"].as_array().unwrap().len(), 2);
            let features = crate::quant_features::build_quant_feature_snapshot(
                &evidence.normalized,
                "bars:QQQ:1d:2026-11-24:3",
                &evidence.source_uri,
                &akzio_domain::DecisionClock {
                    decision_cutoff: "2026-11-30T22:00:00Z".parse().unwrap(),
                },
            )
            .unwrap();
            assert_eq!(features.return_1d_ppm, None);
            assert_eq!(features.maximum_drawdown_ppm, None);
            let raw: Value = serde_json::from_slice(&evidence.raw).unwrap();
            assert_eq!(
                raw["pages"][0]["response"]["bars"]
                    .as_array()
                    .unwrap()
                    .len(),
                3
            );
        }
    }

    /// WHY THIS TEST EXISTS (Task #33): PositionPlan may omit a feed. Its
    /// adjusted bars still need usable daily features and honest feed provenance.
    #[tokio::test]
    async fn research_bars_allow_provider_default_without_claiming_its_feed() {
        let resource = "bars:QQQ:1d:2026-11-24:3";
        let bars = serde_json::json!({"bars":[bar("2026-11-30", 100), bar("2026-11-27", 100), bar("2026-11-25", 100)]});
        let evidence = acquire_fixture(None, resource, vec![bars]).await.unwrap();
        assert!(evidence.normalized["feed"].is_null());
        assert!(evidence.normalized["bar_session_coverage"]["feed"].is_null());
        let features = crate::quant_features::build_quant_feature_snapshot(
            &evidence.normalized,
            resource,
            &evidence.source_uri,
            &akzio_domain::DecisionClock {
                decision_cutoff: "2026-11-30T22:00:00Z".parse().unwrap(),
            },
        )
        .unwrap();
        assert_eq!(features.feed, None);
        assert_eq!(features.return_1d_ppm, Some(0));
        let raw_without_feed =
            acquire_fixture(None, "bars:QQQ:1d:2026-11-24:252:raw:2026-11-27", vec![]).await;
        assert!(
            matches!(raw_without_feed, Err(EvidenceAdapterError::DataQuality(message)) if message == "explicit equity feed required")
        );
    }

    /// WHY THIS TEST EXISTS (Task #33): Outcome-bound raw coverage must freeze
    /// the requested historical span even if the capture clock is on a later date.
    #[tokio::test]
    async fn governed_raw_bars_keep_outcome_prices_and_freeze_requested_calendar() {
        let mut final_bar = bar("2026-11-27", 100);
        final_bar["t"] = "2026-11-28T02:00:00Z".into();
        let evidence = acquire_fixture(
            Some(AlpacaMarketDataFeed::Sip),
            "bars:QQQ:1d:2026-11-24:252:raw:2026-11-27",
            vec![serde_json::json!({"bars":[bar("2026-11-24", 100), final_bar]})],
        )
        .await
        .unwrap();
        let coverage = BarSessionCoverage::read(&evidence.normalized)
            .unwrap()
            .unwrap();
        assert_eq!(coverage.adjustment, BarAdjustment::Raw);
        assert_eq!(coverage.feed, Some(AlpacaMarketDataFeed::Sip));
        assert_eq!(coverage.expected_closes.len(), 3);
        assert_eq!(coverage.missing_dates[0].to_string(), "2026-11-25");
        let decoded = crate::parse_daily_bars(&evidence.normalized, evidence.observed_at).unwrap();
        assert_eq!(
            decoded.keys().copied().collect::<Vec<_>>(),
            coverage.observed_dates
        );
        assert_eq!(
            evidence.normalized["latest_completed_session"],
            "2026-11-27"
        );
        assert_eq!(
            evidence.normalized["content_available_at"],
            "2026-11-27T18:20:00Z"
        );
        assert_eq!(
            evidence.normalized["session_closes"]
                .as_object()
                .unwrap()
                .len(),
            2
        );
    }

    /// WHY THIS TEST EXISTS (Task #33): Volume, duplicate sessions and pagination
    /// faults must fail before creating a formally covered history.
    #[tokio::test]
    async fn governed_bars_reject_invalid_volume_and_pagination() {
        let cases = [
            (
                vec![
                    serde_json::json!({"bars":[bar("2026-11-30", 0), bar("2026-11-27", 100), bar("2026-11-25", 100)]}),
                ],
                "OHLCV invalid",
            ),
            (
                vec![
                    serde_json::json!({"bars":[bar("2026-11-30", 100)], "next_page_token":"first"}),
                    serde_json::json!({"bars":[bar("2026-11-30", 200)], "next_page_token":"second"}),
                ],
                "duplicate paginated bar",
            ),
            (
                vec![
                    serde_json::json!({"bars":[bar("2026-11-30", 100)], "next_page_token":"first"}),
                    serde_json::json!({"bars":[bar("2026-11-27", 100)], "next_page_token":"first"}),
                ],
                "bar pagination exhausted or repeated token",
            ),
            (
                vec![
                    serde_json::json!({"bars":[bar("2026-11-27", 100)], "next_page_token":"first"}),
                    serde_json::json!({"bars":[bar("2026-11-30", 100)]}),
                ],
                "bar pagination order differs from request",
            ),
        ];
        for (pages, message) in cases {
            let error = acquire_fixture(
                Some(AlpacaMarketDataFeed::Iex),
                "bars:QQQ:1d:2026-11-24:3",
                pages,
            )
            .await
            .unwrap_err();
            assert!(matches!(error, EvidenceAdapterError::DataQuality(value) if value == message));
        }
    }
}
