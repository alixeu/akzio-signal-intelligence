//! Session-aware, bounded Alpaca daily-bar acquisition. Calendar and each page
//! are retained in RawEvidence; normalized prices carry their availability basis.
use super::*;
use chrono::{NaiveDate, NaiveTime, TimeZone};
use chrono_tz::America::New_York;

const BAR_AVAILABILITY_LAG_MINUTES: i64 = 20;
const MAX_BAR_PAGES: usize = 16;
const MAX_PROVIDER_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn classify_evidence_response(
    response: &reqwest::Response,
) -> Result<(), EvidenceAdapterError> {
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
    EvidenceAdapterError::DataQuality(message.to_owned())
}

/// The acquisition range finds common sessions; only this valuation range can
/// invalidate a frozen stage. Dates follow Alpaca's corporate-action models.
pub fn validate_outcome_price_window(
    normalized: &Value,
    baseline: NaiveDate,
    through: NaiveDate,
) -> Result<(), EvidenceAdapterError> {
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
        let mut market_requests = Vec::new();
        let market = self
            .capture_stock(asset.symbol(), cutoff, &mut market_requests)
            .await?;
        let end_date = end.unwrap_or(cutoff.with_timezone(&New_York).date_naive());
        let start_date = start.unwrap_or(end_date - Duration::days(400));
        if end_date > cutoff.with_timezone(&New_York).date_naive()
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
            .into_iter()
            .filter(|(_, close)| *close + Duration::minutes(BAR_AVAILABILITY_LAG_MINUTES) <= cutoff)
            .collect::<BTreeMap<_, _>>();
        let Some((&latest_session, &latest_close)) = closes.last_key_value() else {
            return Err(EvidenceAdapterError::Pending(
                "no available completed session".to_owned(),
            ));
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
                (
                    "adjustment",
                    if raw_prices { "raw" } else { "all" }.to_owned(),
                ),
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
        if let Some(feed) = self.market_data_feed {
            url.query_pairs_mut().append_pair("feed", feed.as_str());
        }
        let source_uri = url.to_string();
        let mut pages = Vec::new();
        let mut bars = BTreeMap::<NaiveDate, Value>::new();
        let mut tokens = BTreeSet::new();
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
            }
            let token = page
                .get("next_page_token")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            pages.push(serde_json::json!({"request_url":url.as_str(),"response":page}));
            if bars.len() >= usize::from(limit) || token.is_none() {
                break;
            }
            let token = token.unwrap();
            if !tokens.insert(token.clone()) || page_index + 1 == MAX_BAR_PAGES {
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
                || bars.len() < usize::from(limit))
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
        let mut selected = bars.into_iter().collect::<Vec<_>>();
        if raw_prices {
            selected.truncate(usize::from(limit));
        } else if selected.len() > usize::from(limit) {
            selected = selected.split_off(selected.len() - usize::from(limit));
        }
        let content_available_at = selected
            .iter()
            .filter_map(|(day, _)| closes.get(day))
            .max()
            .copied()
            .unwrap_or(latest_close)
            + Duration::minutes(BAR_AVAILABILITY_LAG_MINUTES);
        let observed_at = Utc::now();
        let normalized = serde_json::json!({
            "market": market, "feed": self.market_data_feed.map(|f| f.as_str()),
            "request_url": source_uri, "timezone": "America/New_York", "decision_cutoff": cutoff,
            "symbol": asset.symbol(), "bars": selected.iter().map(|(_, bar)| bar).collect::<Vec<_>>(),
            "price_basis": if raw_prices { "raw_requires_stage_action_check" } else { "adjusted_research" },
            "corporate_actions_response": corporate_actions,
            "session_closes": selected.iter().map(|(date, _)| (date.to_string(), closes[date])).collect::<BTreeMap<_, _>>(),
            "content_available_at": content_available_at, "latest_completed_session": latest_session,
            "forecast_session_closes": forecast_session_closes,
            "calendar_source": calendar_url.as_str(), "availability_lag_minutes": BAR_AVAILABILITY_LAG_MINUTES,
        });
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
            quality: EvidenceQuality::default(),
        })
    }
}
