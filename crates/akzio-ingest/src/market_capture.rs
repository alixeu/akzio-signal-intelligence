//! Bounded market captures. Only provider fields enter evidence; every GET,
//! feed, page and rejection is retained in the raw request ledger.
use super::*;
use chrono::NaiveDate;
use chrono_tz::America::New_York;

const MAX_CONTRACTS: usize = 512;
const MAX_PAGES: usize = 4;
const PAGE_SIZE: usize = 128;
// Alpaca latest option quotes accepts at most 100 symbols per request.
const QUOTE_BATCH: usize = 100;
const MAX_SUPPLEMENTS: usize = 6;

fn invalid(message: &str) -> EvidenceAdapterError {
    // 适配器发现响应形状或业务字段不可信时统一返回 DataQuality，调用方仍可把请求记录保留在 ledger。
    EvidenceAdapterError::DataQuality(message.to_owned())
}

fn timestamp(value: &Value) -> Option<DateTime<Utc>> {
    // Alpaca 的不同组件可能使用 t 或 timestamp；缺字段、非字符串或不可解析时返回 None，不借用采集时间。
    value
        .get("t")
        .or_else(|| value.get("timestamp"))?
        .as_str()?
        .parse()
        .ok()
}

fn positive(value: &Value) -> Option<f64> {
    // 价格和隐含波动率只接受有限正数；Option 让缺失或非法数值在后续 coverage 中保持未知。
    value.as_f64().filter(|v| v.is_finite() && *v > 0.0)
}

fn has_quote(snapshot: &Value) -> bool {
    // 合格 quote 需要 bid、正 ask 和 provider 时间戳；只有结构完整才计入 bid/ask coverage。
    snapshot.get("latestQuote").is_some_and(|q| {
        q.get("bp")
            .and_then(Value::as_f64)
            .is_some_and(|p| p >= 0.0)
            && q.get("ap").and_then(positive).is_some()
            && timestamp(q).is_some()
    })
}

/// Component times are independent. Never replace them with retrieval time.
fn filter_snapshot(value: &mut Value, cutoff: DateTime<Utc>) -> usize {
    // 每个市场组件按自己的 provider 时间和同一个 cutoff 独立过滤；拒绝任一组件时同步清除依赖它的 Greeks/IV。
    let mut rejected = 0;
    for key in [
        "latestQuote",
        "latestTrade",
        "minuteBar",
        "dailyBar",
        "prevDailyBar",
    ] {
        if let Some(component) = value.get(key) {
            let at = timestamp(component);
            if at.is_none_or(|at| at > cutoff) {
                rejected += usize::from(at.is_some_and(|at| at > cutoff));
                value.as_object_mut().unwrap().remove(key);
            }
        }
    }
    if rejected > 0 {
        value.as_object_mut().unwrap().remove("impliedVolatility");
        value.as_object_mut().unwrap().remove("greeks");
    }
    rejected
}

impl AlpacaPaperEvidenceTransport {
    pub(super) async fn capture_get(
        &self,
        url: Url,
        ledger: &mut Vec<Value>,
    ) -> Result<Value, EvidenceAdapterError> {
        // 每次 GET 都先记 requested_at，成功或失败都写入原始请求账本；Result 保留 provider 错误给上层分类。
        let requested_at = Utc::now();
        match self.bounded_json(&url).await {
            Ok(value) => {
                ledger.push(serde_json::json!({"request_url":url.as_str(),
                    "requested_at":requested_at,"retrieved_at":Utc::now(),"response":value}));
                Ok(value)
            }
            Err(error) => {
                let class = match &error {
                    EvidenceAdapterError::Unauthorized(_) => "entitlement_or_permission",
                    EvidenceAdapterError::Permanent(_) => "request_error",
                    EvidenceAdapterError::DataQuality(_) => "field_projection_error",
                    _ => "transport_error",
                };
                ledger.push(serde_json::json!({"request_url":url.as_str(),
                    "requested_at":requested_at,"retrieved_at":Utc::now(),"error_class":class,"error":error.to_string()}));
                Err(error)
            }
        }
    }

    pub(super) async fn capture_stock(
        &self,
        symbol: &str,
        cutoff: DateTime<Utc>,
        ledger: &mut Vec<Value>,
    ) -> Result<Value, EvidenceAdapterError> {
        // 股票证据来自显式选择的 Alpaca feed；先验证资产状态，再采集 clock、calendar 和各市场组件。
        let feed = self
            .market_data_feed
            .ok_or_else(|| invalid("explicit equity feed required"))?
            .as_str();
        let asset_url = Url::parse(&format!("{}/v2/assets/{symbol}", self.base_url))
            .map_err(|_| invalid("asset URL"))?;
        let asset = self.capture_get(asset_url, ledger).await?;
        if asset["symbol"] != symbol || asset["status"] != "active" || asset["tradable"] != true {
            return Err(invalid("asset must be active and tradable"));
        }
        let clock_url =
            Url::parse(&format!("{}/v2/clock", self.base_url)).map_err(|_| invalid("clock URL"))?;
        let clock = self.capture_get(clock_url, ledger).await?;
        let day = cutoff.with_timezone(&New_York).date_naive();
        let calendar_url = Url::parse_with_params(
            &format!("{}/v2/calendar", self.base_url),
            &[
                ("start", (day - Duration::days(14)).to_string()),
                ("end", day.to_string()),
            ],
        )
        .map_err(|_| invalid("calendar URL"))?;
        let calendar = self.capture_get(calendar_url, ledger).await?;
        // 日历 close 决定历史日线何时完整；clock 只描述当前状态，不能替代组件自己的时间戳。
        let closes = super::session_bars::session_closes(&calendar)?;
        let last = closes
            .iter()
            .rfind(|(_, close)| **close <= cutoff)
            .map(|(day, close)| (*day, *close));
        let mut result = serde_json::json!({"asset":asset,"clock":clock,"calendar":calendar,
            "feed":feed,"timezone":"America/New_York","decision_cutoff":cutoff,
            "latest_completed_session":last.map(|(day,_)|day),
            "market_state":if clock["is_open"] == false {"closed_last_available_data"} else {"open"},
            "rejected_after_cutoff":0,"errors":[]});
        let mut rejected = 0;
        for (path, name, envelope) in [
            ("snapshot", "snapshot", None),
            ("quotes/latest", "latest_quote", Some("quote")),
            ("trades/latest", "latest_trade", Some("trade")),
        ] {
            // snapshot、latest quote、latest trade 走不同 provider 端点；统一在这里按 cutoff 过滤并保留各自错误。
            let url = Url::parse_with_params(
                &format!("{}/v2/stocks/{symbol}/{path}", self.market_data_url),
                &[("feed", feed)],
            )
            .map_err(|_| invalid("stock URL"))?;
            match self.capture_get(url, ledger).await {
                Ok(payload) => {
                    let mut value = envelope.map_or(payload.clone(), |key| payload[key].clone());
                    if envelope.is_some() {
                        if timestamp(&value).is_none_or(|at| at > cutoff) {
                            rejected +=
                                usize::from(timestamp(&value).is_some_and(|at| at > cutoff));
                            value = Value::Null;
                        }
                    } else {
                        rejected += filter_snapshot(&mut value, cutoff);
                        // Daily bars label session start; complete content is available after close.
                        for key in ["dailyBar", "prevDailyBar"] {
                            if timestamp(&value[key]).is_some_and(|at| {
                                closes
                                    .get(&at.with_timezone(&New_York).date_naive())
                                    .is_none_or(|close| *close + Duration::minutes(20) > cutoff)
                            }) {
                                value.as_object_mut().unwrap().remove(key);
                            }
                        }
                    }
                    result[name] = value;
                }
                Err(error) => result["errors"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({"component":name,"error":error.to_string()})),
            }
        }
        result["rejected_after_cutoff"] = rejected.into();
        Ok(result)
    }

    pub(super) async fn acquire_option_capture(
        &self,
        resource: &str,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 先把字符串资源解析成 typed option chain；解析失败或资源类型不符都在任何网络请求前返回 Result 错误。
        let GovernedResource::AlpacaOptionChain {
            asset,
            expiration_start,
            expiration_end,
        } = GovernedResource::parse(EvidenceSource::Alpaca, resource)
            .map_err(|_| invalid("option resource"))?
        else {
            return Err(invalid("option resource"));
        };
        let mut ledger = Vec::new();
        let stock = self
            .capture_stock(asset.symbol(), cutoff, &mut ledger)
            .await?;
        let price = stock
            .pointer("/latest_trade/p")
            .and_then(positive)
            .or_else(|| stock.pointer("/snapshot/dailyBar/c").and_then(positive))
            .ok_or_else(|| {
                invalid("bounded option strikes require a cutoff-valid underlying trade or bar")
            })?;
        // 期权到期日最多扩展 30 天，行权价围绕 cutoff-valid 的标的价格限定在 90%--110%。
        let end = expiration_end.min(expiration_start + Duration::days(30));
        let low = price * 0.9;
        let high = price * 1.1;
        let filters = [
            ("expiration_date_gte", expiration_start.to_string()),
            ("expiration_date_lte", end.to_string()),
            ("strike_price_gte", format!("{low:.4}")),
            ("strike_price_lte", format!("{high:.4}")),
            ("limit", PAGE_SIZE.to_string()),
        ];
        let mut contract_url =
            Url::parse_with_params(&format!("{}/v2/options/contracts", self.base_url), &filters)
                .map_err(|_| invalid("contracts URL"))?;
        contract_url
            .query_pairs_mut()
            .append_pair("underlying_symbols", asset.symbol())
            .append_pair("status", "active");
        let mut contracts = BTreeMap::new();
        let mut truncated = false;
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        for page_index in 0..MAX_PAGES {
            // cursor 是 Option：只有 provider 返回非空 page token 才继续分页，并用 seen 防止循环。
            let mut url = contract_url.clone();
            if let Some(ref token) = cursor {
                url.query_pairs_mut().append_pair("page_token", token);
            }
            let page = self.capture_get(url, &mut ledger).await?;
            for contract in page["option_contracts"]
                .as_array()
                .ok_or_else(|| invalid("option_contracts field missing"))?
            {
                let symbol = contract["symbol"]
                    .as_str()
                    .ok_or_else(|| invalid("OCC symbol missing"))?;
                let strike = contract["strike_price"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| contract["strike_price"].as_f64())
                    .ok_or_else(|| invalid("strike missing"))?;
                let expiration: NaiveDate = contract["expiration_date"]
                    .as_str()
                    .ok_or_else(|| invalid("expiration missing"))?
                    .parse()
                    .map_err(|_| invalid("expiration invalid"))?;
                if contract["status"] != "active"
                    || contract["underlying_symbol"] != asset.symbol()
                    || !(expiration_start..=end).contains(&expiration)
                    || strike < low
                    || strike > high
                {
                    return Err(invalid("contracts response violates requested filters"));
                }
                if contracts
                    .insert(symbol.to_owned(), contract.clone())
                    .is_some()
                {
                    return Err(invalid("duplicate contract"));
                }
                if contracts.len() >= MAX_CONTRACTS {
                    break;
                }
            }
            cursor = page["next_page_token"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            if cursor.is_none() {
                break;
            }
            if !seen.insert(cursor.clone()) {
                return Err(invalid("repeated contracts cursor"));
            }
            if contracts.len() >= MAX_CONTRACTS || page_index + 1 == MAX_PAGES {
                truncated = true;
                break;
            }
        }
        // A feed choice is explicit and frozen for the entire capture. No fallback.
        let feed = self.option_data_feed.as_str();
        let mut base = Url::parse_with_params(
            &format!(
                "{}/v1beta1/options/snapshots/{}",
                self.market_data_url,
                asset.symbol()
            ),
            &filters,
        )
        .map_err(|_| invalid("chain URL"))?;
        base.query_pairs_mut().append_pair("feed", feed);
        let mut snapshots = serde_json::Map::new();
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut errors = Vec::new();
        let mut permission_denied = false;
        for page_index in 0..MAX_PAGES {
            // 行情链沿用同一个显式 feed；权限拒绝会停止补采并在 coverage 标成 permission_denied。
            let mut url = base.clone();
            if let Some(ref token) = cursor {
                url.query_pairs_mut().append_pair("page_token", token);
            }
            let page = match self.capture_get(url, &mut ledger).await {
                Ok(v) => v,
                Err(e) => {
                    permission_denied = matches!(e, EvidenceAdapterError::Unauthorized(_));
                    truncated = true;
                    errors.push(e.to_string());
                    break;
                }
            };
            let rows = page["snapshots"]
                .as_object()
                .ok_or_else(|| invalid("chain snapshots field missing"))?;
            for (symbol, snapshot) in rows {
                if contracts.contains_key(symbol) {
                    snapshots.insert(symbol.clone(), snapshot.clone());
                }
            }
            cursor = page["next_page_token"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            if cursor.is_none() {
                break;
            }
            if !seen.insert(cursor.clone()) {
                return Err(invalid("repeated chain cursor"));
            }
            if page_index + 1 == MAX_PAGES {
                truncated = true;
            }
        }
        let missing = contracts
            .keys()
            .filter(|s| !snapshots.get(*s).is_some_and(has_quote))
            .cloned()
            .collect::<Vec<_>>();
        // 缺 quote 的合约按 provider 批量补采；权限已拒绝时不重试，并且所有补采仍受固定次数上限约束。
        let mut supplemental_requests = 0;
        for batch in missing.chunks(QUOTE_BATCH).take(if permission_denied {
            0
        } else {
            MAX_SUPPLEMENTS
        }) {
            supplemental_requests += 1;
            let url = Url::parse_with_params(
                &format!("{}/v1beta1/options/quotes/latest", self.market_data_url),
                &[("symbols", batch.join(",")), ("feed", feed.to_owned())],
            )
            .map_err(|_| invalid("option quote URL"))?;
            match self.capture_get(url, &mut ledger).await {
                Ok(page) => {
                    for (symbol, quote) in page["quotes"]
                        .as_object()
                        .ok_or_else(|| invalid("option quotes field missing"))?
                    {
                        if batch.contains(symbol) {
                            snapshots
                                .entry(symbol.clone())
                                .or_insert_with(|| serde_json::json!({}))["latestQuote"] =
                                quote.clone();
                        }
                    }
                }
                Err(e) => {
                    let denied = matches!(e, EvidenceAdapterError::Unauthorized(_));
                    truncated = true;
                    errors.push(e.to_string());
                    if denied {
                        break;
                    }
                }
            }
        }
        let mut rejected = 0;
        let mut stale = 0;
        let mut trade = 0;
        let mut bid_ask = 0;
        let mut iv = 0;
        let mut greeks = 0;
        let mut oi = 0;
        for (symbol, snapshot) in &mut snapshots {
            // 将 contract 元数据和 provider 时间戳合并到规范化快照；缺少 IV/Greeks 独立时间戳时明确保留 unknown。
            rejected += usize::from(filter_snapshot(snapshot, cutoff) > 0);
            let contract = &contracts[symbol];
            snapshot["expiration_date"] = contract["expiration_date"].clone();
            snapshot["feed"] = feed.into();
            // Open interest is a dated contracts observation, never a chain default.
            let oi_date = contract["open_interest_date"]
                .as_str()
                .and_then(|s| s.parse::<NaiveDate>().ok());
            let oi_value = contract["open_interest"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .or_else(|| contract["open_interest"].as_u64());
            if oi_date.is_some_and(|day| day < cutoff.with_timezone(&New_York).date_naive())
                && oi_value.is_some()
            {
                snapshot["open_interest"] = serde_json::json!(oi_value);
                snapshot["open_interest_date"] = contract["open_interest_date"].clone();
                oi += 1;
            }
            snapshot["quote_timestamp"] = snapshot
                .pointer("/latestQuote/t")
                .cloned()
                .unwrap_or(Value::Null);
            snapshot["trade_timestamp"] = snapshot
                .pointer("/latestTrade/t")
                .cloned()
                .unwrap_or(Value::Null);
            snapshot["iv_timestamp"] = Value::Null;
            snapshot["greeks_timestamp"] = Value::Null;
            snapshot["derived_fields_temporal_status"] =
                "provider_does_not_supply_timestamp_unknown".into();
            trade += usize::from(snapshot.get("latestTrade").is_some());
            bid_ask += usize::from(has_quote(snapshot));
            iv += usize::from(
                snapshot
                    .get("impliedVolatility")
                    .and_then(positive)
                    .is_some(),
            );
            greeks += usize::from(snapshot.get("greeks").is_some_and(Value::is_object));
            // Absolute-age diagnostic, including closed sessions; never labelled realtime.
            stale += usize::from(
                timestamp(&snapshot["latestQuote"])
                    .is_none_or(|t| cutoff - t > Duration::minutes(15)),
            );
        }
        let coverage = serde_json::json!({"contracts_requested":contracts.len(),"contracts_returned":snapshots.len(),
            "contracts_with_trade":trade,"contracts_with_bid_ask":bid_ask,"contracts_with_iv":iv,
            "contracts_with_greeks":greeks,"contracts_with_open_interest":oi,"stale_contracts":stale,
            "rejected_after_cutoff":rejected,"feed":feed,"errors":errors,
            "entitlement_permission_error":ledger.iter().any(|r|r["error_class"]=="entitlement_or_permission"),
            "pagination_complete":!truncated,"supplemental_requests":supplemental_requests,
            "missing_quote_contracts":contracts.len().saturating_sub(bid_ask),
            "collection_status":if ledger.iter().any(|r|r["error_class"]=="entitlement_or_permission") {"permission_denied"}
                else if !errors.is_empty() {"provider_error"} else if snapshots.is_empty() {"no_market_data"}
                else if truncated {"bounded_partial"} else {"available"},
            "iv_greeks_timestamp_status":"unknown; provider supplies no independent timestamps",
            "stale_threshold_seconds":900});
        // normalized 是受控 Context 投影，raw 只保存完整请求/响应账本；两者都带上分页、权限和截断状态。
        let normalized = serde_json::json!({"snapshots":snapshots,"coverage":coverage,"feed":feed,
            "underlying":stock,"decision_cutoff":cutoff,"pagination_complete":!truncated,
            "bounds":{"expiration_start":expiration_start,"expiration_end":end,"strike_price_gte":low,"strike_price_lte":high,
                "max_pages":MAX_PAGES,"max_contracts":MAX_CONTRACTS,"quote_batch_limit":QUOTE_BATCH,"max_supplemental_requests":MAX_SUPPLEMENTS},
            "requests":ledger.iter().map(|v|serde_json::json!({"url":v["request_url"],"requested_at":v["requested_at"],"retrieved_at":v["retrieved_at"],"error_class":v["error_class"]})).collect::<Vec<_>>()});
        let raw = serde_json::to_vec(&serde_json::json!({"requests":ledger}))
            .map_err(|_| invalid("capture serialization"))?;
        let observed_at = Utc::now();
        let source_uri = base.to_string();
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
            media_type: "application/json".into(),
            source_uri,
            observed_at,
            normalized,
            quality: EvidenceQuality::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cutoff_filters_each_component_without_turning_quote_into_trade() {
        let cutoff = "2026-09-18T20:00:00Z".parse().unwrap();
        let mut snapshot = serde_json::json!({"latestQuote":{"t":"2026-09-18T19:59:00Z","bp":1,"ap":2},
            "latestTrade":{"t":"2026-09-18T20:00:01Z","p":3},"impliedVolatility":0.3});
        assert_eq!(filter_snapshot(&mut snapshot, cutoff), 1);
        assert!(has_quote(&snapshot));
        assert!(snapshot.get("latestTrade").is_none());
        assert!(snapshot.get("impliedVolatility").is_none());
    }
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bounded_capture_pages_contracts_and_supplements_same_feed_by_occ() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut paths = Vec::new();
            for _ in 0..10 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 8192];
                let count = stream.read(&mut buffer).await.unwrap();
                let request = std::str::from_utf8(&buffer[..count]).unwrap();
                let path = request.split_whitespace().nth(1).unwrap();
                paths.push(path.to_owned());
                let value = if path.starts_with("/v2/assets/") {
                    serde_json::json!({"symbol":"TQQQ","status":"active","tradable":true})
                } else if path == "/v2/clock" {
                    serde_json::json!({"is_open":false,"timestamp":"2026-09-20T12:00:01Z"})
                } else if path.starts_with("/v2/calendar") {
                    serde_json::json!([{"date":"2026-09-18","open":"09:30","close":"16:00"}])
                } else if path.starts_with("/v2/stocks/") {
                    assert!(path.contains("feed=iex"));
                    if path.contains("trades/latest") {
                        serde_json::json!({"trade":{"p":100,"t":"2026-09-18T20:00:00Z"}})
                    } else if path.contains("quotes/latest") {
                        serde_json::json!({"quote":{"bp":99,"ap":101,"t":"2026-09-20T12:00:01Z"}})
                    } else {
                        serde_json::json!({"latestTrade":{"p":100,"t":"2026-09-18T20:00:00Z"}})
                    }
                } else if path.starts_with("/v2/options/contracts") {
                    assert!(path.contains("status=active") && path.contains("strike_price_gte=90"));
                    let second = path.contains("page_token=second");
                    serde_json::json!({"option_contracts":[{"symbol":if second {"TQQQ261002C00101000"} else {"TQQQ260925C00100000"},
                        "underlying_symbol":"TQQQ","status":"active","strike_price":if second {"101"} else {"100"},
                        "expiration_date":if second {"2026-10-02"} else {"2026-09-25"},
                        "open_interest":if second {None} else {Some("12")},"open_interest_date":"2026-09-17"}],
                        "next_page_token":if second {None} else {Some("second")}})
                } else if path.starts_with("/v1beta1/options/snapshots/") {
                    assert!(path.contains("feed=indicative"));
                    serde_json::json!({"snapshots":{"TQQQ260925C00100000":{
                        "impliedVolatility":0.5,"latestTrade":{"p":1.5,"t":"2026-09-18T19:00:00Z"}}}})
                } else {
                    assert!(
                        path.starts_with("/v1beta1/options/quotes/latest")
                            && path.contains("feed=indicative")
                    );
                    serde_json::json!({"quotes":{
                        "TQQQ260925C00100000":{"bp":1,"ap":2,"t":"2026-09-18T19:59:00Z"},
                        "TQQQ261002C00101000":{"bp":2,"ap":3,"t":"2026-09-18T19:59:01Z"}}})
                };
                let body = value.to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            paths
        });
        let mut adapter = AlpacaPaperEvidenceTransport::new(
            "https://paper-api.alpaca.markets",
            "fixture",
            "fixture",
            Some(AlpacaMarketDataFeed::Iex),
        )
        .unwrap();
        adapter.base_url = format!("http://{address}");
        adapter.market_data_url = adapter.base_url.clone();
        let value = adapter
            .acquire_option_capture(
                "option_chain:TQQQ:2026-09-20:2026-10-20",
                "2026-09-20T12:00:00Z".parse().unwrap(),
            )
            .await
            .unwrap();
        let coverage = &value.normalized["coverage"];
        assert_eq!(coverage["contracts_requested"], 2);
        assert_eq!(coverage["contracts_returned"], 2);
        assert_eq!(coverage["contracts_with_bid_ask"], 2);
        assert_eq!(coverage["contracts_with_trade"], 1);
        assert_eq!(coverage["contracts_with_open_interest"], 1);
        assert_eq!(coverage["supplemental_requests"], 1);
        assert!(value.normalized["underlying"]["latest_quote"].is_null());
        assert_eq!(value.normalized["underlying"]["rejected_after_cutoff"], 1);
        assert!(value.normalized["snapshots"]["TQQQ261002C00101000"]
            .get("open_interest")
            .is_none());
        assert!(value.normalized["snapshots"]["TQQQ260925C00100000"]["iv_timestamp"].is_null());
        assert_eq!(server.await.unwrap().len(), 10);
        assert_eq!(
            serde_json::from_slice::<Value>(&value.raw).unwrap()["requests"]
                .as_array()
                .unwrap()
                .len(),
            10
        );
    }
}
