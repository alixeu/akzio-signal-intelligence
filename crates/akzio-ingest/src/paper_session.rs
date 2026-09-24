//! Read-only provider captures for execution sessions and the matching quote venue.

// 文件导读：执行市场采集把 Paper clock、calendar、Overnight 资产资格和报价 feed 放在同一
// raw ledger 中。Regular 时段使用显式 IEX/SIP，Overnight 映射到 overnight/BOATS，不回退到
// IEX；返回的 normalized 时间仍由 provider payload/领域 session 解码，不能把抓取成功当作
// 报价 freshness 或订单授权。
use super::*;
use akzio_domain::{Asset, ExchangeSession, TradingSession, TradingSessionSnapshot};

impl AlpacaPaperEvidenceTransport {
    pub(super) async fn acquire_execution_market(
        &self,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 先读取 clock/calendar 建立真实 TradingSession；clock 请求若进入 Overnight，再逐
        // 资产检查 overnight_tradable/halted，最后按资源选择 clock 或 quotes 并保留完整 ledger。
        let invalid = |message: &str| EvidenceAdapterError::DataQuality(message.into());
        let url = |base: &str, path: &str| {
            Url::parse(&format!("{base}{path}")).map_err(|_| invalid("invalid session URL"))
        };
        let mut ledger = Vec::new();
        let clock = self
            .capture_get(url(&self.base_url, "/v2/clock")?, &mut ledger)
            .await?;
        let observed: DateTime<Utc> = clock["timestamp"]
            .as_str()
            .ok_or_else(|| invalid("clock timestamp missing"))?
            .parse()
            .map_err(|_| invalid("clock timestamp invalid"))?;
        let date = observed
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive();
        let calendar = self
            .capture_get(
                url(
                    &self.base_url,
                    &format!(
                        "/v2/calendar?start={}&end={}",
                        date - chrono::Duration::days(1),
                        date + chrono::Duration::days(14)
                    ),
                )?,
                &mut ledger,
            )
            .await?;
        let regular_open = clock["is_open"]
            .as_bool()
            .ok_or_else(|| invalid("clock is_open missing"))?;
        let mut session = TradingSessionSnapshot::from_calendar(
            observed,
            regular_open,
            &ExchangeSession::from_alpaca(&calendar).map_err(|e| invalid(&e.to_string()))?,
        )
        .map_err(|e| invalid(&e.to_string()))?;
        let (normalized, source_uri) = if resource == "paper.clock" {
            if session.kind == TradingSession::Overnight {
                for asset in Asset::EXECUTABLE {
                    let value = self
                        .capture_get(
                            url(&self.base_url, &format!("/v2/assets/{}", asset.symbol()))?,
                            &mut ledger,
                        )
                        .await?;
                    if akzio_domain::alpaca_overnight_asset_available(&value, asset) {
                        session.overnight_assets.insert(asset);
                    }
                }
            }
            let mut normalized = clock;
            normalized["session"] =
                serde_json::to_value(session).map_err(|e| invalid(&e.to_string()))?;
            (normalized, format!("{}/v2/clock", self.base_url))
        } else {
            // SIP entitlement maps to BOATS; basic IEX maps to the documented
            // real-time indicative overnight feed. Never fall back to IEX at night.
            let feed = if session.kind == TradingSession::Overnight {
                if self.market_data_feed == Some(AlpacaMarketDataFeed::Sip) {
                    "boats"
                } else {
                    "overnight"
                }
            } else {
                self.market_data_feed
                    .ok_or_else(|| invalid("explicit execution quote feed required"))?
                    .as_str()
            };
            let endpoint = url(
                &self.market_data_url,
                &format!("/v2/stocks/quotes/latest?symbols=TQQQ,QQQ,SOXX,SOXL&feed={feed}"),
            )?;
            let mut quotes = self.capture_get(endpoint.clone(), &mut ledger).await?;
            quotes["feed"] = serde_json::json!(feed);
            (quotes, endpoint.to_string())
        };
        let raw = serde_json::to_vec(&serde_json::json!({"records":ledger}))
            .map_err(|e| invalid(&e.to_string()))?;
        let retrieved = Utc::now();
        Ok(AcquiredEvidence {
            provenance: EvidenceProvenance {
                document_id: Some(resource.into()),
                published_at: None,
                observed_at: retrieved,
                revision: None,
                source_uri: source_uri.clone(),
                dedupe_key: format!("alpaca:{}", ContentHash::of_bytes(&raw)),
                citations: vec![],
            },
            raw,
            normalized,
            source_uri,
            observed_at: retrieved,
            media_type: "application/json".into(),
            quality: EvidenceQuality::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn execution_capture_keeps_clock_and_quote_times_and_uses_overnight_venue() {
        // 回归覆盖 IEX/SIP 到 overnight/BOATS 的映射、下一交易日 trade_date、资产资格和
        // provider quote timestamp；不得出现 feed=iex 的夜盘回退。
        for (configured, expected) in [
            (AlpacaMarketDataFeed::Iex, "overnight"),
            (AlpacaMarketDataFeed::Sip, "boats"),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut paths = Vec::new();
                for _ in 0..9 {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut bytes = [0; 8192];
                    let size = stream.read(&mut bytes).await.unwrap();
                    let request = std::str::from_utf8(&bytes[..size]).unwrap();
                    assert!(request.starts_with("GET "));
                    let path = request.split_whitespace().nth(1).unwrap();
                    paths.push(path.to_owned());
                    let value = if path == "/v2/clock" {
                        serde_json::json!({"is_open":false,"timestamp":"2026-09-22T21:00:00-04:00"})
                    } else if path.starts_with("/v2/calendar?") {
                        serde_json::json!([{"date":"2026-09-23","open":"09:30","close":"16:00"}])
                    } else if let Some(symbol) = path.strip_prefix("/v2/assets/") {
                        serde_json::json!({"symbol":symbol,"status":"active","tradable":true,
                            "attributes":if symbol == "SOXL" { vec!["overnight_tradable","overnight_halted"] } else {vec!["overnight_tradable"]}})
                    } else {
                        assert!(path.starts_with("/v2/stocks/quotes/latest?"));
                        assert!(path.ends_with(&format!("feed={expected}")));
                        serde_json::json!({"quotes":{"QQQ":{"bp":99,"ap":100,"t":"2026-09-23T00:59:59Z"}}})
                    };
                    let body = value.to_string();
                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                }
                paths
            });
            let mut adapter = AlpacaPaperEvidenceTransport::new(
                "https://paper-api.alpaca.markets",
                "fixture",
                "fixture",
                Some(configured),
            )
            .unwrap();
            adapter.base_url = format!("http://{address}");
            adapter.market_data_url = adapter.base_url.clone();
            adapter.client = Client::builder()
                .no_proxy()
                .timeout(StdDuration::from_secs(3))
                .build()
                .unwrap();
            let clock = adapter
                .acquire_execution_market("paper.clock")
                .await
                .unwrap();
            assert_eq!(clock.normalized["is_open"], false);
            let decoded =
                crate::decode_paper_clock(&clock.normalized, "2026-09-22".into(), Utc::now())
                    .unwrap();
            assert_eq!(decoded.trading_session(), TradingSession::Overnight);
            assert_eq!(
                decoded.session.as_ref().unwrap().trade_date.to_string(),
                "2026-09-23"
            );
            assert_eq!(decoded.session.as_ref().unwrap().overnight_assets.len(), 3);
            assert!(!decoded
                .session
                .unwrap()
                .overnight_assets
                .contains(&Asset::Soxl));
            let raw: Value = serde_json::from_slice(&clock.raw).unwrap();
            assert_eq!(raw["records"].as_array().unwrap().len(), 6);
            assert_eq!(raw["records"][0]["response"]["is_open"], false);
            let quotes = adapter
                .acquire_execution_market("paper.quotes")
                .await
                .unwrap();
            assert_eq!(quotes.normalized["feed"], expected);
            assert_eq!(
                quotes.normalized["quotes"]["QQQ"]["t"],
                "2026-09-23T00:59:59Z"
            );
            let paths = server.await.unwrap();
            assert!(!paths.iter().any(|path| path.contains("feed=iex")));
        }
    }
}
