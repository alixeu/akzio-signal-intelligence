//! Direct, issuer-owned acquisition for instrument research material.
//!
//! The persisted EvidenceNeed vocabulary still says `news_web` for backward
//! compatibility.  The resource itself is parsed into a typed
//! `GovernedResource`, so issuer documents do not depend on the native web
//! capability or on model-generated citations.

use std::{collections::BTreeMap, time::Duration};

use akzio_domain::{Asset, ContentHash};
use chrono::{DateTime, NaiveDate, Utc};
use futures::{future::BoxFuture, StreamExt};
use reqwest::{header, Client, Url};
use serde_json::{json, Value};

use crate::runtime::{
    AcquiredEvidence, AsyncEvidenceAdapter, EvidenceAdapterError, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, EvidenceSource, GovernedResource,
};

const INVESCO_HOLDINGS: &str =
    "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/holdings/fund?idType=ticker&interval=monthly&productType=ETF";
const INVESCO_DETAILS: &str =
    "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ?idType=ticker&variationType=fundDetails&productType=ETF";
const INVESCO_PAGE: &str = "https://www.invesco.com/qqq-etf/en/about.html";
const PROSHARES_HOLDINGS: &str = "https://accounts.profunds.com/etfdata/psdlyhld.csv";
const PROSHARES_PAGE: &str = "https://www.proshares.com/our-etfs/leveraged-and-inverse/tqqq";
const ISHARES_HOLDINGS: &str =
    "https://www.ishares.com/us/products/239705/ishares-semiconductor-etf/latest-holdings.csv";
const ISHARES_PAGE: &str = "https://www.ishares.com/us/products/239705/SOX";
const DIREXION_HOLDINGS: &str = "https://www.direxion.com/holdings/SOXL.csv";

const MAX_OFFICIAL_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
struct HttpDocument {
    url: String,
    body: Vec<u8>,
    media_type: String,
    revision: Option<String>,
    available_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct OfficialInstrumentEvidenceTransport {
    client: Client,
}

impl std::fmt::Debug for OfficialInstrumentEvidenceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OfficialInstrumentEvidenceTransport")
            .finish_non_exhaustive()
    }
}

impl OfficialInstrumentEvidenceTransport {
    pub fn new() -> Result<Self, EvidenceAdapterError> {
        // issuer 直连客户端禁止自动 redirect，并设置连接/整体超时；构造失败通过 Result 传回而不生成不可用适配器。
        let client = Client::builder()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        Ok(Self { client })
    }

    async fn fetch(&self, source_uri: &str) -> Result<HttpDocument, EvidenceAdapterError> {
        // URL 必须是 HTTPS 且 host 在固定发行方 allowlist 内；这是发起 HTTP I/O 前的权限边界。
        let url = Url::parse(source_uri)
            .map_err(|_| EvidenceAdapterError::DataQuality("official URL is invalid".into()))?;
        if url.scheme() != "https" || !official_host_allowed(url.host_str().unwrap_or_default()) {
            return Err(EvidenceAdapterError::Policy {
                evidence_source: EvidenceSource::NewsWeb,
                resource: source_uri.to_owned(),
                reason: "official source host is outside the issuer allowlist".into(),
            });
        }
        let response = self
            .client
            .get(url.clone())
            .header(header::USER_AGENT, "AkzioEvidence/2.0")
            .header(
                header::ACCEPT,
                "application/json, text/html, text/csv, text/plain;q=0.9, */*;q=0.1",
            )
            .send()
            .await
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        // 先按 HTTP 状态分类，再比较最终 URL；任何 redirect 都被拒绝，不把跳转后的站点默认为官方来源。
        crate::runtime::classify_evidence_response(&response)?;
        if response.url() != &url {
            return Err(EvidenceAdapterError::Policy {
                evidence_source: EvidenceSource::NewsWeb,
                resource: source_uri.to_owned(),
                reason: "official source redirected; redirect chain is not allowlisted".into(),
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_OFFICIAL_BODY_BYTES as u64)
        {
            return Err(EvidenceAdapterError::DataQuality(
                "official response exceeds the bounded body limit".into(),
            ));
        }
        // content_length 只是早期上限检查，流式读取还会再次检查累计 body，防止 provider 不报长度时越过边界。
        let media_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let revision = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .or_else(|| {
                response
                    .headers()
                    .get(header::LAST_MODIFIED)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            });
        // ETag/Last-Modified 是可选 revision；available_at 优先取 Last-Modified，其次才使用 HTTP Date。
        let available_at = response
            .headers()
            .get(header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .or_else(|| {
                response
                    .headers()
                    .get(header::DATE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
                    .map(|value| value.with_timezone(&Utc))
            });
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
            if body.len().saturating_add(chunk.len()) > MAX_OFFICIAL_BODY_BYTES {
                return Err(EvidenceAdapterError::DataQuality(
                    "official response exceeds the bounded body limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        if body.is_empty() {
            return Err(EvidenceAdapterError::DataQuality(
                "official response is empty".into(),
            ));
        }
        // 返回值同时保留原始 body、媒体类型和版本头；上层 parser 决定如何转换为 normalized evidence。
        Ok(HttpDocument {
            url: source_uri.to_owned(),
            body,
            media_type,
            revision,
            available_at,
        })
    }

    async fn acquire_inner(
        &self,
        request: &EvidenceRequest,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // official adapter 只服务 NewsWeb 下的 typed issuer resources；先检查 source，再解析 GovernedResource。
        if request.source != EvidenceSource::NewsWeb {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let governed =
            GovernedResource::parse(request.source, &request.resource).map_err(|error| {
                EvidenceAdapterError::Policy {
                    evidence_source: request.source,
                    resource: request.resource.clone(),
                    reason: error.to_string(),
                }
            })?;
        // match 将不同官方材料分派到各自 parser；最近新闻、未类型化查询和不支持类别显式返回 gap。
        match governed {
            GovernedResource::OfficialFundHoldings { asset, as_of } => {
                self.acquire_holdings(asset, as_of, cutoff).await
            }
            GovernedResource::OfficialIndexMetadata { asset, as_of } => {
                self.acquire_index_metadata(asset, as_of, cutoff).await
            }
            GovernedResource::OfficialLeveragedEtfTerms { asset, as_of } => {
                self.acquire_leverage_terms(asset, as_of, cutoff).await
            }
            GovernedResource::OfficialEarningsEventCalendar { asset, .. } => {
                Err(EvidenceAdapterError::NotConfigured(format!(
                    "issuer holdings do not provide a component-company earnings calendar for {}",
                    asset.symbol()
                )))
            }
            GovernedResource::RecentNews { asset, .. } => Err(EvidenceAdapterError::NotConfigured(
                format!(
                    "no authorized news API is configured for recent {} news; native web remains an independent supplement",
                    asset.symbol()
                ),
            )),
            GovernedResource::NewsWeb { .. } => Err(EvidenceAdapterError::NotConfigured(
                "untyped news query has no registered direct provider".into(),
            )),
            _ => Err(EvidenceAdapterError::SourceMismatch),
        }
    }

    async fn acquire_holdings(
        &self,
        asset: Asset,
        requested_as_of: NaiveDate,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 四只资产映射到各自发行方 endpoint；parser 返回 effective_as_of 与结构化 rows，原始响应仍原样保留。
        let document = match asset {
            Asset::Qqq => self.fetch(INVESCO_HOLDINGS).await?,
            Asset::Tqqq => self.fetch(PROSHARES_HOLDINGS).await?,
            Asset::Soxx => self.fetch(ISHARES_HOLDINGS).await?,
            Asset::Soxl => self.fetch(DIREXION_HOLDINGS).await?,
        };
        let (effective_as_of, value) = match asset {
            Asset::Qqq => parse_invesco_holdings(&document.body, asset)?,
            Asset::Tqqq => parse_proshares_holdings(&document.body, asset)?,
            Asset::Soxx => parse_ishares_holdings(&document.body, asset)?,
            Asset::Soxl => parse_direxion_holdings(&document.body, asset)?,
        };
        // 发行方文档的有效日期必须不晚于请求日期和 cutoff，不能用 HTTP retrieval 时间替代业务日期。
        validate_effective_date(effective_as_of, requested_as_of, cutoff)?;
        let source_document = source_document_metadata(
            &document,
            &format!("fund_holdings:{}", asset.symbol()),
            effective_as_of,
            "complete_endpoint_snapshot",
        );
        let normalized = json!({
            "provider": "issuer_direct",
            "issuer": issuer_for(asset),
            "asset": asset.symbol(),
            "category": "fund_holdings",
            "effective_as_of": effective_as_of,
            "coverage": "complete",
            "data": value,
            "source_document": source_document,
        });
        self.acquired(
            request_resource(asset, "etf_holdings", requested_as_of),
            document,
            normalized,
            effective_as_of,
        )
    }

    async fn acquire_index_metadata(
        &self,
        asset: Asset,
        requested_as_of: NaiveDate,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 指数元数据需要发行方 API 或产品页中的身份/benchmark 事实；每个分支都单独验证日期和文本证据。
        let (document, effective_as_of, benchmark_index, data) = match asset {
            Asset::Qqq => {
                // QQQ 同时读取 JSON details 和官方页面，只有两者能证明产品与 Nasdaq-100 的绑定才继续。
                let details = self.fetch(INVESCO_DETAILS).await?;
                let page = self.fetch(INVESCO_PAGE).await?;
                let details_value: Value = serde_json::from_slice(&details.body).map_err(|_| {
                    EvidenceAdapterError::DataQuality("invalid Invesco details JSON".into())
                })?;
                let effective = details_value
                    .get("effectiveBusinessDate")
                    .and_then(Value::as_str)
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
                    .ok_or_else(|| {
                        EvidenceAdapterError::DataQuality(
                            "Invesco details has no effective business date".into(),
                        )
                    })?;
                let page_text = String::from_utf8_lossy(&page.body);
                if !page_text.contains("Invesco QQQ") || !page_text.contains("Nasdaq-100 Index") {
                    return Err(EvidenceAdapterError::DataQuality(
                        "Invesco page does not prove QQQ benchmark".into(),
                    ));
                }
                let raw = serde_json::to_vec(&json!({
                    "fund_details": details_value,
                    "official_page_utf8": page_text,
                }))
                .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
                let document = HttpDocument {
                    url: details.url,
                    body: raw,
                    media_type: "application/json".into(),
                    revision: details.revision.or(page.revision),
                    available_at: details.available_at.or(page.available_at),
                };
                (
                    document,
                    effective,
                    "Nasdaq-100 Index",
                    json!({"fund_details": details_value}),
                )
            }
            Asset::Tqqq => {
                // TQQQ 的产品版本由 issuer parser 选择，不能从页面响应时间推导有效日期。
                let mut document = self.fetch(PROSHARES_PAGE).await?;
                let (text, effective) = proshares_product_version(&mut document, cutoff)?;
                if !text.contains("TQQQ") || !text.contains("Nasdaq-100 Index") {
                    return Err(EvidenceAdapterError::DataQuality(
                        "ProShares page does not prove TQQQ benchmark".into(),
                    ));
                }
                (
                    document,
                    effective,
                    "Nasdaq-100 Index",
                    json!({"page_fact": "TQQQ tracks the Nasdaq-100 Index"}),
                )
            }
            Asset::Soxx => {
                // SOXX 页面必须同时出现资产身份、benchmark 和不晚于 cutoff 的 as-of 日期。
                let document = self.fetch(ISHARES_PAGE).await?;
                let text = String::from_utf8_lossy(&document.body);
                if !text.contains("SOXX") || !text.contains("NYSE Semiconductor Index") {
                    return Err(EvidenceAdapterError::DataQuality(
                        "iShares page does not prove SOXX benchmark".into(),
                    ));
                }
                let effective = latest_as_of_date(&text, cutoff.date_naive()).ok_or_else(|| {
                    EvidenceAdapterError::DataQuality(
                        "iShares index metadata has no as-of date".into(),
                    )
                })?;
                (
                    document,
                    effective,
                    "NYSE Semiconductor Index",
                    json!({"page_fact": "SOXX benchmark is NYSE Semiconductor Index"}),
                )
            }
            Asset::Soxl => {
                // SOXL 当前没有可接受的产品页解析器；明确报告 NotConfigured，不把 holdings CSV 当成指数元数据。
                return Err(EvidenceAdapterError::NotConfigured(
                    "Direxion product-page metadata is protected by a browser challenge; holdings CSV is registered independently".into(),
                ));
            }
        };
        // 解析得到的有效日期再次和请求 cutoff 比较，normalized 才能声明 complete official source。
        validate_effective_date(effective_as_of, requested_as_of, cutoff)?;
        let source_document = source_document_metadata(
            &document,
            &format!("index_metadata:{}", asset.symbol()),
            effective_as_of,
            "official_page_or_issuer_api",
        );
        let normalized = json!({
            "provider": "issuer_direct",
            "issuer": issuer_for(asset),
            "asset": asset.symbol(),
            "category": "index_metadata",
            "benchmark_index": benchmark_index,
            "effective_as_of": effective_as_of,
            "data": data,
            "source_document": source_document,
        });
        self.acquired(
            request_resource(asset, "index_metadata", requested_as_of),
            document,
            normalized,
            effective_as_of,
        )
    }

    async fn acquire_leverage_terms(
        &self,
        asset: Asset,
        requested_as_of: NaiveDate,
        cutoff: DateTime<Utc>,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 杠杆条款只在页面明确包含资产、daily reset、倍数和 benchmark 时转换；缺少解析器的 SOXL 保持缺口。
        let (document, effective_as_of, benchmark_index, multiple, text) = match asset {
            Asset::Tqqq => {
                // TQQQ 的 description 由内容 revision 定日期，文本关键词只用于确认条款完整性。
                let mut document = self.fetch(PROSHARES_PAGE).await?;
                let (text, effective) = proshares_product_version(&mut document, cutoff)?;
                if !text.contains("TQQQ")
                    || !text.contains("daily investment results")
                    || !text.contains("3x")
                    || !text.contains("Nasdaq-100 Index")
                {
                    return Err(EvidenceAdapterError::DataQuality(
                        "ProShares leverage terms are incomplete".into(),
                    ));
                }
                (document, effective, "Nasdaq-100 Index", 3_u8, text)
            }
            Asset::Soxl => {
                // 原始 PDF header 不足以证明完整条款，因此不从中提升 evidence。
                return Err(EvidenceAdapterError::NotConfigured(
                    "Direxion leverage terms require the issuer page/PDF text parser; no text is promoted from a raw PDF header".into(),
                ));
            }
            _ => return Err(EvidenceAdapterError::SourceMismatch),
        };
        // 条款的 effective_as_of 仍必须早于请求日期和 cutoff，避免未来版本污染历史研究。
        validate_effective_date(effective_as_of, requested_as_of, cutoff)?;
        let source_document = source_document_metadata(
            &document,
            &format!("leveraged_etf_terms:{}", asset.symbol()),
            effective_as_of,
            "official_product_page",
        );
        let normalized = json!({
            "provider": "issuer_direct",
            "issuer": issuer_for(asset),
            "asset": asset.symbol(),
            "category": "leveraged_etf_terms",
            "benchmark_index": benchmark_index,
            "daily_reset": true,
            "daily_leverage_multiple": multiple,
            "effective_as_of": effective_as_of,
            "terms_text_sha256": ContentHash::of_bytes(text.as_bytes()),
            "source_document": source_document,
        });
        self.acquired(
            request_resource(asset, "leveraged_etf_terms", requested_as_of),
            document,
            normalized,
            effective_as_of,
        )
    }

    fn acquired(
        &self,
        resource: String,
        document: HttpDocument,
        normalized: Value,
        effective_as_of: NaiveDate,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        // 组装结果时同时返回 provider 原文与 normalized 投影；revision 优先使用响应头，否则回退到 body hash。
        let observed_at = Utc::now();
        let published_at = effective_as_of
            .and_hms_opt(0, 0, 0)
            .map(|value| value.and_utc());
        let published_at = document.available_at.or(published_at);
        let raw_hash = ContentHash::of_bytes(&document.body);
        Ok(AcquiredEvidence {
            raw: document.body,
            media_type: document.media_type,
            source_uri: document.url.clone(),
            observed_at,
            normalized,
            provenance: EvidenceProvenance {
                document_id: Some(resource),
                published_at,
                observed_at,
                revision: document.revision.or_else(|| Some(raw_hash.to_string())),
                source_uri: document.url,
                dedupe_key: format!("issuer-direct:{raw_hash}"),
                citations: vec![],
            },
            quality: EvidenceQuality::default(),
        })
    }
}

impl AsyncEvidenceAdapter for OfficialInstrumentEvidenceTransport {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::NewsWeb
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        // trait 的无 cutoff 入口使用当前 UTC 时间；需要重放或历史校验时由 acquire_at 注入固定 cutoff。
        self.acquire_at(request, Utc::now())
    }

    fn acquire_at<'a>(
        &'a self,
        request: &'a EvidenceRequest,
        cutoff: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        // BoxFuture 把 async acquire_inner 适配到共享 trait；所有解析、权限和日期错误都沿 Result 返回。
        Box::pin(async move { self.acquire_inner(request, cutoff).await })
    }
}

fn official_host_allowed(host: &str) -> bool {
    // allowlist 按完整 host 比较；未列出的域名即使返回相似内容也不能作为官方证据来源。
    matches!(
        host,
        "dng-api.invesco.com"
            | "www.invesco.com"
            | "accounts.profunds.com"
            | "www.proshares.com"
            | "www.ishares.com"
            | "www.direxion.com"
    )
}

fn issuer_for(asset: Asset) -> &'static str {
    // 将领域资产映射到其发行方标签，仅用于 normalized/provenance，不参与网络重定向或内容猜测。
    match asset {
        Asset::Qqq => "Invesco",
        Asset::Tqqq => "ProShares",
        Asset::Soxx => "iShares",
        Asset::Soxl => "Direxion",
    }
}

fn request_resource(asset: Asset, category: &str, as_of: NaiveDate) -> String {
    // 资源 ID 由 typed asset、类别和请求日期确定，供 provenance 绑定原始请求语义。
    format!("research:{category}:{}:{as_of}", asset.symbol())
}

fn source_document_metadata(
    document: &HttpDocument,
    document_id: &str,
    effective_as_of: NaiveDate,
    coverage: &str,
) -> Value {
    // source_document 明确记录 direct acquisition、policy 版本、完整覆盖、原文 hash 和 revision，供后续来源校验使用。
    json!({
        "acquisition_kind": "official_direct",
        "acquisition_mode": akzio_domain::EvidenceAcquisitionMode::VerifiedSource.as_str(),
        "acquisition_policy_version": akzio_domain::EVIDENCE_ACQUISITION_POLICY_VERSION,
        "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
        "document_id": document_id,
        "provider": "issuer_direct",
        "url": document.url,
        "media_type": document.media_type,
        "effective_as_of": effective_as_of,
        "coverage": coverage,
        "source_closure": "complete",
        "required_source_count": 1,
        "verified_source_count": 1,
        "raw_content_hash": ContentHash::of_bytes(&document.body),
        "revision": document.revision,
    })
}

fn validate_effective_date(
    effective_as_of: NaiveDate,
    requested_as_of: NaiveDate,
    cutoff: DateTime<Utc>,
) -> Result<(), EvidenceAdapterError> {
    // 同时比较日历日期与请求日期；未来版本直接返回 DataQuality，不以 provider 的 retrieval 时间掩盖日期越界。
    if effective_as_of > cutoff.date_naive() || effective_as_of > requested_as_of {
        return Err(EvidenceAdapterError::DataQuality(format!(
            "official source version {effective_as_of} is after requested cutoff {requested_as_of}"
        )));
    }
    Ok(())
}

fn proshares_product_version(
    document: &mut HttpDocument,
    cutoff: DateTime<Utc>,
) -> Result<(String, NaiveDate), EvidenceAdapterError> {
    // ProShares 优先解析带 fundSymbol/status/saved 的 JSON；不符合 JSON 时才回退到页面文本日期提取。
    let invalid = |message: &str| EvidenceAdapterError::DataQuality(message.into());
    if let Ok(value) = serde_json::from_slice::<Value>(&document.body) {
        if value.get("fundSymbol").and_then(Value::as_str) != Some("TQQQ")
            || value.get("status").and_then(Value::as_str) != Some("Published")
        {
            return Err(invalid(
                "ProShares product identity or publication status mismatch",
            ));
        }
        let saved = value
            .get("saved")
            .and_then(Value::as_str)
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| v.with_timezone(&Utc))
            .ok_or_else(|| invalid("ProShares product JSON has no saved revision timestamp"))?;
        if saved > cutoff {
            return Err(invalid(
                "ProShares product revision is after research cutoff",
            ));
        }
        let description = value
            .get("description")
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| invalid("ProShares product JSON has no description"))?;
        // The issuer's content revision dates this product description. The
        // HTTP response Date is retrieval time, not the product version.
        document.available_at = Some(saved);
        return Ok((format!("TQQQ: {description}"), saved.date_naive()));
    }
    let text = String::from_utf8_lossy(&document.body).into_owned();
    // 页面回退只接受 cutoff 内的 as-of 或已解析的响应可用时间，不能把当前抓取时刻当成产品版本。
    let effective = latest_as_of_date(&text, cutoff.date_naive())
        .or_else(|| document.available_at.map(|v| v.date_naive()))
        .ok_or_else(|| invalid("ProShares product page has no as-of date"))?;
    Ok((text, effective))
}

fn parse_invesco_holdings(
    body: &[u8],
    asset: Asset,
) -> Result<(NaiveDate, Value), EvidenceAdapterError> {
    // Invesco holdings 是 JSON；先核对 ticker/cusip 和 effectiveBusinessDate，再要求 holdings 数量与总数一致。
    let value: Value = serde_json::from_slice(body)
        .map_err(|_| EvidenceAdapterError::DataQuality("invalid Invesco holdings JSON".into()))?;
    let identity = value
        .get("ticker")
        .or_else(|| value.get("cusip"))
        .and_then(Value::as_str);
    if identity != Some(asset.symbol()) {
        return Err(EvidenceAdapterError::DataQuality(
            "Invesco holdings ticker mismatch".into(),
        ));
    }
    let effective = value
        .get("effectiveBusinessDate")
        .and_then(Value::as_str)
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Invesco holdings missing effective date".into())
        })?;
    let holdings = value
        .get("holdings")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Invesco holdings array missing".into())
        })?;
    let expected = value
        .get("totalNumberOfHoldings")
        .or_else(|| value.get("totalNoOfHoldings"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Invesco total holdings missing".into())
        })?;
    if holdings.is_empty() || holdings.len() != expected as usize {
        return Err(EvidenceAdapterError::DataQuality(
            "Invesco holdings are partial or inconsistent with totalNumberOfHoldings".into(),
        ));
    }
    Ok((effective, value))
}

fn parse_proshares_holdings(
    body: &[u8],
    asset: Asset,
) -> Result<(NaiveDate, Value), EvidenceAdapterError> {
    // ProShares CSV 的 AS OF 行提供业务日期，解析后只选择目标 Fund Ticker，其他行不进入 normalized rows。
    let text = std::str::from_utf8(body).map_err(|_| {
        EvidenceAdapterError::DataQuality("ProShares holdings are not UTF-8 CSV".into())
    })?;
    let effective = text
        .lines()
        .find_map(|line| line.strip_prefix("AS OF "))
        .and_then(|value| parse_mdy_date(value.split(',').next().unwrap_or_default().trim()))
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("ProShares holdings missing AS OF date".into())
        })?;
    let rows = parse_csv(text)?;
    let header_index = rows
        .iter()
        .position(|row| row.first().is_some_and(|value| value == "Fund Ticker"))
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("ProShares holdings header missing".into())
        })?;
    let headers = &rows[header_index];
    let ticker_index = headers
        .iter()
        .position(|value| value == "Fund Ticker")
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("ProShares Fund Ticker column missing".into())
        })?;
    let selected = rows[header_index + 1..]
        .iter()
        .filter(|row| {
            row.get(ticker_index)
                .is_some_and(|value| value == asset.symbol())
        })
        .map(|row| csv_row_object(headers, row))
        .collect::<Result<Vec<_>, _>>()?;
    if selected.is_empty() {
        return Err(EvidenceAdapterError::DataQuality(format!(
            "ProShares holdings do not contain {}",
            asset.symbol()
        )));
    }
    Ok((effective, json!({"headers": headers, "rows": selected})))
}

fn parse_ishares_holdings(
    body: &[u8],
    asset: Asset,
) -> Result<(NaiveDate, Value), EvidenceAdapterError> {
    // iShares 先检查首行基金身份和 Fund Holdings as-of，再保留非空 CSV 行及原始列名映射。
    let text = std::str::from_utf8(body).map_err(|_| {
        EvidenceAdapterError::DataQuality("iShares holdings are not UTF-8 CSV".into())
    })?;
    if !text
        .lines()
        .next()
        .is_some_and(|line| line.contains("iShares Semiconductor ETF"))
    {
        return Err(EvidenceAdapterError::DataQuality(
            "iShares fund identity mismatch".into(),
        ));
    }
    let effective = text
        .lines()
        .find_map(|line| line.strip_prefix("Fund Holdings as of,"))
        .and_then(|value| parse_mdy_date(value.trim_matches('"').trim()))
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("iShares holdings missing as-of date".into())
        })?;
    let rows = parse_csv(text)?;
    let header_index = rows
        .iter()
        .position(|row| row.first().is_some_and(|value| value == "Ticker"))
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("iShares holdings header missing".into())
        })?;
    let selected = rows[header_index + 1..]
        .iter()
        .filter(|row| row.iter().any(|value| !value.trim().is_empty()))
        .map(|row| csv_row_object(&rows[header_index], row))
        .collect::<Result<Vec<_>, _>>()?;
    if selected.is_empty() {
        return Err(EvidenceAdapterError::DataQuality(
            "iShares holdings are empty".into(),
        ));
    }
    Ok((
        effective,
        json!({"headers": rows[header_index], "rows": selected, "asset": asset.symbol()}),
    ))
}

fn parse_direxion_holdings(
    body: &[u8],
    asset: Asset,
) -> Result<(NaiveDate, Value), EvidenceAdapterError> {
    // Direxion 以 TradeDate/AccountTicker 定位目标资产；先筛选账户，再从首个保留行读取有效日期。
    let text = std::str::from_utf8(body).map_err(|_| {
        EvidenceAdapterError::DataQuality("Direxion holdings are not UTF-8 CSV".into())
    })?;
    let rows = parse_csv(text)?;
    let header_index = rows
        .iter()
        .position(|row| row.first().is_some_and(|value| value == "TradeDate"))
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Direxion holdings header missing".into())
        })?;
    let headers = &rows[header_index];
    let account_index = headers
        .iter()
        .position(|value| value == "AccountTicker")
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Direxion AccountTicker column missing".into())
        })?;
    let selected = rows[header_index + 1..]
        .iter()
        .filter(|row| {
            row.get(account_index)
                .is_some_and(|value| value == asset.symbol())
        })
        .map(|row| csv_row_object(headers, row))
        .collect::<Result<Vec<_>, _>>()?;
    let effective = selected
        .first()
        .and_then(|row| row.get("TradeDate"))
        .and_then(Value::as_str)
        .and_then(parse_mdy_date)
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality("Direxion holdings missing TradeDate".into())
        })?;
    if selected.is_empty() {
        return Err(EvidenceAdapterError::DataQuality(
            "Direxion holdings are empty".into(),
        ));
    }
    Ok((effective, json!({"headers": headers, "rows": selected})))
}

fn csv_row_object(headers: &[String], row: &[String]) -> Result<Value, EvidenceAdapterError> {
    // CSV 行宽多余时只允许空尾列，缺列补空字符串；任何非空越界字段都会返回 DataQuality，避免静默丢列。
    let mut values = row.to_vec();
    if values.len() > headers.len() {
        if values[headers.len()..]
            .iter()
            .any(|value| !value.trim().is_empty())
        {
            return Err(EvidenceAdapterError::DataQuality(
                "issuer CSV row has non-empty fields beyond the header".into(),
            ));
        }
        values.truncate(headers.len());
    }
    while values.len() < headers.len() {
        values.push(String::new());
    }
    if values.len() != headers.len() {
        return Err(EvidenceAdapterError::DataQuality(
            "issuer CSV row width does not match the header".into(),
        ));
    }
    let object = headers
        .iter()
        .map(|header| header.trim().to_owned())
        .zip(values.iter().map(|value| value.trim().to_owned()))
        .collect::<BTreeMap<_, _>>();
    Ok(serde_json::to_value(object).expect("CSV row map is JSON serializable"))
}

fn parse_csv(text: &str) -> Result<Vec<Vec<String>>, EvidenceAdapterError> {
    // 这是受限的 CSV 转换器：保留引号内逗号/换行和双引号转义，并以行数上限防止无界输入。
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '"' if quoted && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                row.push(std::mem::take(&mut field));
            }
            '\n' if !quoted => {
                row.push(std::mem::take(&mut field));
                if row.iter().any(|value| !value.trim().is_empty()) {
                    rows.push(std::mem::take(&mut row));
                } else {
                    row.clear();
                }
            }
            '\r' if !quoted => {}
            _ => field.push(character),
        }
        if rows.len() > 100_000 {
            return Err(EvidenceAdapterError::DataQuality(
                "issuer CSV has too many rows".into(),
            ));
        }
    }
    if quoted {
        return Err(EvidenceAdapterError::DataQuality(
            "issuer CSV has an unterminated quote".into(),
        ));
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
}

fn parse_mdy_date(value: &str) -> Option<NaiveDate> {
    // 发行方日期格式不统一；按固定格式顺序尝试，无法解析时返回 None 交给调用方生成明确错误。
    let value = value.trim().trim_matches('"');
    [
        "%m/%d/%Y",
        "%m/%d/%Y %I:%M:%S %p",
        "%b %d, %Y",
        "%B %d, %Y",
        "%Y-%m-%d",
    ]
    .into_iter()
    .find_map(|format| NaiveDate::parse_from_str(value, format).ok())
}

fn latest_as_of_date(text: &str, cutoff: NaiveDate) -> Option<NaiveDate> {
    // 只收集不晚于 cutoff 的 as-of 日期；显式 marker 找不到时再扫描日期 token，结果为空则保持 None。
    let mut dates = Vec::new();
    for marker in ["as of ", "As of ", "AS OF "] {
        let mut start = 0;
        while let Some(index) = text[start..].find(marker) {
            let begin = start + index + marker.len();
            let candidate = text[begin..]
                .split(['<', '>', '\n', '\r', '"', '.'])
                .next()
                .unwrap_or_default()
                .trim();
            if let Some(date) = parse_mdy_date(candidate) {
                if date <= cutoff {
                    dates.push(date);
                }
            }
            start = begin;
        }
    }
    if dates.is_empty() {
        for token in text
            .replace("&nbsp;", " ")
            .split(|character: char| {
                !(character.is_ascii_alphanumeric() || matches!(character, '/' | '-'))
            })
            .filter(|token| !token.is_empty())
        {
            if let Some(date) = parse_mdy_date(token) {
                if date <= cutoff {
                    dates.push(date);
                }
            }
        }
    }
    dates.into_iter().max()
}

#[cfg(test)]
mod tests {
    #[test]
    fn proshares_json_uses_content_revision_not_next_day_response_date() {
        use super::*;
        let cutoff = DateTime::parse_from_rfc3339("2026-09-23T03:55:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let payload = json!({"fundSymbol":"TQQQ","status":"Published","saved":"2026-08-14T17:25:41Z",
            "description":"daily investment results: three times (3x) the Nasdaq-100 Index"});
        let mut doc = HttpDocument {
            url: PROSHARES_PAGE.into(),
            body: serde_json::to_vec(&payload).unwrap(),
            media_type: "application/json".into(),
            revision: Some("etag".into()),
            available_at: Some(cutoff),
        };
        let (text, effective) = proshares_product_version(&mut doc, cutoff).unwrap();
        assert!(text.contains("TQQQ"));
        assert_eq!(effective.to_string(), "2026-08-14");
        assert_eq!(
            doc.available_at.unwrap().to_rfc3339(),
            "2026-08-14T17:25:41+00:00"
        );
        validate_effective_date(
            effective,
            NaiveDate::from_ymd_opt(2026, 9, 22).unwrap(),
            cutoff,
        )
        .unwrap();
        let mut changed = payload.clone();
        changed["saved"] = json!("2026-09-23T04:00:00Z");
        doc.body = serde_json::to_vec(&changed).unwrap();
        assert!(proshares_product_version(&mut doc, cutoff).is_err());
        changed = payload;
        changed["fundSymbol"] = json!("OTHER");
        doc.body = serde_json::to_vec(&changed).unwrap();
        assert!(proshares_product_version(&mut doc, cutoff).is_err());
    }
    use super::*;

    #[test]
    fn proshares_parser_selects_one_fund_without_rewriting_rows() {
        let body = br#"PORTFOLIO HOLDINGS INFORMATION,,,,
AS OF 9/11/2026,,,,
,,,,
Fund Ticker, Fund Name, Security Ticker, Security Description, Shares/Contracts, Exposure Value (Notional + G/L), Market Value
"TQQQ","ProShares UltraPro QQQ","NVDA","NVIDIA CORP",10,123.4,120.0
"QQQ","Invesco QQQ","AAPL","APPLE INC",20,456.7,450.0
"TQQQ","ProShares UltraPro QQQ","CASH","CASH",1,1.0,1.0
"#;
        let (effective, value) = parse_proshares_holdings(body, Asset::Tqqq).unwrap();
        assert_eq!(effective, NaiveDate::from_ymd_opt(2026, 9, 11).unwrap());
        let rows = value["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["Security Ticker"], "NVDA");
        assert_eq!(rows[1]["Security Ticker"], "CASH");
    }

    #[test]
    fn issuer_csv_parsers_keep_cash_and_derivative_rows() {
        let body = br#"iShares Semiconductor ETF
Fund Holdings as of,"Sep 11, 2026"
Ticker,Name,Asset Class,Weight (%)
NVDA,NVIDIA,Equity,9.04
USD,USD CASH,Cash,0.03
"#;
        let (effective, value) = parse_ishares_holdings(body, Asset::Soxx).unwrap();
        assert_eq!(effective, NaiveDate::from_ymd_opt(2026, 9, 11).unwrap());
        assert_eq!(value["rows"].as_array().unwrap().len(), 2);
        assert_eq!(value["rows"][1]["Ticker"], "USD");

        let body = br#"Direxion Daily Semiconductor Bull 3X ETF
SOXL
Shares Outstanding:169000060

"TradeDate","AccountTicker","StockTicker","SecurityDescription","Shares","Price","MarketValue","Cusip","HoldingsPercent"
"9/14/2026 12:00:00 AM","SOXL","AMD","ADVANCED MICRO DEVICES","10","1","10","007903107","5.0"
"9/14/2026 12:00:00 AM","SOXL","","CASH","1","1","1","CASH","0.5"
"#;
        let (effective, value) = parse_direxion_holdings(body, Asset::Soxl).unwrap();
        assert_eq!(effective, NaiveDate::from_ymd_opt(2026, 9, 14).unwrap());
        assert_eq!(value["rows"].as_array().unwrap().len(), 2);
        assert_eq!(value["rows"][0]["StockTicker"], "AMD");
    }

    #[test]
    fn official_resource_parser_rejects_unknown_category_and_parses_typed_needs() {
        assert!(matches!(
            GovernedResource::parse(
                EvidenceSource::NewsWeb,
                "research:etf_holdings:TQQQ:2026-09-14"
            )
            .unwrap(),
            GovernedResource::OfficialFundHoldings {
                asset: Asset::Tqqq,
                ..
            }
        ));
        assert!(matches!(
            GovernedResource::parse(
                EvidenceSource::NewsWeb,
                "news:SOXX:2026-08-31:2026-09-14:market"
            )
            .unwrap(),
            GovernedResource::RecentNews {
                asset: Asset::Soxx,
                topic,
                ..
            } if topic == "market"
        ));
        assert!(GovernedResource::parse(
            EvidenceSource::NewsWeb,
            "research:unknown:QQQ:2026-09-14"
        )
        .is_err());
    }

    #[tokio::test]
    async fn official_adapter_keeps_recent_news_gap_explicit() {
        let adapter = OfficialInstrumentEvidenceTransport::new().unwrap();
        let error = adapter
            .acquire(&EvidenceRequest {
                source: EvidenceSource::NewsWeb,
                resource: "news:QQQ:2026-09-01:2026-09-15:market".to_owned(),
                max_age: chrono::Duration::days(1),
                acquisition_mode: akzio_domain::EvidenceAcquisitionMode::DiscoveryOnly,
            })
            .await
            .expect_err("issuer adapter must not claim to provide recent news");

        assert!(matches!(
            error,
            EvidenceAdapterError::NotConfigured(message)
                if message.contains("no authorized news API")
        ));
    }

    #[test]
    fn future_official_version_is_rejected() {
        let cutoff = DateTime::parse_from_rfc3339("2026-09-14T09:14:11Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(validate_effective_date(
            NaiveDate::from_ymd_opt(2026, 9, 15).unwrap(),
            NaiveDate::from_ymd_opt(2026, 9, 14).unwrap(),
            cutoff
        )
        .is_err());
    }

    #[test]
    fn page_date_extractor_reads_numeric_as_of_dates() {
        let cutoff = NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        assert_eq!(
            latest_as_of_date("Month-End Total Returns as of 8/31/2026", cutoff),
            Some(NaiveDate::from_ymd_opt(2026, 8, 31).unwrap())
        );
    }
}
