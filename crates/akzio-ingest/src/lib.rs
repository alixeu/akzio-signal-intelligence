//! Rust-owned allowlisted evidence acquisition for Akzio.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

// 文件导读：akzio-ingest 把 allowlisted provider/模型采集结果变成带 provenance、时间基准
// 和 Raw→Normalized lineage 的证据输入。adapter 只负责获取和验证外部响应，EvidenceRuntime
// 负责把请求绑定到本 Run 的 EvidenceNeed、检查 cutoff/新鲜度并 stage CAS；Agent 只能通过
// Context 读取封存后的 NormalizedEvidence，不能借此获得网络或 RawEvidence 权限。

mod direct;
mod financial_content;
mod news;
mod official;
mod paper_decode;
mod prompts;
mod quant_features;
pub mod runtime;

pub use akzio_domain::EvidenceAcquisitionMode;
pub use direct::{FredDirectTransport, SecEdgarDirectTransport};
pub use news::configured_news_evidence_transport;
pub use official::OfficialInstrumentEvidenceTransport;
pub use paper_decode::{
    common_bar_dates, decode_paper_account, decode_paper_account_components, decode_paper_clock,
    decode_paper_quotes, parse_daily_bars, parse_money_micros, provider_money, PaperDecodeError,
    PaperDecodeResult,
};
pub use quant_features::{QuantFeatureSnapshot, QUANT_FEATURE_FORMULA_VERSION};
pub use runtime::validate_outcome_price_window;
pub use runtime::{
    model_native_web_evidence_transport, validate_daily_bar_payload, AcquiredEvidence,
    AlpacaMarketDataFeed, AlpacaOptionDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter,
    EvidenceAdapter, EvidenceAdapterError, EvidenceBundle, EvidenceCitation,
    EvidenceContaminationCertificate, EvidenceProvenance, EvidenceQuality, EvidenceRequest,
    EvidenceRuntime, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    EvidenceTimeBasis, FixtureEvidenceAdapter, GovernedResource, NativeWebFailureKind,
    NormalizedEvidencePayload,
};

/// US ETF session date, including UTC evening/day-boundary differences.
pub fn market_session_day(at: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    // 所有 session 日期先转美东再取自然日，避免 UTC 晚间/夏令时把同一交易 Session 切开。
    at.with_timezone(&chrono_tz::America::New_York).date_naive()
}
