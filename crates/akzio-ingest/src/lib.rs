//! Rust-owned allowlisted evidence acquisition for Akzio v2.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

mod direct;
mod paper_decode;
pub mod runtime;

pub use direct::{FredDirectTransport, SecEdgarDirectTransport};
pub use paper_decode::{
    common_bar_dates, decode_paper_account, decode_paper_account_components, decode_paper_clock,
    decode_paper_quotes, parse_daily_bars, parse_money_micros, provider_money, PaperDecodeError,
    PaperDecodeResult,
};
pub use runtime::{
    validate_daily_bar_payload, AcquiredEvidence, AlpacaMarketDataFeed,
    AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, DetailInput, EvidenceAdapter,
    EvidenceBundle, EvidenceCitation, EvidenceProvenance, EvidenceQuality, EvidenceRequest,
    EvidenceRuntime, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    FixtureEvidenceAdapter, GovernedResource, ModelNativeWebEvidenceTransport,
    NormalizedEvidencePayload,
};
