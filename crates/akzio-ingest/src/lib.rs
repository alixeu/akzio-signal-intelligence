//! Rust-owned allowlisted evidence acquisition for Akzio v2.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

mod direct;
pub mod runtime;

pub use direct::{FredDirectTransport, SecEdgarDirectTransport};
pub use runtime::{
    validate_daily_bar_payload, AcquiredEvidence, AlpacaEvidenceAdapter, AlpacaMarketDataFeed,
    AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, AsyncGovernedEvidenceTransport,
    DetailInput, EvidenceAdapter, EvidenceBundle, EvidenceCitation, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError, EvidenceRuntimeResult,
    EvidenceSource, FixtureEvidenceAdapter, FredEvidenceAdapter, GovernedEvidenceTransport,
    GovernedResource, ModelNativeWebEvidenceTransport, NewsWebEvidenceAdapter,
    NormalizedEvidencePayload, SecEdgarEvidenceAdapter,
};
