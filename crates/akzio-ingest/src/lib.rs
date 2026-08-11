//! Rust-owned allowlisted evidence acquisition for Akzio v2.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

pub mod runtime;

pub use runtime::{
    AcquiredEvidence, AlpacaEvidenceAdapter, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter,
    AsyncGovernedEvidenceTransport, DetailInput, EvidenceAdapter, EvidenceBundle, EvidenceCitation,
    EvidenceProvenance, EvidenceQuality, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError,
    EvidenceRuntimeResult, EvidenceSource, FixtureEvidenceAdapter, FredEvidenceAdapter,
    GovernedEvidenceTransport, ModelNativeWebEvidenceTransport, NewsWebEvidenceAdapter,
    NormalizedEvidencePayload, SecEdgarEvidenceAdapter,
};
