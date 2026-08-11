//! Rust-owned allowlisted evidence acquisition for Akzio v2.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

pub mod runtime;

pub use runtime::{
    AcquiredEvidence, AlpacaEvidenceAdapter, DetailInput, EvidenceAdapter, EvidenceBundle,
    EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    FixtureEvidenceAdapter, FredEvidenceAdapter, GovernedEvidenceTransport, NewsWebEvidenceAdapter,
    NormalizedEvidencePayload, SecEdgarEvidenceAdapter,
};

#[doc(hidden)]
pub mod legacy;
