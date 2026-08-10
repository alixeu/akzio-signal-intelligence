//! Rust-owned allowlisted evidence acquisition for Akzio v2.
//!
//! Agents cannot access an HTTP client, filesystem, or raw evidence through
//! this crate. They receive only Store-sealed artifacts through Context grants.

pub mod runtime;

pub use runtime::{
    AcquiredEvidence, DetailInput, EvidenceAdapter, EvidenceBundle, EvidenceRequest,
    EvidenceRuntime, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    FixtureEvidenceAdapter, NormalizedEvidencePayload,
};

#[doc(hidden)]
pub mod legacy;
