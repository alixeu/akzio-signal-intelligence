//! Stable, Rust-owned domain facade for Akzio v2.
//!
//! This crate contains schemas and validation only: no database, model,
//! network, filesystem, or broker I/O.

mod core;
mod schema;

pub mod artifact;
pub mod context;
pub mod contract;
pub mod decision;
pub mod evaluation;
pub mod event;
pub mod execution;
pub mod ids;
pub mod policy;
pub mod workflow;

pub use artifact::*;
pub use context::*;
pub use contract::*;
pub use core::*;
pub use decision::*;
pub use evaluation::*;
pub use event::*;
pub use execution::*;
pub use ids::{
    EvaluationId, EventId, ExperienceId, OutcomeId, PaperCommitmentId, PaperRepriceId,
    PolicyTransitionId, ReconciliationId,
};
pub use policy::*;
pub use workflow::*;

/// Formal schema identity for the source-incompatible v2 domain graph.
pub const V2_DOMAIN_SCHEMA_VERSION: u32 = schema::V2_SCHEMA_VERSION;
pub use schema::V2_SCHEMA_VERSION;
