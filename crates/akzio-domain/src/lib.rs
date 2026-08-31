//! Stable, Rust-owned domain facade for Akzio v2.
//!
//! This crate contains schemas and validation only: no database, model,
//! network, filesystem, or broker I/O.

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                let value = uuid::Uuid::new_v4().simple().to_string();
                Self(value[..16].to_owned())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

mod core;
mod schema;

pub mod artifact;
pub mod canary;
pub mod context;
pub mod contract;
pub mod decision;
pub mod evaluation;
pub mod event;
pub mod execution;
pub mod ids;
pub mod lesson;
pub mod release;
pub mod research;
pub mod runtime_manifest;
pub mod workflow;

pub use artifact::*;
pub use canary::*;
pub use context::*;
pub use contract::*;
pub use core::*;
pub use decision::*;
pub use evaluation::*;
pub use event::*;
pub use execution::*;
pub use ids::{
    EvaluationId, ExperienceId, LessonId, OutcomeId, PaperCommitmentId, PaperRepriceId,
    PolicyTransitionId, ReconciliationId,
};
pub use lesson::*;
pub use release::*;
pub use research::*;
pub use runtime_manifest::*;
pub use schema::FactorLimits;
pub use workflow::*;

/// Formal schema identity for the source-incompatible v2 domain graph.
pub const V2_DOMAIN_SCHEMA_VERSION: u32 = schema::V2_SCHEMA_VERSION;

pub const RESEARCH_PLANNER_RECIPE_ID: &str = "research.planner";
pub const RESEARCH_ANALYST_RECIPE_ID: &str = "research.analyst";
pub const RESEARCH_CRITIC_RECIPE_ID: &str = "research.critic";
pub const RESEARCH_SYNTHESIZER_RECIPE_ID: &str = "research.synthesizer";
pub const LEARNING_OUTCOME_WORKER_RECIPE_ID: &str = "learning.outcome_worker";
pub const GOVERNED_EVIDENCE_SOURCE_FAMILIES: [&str; 4] =
    ["alpaca", "sec_edgar", "fred", "news_web"];

pub fn estimate_tokens_from_bytes(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX)
}

pub fn estimate_json_tokens<T: serde::Serialize>(value: &T) -> Result<u32, serde_json::Error> {
    let bytes = serde_json::to_vec(value)?.len() as u64;
    Ok(estimate_tokens_from_bytes(bytes))
}
