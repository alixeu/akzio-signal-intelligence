//! Outcome-backed learning vocabulary.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    content_hash_json, Asset, AttemptId, ContentHash, DomainError, EvaluationId, ExperienceId,
    MemoryId, MoneyMicros, OrderSide, OutcomeId, PolicyTransitionId, RunId, TaskId, TopologyId,
    DOMAIN_SCHEMA_VERSION,
};
include!("evaluation/outcome.rs");
include!("evaluation/risk_ground_truth.rs");
include!("evaluation/policy.rs");
