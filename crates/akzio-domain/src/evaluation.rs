//! Outcome-backed learning vocabulary.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    AttemptId, ContentHash, DomainError, EvaluationId, ExperienceId, MemoryId, OutcomeId,
    PolicyTransitionId, RunId, TaskId, TopologyId, V2_DOMAIN_SCHEMA_VERSION,
};
include!("evaluation_parts/outcome.rs");
include!("evaluation_parts/policy.rs");
include!("evaluation_parts/tests.rs");
