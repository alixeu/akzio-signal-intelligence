//! Stable Rust-owned Paper execution schemas.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    content_hash_json,
    decision::HardBlocker,
    Asset, ContentHash, DomainError, MoneyMicros, PaperCommitmentId, PaperRepriceId,
    ReconciliationId, RunId, TargetPortfolio, V2_DOMAIN_SCHEMA_VERSION,
};
include!("execution_parts/snapshots.rs");
include!("execution_parts/plan.rs");
include!("execution_parts/effects.rs");
#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;
