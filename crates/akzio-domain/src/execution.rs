//! Stable Rust-owned Paper execution schemas.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    content_hash_json,
    decision::HardBlocker,
    Asset, ContentHash, DomainError, MandateAssessment, MoneyMicros, PaperCancelId,
    PaperCommitmentId, PaperRepriceId, PreTradeSafetySnapshot, ReconciliationId, RunId,
    TargetPortfolio, DOMAIN_SCHEMA_VERSION,
};
include!("execution/snapshots.rs");
include!("execution/session.rs");
include!("execution/plan.rs");
include!("execution/effects.rs");
