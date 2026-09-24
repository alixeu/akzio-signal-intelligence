// 文件导读：组合 Paper 执行所需的快照、交易时段、计划和副作用记录 schema。
// 子模块通过 include! 共享本模块导入，但只描述数据和校验，不直接访问券商或存储。
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
