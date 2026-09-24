// 文件导读：组合 Outcome、风险真值和 DecisionPolicy 的学习领域类型。
// 具体实现拆在同目录文件中，由 include! 在此模块作用域内合并，保持共享导入和公开 API。
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
