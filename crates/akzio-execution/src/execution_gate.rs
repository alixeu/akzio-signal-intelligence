//! Typed execution gate over the persisted Decision/snapshot closure and
//! separately supplied pre-trade safety observations.

// 文件导读：ExecutionGate 是 Decision → ExecutionVerdict 的第二道 Rust 闸门。它从 Store
// 读取 DecisionContext、账户/报价/时钟 NormalizedEvidence，并接收 daemon 组装的
// pre-trade 观察值，检查来源闭包、freshness、session、Paper approval、Policy influence、
// 容量/合规/依赖与订单可行性，生成可选 ExecutionPlan、ExecutionContext 和
// Accepted/NoOrder Verdict。本 runtime 的
// evaluate 不直接调用 Broker；daemon 在调用它之前仍可只读刷新外部快照。
// Accepted 之后仍必须经过 PaperCommitment 的订单写请求前持久化。
// ExecutionRuntime 持有 Store 与三套只读 Policy；本文件声明公开 I/O 形状，`include!` 的
// core/snapshots/validation/helpers 是同一模块内的实现分区，共享私有字段与错误类型。

use std::collections::BTreeSet;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, CandidatePolicy,
    ContextManifestPayload, DecisionContext, DomainError, ExecutionContext, ExecutionVerdict,
    Experience, FreezeState, HardBlocker, MarketClockSnapshot, ModelQualificationGate, MoneyMicros,
    NoOrder, PolicySubject, QuoteSnapshot, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::{Store, StoreError};
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    AllocationError, AllocationInput, AllocationRuntime, ExecutionError, ExecutionGatePolicy,
    ExecutionPolicy, PreTradeSafetyEvidence, PreTradeSafetyInput, PreTradeSafetyPolicy,
};

#[derive(Debug, Error)]
pub enum ExecutionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision context does not belong to the execution task run")]
    DecisionRunMismatch,
    #[error("execution gate integrity failure: {0}")]
    Integrity(&'static str),
}

pub type ExecutionGateResult<T> = std::result::Result<T, ExecutionGateError>;

#[derive(Debug, Clone)]
pub struct ExecutionGateInput {
    // 输入都是已持久化 ArtifactRef；缺失快照不是 Rust 的“默认安全”，会被转成明确 blocker。
    pub permit: TaskWritePermit,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    /// Set when the execution refresh received a quote payload but Rust
    /// rejected it before sealing a QuoteSnapshot.  This distinguishes an
    /// invalid quote from a missing broker response in the durable verdict.
    pub quote_validation_error: Option<String>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub pretrade_safety: Option<PreTradeSafetyEvidence>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ExecutionGateOutput {
    // plan 只在全部分配前提满足时存在；context/verdict 即使 NoOrder 也会保存，供 Outcome
    // 继续评估“无订单研究意图”。
    pub execution_plan: Option<Artifact>,
    pub execution_context: Artifact,
    pub verdict: Artifact,
}

#[derive(Debug, Clone)]
pub struct ExecutionRuntime {
    store: Store,
    allocation: AllocationRuntime,
    gate_policy: ExecutionGatePolicy,
    pretrade_safety: PreTradeSafetyPolicy,
}

include!("execution_gate/core.rs");
include!("execution_gate/snapshots.rs");
include!("execution_gate/validation.rs");
include!("execution_gate/helpers.rs");
