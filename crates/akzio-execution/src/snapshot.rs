use chrono::{DateTime, Utc};
use thiserror::Error;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    DomainError, MarketClockSnapshot, QuoteSnapshot, TaskWritePermit,
};
use akzio_store::{Store, StoreError};

// 文件导读：执行快照是 Alpaca 证据经过领域类型校验后的 Artifact 投影。sealed trait
// 把可写入的 payload 限定为账户、报价和时钟三类；materialize_snapshot_artifact 只
// staging JSON 并组装 Raw/Normalized provenance，真正的 fenced commit 由调用方负责。

#[derive(Debug, Error)]
pub enum SnapshotArtifactError {
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for akzio_domain::AccountSnapshot {}
    impl Sealed for akzio_domain::QuoteSnapshot {}
    impl Sealed for akzio_domain::MarketClockSnapshot {}
}

pub trait ExecutionSnapshotPayload: serde::Serialize + sealed::Sealed {
    // 统一入口让三种快照在进入执行链前复用各自领域校验；trait 不提供任何 I/O 默认实现。
    fn validate_snapshot(&self) -> Result<(), DomainError>;
}

impl ExecutionSnapshotPayload for AccountSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        // 账户快照的活动状态、购买力、持仓与 session 约束由 domain 唯一解释。
        self.validate()
    }
}

impl ExecutionSnapshotPayload for QuoteSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        // 报价快照只在结构合法后才能成为执行侧 provenance 的一部分；新鲜度由 Gate 另查。
        self.validate()
    }
}

impl ExecutionSnapshotPayload for MarketClockSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        // 时钟快照的交易日/session 结构由 domain 校验，是否仍可提交要在即时刷新后判断。
        self.validate()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_snapshot_artifact<T: ExecutionSnapshotPayload>(
    store: &Store,
    permit: &TaskWritePermit,
    normalized_sources: &[&Artifact],
    producer: &str,
    payload: &T,
    observed_at: DateTime<Utc>,
    source_uri: Option<String>,
    now: DateTime<Utc>,
) -> Result<Artifact, SnapshotArtifactError> {
    // 输入→校验 payload 和 permit-bound NormalizedEvidence→收集其 Raw/Normalized 引用→
    // stage JSON→返回待提交 Artifact。这里拒绝跨 run、跨 source family 或非 RunScoped
    // 来源，防止执行 Gate 把任意证据伪装成账户/报价/时钟快照。
    payload.validate_snapshot()?;
    let first_normalized = normalized_sources.first().ok_or_else(|| {
        SnapshotArtifactError::InvalidInput(
            "execution snapshot has no normalized sources".to_owned(),
        )
    })?;

    let expected_origin = permit.artifact_origin();
    let mut source_refs = Vec::with_capacity(normalized_sources.len() * 2);
    for normalized in normalized_sources {
        if normalized.kind != ArtifactKind::NormalizedEvidence
            || normalized.lifecycle != ArtifactLifecycle::RunScoped
            || normalized.origin.as_ref() != Some(&expected_origin)
            || normalized.provenance.source_family != first_normalized.provenance.source_family
        {
            return Err(SnapshotArtifactError::InvalidInput(
                "execution snapshot source is not permit-bound normalized evidence".to_owned(),
            ));
        }
        let raw_source = normalized
            .source_refs
            .iter()
            .find(|source_ref| source_ref.kind == ArtifactKind::RawEvidence)
            .ok_or_else(|| {
                SnapshotArtifactError::InvalidInput(
                    "governed normalized evidence has no RawEvidence source".to_owned(),
                )
            })?;

        source_refs.push(raw_source.clone());
        source_refs.push(ArtifactRef {
            artifact_id: normalized.artifact_id.clone(),
            kind: ArtifactKind::NormalizedEvidence,
        });
    }
    source_refs.sort();
    source_refs.dedup();

    // Callers must commit the returned Artifact with the same Store instance.
    let blob = store.stage_json(payload)?;
    let provenance = ArtifactProvenance {
        source_family: first_normalized.provenance.source_family.clone(),
        observed_at: Some(observed_at),
        retrieved_at: now,
        source_uri,
        confidence_ppm: first_normalized.provenance.confidence_ppm,
        producer_contract_hash: permit.contract_hash.clone(),
    };

    Ok(Artifact::new(
        ArtifactKind::NormalizedEvidence,
        blob,
        producer,
        ArtifactLifecycle::Canonical,
        provenance,
        Some(permit.artifact_origin()),
        source_refs,
        now,
    )?)
}
