use chrono::{DateTime, Utc};
use thiserror::Error;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    DomainError, MarketClockSnapshot, QuoteSnapshot, TaskWritePermit,
};
use akzio_store::{Store, StoreError};

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
    fn validate_snapshot(&self) -> Result<(), DomainError>;
}

impl ExecutionSnapshotPayload for AccountSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        self.validate()
    }
}

impl ExecutionSnapshotPayload for QuoteSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        self.validate()
    }
}

impl ExecutionSnapshotPayload for MarketClockSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
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
