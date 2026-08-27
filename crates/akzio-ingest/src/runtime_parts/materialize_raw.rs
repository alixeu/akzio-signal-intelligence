impl EvidenceRuntime {
    pub fn new(store: V2Store, allowed_sources: impl IntoIterator<Item = EvidenceSource>) -> Self {
        Self {
            store,
            allowed_sources: allowed_sources.into_iter().collect(),
        }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    /// Construct raw and normalized evidence artifacts. The caller returns
    /// them to `TaskRuntime`, which atomically commits the attempt.
    pub fn acquire_and_normalize<A: EvidenceAdapter + ?Sized>(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        self.authorize_request(permit, need, request, adapter.source())?;
        let acquired = adapter.acquire(request)?;
        self.materialize_acquired(permit, need, request, acquired, 1_000_000, now)
    }

    pub async fn acquire_and_normalize_async<A: AsyncEvidenceAdapter + ?Sized>(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        self.authorize_request(permit, need, request, adapter.source())?;
        let acquired = adapter.acquire(request).await?;
        let confidence_ppm = acquired.quality.completeness_ppm;
        self.materialize_acquired(permit, need, request, acquired, confidence_ppm, now)
    }

    fn authorize_request(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter_source: EvidenceSource,
    ) -> EvidenceRuntimeResult<()> {
        request.validate()?;
        if need.kind != ArtifactKind::EvidenceNeed {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let need_artifact = self.store.artifact(&need.artifact_id)?;
        if need_artifact.kind != ArtifactKind::EvidenceNeed
            || need_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&permit.run_id)
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let declared: EvidenceNeed =
            serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
        declared.validate()?;
        let declared_max_age = i64::try_from(declared.max_age_secs)
            .map(Duration::seconds)
            .map_err(|_| EvidenceRuntimeError::InvalidEvidenceNeed)?;
        if declared.source_family != request.source.as_str()
            || declared.resource != request.resource
            || declared_max_age != request.max_age
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        if !self.allowed_sources.contains(&request.source) {
            return Err(EvidenceRuntimeError::SourceNotAllowed(request.source));
        }
        if adapter_source != request.source {
            return Err(EvidenceAdapterError::SourceMismatch.into());
        }
        Ok(())
    }
}
