// 文件导读：EvidenceRuntime 的入口层把 permit、EvidenceNeed、EvidenceRequest 和 adapter
// 绑定起来。同步入口直接获取并 materialize；异步入口先在不写 CAS 的阶段完成 provider
// 校验，允许调用方检查多资源完整性后再 materialize_validated。authorize_request 每次都
// 重新读取 Store 中本 Run 的 Need，避免调用方用任意 resource/source 越过 allowlist。

impl EvidenceRuntime {
    pub fn new(store: Store, allowed_sources: impl IntoIterator<Item = EvidenceSource>) -> Self {
        // 将允许的 EvidenceSource 收敛为集合；它只控制 adapter 能否被调用，不授予模型访问权。
        Self {
            store,
            allowed_sources: allowed_sources.into_iter().collect(),
        }
    }

    pub fn store(&self) -> &Store {
        // 返回同一 Store 的只读引用，Artifact 写入仍必须经过本 runtime 的 materialization。
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
        // 同步路径：授权 Need→调用 adapter→用完整置信度 materialize Raw/Normalized；返回值
        // 仍待 TaskRuntime 原子提交，调用本身不改变 task terminal state。
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
        // 异步路径先调用 acquire_validated_async，确保 provider 读取/验证完成后才 stage CAS，
        // 便于批量证据在任何一项失败时不留下半成品。
        let acquired = self
            .acquire_validated_async(permit, need, request, adapter, now)
            .await?;
        self.materialize_validated(permit, need, request, acquired, now)
    }

    /// Acquire and validate provider bytes without writing CAS blobs. Callers
    /// that must inspect a complete multi-resource surface can defer
    /// materialization until the entire surface is usable.
    pub async fn acquire_validated_async<A: AsyncEvidenceAdapter + ?Sized>(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<AcquiredEvidence> {
        // 只取得并校验 bytes/时间/provenance，不持久化；cutoff 传入 adapter 作为采集时点，
        // 防止请求完成后才用更晚的时钟掩盖未来数据。
        self.authorize_request(permit, need, request, adapter.source())?;
        let acquired = adapter.acquire_at(request, now).await?;
        Self::validate_acquisition(&acquired, request, now)?;
        Ok(acquired)
    }

    /// Materialize a previously validated acquisition after rechecking the
    /// current permit and declared EvidenceNeed.
    pub fn materialize_validated(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        acquired: AcquiredEvidence,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        // 重新授权当前 permit/Need 后以 completeness ppm 作为 Normalized provenance confidence，
        // 将之前已验证的 acquisition 转成 Raw→Normalized 两个待提交 Artifact。
        self.authorize_request(permit, need, request, request.source)?;
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
        // 逐层检查 request 语法、Need kind/origin、Need payload 的 source/resource/max_age、
        // allowlisted source 和 adapter source；任何不一致都在 provider I/O 前失败。
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
