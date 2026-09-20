impl ContextBroker {
    // 验证父 Attempt 产出的 Artifact 是否仍能沿父 Manifest、ReadGrant 和
    // 已授权 raw 闭包回溯。RawEvidence/trace 永远不能直接成为子任务的输出来源。
    fn validate_parent_output_provenance(
        &self,
        output: &Artifact,
        parent_manifest: &ArtifactRef,
        parent_readable: &BTreeSet<ArtifactRef>,
        parent_raw_closure: &BTreeSet<ArtifactId>,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
    ) -> ContextResult<()> {
        if output.kind == ArtifactKind::RawEvidence || is_trace_kind(output.kind) {
            // 输出类型本身越过 Context 的研究材料边界，先于递归 provenance 检查拒绝。
            return Err(ContextError::InvalidManifestClosure);
        }
        let proof = ParentContextProof {
            manifest: parent_manifest,
            readable: parent_readable,
            raw_closure: parent_raw_closure,
            permit: parent_permit,
            contract: parent_contract,
        };
        self.validate_parent_attempt_artifact(output, proof.permit, proof.contract)?;
        self.validate_parent_output_sources(output, &proof, &mut BTreeSet::new())
    }

    fn validate_parent_output_sources(
        &self,
        artifact: &Artifact,
        proof: &ParentContextProof<'_>,
        visiting: &mut BTreeSet<ArtifactId>,
    ) -> ContextResult<()> {
        // visiting 同时防止 source_refs 环路和递归无限展开；每个递归分支只接受
        // Store 中 kind 一致、且能被父 grant 或已验证安全 trace 覆盖的来源。
        if !visiting.insert(artifact.artifact_id.clone()) {
            return Err(ContextError::InvalidManifestClosure);
        }
        for source in &artifact.source_refs {
            let source_artifact = self.store.artifact(&source.artifact_id)?;
            if source_artifact.kind != source.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            if source == proof.manifest {
                if source_artifact.kind != ArtifactKind::ContextManifest {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if source.kind == ArtifactKind::RawEvidence {
                // RawEvidence 不进入 readable，只能由父 Manifest 计算出的 raw_closure 授权。
                if !proof.raw_closure.contains(&source.artifact_id) {
                    return Err(ContextError::InvalidManifestClosure);
                }
                continue;
            }
            if !is_trace_kind(source.kind) && proof.readable.contains(source) {
                continue;
            }
            if is_safe_deliberation_summary(source.kind) {
                // 安全的 deliberation summary 需要沿同一父 Attempt 继续验证，不能
                // 仅凭一个已授权引用绕过其内部 source_refs 闭包。
                self.validate_parent_attempt_artifact(
                    &source_artifact,
                    proof.permit,
                    proof.contract,
                )?;
                self.validate_parent_output_sources(&source_artifact, proof, visiting)?;
                continue;
            }
            if !is_trace_kind(source.kind) {
                return Err(ContextError::InvalidManifestClosure);
            }
            // 其他 trace 只能作为同一父 Attempt 的持久化运行痕迹递归验证，不能当作
            // 普通研究证据直接暴露给子任务。
            self.validate_parent_attempt_artifact(&source_artifact, proof.permit, proof.contract)?;
            self.validate_parent_output_sources(&source_artifact, proof, visiting)?;
        }
        visiting.remove(&artifact.artifact_id);
        Ok(())
    }

    fn validate_parent_attempt_artifact(
        &self,
        artifact: &Artifact,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
    ) -> ContextResult<()> {
        // 先校验 Artifact 自身哈希与 CAS 中的原值，再核对 Run/Task/Attempt/Contract
        // 身份；因此“看起来相同”的外部构造对象不能冒充父 Attempt 产物。
        artifact.validate()?;
        if self.store.artifact(&artifact.artifact_id)? != *artifact {
            return Err(ContextError::InvalidManifestClosure);
        }
        let Some(origin) = artifact.origin.as_ref() else {
            return Err(ContextError::InvalidManifestClosure);
        };
        if origin.run_id.as_ref() != Some(&parent_permit.run_id)
            || origin.task_id.as_ref() != Some(&parent_permit.task_id)
            || origin.attempt_id.as_ref() != Some(&parent_permit.attempt_id)
            || origin.contract_hash.as_ref() != Some(&parent_contract.contract_hash)
            || artifact.provenance.producer_contract_hash.as_ref()
                != Some(&parent_contract.contract_hash)
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        Ok(())
    }

    fn restore_manifest_for_proof(
        &self,
        proof: &SucceededAttemptProof,
        contract: &AgentContract,
        artifact: Artifact,
        payload: ContextManifestPayload,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextManifest> {
        // 这是对已成功 Attempt 的历史 Manifest 恢复，不重新创建 Artifact；恢复后的
        // grant 只用于当前校验调用，过期时间从 now 起算，后续仍受父证明约束。
        contract.validate()?;
        payload.validate(&contract.context)?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let mut expected_source_refs = selected;
        expected_source_refs.extend(
            artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .cloned(),
        );
        if artifact.kind != ArtifactKind::ContextManifest
            || expected_source_refs != artifact.source_refs.iter().cloned().collect()
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        let readable = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        let raw_source_closure = self.raw_closure(&contract.context, &payload.selections)?;
        Ok(ContextManifest {
            artifact,
            payload,
            grant: ReadGrant {
                manifest_artifact_id: proof
                    .context_manifest
                    .as_ref()
                    .ok_or(ContextError::InvalidManifestClosure)?
                    .artifact_id
                    .clone(),
                run_id: proof.run_id.clone(),
                task_id: proof.task_id.clone(),
                attempt_id: proof.attempt_id.clone(),
                lease_id: proof.lease_id.clone(),
                epoch: proof.epoch,
                contract_hash: contract.contract_hash.clone(),
                readable,
                raw_source_closure,
                expires_at: now,
            },
        })
    }

    pub fn read(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        // 普通读取必须同时满足 Attempt/Contract 身份、Manifest readable 集合和
        // 当前持久化闭包；读到 trace 或 RawEvidence 时仍拒绝，不把 grant 变成 raw 许可。
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        if !grant.permits(artifact_id, false, now) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        self.read_from_validated_grant(permit, contract, grant, artifact_id, now)
    }

    // Only used after full persisted-closure validation in this synchronous
    // operation. CAS artifacts are immutable; live attempt authority is not.
    fn read_from_validated_grant(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.store.validate_task_permit(permit)?;
        if !grant.permits(artifact_id, false, now) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        let artifact = self.store.artifact(artifact_id)?;
        if is_trace_kind(artifact.kind) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact.artifact_id,
            });
        }
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(ContextError::RawEvidenceRequiresExplicitRead);
        }
        Ok(artifact)
    }

    pub fn read_raw(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        // RawEvidence 是显式的第二条读取路径：它只能命中 raw_source_closure，且调用者
        // 必须明确使用 read_raw；这不扩大普通 readable 集合，也不改变 Manifest。
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        if !grant.permits(artifact_id, true, now) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        let artifact = self.store.artifact(artifact_id)?;
        if is_trace_kind(artifact.kind) {
            return Err(ContextError::GrantDenied {
                manifest_id: grant.manifest_artifact_id.clone(),
                artifact_id: artifact.artifact_id,
            });
        }
        if artifact.kind != ArtifactKind::RawEvidence {
            return Err(ContextError::ExpectedRawEvidence);
        }
        Ok(artifact)
    }

    pub fn read_document(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<(Artifact, Value)> {
        // 先走普通 Artifact 授权，再把 CAS blob 转成模型可消费的 Value；Blob 读取失败
        // 会向上传播，非 JSON 内容由 document_value 明确降级为 UTF-8 字符串。
        let artifact = self.read(permit, contract, grant, artifact_id, now)?;
        let value = self.document_value(&artifact)?;
        Ok((artifact, value))
    }

pub fn read_raw_document(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<(Artifact, Value)> {
        // 与 read_document 相同，但授权类型固定为 RawEvidence；Value 仍只来自已授权
        // 的 CAS blob，调用方不能借此读取 Manifest 外的原始数据。
        let artifact = self.read_raw(permit, contract, grant, artifact_id, now)?;
        let value = self.document_value(&artifact)?;
    Ok((artifact, value))
}

pub fn read_authority_document(
    &self,
    contract: &AgentContract,
    blob_ref: &BlobRef,
) -> ContextResult<Vec<u8>> {
    // Contract 的 prompt、output schema 和 tool schema 是 Rust 声明的权威 blob；这里只
    // 允许读取这些精确引用，不能把任意 BlobRef 当成提示词或 Schema 读取。
    contract.validate()?;
    let declared = contract.prompt.governance == *blob_ref
        || contract.prompt.role == *blob_ref
        || contract.output.schema == *blob_ref
        || contract
            .tool_specs
            .iter()
            .any(|spec| spec.input_schema == *blob_ref);
    if !declared {
        return Err(ContextError::AuthorityBlobNotDeclared);
    }
    Ok(self.store.read_blob(blob_ref)?)
}

fn document_value(&self, artifact: &Artifact) -> ContextResult<Value> {
        // JSON 文档保持结构化 Value；非 JSON blob 退化为明确的 UTF-8 lossy 字符串，
        // 这只是表示层兼容，不会绕过 Artifact/ReadGrant 权限检查。
        let bytes = self.store.read_blob(&artifact.blob)?;
        Ok(serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())))
    }

    fn validate_persisted_grant(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        now: DateTime<Utc>,
    ) -> ContextResult<()> {
        // 每次工具读取都重新从 Store 载入 Manifest 和 payload，验证 grant 当前仍与
        // 持久化闭包、预算、source_refs 和 raw closure 一致，而不是只相信内存副本。
        let artifact = self.store.artifact(&grant.manifest_artifact_id)?;
        let payload: ContextManifestPayload = self.read_payload(&artifact)?;
        let manifest = ContextManifest {
            artifact,
            payload,
            grant: grant.clone(),
        };
        self.validate_manifest_closure(permit, contract, &manifest, now, true)
            .map(|_| ())
    }
}
