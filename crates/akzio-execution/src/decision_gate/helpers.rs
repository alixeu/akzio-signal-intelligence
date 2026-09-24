// 文件导读：辅助函数只负责从 Proposal 中取得唯一 ContextManifest，以及按领域统一规则
// 估算 Artifact blob 的 token 数；它们不进行业务放宽或 Store 写入。

fn unique_manifest_ref(proposal: &Artifact) -> DecisionGateResult<&ArtifactRef> {
    // 要求恰好一个 Manifest；多一个或少一个都无法确定模型实际看到的证据集合。
    let mut manifests = proposal
        .source_refs
        .iter()
        .filter(|reference| reference.kind == ArtifactKind::ContextManifest);
    let manifest = manifests
        .next()
        .ok_or(DecisionGateError::InvalidManifestReference)?;
    if manifests.next().is_some() {
        return Err(DecisionGateError::InvalidManifestReference);
    }
    Ok(manifest)
}

fn estimate_tokens(bytes: u64) -> u32 {
    // 使用 domain 的统一字节→token 估算，与 ContextManifest 生成时的预算口径保持一致。
    akzio_domain::estimate_tokens_from_bytes(bytes)
}
