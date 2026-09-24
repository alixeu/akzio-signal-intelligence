// 文件导读：辅助函数只负责从 Proposal 中取得唯一 ContextManifest，以及按领域统一规则
// 估算 Artifact blob 的 token 数；它们不进行业务放宽或 Store 写入。

fn unique_manifest_ref(proposal: &Artifact) -> DecisionGateResult<&ArtifactRef> {
    // 返回类型省略的生命周期与 `proposal` 输入借用关联，因此引用只能在 proposal
    // 有效期间使用；函数不克隆、不移走其 source_refs 中的元素。
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
    // `bytes` 按值传入并复制给纯计算函数；返回统一估算值，不读取 Artifact 正文。
    // 使用 domain 的统一字节→token 估算，与 ContextManifest 生成时的预算口径保持一致。
    akzio_domain::estimate_tokens_from_bytes(bytes)
}
