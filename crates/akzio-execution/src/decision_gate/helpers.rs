fn unique_manifest_ref(proposal: &Artifact) -> DecisionGateResult<&ArtifactRef> {
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
    akzio_domain::estimate_tokens_from_bytes(bytes)
}
