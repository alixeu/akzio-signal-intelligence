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

fn manifest_input_hash(
    selections: &[akzio_domain::ContextSelection],
) -> Result<akzio_domain::ContentHash, serde_json::Error> {
    content_hash_json(&serde_json::to_value(
        selections
            .iter()
            .map(|selection| (&selection.artifact.artifact_id, selection.artifact.kind))
            .collect::<Vec<_>>(),
    )?)
}

fn estimate_tokens(bytes: u64) -> u32 {
    akzio_domain::estimate_tokens_from_bytes(bytes)
}
