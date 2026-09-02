const MAX_CONTEXT_RANGE_BYTES: usize = 32 * 1024;
const MAX_CONTEXT_SEARCH_RESULTS: usize = 16;
const MAX_CONTEXT_COMPARE_SOURCES: usize = 4;

impl ContextBroker {
    pub fn read_document_result(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        let (artifact, value) = self.read_document(permit, contract, grant, artifact_id, now)?;
        Ok(ContextReadResult {
            artifacts: vec![artifact],
            value,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_range(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        start_byte: usize,
        end_byte: usize,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        let artifact = self.read(permit, contract, grant, artifact_id, now)?;
        if start_byte >= end_byte || end_byte.saturating_sub(start_byte) > MAX_CONTEXT_RANGE_BYTES {
            return Err(ContextError::InvalidRange);
        }
        let bytes = self.store.read_blob(&artifact.blob)?;
        let range = bytes
            .get(start_byte..end_byte)
            .ok_or(ContextError::InvalidRange)?;
        // Byte offsets are exact. Reject a split UTF-8 codepoint instead of
        // returning replacement characters which were never in the evidence.
        let text = std::str::from_utf8(range).map_err(|_| ContextError::InvalidRange)?;
        Ok(ContextReadResult {
            artifacts: vec![artifact.clone()],
            value: serde_json::json!({
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind,
                "start_byte": start_byte,
                "end_byte": end_byte,
                "total_bytes": bytes.len(),
                "text": text,
            }),
        })
    }

    pub fn search_context(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        query: &str,
        max_results: usize,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        let query = query.trim();
        if query.is_empty()
            || query.chars().count() > 256
            || !(1..=MAX_CONTEXT_SEARCH_RESULTS).contains(&max_results)
        {
            return Err(ContextError::InvalidSearch);
        }
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        let manifest_artifact = self.store.artifact(&grant.manifest_artifact_id)?;
        let manifest: ContextManifestPayload = self.read_payload(&manifest_artifact)?;
        let needle = query.to_lowercase();
        let mut artifacts = Vec::new();
        let mut matches = Vec::new();
        for selection in manifest.selections {
            if matches.len() >= max_results {
                break;
            }
            let (artifact, value) = self.read_document(
                permit,
                contract,
                grant,
                &selection.artifact.artifact_id,
                now,
            )?;
            let text = match &value {
                Value::String(text) => text.clone(),
                _ => serde_json::to_string(&value)?,
            };
            let Some((start_byte, end_byte, match_byte)) = search_snippet_range(&text, &needle)
            else {
                continue;
            };
            let snippet = &text[start_byte..end_byte];
            matches.push(serde_json::json!({
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind,
                "source": artifact.provenance.source_family,
                "snippet": snippet,
                "start_byte": start_byte,
                "end_byte": end_byte,
                "match_byte": match_byte,
                "offset_basis": "document_value_text",
            }));
            artifacts.push(artifact);
        }
        Ok(ContextReadResult {
            artifacts,
            value: serde_json::json!({
                "query": query,
                "matches": matches,
            }),
        })
    }

    pub fn read_claim_evidence(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        claim_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        let claim_artifact = self.read(permit, contract, grant, claim_id, now)?;
        if claim_artifact.kind != ArtifactKind::Claim {
            return Err(ContextError::ExpectedClaim);
        }
        let claim: ResearchClaim = self.read_payload(&claim_artifact)?;
        claim.validate()?;
        let mut artifacts = vec![claim_artifact.clone()];
        let mut evidence = Vec::new();
        let mut seen = BTreeSet::new();
        for ground in &claim.grounds {
            if !seen.insert(ground.evidence.artifact_id.clone()) {
                continue;
            }
            let (artifact, value) =
                self.read_document(permit, contract, grant, &ground.evidence.artifact_id, now)?;
            if artifact.kind != ground.evidence.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            evidence.push(serde_json::json!({
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind,
                "support": ground.support,
                "value": value,
            }));
            artifacts.push(artifact);
        }
        Ok(ContextReadResult {
            artifacts,
            value: serde_json::json!({
                "claim_artifact_id": claim_artifact.artifact_id,
                "claim": claim,
                "evidence": evidence,
            }),
        })
    }

    pub fn compare_sources(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_ids: &[ArtifactId],
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        if !(2..=MAX_CONTEXT_COMPARE_SOURCES).contains(&artifact_ids.len())
            || artifact_ids.iter().collect::<BTreeSet<_>>().len() != artifact_ids.len()
        {
            return Err(ContextError::InvalidComparison);
        }
        let mut artifacts = Vec::with_capacity(artifact_ids.len());
        let mut sources = Vec::with_capacity(artifact_ids.len());
        for artifact_id in artifact_ids {
            let (artifact, value) =
                self.read_document(permit, contract, grant, artifact_id, now)?;
            sources.push(serde_json::json!({
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind,
                "source": artifact.provenance.source_family,
                "observed_at": artifact.provenance.observed_at,
                "value": value,
            }));
            artifacts.push(artifact);
        }
        Ok(ContextReadResult {
            artifacts,
            value: serde_json::json!({ "sources": sources }),
        })
    }
}

// Lowercasing can change UTF-8 length (for example İ). Keep the mapping to
// original byte boundaries so a case-insensitive match never corrupts offsets.
fn search_snippet_range(text: &str, needle: &str) -> Option<(usize, usize, usize)> {
    let mut folded = String::new();
    let mut offsets = Vec::new();
    let chars = text.char_indices().collect::<Vec<_>>();
    for (index, (_, ch)) in chars.iter().enumerate() {
        let lower = ch.to_lowercase().collect::<String>();
        offsets.extend(std::iter::repeat_n(index, lower.len()));
        folded.push_str(&lower);
    }
    let found = folded.find(needle)?;
    let first = offsets[found];
    let last = offsets[found + needle.len() - 1];
    let start = first.saturating_sub(160);
    let end = (last + 1 + 320).min(chars.len());
    Some((
        chars[start].0,
        chars.get(end).map_or(text.len(), |item| item.0),
        chars[first].0,
    ))
}
