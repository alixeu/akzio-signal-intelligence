const MAX_CONTEXT_RANGE_BYTES: usize = 32 * 1024;
const MAX_CONTEXT_SEARCH_RESULTS: usize = 16;
const MAX_CONTEXT_COMPARE_SOURCES: usize = 4;

fn validate_document_response_size(value: &Value) -> ContextResult<()> {
    if serde_json::to_vec(value)?.len() > MAX_CONTEXT_RANGE_BYTES {
        return Err(ContextError::DocumentRequiresRange);
    }
    Ok(())
}

impl ContextBroker {
    /// Byte spans point at complete serialized top-level values, so a model need
    /// not guess offsets into OHLCV. The original grant is checked before sizing.
    pub fn document_range_metadata(
        &self, permit: &TaskWritePermit, contract: &AgentContract, grant: &ReadGrant,
        artifact_id: &ArtifactId, now: DateTime<Utc>,
    ) -> ContextResult<Value> {
        let artifact = self.read(permit, contract, grant, artifact_id, now)?;
        let bytes = self.store.read_blob(&artifact.blob)?;
        let value: Value = serde_json::from_slice(&bytes)?;
        Ok(range_metadata(&value, &bytes))
    }
    pub fn read_document_result(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        artifact_id: &ArtifactId,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextReadResult> {
        let (artifact, value) = self.read_document(permit, contract, grant, artifact_id, now)?;
        validate_document_response_size(&value)?;
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
                // Use the same explicit projection as the inline context. Full
                // originals remain available through the unchanged read grant.
                "value": compact_governed_projection(artifact.kind, value),
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

#[cfg(test)]
mod comparison_projection_tests {
    use super::*;

    #[test]
    fn oversized_full_document_is_rejected_not_silently_truncated() {
        let value = serde_json::json!({"text": "x".repeat(32768)});
        assert!(matches!(validate_document_response_size(&value), Err(ContextError::DocumentRequiresRange)));
        assert_eq!(value["text"].as_str().unwrap().len(), 32768);
        assert!(validate_document_response_size(&serde_json::json!({"text":"bounded"})).is_ok());
    }

    #[test]
    fn comparison_uses_governed_projection_without_replaying_252_bars() {
        let value = serde_json::json!({"resource":"bars:SOXX:1d:2025-08-05:252", "source":"alpaca",
            "time_basis":{"decision_clock":{"decision_cutoff":"2026-09-09T07:00:00Z"}},
            "quality":{"citations_complete":true}, "quant_features":{"sample_count":252},
            "value":{"bars":(0..252).map(|i| serde_json::json!({"c":i,"t":i})).collect::<Vec<_>>()}});
        let projected = compact_governed_projection(ArtifactKind::NormalizedEvidence, value.clone());
        assert_eq!(projected["time_basis"], value["time_basis"]);
        assert_eq!(projected["quality"], value["quality"]);
        assert_eq!(projected["quant_features"], value["quant_features"]);
        assert_eq!(projected["value_summary"]["bars"].as_array().unwrap().len(), 5);
        assert_eq!(projected["value_summary"]["bars_projection"]["original_count"], 252);
        assert!(projected["full_document"].as_str().unwrap().contains("read_range"));
        assert!(serde_json::to_vec(&projected).unwrap().len() < 2048);
    }

    #[test]
    fn option_chain_projection_is_bounded_and_retains_exact_time_and_source_fields() {
        let snapshots = (0..2_048)
            .map(|index| {
                (
                    format!("TQQQ260918C{:05}", 50_000 + index),
                    serde_json::json!({
                        "impliedVolatility": 0.5 + f64::from(index % 10) / 100.0,
                        "bid": 1.25,
                        "ask": 1.35,
                        "openInterest": index,
                        "delta": 0.42,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let value = serde_json::json!({
            "source": "alpaca",
            "resource": "option_chain:TQQQ:2026-09-10:2027-01-08",
            "need": {"artifact_id": "need", "kind": "evidence_need"},
            "raw": {"artifact_id": "raw", "kind": "raw_evidence"},
            "observed_at": "2026-09-10T20:00:00Z",
            "time_basis": {"available_at": "2026-09-10T20:00:00Z"},
            "quality": {"completeness_ppm": 1_000_000, "normalized": true},
            "provenance": {"source_uri": "https://data.alpaca.markets/options"},
            "value": {"snapshots": snapshots, "next_page_token": null},
        });

        let projected = compact_governed_projection(ArtifactKind::NormalizedEvidence, value.clone());
        assert_eq!(projected["type"], "option_chain_projection");
        assert_eq!(projected["resource"], value["resource"]);
        assert_eq!(projected["time_basis"], value["time_basis"]);
        assert_eq!(projected["features"]["contracts_total"], 2_048);
        assert_eq!(projected["features"]["contracts_with_iv"], 2_048);
        assert_eq!(projected["sample_contracts"].as_array().unwrap().len(), 8);
        assert!(projected["missing_items"].as_array().unwrap().is_empty());
        assert!(serde_json::to_vec(&projected).unwrap().len() < 16 * 1024);
    }
}

fn range_metadata(value: &Value, bytes: &[u8]) -> Value {
    // Offsets are safe only when the parsed/re-encoded JSON is byte-identical
    // to the logical CAS bytes. serde_json::Value does not preserve source key
    // order, so a same-length re-encoding is not enough evidence.
    let total_bytes = bytes.len();
    let encoded = serde_json::to_vec(value).expect("JSON value serializes");
    let mut ranges = Vec::new();
    if encoded == bytes {
        if let Some(object) = value.as_object() {
        let mut offset = 1;
        for (key, value) in object {
            let prefix = serde_json::to_vec(key).unwrap().len() + 1;
            let length = serde_json::to_vec(value).unwrap().len();
            if length <= MAX_CONTEXT_RANGE_BYTES {
                ranges.push(serde_json::json!({"field":key,"start_byte":offset+prefix,"end_byte":offset+prefix+length}));
            }
            offset += prefix + length + 1;
            }
        }
    }
    serde_json::json!({"total_bytes":total_bytes,"encoding":"UTF-8 canonical JSON",
        "max_range_bytes":MAX_CONTEXT_RANGE_BYTES,"ranges":if encoded.len()==total_bytes {ranges} else {Vec::new()},
        "guidance":"Use the existing context projection first. These ranges are complete top-level JSON values; request only a needed field. Large omitted fields remain in the original document."})
}

#[cfg(test)]
mod range_metadata_tests {
    use super::*;
    #[test]
    fn recommended_ranges_are_exact_json_values() {
        let v=serde_json::json!({"bars":[1,2,3],"label":"价格", "quant_features":{"return_ppm":12}});
        let bytes=serde_json::to_vec(&v).unwrap();
        let hint=range_metadata(&v,&bytes);
        for range in hint["ranges"].as_array().unwrap() {
            let start=range["start_byte"].as_u64().unwrap() as usize;
            let end=range["end_byte"].as_u64().unwrap() as usize;
            let parsed:Value=serde_json::from_slice(&bytes[start..end]).unwrap();
            assert_eq!(parsed,v[range["field"].as_str().unwrap()]);
        }
    }

    #[test]
    fn key_order_mismatch_never_emits_wrong_ranges() {
        let bytes = br#"{"z":{"value":1},"a":{"value":2}}"#;
        let value: Value = serde_json::from_slice(bytes).unwrap();
        let hint = range_metadata(&value, bytes);
        assert!(hint["ranges"].as_array().unwrap().is_empty());
    }
}
