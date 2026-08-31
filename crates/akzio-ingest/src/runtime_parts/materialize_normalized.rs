impl EvidenceRuntime {
    fn materialize_raw(
        &self,
        permit: &TaskWritePermit,
        request: &EvidenceRequest,
        acquired: &AcquiredEvidence,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::RawEvidence,
            self.store.put_bytes(&acquired.raw, &acquired.media_type)?,
            format!("akzio.ingest.{}.raw", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![],
            now,
        )?)
    }

    fn validate_source_uri(source_uri: &str) -> EvidenceRuntimeResult<()> {
        let parsed = Url::parse(source_uri).map_err(|_| EvidenceRuntimeError::UnsafeSourceUri)?;
        if parsed.username() != ""
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || parsed.query_pairs().any(|(key, _)| {
                let key = key.to_ascii_lowercase();
                key.contains("token")
                    || key.contains("secret")
                    || key.contains("password")
                    || key.contains("api_key")
                    || key == "key"
                    || key.contains("authorization")
            })
        {
            return Err(EvidenceRuntimeError::UnsafeSourceUri);
        }
        Ok(())
    }

    fn attach_news_source_blobs(
        &self,
        value: &mut Value,
        raw: &[u8],
        citations: &[EvidenceCitation],
    ) -> EvidenceRuntimeResult<()> {
        let Some(sources) = value
            .get_mut("source_document")
            .and_then(|document| document.get_mut("sources"))
            .and_then(Value::as_array_mut)
        else {
            return Ok(());
        };

        for source in sources {
            if source.get("status").and_then(Value::as_str) != Some("snapshot") {
                continue;
            }
            let start = source
                .get("bundle_start_byte")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            let end = source
                .get("bundle_end_byte")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            let bytes = raw
                .get(start..end)
                .filter(|bytes| !bytes.is_empty())
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            if source
                .get("claim_binding")
                .and_then(|binding| binding.get("status"))
                .and_then(Value::as_str)
                == Some("exact_quote")
            {
                let binding = source
                    .get("claim_binding")
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let quote = binding
                    .get("quote")
                    .and_then(Value::as_str)
                    .filter(|quote| !quote.trim().is_empty())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let source_start = binding
                    .get("source_start_byte")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let source_end = binding
                    .get("source_end_byte")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let bundle_start = binding
                    .get("bundle_start_byte")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let bundle_end = binding
                    .get("bundle_end_byte")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                if bytes.get(source_start..source_end) != Some(quote.as_bytes())
                    || bundle_start != start.saturating_add(source_start)
                    || bundle_end != start.saturating_add(source_end)
                    || !citations.iter().any(|citation| {
                        citation.start_byte == bundle_start
                            && citation.end_byte == bundle_end
                            && citation.quote == quote
                    })
                {
                    return Err(EvidenceRuntimeError::InvalidCitation);
                }
            }
            let expected_hash = source
                .get("content_hash")
                .and_then(Value::as_str)
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            if ContentHash::of_bytes(bytes).as_str() != expected_hash {
                return Err(EvidenceRuntimeError::InvalidAcquisition);
            }
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            let blob = self.store.put_bytes(bytes, media_type)?;
            source
                .as_object_mut()
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?
                .insert("blob".to_owned(), serde_json::to_value(blob)?);
        }
        Ok(())
    }

    fn materialize_acquired(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        acquired: AcquiredEvidence,
        confidence_ppm: u32,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        Self::validate_acquisition(&acquired, request, now)?;

        let raw = self.materialize_raw(permit, request, &acquired, now)?;
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        };
        let mut normalized_value = acquired.normalized.clone();
        if request.source == EvidenceSource::NewsWeb {
            self.attach_news_source_blobs(
                &mut normalized_value,
                &acquired.raw,
                &acquired.provenance.citations,
            )?;
        }
        let normalized_payload = NormalizedEvidencePayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            need: need.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            value: normalized_value,
            provenance: acquired.provenance.clone(),
            quality: acquired.quality.clone(),
        };
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            self.store.put_json(&normalized_payload)?,
            format!("akzio.ingest.{}.normalized", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![raw_ref, need.clone()],
            now,
        )?;
        Ok(EvidenceBundle { raw, normalized })
    }

    fn validate_acquisition(
        acquired: &AcquiredEvidence,
        request: &EvidenceRequest,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<()> {
        if acquired.raw.is_empty()
            || acquired.media_type.trim().is_empty()
            || acquired.source_uri.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        acquired.provenance.validate(
            &acquired.raw,
            &acquired.source_uri,
            acquired.observed_at,
        )?;
        acquired.quality.validate()?;
        Self::validate_source_uri(&acquired.source_uri)?;
        if now.signed_duration_since(acquired.observed_at) > request.max_age {
            return Err(EvidenceRuntimeError::StaleEvidence);
        }
        Ok(())
    }
}
