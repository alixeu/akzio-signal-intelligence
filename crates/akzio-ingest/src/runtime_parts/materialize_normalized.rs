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
        let normalized_payload = NormalizedEvidencePayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            need: need.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            value: acquired.normalized.clone(),
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
