// 文件导读：标准化 materialization 负责把 acquisition 的 raw bytes、时间基准、污染证书、
// quant features、financial-content 评估和 provider provenance 合成 NormalizedEvidence。
// 新闻 snapshot 的 byte binding 会先切出 Raw 子 blob；日线按 cutoff 计算特征；所有验证
// 先于 Artifact::new，最终只 stage，不绕过 Store 的任务提交事务。

impl EvidenceRuntime {
    fn materialize_raw(
        &self,
        permit: &TaskWritePermit,
        request: &EvidenceRequest,
        acquired: &AcquiredEvidence,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<Artifact> {
        // 原始 provider bytes 以 source family、observed/retrieved 时间和 permit origin 封存，
        // 不在 RawEvidence 中加入模型重写内容。
        Ok(Artifact::new(
            ArtifactKind::RawEvidence,
            self.store
                .stage_bytes(&acquired.raw, &acquired.media_type)?,
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
        // 复用统一 governed URI 规则，拒绝认证信息、敏感 query、fragment 或非可解析 URL。
        if governed_source_uri_is_safe(source_uri) {
            Ok(())
        } else {
            Err(EvidenceRuntimeError::UnsafeSourceUri)
        }
    }

    fn attach_news_source_blobs(
        &self,
        value: &mut Value,
        raw: &[u8],
        raw_blob: &BlobRef,
        citations: &[EvidenceCitation],
    ) -> EvidenceRuntimeResult<()> {
        // 对新闻 source_document 的 snapshot 元数据做精确 byte/hash/quote binding；任何声明
        // 与 Raw bundle 不一致都视为 provenance 破坏，不能降级成普通缺少引用。
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
            // A declared snapshot is an integrity claim about this exact raw
            // bundle. Contradictory metadata is not ordinary missing content.
            let start = source
                .get("bundle_start_byte")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
            let end = source
                .get("bundle_end_byte")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
            let bytes = raw
                .get(start..end)
                .filter(|bytes| !bytes.is_empty())
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
            let bindings = source
                .get("claim_bindings")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if source.get("claim_binding") != bindings.first() {
                return Err(EvidenceRuntimeError::InvalidCitation);
            }
            for binding in &bindings {
                if binding.get("status").and_then(Value::as_str) != Some("exact_quote") {
                    continue;
                }
                let quote = binding
                    .get("quote")
                    .and_then(Value::as_str)
                    .filter(|quote| !quote.trim().is_empty())
                    .ok_or(EvidenceRuntimeError::InvalidCitation)?;
                let source_start = binding_byte(binding, "source_start_byte")?;
                let source_end = binding_byte(binding, "source_end_byte")?;
                let bundle_start = binding_byte(binding, "bundle_start_byte")?;
                let bundle_end = binding_byte(binding, "bundle_end_byte")?;
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
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
            if ContentHash::of_bytes(bytes).as_str() != expected_hash {
                return Err(EvidenceRuntimeError::InvalidProvenance);
            }
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
            let blob = self.store.stage_slice(raw_blob, start, end, media_type)?;
            source
                .as_object_mut()
                .ok_or(EvidenceRuntimeError::InvalidProvenance)?
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
        // 先计算 EvidenceTimeBasis 和 contamination certificate，再封 Raw、绑定新闻子 blob、
        // 按资源类型构造 quant/financial projection，最后创建 NormalizedEvidence 及其 lineage。
        let time_basis = Self::validate_acquired_evidence(request, &acquired, now)?;
        let contamination_certificate =
            EvidenceContaminationCertificate::for_time_basis(&time_basis)?;
        let mut normalized_value = acquired.normalized.clone();
        let raw = self.materialize_raw(permit, request, &acquired, now)?;
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        };
        if request.source == EvidenceSource::NewsWeb {
            self.attach_news_source_blobs(
                &mut normalized_value,
                &acquired.raw,
                &raw.blob,
                &acquired.provenance.citations,
            )?;
        }
        let quant_features = if matches!(
            GovernedResource::parse(request.source, &request.resource)?,
            GovernedResource::AlpacaBars {
                raw_prices: false,
                ..
            }
        ) {
            Some(crate::quant_features::build_quant_feature_snapshot(
                &normalized_value,
                &request.resource,
                &acquired.source_uri,
                &time_basis.decision_clock,
            )?)
        } else {
            None
        };
        let financial_content = matches!(
            request.source,
            EvidenceSource::NewsWeb | EvidenceSource::SecEdgar
        )
        .then(|| {
            crate::financial_content::assess_financial_content(
                &acquired.raw,
                &normalized_value,
                &acquired.provenance,
                request.source,
                &request.resource,
                now,
            )
        })
        .transpose()?;

        let normalized_payload = NormalizedEvidencePayload {
            schema_version: DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            need: need.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            time_basis,
            contamination_certificate,
            quant_features,
            financial_content,
            value: normalized_value,
            provenance: acquired.provenance.clone(),
            quality: acquired.quality.clone(),
        };
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            self.store
                .stage_json_with_dictionary(&normalized_payload, &raw.blob)?,
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

    /// Shared read-only validation for adapter preflight and CAS materialization.
    pub fn validate_acquired_evidence(
        request: &EvidenceRequest,
        acquired: &AcquiredEvidence,
        cutoff: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceTimeBasis> {
        // 给 adapter preflight 和 CAS materialization 共用的只读校验入口，避免两条路径对
        // freshness/cutoff/provenance 得出不同结论。
        Self::validate_acquisition(acquired, request, cutoff)?;
        Self::time_basis(request, acquired, cutoff)
    }

    fn time_basis(
        request: &EvidenceRequest,
        acquired: &AcquiredEvidence,
        decision_cutoff: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceTimeBasis> {
        // 按 provider/source 选择 event、published/vintage、availability 和 retrieval 时间；
        // Alpaca bars 必须有 session close/fixture cutoff 证明，Fred 使用 vintage，新闻/SEC
        // 缺出版时间时不凭检索时间倒推历史可用性。
        let governed = GovernedResource::parse(request.source, &request.resource)?;
        let vintage = match &governed {
            GovernedResource::Fred { vintage, .. } => *vintage,
            GovernedResource::FredReleaseCalendar { vintage, .. } => Some(*vintage),
            _ => None,
        };
        let event_time = match request.source {
            EvidenceSource::Alpaca => {
                let value = &acquired.normalized;
                if value.get("market").is_some() { latest_rfc3339_timestamp(&value["bars"]) }
                else if value.get("coverage").is_some() { latest_rfc3339_timestamp(&value["snapshots"]) }
                else { latest_rfc3339_timestamp(value) }
            },
            EvidenceSource::Fred => latest_fred_observation_date(&acquired.normalized),
            EvidenceSource::SecEdgar | EvidenceSource::NewsWeb => None,
        };
        let available_at = match request.source {
            EvidenceSource::Fred => vintage
                .and_then(|date| date.and_hms_opt(23, 59, 59))
                .map(|value| value.and_utc())
                .ok_or(EvidenceRuntimeError::MissingAvailability)?,
            // When a source does not expose a trustworthy publication time,
            // first verified retrieval is the earliest defensible availability.
            // Historical replay therefore fails closed because retrieved_at is
            // later than its historical decision cutoff.
            EvidenceSource::NewsWeb | EvidenceSource::SecEdgar => acquired
                .provenance
                .published_at
                .unwrap_or(acquired.observed_at),
            EvidenceSource::Alpaca => acquired
                .normalized
                .get("content_available_at")
                .and_then(Value::as_str)
                .map(|value| {
                    DateTime::parse_from_rfc3339(value).map(|time| time.with_timezone(&Utc))
                })
                .transpose()
                .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?
                .unwrap_or_else(|| event_time.unwrap_or(acquired.observed_at)),
        };
        let decision_clock = DecisionClock { decision_cutoff };
        if matches!(governed, GovernedResource::AlpacaBars { .. }) {
            if acquired.normalized.get("session_closes").is_some() {
                if !decision_clock.contains(available_at) {
                    return Err(EvidenceRuntimeError::TemporalContamination);
                }
            } else if acquired.source_uri.starts_with("fixture://") {
                if event_time
                    .is_some_and(|value| !decision_clock.contains_completed_daily_bar(value))
                {
                    return Err(EvidenceRuntimeError::TemporalContamination);
                }
            } else {
                return Err(EvidenceRuntimeError::MissingAvailability);
            }
        }
        let time_basis = EvidenceTimeBasis {
            event_time,
            released_at: acquired.provenance.published_at,
            available_at,
            retrieved_at: acquired.observed_at,
            decision_clock,
            vintage,
            revision: acquired.provenance.revision.clone(),
        };
        time_basis.validate()?;
        Ok(time_basis)
    }

    fn validate_acquisition(
        acquired: &AcquiredEvidence,
        request: &EvidenceRequest,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<()> {
        // 在任何 CAS 写入前检查 raw/media/source URI、provenance/citation、quality 和 max_age。
        // 下载成功只证明取得了 bytes，不证明时间上可用于当前 Decision。
        if acquired.raw.is_empty()
            || acquired.media_type.trim().is_empty()
            || acquired.source_uri.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        acquired
            .provenance
            .validate(&acquired.raw, &acquired.source_uri, acquired.observed_at)?;
        acquired.quality.validate()?;
        Self::validate_source_uri(&acquired.source_uri)?;
        if now.signed_duration_since(acquired.observed_at) > request.max_age {
            return Err(EvidenceRuntimeError::StaleEvidence);
        }
        Ok(())
    }
}

fn latest_rfc3339_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    // 递归遍历 provider JSON，取 t/timestamp 字段中的最晚 RFC3339 时间供市场 availability
    // 推导；未识别结构返回 None。
    match value {
        Value::Array(values) => values.iter().filter_map(latest_rfc3339_timestamp).max(),
        Value::Object(values) => values
            .iter()
            .filter_map(|(key, value)| {
                let direct = if matches!(key.as_str(), "t" | "timestamp") {
                    value
                        .as_str()
                        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                        .map(|value| value.with_timezone(&Utc))
                } else {
                    None
                };
                direct.or_else(|| latest_rfc3339_timestamp(value))
            })
            .max(),
        _ => None,
    }
}

fn latest_fred_observation_date(value: &Value) -> Option<DateTime<Utc>> {
    // 从 FRED observations 的 date 字段取最晚观测日，并转成 UTC 午夜用于时间基准比较。
    value
        .get("observations")?
        .as_array()?
        .iter()
        .filter_map(|observation| observation.get("date").and_then(Value::as_str))
        .filter_map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
        .filter_map(|date| date.and_hms_opt(0, 0, 0))
        .map(|value| value.and_utc())
        .max()
}

fn binding_byte(binding: &Value, field: &str) -> EvidenceRuntimeResult<usize> {
    // 将 JSON binding 的非负字节偏移转换为 usize，缺失/越界即 citation invalid。
    claim_binding_byte(binding, field).ok_or(EvidenceRuntimeError::InvalidCitation)
}

#[cfg(test)]
mod live_snapshot_cutoff_tests {
    use super::*;

    #[test]
    fn receipt_time_requires_completed_snapshot_and_future_data_stays_blocked() {
        // 回归：retrieved/available 晚于 decision cutoff 时阻断；cutoff 推迟到可用时间后
        // 才能通过，event 再次落到未来仍要失败。
        let started = Utc::now();
        let received = started + Duration::milliseconds(10);
        let frozen = received + Duration::milliseconds(10);
        let mut basis = EvidenceTimeBasis {
            event_time: None,
            released_at: None,
            available_at: received,
            retrieved_at: received,
            decision_clock: DecisionClock {
                decision_cutoff: started,
            },
            vintage: None,
            revision: None,
        };
        assert!(matches!(
            basis.validate(),
            Err(EvidenceRuntimeError::TemporalContamination)
        ));
        basis.decision_clock.decision_cutoff = frozen;
        assert!(basis.validate().is_ok());
        basis.event_time = Some(frozen + Duration::seconds(1));
        assert!(matches!(
            basis.validate(),
            Err(EvidenceRuntimeError::TemporalContamination)
        ));
    }
}
