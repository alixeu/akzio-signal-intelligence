// 文件导读：标准化 materialization 负责把 acquisition 的 raw bytes、时间基准、污染证书、
// quant features、financial-content 评估和 provider provenance 合成 NormalizedEvidence。
// 新闻 snapshot 的 byte binding 会切出 Raw 子 blob；日线按 cutoff 计算特征。基础
// acquisition/时间验证先于 Raw stage，新闻 binding 与 quant/financial 评估则在
// Raw Artifact 构造之后；这些中间 stage 不等于已完成任务提交事务。

impl EvidenceRuntime {
    fn materialize_raw(
        &self,
        permit: &TaskWritePermit,
        request: &EvidenceRequest,
        acquired: &AcquiredEvidence,
        clock: &EvidenceMaterializationClock,
    ) -> EvidenceRuntimeResult<Artifact> {
        // acquired.raw 是采集适配器保留的原始响应账本；ModelReviewed 新闻可同时
        // 包含 discovery 与 reviewer 的 provider 响应，它仍不是独立来源快照。
        // 本层只封存这些 bytes，不把 normalized projection 回写为原始来源。
        Ok(Artifact::new(
            ArtifactKind::RawEvidence,
            self.store
                .stage_bytes(&acquired.raw, &acquired.media_type)?,
            format!("akzio.ingest.{}.raw", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: clock.recorded_receipt_at,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![],
            clock.sealed_at,
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
        clock: EvidenceMaterializationClock,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        // 先计算 EvidenceTimeBasis 和 contamination certificate，再封 Raw、绑定新闻子 blob、
        // 按资源类型构造 quant/financial projection，最后创建 NormalizedEvidence 及其 lineage。
        Self::validate_acquisition(&acquired, request, clock.sealed_at)?;
        let time_basis = Self::time_basis(request, &acquired, clock.decision_cutoff)?;
        let contamination_certificate =
            EvidenceContaminationCertificate::for_time_basis(&time_basis)?;
        let mut normalized_value = acquired.normalized.clone();
        let raw = self.materialize_raw(permit, request, &acquired, &clock)?;
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
        let governed = GovernedResource::parse(request.source, &request.resource)?;
        let availability_proof = crate::EvidenceAvailabilityProof::seal(request, &governed, &acquired);
        let quant_features = if matches!(
            governed,
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
                clock.decision_cutoff,
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
            availability_proof: Some(availability_proof),
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
                retrieved_at: clock.recorded_receipt_at,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: acquired.quality.completeness_ppm,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![raw_ref, need.clone()],
            clock.sealed_at,
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
            // 无 published_at 时以 adapter 的 observed_at 作保守可用时间下界；
            // ModelReviewed 新闻的搜索/审核并非独立来源验证，不能称“verified retrieval”。
            // 历史 cutoff 早于这一时刻时，由后续 time_basis 校验阻断回填。
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
    use akzio_domain::{ArtifactId, AttemptId, LeaseId, RunId, TaskId};
    use serde_json::json;

    use crate::{EvidenceAvailabilityBasis as Basis, EvidenceAvailabilityUnknown as Unknown};

    fn with_test_store(test: impl FnOnce(&EvidenceRuntime, &TaskWritePermit)) {
        let permit = TaskWritePermit {
            run_id: RunId::new(),
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            lease_id: LeaseId::new(),
            epoch: 1,
            contract_hash: None,
        };
        let path = std::env::temp_dir().join(format!("akzio-availability-{}", permit.run_id));
        let runtime = EvidenceRuntime::new(Store::open(&path).unwrap(), []);
        test(&runtime, &permit);
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn request(source: EvidenceSource, resource: &str) -> EvidenceRequest {
        EvidenceRequest {
            source,
            resource: resource.to_owned(),
            max_age: Duration::days(7),
            acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
        }
    }

    fn fixture_capture(
        request: &EvidenceRequest,
        source_uri: &str,
        observed_at: DateTime<Utc>,
        normalized: Value,
    ) -> AcquiredEvidence {
        let raw = serde_json::to_vec(&normalized).unwrap();
        AcquiredEvidence {
            provenance: EvidenceProvenance {
                document_id: Some(request.resource.clone()),
                published_at: None,
                observed_at,
                revision: None,
                source_uri: source_uri.to_owned(),
                dedupe_key: format!(
                    "{}:{}",
                    request.source.as_str(),
                    ContentHash::of_bytes(&raw)
                ),
                citations: vec![],
            },
            raw,
            media_type: "application/json".to_owned(),
            source_uri: source_uri.to_owned(),
            observed_at,
            normalized,
            quality: EvidenceQuality::default(),
        }
    }

    fn materialize_for_cutoff(
        runtime: &EvidenceRuntime,
        permit: &TaskWritePermit,
        request: &EvidenceRequest,
        acquired: AcquiredEvidence,
        cutoff: DateTime<Utc>,
    ) -> NormalizedEvidencePayload {
        let original_raw = acquired.raw.clone();
        let clock = EvidenceMaterializationClock {
            decision_cutoff: cutoff,
            sealed_at: cutoff.max(acquired.observed_at),
            recorded_receipt_at: acquired.observed_at,
        };
        let need = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(request.resource.as_bytes())),
            kind: ArtifactKind::EvidenceNeed,
        };
        let bundle = runtime
            .materialize_acquired(permit, &need, request, acquired, clock)
            .unwrap();
        assert_eq!(
            runtime.store().read_blob(&bundle.raw.blob).unwrap(),
            original_raw
        );
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&runtime.store().read_blob(&bundle.normalized.blob).unwrap())
                .unwrap();
        assert_eq!(payload.raw.artifact_id, bundle.raw.artifact_id);
        payload
    }

    /// WHY THIS TEST EXISTS (Task #13): source dates cannot qualify a later recomputed or revised version.
    /// Do not weaken or delete this assertion.
    #[test]
    fn availability_proof_distinguishes_receipt_from_bars_window_and_release_date() {
        with_test_store(|runtime, permit| {
            let cutoff = "2026-09-15T20:25:00Z".parse().unwrap();
            let captured_at = cutoff + Duration::minutes(2);
            let bars = request(EvidenceSource::Alpaca, "bars:QQQ:1d");
            let uri = "https://data.alpaca.markets/v2/stocks/QQQ/bars?feed=sip&adjustment=all&limit=1";
            let closes = json!({"2026-09-15":"2026-09-15T20:00:00Z"});
            let calendar_uri = "https://paper-api.alpaca.markets/v2/calendar";
            let capture = fixture_capture(
                &bars,
                uri,
                captured_at,
                json!({
                    "market":"stocks", "decision_cutoff":cutoff,
                    "bars":[{"t":"2026-09-15T04:00:00Z","o":10,"l":9,"h":12,"c":11,"v":5}],
                    "session_closes":closes, "content_available_at":"2026-09-15T20:20:00Z",
                    "feed":"sip", "price_basis":"adjusted_research", "request_url":uri,
                    "calendar_source":calendar_uri,
                    "bar_session_coverage":{
                        "version":1, "expected_closes":closes, "observed_dates":["2026-09-15"],
                        "missing_dates":[], "outside_window_dates":[], "feed":"sip",
                        "adjustment":"all", "provider_revision":null, "calendar_source":calendar_uri,
                        "cutoff":cutoff, "requested_limit":1, "provider_has_more":false
                    }
                }),
            );
            let t0 = captured_at + Duration::seconds(1);
            // Keep the coverage frozen at the request cutoff and qualify the sealed capture at T0.
            let timely = materialize_for_cutoff(runtime, permit, &bars, capture.clone(), cutoff);
            assert_eq!(
                timely.availability_at(captured_at),
                Ok(Basis::Captured { captured_at })
            );
            assert_eq!(
                timely.availability_at(t0),
                Ok(Basis::Captured { captured_at })
            );
            let proof = serde_json::to_value(&timely).unwrap();
            assert_eq!(
                proof["quant_features"]["formula_version"],
                crate::QUANT_FEATURE_FORMULA_VERSION
            );
            assert_eq!(
                proof["availability_proof"]["bars"],
                json!({
                    "requested_cutoff":cutoff,"requested_feed":"sip","requested_adjustment":"all"
                })
            );
            assert!(timely.time_basis.revision.is_none());
            let mut revised = capture;
            revised.normalized["bars"][0]["c"] = json!(12);
            revised.raw = serde_json::to_vec(&revised.normalized).unwrap();
            let late = materialize_for_cutoff(runtime, permit, &bars, revised, cutoff);
            assert_eq!(
                late.availability_at(cutoff),
                Err(Unknown::LateCaptureWithoutVersion)
            );
            assert_eq!(
                late.availability_at("2026-09-15T20:19:59Z".parse().unwrap()),
                Err(Unknown::SourceTimeOutsideCutoff)
            );

            for (source, resource) in [
                (EvidenceSource::NewsWeb, "query:qqq"),
                (EvidenceSource::SecEdgar, "submissions:0001067839"),
            ] {
                let disclosure = request(source, resource);
                let mut revised = fixture_capture(
                    &disclosure,
                    "https://example.com/revised",
                    captured_at,
                    json!({"document":"later revision"}),
                );
                revised.provenance.published_at = Some(cutoff - Duration::days(1));
                revised.provenance.revision = Some("revision-2".to_owned());
                let late = materialize_for_cutoff(runtime, permit, &disclosure, revised, cutoff);
                assert_eq!(
                    late.availability_at(cutoff),
                    Err(Unknown::LateCaptureWithoutVersion)
                );
                assert_eq!(
                    late.availability_at(t0),
                    Ok(Basis::Captured { captured_at })
                );
            }
            let mut unsupported = proof.clone();
            unsupported["availability_proof"]["version"] = json!(2);
            let unsupported: NormalizedEvidencePayload =
                serde_json::from_value(unsupported).unwrap();
            assert_eq!(
                unsupported.availability_at(t0),
                Err(Unknown::UnsupportedProofVersion)
            );

            let mut legacy = proof;
            legacy.as_object_mut().unwrap().remove("availability_proof");
            let legacy: NormalizedEvidencePayload = serde_json::from_value(legacy).unwrap();
            assert_eq!(legacy.value, timely.value);
            assert_eq!(legacy.availability_at(t0), Err(Unknown::MissingProof));
        });
    }

    /// WHY THIS TEST EXISTS (Task #13): only a validated, complete direct FRED point vintage can backfill.
    /// Do not weaken or delete this assertion.
    #[test]
    fn availability_proof_revalidates_original_fred_rows_and_request_before_sealing() {
        with_test_store(|runtime, permit| {
            let request = request(
                EvidenceSource::Fred,
                "series:DFII10:2026-09-01:2026-09-15:2026-09-15",
            );
            let uri = "https://api.stlouisfed.org/fred/series/observations?series_id=DFII10&file_type=json&observation_start=2026-09-01&observation_end=2026-09-15&realtime_start=2026-09-15&realtime_end=2026-09-15";
            let cutoff = "2026-09-16T20:00:00Z".parse().unwrap();
            let captured_at = cutoff + Duration::minutes(30);
            let response = json!({
                "realtime_start":"2026-09-15","realtime_end":"2026-09-15","count":2,"offset":0,"limit":1000,
                "observations":[
                    {"date":"2026-09-11","value":"1.25","realtime_start":"2026-09-15","realtime_end":"2026-09-15"},
                    {"date":"2026-09-14","value":".","realtime_start":"2026-09-15","realtime_end":"2026-09-15"}
                ]
            });
            let acquire = |uri, body| {
                let mut capture = fixture_capture(&request, uri, captured_at, body);
                (capture.normalized, capture.quality) =
                    crate::FredDirectTransport::normalize_response(&request.resource, &capture.raw)
                        .unwrap();
                capture
            };
            let exact = acquire(uri, response.clone());
            let qualified =
                materialize_for_cutoff(runtime, permit, &request, exact.clone(), cutoff);
            let vintage = "2026-09-15".parse().unwrap();
            let basis = Ok(Basis::FredPointSnapshot { vintage });
            assert_eq!(qualified.availability_at(cutoff), basis);
            assert_eq!(
                qualified.availability_at("2026-09-15T14:00:00Z".parse().unwrap()),
                Err(Unknown::SourceTimeOutsideCutoff)
            );
            let restored: NormalizedEvidencePayload =
                serde_json::from_slice(&serde_json::to_vec(&qualified).unwrap()).unwrap();
            assert_eq!(restored.availability_at(cutoff), basis);

            let wrong_series_uri = uri.replace("series_id=DFII10", "series_id=DFF");
            let mut substituted = exact;
            substituted.normalized["observations"][0]["value"] = json!("4.25");
            let mut truncated = response.clone();
            truncated["count"] = json!(3);
            let mut current_row = response.clone();
            current_row["observations"][1] = json!({"date":"2026-09-14","value":"2.25","realtime_start":"2026-09-16","realtime_end":"2026-09-16"});
            for unproved in [
                acquire(&wrong_series_uri, response),
                substituted,
                acquire(uri, truncated),
                acquire(uri, current_row),
            ] {
                let payload = materialize_for_cutoff(runtime, permit, &request, unproved, cutoff);
                assert_eq!(
                    payload.availability_at(cutoff),
                    Err(Unknown::FredPointSnapshotUnverified)
                );
            }
        });
    }

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
