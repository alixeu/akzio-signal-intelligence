//! Verifies candidate facts against the actual Submit input and sealed evidence.

use super::*;
use akzio_domain::{EvidenceFact, ObservedBarClose, ObservedFredValue, ObservedPriceDirection};
use akzio_ingest::{
    market_session_day, parse_money_micros, EvidenceSource, GovernedResource,
    NormalizedEvidencePayload,
};
use chrono::NaiveDate;
use std::cmp::Ordering;

// A bound ground's alternatives can mask the nested unit error with a scope diagnostic.
pub(super) fn unsupported_fact_unit(arguments: &Value) -> Option<ResearchError> {
    arguments["result"]["grounds"]
        .as_array()?
        .iter()
        .enumerate()
        .find_map(|(index, ground)| {
            let fact = &ground["fact"];
            match fact["kind"].as_str() {
                Some("fred_observation" | "fred_observation_comparison") => (fact["unit"]
                    != "series_native")
                    .then(|| invalid_fact(index, "unsupported_fred_unit")),
                _ => (fact.is_object() && fact["unit"] != "usd_micros")
                    .then(|| invalid_fact(index, "unsupported_price_unit")),
            }
        })
}

pub(super) fn verify_facts(
    store: &Store,
    manifest: &ContextManifest,
    request: &AgentModelRequest,
    frozen_result: Option<(&Value, &ArtifactRef)>,
    output: &Value,
) -> ResearchResult<()> {
    let claim: ResearchClaim = serde_json::from_value(output.clone())?;
    let grounds = claim
        .grounds
        .iter()
        .enumerate()
        .filter_map(|(index, ground)| ground.fact.as_ref().map(|fact| (index, ground, fact)));
    let mut grounds = grounds.peekable();
    let Some((first, _, _)) = grounds.peek() else {
        return Ok(());
    };
    let projection = match frozen_result {
        Some((result, turn)) => frozen_fact_projection(store, manifest, result, turn)?,
        None => request_projection(request),
    }
    .ok_or_else(|| invalid_fact(*first, "submit_projection_unknown"))?;

    for (index, ground, fact) in grounds {
        let fail = |reason| invalid_fact(index, reason);
        if ground.evidence.kind != ArtifactKind::NormalizedEvidence
            || !manifest
                .payload
                .selections
                .iter()
                .any(|selection| selection.artifact == ground.evidence)
        {
            return Err(fail("evidence_not_granted"));
        }
        let document = projection
            .iter()
            .find(|document| {
                document["type"] == "must_read"
                    && document["metadata"]["kind"] == "normalized_evidence"
                    && document["metadata"]["document_id"] == json!(ground.evidence.artifact_id)
            })
            .ok_or_else(|| fail("evidence_not_presented"))?;
        let artifact = store.artifact(&ground.evidence.artifact_id)?;
        artifact.validate()?;
        let presented = &document["value"];
        let (asset, bar) = match fact {
            EvidenceFact::AlpacaDailyClose { asset, bar, .. } => (*asset, bar),
            EvidenceFact::AlpacaDailyCloseComparison { asset, earlier, .. } => (*asset, earlier),
            EvidenceFact::FredObservation {
                series_id,
                vintage,
                observation,
                ..
            } => {
                let evidence = verify_fred_evidence(
                    store, manifest, &artifact, presented, series_id, *vintage, fail,
                )?;
                verify_fred_row(presented, &evidence, observation).map_err(fail)?;
                continue;
            }
            EvidenceFact::FredObservationComparison {
                series_id,
                vintage,
                earlier,
                later,
                direction,
                ..
            } => {
                let evidence = verify_fred_evidence(
                    store, manifest, &artifact, presented, series_id, *vintage, fail,
                )?;
                let earlier_value = verify_fred_row(presented, &evidence, earlier).map_err(fail)?;
                let later_value = verify_fred_row(presented, &evidence, later).map_err(fail)?;
                if earlier.date >= later.date {
                    return Err(fail("invalid_chronology"));
                }
                if observed_direction(later_value.cmp(&earlier_value)) != *direction {
                    return Err(fail("direction_mismatch"));
                }
                continue;
            }
        };
        if !ground.assets.contains(&asset) {
            return Err(fail("asset_mismatch"));
        }
        if artifact.kind != ground.evidence.kind
            || artifact.producer != "akzio.ingest.alpaca.normalized"
            || artifact.provenance.source_family != EvidenceSource::Alpaca.as_str()
        {
            return Err(fail("unsupported_price_source"));
        }
        let evidence: NormalizedEvidencePayload =
            serde_json::from_slice(&store.read_blob(&artifact.blob)?)
                .map_err(|_| fail("normalized_price_payload_unknown"))?;
        let resource = GovernedResource::parse(evidence.source, &evidence.resource);
        if evidence.source != EvidenceSource::Alpaca
            || !matches!(resource, Ok(GovernedResource::AlpacaBars { asset: expected, raw_prices: false, .. }) if expected == asset)
            || evidence.value["symbol"] != asset.symbol()
            || evidence.value["price_basis"] != "adjusted_research"
            || !evidence.quality.normalized
        {
            return Err(fail("unsupported_adjusted_daily_bar"));
        }
        if presented["source"] != "alpaca" || presented["resource"] != evidence.resource {
            return Err(fail("presented_resource_mismatch"));
        }
        let observed = evidence.time_basis.retrieved_at;
        let clock = &evidence.time_basis.decision_clock;
        if observed != evidence.observed_at || artifact.provenance.observed_at != Some(observed) {
            return Err(fail("capture_time_unknown"));
        }
        if !clock.contains(observed) {
            return Err(fail("capture_after_cutoff"));
        }
        if timestamp(&evidence.value["decision_cutoff"]) != Some(clock.decision_cutoff)
            || evidence.time_basis.available_at > observed
        {
            return Err(fail("frozen_availability_unknown"));
        }
        let first = verify_close(presented, &evidence, bar).map_err(fail)?;
        if let EvidenceFact::AlpacaDailyCloseComparison {
            later, direction, ..
        } = fact
        {
            let second = verify_close(presented, &evidence, later).map_err(fail)?;
            if market_session_day(first) >= market_session_day(second) {
                return Err(fail("invalid_chronology"));
            }
            if observed_direction(later.value_micros.cmp(&bar.value_micros)) != *direction {
                return Err(fail("direction_mismatch"));
            }
        }
    }
    Ok(())
}

fn verify_fred_evidence(
    store: &Store,
    manifest: &ContextManifest,
    artifact: &Artifact,
    presented: &Value,
    series_id: &str,
    vintage: NaiveDate,
    fail: impl Fn(&'static str) -> ResearchError,
) -> ResearchResult<NormalizedEvidencePayload> {
    let source_run = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref());
    let context_run = manifest
        .artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref());
    if artifact.kind != ArtifactKind::NormalizedEvidence
        || artifact.producer != "akzio.ingest.fred.normalized"
        || artifact.provenance.source_family != "fred"
        || source_run.is_none()
        || source_run != context_run
    {
        return Err(fail("unsupported_fred_source"));
    }
    let evidence: NormalizedEvidencePayload =
        serde_json::from_slice(&store.read_blob(&artifact.blob)?)
            .map_err(|_| fail("normalized_fred_payload_unknown"))?;
    let resource = GovernedResource::parse(evidence.source, &evidence.resource);
    if evidence.source != EvidenceSource::Fred
        || !evidence.quality.normalized
        || !matches!(resource, Ok(GovernedResource::Fred { series_id: exact, vintage: Some(point), .. }) if exact == series_id && point == vintage)
        || evidence.time_basis.vintage != Some(vintage)
    {
        return Err(fail("fred_series_or_vintage_mismatch"));
    }
    if presented["source"] != "fred" || presented["resource"] != evidence.resource {
        return Err(fail("presented_resource_mismatch"));
    }
    if artifact.provenance.observed_at != Some(evidence.observed_at)
        || artifact.provenance.source_uri.as_deref()
            != Some(evidence.provenance.source_uri.as_str())
    {
        return Err(fail("capture_time_unknown"));
    }
    evidence
        .availability_at(evidence.time_basis.decision_clock.decision_cutoff)
        .map_err(|reason| fail(reason.as_str()))?;

    // The upstream attestation is derived by the direct adapter from these
    // exact raw bytes. The verifier needs only their sealed identity.
    if evidence.raw.kind != ArtifactKind::RawEvidence
        || !artifact.source_refs.contains(&evidence.raw)
    {
        return Err(fail("fred_raw_snapshot_unbound"));
    }
    let raw = store.artifact(&evidence.raw.artifact_id)?;
    raw.validate()?;
    if raw.kind != ArtifactKind::RawEvidence
        || raw.producer != "akzio.ingest.fred.raw"
        || raw.origin != artifact.origin
        || raw.provenance.observed_at != artifact.provenance.observed_at
        || raw.provenance.source_uri != artifact.provenance.source_uri
        || !evidence.proves_fred_snapshot(&raw.blob.hash)
    {
        return Err(fail("fred_raw_snapshot_unverified"));
    }
    // FRED's linear response proves the original scale of the requested series.
    if evidence.value["units"] != "lin" {
        return Err(fail("fred_native_units_unverified"));
    }
    Ok(evidence)
}

fn verify_fred_row(
    presented: &Value,
    evidence: &NormalizedEvidencePayload,
    assertion: &ObservedFredValue,
) -> Result<i128, &'static str> {
    let date = assertion.date.to_string();
    let shown = unique_row(&presented["value_summary"]["observations"], "date", &date)
        .ok_or("fred_observation_not_presented")?;
    let sealed = unique_row(&evidence.value["observations"], "date", &date)
        .ok_or("fred_observation_not_sealed")?;
    let actual = sealed["value"]
        .as_str()
        .and_then(fred_decimal_coefficient)
        .ok_or("fred_decimal_out_of_range")?;
    if shown != sealed || fred_decimal_coefficient(&assertion.value) != Some(actual) {
        return Err("fred_value_mismatch");
    }
    Ok(actual)
}

fn fred_decimal_coefficient(value: &str) -> Option<i128> {
    const SCALE: u32 = 12;
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((_, "")) => return None,
        Some(parts) => parts,
        None => (unsigned, ""),
    };
    if integer.is_empty()
        || integer.len() > 18
        || fraction.len() > SCALE as usize
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|digit| digit.is_ascii_digit())
    {
        return None;
    }
    let whole = integer
        .parse::<i128>()
        .ok()?
        .checked_mul(10_i128.pow(SCALE))?;
    let fraction_coefficient = if fraction.is_empty() {
        0
    } else {
        let exponent = SCALE.checked_sub(u32::try_from(fraction.len()).ok()?)?;
        fraction
            .parse::<i128>()
            .ok()?
            .checked_mul(10_i128.pow(exponent))?
    };
    let coefficient = whole.checked_add(fraction_coefficient)?;
    if value.starts_with('-') {
        coefficient.checked_neg()
    } else {
        Some(coefficient)
    }
}

fn observed_direction(ordering: Ordering) -> ObservedPriceDirection {
    match ordering {
        Ordering::Less => ObservedPriceDirection::Decrease,
        Ordering::Equal => ObservedPriceDirection::Unchanged,
        Ordering::Greater => ObservedPriceDirection::Increase,
    }
}

fn verify_close(
    presented: &Value,
    evidence: &NormalizedEvidencePayload,
    assertion: &ObservedBarClose,
) -> std::result::Result<DateTime<Utc>, &'static str> {
    let Some(observed) = unique_row(
        &presented["value_summary"]["bars"],
        "t",
        &assertion.bar_timestamp,
    ) else {
        return Err("bar_not_presented");
    };
    let Some(sealed) = unique_row(&evidence.value["bars"], "t", &assertion.bar_timestamp) else {
        return Err("bar_not_sealed");
    };
    let bar_time = timestamp(&sealed["t"]).ok_or("bar_timestamp_invalid")?;
    let day = market_session_day(bar_time);
    let close = timestamp(&evidence.value["session_closes"][day.to_string()])
        .filter(|close| *close > bar_time && market_session_day(*close) == day)
        .ok_or("session_close_unknown")?;
    let lag = evidence.value["availability_lag_minutes"]
        .as_i64()
        .filter(|lag| *lag > 0)
        .and_then(Duration::try_minutes)
        .ok_or("publication_lag_unknown")?;
    let availability = close
        .checked_add_signed(lag)
        .ok_or("publication_lag_unknown")?;
    if availability > evidence.time_basis.available_at
        || !evidence.time_basis.decision_clock.contains(availability)
    {
        return Err("bar_after_cutoff");
    }
    let price = parse_money_micros(&sealed["c"])
        .filter(|price| price.0 > 0)
        .ok_or("unrepresentable_usd_micros")?;
    if observed["c"] != sealed["c"] || price.0 != assertion.value_micros {
        return Err("value_mismatch");
    }
    Ok(bar_time)
}

fn unique_row<'a>(rows: &'a Value, selector: &str, key: &str) -> Option<&'a Value> {
    let mut rows = rows
        .as_array()?
        .iter()
        .filter(|row| row[selector].as_str() == Some(key));
    let row = rows.next()?;
    rows.next().is_none().then_some(row)
}

fn timestamp(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.as_str()?)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

fn request_projection(request: &AgentModelRequest) -> Option<Vec<Value>> {
    let Some(continuation) = &request.continuation else {
        return Some(request.context.clone());
    };
    // Responses Continue resends the original Rust-owned input as the first user item.
    let original = continuation.items().first()?;
    if original["role"] != "user" {
        return None;
    }
    let input: Value = serde_json::from_str(original["content"].as_str()?).ok()?;
    if input["context_manifest"] != json!(request.manifest_artifact_id) {
        return None;
    }
    input["context"].as_array().cloned()
}

fn frozen_fact_projection(
    store: &Store,
    manifest: &ContextManifest,
    result: &Value,
    turn: &ArtifactRef,
) -> ResearchResult<Option<Vec<Value>>> {
    let reference = ArtifactRef {
        artifact_id: manifest.artifact.artifact_id.clone(),
        kind: ArtifactKind::ContextManifest,
    };
    // The repair path carries the exact Turn that supplied the frozen result.
    if turn.kind != ArtifactKind::AgentTurn {
        return Ok(None);
    }
    let artifact = store.artifact(&turn.artifact_id)?;
    if artifact.kind != ArtifactKind::AgentTurn || !artifact.source_refs.contains(&reference) {
        return Ok(None);
    }
    artifact.validate()?;
    let record: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
    if record.pointer("/response/terminal_submission/arguments/result") != Some(result) {
        return Ok(None);
    }
    let request: AgentModelRequest = serde_json::from_value(record["request"].clone())?;
    if request.manifest_artifact_id != reference.artifact_id
        || request.contract_hash != manifest.payload.contract_hash
    {
        return Ok(None);
    }
    Ok(request_projection(&request))
}

fn invalid_fact(index: usize, reason: &str) -> ResearchError {
    ResearchError::InvalidOutput(format!(
        "result.grounds[{index}].fact: {reason}. Use the exact presented adjusted daily bar in USD micros or verified FRED point snapshot in series_native. Use null when unverified"
    ))
}
