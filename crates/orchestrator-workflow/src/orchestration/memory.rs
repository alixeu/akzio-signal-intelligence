use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate};
use orchestrator_domain::Phase;
use serde_json::Value;

pub(crate) use orchestrator_core::role_registry::MEMORY_REFLECTOR_ROLE;

pub(crate) fn record_memory_reflector_status(state: &mut Value, status: &str, message: &str) {
    state["memory_reflector"] = serde_json::json!({
        "status": status,
        "message": message,
        "role": MEMORY_REFLECTOR_ROLE,
        "phase": Phase::Phase3MemoryReflector.as_i64()
    });
}

pub(crate) fn validate_memory_update_proposal(artifact: &Value, tickers: &[String]) -> Result<()> {
    if !artifact.is_object() {
        bail!("MemoryUpdateProposal must be a JSON object");
    }
    let artifact_type = required_non_empty_str(artifact, "artifact_type")?;
    if artifact_type != "MemoryUpdateProposal" {
        bail!("artifact_type must be MemoryUpdateProposal");
    }
    let schema_version = artifact
        .get("schema_version")
        .and_then(Value::as_i64)
        .context("schema_version must be integer 1")?;
    if schema_version != 1 {
        bail!("schema_version must be 1");
    }
    let source_role = required_non_empty_str(artifact, "source_role")?;
    if source_role != "manager.research" {
        bail!("source_role must be manager.research");
    }
    required_non_empty_str(artifact, "run_id")?;
    validate_rfc3339_field(artifact, "generated_at")?;
    let proposals = artifact
        .get("proposals")
        .and_then(Value::as_array)
        .context("proposals must be an array")?;
    if proposals.is_empty() {
        required_non_empty_str(artifact, "no_update_reason")?;
        return Ok(());
    }
    for (index, proposal) in proposals.iter().enumerate() {
        validate_memory_update_item(proposal, index, tickers)?;
    }
    Ok(())
}

pub(crate) fn validate_memory_update_item(
    proposal: &Value,
    index: usize,
    tickers: &[String],
) -> Result<()> {
    if !proposal.is_object() {
        bail!("proposals[{index}] must be an object");
    }
    validate_enum_field(
        proposal,
        "update_type",
        &["thesis", "observation", "risk", "follow_up"],
    )
    .with_context(|| format!("proposals[{index}].update_type"))?;
    let ticker = required_non_empty_str(proposal, "ticker")?;
    if !tickers.is_empty() && !tickers.iter().any(|item| item == ticker) {
        bail!("proposals[{index}].ticker {ticker:?} is not in run tickers");
    }
    validate_enum_field(
        proposal,
        "scope",
        &["ticker", "sector", "macro", "market", "portfolio"],
    )
    .with_context(|| format!("proposals[{index}].scope"))?;
    validate_rfc3339_field(proposal, "observed_at")?;
    validate_date_field(proposal, "source_date")?;
    validate_nullable_rfc3339_field(proposal, "expires_at")?;
    validate_confidence(proposal, index)?;
    required_non_empty_str(proposal, "summary")?;
    validate_evidence_refs(proposal, index)?;
    validate_non_empty_string_array(proposal, "invalidation_conditions", index)?;
    validate_non_empty_string_array(proposal, "follow_up_checks", index)?;
    if proposal
        .get("update_type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "thesis")
    {
        validate_thesis_update(proposal, index)?;
    }
    Ok(())
}

pub(crate) fn validate_confidence(proposal: &Value, index: usize) -> Result<()> {
    let confidence = proposal
        .get("confidence")
        .and_then(Value::as_f64)
        .with_context(|| format!("proposals[{index}].confidence must be a number"))?;
    if !(0.0..=1.0).contains(&confidence) {
        bail!("proposals[{index}].confidence must be between 0 and 1");
    }
    Ok(())
}

pub(crate) fn validate_evidence_refs(proposal: &Value, index: usize) -> Result<()> {
    let refs = proposal
        .get("evidence_refs")
        .and_then(Value::as_array)
        .with_context(|| format!("proposals[{index}].evidence_refs must be an array"))?;
    if refs.is_empty() {
        bail!("proposals[{index}].evidence_refs must not be empty");
    }
    for (ref_index, item) in refs.iter().enumerate() {
        validate_enum_field(
            item,
            "source_type",
            &[
                "final_research",
                "debate_brief",
                "evidence_brief",
                "source_item",
                "prior_memory",
            ],
        )
        .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
        required_non_empty_str(item, "source_id")
            .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
        required_non_empty_str(item, "quote_or_fact")
            .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
    }
    Ok(())
}

pub(crate) fn validate_thesis_update(proposal: &Value, index: usize) -> Result<()> {
    let thesis = proposal
        .get("thesis")
        .with_context(|| format!("proposals[{index}].thesis is required for thesis updates"))?;
    let status = required_non_empty_str(thesis, "status")
        .with_context(|| format!("proposals[{index}].thesis.status"))?;
    match status {
        "new" => Ok(()),
        "update" => {
            required_non_empty_str(thesis, "prior_thesis_id")
                .with_context(|| format!("proposals[{index}].thesis.prior_thesis_id"))?;
            Ok(())
        }
        other => bail!("proposals[{index}].thesis.status {other:?} must be new or update"),
    }
}

pub(crate) fn required_non_empty_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .with_context(|| format!("{field} must be a non-empty string"))
}

pub(crate) fn validate_enum_field<'a>(
    value: &'a Value,
    field: &str,
    allowed: &[&str],
) -> Result<&'a str> {
    let text = required_non_empty_str(value, field)?;
    if allowed.contains(&text) {
        Ok(text)
    } else {
        bail!("{field} must be one of {}", allowed.join(", "))
    }
}

pub(crate) fn validate_rfc3339_field(value: &Value, field: &str) -> Result<()> {
    let text = required_non_empty_str(value, field)?;
    DateTime::parse_from_rfc3339(text).with_context(|| format!("{field} must be RFC3339"))?;
    Ok(())
}

pub(crate) fn validate_nullable_rfc3339_field(value: &Value, field: &str) -> Result<()> {
    if matches!(value.get(field), Some(Value::Null)) {
        return Ok(());
    }
    validate_rfc3339_field(value, field)
}

pub(crate) fn validate_date_field(value: &Value, field: &str) -> Result<()> {
    let text = required_non_empty_str(value, field)?;
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .with_context(|| format!("{field} must use YYYY-MM-DD"))?;
    Ok(())
}

pub(crate) fn validate_non_empty_string_array(
    value: &Value,
    field: &str,
    index: usize,
) -> Result<()> {
    let items = value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("proposals[{index}].{field} must be an array"))?;
    if items.is_empty() {
        bail!("proposals[{index}].{field} must not be empty");
    }
    for (item_index, item) in items.iter().enumerate() {
        if item
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .is_none()
        {
            bail!("proposals[{index}].{field}[{item_index}] must be a non-empty string");
        }
    }
    Ok(())
}
