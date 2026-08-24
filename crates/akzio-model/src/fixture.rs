//! Deterministic fixture response materialization.

use super::*;

pub(super) fn materialize_fixture(mut raw: Value, request: &ModelRequest) -> Value {
    let input = fixture_input(request).unwrap_or_default();
    let evidence_id = fixture_context_artifact_id(&input, "normalized_evidence")
        .or_else(|| fixture_context_artifact_id(&input, "semantic_detail"));
    let claim_id = fixture_context_artifact_id(&input, "claim");
    if evidence_id.is_none() && claim_id.is_none() {
        return raw;
    }
    if let Some(Value::String(output_text)) = raw.get_mut("output_text") {
        if let Ok(mut output) = serde_json::from_str(output_text) {
            materialize_fixture_value(&mut output, evidence_id.as_deref(), claim_id.as_deref());
            if let Ok(text) = serde_json::to_string(&output) {
                *output_text = text;
            }
        }
    }
    if let Some(items) = raw.get_mut("output").and_then(Value::as_array_mut) {
        for item in items {
            let Some(Value::String(arguments)) = item.get_mut("arguments") else {
                continue;
            };
            if let Ok(mut value) = serde_json::from_str(arguments) {
                materialize_fixture_value(&mut value, evidence_id.as_deref(), claim_id.as_deref());
                if let Ok(text) = serde_json::to_string(&value) {
                    *arguments = text;
                }
            }
        }
    }
    materialize_fixture_value(&mut raw, evidence_id.as_deref(), claim_id.as_deref());
    raw
}

pub(super) fn fixture_input(request: &ModelRequest) -> Option<String> {
    match &request.input {
        ModelInput::Fresh { text } => Some(text.clone()),
        ModelInput::Continue { continuation, .. } => continuation.fixture_input.clone(),
    }
}

pub(super) fn fixture_context_artifact_id(input: &str, kind: &str) -> Option<String> {
    serde_json::from_str::<Value>(input)
        .ok()?
        .get("context")?
        .as_array()?
        .iter()
        .find(|artifact| artifact.get("kind").and_then(Value::as_str) == Some(kind))?
        .get("artifact_id")?
        .as_str()
        .map(ToOwned::to_owned)
}

pub(super) fn materialize_fixture_value(
    value: &mut Value,
    evidence_id: Option<&str>,
    claim_id: Option<&str>,
) {
    match value {
        Value::String(text) if text == FIXTURE_CONTEXT_EVIDENCE_ID => {
            if let Some(evidence_id) = evidence_id {
                *text = evidence_id.to_owned();
            }
        }
        Value::String(text) if text == FIXTURE_CONTEXT_CLAIM_ID => {
            if let Some(claim_id) = claim_id {
                *text = claim_id.to_owned();
            }
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| materialize_fixture_value(value, evidence_id, claim_id)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| materialize_fixture_value(value, evidence_id, claim_id)),
        _ => {}
    }
}
