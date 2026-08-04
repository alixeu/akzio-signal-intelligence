//! Terminal protocol tools for the Rust-owned Phase 2 topic debate tree.
//!
//! These tools do not mutate the tree directly (the tree lives in the
//! workflow crate). They validate a model's intent, end the current agent
//! turn, and return a typed command for the workflow event pump to apply.

use super::{truncate_chars, ToolDefinition};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub const SUBMIT_DEBATE_TURN: &str = "submit_debate_turn";
pub const ROUTE_DEBATE_TURN: &str = "route_debate_turn";
pub const WAIT_FOR_DEBATE_TURN: &str = "wait_for_debate_turn";
pub const CLOSE_DEBATE: &str = "close_debate";

pub fn definition(name: &str) -> Option<ToolDefinition> {
    let (description, parameters) = match name {
        SUBMIT_DEBATE_TURN => (
            "Submit Bull or Bear's current position to the Topic Controller and end this turn.",
            json!({"type":"object","additionalProperties":false,"required":["stance","message","evidence_refs","evidence_links","report"],"properties":{
                "stance":{"type":"string","enum":["challenge","partial_agree","agree","retract","needs_evidence","no_new_info"]},
                "message":{"type":"string","maxLength":1200}, "reply_to_node_id":{"type":"string"},
                "evidence_refs":{"type":"array","maxItems":3,"items":{"type":"string"}},
                "evidence_links":{"type":"array","maxItems":3,"items":{"type":"object","additionalProperties":false,"required":["evidence_ref","relation"],"properties":{
                    "evidence_ref":{"type":"string"},
                    "relation":{"type":"string","enum":["supports","refutes","qualifies"]}
                }}},
                "report":{"type":"string","maxLength":4000}
            }}),
        ),
        ROUTE_DEBATE_TURN => (
            "Route a concrete debate instruction to both Bull and Bear, then end this Controller turn.",
            json!({"type":"object","additionalProperties":false,"required":["targets","reply_to_node_id","message","report"],"properties":{
                "targets":{"type":"array","description":"Always exactly [\"bull\", \"bear\"].","default":["bull","bear"],"minItems":2,"maxItems":2,"uniqueItems":true,"items":{"type":"string","enum":["bull","bear"]}},
                "reply_to_node_id":{"type":"string"}, "message":{"type":"string","maxLength":1200}, "report":{"type":"string","maxLength":4000}
            }}),
        ),
        WAIT_FOR_DEBATE_TURN => (
            "Record that the Controller is waiting for the remaining participant response, then end this turn.",
            json!({"type":"object","additionalProperties":false,"required":["message","report"],"properties":{
                "message":{"type":"string","maxLength":1200}, "report":{"type":"string","maxLength":4000}
            }}),
        ),
        CLOSE_DEBATE => (
            "Close the topic debate. Only the Controller may invoke this terminal action.",
            json!({"type":"object","additionalProperties":false,"required":["reason","message","report"],"properties":{
                "reason":{"type":"string","enum":["consensus","unresolved_disagreement","evidence_exhausted","agent_failure","round_limit"]},
                "message":{"type":"string","maxLength":1200},
                "accepted_claims":{"type":"array","maxItems":2,"items":{"type":"object","additionalProperties":false,"required":["claim_id","evidence_refs"],"properties":{
                    "claim_id":{"type":"string"},
                    "evidence_refs":{"type":"array","minItems":1,"maxItems":3,"items":{"type":"string"}}
                }}},
                "report":{"type":"string","maxLength":4000}
            }}),
        ),
        _ => return None,
    };
    Some(ToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        parameters,
    })
}

pub fn execute(name: &str, args: Value) -> Result<Value> {
    let object = args
        .as_object()
        .context("Phase 2 stree command must be an object")?;
    let report = required_string(object, "report", 4_000)?;
    match name {
        SUBMIT_DEBATE_TURN => {
            required_string(object, "stance", 32)?;
            required_string(object, "message", 1_200)?;
            required_evidence_refs(object)?;
            required_evidence_links(object)?;
        }
        ROUTE_DEBATE_TURN => {
            required_string(object, "reply_to_node_id", 128)?;
            required_string(object, "message", 1_200)?;
            let targets = object
                .get("targets")
                .and_then(Value::as_array)
                .context("route_debate_turn requires targets")?;
            let has_bull = targets.iter().any(|target| target.as_str() == Some("bull"));
            let has_bear = targets.iter().any(|target| target.as_str() == Some("bear"));
            if targets.len() != 2 || !has_bull || !has_bear {
                bail!("route_debate_turn requires exactly one bull and one bear target");
            }
        }
        WAIT_FOR_DEBATE_TURN => {
            required_string(object, "message", 1_200)?;
        }
        CLOSE_DEBATE => {
            required_string(object, "reason", 64)?;
            required_string(object, "message", 1_200)?;
        }
        _ => bail!("unknown Phase 2 stree terminal {name}"),
    }
    Ok(json!({
        "terminal": true,
        "artifact": {
            "response_text": truncate_chars(&report, 4_000),
            "phase2_stree": {"command": name, "payload": args}
        }
    }))
}

fn required_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    max_chars: usize,
) -> Result<String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("Phase 2 stree command requires non-empty {field}"))?;
    if value.chars().count() > max_chars {
        bail!("Phase 2 stree {field} exceeds {max_chars} characters");
    }
    Ok(value)
}

fn required_evidence_refs(object: &serde_json::Map<String, Value>) -> Result<()> {
    let references = object
        .get("evidence_refs")
        .and_then(Value::as_array)
        .context("submit_debate_turn requires evidence_refs")?;
    if references.len() > 3 {
        bail!("submit_debate_turn permits at most three evidence_refs")
    }
    if references.iter().any(|reference| {
        reference
            .as_str()
            .is_none_or(|reference| reference.trim().is_empty())
    }) {
        bail!("submit_debate_turn evidence_refs must contain non-empty strings")
    }
    Ok(())
}

/// Evidence IDs prove only that a source was observed.  Each submitted claim
/// must also declare the narrow role that each source plays: support,
/// refutation, or qualification.  Rust cannot infer semantic truth from text,
/// but this one-to-one edge makes the declared relationship auditable and
/// prevents a participant from attaching a bag of unrelated IDs.
fn required_evidence_links(object: &serde_json::Map<String, Value>) -> Result<()> {
    let references = object
        .get("evidence_refs")
        .and_then(Value::as_array)
        .context("submit_debate_turn requires evidence_refs")?
        .iter()
        .map(|reference| {
            reference
                .as_str()
                .map(str::trim)
                .filter(|reference| !reference.is_empty())
                .map(ToOwned::to_owned)
                .context("submit_debate_turn evidence_refs must contain non-empty strings")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let links = object
        .get("evidence_links")
        .and_then(Value::as_array)
        .context("submit_debate_turn requires evidence_links")?;
    if links.len() > 3 {
        bail!("submit_debate_turn permits at most three evidence_links")
    }
    let mut linked = BTreeSet::new();
    for link in links {
        let link = link
            .as_object()
            .context("submit_debate_turn evidence_links must contain objects")?;
        let reference = required_string(link, "evidence_ref", 128)?;
        let relation = required_string(link, "relation", 32)?;
        if !matches!(relation.as_str(), "supports" | "refutes" | "qualifies") {
            bail!("submit_debate_turn evidence_links.relation is invalid")
        }
        if !linked.insert(reference) {
            bail!("submit_debate_turn evidence_links contains duplicate evidence_ref")
        }
    }
    if linked != references {
        bail!("submit_debate_turn evidence_links must cover each evidence_ref exactly once")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_requires_explicit_required_fields() {
        let missing_report = execute(
            ROUTE_DEBATE_TURN,
            json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": "topic-a:stree:4",
                "message": "Both sides must address the opposing opening."
            }),
        )
        .unwrap_err();
        assert!(missing_report.to_string().contains("non-empty report"));

        let missing_targets = execute(
            ROUTE_DEBATE_TURN,
            json!({
                "reply_to_node_id": "topic-a:stree:4",
                "message": "Both sides must address the opposing opening.",
                "report": "Controller requests a collision wave."
            }),
        )
        .unwrap_err();
        assert!(missing_targets.to_string().contains("requires targets"));
    }

    #[test]
    fn submission_requires_an_explicit_evidence_array() {
        let error = execute(
            SUBMIT_DEBATE_TURN,
            json!({
                "stance": "needs_evidence",
                "message": "The claim needs a verified source.",
                "report": "No evidence is claimed without an explicit empty array."
            }),
        )
        .unwrap_err();

        assert!(error.to_string().contains("requires evidence_refs"));
    }

    #[test]
    fn submission_requires_one_declared_relation_per_evidence_ref() {
        let reference = format!("web-{}", "a".repeat(64));
        let error = execute(
            SUBMIT_DEBATE_TURN,
            json!({
                "stance": "challenge",
                "message": "The source contradicts the current inference.",
                "evidence_refs": [reference],
                "evidence_links": [],
                "report": "The position is evidence-bounded."
            }),
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("cover each evidence_ref exactly once"));
    }

    #[test]
    fn terminal_tool_schemas_expose_the_runtime_text_limits() {
        for name in [
            SUBMIT_DEBATE_TURN,
            ROUTE_DEBATE_TURN,
            WAIT_FOR_DEBATE_TURN,
            CLOSE_DEBATE,
        ] {
            let definition = definition(name).unwrap();
            assert_eq!(
                definition.parameters["properties"]["message"]["maxLength"],
                1_200
            );
            assert_eq!(
                definition.parameters["properties"]["report"]["maxLength"],
                4_000
            );
        }
        assert_eq!(
            definition(SUBMIT_DEBATE_TURN).unwrap().parameters["properties"]["evidence_refs"]
                ["maxItems"],
            3
        );
        assert_eq!(
            definition(SUBMIT_DEBATE_TURN).unwrap().parameters["properties"]["evidence_links"]
                ["maxItems"],
            3
        );
    }
}
