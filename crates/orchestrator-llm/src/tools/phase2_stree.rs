//! Terminal protocol tools for the Rust-owned Phase 2 topic debate tree.
//!
//! These tools do not mutate the tree directly (the tree lives in the
//! workflow crate). They validate a model's intent, end the current agent
//! turn, and return a typed command for the workflow event pump to apply.

use super::{truncate_chars, ToolDefinition};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

pub const SUBMIT_DEBATE_TURN: &str = "submit_debate_turn";
pub const ROUTE_DEBATE_TURN: &str = "route_debate_turn";
pub const WAIT_FOR_DEBATE_TURN: &str = "wait_for_debate_turn";
pub const CLOSE_DEBATE: &str = "close_debate";

pub fn definition(name: &str) -> Option<ToolDefinition> {
    let (description, parameters) = match name {
        SUBMIT_DEBATE_TURN => (
            "Submit Bull or Bear's current position to the Topic Controller and end this turn.",
            json!({"type":"object","additionalProperties":false,"required":["stance","message","report"],"properties":{
                "stance":{"type":"string","enum":["challenge","partial_agree","agree","retract","needs_evidence","no_new_info"]},
                "message":{"type":"string","maxLength":1200}, "reply_to_node_id":{"type":"string"},
                "evidence_refs":{"type":"array","items":{"type":"string"}},
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
                "message":{"type":"string","maxLength":1200}, "report":{"type":"string","maxLength":4000}
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

pub fn execute(name: &str, mut args: Value) -> Result<Value> {
    let object = args
        .as_object_mut()
        .context("Phase 2 stree command must be an object")?;
    if matches!(
        name,
        SUBMIT_DEBATE_TURN | ROUTE_DEBATE_TURN | WAIT_FOR_DEBATE_TURN | CLOSE_DEBATE
    ) {
        // `report` is a compact audit copy of the command, so an omitted
        // value can be recovered losslessly from the same command's message.
        // Keep the agent turn terminal instead of forcing an avoidable retry.
        let fallback_report = object
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .unwrap_or("Phase 2 stree terminal command.")
            .to_owned();
        object
            .entry("report".to_owned())
            .or_insert(Value::String(fallback_report));
    }
    if name == ROUTE_DEBATE_TURN {
        // The tree only has one valid routing fan-out. Some model providers
        // occasionally omit a schema-required field, so canonicalize this
        // unique default rather than turning a recoverable omission into a
        // failed debate turn.
        object
            .entry("targets".to_owned())
            .or_insert_with(|| json!(["bull", "bear"]));
    }
    let object = args
        .as_object()
        .context("Phase 2 stree command must be an object")?;
    let report = required_string(object, "report", 4_000)?;
    match name {
        SUBMIT_DEBATE_TURN => {
            required_string(object, "stance", 32)?;
            required_string(object, "message", 1_200)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_defaults_to_the_required_collision_wave() {
        let result = execute(
            ROUTE_DEBATE_TURN,
            json!({
                "reply_to_node_id": "topic-a:stree:4",
                "message": "Both sides must address the opposing opening."
            }),
        )
        .unwrap();

        assert_eq!(
            result.pointer("/artifact/phase2_stree/payload/targets"),
            Some(&json!(["bull", "bear"]))
        );
        assert_eq!(
            result.pointer("/artifact/phase2_stree/payload/report"),
            Some(&json!("Both sides must address the opposing opening."))
        );
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
    }
}
