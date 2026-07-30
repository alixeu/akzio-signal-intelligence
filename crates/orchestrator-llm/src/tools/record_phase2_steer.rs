use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::agent_loop::ToolRuntimeTurnContext;

use super::{ExternalToolConfig, ToolDefinition};

pub const NAME: &str = "record_phase2_steer";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: NAME.to_owned(),
        description: "Record the Rust-bound Phase 2 topic, round, and steer kind for this turn."
            .to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    if args.as_object().is_none_or(|object| !object.is_empty()) {
        bail!("{NAME} accepts no model-selected arguments");
    }
    let context = turn_context.context("record_phase2_steer requires turn context")?;
    if context.phase != Some(2) {
        bail!("{NAME} is available only in Phase 2");
    }
    let steer = config
        .phase2_steer
        .as_ref()
        .context("record_phase2_steer requires a Rust-bound steer")?;
    if steer.get("role").and_then(Value::as_str) != Some(context.role.as_str()) {
        bail!("{NAME} role does not match the Rust-bound turn");
    }
    Ok(json!({
        "status": "recorded",
        "steer": steer,
        "turn_id": context.turn_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ExternalToolConfig;

    #[test]
    fn records_only_the_rust_bound_phase2_steer() {
        let config = ExternalToolConfig {
            phase2_steer: Some(json!({
                "role": "researcher.bull.interaction",
                "kind": "point_debate",
                "topic_id": "topic-a",
                "round": 1,
                "round_num": 1
            })),
            ..ExternalToolConfig::default()
        };
        let context = ToolRuntimeTurnContext {
            run_id: "run-a".to_owned(),
            session_id: "session-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            role: "researcher.bull.interaction".to_owned(),
            phase: Some(2),
        };

        let output = execute(json!({}), &config, Some(&context)).unwrap();
        assert_eq!(output["status"], "recorded");
        assert_eq!(output["steer"]["round_num"], 1);
        assert!(execute(json!({"round_num": 9}), &config, Some(&context)).is_err());
    }
}
