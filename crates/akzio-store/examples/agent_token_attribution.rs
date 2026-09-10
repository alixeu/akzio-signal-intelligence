//! Read-only size attribution. Never prints prompts, evidence, or opaque reasoning.
use akzio_domain::{ArtifactId, ArtifactKind, ContentHash, RunId};
use akzio_store::Store;
use serde_json::{json, Value};
fn bytes(v: &Value) -> usize {
    serde_json::to_vec(v).expect("JSON").len()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: agent_token_attribution STORE AGENT_TURN_ID...")?;
    let store = Store::open_existing(root)?;
    let mut rows = Vec::new();
    let mut ids = args.collect::<Vec<_>>();
    if ids.first().is_some_and(|id| id == "--run") {
        if ids.len() != 2 {
            return Err("usage: agent_token_attribution STORE --run RUN_ID".into());
        }
        ids = store
            .run_artifacts_by_kind(&RunId(ids[1].clone()), ArtifactKind::AgentTurn)?
            .into_iter()
            .map(|a| a.artifact_id.0.to_string())
            .collect();
    }
    for id in ids {
        let a = store.artifact(&ArtifactId(ContentHash::new(id)?))?;
        if a.kind != ArtifactKind::AgentTurn {
            return Err("expected AgentTurn".into());
        }
        let v: Value = serde_json::from_slice(&store.read_blob(&a.blob)?)?;
        let r = &v["request"];
        let mut context = r["context"].clone();
        let mut history = 0;
        let mut replay = 0;
        let mut opaque = 0;
        if let Some(items) = r.pointer("/continuation/items").and_then(Value::as_array) {
            for item in items {
                if item["type"] == "function_call_output" {
                    replay += bytes(item);
                } else if item["role"] == "user"
                    && item["content"]
                        .as_str()
                        .is_some_and(|s| s.starts_with("{\"context\""))
                {
                    let original: Value = serde_json::from_str(item["content"].as_str().unwrap())?;
                    context = original["context"].clone();
                } else {
                    history += bytes(item);
                }
                opaque += item["encrypted_content"].as_str().map_or(0, str::len);
            }
        }
        let hash: ContentHash = serde_json::from_value(v["contract_hash"].clone())?;
        let contract = store
            .contract_installation(&hash)?
            .ok_or("missing contract")?;
        let mut metadata = 0;
        let mut facts = 0;
        let mut task_contract = 0;
        for d in context.as_array().into_iter().flatten() {
            if d["class"] == "task_contract" {
                task_contract += bytes(d);
            } else if d["type"] == "context_metadata_ledger" {
                metadata += bytes(d);
            } else {
                metadata += bytes(&d["metadata"]);
                facts += bytes(&d["value"]);
            }
        }
        rows.push(json!({"artifact_id":a.artifact_id,"origin":a.origin,"turn":v["turn"],
            "whole_request_json_bytes":bytes(r),"runtime_request_estimate_tokens":akzio_domain::estimate_json_tokens(r)?,
            "prompt_json_bytes":bytes(&r["prompt"]),
            "governance_source_bytes":store.read_blob(&contract.contract.prompt.governance)?.len(),
            "role_source_bytes":store.read_blob(&contract.contract.prompt.role)?.len(),
            "context_json_bytes":bytes(&context),"context_metadata_bytes":metadata,"context_facts_bytes":facts,"task_contract_bytes":task_contract,
            "previous_output_json_bytes":history,"opaque_continuation_bytes_in_history":opaque,
            "replayed_tool_json_bytes":replay,"new_tool_json_bytes":bytes(&r["tool_outputs"]),
            "read_tools_json_bytes":bytes(&r["tools"]),"terminal_schema_json_bytes":bytes(&r["terminal"]),
            "provider_usage":v.pointer("/response/telemetry")}));
    }
    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}
