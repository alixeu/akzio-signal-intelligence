//! Read-only size attribution. Never prints prompts, evidence, or opaque reasoning.
// 文件导读：示例只通过 open_existing 读取 AgentTurn 与 Contract CAS，按 JSON 结构计算
// 大小归因；它不写 Store、不解密 continuation，也不把指标解释成研究/Decision 完成。
use akzio_domain::{ArtifactId, ArtifactKind, ContentHash, RunId};
use akzio_store::Store;
use serde_json::{json, Value};

// 统计一个已经解析的 JSON 值的紧凑序列化字节数，用于请求分项归因。
fn bytes(v: &Value) -> usize {
    serde_json::to_vec(v).expect("JSON").len()
}

// 从既有 Store 读取 AgentTurn，并把请求、上下文、历史续接和工具负载拆成只读大小指标。
// `--run` 只负责按 Run 找到 AgentTurn；本示例不改变任何 Artifact、BLOB 或运行状态。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: agent_token_attribution STORE AGENT_TURN_ID...")?;
    let store = Store::open_existing(root)?;
    let mut rows = Vec::new();
    let mut ids = args.collect::<Vec<_>>();

    // `--run` 要求唯一的 Run ID，并从 Store 的事件/Artifact 索引中取 AgentTurn。
    // 这里得到的仍是历史 Artifact 列表，不代表 Run 已完成或 Decision 已获准。
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
        // 每个参数必须指向 AgentTurn；随后只读取其 CAS BLOB 和对应的 Contract 安装。
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
                // function_call_output 是重放给后续请求的工具结果；带原始 context 的 user
                // 项恢复首次上下文，其余项才计入对话历史。加密内容只能按字节计数，不能解密或输出。
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
        // Contract 由 AgentTurn 的哈希绑定；缺失安装时停止，避免把治理源大小归因到未知版本。
        let hash: ContentHash = serde_json::from_value(v["contract_hash"].clone())?;
        let contract = store
            .contract_installation(&hash)?
            .ok_or("missing contract")?;
        let mut metadata = 0;
        let mut facts = 0;
        let mut task_contract = 0;
        for d in context.as_array().into_iter().flatten() {
            // Context 中的任务契约、元数据和事实分别计数；这里只计算 JSON 结构大小。
            if d["class"] == "task_contract" {
                task_contract += bytes(d);
            } else if d["type"] == "context_metadata_ledger" {
                metadata += bytes(d);
            } else {
                metadata += bytes(&d["metadata"]);
                facts += bytes(&d["value"]);
            }
        }
        // 输出的是可审计的大小/telemetry 指标，不包含 prompt、证据正文或不透明推理内容。
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
    // 所有输入处理完成后一次性打印指标；打印成功不表示任何研究、Decision 或执行阶段完成。
    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}
