//! 既有 Store 的只读请求大小归因；输出结构化计数，不主动打印 request/prompt/证据正文。
// 文件导读：示例只通过 open_existing 读取 AgentTurn 与 Contract CAS，按 JSON 结构计算
// 大小归因；它不写 Store、不解密 continuation，也不把指标解释成研究/Decision 完成。
// `provider_usage` 会原样带出持久化 telemetry；此本地示例不是通用的分享安全脱敏导出器。
use akzio_domain::{ArtifactId, ArtifactKind, ContentHash, RunId};
use akzio_store::Store;
use serde_json::{json, Value};

// 统计一个已经解析的 JSON 值的紧凑序列化字节数，用于请求分项归因。
// `expect` 把 serde 序列化错误转为 panic；这里输入已是内存 JSON Value，示例选择让异常终止而不是返回部分统计。
fn bytes(v: &Value) -> usize {
    serde_json::to_vec(v).expect("JSON").len()
}

// 从既有 Store 读取 AgentTurn，并把请求、上下文、历史续接和工具负载拆成只读大小指标。
// `--run` 只负责按 Run 找到 AgentTurn；本示例不改变任何 Artifact、BLOB 或运行状态。
// 返回 `Box<dyn Error>` 用一个堆上 trait object 统一承接 CLI 解析、Store 与 JSON 的不同错误类型；
// `?` 通过各错误类型到 Box<dyn Error> 的 From 转换逐层提前返回。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: agent_token_attribution STORE AGENT_TURN_ID...")?;
    let store = Store::open_existing(root)?;
    let mut rows = Vec::new();
    let mut ids = args.collect::<Vec<_>>();

    // `--run` 要求唯一的 Run ID，并按 Artifact.origin.run_id/kind 索引读取 AgentTurn；
    // 不依赖成功 Attempt output 索引，也不从事件推断调用已成功或 Run 已完成。
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
        // serde_json 的 `[]` 索引在字段缺失时返回 Null；此工具不把缺失请求校验为错误，
        // 后续 `bytes`/估算会计算 Null 的 JSON 大小，不能视作完整 provider 请求。
        let r = &v["request"];
        let mut context = r["context"].clone();
        let mut history = 0;
        let mut replay = 0;
        let mut opaque = 0;
        if let Some(items) = r.pointer("/continuation/items").and_then(Value::as_array) {
            for item in items {
                // function_call_output 计入工具结果；带 JSON context 的 user 项会更新 context，
                // 如果出现多项则以最后命中的为准，不能称为“必然恢复首次上下文”。
                // 其余项计入历史；这些是紧凑 JSON 字节分类，不是 provider 实际计费 token。
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
                // 只统计每项顶层 encrypted_content 字符串的 UTF-8 字节数；它可能已包含在
                // history/replay 的 JSON 字节中，不可把这些栏目直接相加当作独立总量。
                opaque += item["encrypted_content"].as_str().map_or(0, str::len);
            }
        }
        // 从 AgentTurn payload 的 contract_hash 查询安装记录；缺失/无效 hash 或安装均返回 Err。
        // 此处没有额外比较 Artifact.origin 的 contract_hash，不能单凭查询称为完整来源绑定证明。
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
        // 输出的是 JSON 字节数与约每 4 字节一个 token 的估算，不是 provider 实际 usage；
        // governance/role 取 BLOB 逻辑长度，telemetry 原样回显但不主动输出 prompt/证据正文。
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
