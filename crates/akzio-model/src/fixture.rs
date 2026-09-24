//! Deterministic fixture response materialization.

use super::*;

pub(super) fn materialize_fixture(mut raw: Value, request: &ModelRequest) -> Value {
    // raw 仍是 fixture 提供的 Responses 风格 JSON；这里仅做离线占位符物化，
    // 不模拟 HTTP/SSE，也不把物化结果提升为 provider 或业务层的证明。
    // 仅离线 fixture 会走这里：从本次请求携带的受控 Context 中取出稳定
    // Artifact ID，替换 fixture 占位符；没有可绑定的 ID 时保持原响应不变。
    let input = fixture_input(request).unwrap_or_default();
    let evidence_id = fixture_context_artifact_id(&input, "normalized_evidence")
        .or_else(|| fixture_context_artifact_id(&input, "semantic_detail"));
    let claim_id = fixture_context_artifact_id(&input, "claim");
    if evidence_id.is_none() && claim_id.is_none() {
        return raw;
    }
    // 某些 fixture 把结构化结果编码在 output_text 中；只能在字符串确实是
    // JSON 时替换，解析失败则保留原始文本，不在 fixture 层猜测或改写。
    if let Some(Value::String(output_text)) = raw.get_mut("output_text") {
        if let Ok(mut output) = serde_json::from_str(output_text) {
            materialize_fixture_value(&mut output, evidence_id.as_deref(), claim_id.as_deref());
            if let Ok(text) = serde_json::to_string(&output) {
                *output_text = text;
            }
        }
    }
    // 函数调用参数同样是 JSON 字符串；这里绑定 fixture 的输入身份，不能
    // 代替真正 provider 响应的 Schema、Manifest 或业务校验。
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
    // 最后递归扫描响应其余位置，覆盖数组、对象和已经解码的值。
    materialize_fixture_value(&mut raw, evidence_id.as_deref(), claim_id.as_deref());
    raw
}

pub(super) fn fixture_input(request: &ModelRequest) -> Option<String> {
    // Fresh 请求直接使用输入文本；Continue 请求依赖上一轮 fixture 保存的
    // 私有关联值，而不是把 provider transcript 当作新的 Context 来源。
    match &request.input {
        ModelInput::Fresh { text } => Some(text.clone()),
        ModelInput::Continue { continuation, .. } => continuation.fixture_input.clone(),
    }
}

pub(super) fn fixture_context_artifact_id(input: &str, kind: &str) -> Option<String> {
    // fixture 输入约定为带 context 数组的 JSON。搜索顺序固定为 Context 条目、
    // documents、metadata，并返回首个精确 kind 的 artifact_id/document_id。
    let input = serde_json::from_str::<Value>(input).ok()?;
    let context = input.get("context")?.as_array()?;
    context.iter().find_map(|entry| {
        fixture_artifact_identity(entry, kind).or_else(|| {
            entry
                .get("documents")
                .and_then(Value::as_array)
                .and_then(|documents| {
                    documents
                        .iter()
                        .find_map(|document| fixture_artifact_identity(document, kind))
                })
                .or_else(|| {
                    entry
                        .get("metadata")
                        .and_then(|metadata| fixture_artifact_identity(metadata, kind))
                })
        })
    })
}

fn fixture_artifact_identity(value: &Value, kind: &str) -> Option<String> {
    // 只接受明确的 kind；document_id 仅是 fixture 输入的兼容字段，输出仍绑定
    // 其实际保存的身份字符串。
    if value.get("kind").and_then(Value::as_str) != Some(kind) {
        return None;
    }
    value
        .get("artifact_id")
        .or_else(|| value.get("document_id"))?
        .as_str()
        .map(ToOwned::to_owned)
}

pub(super) fn materialize_fixture_value(
    value: &mut Value,
    evidence_id: Option<&str>,
    claim_id: Option<&str>,
) {
    // 递归只替换两个已知占位符；缺少对应 Context 身份时不制造新值，其他
    // 字符串、数字和布尔值原样保留。
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

/// Fixture-only placeholders are resolved from this request's immutable input
/// and bound wire schema. Real provider responses never enter this function.
pub(super) fn materialize_phase_fixture(mut raw: Value, request: &ModelRequest) -> Result<Value> {
    // 阶段 fixture 的 JSON 解析和重新序列化失败都传播为 MissingOutput；它只处理
    // 已找到的 function-call arguments，其余 raw 字段不会被这个函数自动修复。
    // 阶段 fixture 只改 output 中函数 arguments（canonical fixture 预期为
    // submit_result），并把 schema 依赖的占位符解析为当前请求的受控范围；它不是
    // provider 输出的通用修复器。
    if let Some(items) = raw.get_mut("output").and_then(Value::as_array_mut) {
        for item in items {
            let Some(Value::String(arguments)) = item.get_mut("arguments") else {
                continue;
            };
            let mut value: Value =
                serde_json::from_str(arguments).map_err(|_| ModelError::MissingOutput)?;
            materialize_phase_value(&mut value, request)?;
            materialize_phase_metadata(&mut value, request)?;
            *arguments = serde_json::to_string(&value).map_err(|_| ModelError::MissingOutput)?;
        }
    }
    Ok(raw)
}

// Adapt only the canonical phase fixture's metadata to the actual Submit
// schema. Fault-injection Fixture/FixtureSequence responses never enter here.
fn materialize_phase_metadata(value: &mut Value, request: &ModelRequest) -> Result<()> {
    // 读取本次请求实际发送的 submit_result Schema，确保 fixture 不凭空生成
    // 当前 Schema 已禁止的字段或缺失必需引用。
    let schema = &request
        .tools
        .iter()
        .find(|tool| tool.name == "submit_result")
        .ok_or(ModelError::MissingOutput)?
        .input_schema;
    if let Some(basis_schema) =
        schema.pointer("/properties/deliberation/properties/basis_artifact_ids")
    {
        let required = basis_schema["minItems"].as_u64().unwrap_or(0) as usize;
        if let Some(basis) = value
            .pointer_mut("/deliberation/basis_artifact_ids")
            .and_then(Value::as_array_mut)
        {
            if basis.is_empty() && required > 0 {
                // 仅在 fixture 没提供 basis 时，从 Schema 的 enum 中补齐最小要求；
                // 非空内容保持不动，后续仍由正式 Rust 校验确认引用闭包和 kind。
                let allowed = basis_schema
                    .pointer("/items/enum")
                    .and_then(Value::as_array)
                    .filter(|ids| ids.len() >= required)
                    .ok_or(ModelError::MissingOutput)?;
                basis.extend(allowed.iter().take(required).cloned());
            }
        }
    }
    if let Some(properties) =
        schema.pointer("/properties/result/properties/forecasts/items/properties/thesis/properties")
    {
        if let Some(forecasts) = value
            .pointer_mut("/result/forecasts")
            .and_then(Value::as_array_mut)
        {
            for forecast in forecasts {
                if let Some(thesis) = forecast.get_mut("thesis").and_then(Value::as_object_mut) {
                    // 旧 fixture 可能带有由 Rust 计算的期限字段；Schema 未声明时
                    // 将其移除，避免把模型 fixture 写成期限或交易日历的权威来源。
                    for field in ["thesis_valid_until", "expected_holding_period_days"] {
                        if properties.get(field).is_none() {
                            thesis.remove(field);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// 解析 canonical fixture 使用的少量阶段标记；结果仍只是 fixture 输入，实际
// Artifact 身份和业务约束由运行时正式校验负责。
fn materialize_phase_value(value: &mut Value, request: &ModelRequest) -> Result<()> {
    match value {
        Value::String(marker) if marker == "$fixture.task.horizon" => {
            // 新 fixture 从 objective 的显式 scope 读取 horizon；旧的单 Analyst
            // fixture 没有 scope，沿用 t5 以保持其既有测试输入含义。
            let input: Value =
                serde_json::from_str(&fixture_input(request).ok_or(ModelError::MissingOutput)?)
                    .map_err(|_| ModelError::MissingOutput)?;
            let objective = input
                .get("objective")
                .and_then(Value::as_str)
                .ok_or(ModelError::MissingOutput)?;
            let horizon = match objective.strip_prefix("[research_horizon=") {
                Some(scoped) => scoped
                    .split_once(']')
                    .map(|(horizon, _)| horizon)
                    .ok_or(ModelError::MissingOutput)?,
                None => "t5", // Legacy single-Analyst fixture has no scoped objective.
            };
            if !["t1", "t3", "t5"].contains(&horizon) {
                return Err(ModelError::MissingOutput);
            }
            *value = Value::String(horizon.to_owned());
        }
        Value::String(marker) if marker.starts_with("$fixture.schema.") => {
            // Schema marker 只沿着 marker 指定的 pointer 查找 anyOf 分支，避免
            // 从无关字段猜测可用 Artifact ID。
            let (all, pointer) =
                if let Some(pointer) = marker.strip_prefix("$fixture.schema.all_refs:") {
                    (true, pointer)
                } else if let Some(pointer) = marker.strip_prefix("$fixture.schema.first_ref:") {
                    (false, pointer)
                } else {
                    return Err(ModelError::MissingOutput);
                };
            let schema = &request
                .tools
                .iter()
                .find(|tool| tool.name == "submit_result")
                .ok_or(ModelError::MissingOutput)?
                .input_schema;
            let reference_schema = fixture_schema_locations(schema, pointer);
            if reference_schema.is_empty() {
                return Err(ModelError::MissingOutput);
            }
            let mut ids = Vec::new();
            for reference in reference_schema {
                let allowed = reference
                    .pointer("/properties/artifact_id/enum")
                    .and_then(Value::as_array)
                    .ok_or(ModelError::MissingOutput)?;
                for id in allowed {
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
            }
            let references = ids
                .iter()
                .map(|id| {
                    id.as_str()
                        .map(|id| json!({"artifact_id": id}))
                        .ok_or(ModelError::MissingOutput)
                })
                .collect::<Result<Vec<_>>>()?;
            // Wire refs intentionally omit kind. The unchanged Rust Manifest
            // ledger resolves and validates the exact stored kind after Submit.
            *value = if all {
                Value::Array(references)
            } else {
                references
                    .into_iter()
                    .next()
                    .ok_or(ModelError::MissingOutput)?
            };
        }
        Value::Array(values) => {
            for value in values {
                materialize_phase_value(value, request)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                materialize_phase_value(value, request)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// Scoped grounds replace an object with anyOf branches. Follow the same
// explicit fixture pointer through those branches, never search unrelated
// schema fields for an arbitrary reference. Branch order remains deterministic.
// 空 pointer 返回当前节点；路径不存在时返回空集合。
fn fixture_schema_locations<'a>(schema: &'a Value, pointer: &str) -> Vec<&'a Value> {
    // 这是对 serde_json::Value 树的只读递归查找：返回的是借用的 Schema 节点，
    // 不复制、不修改请求，也不把 anyOf 分支以外的字段当作引用来源。
    if pointer.is_empty() {
        return vec![schema];
    }
    let Some(path) = pointer.strip_prefix('/') else {
        return vec![];
    };
    let (segment, rest) = path
        .split_once('/')
        .map_or((path, String::new()), |(segment, rest)| {
            (segment, format!("/{rest}"))
        });
    if let Some(child) = schema.pointer(&format!("/{segment}")) {
        return fixture_schema_locations(child, &rest);
    }
    schema
        .get("anyOf")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|branch| fixture_schema_locations(branch, pointer))
        .collect()
}

#[cfg(test)]
mod scoped_reference_tests {
    use super::*;

    #[test]
    fn phase_metadata_obeys_rust_owned_timing_and_bound_basis() {
        let mut request = ModelRequest {
            instructions: "Offline fixture".into(),
            input: ModelInput::Fresh { text: "{}".into() },
            max_output_tokens: 1000,
            reasoning_effort: None,
            fixture_key: None,
            tool_choice: ModelToolChoice::RequiredFunction("submit_result".into()),
            tools: vec![ModelToolDefinition {
                name: "submit_result".into(),
                description: "fixture".into(),
                strict: true,
                input_schema: json!({"properties":{
                    "deliberation":{"properties":{"basis_artifact_ids":{"minItems":1,"items":{"enum":["selected"]}}}},
                    "result":{"properties":{"forecasts":{"items":{"properties":{"thesis":{"properties":{"exit_condition":{"type":"string"}}}}}}}}
                }}),
            }],
        };
        let original = json!({"deliberation":{"basis_artifact_ids":[]},"result":{"forecasts":[{
            "expected_return_ppm":0,"thesis":{"thesis_valid_until":"2030-01-01T00:00:00Z",
                "expected_holding_period_days":1,"exit_condition":"fixture exit"}}]}});
        let mut value = original.clone();
        materialize_phase_metadata(&mut value, &request).unwrap();
        assert_eq!(
            value["deliberation"]["basis_artifact_ids"],
            json!(["selected"])
        );
        assert_eq!(
            value["result"]["forecasts"][0],
            json!({"expected_return_ppm":0,"thesis":{"exit_condition":"fixture exit"}})
        );
        request.tools[0].input_schema["properties"]["result"]["properties"]["forecasts"]["items"]
            ["properties"]["thesis"]["properties"] =
            json!({"thesis_valid_until":{},"expected_holding_period_days":{}});
        let mut legacy = original.clone();
        materialize_phase_metadata(&mut legacy, &request).unwrap();
        assert_eq!(legacy["result"], original["result"]);
        request.tools[0].input_schema["properties"]["deliberation"]["properties"]
            ["basis_artifact_ids"]["items"]["enum"] = json!([]);
        assert!(materialize_phase_metadata(&mut original.clone(), &request).is_err());
    }

    #[test]
    fn phase_fixture_resolves_ground_references_inside_scoped_any_of() {
        let mut request = ModelRequest {
            instructions: "Offline fixture".into(),
            input: ModelInput::Fresh { text: "{}".into() },
            max_output_tokens: 1000,
            reasoning_effort: None,
            tools: vec![ModelToolDefinition {
                name: "submit_result".into(),
                description: "Submit fixture".into(),
                strict: true,
                input_schema: json!({"properties":{"result":{"properties":{"grounds":{"items":{
                    "anyOf":[
                        {"properties":{"evidence":{"properties":{"artifact_id":{"enum":["first"]}}}}},
                        {"properties":{"evidence":{"properties":{"artifact_id":{"enum":["second"]}}}}}
                    ]
                }}}}}}),
            }],
            tool_choice: ModelToolChoice::RequiredFunction("submit_result".into()),
            fixture_key: None,
        };
        let raw = json!({"output":[{"arguments":json!({"evidence":
            "$fixture.schema.first_ref:/properties/result/properties/grounds/items/properties/evidence"
        }).to_string()}]});
        let resolved = materialize_phase_fixture(raw.clone(), &request).unwrap();
        let result: Value =
            serde_json::from_str(resolved["output"][0]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(result["evidence"], json!({"artifact_id":"first"}));
        request.tools[0].input_schema =
            json!({"properties":{"unrelated":{"properties":{"artifact_id":{"enum":["outside"]}}}}});
        assert!(materialize_phase_fixture(raw, &request).is_err());
    }
}
