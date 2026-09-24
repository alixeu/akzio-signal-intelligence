//! Provider schema sanitization.

use super::*;

// 文件导读：provider_schema 将本地工具 Schema 投影成 Responses API 可发送的形状，
// 由 `openai_responses_request_body` 在请求序列化时调用。它返回新 Value，不修改 Rust
// 本地持有的原始 Schema；最终参数合法性仍由 AgentRuntime/Context 工具校验。
pub(super) fn provider_schema(value: &Value) -> Value {
    // 这是对 serde_json::Value 的纯转换：输入 Schema 不被原地修改，返回的新树才是
    // provider wire 用的参数 Schema；Rust 本地校验仍使用调用方持有的原始树。
    // 递归生成 provider 可接受的参数 Schema：移除仅供本地校验的长度、数值和
    // 属性约束；带 properties 的对象会把全部字段列为 provider 的 required。
    // 这比本地可选字段更严格，Rust 仍需按原 Schema 验证响应，不能只信 provider。
    match value {
        Value::Object(object) => {
            let properties = object.get("properties").and_then(Value::as_object);
            let mut sanitized = serde_json::Map::new();
            for (key, value) in object {
                // 本地约束仍留在 ModelToolDefinition.input_schema 中供 Rust 使用，
                // 但不直接发送到 provider；原子类型和其他 provider 字段继续递归保留。
                if matches!(
                    key.as_str(),
                    "minLength"
                        | "maxLength"
                        | "pattern"
                        | "format"
                        | "minimum"
                        | "maximum"
                        | "exclusiveMinimum"
                        | "exclusiveMaximum"
                        | "multipleOf"
                        | "minItems"
                        | "maxItems"
                        | "uniqueItems"
                        | "minProperties"
                        | "maxProperties"
                        | "patternProperties"
                ) || (properties.is_some() && key == "required")
                {
                    continue;
                }
                if key == "properties" {
                    // 属性值继续递归清理，名称和顺序沿用输入 Map 的迭代结果。
                    let sanitized_properties = properties
                        .expect("object properties were checked above")
                        .iter()
                        .map(|(name, schema)| (name.clone(), provider_schema(schema)))
                        .collect();
                    sanitized.insert(key.clone(), Value::Object(sanitized_properties));
                } else {
                    sanitized.insert(key.clone(), provider_schema(value));
                }
            }
            if let Some(properties) = properties {
                // 发送给 provider 的对象 Schema 在这里将全部 properties 列入 required；
                // 本地可选字段在 wire 上也必须显式出现（通常用 null 分支表达）；
                // 这是 provider 结构化输出约束，不会改变 Rust 侧原 Schema。
                sanitized.insert(
                    "required".to_owned(),
                    Value::Array(properties.keys().cloned().map(Value::String).collect()),
                );
            }
            Value::Object(sanitized)
        }
        Value::Array(values) => values.iter().map(provider_schema).collect(),
        value => value.clone(),
    }
}
