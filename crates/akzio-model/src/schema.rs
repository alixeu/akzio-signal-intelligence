//! Provider schema sanitization.

use super::*;

pub(super) fn provider_schema(value: &Value) -> Value {
    // 递归生成 provider 可接受的参数 Schema：移除仅供本地校验的长度、数值和
    // 属性约束；带 properties 的对象再由当前属性集合重建 required，保持 wire
    // payload 与 Rust 侧实际字段边界一致。
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
                // 这样不会让本地可选字段与 wire Schema 不一致。
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
