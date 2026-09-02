//! Provider schema sanitization.

use super::*;

pub(super) fn provider_schema(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let properties = object.get("properties").and_then(Value::as_object);
            let mut sanitized = serde_json::Map::new();
            for (key, value) in object {
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
