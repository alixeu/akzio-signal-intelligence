use super::*;

pub(super) fn validate_model_capabilities(
    snapshot: &ModelCapabilitySnapshot,
    request: &AgentModelRequest,
) -> ResearchResult<()> {
    if !snapshot.supports_stateless_continuation {
        return Err(ResearchError::CapabilityMismatch {
            capability: "stateless_continuation",
            provider_id: snapshot.provider_id.clone(),
            model_id: snapshot.model_id.clone(),
        });
    }
    if (!request.tools.is_empty() || request.terminal.is_some()) && !snapshot.supports_tool_calls {
        return Err(ResearchError::CapabilityMismatch {
            capability: "tool_calls",
            provider_id: snapshot.provider_id.clone(),
            model_id: snapshot.model_id.clone(),
        });
    }
    Ok(())
}

/// A minimal, deterministic subset of JSON Schema sufficient for the contracts
/// owned by this workspace. Contract authors must not rely on an unvalidated schema
/// keyword; unsupported shapes are rejected rather than prompt-softened.
pub(super) fn validate_output_schema(
    store: &V2Store,
    contract: &AgentContract,
    output: &Value,
) -> ResearchResult<()> {
    let schema: Value = serde_json::from_slice(&store.read_blob(&contract.output.schema)?)?;
    let schema = if contract.deliberation_policy == DeliberationPolicy::Required {
        schema
            .get("properties")
            .and_then(|properties| properties.get("result"))
            .cloned()
            .ok_or_else(|| {
                ResearchError::InvalidOutput(
                    "required deliberation schema.result missing".to_owned(),
                )
            })?
    } else {
        schema
    };
    validate_schema_value(output, &schema, "$").map_err(ResearchError::InvalidOutput)?;
    if schema.get("type").and_then(Value::as_str) != Some("object") || !output.is_object() {
        return Err(ResearchError::InvalidOutput(
            "schema and output must both be JSON objects".to_owned(),
        ));
    }
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| ResearchError::InvalidOutput("schema.required missing".to_owned()))?;
    for field in required {
        let Some(field) = field.as_str() else {
            return Err(ResearchError::InvalidOutput(
                "schema.required must contain strings".to_owned(),
            ));
        };
        if output.get(field).is_none() {
            return Err(ResearchError::InvalidOutput(format!(
                "required field {field} is missing"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_submission_schema(
    store: &V2Store,
    contract: &AgentContract,
    submission: &Value,
) -> ResearchResult<()> {
    let schema: Value = serde_json::from_slice(&store.read_blob(&contract.output.schema)?)?;
    validate_schema_value(submission, &schema, "$").map_err(ResearchError::InvalidOutput)
}

pub(super) fn value_matches_schema_kind(value: &Value, kind: &str) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "null" => value.is_null(),
        _ => false,
    }
}

pub(super) fn validate_schema_value(
    value: &Value,
    schema: &Value,
    path: &str,
) -> Result<(), String> {
    let definition = schema
        .as_object()
        .ok_or_else(|| format!("{path} schema must be an object"))?;
    for key in definition.keys() {
        if !matches!(
            key.as_str(),
            "type"
                | "description"
                | "enum"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "minimum"
                | "maximum"
                | "pattern"
                | "minLength"
                | "maxLength"
                | "minItems"
                | "maxItems"
                | "minProperties"
                | "maxProperties"
        ) {
            return Err(format!("{path} schema keyword {key} is unsupported"));
        }
    }
    let kind = match definition.get("type") {
        Some(Value::String(kind)) => kind.as_str(),
        Some(Value::Array(kinds)) => kinds
            .iter()
            .map(|kind| {
                kind.as_str()
                    .ok_or_else(|| format!("{path} schema.type entries must be strings"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|kind| value_matches_schema_kind(value, kind))
            .ok_or_else(|| format!("{path} does not match any schema.type option"))?,
        Some(_) => return Err(format!("{path} schema.type must be a string or array")),
        None => return Err(format!("{path} schema.type is missing")),
    };
    if !value_matches_schema_kind(value, kind) {
        return Err(format!("{path} must be a {kind}"));
    }
    validate_schema_bounds(value, definition, kind, path)?;
    if let Some(options) = definition.get("enum") {
        let options = options
            .as_array()
            .ok_or_else(|| format!("{path} schema.enum must be an array"))?;
        if !options.iter().any(|option| option == value) {
            return Err(format!("{path} is not an allowed enum value"));
        }
    }

    match kind {
        "object" => validate_object_schema(value, definition, path),
        "array" => {
            let item_schema = definition
                .get("items")
                .ok_or_else(|| format!("{path} array schema.items missing"))?;
            for (index, item) in value
                .as_array()
                .expect("validated array")
                .iter()
                .enumerate()
            {
                validate_schema_value(item, item_schema, &format!("{path}[{index}]"))?;
            }
            if definition.contains_key("properties")
                || definition.contains_key("required")
                || definition.contains_key("additionalProperties")
            {
                return Err(format!("{path} array schema contains object-only keywords"));
            }
            Ok(())
        }
        _ => {
            if definition.contains_key("properties")
                || definition.contains_key("required")
                || definition.contains_key("additionalProperties")
                || definition.contains_key("items")
            {
                return Err(format!("{path} scalar schema contains container keywords"));
            }
            Ok(())
        }
    }
}

pub(super) fn validate_schema_bounds(
    value: &Value,
    definition: &serde_json::Map<String, Value>,
    kind: &str,
    path: &str,
) -> Result<(), String> {
    match kind {
        "integer" | "number" => {
            let actual = value
                .as_f64()
                .ok_or_else(|| format!("{path} must be numeric"))?;
            for (keyword, accepts) in [("minimum", true), ("maximum", false)] {
                if let Some(bound) = definition.get(keyword) {
                    let bound = bound
                        .as_f64()
                        .ok_or_else(|| format!("{path} schema.{keyword} must be numeric"))?;
                    if (accepts && actual < bound) || (!accepts && actual > bound) {
                        return Err(format!("{path} violates schema.{keyword}"));
                    }
                }
            }
            if definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!(
                    "{path} numeric schema contains incompatible bounds"
                ));
            }
        }
        "string" => {
            validate_size_bounds(
                value.as_str().expect("validated string").chars().count(),
                definition,
                "minLength",
                "maxLength",
                path,
            )?;
            if let Some(pattern) = definition.get("pattern") {
                let pattern = pattern
                    .as_str()
                    .ok_or_else(|| format!("{path} schema.pattern must be a string"))?;
                let pattern = Regex::new(pattern)
                    .map_err(|error| format!("{path} schema.pattern is invalid: {error}"))?;
                if !pattern.is_match(value.as_str().expect("validated string")) {
                    return Err(format!("{path} violates schema.pattern"));
                }
            }
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} string schema contains incompatible bounds"));
            }
        }
        "array" => {
            validate_size_bounds(
                value.as_array().expect("validated array").len(),
                definition,
                "minItems",
                "maxItems",
                path,
            )?;
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} array schema contains incompatible bounds"));
            }
        }
        "object" => {
            validate_size_bounds(
                value.as_object().expect("validated object").len(),
                definition,
                "minProperties",
                "maxProperties",
                path,
            )?;
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
            {
                return Err(format!("{path} object schema contains incompatible bounds"));
            }
        }
        "null" => {}
        _ => {
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} scalar schema contains bounds"));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_size_bounds(
    actual: usize,
    definition: &serde_json::Map<String, Value>,
    minimum_key: &str,
    maximum_key: &str,
    path: &str,
) -> Result<(), String> {
    let parse_bound = |key: &str| -> Result<Option<usize>, String> {
        definition
            .get(key)
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| format!("{path} schema.{key} must be a non-negative integer"))
            })
            .transpose()
    };
    if let Some(minimum) = parse_bound(minimum_key)? {
        if actual < minimum {
            return Err(format!("{path} violates schema.{minimum_key}"));
        }
    }
    if let Some(maximum) = parse_bound(maximum_key)? {
        if actual > maximum {
            return Err(format!("{path} violates schema.{maximum_key}"));
        }
    }
    Ok(())
}

pub(super) fn validate_object_schema(
    value: &Value,
    definition: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    if definition.contains_key("items") {
        return Err(format!("{path} object schema contains array-only items"));
    }
    let properties = definition
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path} object schema.properties missing"))?;
    let required = definition
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{path} object schema.required missing"))?;
    let additional_properties = definition
        .get("additionalProperties")
        .cloned()
        .unwrap_or(Value::Bool(false));
    let object = value.as_object().expect("validated object");
    for required_name in required {
        let name = required_name
            .as_str()
            .ok_or_else(|| format!("{path} schema.required must contain strings"))?;
        if !properties.contains_key(name) {
            return Err(format!(
                "{path} required field {name} has no property schema"
            ));
        }
        if !object.contains_key(name) {
            return Err(format!("{path}.{name} is required"));
        }
    }
    for (name, item) in object {
        match properties.get(name) {
            Some(property_schema) => {
                validate_schema_value(item, property_schema, &format!("{path}.{name}"))?;
            }
            None if additional_properties == Value::Bool(true) => {}
            None if additional_properties == Value::Bool(false) => {
                return Err(format!("{path}.{name} is not allowed"));
            }
            None if additional_properties.is_object() => {
                validate_schema_value(item, &additional_properties, &format!("{path}.{name}"))?;
            }
            None => {
                return Err(format!(
                    "{path} schema.additionalProperties must be a boolean or schema object"
                ));
            }
        }
    }
    Ok(())
}
