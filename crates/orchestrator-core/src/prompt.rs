use serde_json::Value;

pub fn replace_placeholders(template: &str, values: &Value) -> String {
    let mut out = template.to_string();
    if let Value::Object(map) = values {
        for (key, value) in map {
            let replacement = match value {
                Value::String(text) => text.clone(),
                _ => value.to_string(),
            };
            out = out.replace(&format!("{{{key}}}"), &replacement);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replaces_known_placeholders() {
        assert_eq!(
            replace_placeholders("ticker={ticker}; n={n}", &json!({"ticker": "QQQ", "n": 3})),
            "ticker=QQQ; n=3"
        );
    }
}
