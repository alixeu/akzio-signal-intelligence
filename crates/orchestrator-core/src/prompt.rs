use anyhow::{bail, Result};
use serde_json::Value;

/// Replace placeholders found in the original template exactly once.
///
/// Replacement text is copied verbatim and is never scanned again, so JSON or
/// prose injected by Rust may safely contain `{ticker}`, `{unknown}` or `{}`.
pub fn replace_placeholders(template: &str, values: &Value) -> String {
    render_template_inner(template, values, false).unwrap_or_else(|_| template.to_string())
}

/// Strict prompt renderer. Unknown placeholders in the original template fail
/// fast; placeholder-like text inside injected values remains literal.
pub fn render_template(template: &str, values: &Value) -> Result<String> {
    render_template_inner(template, values, true)
}

fn render_template_inner(template: &str, values: &Value, strict: bool) -> Result<String> {
    let map = values
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("template render values must be a JSON object"))?;
    let bytes = template.as_bytes();
    let mut output = String::with_capacity(template.len());
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'{' {
            let next = bytes[cursor..]
                .iter()
                .position(|byte| *byte == b'{')
                .map(|offset| cursor + offset)
                .unwrap_or(bytes.len());
            output.push_str(&template[cursor..next]);
            cursor = next;
            continue;
        }

        let start = cursor + 1;
        let Some(end_offset) = bytes[start..].iter().position(|byte| *byte == b'}') else {
            output.push_str(&template[cursor..]);
            break;
        };
        let end = start + end_offset;
        let name = &template[start..end];
        let is_placeholder = !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !is_placeholder {
            output.push_str(&template[cursor..=end]);
            cursor = end + 1;
            continue;
        }

        match map.get(name) {
            Some(Value::String(text)) => output.push_str(text),
            Some(value) => output.push_str(&value.to_string()),
            None if strict => bail!("unknown template variable {{{name}}}"),
            None => output.push_str(&template[cursor..=end]),
        }
        cursor = end + 1;
    }
    Ok(output)
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

    #[test]
    fn replacement_text_is_not_scanned_again() {
        let values = json!({
            "payload": "{\"ticker\":\"{ticker}\",\"unknown\":\"{missing_variable}\",\"empty\":{}}",
            "ticker": "QQQ"
        });
        assert_eq!(
            render_template("ticker={ticker}; payload={payload}", &values).unwrap(),
            "ticker=QQQ; payload={\"ticker\":\"{ticker}\",\"unknown\":\"{missing_variable}\",\"empty\":{}}"
        );
    }

    #[test]
    fn strict_render_rejects_only_unknown_original_variables() {
        let error = render_template("{missing_variable}", &json!({})).unwrap_err();
        assert!(error.to_string().contains("missing_variable"));
        assert_eq!(render_template("{}", &json!({})).unwrap(), "{}");
    }

    #[test]
    fn adjacent_and_repeated_placeholders_render() {
        assert_eq!(
            render_template(
                "{ticker}{ticker}/{role}/{phase1_index}",
                &json!({"ticker": "Q", "role": "r", "phase1_index": "index"})
            )
            .unwrap(),
            "QQ/r/index"
        );
    }
}
