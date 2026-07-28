use serde::{Deserialize, Serialize};

/// Manifest for a prompt component plugin.
/// Lives at `prompts/common/components/<name>/manifest.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComponentManifest {
    /// Unique component name (e.g., "ticker", "anti_injection").
    pub name: String,
    /// Which roles this component should be injected into.
    /// Values: "*" (all roles), or a list of role IDs like ["analyst.technical"].
    pub injection_points: Vec<String>,
    /// Priority for ordering when multiple components inject into the same role.
    /// Lower = earlier in the prompt. Default: 100.
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// Placeholder key the component content will be assigned to.
    /// The role template references this via `{placeholder_key}`.
    pub placeholder_key: String,
    /// Variables this component's template requires.
    /// These must be present in the render values map.
    #[serde(default)]
    pub required_variables: Vec<String>,
    /// Whether this component is enabled. Can be overridden by config.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_priority() -> i32 {
    100
}

fn default_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_component_manifest_defaults() {
        let manifest: ComponentManifest = toml::from_str(
            r#"
name = "ticker"
injection_points = ["*"]
placeholder_key = "common_ticker_prompt"
"#,
        )
        .unwrap();

        assert_eq!(manifest.name, "ticker");
        assert_eq!(manifest.priority, 100);
        assert!(manifest.enabled);
        assert!(manifest.required_variables.is_empty());
    }

    #[test]
    fn rejects_invalid_component_manifest_without_placeholder_key() {
        let err = toml::from_str::<ComponentManifest>(
            r#"
name = "bad"
injection_points = ["*"]
"#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("placeholder_key"));
    }
}
