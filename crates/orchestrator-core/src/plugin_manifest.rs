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
    /// Optional schema placeholder keys this component depends on.
    #[serde(default)]
    pub schema_dependencies: Vec<String>,
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

/// Manifest for a role plugin.
/// Lives at `prompts/roles/<name>/manifest.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoleManifest {
    /// Unique role ID (e.g., "analyst.technical", "manager.research").
    pub role_id: String,
    /// Short name for CLI usage (e.g., "technical").
    pub short_name: String,
    /// Workflow phase this role belongs to (1-7).
    pub phase: i64,
    /// Component names this role depends on.
    #[serde(default)]
    pub components: Vec<String>,
    /// Tool names this role is allowed to use.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Preflight tool to run before the role.
    #[serde(default)]
    pub preflight_tool: Option<String>,
    /// Default analyst weight (0.0-100.0). Only relevant for Phase 1 analysts.
    #[serde(default)]
    pub default_weight: f64,
    /// Whether this role is critical (blocks workflow if missing).
    #[serde(default)]
    pub is_critical: bool,
    /// Output schema type for validation.
    #[serde(default = "default_output_schema")]
    pub output_schema: String,
    /// Whether this role supports a `_monitor` variant.
    #[serde(default)]
    pub supports_monitor_mode: bool,
}

fn default_output_schema() -> String {
    "json_artifact".to_string()
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
        assert!(manifest.schema_dependencies.is_empty());
    }

    #[test]
    fn parses_role_manifest_defaults() {
        let manifest: RoleManifest = toml::from_str(
            r#"
role_id = "analyst.test"
short_name = "test"
phase = 1
"#,
        )
        .unwrap();

        assert_eq!(manifest.role_id, "analyst.test");
        assert_eq!(manifest.output_schema, "json_artifact");
        assert!(manifest.components.is_empty());
        assert!(manifest.tools.is_empty());
        assert!(!manifest.is_critical);
        assert!(!manifest.supports_monitor_mode);
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
