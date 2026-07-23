use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::plugin_manifest::{ComponentManifest, RoleManifest};
use crate::prompt::replace_placeholders;

pub const KNOWN_RENDER_VARIABLES: &[&str] = &[
    "ticker",
    "tickers",
    "common_ticker_prompt",
    "analyst_output_contract",
    "anti_injection",
    "research_calibration",
    "research_drivers",
    "leveraged_etf_rules",
    "analyst_output_structure",
    "analyst_artifact_schema",
    "research_artifact_schema",
    "trade_intent_schema",
    "risk_constraints_schema",
    "final_validation_schema",
    "portfolio_allocation_schema",
    "risk_analyst_body",
    "role",
    "phase",
    "kind",
    "lang",
    "side",
    "side_label",
    "opponent",
    "opponent_label",
    "stance",
    "stance_label",
    "stance_intro",
    "stance_rules",
    "stance_schema_extra",
    "researcher_body",
    "workflow_pattern",
    "run_id",
    "date",
    "window_days",
    "round",
    "topic_id",
    "topic",
    "analyst_reports",
    "research_plan",
    "trader_plan",
    "risk_history",
    "portfolio_decision",
    "allocation_context",
];

/// Discovered component plugins, keyed by component name.
#[derive(Debug, Clone, Default)]
pub struct ComponentRegistry {
    pub components: BTreeMap<String, ComponentPlugin>,
}

#[derive(Debug, Clone)]
pub struct ComponentPlugin {
    pub manifest: ComponentManifest,
    pub template: String,
    pub path: PathBuf,
}

/// Discovered role plugins, keyed by role_id.
#[derive(Debug, Clone, Default)]
pub struct RolePluginRegistry {
    pub roles: BTreeMap<String, RolePlugin>,
}

#[derive(Debug, Clone)]
pub struct RolePlugin {
    pub manifest: RoleManifest,
    pub template: String,
    pub path: PathBuf,
}

impl ComponentRegistry {
    /// Scan `prompts/common/components/` for subdirectories containing `manifest.toml`.
    pub fn discover(prompts_dir: &Path) -> Result<Self> {
        Self::discover_dir(&prompts_dir.join("common/components"))
    }

    /// Scan one component plugin root directory.
    pub fn discover_dir(components_dir: &Path) -> Result<Self> {
        let mut registry = Self::default();
        registry.discover_dir_into(components_dir)?;
        Ok(registry)
    }

    /// Scan multiple component plugin roots and merge them by component name.
    /// Later roots override earlier roots with the same component name.
    pub fn discover_all(component_dirs: &[PathBuf]) -> Result<Self> {
        let mut registry = Self::default();
        for dir in component_dirs {
            registry.discover_dir_into(dir)?;
        }
        Ok(registry)
    }

    fn discover_dir_into(&mut self, components_dir: &Path) -> Result<()> {
        if !components_dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(components_dir).with_context(|| {
            format!("failed to read components dir {}", components_dir.display())
        })? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let dir = entry.path();
            if !dir.join("manifest.toml").exists() {
                continue;
            }
            let plugin = load_component_plugin(&dir)?;
            self.components.insert(plugin.manifest.name.clone(), plugin);
        }
        Ok(())
    }

    /// Disable components by manifest name after discovery.
    pub fn disable_components(&mut self, disabled: &[String]) {
        let disabled = disabled.iter().collect::<BTreeSet<_>>();
        for plugin in self.components.values_mut() {
            if disabled.contains(&plugin.manifest.name) {
                plugin.manifest.enabled = false;
            }
        }
    }

    /// Return components that should be injected into the given role, sorted by
    /// priority ascending and then by component name for stable ordering.
    pub fn for_role(&self, role_id: &str) -> Vec<&ComponentPlugin> {
        let mut plugins = self
            .components
            .values()
            .filter(|plugin| {
                plugin.manifest.enabled
                    && (plugin
                        .manifest
                        .injection_points
                        .iter()
                        .any(|point| point == "*")
                        || plugin
                            .manifest
                            .injection_points
                            .iter()
                            .any(|point| point == role_id))
            })
            .collect::<Vec<_>>();
        plugins.sort_by(|left, right| {
            left.manifest
                .priority
                .cmp(&right.manifest.priority)
                .then_with(|| left.manifest.name.cmp(&right.manifest.name))
        });
        plugins
    }

    pub fn placeholder_keys(&self) -> Vec<String> {
        self.components
            .values()
            .filter(|plugin| plugin.manifest.enabled)
            .map(|plugin| plugin.manifest.placeholder_key.clone())
            .collect()
    }

    pub fn has_enabled_placeholder(&self, placeholder_key: &str) -> bool {
        self.components.values().any(|plugin| {
            plugin.manifest.enabled && plugin.manifest.placeholder_key == placeholder_key
        })
    }

    pub fn has_enabled_placeholder_for_role(&self, placeholder_key: &str, role_id: &str) -> bool {
        self.for_role(role_id)
            .iter()
            .any(|plugin| plugin.manifest.placeholder_key == placeholder_key)
    }

    pub fn render_for_role(&self, role_id: &str, values: &mut Value) -> Result<()> {
        if !values.is_object() {
            bail!("component render values must be a JSON object");
        }
        for plugin in self.for_role(role_id) {
            let rendered = replace_placeholders(&plugin.template, values);
            if let Some(map) = values.as_object_mut() {
                map.insert(
                    plugin.manifest.placeholder_key.clone(),
                    Value::String(rendered),
                );
            }
        }
        Ok(())
    }

    pub fn validate_required_variables(&self) -> Result<()> {
        let known = KNOWN_RENDER_VARIABLES
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let plugin_placeholders = self
            .components
            .values()
            .map(|plugin| plugin.manifest.placeholder_key.as_str())
            .collect::<BTreeSet<_>>();
        for plugin in self.components.values() {
            validate_component_manifest(&plugin.manifest, &plugin.path)?;
            for variable in plugin
                .manifest
                .required_variables
                .iter()
                .chain(plugin.manifest.schema_dependencies.iter())
            {
                if !known.contains(variable.as_str())
                    && !plugin_placeholders.contains(variable.as_str())
                {
                    bail!(
                        "component plugin {} requires unknown render variable {variable:?}",
                        plugin.manifest.name
                    );
                }
            }
        }
        Ok(())
    }

    pub fn validate_role_dependencies(&self, roles: &RolePluginRegistry) -> Result<()> {
        for role in roles.roles.values() {
            role.validate_components(self)?;
        }
        Ok(())
    }
}

impl RolePluginRegistry {
    /// Scan `prompts/roles/` for subdirectories containing `manifest.toml`.
    pub fn discover(prompts_dir: &Path) -> Result<Self> {
        Self::discover_dir(&prompts_dir.join("roles"))
    }

    /// Scan one role plugin root directory.
    pub fn discover_dir(roles_dir: &Path) -> Result<Self> {
        let mut roles = BTreeMap::new();
        if !roles_dir.exists() {
            return Ok(Self { roles });
        }
        for entry in std::fs::read_dir(roles_dir)
            .with_context(|| format!("failed to read roles dir {}", roles_dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let dir = entry.path();
            if !dir.join("manifest.toml").exists() {
                continue;
            }
            let plugin = load_role_plugin(&dir)?;
            if roles.contains_key(&plugin.manifest.role_id) {
                bail!("duplicate role plugin id {:?}", plugin.manifest.role_id);
            }
            roles.insert(plugin.manifest.role_id.clone(), plugin);
        }
        Ok(Self { roles })
    }

    pub fn validate_components(&self, components: &ComponentRegistry) -> Result<()> {
        for role in self.roles.values() {
            validate_role_manifest(&role.manifest, &role.path)?;
            role.validate_components(components)?;
        }
        Ok(())
    }
}

impl RolePlugin {
    pub fn role_path(&self) -> PathBuf {
        self.path.join("role.md")
    }

    fn validate_components(&self, components: &ComponentRegistry) -> Result<()> {
        for component_name in &self.manifest.components {
            match components.components.get(component_name) {
                Some(component) if component.manifest.enabled => {}
                Some(_) => bail!(
                    "role plugin {} references disabled component {component_name:?}",
                    self.manifest.role_id
                ),
                None => bail!(
                    "role plugin {} references missing component {component_name:?}",
                    self.manifest.role_id
                ),
            }
        }
        Ok(())
    }
}

pub fn validate_plugins(components: &ComponentRegistry, roles: &RolePluginRegistry) -> Result<()> {
    components.validate_required_variables()?;
    roles.validate_components(components)?;
    Ok(())
}

fn load_component_plugin(dir: &Path) -> Result<ComponentPlugin> {
    let manifest_path = dir.join("manifest.toml");
    let template_path = dir.join("component.md");
    if !manifest_path.exists() {
        bail!(
            "component plugin directory {} is missing manifest.toml",
            dir.display()
        );
    }
    if !template_path.exists() {
        bail!(
            "component plugin directory {} is missing component.md",
            dir.display()
        );
    }
    let manifest_text = std::fs::read_to_string(&manifest_path).with_context(|| {
        format!(
            "failed to read component manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: ComponentManifest = toml::from_str(&manifest_text).with_context(|| {
        format!(
            "failed to parse component manifest {}",
            manifest_path.display()
        )
    })?;
    let template = std::fs::read_to_string(&template_path).with_context(|| {
        format!(
            "failed to read component template {}",
            template_path.display()
        )
    })?;
    validate_component_manifest(&manifest, dir)?;
    Ok(ComponentPlugin {
        manifest,
        template,
        path: dir.to_path_buf(),
    })
}

fn load_role_plugin(dir: &Path) -> Result<RolePlugin> {
    let manifest_path = dir.join("manifest.toml");
    let template_path = dir.join("role.md");
    if !manifest_path.exists() {
        bail!(
            "role plugin directory {} is missing manifest.toml",
            dir.display()
        );
    }
    if !template_path.exists() {
        bail!("role plugin directory {} is missing role.md", dir.display());
    }
    let manifest_text = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read role manifest {}", manifest_path.display()))?;
    let manifest: RoleManifest = toml::from_str(&manifest_text)
        .with_context(|| format!("failed to parse role manifest {}", manifest_path.display()))?;
    let template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read role template {}", template_path.display()))?;
    validate_role_manifest(&manifest, dir)?;
    Ok(RolePlugin {
        manifest,
        template,
        path: dir.to_path_buf(),
    })
}

fn validate_component_manifest(manifest: &ComponentManifest, dir: &Path) -> Result<()> {
    if manifest.name.trim().is_empty() {
        bail!("component plugin {} has empty name", dir.display());
    }
    if manifest.placeholder_key.trim().is_empty() {
        bail!(
            "component plugin {} has empty placeholder_key",
            manifest.name
        );
    }
    if manifest.injection_points.is_empty() {
        bail!(
            "component plugin {} must declare at least one injection point",
            manifest.name
        );
    }
    Ok(())
}

fn validate_role_manifest(manifest: &RoleManifest, dir: &Path) -> Result<()> {
    if manifest.role_id.trim().is_empty() {
        bail!("role plugin {} has empty role_id", dir.display());
    }
    if manifest.short_name.trim().is_empty() {
        bail!("role plugin {} has empty short_name", manifest.role_id);
    }
    if !(1..=7).contains(&manifest.phase) {
        bail!(
            "role plugin {} phase must be between 1 and 7",
            manifest.role_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_component(root: &Path, name: &str, injection_points: &str, priority: i32) {
        let dir = root.join("common/components").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("manifest.toml"),
            format!(
                r#"name = "{name}"
injection_points = {injection_points}
priority = {priority}
placeholder_key = "{name}_prompt"
required_variables = ["ticker"]
"#
            ),
        )
        .unwrap();
        std::fs::write(dir.join("component.md"), format!("component {name}")).unwrap();
    }

    #[test]
    fn discovers_component_plugins() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        write_component(&prompts, "test_comp", "[\"*\"]", 100);

        let registry = ComponentRegistry::discover(&prompts).unwrap();

        assert_eq!(registry.components.len(), 1);
        assert_eq!(
            registry.components["test_comp"].template,
            "component test_comp"
        );
    }

    #[test]
    fn for_role_filters_and_sorts_by_priority() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        write_component(&prompts, "late", "[\"*\"]", 200);
        write_component(&prompts, "early", "[\"analyst.technical\"]", 10);
        write_component(&prompts, "other", "[\"analyst.news_macro\"]", 1);

        let registry = ComponentRegistry::discover(&prompts).unwrap();
        let names = registry
            .for_role("analyst.technical")
            .into_iter()
            .map(|plugin| plugin.manifest.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["early", "late"]);
    }

    #[test]
    fn renders_components_into_value_map() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        let dir = prompts.join("common/components/ticker");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("manifest.toml"),
            r#"name = "ticker"
injection_points = ["*"]
priority = 10
placeholder_key = "common_ticker_prompt"
required_variables = ["ticker", "tickers"]
"#,
        )
        .unwrap();
        std::fs::write(dir.join("component.md"), "Ticker {ticker}: {tickers}").unwrap();
        let registry = ComponentRegistry::discover(&prompts).unwrap();
        let mut values = serde_json::json!({
            "ticker": "QQQ",
            "tickers": "QQQ,SOXX",
            "common_ticker_prompt": ""
        });

        registry
            .render_for_role("analyst.technical", &mut values)
            .unwrap();

        assert_eq!(
            values["common_ticker_prompt"].as_str(),
            Some("Ticker QQQ: QQQ,SOXX")
        );
    }

    #[test]
    fn discovers_role_plugins() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        let dir = prompts.join("roles/analyst.test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("manifest.toml"),
            r#"role_id = "analyst.test"
short_name = "test"
phase = 1
components = ["test_comp"]
tools = ["read_run_context"]
default_weight = 5.0
"#,
        )
        .unwrap();
        std::fs::write(dir.join("role.md"), "role template").unwrap();

        let registry = RolePluginRegistry::discover(&prompts).unwrap();

        assert_eq!(registry.roles.len(), 1);
        assert_eq!(registry.roles["analyst.test"].template, "role template");
    }

    #[test]
    fn validation_fails_for_missing_component_dependency() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        let dir = prompts.join("roles/analyst.test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("manifest.toml"),
            r#"role_id = "analyst.test"
short_name = "test"
phase = 1
components = ["missing"]
"#,
        )
        .unwrap();
        std::fs::write(dir.join("role.md"), "role template").unwrap();
        let components = ComponentRegistry::default();
        let roles = RolePluginRegistry::discover(&prompts).unwrap();

        let err = validate_plugins(&components, &roles).unwrap_err();

        assert!(err.to_string().contains("missing component"));
    }

    #[test]
    fn disabled_components_are_not_returned_for_role() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        write_component(&prompts, "banner", "[\"*\"]", 100);
        let mut registry = ComponentRegistry::discover(&prompts).unwrap();

        registry.disable_components(&["banner".to_string()]);

        assert!(registry.for_role("analyst.technical").is_empty());
    }
}
