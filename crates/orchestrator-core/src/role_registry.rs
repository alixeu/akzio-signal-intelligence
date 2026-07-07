use crate::plugin_manifest::RoleManifest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub role_id: String,
    pub short_name: String,
    pub phase: i64,
    pub prompt_path: PathBuf,
    pub preflight_tool: Option<String>,
    pub default_tools: Vec<String>,
    pub default_weight: f64,
    pub is_critical: bool,
}

impl AgentDefinition {
    pub fn from_manifest(manifest: &RoleManifest, prompt_path: PathBuf) -> Self {
        Self {
            role_id: manifest.role_id.clone(),
            short_name: manifest.short_name.clone(),
            phase: manifest.phase,
            prompt_path,
            preflight_tool: manifest.preflight_tool.clone(),
            default_tools: manifest.tools.clone(),
            default_weight: manifest.default_weight,
            is_critical: manifest.is_critical,
        }
    }

    pub fn preflight_tool_name(&self) -> Option<&str> {
        self.preflight_tool.as_deref()
    }
}

#[derive(Debug, Clone, Default)]
pub struct AgentRegistry {
    agents: BTreeMap<String, AgentDefinition>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self {
            agents: BTreeMap::new(),
        }
    }

    pub fn register(&mut self, def: AgentDefinition) {
        self.agents.insert(def.role_id.clone(), def);
    }

    pub fn get(&self, role_id: &str) -> Option<&AgentDefinition> {
        self.agents.get(role_id)
    }

    pub fn phase1_agents(&self) -> Vec<&AgentDefinition> {
        self.agents.values().filter(|a| a.phase == 1).collect()
    }

    pub fn all_role_ids(&self) -> Vec<String> {
        self.agents.keys().cloned().collect()
    }

    /// Look up a role_id from a short name (e.g. "technical" -> "analyst.technical")
    pub fn role_id_from_short(&self, short: &str) -> Option<String> {
        self.agents
            .values()
            .find(|a| a.short_name == short)
            .map(|a| a.role_id.clone())
    }

    /// Parse comma-separated role list, supporting both short names and full role_ids
    pub fn parse_role_list(&self, raw: &str) -> Result<Vec<String>, String> {
        let mut roles = Vec::new();
        for item in raw.split(',') {
            let text = item.trim();
            if text.is_empty() {
                continue;
            }
            // Legacy alias: "news" -> "news_macro"
            let lookup = if text == "news" { "news_macro" } else { text };
            let role_id = if self.agents.contains_key(lookup) {
                lookup.to_string()
            } else if let Some(id) = self.role_id_from_short(lookup) {
                id
            } else if text == "fundamental" {
                return Err(
                    "standalone fundamental analyst was removed; use news/news_macro".into(),
                );
            } else {
                return Err(format!("unsupported phase1 agent {text:?}"));
            };
            roles.push(role_id);
        }
        Ok(roles)
    }

    /// Normalize a short name to full role_id
    pub fn normalize_role_name(&self, name: &str) -> String {
        let trimmed = name.trim();
        if let Some(def) = self.get(trimmed) {
            return def.role_id.clone();
        }
        // Legacy alias: "news" -> "news_macro"
        let lookup = if trimmed == "news" {
            "news_macro"
        } else {
            trimmed
        };
        self.role_id_from_short(lookup)
            .unwrap_or_else(|| trimmed.to_string())
    }

    pub fn from_role_manifests<'a>(
        plugins: impl IntoIterator<Item = (&'a RoleManifest, PathBuf)>,
    ) -> Self {
        let mut registry = Self::new();
        for (manifest, prompt_path) in plugins {
            registry.register(AgentDefinition::from_manifest(manifest, prompt_path));
        }
        registry
    }

    pub fn extend_role_manifests<'a>(
        &mut self,
        plugins: impl IntoIterator<Item = (&'a RoleManifest, PathBuf)>,
    ) {
        for (manifest, prompt_path) in plugins {
            self.register(AgentDefinition::from_manifest(manifest, prompt_path));
        }
    }

    pub fn builtin() -> Self {
        let mut registry = Self::new();
        registry.register(AgentDefinition {
            role_id: "analyst.technical".into(),
            short_name: "technical".into(),
            phase: 1,
            prompt_path: "prompts/analysts/technical.md".into(),
            preflight_tool: Some("run_technical_indicators".into()),
            default_tools: vec!["read_run_context".into()],
            default_weight: 40.0,
            is_critical: true,
        });
        registry.register(AgentDefinition {
            role_id: "analyst.news_macro".into(),
            short_name: "news_macro".into(),
            phase: 1,
            prompt_path: "prompts/analysts/news_macro.md".into(),
            preflight_tool: Some("fetch_jin10_flash".into()),
            default_tools: vec![],
            default_weight: 35.0,
            is_critical: true,
        });
        registry.register(AgentDefinition {
            role_id: "analyst.youtube".into(),
            short_name: "youtube".into(),
            phase: 1,
            prompt_path: "prompts/analysts/youtube.md".into(),
            preflight_tool: None,
            default_tools: vec![],
            default_weight: 8.0,
            is_critical: false,
        });
        registry.register(AgentDefinition {
            role_id: "analyst.reddit".into(),
            short_name: "reddit".into(),
            phase: 1,
            prompt_path: "prompts/analysts/reddit.md".into(),
            preflight_tool: None,
            default_tools: vec![],
            default_weight: 9.0,
            is_critical: false,
        });
        registry.register(AgentDefinition {
            role_id: "analyst.x".into(),
            short_name: "x".into(),
            phase: 1,
            prompt_path: "prompts/analysts/x.md".into(),
            preflight_tool: None,
            default_tools: vec![],
            default_weight: 8.0,
            is_critical: false,
        });
        registry
    }
}

pub const DEFAULT_PHASE1_AGENTS: &str = "technical,news,youtube,reddit,x";
