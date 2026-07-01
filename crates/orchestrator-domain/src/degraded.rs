use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegradedReport {
    pub is_degraded: bool,
    pub roles: Vec<DegradedEntry>,
}

impl DegradedReport {
    pub fn new() -> Self {
        DegradedReport {
            is_degraded: false,
            roles: vec![],
        }
    }

    pub fn add_role(&mut self, entry: DegradedEntry) {
        self.is_degraded = true;
        self.roles.push(entry);
    }
}

impl Default for DegradedReport {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegradedEntry {
    pub role: String,
    pub phase: i64,
    pub error: String,
    pub used_fallback: bool,
    #[serde(default)]
    pub confidence_impact: ConfidenceImpact,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum ConfidenceImpact {
    #[default]
    None,
    Minor,
    Moderate,
    Severe,
}
