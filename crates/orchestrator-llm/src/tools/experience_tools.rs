//! Bounded, read-only Experience retrieval tools.
//!
//! The model never supplies a run, path, phase, role, ticker, or arbitrary
//! source reference. It may only formulate lexical terms and expand a pattern
//! returned by its own immediately preceding search.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use orchestrator_core::{MemoryApplicationDisposition, ToolId};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, ToolDefinition};

pub const SEARCH_EXPERIENCES_NAME: &str = ToolId::SearchExperiences.as_str();
pub const READ_EXPERIENCE_CASES_NAME: &str = ToolId::ReadExperienceCases.as_str();
pub const RECORD_MEMORY_APPLICATION_NAME: &str = ToolId::RecordMemoryApplication.as_str();

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    query: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    pattern_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationArgs {
    pattern_id: String,
    disposition: MemoryApplicationDisposition,
    reason: String,
}

pub trait ExperienceRetrievalService: Send + Sync {
    fn search(&self, lexical_query: &str) -> Result<Value>;
    fn read_cases(&self, pattern_id: &str) -> Result<Value>;
    fn record_application(
        &self,
        pattern_id: &str,
        disposition: MemoryApplicationDisposition,
        reason: &str,
    ) -> Result<Value>;
}

#[derive(Clone)]
pub struct ExperienceRetrievalBinding {
    service: Arc<dyn ExperienceRetrievalService>,
    visible_pattern_ids: Arc<Mutex<BTreeSet<String>>>,
}

impl fmt::Debug for ExperienceRetrievalBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExperienceRetrievalBinding")
            .field("service", &"ExperienceRetrievalService")
            .finish()
    }
}

impl ExperienceRetrievalBinding {
    pub fn new(service: Arc<dyn ExperienceRetrievalService>) -> Self {
        Self {
            service,
            visible_pattern_ids: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    pub fn execute(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            SEARCH_EXPERIENCES_NAME => {
                let args: SearchArgs = serde_json::from_value(arguments)
                    .context("search_experiences arguments are invalid")?;
                if args.query.trim().is_empty() || args.query.len() > 500 {
                    bail!("search_experiences.query must be 1..=500 characters");
                }
                let output = self.service.search(args.query.trim())?;
                let items = output
                    .get("items")
                    .and_then(Value::as_array)
                    .context("Experience retrieval service returned no items array")?;
                let mut visible = self.visible_pattern_ids.lock().map_err(|_| {
                    anyhow::anyhow!("experience retrieval visibility lock poisoned")
                })?;
                visible.clear();
                for item in items {
                    let pattern_id = item
                        .get("pattern_id")
                        .and_then(Value::as_str)
                        .context("Experience retrieval item has no pattern_id")?;
                    visible.insert(pattern_id.to_owned());
                }
                Ok(output)
            }
            READ_EXPERIENCE_CASES_NAME => {
                let args: ReadArgs = serde_json::from_value(arguments)
                    .context("read_experience_cases arguments are invalid")?;
                if !self
                    .visible_pattern_ids
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval visibility lock poisoned"))?
                    .contains(&args.pattern_id)
                {
                    bail!("pattern_id was not returned by this turn's search_experiences");
                }
                self.service.read_cases(&args.pattern_id)
            }
            RECORD_MEMORY_APPLICATION_NAME => {
                let args: ApplicationArgs = serde_json::from_value(arguments)
                    .context("record_memory_application arguments are invalid")?;
                if args.reason.trim().is_empty() || args.reason.len() > 1_000 {
                    bail!("record_memory_application.reason must be 1..=1000 characters");
                }
                if !self
                    .visible_pattern_ids
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval visibility lock poisoned"))?
                    .contains(&args.pattern_id)
                {
                    bail!("pattern_id was not returned by this turn's search_experiences");
                }
                self.service.record_application(
                    &args.pattern_id,
                    args.disposition,
                    args.reason.trim(),
                )
            }
            _ => bail!("unknown Experience retrieval tool {name}"),
        }
    }
}

pub fn definition(name: &str) -> Option<ToolDefinition> {
    let (description, parameters) = match name {
        SEARCH_EXPERIENCES_NAME => (
            "Search Rust-authorized Experience Views using a bounded lexical query. Phase, role, scope, ticker, regime and budget are fixed by Rust.",
            json!({"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":500}},"required":["query"],"additionalProperties":false}),
        ),
        READ_EXPERIENCE_CASES_NAME => (
            "Read cases for one pattern returned by this turn's search_experiences call. The model cannot expand arbitrary patterns.",
            json!({"type":"object","properties":{"pattern_id":{"type":"string","minLength":1}},"required":["pattern_id"],"additionalProperties":false}),
        ),
        RECORD_MEMORY_APPLICATION_NAME => (
            "Record whether one pattern returned by this turn's search was applied or rejected. Rust records the actual call; the reason remains an untrusted model claim.",
            json!({"type":"object","properties":{"pattern_id":{"type":"string","minLength":1},"disposition":{"type":"string","enum":["applied","rejected"]},"reason":{"type":"string","minLength":1,"maxLength":1000}},"required":["pattern_id","disposition","reason"],"additionalProperties":false}),
        ),
        _ => return None,
    };
    Some(ToolDefinition {
        name: api_tool_name(name),
        description: description.to_owned(),
        parameters,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Service;
    impl ExperienceRetrievalService for Service {
        fn search(&self, _lexical_query: &str) -> Result<Value> {
            Ok(json!({"items":[{"pattern_id":"visible"}],"stop_reason":"sufficient"}))
        }
        fn read_cases(&self, pattern_id: &str) -> Result<Value> {
            Ok(json!({"pattern_id":pattern_id,"untrusted_historical_data":[]}))
        }
        fn record_application(
            &self,
            pattern_id: &str,
            disposition: MemoryApplicationDisposition,
            _reason: &str,
        ) -> Result<Value> {
            Ok(json!({"pattern_id":pattern_id,"disposition":disposition,"recorded_by":"rust"}))
        }
    }

    #[test]
    fn cases_must_follow_a_search_in_the_same_binding() {
        let binding = ExperienceRetrievalBinding::new(Arc::new(Service));
        assert!(binding
            .execute(READ_EXPERIENCE_CASES_NAME, json!({"pattern_id":"visible"}))
            .is_err());
        binding
            .execute(SEARCH_EXPERIENCES_NAME, json!({"query":"technical"}))
            .unwrap();
        assert!(binding
            .execute(READ_EXPERIENCE_CASES_NAME, json!({"pattern_id":"visible"}))
            .is_ok());
        assert!(binding
            .execute(READ_EXPERIENCE_CASES_NAME, json!({"pattern_id":"other"}))
            .is_err());
        assert!(binding
            .execute(
                RECORD_MEMORY_APPLICATION_NAME,
                json!({"pattern_id":"visible","disposition":"applied","reason":"matches current evidence"}),
            )
            .is_ok());
    }
}
