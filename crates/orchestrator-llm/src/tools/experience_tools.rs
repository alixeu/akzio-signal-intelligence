//! Bounded, read-only Experience retrieval tools.
//!
//! The model never supplies a run, path, phase, role, ticker, or arbitrary
//! source reference. It may only formulate lexical terms and expand a pattern
//! returned by its own immediately preceding search.

use std::{
    collections::{BTreeMap, BTreeSet},
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
    #[serde(default)]
    ticker: Option<String>,
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
    fn search(&self, lexical_query: &str, ticker: Option<&str>) -> Result<Value>;
    fn read_cases(&self, pattern_id: &str, ticker: Option<&str>) -> Result<Value>;
    fn record_application(
        &self,
        pattern_id: &str,
        ticker: Option<&str>,
        disposition: MemoryApplicationDisposition,
        reason: &str,
    ) -> Result<Value>;
}

#[derive(Clone)]
pub struct ExperienceRetrievalBinding {
    service: Arc<dyn ExperienceRetrievalService>,
    /// Pattern IDs returned by the latest search, with the Rust-authorized
    /// ticker context that produced them.
    visible_patterns: Arc<Mutex<BTreeMap<String, Option<String>>>>,
    expanded_patterns: Arc<Mutex<BTreeSet<String>>>,
    recorded_applications: Arc<Mutex<BTreeSet<String>>>,
    allowed_tickers: BTreeSet<String>,
    max_case_reads: usize,
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
        Self::new_scoped(service, std::iter::empty::<String>(), 1)
    }

    /// Create a binding whose ticker scope and Detail-read budget are fixed
    /// by Rust. The model may select only one of `allowed_tickers` per search,
    /// so a multi-asset role cannot silently treat one ticker's lesson as
    /// evidence for another.
    pub fn new_scoped(
        service: Arc<dyn ExperienceRetrievalService>,
        allowed_tickers: impl IntoIterator<Item = String>,
        max_case_reads: usize,
    ) -> Self {
        Self {
            service,
            visible_patterns: Arc::new(Mutex::new(BTreeMap::new())),
            expanded_patterns: Arc::new(Mutex::new(BTreeSet::new())),
            recorded_applications: Arc::new(Mutex::new(BTreeSet::new())),
            allowed_tickers: allowed_tickers
                .into_iter()
                .map(|ticker| ticker.trim().to_owned())
                .filter(|ticker| !ticker.is_empty())
                .collect(),
            max_case_reads: max_case_reads.clamp(1, 20),
        }
    }

    fn scoped_ticker(&self, ticker: Option<&str>) -> Result<Option<String>> {
        let ticker = ticker
            .map(str::trim)
            .filter(|ticker| !ticker.is_empty())
            .map(ToOwned::to_owned);
        if self.allowed_tickers.is_empty() {
            if ticker.is_some() {
                bail!("search_experiences does not permit ticker selection for this role");
            }
            return Ok(None);
        }
        let ticker = ticker.context("search_experiences requires one Rust-authorized ticker")?;
        if !self.allowed_tickers.contains(&ticker) {
            bail!("search_experiences ticker is outside this role's Rust-authorized scope");
        }
        Ok(Some(ticker))
    }

    fn visible_ticker(&self, pattern_id: &str) -> Result<Option<String>> {
        self.visible_patterns
            .lock()
            .map_err(|_| anyhow::anyhow!("experience retrieval visibility lock poisoned"))?
            .get(pattern_id)
            .cloned()
            .with_context(|| "pattern_id was not returned by this turn's search_experiences")
    }

    pub fn execute(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            SEARCH_EXPERIENCES_NAME => {
                let args: SearchArgs = serde_json::from_value(arguments)
                    .context("search_experiences arguments are invalid")?;
                if args.query.trim().is_empty() || args.query.len() > 500 {
                    bail!("search_experiences.query must be 1..=500 characters");
                }
                let ticker = self.scoped_ticker(args.ticker.as_deref())?;
                let output = self.service.search(args.query.trim(), ticker.as_deref())?;
                let items = output
                    .get("items")
                    .and_then(Value::as_array)
                    .context("Experience retrieval service returned no items array")?;
                let mut visible = self.visible_patterns.lock().map_err(|_| {
                    anyhow::anyhow!("experience retrieval visibility lock poisoned")
                })?;
                visible.clear();
                for item in items {
                    let pattern_id = item
                        .get("pattern_id")
                        .and_then(Value::as_str)
                        .context("Experience retrieval item has no pattern_id")?;
                    visible.insert(pattern_id.to_owned(), ticker.clone());
                }
                self.expanded_patterns
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval expansion lock poisoned"))?
                    .clear();
                self.recorded_applications
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval application lock poisoned"))?
                    .clear();
                Ok(output)
            }
            READ_EXPERIENCE_CASES_NAME => {
                let args: ReadArgs = serde_json::from_value(arguments)
                    .context("read_experience_cases arguments are invalid")?;
                let ticker = self.visible_ticker(&args.pattern_id)?;
                {
                    let expanded = self.expanded_patterns.lock().map_err(|_| {
                        anyhow::anyhow!("experience retrieval expansion lock poisoned")
                    })?;
                    if !expanded.contains(&args.pattern_id) && expanded.len() >= self.max_case_reads
                    {
                        bail!("read_experience_cases exceeded this turn's Rust Detail budget");
                    }
                }
                let output = self
                    .service
                    .read_cases(&args.pattern_id, ticker.as_deref())?;
                let mut expanded = self
                    .expanded_patterns
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval expansion lock poisoned"))?;
                expanded.insert(args.pattern_id);
                Ok(output)
            }
            RECORD_MEMORY_APPLICATION_NAME => {
                let args: ApplicationArgs = serde_json::from_value(arguments)
                    .context("record_memory_application arguments are invalid")?;
                if args.reason.trim().is_empty() || args.reason.len() > 1_000 {
                    bail!("record_memory_application.reason must be 1..=1000 characters");
                }
                let ticker = self.visible_ticker(&args.pattern_id)?;
                if !self
                    .expanded_patterns
                    .lock()
                    .map_err(|_| anyhow::anyhow!("experience retrieval expansion lock poisoned"))?
                    .contains(&args.pattern_id)
                {
                    bail!("record_memory_application requires read_experience_cases for the same visible pattern");
                }
                let mut recorded = self.recorded_applications.lock().map_err(|_| {
                    anyhow::anyhow!("experience retrieval application lock poisoned")
                })?;
                if recorded.contains(&args.pattern_id) {
                    bail!("record_memory_application accepts at most one disposition per pattern per turn");
                }
                let output = self.service.record_application(
                    &args.pattern_id,
                    ticker.as_deref(),
                    args.disposition,
                    args.reason.trim(),
                )?;
                recorded.insert(args.pattern_id);
                Ok(output)
            }
            _ => bail!("unknown Experience retrieval tool {name}"),
        }
    }
}

pub fn definition(name: &str) -> Option<ToolDefinition> {
    let (description, parameters) = match name {
        SEARCH_EXPERIENCES_NAME => (
            "Search Rust-authorized Experience Views using a bounded lexical query. If this role covers multiple assets, choose exactly one Rust-authorized ticker; phase, scope, horizon, regime, as-of boundary and budget remain fixed by Rust.",
            json!({"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":500},"ticker":{"type":"string","minLength":1}},"required":["query"],"additionalProperties":false}),
        ),
        READ_EXPERIENCE_CASES_NAME => (
            "Read one Detail-budgeted case for a pattern returned by this turn's search_experiences call. The model cannot expand arbitrary patterns.",
            json!({"type":"object","properties":{"pattern_id":{"type":"string","minLength":1}},"required":["pattern_id"],"additionalProperties":false}),
        ),
        RECORD_MEMORY_APPLICATION_NAME => (
            "Record whether one expanded pattern was applied or rejected. Rust records the actual call and its selected ticker; the reason remains an untrusted model claim.",
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
        fn search(&self, _lexical_query: &str, _ticker: Option<&str>) -> Result<Value> {
            Ok(
                json!({"items":[{"pattern_id":"visible"},{"pattern_id":"visible-2"}],"stop_reason":"sufficient"}),
            )
        }
        fn read_cases(&self, pattern_id: &str, _ticker: Option<&str>) -> Result<Value> {
            Ok(json!({"pattern_id":pattern_id,"untrusted_historical_data":[]}))
        }
        fn record_application(
            &self,
            pattern_id: &str,
            _ticker: Option<&str>,
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

    #[test]
    fn scoped_ticker_and_detail_budget_are_enforced_before_service_calls() {
        let binding = ExperienceRetrievalBinding::new_scoped(
            Arc::new(Service),
            ["QQQ".to_owned(), "SOXX".to_owned()],
            1,
        );

        assert!(binding
            .execute(SEARCH_EXPERIENCES_NAME, json!({"query":"technical"}))
            .is_err());
        assert!(binding
            .execute(
                SEARCH_EXPERIENCES_NAME,
                json!({"query":"technical", "ticker":"VIX"}),
            )
            .is_err());
        binding
            .execute(
                SEARCH_EXPERIENCES_NAME,
                json!({"query":"technical", "ticker":"QQQ"}),
            )
            .unwrap();
        assert!(binding
            .execute(
                RECORD_MEMORY_APPLICATION_NAME,
                json!({"pattern_id":"visible","disposition":"applied","reason":"not expanded"}),
            )
            .is_err());
        binding
            .execute(READ_EXPERIENCE_CASES_NAME, json!({"pattern_id":"visible"}))
            .unwrap();
        assert!(binding
            .execute(
                READ_EXPERIENCE_CASES_NAME,
                json!({"pattern_id":"visible-2"})
            )
            .is_err());
        binding
            .execute(
                RECORD_MEMORY_APPLICATION_NAME,
                json!({"pattern_id":"visible","disposition":"applied","reason":"matches current evidence"}),
            )
            .unwrap();
        assert!(binding
            .execute(
                RECORD_MEMORY_APPLICATION_NAME,
                json!({"pattern_id":"visible","disposition":"rejected","reason":"duplicate"}),
            )
            .is_err());
    }
}
