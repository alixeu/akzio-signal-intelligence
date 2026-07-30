use anyhow::{bail, Context, Result};
use futures::future::BoxFuture;
use orchestrator_core::md5_3;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use crate::agent_loop::ToolRuntimeTurnContext;

use super::ToolDefinition;

pub const NAME: &str = "research_evidence_gap";
pub const MAX_WEB_SEARCH_QUERIES: usize = 5;
pub const VERIFIED_PACKET_MARKER: &str = "## Rust-verified Web evidence packets";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceResearchRequest {
    pub tickers: Vec<String>,
    pub claim: String,
    pub evidence_gap: String,
    pub needed_facts: Vec<String>,
    pub time_window: String,
}

impl EvidenceResearchRequest {
    fn validate(&mut self, allowed_tickers: &BTreeSet<String>) -> Result<()> {
        self.tickers = self
            .tickers
            .iter()
            .map(|ticker| ticker.trim().to_ascii_uppercase())
            .filter(|ticker| !ticker.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if self.tickers.is_empty() || self.tickers.len() > 3 {
            bail!("{NAME}.tickers must contain 1..=3 tickers");
        }
        if self
            .tickers
            .iter()
            .any(|ticker| !allowed_tickers.contains(ticker))
        {
            bail!("{NAME}.tickers must stay within the Rust-bound analysis universe");
        }
        self.claim = bounded_text("claim", &self.claim, 1_000)?;
        self.evidence_gap = bounded_text("evidence_gap", &self.evidence_gap, 1_000)?;
        if self.needed_facts.is_empty() || self.needed_facts.len() > 5 {
            bail!("{NAME}.needed_facts must contain 1..=5 items");
        }
        self.needed_facts = self
            .needed_facts
            .iter()
            .map(|fact| bounded_text("needed_facts[]", fact, 300))
            .collect::<Result<Vec<_>>>()?;
        self.time_window = self.time_window.trim().to_owned();
        if !matches!(
            self.time_window.as_str(),
            "0-3d" | "0-7d" | "30d" | "historical"
        ) {
            bail!("{NAME}.time_window must be one of 0-3d, 0-7d, 30d, historical");
        }
        Ok(())
    }
}

fn bounded_text(field: &str, value: &str, max_chars: usize) -> Result<String> {
    let value = value.trim();
    let len = value.chars().count();
    if value.is_empty() || len > max_chars {
        bail!("{NAME}.{field} must contain 1..={max_chars} characters");
    }
    Ok(value.to_owned())
}

#[derive(Debug, Clone)]
pub struct EvidenceResearchScope {
    pub scope_key: String,
    pub role: String,
    pub topic_id: Option<String>,
    pub allowed_tickers: BTreeSet<String>,
    pub max_calls: usize,
}

impl EvidenceResearchScope {
    pub fn validate(&self) -> Result<()> {
        if self.scope_key.trim().is_empty()
            || self.role.trim().is_empty()
            || self.allowed_tickers.is_empty()
            || self.max_calls == 0
        {
            bail!("EvidenceResearchScope requires scope, role, tickers, and max_calls");
        }
        Ok(())
    }
}

pub trait EvidenceResearchService: Send + Sync {
    fn research(
        &self,
        request: EvidenceResearchRequest,
        request_id: String,
        topic_id: Option<String>,
    ) -> BoxFuture<'static, Result<Value>>;
}

#[derive(Debug, Default)]
struct ScopeBudget {
    request_ids: BTreeSet<String>,
    results: BTreeMap<String, Value>,
}

#[derive(Clone, Default)]
pub struct EvidenceResearchCoordinator {
    scopes: Arc<Mutex<BTreeMap<String, ScopeBudget>>>,
}

impl fmt::Debug for EvidenceResearchCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvidenceResearchCoordinator")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct EvidenceResearchBinding {
    service: Arc<dyn EvidenceResearchService>,
    coordinator: EvidenceResearchCoordinator,
    scope: EvidenceResearchScope,
}

impl fmt::Debug for EvidenceResearchBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvidenceResearchBinding")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl EvidenceResearchBinding {
    pub fn new(
        service: Arc<dyn EvidenceResearchService>,
        coordinator: EvidenceResearchCoordinator,
        scope: EvidenceResearchScope,
    ) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            service,
            coordinator,
            scope,
        })
    }

    pub async fn execute(&self, arguments: Value, turn: &ToolRuntimeTurnContext) -> Result<Value> {
        if turn.phase != Some(2) || turn.role != self.scope.role || turn.run_id.trim().is_empty() {
            bail!("{NAME} turn does not match its Rust-bound Phase 2 scope");
        }
        let mut request: EvidenceResearchRequest = serde_json::from_value(arguments)
            .context("research_evidence_gap arguments are invalid")?;
        request.validate(&self.scope.allowed_tickers)?;
        let canonical = serde_json::to_string(&request)?;
        let request_id = format!("web-{}", md5_3(&canonical));

        {
            let mut scopes = self
                .coordinator
                .scopes
                .lock()
                .map_err(|_| anyhow::anyhow!("Evidence research budget lock poisoned"))?;
            let budget = scopes.entry(self.scope.scope_key.clone()).or_default();
            if let Some(cached) = budget.results.get(&request_id) {
                let mut cached = cached.clone();
                cached["cached"] = Value::Bool(true);
                return Ok(cached);
            }
            if budget.request_ids.contains(&request_id) {
                return Ok(json!({
                    "status": "duplicate_in_progress",
                    "request_id": request_id,
                    "scope": self.scope.scope_key,
                }));
            }
            if budget.request_ids.len() >= self.scope.max_calls {
                return Ok(json!({
                    "status": "budget_exhausted",
                    "request_id": request_id,
                    "scope": self.scope.scope_key,
                    "max_calls": self.scope.max_calls,
                }));
            }
            budget.request_ids.insert(request_id.clone());
        }

        let result = match self
            .service
            .research(request, request_id.clone(), self.scope.topic_id.clone())
            .await
        {
            Ok(mut output) => {
                output["request_id"] = Value::String(request_id.clone());
                output["scope"] = Value::String(self.scope.scope_key.clone());
                output["cached"] = Value::Bool(false);
                output
            }
            Err(error) => {
                tracing::warn!(
                    request_id,
                    scope = self.scope.scope_key,
                    error = %error,
                    "Web evidence subagent failed"
                );
                json!({
                    "status": "unavailable",
                    "request_id": request_id,
                    "scope": self.scope.scope_key,
                    "reason": "subagent_failed",
                    "evidence": [],
                    "counterevidence": [],
                    "unresolved_gaps": ["Web evidence subagent failed"],
                })
            }
        };
        let mut scopes = self
            .coordinator
            .scopes
            .lock()
            .map_err(|_| anyhow::anyhow!("Evidence research budget lock poisoned"))?;
        scopes
            .entry(self.scope.scope_key.clone())
            .or_default()
            .results
            .insert(request_id, result.clone());
        Ok(result)
    }
}

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: NAME.to_owned(),
        description: "Delegate one explicit evidence gap to a neutral Web research subagent. Call only after read_indexes and a relevant read_index_details expansion; disagreement with the caller's preferred stance is not a gap.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "maxItems": 3
                },
                "claim": {"type": "string", "minLength": 1, "maxLength": 1000},
                "evidence_gap": {"type": "string", "minLength": 1, "maxLength": 1000},
                "needed_facts": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1, "maxLength": 300},
                    "minItems": 1,
                    "maxItems": 5
                },
                "time_window": {
                    "type": "string",
                    "enum": ["0-3d", "0-7d", "30d", "historical"]
                }
            },
            "required": ["tickers", "claim", "evidence_gap", "needed_facts", "time_window"],
            "additionalProperties": false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Service(Arc<AtomicUsize>);

    impl EvidenceResearchService for Service {
        fn research(
            &self,
            _request: EvidenceResearchRequest,
            _request_id: String,
            _topic_id: Option<String>,
        ) -> BoxFuture<'static, Result<Value>> {
            let calls = Arc::clone(&self.0);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"status":"not_found","evidence":[],"counterevidence":[]}))
            })
        }
    }

    fn arguments(claim: &str) -> Value {
        json!({
            "tickers": ["QQQ"],
            "claim": claim,
            "evidence_gap": "Phase 1 detail contains no current primary source",
            "needed_facts": ["current official figure"],
            "time_window": "0-3d"
        })
    }

    fn turn() -> ToolRuntimeTurnContext {
        ToolRuntimeTurnContext {
            run_id: "run-a".to_owned(),
            session_id: "session-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            role: "mediator.topic".to_owned(),
            phase: Some(2),
        }
    }

    #[tokio::test]
    async fn deduplicates_and_enforces_the_rust_owned_scope_budget() {
        let calls = Arc::new(AtomicUsize::new(0));
        let binding = EvidenceResearchBinding::new(
            Arc::new(Service(Arc::clone(&calls))),
            EvidenceResearchCoordinator::default(),
            EvidenceResearchScope {
                scope_key: "run-a:topic-generation".to_owned(),
                role: "mediator.topic".to_owned(),
                topic_id: None,
                allowed_tickers: BTreeSet::from(["QQQ".to_owned()]),
                max_calls: 1,
            },
        )
        .unwrap();

        let first = binding
            .execute(arguments("claim-a"), &turn())
            .await
            .unwrap();
        let cached = binding
            .execute(arguments("claim-a"), &turn())
            .await
            .unwrap();
        let exhausted = binding
            .execute(arguments("claim-b"), &turn())
            .await
            .unwrap();

        assert_eq!(first["status"], "not_found");
        assert_eq!(cached["cached"], true);
        assert_eq!(exhausted["status"], "budget_exhausted");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
