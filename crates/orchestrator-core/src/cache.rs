use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub const TTL_TECHNICAL: Duration = Duration::from_secs(300);
pub const TTL_NEWS: Duration = Duration::from_secs(1800);
pub const TTL_SOCIAL: Duration = Duration::from_secs(3600);
pub const TTL_WEB_SEARCH: Duration = Duration::from_secs(600);
pub const TTL_RUN_CONTEXT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct CacheEntry {
    pub value: Value,
    pub inserted_at: Instant,
    pub ttl: Duration,
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        self.inserted_at.elapsed() > self.ttl
    }
}

#[derive(Clone, Default)]
pub struct TtlCache {
    entries: HashMap<String, CacheEntry>,
}

impl TtlCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .get(key)
            .filter(|entry| !entry.is_expired())
            .map(|entry| &entry.value)
    }

    pub fn set(&mut self, key: String, value: Value, ttl: Duration) {
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted_at: Instant::now(),
                ttl,
            },
        );
    }

    pub fn evict_expired(&mut self) {
        self.entries.retain(|_, e| !e.is_expired());
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub fn cache_key(tool_name: &str, kind: &str, ticker: &str, phase: i64) -> String {
    format!("{tool_name}:{kind}:{ticker}:{phase}")
}

pub fn cache_key_for_args(tool_name: &str, args: &Value) -> Option<String> {
    let kind = args.get("kind").and_then(Value::as_str).unwrap_or("");
    let ticker = args
        .get("ticker")
        .and_then(Value::as_str)
        .or_else(|| {
            args.get("tickers")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(Value::as_str)
        })
        .unwrap_or("");
    if kind.is_empty() && ticker.is_empty() {
        return None;
    }
    Some(cache_key(tool_name, kind, ticker, 1))
}

pub fn ttl_for_tool(name: &str, args: &Value) -> Option<Duration> {
    if name == "web.run" {
        return Some(TTL_WEB_SEARCH);
    }
    if name != "read_run_context" {
        return None;
    }
    let kind = args.get("kind").and_then(Value::as_str).unwrap_or("");
    match kind {
        "technical" | "technical_daily" | "technical_3h" | "technical_20min" => Some(TTL_TECHNICAL),
        "jin10" => Some(TTL_NEWS),
        "compose_context" | "research_inputs" | "analyst_reports" | "debate_history"
        | "topic_state" | "mediator_reviews" | "role_summaries" | "turn_context"
        | "prior_memory" => Some(TTL_RUN_CONTEXT),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cache_hit_and_expiry() {
        let mut cache = TtlCache::new();
        let key = "test:key";
        cache.set(
            key.to_string(),
            json!({"ok": true}),
            Duration::from_millis(100),
        );
        assert!(cache.get(key).is_some());
        std::thread::sleep(Duration::from_millis(150));
        assert!(cache.get(key).is_none());
    }

    #[test]
    fn evict_removes_expired() {
        let mut cache = TtlCache::new();
        cache.set("a".to_string(), json!(1), Duration::from_millis(1));
        cache.set("b".to_string(), json!(2), Duration::from_secs(3600));
        std::thread::sleep(Duration::from_millis(10));
        cache.evict_expired();
        assert_eq!(cache.len(), 1);
        assert!(cache.get("b").is_some());
    }

    #[test]
    fn cache_key_generation() {
        let key = cache_key("read_run_context", "technical", "TQQQ", 1);
        assert!(key.contains("read_run_context"));
        assert!(key.contains("technical"));
        assert!(key.contains("TQQQ"));
    }
}
