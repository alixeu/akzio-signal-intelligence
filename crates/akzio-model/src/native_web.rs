//! Native web policy and citation extraction.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebPolicy {
    pub tool_name: String,
    pub allowed_hosts: Vec<String>,
    pub max_query_chars: usize,
    pub max_results: usize,
    pub max_citations: usize,
}

impl Default for NativeWebPolicy {
    fn default() -> Self {
        Self {
            tool_name: NATIVE_WEB_SEARCH_TOOL.to_owned(),
            allowed_hosts: vec![
                "sec.gov".to_owned(),
                "fred.stlouisfed.org".to_owned(),
                "reuters.com".to_owned(),
                "apnews.com".to_owned(),
            ],
            max_query_chars: 2_000,
            max_results: 8,
            max_citations: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebQuery {
    pub query: String,
    pub domains: Vec<String>,
    pub max_results: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebCitation {
    pub uri: String,
    pub title: Option<String>,
    pub excerpt: Option<String>,
    #[serde(default)]
    pub published_at: Option<String>,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub document_id: Option<String>,
}

impl NativeWebPolicy {
    pub fn tool_definition(&self) -> ModelToolDefinition {
        ModelToolDefinition {
            name: self.tool_name.clone(),
            description: "Rust-governed native web search; citations are mandatory".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": self.max_query_chars},
                    "domains": {"type": "array", "items": {"type": "string"}},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": self.max_results}
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            strict: true,
        }
    }

    pub fn validate_tool_calls(&self, calls: &[ModelToolCall]) -> Result<Vec<NativeWebQuery>> {
        let mut queries = Vec::with_capacity(calls.len());
        for call in calls {
            if call.name != self.tool_name {
                return Err(ModelError::NativeWebToolNotAllowed);
            }
            let object = call
                .arguments
                .as_object()
                .ok_or(ModelError::NativeWebArgumentsInvalid)?;
            let query = object
                .get("query")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(ModelError::NativeWebArgumentsInvalid)?
                .trim()
                .to_owned();
            if query.chars().count() > self.max_query_chars {
                return Err(ModelError::NativeWebLimitExceeded);
            }
            let domains = match object.get("domains") {
                None => Vec::new(),
                Some(value) => value
                    .as_array()
                    .ok_or(ModelError::NativeWebArgumentsInvalid)?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or(ModelError::NativeWebArgumentsInvalid)
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
            if domains
                .iter()
                .any(|domain| !self.allowed_hosts.iter().any(|allowed| domain == allowed))
            {
                return Err(ModelError::NativeWebToolNotAllowed);
            }
            let max_results = object
                .get("max_results")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(self.max_results);
            if max_results == 0 || max_results > self.max_results {
                return Err(ModelError::NativeWebLimitExceeded);
            }
            queries.push(NativeWebQuery {
                query,
                domains,
                max_results,
            });
        }
        Ok(queries)
    }

    pub fn extract_citations(&self, raw: &Value) -> Result<Vec<NativeWebCitation>> {
        let mut citations = Vec::new();
        collect_citations(raw, &mut citations, self.max_citations);
        citations.sort_by(|left, right| left.uri.cmp(&right.uri));
        citations.dedup_by(|left, right| left.uri == right.uri);
        if citations.is_empty() {
            return Err(ModelError::NativeWebCitationsMissing);
        }
        if citations.len() > self.max_citations {
            return Err(ModelError::NativeWebLimitExceeded);
        }
        for citation in &citations {
            let parsed = reqwest::Url::parse(&citation.uri)
                .map_err(|_| ModelError::NativeWebUnsafeCitation)?;
            if parsed.username() != ""
                || parsed.password().is_some()
                || parsed.query().is_some()
                || !self
                    .allowed_hosts
                    .iter()
                    .any(|host| parsed.host_str() == Some(host.as_str()))
            {
                return Err(ModelError::NativeWebUnsafeCitation);
            }
        }
        Ok(citations)
    }
}

fn collect_citations(value: &Value, output: &mut Vec<NativeWebCitation>, limit: usize) {
    if output.len() >= limit {
        return;
    }
    match value {
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_citations(value, output, limit)),
        Value::Object(object) => {
            let uri = object
                .get("url")
                .or_else(|| object.get("uri"))
                .and_then(Value::as_str);
            if let Some(uri) = uri.filter(|value| !value.trim().is_empty()) {
                output.push(NativeWebCitation {
                    uri: uri.to_owned(),
                    title: object
                        .get("title")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    excerpt: object
                        .get("quote")
                        .or_else(|| object.get("text"))
                        .or_else(|| object.get("excerpt"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    published_at: object
                        .get("published_at")
                        .or_else(|| object.get("publishedAt"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    revision: object
                        .get("revision")
                        .or_else(|| object.get("version"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    document_id: object
                        .get("document_id")
                        .or_else(|| object.get("documentId"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            object
                .values()
                .for_each(|value| collect_citations(value, output, limit));
        }
        _ => {}
    }
}
