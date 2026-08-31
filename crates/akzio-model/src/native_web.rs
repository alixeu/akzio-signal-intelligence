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
        let domains_schema = json!({
            "type": "array",
            "minItems": usize::from(!self.allowed_hosts.is_empty()),
            "maxItems": self.allowed_hosts.len().max(1),
            "items": {
                "type": "string",
                "enum": self.allowed_hosts,
            }
        });
        let required = if self.allowed_hosts.is_empty() {
            vec!["query"]
        } else {
            vec!["query", "domains"]
        };
        ModelToolDefinition {
            name: self.tool_name.clone(),
            description: "Rust-governed native web search; citations are mandatory".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": self.max_query_chars},
                    "domains": domains_schema,
                    "max_results": {"type": "integer", "minimum": 1, "maximum": self.max_results}
                },
                "required": required,
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
                None if self.allowed_hosts.is_empty() => Vec::new(),
                None => return Err(ModelError::NativeWebArgumentsInvalid),
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
            if !self.allowed_hosts.is_empty() && domains.is_empty() {
                return Err(ModelError::NativeWebToolNotAllowed);
            }
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

    /// Validate the hosted Responses `web_search_call` trace. Hosted web
    /// search is not a function call, so its Rust-owned bounds must be checked
    /// against `output[].action` rather than `ModelToolCall`.
    pub fn validate_provider_response(&self, raw: &Value) -> Result<()> {
        let calls = raw
            .get("output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("web_search_call"))
            .collect::<Vec<_>>();
        if calls.is_empty() {
            return Err(ModelError::NativeWebUnavailable);
        }

        let mut saw_search = false;
        let mut sources = std::collections::BTreeSet::new();
        for call in calls {
            if call.get("status").and_then(Value::as_str) != Some("completed") {
                return Err(ModelError::NativeWebUnavailable);
            }
            let action = call
                .get("action")
                .and_then(Value::as_object)
                .ok_or(ModelError::NativeWebArgumentsInvalid)?;
            match action.get("type").and_then(Value::as_str) {
                Some("search") => {
                    saw_search = true;
                    let mut queries = Vec::new();
                    if let Some(query) = action.get("query").and_then(Value::as_str) {
                        queries.push(query);
                    }
                    if let Some(values) = action.get("queries").and_then(Value::as_array) {
                        queries.extend(values.iter().filter_map(Value::as_str));
                    }
                    if queries.is_empty()
                        || queries.iter().any(|query| {
                            query.trim().is_empty() || query.chars().count() > self.max_query_chars
                        })
                    {
                        return Err(ModelError::NativeWebLimitExceeded);
                    }
                    let action_sources = action
                        .get("sources")
                        .and_then(Value::as_array)
                        .ok_or(ModelError::NativeWebArgumentsInvalid)?;
                    for source in action_sources {
                        let uri = source
                            .get("url")
                            .or_else(|| source.get("uri"))
                            .and_then(Value::as_str)
                            .ok_or(ModelError::NativeWebArgumentsInvalid)?;
                        self.validate_uri(uri)?;
                        sources.insert(uri.to_owned());
                    }
                }
                Some("open_page" | "find_in_page") => {}
                _ => return Err(ModelError::NativeWebArgumentsInvalid),
            }
        }
        if !saw_search || sources.is_empty() {
            return Err(ModelError::NativeWebArgumentsInvalid);
        }
        if sources.len() > self.max_results {
            return Err(ModelError::NativeWebLimitExceeded);
        }
        Ok(())
    }

    pub fn extract_citations(&self, raw: &Value) -> Result<Vec<NativeWebCitation>> {
        let mut citations = Vec::new();
        collect_citations(raw, &mut citations);
        let mut merged = BTreeMap::<String, NativeWebCitation>::new();
        for citation in citations {
            merged
                .entry(citation.uri.clone())
                .and_modify(|existing| existing.merge_missing(&citation))
                .or_insert(citation);
        }
        let citations = merged.into_values().collect::<Vec<_>>();
        if citations.is_empty() {
            return Err(ModelError::NativeWebCitationsMissing);
        }
        if citations.len() > self.max_citations {
            return Err(ModelError::NativeWebLimitExceeded);
        }
        for citation in &citations {
            self.validate_uri(&citation.uri)?;
        }
        Ok(citations)
    }

    fn validate_uri(&self, uri: &str) -> Result<()> {
        let parsed = reqwest::Url::parse(uri).map_err(|_| ModelError::NativeWebUnsafeCitation {
            uri: uri.to_owned(),
            reason: "invalid URL".to_owned(),
        })?;
        if parsed.scheme() != "https"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !self
                .allowed_hosts
                .iter()
                .any(|host| parsed.host_str() == Some(host.as_str()))
        {
            return Err(ModelError::NativeWebUnsafeCitation {
                uri: uri.to_owned(),
                reason: "scheme, credentials, port, or host is not allowed".to_owned(),
            });
        }
        Ok(())
    }
}

impl NativeWebCitation {
    fn merge_missing(&mut self, candidate: &Self) {
        if self.title.is_none() {
            self.title.clone_from(&candidate.title);
        }
        if self.excerpt.is_none() {
            self.excerpt.clone_from(&candidate.excerpt);
        }
        if self.published_at.is_none() {
            self.published_at.clone_from(&candidate.published_at);
        }
        if self.revision.is_none() {
            self.revision.clone_from(&candidate.revision);
        }
        if self.document_id.is_none() {
            self.document_id.clone_from(&candidate.document_id);
        }
    }
}

fn collect_citations(value: &Value, output: &mut Vec<NativeWebCitation>) {
    match value {
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_citations(value, output)),
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
                .for_each(|value| collect_citations(value, output));
        }
        _ => {}
    }
}
