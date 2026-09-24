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

// Default trait 只提供 adapter 的静态 policy；Serialize/Deserialize 让 policy 能作为
// 配置或审计数据往返，均不会触发网络访问，也不会替代 Store 的来源核验。
impl Default for NativeWebPolicy {
    fn default() -> Self {
        // 默认 allowlist 和数量上限只描述 model adapter 可接受的来源范围；实际
        // 研究证据是否入账仍由 ingest/Store 的来源与 freshness 校验决定。
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
    // 缺失的 provider 元数据由 Serde 保持为 None；adapter 不会用 URL、标题或当前时间
    // 推造 published_at、revision 或 document_id。
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
        // 将 Rust policy 编译为 provider 工具 Schema。allowlist 为空时 domains
        // 可省略，否则要求模型显式回传受限域名；该定义不直接执行网络请求。
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
        // 校验显式 function/tool call 的参数并转成受限查询；这里只返回已校验的
        // 查询意图，不执行抓取，也不把调用本身升级为已验证来源。
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
            // 有 allowlist 时，缺域名或包含域名外值都拒绝；没有 allowlist 才允许空列表。
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
            // provider 未给 max_results 时使用 Rust 上限；显式的 0 或过大值不能被接受。
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
        // hosted native web 不一定出现在 ModelToolCall 中，必须从 provider raw 的
        // web_search_call/action 轨迹和可验证 citation 共同确认，普通模型文字不算。
        let mut calls = raw
            .get("output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("web_search_call"))
            .peekable();
        if calls.peek().is_none() {
            return Err(ModelError::NativeWebUnavailable);
        }

        let mut saw_action = false;
        let mut sources = std::collections::BTreeSet::new();
        for call in calls {
            // 只有 completed action 才能作为已发生的 hosted 操作；未完成或缺 action
            // 的记录不能为后续来源提供证明。
            if call.get("status").and_then(Value::as_str) != Some("completed") {
                return Err(ModelError::NativeWebUnavailable);
            }
            let action = call
                .get("action")
                .and_then(Value::as_object)
                .ok_or(ModelError::NativeWebArgumentsInvalid)?;
            match action.get("type").and_then(Value::as_str) {
                Some("search") => {
                    // hosted 响应中的 query/queries 和 action.sources 都是可选字段；若
                    // 存在则检查长度与每个 URL，citation 注释在循环后再合并校验。
                    saw_action = true;
                    let mut queries = Vec::new();
                    if let Some(query) = action.get("query").and_then(Value::as_str) {
                        queries.push(query);
                    }
                    if let Some(values) = action.get("queries").and_then(Value::as_array) {
                        queries.extend(values.iter().filter_map(Value::as_str));
                    }
                    if queries.iter().any(|query| {
                        query.trim().is_empty() || query.chars().count() > self.max_query_chars
                    }) {
                        return Err(ModelError::NativeWebLimitExceeded);
                    }
                    let action_sources = action.get("sources").and_then(Value::as_array);
                    for source in action_sources.into_iter().flatten() {
                        let uri = source
                            .get("url")
                            .or_else(|| source.get("uri"))
                            .and_then(Value::as_str)
                            .ok_or(ModelError::NativeWebArgumentsInvalid)?;
                        self.validate_uri(uri)?;
                        sources.insert(uri.to_owned());
                    }
                }
                Some("open_page" | "find_in_page") => {
                    // 打开/页内查找也属于已完成的 hosted action，但 URL 仍必须经过
                    // 同一 allowlist，不要求再次出现 search action。
                    saw_action = true;
                    let uri = action
                        .get("url")
                        .and_then(Value::as_str)
                        .ok_or(ModelError::NativeWebArgumentsInvalid)?;
                    self.validate_uri(uri)?;
                    sources.insert(uri.to_owned());
                }
                _ => return Err(ModelError::NativeWebArgumentsInvalid),
            }
        }
        // Query and action.sources are optional in hosted responses. Native
        // annotations also carry provenance; a model-written URL alone cannot
        // get here without a completed hosted action.
        if !saw_action {
            return Err(ModelError::NativeWebUnavailable);
        }
        // 只有 provider action 已存在后，模型输出里的 annotation URL 才能进入来源
        // 集合；集合去重后再施加 citation 总量上限。
        sources.extend(self.extract_citations(raw)?.into_iter().map(|c| c.uri));
        if sources.len() > self.max_citations {
            return Err(ModelError::NativeWebLimitExceeded);
        }
        Ok(())
    }

    pub fn extract_citations(&self, raw: &Value) -> Result<Vec<NativeWebCitation>> {
        // BTreeMap 以 URI 去重并按 URI 排序；因此返回顺序是规范化后的稳定顺序，
        // 不承诺等同于 provider 原始嵌套数组的出现顺序。
        // 递归收集 provider raw 中的 url/uri 及其常见元数据，按 URI 合并重复项；
        // 返回前统一执行非空、数量和 HTTPS/host allowlist 校验。
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
        // 来源必须是 HTTPS 且不带用户信息、端口；host 只能精确匹配 allowlist 或
        // 其合法子域名，避免 attacker.example 这类后缀伪装。
        let parsed = reqwest::Url::parse(uri).map_err(|_| ModelError::NativeWebUnsafeCitation {
            uri: uri.to_owned(),
            reason: "invalid URL".to_owned(),
        })?;
        if parsed.scheme() != "https"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !self.allowed_hosts.iter().any(|allowed| {
                parsed
                    .host_str()
                    .is_some_and(|host| host == allowed || host.ends_with(&format!(".{allowed}")))
            })
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
        // 同一 URI 的第一条记录保留已有字段，后续记录只补齐缺失元数据；最终返回
        // 顺序由上层的 URI 映射决定，不在这里承诺输入顺序。
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
    // 不预设 provider 响应的嵌套层级；遇到带 url/uri 的对象先收集，再继续遍历
    // 其所有子值，以覆盖 action.sources 和 message annotations 等位置。
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

#[cfg(test)]
mod hosted_response_tests {
    use super::*;
    #[test]
    fn approved_subdomain_does_not_allow_a_spoofed_suffix() {
        let policy = NativeWebPolicy::default();
        assert!(policy
            .validate_uri("https://www.reuters.com/article")
            .is_ok());
        assert!(policy
            .validate_uri("https://reuters.com.attacker.example/article")
            .is_err());
        assert!(policy
            .validate_uri("https://attackerreuters.com/article")
            .is_err());
    }
    #[test]
    fn hosted_search_with_annotations_does_not_require_optional_query_or_sources() {
        let raw = json!({"output":[
            {"type":"web_search_call","status":"completed","action":{"type":"search"}},
            {"type":"message","content":[{"type":"output_text","text":"Source-backed observation","annotations":[{"type":"url_citation","url":"https://www.reuters.com/markets/example","title":"Example","start_index":0,"end_index":10}]}]}
        ]});
        let policy = NativeWebPolicy::default();
        assert!(policy.validate_provider_response(&raw).is_ok());
        assert_eq!(policy.extract_citations(&raw).unwrap().len(), 1);
    }
    #[test]
    fn hosted_open_page_can_verify_a_known_source_without_searching_again() {
        let raw = json!({"output":[{"type":"web_search_call","status":"completed","action":{"type":"open_page","url":"https://reuters.com/markets/example"}}]});
        assert!(NativeWebPolicy::default()
            .validate_provider_response(&raw)
            .is_ok());
    }
    #[test]
    fn plain_model_text_never_proves_hosted_search() {
        let raw = json!({"output":[{"type":"message","content":[{"type":"output_text","text":"I searched Reuters","annotations":[{"type":"url_citation","url":"https://reuters.com/markets/example"}]}]}]});
        assert!(NativeWebPolicy::default()
            .validate_provider_response(&raw)
            .is_err());
    }
}
