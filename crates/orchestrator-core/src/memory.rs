use serde::{Deserialize, Serialize};

use crate::evaluation::{DocumentRef, PolicyRef};

pub const MEMORY_USAGE_EVENT_SCHEMA_VERSION: u32 = 1;
pub const MEMORY_USAGE_REPORT_SCHEMA_VERSION: u32 = 1;
pub const MEMORY_ATTRIBUTION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryUsageEventKind {
    Search,
    Expand,
    Application,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryApplicationDisposition {
    Applied,
    Rejected,
}

/// Rust-observed memory access. It records what the runtime actually returned
/// or expanded; it deliberately does not claim the model applied a rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryUsageEventV1 {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: String,
    pub kind: MemoryUsageEventKind,
    pub role: String,
    pub phase: u8,
    pub ticker: Option<String>,
    pub unit_key: String,
    pub lexical_query: Option<String>,
    pub retrieved_pattern_ids: Vec<String>,
    pub expanded_pattern_id: Option<String>,
    pub retrieval_stop_reason: Option<String>,
    /// A model claim made through a Rust-observed tool call. It is distinct
    /// from actual retrieval and must never be treated as outcome utility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_disposition: Option<MemoryApplicationDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_reason: Option<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryUsageReportV1 {
    pub schema_version: u32,
    pub report_id: String,
    pub run_id: String,
    pub events: Vec<MemoryUsageEventV1>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryUsageReferenceV1 {
    pub report_ref: DocumentRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryAttributionLabel {
    Helpful,
    Harmful,
    Irrelevant,
    Unverifiable,
}

/// Attribution is deliberately evidence-first. Helpful/Harmful require a
/// future controlled-evaluation policy; materializers must default to
/// Unverifiable rather than infer causality from one profitable outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryAttributionItemV1 {
    pub pattern_id: String,
    pub label: MemoryAttributionLabel,
    pub reason: String,
    pub usage_event_refs: Vec<DocumentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryAttributionRecordV1 {
    pub schema_version: u32,
    pub attribution_id: String,
    pub outcome_ref: DocumentRef,
    pub decision_ref: DocumentRef,
    pub memory_usage_report_ref: DocumentRef,
    pub policy_ref: PolicyRef,
    pub items: Vec<MemoryAttributionItemV1>,
    pub created_at: String,
    pub content_hash: String,
}
