use super::*;

pub(super) fn deliberation_output_schema(result_schema: &Value) -> Value {
    json!({
        "type": "object",
        "required": ["result", "deliberation"],
        "properties": {
            "result": result_schema,
            "deliberation": {
                "type": "object",
                "required": ["selected_path", "alternatives", "uncertainties", "basis_artifact_ids", "confidence_ppm"],
                "properties": {
                    "selected_path": {"type": "string", "maxLength": 1000},
                    "alternatives": {"type": "array", "maxItems": 3, "items": {"type": "string", "maxLength": 500}},
                    "uncertainties": {"type": "array", "maxItems": 3, "items": {"type": "string", "maxLength": 500}},
                    "basis_artifact_ids": {"type": "array", "maxItems": 8, "items": {"type": "string"}},
                    "confidence_ppm": {"type": "integer", "minimum": 0, "maximum": 1000000}
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    })
}

pub(super) fn artifact_id_tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"artifact_id": {"type": "string", "minLength": 1}},
        "required": ["artifact_id"],
        "additionalProperties": false,
    })
}

pub(super) fn evidence_read_tool_specs(store: &V2Store) -> ResearchResult<Vec<ToolSpec>> {
    Ok(vec![ToolSpec {
        name: "read_artifact".to_owned(),
        description: "Read one artifact explicitly granted by ContextManifest.".to_owned(),
        kind: ToolKind::ReadEvidence,
        input_schema: store.put_json(&artifact_id_tool_input_schema())?,
        strict: true,
    }])
}

pub(super) fn retrospective_draft_output_schema() -> Value {
    let reference_kinds = [
        "decision",
        "decision_context",
        "execution_context",
        "execution_verdict",
        "execution_commitment",
        "order_receipt",
        "reconciliation",
        "outcome_schedule",
        "outcome",
        "normalized_evidence",
        "semantic_detail",
        "deliberation_note",
        "retrospective",
    ];
    json!({
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer", "enum": [V2_DOMAIN_SCHEMA_VERSION]},
            "outcome_id": {"type": "string", "minLength": 1},
            "horizon": {"type": "string", "enum": ["t1", "t3", "t5"]},
            "summary": {"type": "string", "maxLength": 4000},
            "findings": {"type": "array", "maxItems": 12, "items": {
                "type": "object",
                "properties": {
                    "category": {"type": "string", "enum": ["research", "evidence", "risk", "decision", "execution", "topology", "contract"]},
                    "conclusion": {"type": "string", "enum": ["worked", "failed", "mixed", "unresolved"]},
                    "statement": {"type": "string", "minLength": 1, "maxLength": 4000},
                    "artifact_refs": {"type": "array", "maxItems": 8, "items": artifact_ref_schema(&reference_kinds)},
                    "confidence_ppm": {"type": "integer", "minimum": 0, "maximum": 1000000}
                },
                "required": ["category", "conclusion", "statement", "artifact_refs", "confidence_ppm"],
                "additionalProperties": false
            }},
            "counterfactuals": {"type": "array", "maxItems": 3, "items": {"type": "string", "maxLength": 4000}},
            "lesson_candidates": {"type": "array", "maxItems": 8, "items": {"type": "string", "maxLength": 4000}},
            "diagnostic_gaps": {"type": "array", "maxItems": 8, "items": {"type": "string", "maxLength": 4000}},
            "source_refs": {"type": "array", "maxItems": 8, "items": artifact_ref_schema(&reference_kinds)},
            "created_at": {"type": "string", "format": "date-time"}
        },
        "required": ["schema_version", "outcome_id", "horizon", "summary", "findings", "counterfactuals", "lesson_candidates", "diagnostic_gaps", "source_refs", "created_at"],
        "additionalProperties": false
    })
}

pub(super) fn planner_draft_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {
                "type": "integer",
                "enum": [V2_SCHEMA_VERSION]
            },
            "topology_id": {"type": "string", "enum": ["active"]},
            "tasks": {
                "type": "object",
                "properties": {},
                "required": [],
                "maxProperties": PLANNER_MAX_DRAFT_TASKS,
                "additionalProperties": planner_draft_task_schema()
            },
            "stop_reason": {
                "type": "string",
                "minLength": 1,
                "maxLength": 1024
            }
        },
        "required": ["schema_version", "topology_id", "tasks"],
        "additionalProperties": false
    })
}

pub(super) fn planner_draft_task_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "recipe_id": {
                "type": "string",
                "enum": PLANNER_CHILD_RECIPE_IDS
            },
            "objective": {
                "type": "string",
                "minLength": 1,
                "maxLength": 2048
            },
            "depends_on": {
                "type": "array",
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128
                },
                "maxItems": PLANNER_MAX_DRAFT_TASKS
            },
            "priority": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100
            },
        "evidence_needs": {
            "type": "array",
            "items": evidence_need_output_schema(),
            "maxItems": PLANNER_MAX_DRAFT_TASKS
        },
        "research_intents": {
            "type": "array",
            "items": research_intent_output_schema(),
            "maxItems": PLANNER_MAX_DRAFT_TASKS
        }
        },
        "required": [
            "recipe_id",
            "objective",
            "depends_on",
            "priority",
            "evidence_needs"
        ],
        "additionalProperties": false
    })
}

pub(super) fn research_intent_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer", "enum": [V2_SCHEMA_VERSION]},
            "source_family": {"type": "string", "enum": GOVERNED_EVIDENCE_SOURCE_FAMILIES},
            "resource": {"type": "string", "minLength": 1, "maxLength": 2048},
            "query": {"type": "string", "minLength": 1, "maxLength": 2000},
            "assets": {
                "type": "array",
                "maxItems": 4,
                "items": {"type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"]}
            },
                    "window_start": {
                        "type": ["string", "null"],
                        "pattern": RFC3339_TIMESTAMP_PATTERN,
                    },
                    "window_end": {
                        "type": ["string", "null"],
                        "pattern": RFC3339_TIMESTAMP_PATTERN,
                    },
            "max_age_secs": {"type": "integer", "maximum": 604800},
            "max_results": {"type": "integer", "maximum": 32}
        },
        "required": [
            "schema_version", "source_family", "resource", "query", "assets",
            "window_start", "window_end", "max_age_secs", "max_results"
        ],
        "additionalProperties": false
    })
}

pub(super) fn evidence_need_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {
                "type": "integer",
                "enum": [V2_SCHEMA_VERSION]
            },
            "source_family": {
                "type": "string",
                "enum": GOVERNED_EVIDENCE_SOURCE_FAMILIES
            },
            "resource": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512
            },
            "max_age_secs": {"type": "integer"}
        },
        "required": ["schema_version", "source_family", "resource", "max_age_secs"],
        "additionalProperties": false
    })
}

pub(super) fn claim_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "statement": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "horizon": { "type": "string", "enum": ["t1", "t3", "t5"] },
            "stance": { "type": "string", "enum": ["bullish", "bearish", "neutral"] },
            "materiality_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "confidence_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "grounds": {
                "type": "array",
                "minItems": 1,
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "evidence_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "topic", "statement", "horizon", "stance", "materiality_ppm",
            "confidence_ppm", "grounds", "evidence_gaps"
        ],
        "additionalProperties": false
    })
}

pub(super) fn critique_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "target": artifact_ref_schema(&["claim"]),
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "severity": { "type": "string", "enum": ["low", "medium", "high"] },
            "blocker": { "type": "boolean" },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "grounds": {
                "type": "array",
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "evidence_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "target", "topic", "severity", "blocker", "rationale", "grounds",
            "evidence_gaps"
        ],
        "additionalProperties": false
    })
}

pub(super) fn resolution_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "claim": artifact_ref_schema(&["claim"]),
            "critique": artifact_ref_schema(&["critique"]),
            "disposition": { "type": "string", "enum": ["accepted", "rebutted", "unresolved"] },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "grounds": {
                "type": "array",
                "minItems": 1,
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "remaining_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "claim", "critique", "disposition", "rationale", "grounds",
            "remaining_gaps"
        ],
        "additionalProperties": false
    })
}

pub(super) fn evidence_ground_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "evidence": artifact_ref_schema(&["normalized_evidence", "semantic_detail"]),
            "support": { "type": "string", "minLength": 1, "maxLength": 2048 }
        },
        "required": ["evidence", "support"],
        "additionalProperties": false
    })
}

pub(super) fn evidence_gap_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 }
        },
        "required": ["topic", "rationale"],
        "additionalProperties": false
    })
}

pub(super) fn decision_proposal_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "summary": { "type": "string", "minLength": 1 },
            "confidence_ppm": { "type": "integer", "minimum": 0, "maximum": 1000000 },
            "forecasts": {
                "type": "array",
                "minItems": 12,
                "maxItems": 12,
                "items": {
                    "type": "object",
                    "properties": {
                        "asset": { "type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"] },
                        "horizon": { "type": "string", "enum": ["t1", "t3", "t5"] },
                        "positive_return_probability_ppm": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 1000000
                        },
                        "expected_return_ppm": { "type": "integer" }
                    },
                    "required": [
                        "asset",
                        "horizon",
                        "positive_return_probability_ppm",
                        "expected_return_ppm"
                    ],
                    "additionalProperties": false
                }
            },
            "claims": { "type": "array", "items": artifact_ref_schema(&["claim"]) },
            "critiques": { "type": "array", "items": artifact_ref_schema(&["critique"]) },
            "evidence": {
                "type": "array",
                "items": artifact_ref_schema(&["normalized_evidence", "semantic_detail"])
            },
            "material_conflicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "claim": artifact_ref_schema(&["claim"]),
                        "critique": artifact_ref_schema(&["critique"]),
                        "topic": { "type": "string", "minLength": 1 },
                        "rationale": { "type": "string", "minLength": 1 }
                    },
                    "required": ["claim", "critique", "topic", "rationale"],
                    "additionalProperties": false
                }
            },
            "hard_blockers": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "unsupported_universe", "no_executable_order", "frozen",
                        "missing_evidence", "invalid_provenance", "material_conflict",
                        "stale_quote", "missing_quote", "stale_account", "missing_account",
                        "market_closed", "factor_limit", "pair_exposure_limit",
                        "turnover_limit", "plan_hash_mismatch", "duplicate_commitment",
                        "non_paper_endpoint", "non_canonical_run", "recovery_incomplete"
                    ]
                }
            },
            "soft_warnings": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "low_confidence", "incomplete_evidence", "elevated_turnover",
                        "slow_model_response", "stale_noncritical_evidence"
                    ]
                }
            }
        },
        "required": [
            "summary", "confidence_ppm", "forecasts", "claims", "critiques",
            "evidence", "material_conflicts", "hard_blockers", "soft_warnings"
        ],
        "additionalProperties": false
    })
}

pub(super) fn artifact_ref_schema(kinds: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "pattern": "^[0-9a-f]{64}$"
            },
            "kind": { "type": "string", "enum": kinds }
        },
        "required": ["artifact_id", "kind"],
        "additionalProperties": false
    })
}
