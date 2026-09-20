use super::*;

pub(super) fn deliberation_output_schema(result_schema: &Value) -> Value {
    json!({
        "type": "object",
        "required": ["result", "deliberation"],
        "properties": {
            "result": result_schema,
            "deliberation": {
                "type": "object",
                    "required": ["selected_path", "alternatives", "alternative_match_ppm", "uncertainties", "uncertainty_weight_ppm", "basis_artifact_ids", "confidence_ppm"],
                    "properties": {
                        "selected_path": {"type": "string", "maxLength": 1000},
                "alternatives": {"type": "array", "description": "Return at most 3 alternatives.", "maxItems": 3, "items": {"type": "string", "maxLength": 500}},
                "alternative_match_ppm": {"type": "array", "description": "Return exactly one model-assessed score per alternative.", "maxItems": 3, "items": {"type": "integer", "minimum": 0, "maximum": 1000000}},
                "uncertainties": {"type": "array", "description": "Return at most 3 uncertainties.", "maxItems": 3, "items": {"type": "string", "maxLength": 500}},
                "uncertainty_weight_ppm": {"type": "array", "description": "Return exactly one weight per uncertainty; weights sum to 1000000 minus confidence_ppm.", "maxItems": 3, "items": {"type": "integer", "minimum": 0, "maximum": 1000000}},
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

pub(super) fn read_range_tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "artifact_id": {"type": "string", "minLength": 1},
            "start_byte": {"type": "integer", "minimum": 0},
            "end_byte": {"type": "integer", "minimum": 1}
        },
        "required": ["artifact_id", "start_byte", "end_byte"],
        "additionalProperties": false,
    })
}

pub(super) fn search_context_tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1, "maxLength": 256},
            "max_results": {"type": "integer", "minimum": 1, "maximum": 16}
        },
        "required": ["query", "max_results"],
        "additionalProperties": false,
    })
}

pub(super) fn compare_sources_tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "artifact_ids": {
                "type": "array",
                "minItems": 2,
                "maxItems": 4,
                "uniqueItems": true,
                "items": {"type": "string", "minLength": 1}
            }
        },
        "required": ["artifact_ids"],
        "additionalProperties": false,
    })
}

pub(super) fn context_tool_input_schema(name: &str) -> Option<Value> {
    match name {
        "read_artifact" | "read_document" | "read_claim_evidence" => {
            Some(artifact_id_tool_input_schema())
        }
        "read_range" => Some(read_range_tool_input_schema()),
        "search_context" => Some(search_context_tool_input_schema()),
        "compare_sources" => Some(compare_sources_tool_input_schema()),
        _ => None,
    }
}

pub(super) fn evidence_read_tool_specs(store: &Store) -> ResearchResult<Vec<ToolSpec>> {
    [
        (
            "read_document",
            "读取一个由 ContextManifest 明确授权的完整文档。\n",
            artifact_id_tool_input_schema(),
        ),
        (
            "read_range",
            "读取一个已授权文档中的有界字节范围。\n",
            read_range_tool_input_schema(),
        ),
        (
            "search_context",
            "只搜索当前有效 ContextManifest 选中的文档。\n",
            search_context_tool_input_schema(),
        ),
        (
            "read_claim_evidence",
            "读取一个已授权 Claim 及其已授权的 evidence grounds。\n",
            artifact_id_tool_input_schema(),
        ),
        (
            "compare_sources",
            "读取并比较 2 到 4 个已授权的来源文档。\n",
            compare_sources_tool_input_schema(),
        ),
    ]
    .into_iter()
    .map(|(name, description, schema)| {
        Ok(ToolSpec {
            name: name.to_owned(),
            description: description.to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.stage_json(&schema)?,
            strict: true,
        })
    })
    .collect()
}

pub(super) fn retrospective_draft_output_schema() -> Value {
    let reference_kinds = [
        "claim",
        "critique",
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
            "schema_version": {"type": "integer", "enum": [DOMAIN_SCHEMA_VERSION]},
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
            "lesson_candidates": {"type": "array", "maxItems": 0, "items": {"type": "string", "maxLength": 4000}},
            "lesson_proposals": {"type":"array", "maxItems":4, "items": {
                "type":"object", "properties": {
                    "statement":{"type":"string","minLength":1,"maxLength":1000},
                    "recommended_behavior":{"type":"string","minLength":1,"maxLength":1000},
                    "exclusions":{"type":"array","minItems":1,"maxItems":4,"items":{"type":"string","minLength":1,"maxLength":300}},
                    "assets":{"type":"array","minItems":1,"maxItems":4,"uniqueItems":true,"items":{"type":"string","enum":["TQQQ","QQQ","SOXX","SOXL"]}},
                    "horizons":{"type":"array","minItems":1,"maxItems":3,"uniqueItems":true,"items":{"type":"string","enum":["t1","t3","t5"]}},
                    "evidence_refs":{"type":"array","minItems":1,"maxItems":4,"items":artifact_ref_schema(&reference_kinds)}
                },"required":["statement","recommended_behavior","exclusions","assets","horizons","evidence_refs"],"additionalProperties":false
            }},
            "diagnostic_gaps": {"type": "array", "maxItems": 8, "items": {"type": "string", "maxLength": 4000}},
            "source_refs": {"type": "array", "maxItems": 8, "items": artifact_ref_schema(&reference_kinds)},
            "created_at": {"type": "string", "pattern": RFC3339_TIMESTAMP_PATTERN}
        },
        "required": ["schema_version", "outcome_id", "horizon", "summary", "findings", "counterfactuals", "lesson_candidates", "lesson_proposals", "diagnostic_gaps", "source_refs", "created_at"],
        "additionalProperties": false
    })
}

pub(super) fn research_intent_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer", "enum": [DOMAIN_SCHEMA_VERSION]},
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

pub(super) fn claim_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [DOMAIN_SCHEMA_VERSION] },
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "statement": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "horizon": { "type": "string", "enum": ["t1", "t3", "t5"] },
            "stance": { "type": "string", "enum": ["bullish", "bearish", "neutral"] },
            "materiality_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "confidence_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "grounds": {
                "type": "array",
                "minItems": 1,
                "maxItems": 12,
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
            "schema_version": { "type": "integer", "enum": [DOMAIN_SCHEMA_VERSION] },
            "target": artifact_ref_schema(&["claim"]),
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "severity": { "type": "string", "enum": ["low", "medium", "high"] },
            "blocker": { "type": "boolean" },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "verification_status": {
                "type": "string",
                "enum": ["supported", "contradicted", "not_enough_information"]
            },
            "supporting_refs": {
                "type": "array",
                "maxItems": 12,
                "items": claim_verification_evidence_schema()
            },
            "conflicting_refs": {
                "type": "array",
                "maxItems": 12,
                "items": claim_verification_evidence_schema()
            },
            "grounds": {
                "type": "array",
                "maxItems": 12,
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
            "evidence_gaps", "verification_status", "supporting_refs", "conflicting_refs"
        ],
        "additionalProperties": false
    })
}

fn claim_verification_evidence_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "evidence": artifact_ref_schema(&["normalized_evidence", "semantic_detail"]),
            "authority": {
                "type": "string",
                "enum": ["primary", "official", "established_secondary", "unrated"]
            },
            "temporal_validity": {
                "type": "string",
                "enum": ["valid_at_decision_cutoff", "stale", "unknown"]
            }
        },
        "required": ["evidence", "authority", "temporal_validity"],
        "additionalProperties": false
    })
}

pub(super) fn resolution_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [DOMAIN_SCHEMA_VERSION] },
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
            "support": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "role": { "type": "string", "enum": ["descriptive", "directional"] },
            "assets": {
                "type": "array",
                "maxItems": 4,
                "items": { "type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"] }
            },
            "domain": {
                "type": ["string", "null"],
                "enum": [
                    "price_market_structure",
                    "macro",
                    "fundamentals_semiconductor",
                    "news_event",
                    null
                ]
            }
        },
        "required": ["evidence", "support", "role", "assets", "domain"],
        "additionalProperties": false
    })
}

pub(super) fn evidence_gap_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "impact": { "type": "string", "enum": ["warning", "blocks_directional_forecast"] },
            "assets": { "type": "array", "maxItems": 4, "uniqueItems": true, "items": { "type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"] } },
            "horizons": { "type": "array", "maxItems": 3, "uniqueItems": true, "items": { "type": "string", "enum": ["t1", "t3", "t5"] } },
            "retriable": { "type": "boolean" },
            "supplemental_needs": {
                "type": "array",
                "maxItems": 8,
                "items": research_intent_output_schema()
            }
        },
        "required": ["topic", "rationale", "impact", "assets", "horizons", "supplemental_needs", "retriable"],
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
                        "expected_return_ppm": { "type": "integer" },
                        "thesis": {
                            "type": "object",
                            "properties": {
                                "thesis_valid_until": { "type": "string", "minLength": 20, "pattern": RFC3339_TIMESTAMP_PATTERN, "description": "Thesis expiry as a full RFC3339 timestamp with timezone, never a date-only value. This timestamp is not a trading-session count." },
                                "expected_holding_period_days": { "type": "integer", "enum": [1, 3, 5] },
                                "exit_condition": { "type": "string", "minLength": 1 },
                                "invalidation_conditions": {
                                    "type": "array", "minItems": 1,
                                    "items": { "type": "string", "minLength": 1 }
                                }
                            },
                            "required": [
                                "thesis_valid_until", "expected_holding_period_days",
                                "exit_condition", "invalidation_conditions"
                            ],
                            "additionalProperties": false
                        }
                    },
                    "required": [
                        "asset",
                        "horizon",
                        "positive_return_probability_ppm",
                        "expected_return_ppm",
                        "thesis"
                    ],
                    "additionalProperties": false
                }
            },
            "research_allocation": {
                "type": "object",
                "description": "Research-only target composition. It is not an order or execution permit.",
                "properties": {
                    "cash_weight_ppm": { "type": "integer", "minimum": 0, "maximum": 1000000 },
                    "allocations": {
                        "type": "array",
                        "minItems": 4,
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "properties": {
                                "asset": { "type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"] },
                                "target_weight_ppm": { "type": "integer", "minimum": 0, "maximum": 1000000 },
                                "supporting_horizons": { "type": "array", "maxItems": 3, "uniqueItems": true, "items": { "type": "string", "enum": ["t1", "t3", "t5"] } },
                                "evidence_refs": { "type": "array", "items": artifact_ref_schema(&["claim", "critique", "normalized_evidence", "semantic_detail"]) },
                                "rationale": { "type": "string", "minLength": 1 },
                                "abstention_reason": { "type": ["string", "null"] }
                            },
                            "required": ["asset", "target_weight_ppm", "supporting_horizons", "evidence_refs", "rationale", "abstention_reason"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["cash_weight_ppm", "allocations"],
                "additionalProperties": false
            },
            "claims": { "type": "array", "items": artifact_ref_schema(&["claim"]) },
            "critiques": { "type": "array", "items": artifact_ref_schema(&["critique"]) },
    "evidence": {
      "type": "array",
      "items": artifact_ref_schema(&["normalized_evidence", "semantic_detail"])
    },
    "applied_learning_refs": {
      "type": "array",
      "items": artifact_ref_schema(&["lesson", "experience", "candidate_policy"])
    },
    "rejected_learning_refs": {
      "type": "array",
      "items": artifact_ref_schema(&["lesson", "experience", "candidate_policy"])
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
            "summary", "confidence_ppm", "forecasts", "research_allocation", "claims", "critiques",
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
