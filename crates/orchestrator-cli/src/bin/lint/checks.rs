//! Individual lint checks for prompt template files.

use super::{LintIssue, SCHEMA_PLACEHOLDERS, VALID_PLACEHOLDERS};
use anyhow::{Context, Result};
use orchestrator_core::{
    analyst_artifact_schema, final_validation_schema, portfolio_allocation_schema,
    replace_placeholders, research_artifact_schema, risk_constraints_schema, trade_intent_schema,
    ComponentRegistry,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

fn placeholder_regex() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(r"\{([a-z_][a-z0-9_]*)\}").expect("placeholder regex is valid")
    })
}

/// Check 1: every `{placeholder}` in the file is a key the renderer provides.
///
/// Lines inside fenced code blocks (``` ... ```) are skipped so JSON example
/// fields like `{"direction": ...}` do not produce false positives.
pub fn check_placeholder_completeness(
    file_path: &Path,
    content: &str,
    component_registry: &ComponentRegistry,
    issues: &mut Vec<LintIssue>,
) {
    let re = placeholder_regex();
    let mut in_fence = false;
    for (line_num, line) in content.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        for cap in re.captures_iter(line) {
            let placeholder = cap.get(1).unwrap().as_str();
            if !VALID_PLACEHOLDERS.contains(&placeholder)
                && !component_registry.has_enabled_placeholder(placeholder)
            {
                issues.push(LintIssue {
                    file: file_path.display().to_string(),
                    line: Some(line_num + 1),
                    severity: "error".to_string(),
                    check: "placeholder_completeness".to_string(),
                    message: format!(
                        "Placeholder {{{placeholder}}} is not in the renderer values map"
                    ),
                });
            }
        }
    }
}

/// Check 2: `{..._schema}` placeholders reference known schema functions.
pub fn check_schema_references(file_path: &Path, content: &str, issues: &mut Vec<LintIssue>) {
    let re = placeholder_regex();
    for (line_num, line) in content.lines().enumerate() {
        for cap in re.captures_iter(line) {
            let placeholder = cap.get(1).unwrap().as_str();
            if placeholder.ends_with("_schema") && !SCHEMA_PLACEHOLDERS.contains(&placeholder) {
                issues.push(LintIssue {
                    file: file_path.display().to_string(),
                    line: Some(line_num + 1),
                    severity: "error".to_string(),
                    check: "schema_reference".to_string(),
                    message: format!(
                        "Placeholder {{{placeholder}}} resembles a schema reference but is not a known schema function"
                    ),
                });
            }
        }
    }
}

/// Check 3: common component includes resolve to existing files.
pub fn check_common_components(
    file_path: &Path,
    content: &str,
    prompts_dir: &Path,
    component_registry: &ComponentRegistry,
    issues: &mut Vec<LintIssue>,
) {
    let checks: &[(&str, &[&str])] = &[
        (
            "analyst_output_contract",
            &["common/analyst_output_contract.md"],
        ),
        ("anti_injection", &["common/anti_injection.md"]),
        ("research_calibration", &["common/research_calibration.md"]),
        ("research_drivers", &["common/research_drivers.md"]),
        ("analysis_trace_contract", &["common/analysis_trace.md"]),
        ("leveraged_etf_rules", &["common/leveraged_etf_rules.md"]),
        // researcher prompts are standalone; {researcher_body} is only a
        // compatibility placeholder and no longer expands a common component.
        ("risk_analyst_body", &["phase5/risk_analyst.md"]),
    ];
    for (placeholder, files) in checks {
        let token = format!("{{{placeholder}}}");
        if content.contains(&token) {
            for component_file in *files {
                let component_path = prompts_dir.join(component_file);
                if !component_path.exists() {
                    issues.push(LintIssue {
                        file: file_path.display().to_string(),
                        line: None,
                        severity: "error".to_string(),
                        check: "common_component_existence".to_string(),
                        message: format!(
                            "Prompt references {{{placeholder}}} but prompts/common/{component_file} does not exist"
                        ),
                    });
                }
            }
        }
    }
    {
        let placeholder = "common_ticker_prompt";
        let token = format!("{{{placeholder}}}");
        if content.contains(&token) && !component_registry.has_enabled_placeholder(placeholder) {
            issues.push(LintIssue {
                file: file_path.display().to_string(),
                line: None,
                severity: "error".to_string(),
                check: "component_existence".to_string(),
                message: format!(
                    "Prompt references {{{placeholder}}} but no enabled prompt component provides it"
                ),
            });
        }
    }
}

/// Check 4: render the prompt against a mock state and flag surviving
/// placeholders. A surviving `{known_token}` means the renderer never set a
/// value for it (or set it to an empty/null value).
pub fn check_orphan_placeholders(
    file_path: &Path,
    _content: &str,
    role: &str,
    component_registry: &ComponentRegistry,
    issues: &mut Vec<LintIssue>,
) {
    let state = json!({
        "ticker": "QQQ",
        "tickers": ["QQQ", "SOXX", "VIX"],
        "current_date": "2026-07-06",
        "window_days": 5,
        "lang": "zh",
        "run_id": "lint-check",
        "analyst_reports": {"analyst.technical": {"per_ticker": {}}},
        "research_plan": {"rating": "Hold"},
        "trader_investment_plan": {"action": "Hold"},
        "risk_debate_state": {"history": []},
        "final_trade_decision": {"rating": "Hold"},
        "allocation_context": {"investable_assets": ["QQQ", "SOXX"]},
        "reflection_task": {"task_id": 1},
    });
    match render_for_lint(
        &state,
        role,
        1,
        "artifact",
        Some(2),
        Some("QQQ-aggregate"),
        Some(file_path),
        component_registry,
    ) {
        Ok(rendered) => {
            let re = placeholder_regex();
            for cap in re.captures_iter(&rendered) {
                let placeholder = cap.get(1).unwrap().as_str();
                if VALID_PLACEHOLDERS.contains(&placeholder) {
                    issues.push(LintIssue {
                        file: file_path.display().to_string(),
                        line: None,
                        severity: "error".to_string(),
                        check: "orphan_placeholder".to_string(),
                        message: format!(
                            "Placeholder {{{placeholder}}} survived rendering — value may be null or empty"
                        ),
                    });
                }
            }
        }
        Err(e) => {
            issues.push(LintIssue {
                file: file_path.display().to_string(),
                line: None,
                severity: "error".to_string(),
                check: "render_failure".to_string(),
                message: format!("Failed to render prompt: {e}"),
            });
        }
    }
}

/// Check 5: warn when a prompt file exceeds the recommended token budget.
pub fn check_file_size(file_path: &Path, content: &str, issues: &mut Vec<LintIssue>) {
    let token_estimate = orchestrator_core::token::estimate_tokens(content);
    if token_estimate > 2000 {
        issues.push(LintIssue {
            file: file_path.display().to_string(),
            line: None,
            severity: "warning".to_string(),
            check: "file_size".to_string(),
            message: format!(
                "Prompt file is ~{token_estimate} tokens, exceeds recommended 2000 token limit"
            ),
        });
    }
}

/// Check 6: flag large duplicated text blocks across prompt files.
pub fn check_duplicate_content(files: &[(PathBuf, String)], issues: &mut Vec<LintIssue>) {
    const MIN_BLOCK_SIZE: usize = 200;
    let mut blocks: Vec<(String, PathBuf)> = Vec::new();
    for (path, content) in files {
        for paragraph in content.split("\n\n") {
            let trimmed = paragraph.trim();
            if trimmed.len() >= MIN_BLOCK_SIZE {
                blocks.push((trimmed.to_string(), path.clone()));
            }
        }
    }
    for i in 0..blocks.len() {
        for j in (i + 1)..blocks.len() {
            let (block_a, path_a) = &blocks[i];
            let (block_b, path_b) = &blocks[j];
            if path_a == path_b {
                continue;
            }
            let similarity = string_similarity(block_a, block_b);
            if similarity > 0.8 {
                issues.push(LintIssue {
                    file: path_b.display().to_string(),
                    line: None,
                    severity: "info".to_string(),
                    check: "duplicate_content".to_string(),
                    message: format!(
                        "Large text block ({} chars) similar to {} ({:.0}% match)",
                        block_b.len(),
                        path_a.display(),
                        similarity * 100.0
                    ),
                });
            }
        }
    }
}

/// Check 7: verify anti-injection guidance reaches roles using phase-summary tools.
pub fn check_anti_injection(
    file_path: &Path,
    content: &str,
    role: &str,
    config: &Value,
    issues: &mut Vec<LintIssue>,
) {
    if role.is_empty() {
        return;
    }
    let tools =
        orchestrator_core::config_get(config, &format!("orchestrator.llm.roles.{role}.tools"))
            .and_then(Value::as_array);
    let has_read_context = tools
        .map(|tools| {
            tools.iter().any(|value| {
                matches!(
                    value.as_str(),
                    Some("read_phase_summaries" | "read_phase_summary_details")
                )
            })
        })
        .unwrap_or(false);
    if !has_read_context {
        return;
    }
    // {researcher_body} and {risk_analyst_body} expand to shared components that
    // already embed {anti_injection}, so they count as covered.
    let covered = content.contains("{anti_injection}")
        || content.contains("{researcher_body}")
        || content.contains("{risk_analyst_body}");
    if !covered {
        issues.push(LintIssue {
            file: file_path.display().to_string(),
            line: None,
            severity: "info".to_string(),
            check: "anti_injection_presence".to_string(),
            message: format!(
                "Role {role} uses phase-summary tools but prompt does not include {{anti_injection}}"
            ),
        });
    }
}

// ===========================================================================
// Render approximation
//
// `render_prompt` in orchestrator-workflow is `pub(crate)` and therefore not
// reachable from this crate. This function mirrors its two-pass replacement
// pipeline using the public `replace_placeholders` helper and the public schema
// functions so the orphan-placeholder check stays faithful to production
// rendering. Keep this in sync with
// `crates/orchestrator-workflow/src/orchestration/render.rs::render_prompt`.
// ===========================================================================

#[allow(clippy::too_many_arguments)]
fn render_for_lint(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&Path>,
    component_registry: &ComponentRegistry,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let ticker = state
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| tickers.first().map(String::as_str))
        .unwrap_or("");
    let path = prompt_path.context("missing prompt path for lint rendering")?;
    let template = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read prompt template {}", path.display()))?;
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let analyst_output_contract_template =
        prompt_component(prompt_path, "common/analyst_output_contract.md")?;
    let anti_injection_template = prompt_component(prompt_path, "common/anti_injection.md")?;
    let research_calibration_template =
        prompt_component(prompt_path, "common/research_calibration.md")?;
    let research_drivers_template = prompt_component(prompt_path, "common/research_drivers.md")?;
    let analysis_trace_contract_template =
        prompt_component(prompt_path, "common/analysis_trace.md")?;
    let risk_analyst_template = prompt_component(prompt_path, "phase5/risk_analyst.md")?;
    let leveraged_etf_rules_template =
        prompt_component(prompt_path, "common/leveraged_etf_rules.md")?;
    let component_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "role": role,
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
    });
    let analyst_output_contract =
        replace_placeholders(&analyst_output_contract_template, &component_values);
    let anti_injection = replace_placeholders(&anti_injection_template, &component_values);
    let research_calibration =
        replace_placeholders(&research_calibration_template, &component_values);
    let research_drivers = replace_placeholders(&research_drivers_template, &component_values);
    let analysis_trace_contract =
        replace_placeholders(&analysis_trace_contract_template, &component_values);
    let leveraged_etf_rules = if contains_leveraged_etf(&tickers) {
        replace_placeholders(&leveraged_etf_rules_template, &component_values)
    } else {
        String::new()
    };
    let (side, side_label, opponent, opponent_label) = researcher_side_params(role);
    let stance_label = risk_stance_label(role);
    let (stance_role_label, stance_intro, stance_rules, stance_schema_extra) =
        risk_stance_fragments(role);
    let mut values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "ticker": ticker,
        "tickers": tickers.join(","),
        "common_ticker_prompt": "",
        "analyst_output_contract": analyst_output_contract,
        "anti_injection": anti_injection,
        "research_calibration": research_calibration,
        "research_drivers": research_drivers,
        "analysis_trace_contract": analysis_trace_contract,
        "leveraged_etf_rules": leveraged_etf_rules,
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
        "side": side,
        "side_label": side_label,
        "opponent": opponent,
        "opponent_label": opponent_label,
        "stance": stance_label,
        "stance_label": stance_role_label,
        "stance_intro": stance_intro,
        "stance_rules": stance_rules,
        "stance_schema_extra": stance_schema_extra,
        "date": state.get("current_date").and_then(Value::as_str).unwrap_or(""),
        "lang": state.get("lang").and_then(Value::as_str).unwrap_or("zh"),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "role": role,
        "phase": phase,
        "kind": kind,
        "round": round.unwrap_or_default(),
        "topic_id": topic_id.unwrap_or(""),
        "topic": serde_json::to_string_pretty(&current_topic)?,
        "analyst_reports": serde_json::to_string_pretty(&state.get("analyst_reports").cloned().unwrap_or(Value::Null))?,
        "research_plan": serde_json::to_string_pretty(&state.get("research_plan").cloned().unwrap_or(Value::Null))?,
        "trader_plan": serde_json::to_string_pretty(&state.get("trader_investment_plan").cloned().unwrap_or(Value::Null))?,
        "risk_history": serde_json::to_string_pretty(&state.get("risk_debate_state").and_then(|v| v.get("history")).cloned().unwrap_or_else(|| json!([])))?,
        "portfolio_decision": serde_json::to_string_pretty(&state.get("final_trade_decision").cloned().unwrap_or(Value::Null))?,
        "allocation_context": serde_json::to_string_pretty(&state.get("allocation_context").cloned().unwrap_or(Value::Null))?,
        "reflection_task": serde_json::to_string_pretty(&state.get("reflection_task").cloned().unwrap_or(Value::Null))?,
        "phase3_context": "{}",
        "risk_context": "{}",
        "portfolio_context": "{}",
        "alpaca_mode": "disabled",
        "phase1_index": "{}",
        "prior_phase_summaries": "{\"items\":[]}",
        "common_ground": "{}",
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    component_registry.render_for_role(role, &mut values)?;
    if template.contains("{common_ticker_prompt}")
        && values
            .get("common_ticker_prompt")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        let path = prompt_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<inline prompt>".to_string());
        anyhow::bail!(
            "prompt {path} references {{common_ticker_prompt}} but no enabled ticker component injected it for role {role}"
        );
    }
    // Researcher prompts are standalone markdown files; keep researcher_body
    // empty for placeholder compatibility only.
    let researcher_body = String::new();
    let risk_analyst_body = replace_placeholders(&risk_analyst_template, &values);
    if let Some(map) = values.as_object_mut() {
        map.insert(
            "researcher_body".to_string(),
            Value::String(researcher_body),
        );
        map.insert(
            "risk_analyst_body".to_string(),
            Value::String(risk_analyst_body),
        );
    }
    Ok(replace_placeholders(&template, &values))
}

fn tickers_from_state(state: &Value) -> Vec<String> {
    state
        .get("tickers")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn contains_leveraged_etf(tickers: &[String]) -> bool {
    tickers.iter().any(|ticker| {
        matches!(
            ticker.trim().to_ascii_uppercase().as_str(),
            "TQQQ" | "SQQQ" | "SOXL" | "SOXS" | "UPRO" | "SPXU"
        )
    })
}

fn topic_state(state: &Value, topic_id: &str) -> Option<Value> {
    state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .and_then(|items| items.get(topic_id))
        .cloned()
}

fn prompt_component(prompt_path: Option<&Path>, relative_path: &str) -> Result<String> {
    let Some(path) = prompt_path else {
        return Ok(String::new());
    };
    let Some(prompts_dir) = path.parent().and_then(|parent| parent.parent()) else {
        return Ok(String::new());
    };
    let component_path = prompts_dir.join(relative_path);
    if component_path.exists() {
        std::fs::read_to_string(&component_path).with_context(|| {
            format!(
                "failed to read prompt template {}",
                component_path.display()
            )
        })
    } else {
        Ok(String::new())
    }
}

fn researcher_side_params(role: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    if role.starts_with("researcher.bull") {
        ("bull", "看多", "bear", "看空")
    } else if role.starts_with("researcher.bear") {
        ("bear", "看空", "bull", "看多")
    } else {
        ("", "", "", "")
    }
}

fn risk_stance_label(role: &str) -> &'static str {
    if role == "risk.conservative" {
        "conservative"
    } else {
        ""
    }
}

fn risk_stance_fragments(role: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    match role {
        "risk.conservative" => (
            "保守风险分析师",
            "你的任务是保护资产、降低波动，指出拟议方案中过度冒险的部分，但不能因为天然保守就否定所有机会。",
            "2. `key_risks` 只列 2-5 个真正会改变执行的风险，区分\u{201c}必须降风险\u{201d}与\u{201c}只需监控\u{201d}。\n3. 若 trader_plan 已经保守，指出无需进一步收缩，避免过度防御。",
            "\n  \"key_risks\": [\"主要风险\"],",
        ),
        _ => ("", "", "", ""),
    }
}

/// Word-trigram Jaccard similarity in [0, 1].
fn string_similarity(a: &str, b: &str) -> f64 {
    let shingles_a = word_trigrams(a);
    let shingles_b = word_trigrams(b);
    if shingles_a.is_empty() && shingles_b.is_empty() {
        return 1.0;
    }
    let intersection = shingles_a.intersection(&shingles_b).count();
    let union = shingles_a.union(&shingles_b).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn word_trigrams(text: &str) -> HashSet<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < 3 {
        return words.iter().map(|s| (*s).to_string()).collect();
    }
    (0..=words.len() - 3)
        .map(|i| format!("{} {} {}", words[i], words[i + 1], words[i + 2]))
        .collect()
}
