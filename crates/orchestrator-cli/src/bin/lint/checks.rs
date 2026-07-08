//! Individual lint checks for prompt template files.

use super::{LintIssue, SCHEMA_PLACEHOLDERS, VALID_PLACEHOLDERS};
use anyhow::{Context, Result};
use orchestrator_core::replace_placeholders;
use orchestrator_core::{
    analyst_artifact_schema, final_validation_schema, portfolio_allocation_schema,
    research_artifact_schema, risk_constraints_schema, trade_intent_schema,
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
            if !VALID_PLACEHOLDERS.contains(&placeholder) {
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
    issues: &mut Vec<LintIssue>,
) {
    let checks: &[(&str, &[&str])] = &[
        ("common_ticker_prompt", &["ticker.md"]),
        ("analyst_output_contract", &["analyst_output_contract.md"]),
        ("anti_injection", &["anti_injection.md"]),
        ("research_calibration", &["research_calibration.md"]),
        ("research_dedup", &["research_dedup.md"]),
        ("research_drivers", &["research_drivers.md"]),
        ("leveraged_etf_rules", &["leveraged_etf_rules.md"]),
        ("analyst_output_structure", &["analyst_output_structure.md"]),
        (
            "researcher_body",
            &["researcher_seed.md", "researcher_interaction.md"],
        ),
        ("risk_analyst_body", &["risk_analyst.md"]),
    ];
    for (placeholder, files) in checks {
        let token = format!("{{{placeholder}}}");
        if content.contains(&token) {
            for component_file in *files {
                let component_path = prompts_dir.join("common").join(component_file);
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
}

/// Check 4: render the prompt against a mock state and flag surviving
/// placeholders. A surviving `{known_token}` means the renderer never set a
/// value for it (or set it to an empty/null value).
pub fn check_orphan_placeholders(
    file_path: &Path,
    _content: &str,
    role: &str,
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
        "allocation_context": {"investable_tickers": ["QQQ", "SOXX"]},
    });
    match render_for_lint(
        &state,
        role,
        1,
        "artifact",
        Some(2),
        Some("QQQ-aggregate"),
        Some(file_path),
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

/// Check 7: verify anti-injection guidance reaches roles using read_run_context.
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
        .map(|t| t.iter().any(|v| v.as_str() == Some("read_run_context")))
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
                "Role {role} uses read_run_context but prompt does not include {{anti_injection}}"
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
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let ticker = state
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| tickers.first().map(String::as_str))
        .unwrap_or("");
    let template = if let Some(path) = prompt_path {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt template {}", path.display()))?
    } else {
        "Return only artifact JSON for role {role}, kind {kind}, phase {phase}, and tickers {tickers}. Include per_ticker for every ticker.".to_string()
    };
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let current_controller = current_topic_state
        .get("controller_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked_repeats = current_controller
        .get("blocked_repeats")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let next_agenda = current_controller
        .get("next_agenda")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let common_ticker_prompt_template = common_component(prompt_path, "ticker.md")?;
    let analyst_output_contract_template =
        common_component(prompt_path, "analyst_output_contract.md")?;
    let anti_injection_template = common_component(prompt_path, "anti_injection.md")?;
    let research_calibration_template = common_component(prompt_path, "research_calibration.md")?;
    let research_dedup_template = common_component(prompt_path, "research_dedup.md")?;
    let research_drivers_template = common_component(prompt_path, "research_drivers.md")?;
    let researcher_seed_template = common_component(prompt_path, "researcher_seed.md")?;
    let researcher_interaction_template =
        common_component(prompt_path, "researcher_interaction.md")?;
    let risk_analyst_template = common_component(prompt_path, "risk_analyst.md")?;
    let leveraged_etf_rules_template = common_component(prompt_path, "leveraged_etf_rules.md")?;
    let analyst_output_structure_template =
        common_component(prompt_path, "analyst_output_structure.md")?;
    let component_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
    });
    let common_ticker_prompt =
        replace_placeholders(&common_ticker_prompt_template, &component_values);
    let analyst_output_contract =
        replace_placeholders(&analyst_output_contract_template, &component_values);
    let anti_injection = replace_placeholders(&anti_injection_template, &component_values);
    let research_calibration =
        replace_placeholders(&research_calibration_template, &component_values);
    let research_dedup = replace_placeholders(&research_dedup_template, &component_values);
    let research_drivers = replace_placeholders(&research_drivers_template, &component_values);
    let leveraged_etf_rules =
        replace_placeholders(&leveraged_etf_rules_template, &component_values);
    let analyst_output_structure =
        replace_placeholders(&analyst_output_structure_template, &component_values);
    let (side, side_label, opponent, opponent_label) = researcher_side_params(role);
    let stance_label = risk_stance_label(role);
    let (stance_role_label, stance_intro, stance_rules, stance_schema_extra) =
        risk_stance_fragments(role);
    let values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "ticker": ticker,
        "tickers": tickers.join(","),
        "common_ticker_prompt": common_ticker_prompt,
        "analyst_output_contract": analyst_output_contract,
        "anti_injection": anti_injection,
        "research_calibration": research_calibration,
        "research_dedup": research_dedup,
        "research_drivers": research_drivers,
        "leveraged_etf_rules": leveraged_etf_rules,
        "analyst_output_structure": analyst_output_structure,
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
        "blocked_repeats": serde_json::to_string_pretty(&blocked_repeats)?,
        "next_agenda": serde_json::to_string_pretty(&next_agenda)?,
        "analyst_reports": serde_json::to_string_pretty(&state.get("analyst_reports").cloned().unwrap_or(Value::Null))?,
        "research_plan": serde_json::to_string_pretty(&state.get("research_plan").cloned().unwrap_or(Value::Null))?,
        "trader_plan": serde_json::to_string_pretty(&state.get("trader_investment_plan").cloned().unwrap_or(Value::Null))?,
        "risk_history": serde_json::to_string_pretty(&state.get("risk_debate_state").and_then(|v| v.get("history")).cloned().unwrap_or_else(|| json!([])))?,
        "portfolio_decision": serde_json::to_string_pretty(&state.get("final_trade_decision").cloned().unwrap_or(Value::Null))?,
        "allocation_context": serde_json::to_string_pretty(&state.get("allocation_context").cloned().unwrap_or(Value::Null))?,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    let researcher_body_template = if side.is_empty() {
        String::new()
    } else if role.ends_with(".interaction") {
        researcher_interaction_template
    } else {
        researcher_seed_template
    };
    let researcher_body = replace_placeholders(&researcher_body_template, &values);
    let risk_analyst_body = replace_placeholders(&risk_analyst_template, &values);
    let mut values = values;
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

fn topic_state(state: &Value, topic_id: &str) -> Option<Value> {
    state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .and_then(|items| items.get(topic_id))
        .cloned()
}

fn common_component(prompt_path: Option<&Path>, file_name: &str) -> Result<String> {
    let Some(path) = prompt_path else {
        return Ok(String::new());
    };
    let Some(prompts_dir) = path.parent().and_then(|parent| parent.parent()) else {
        return Ok(String::new());
    };
    let common_path = prompts_dir.join("common").join(file_name);
    if common_path.exists() {
        std::fs::read_to_string(&common_path)
            .with_context(|| format!("failed to read prompt template {}", common_path.display()))
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
    match role {
        "risk.aggressive" => "aggressive",
        "risk.conservative" => "conservative",
        "risk.neutral" => "neutral",
        _ => "",
    }
}

fn risk_stance_fragments(role: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    match role {
        "risk.aggressive" => (
            "激进风险分析师",
            "你的任务是为高回报路径辩护，指出保守和中性视角可能错失的机会，但不能无视已知风险或编造新催化。",
            "2. 指出支持更高风险的 1-3 个最强依据，并说明它们是否已在 analyst_reports 中独立出现。\n3. 明确列出愿意接受的风险，不把风险淡化成机会；若 trader_plan 已很激进，优先建议保持而非继续加码。",
            "\n  \"key_risks_accepted\": [\"接受的风险\"],",
        ),
        "risk.conservative" => (
            "保守风险分析师",
            "你的任务是保护资产、降低波动，指出拟议方案中过度冒险的部分，但不能因为天然保守就否定所有机会。",
            "2. `key_risks` 只列 2-5 个真正会改变执行的风险，区分\u{201c}必须降风险\u{201d}与\u{201c}只需监控\u{201d}。\n3. 若 trader_plan 已经保守，指出无需进一步收缩，避免过度防御。",
            "\n  \"key_risks\": [\"主要风险\"],",
        ),
        "risk.neutral" => (
            "中性风险分析师",
            "你的任务是在激进与保守之间给出平衡观点，评估收益与风险，并给出最少改动的折中方案；既不因单一利好追高，也不因单一风险完全否定方案。",
            "2. `balanced_view` 列出 2-4 条平衡观察，每条都连接到 trader_plan 或 analyst_reports。\n3. 如果证据不足以支持执行，明确建议转为观察，而不是模糊折中。",
            "\n  \"balanced_view\": [\"平衡观察\"],",
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
