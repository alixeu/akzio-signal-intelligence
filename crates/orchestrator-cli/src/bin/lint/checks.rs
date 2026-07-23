//! Individual lint checks for prompt template files.

use super::{LintIssue, SCHEMA_PLACEHOLDERS, VALID_PLACEHOLDERS};
use anyhow::{Context, Result};
use orchestrator_core::ComponentRegistry;
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
        ("research_calibration", &["common/research_calibration.md"]),
        ("research_drivers", &["common/research_drivers.md"]),
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
            let token_estimate = orchestrator_core::token::estimate_tokens(&rendered);
            if token_estimate > 2000 {
                issues.push(LintIssue {
                    file: file_path.display().to_string(),
                    line: None,
                    severity: "warning".to_string(),
                    check: "rendered_prompt_size".to_string(),
                    message: format!(
                        "Rendered {role} prompt is ~{token_estimate} tokens, exceeds recommended 2000 token limit"
                    ),
                });
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

/// Check 5: retain source-file diagnostics for unusually large templates.
/// The actionable prompt budget is checked after full production rendering.
pub fn check_file_size(file_path: &Path, content: &str, issues: &mut Vec<LintIssue>) {
    let token_estimate = orchestrator_core::token::estimate_tokens(content);
    if token_estimate > 2000 {
        issues.push(LintIssue {
            file: file_path.display().to_string(),
            line: None,
            severity: "warning".to_string(),
            check: "file_size".to_string(),
            message: format!(
                "Prompt source file is ~{token_estimate} tokens, exceeds recommended 2000 token limit; rendered prompt size is checked separately"
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
// Production renderer bridge
//
// Prompt lint uses the production rendering path so placeholder checks cannot
// drift from runtime component composition.
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
    let prompt_path = prompt_path.context("missing prompt path for lint rendering")?;
    orchestrator_workflow::orchestration::render::render_prompt_for_lint(
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        prompt_path,
        component_registry,
    )
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
