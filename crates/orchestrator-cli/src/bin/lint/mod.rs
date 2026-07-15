//! Prompt lint data structures and orchestration.

use anyhow::Result;
use orchestrator_core::ComponentRegistry;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

mod checks;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintReport {
    pub summary: LintSummary,
    pub issues: Vec<LintIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintSummary {
    pub files_checked: usize,
    pub errors: usize,
    pub warnings: usize,
    pub info: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintIssue {
    pub file: String,
    pub line: Option<usize>,
    pub severity: String,
    pub check: String,
    pub message: String,
}

/// All placeholders that `replace_placeholders()` can resolve.
///
/// Must be kept in sync with the `values` map in
/// `crates/orchestrator-workflow/src/orchestration/render.rs::render_prompt`.
pub const VALID_PLACEHOLDERS: &[&str] = &[
    "run_id",
    "ticker",
    "tickers",
    "common_ticker_prompt",
    "analyst_output_contract",
    "anti_injection",
    "research_calibration",
    "research_drivers",
    "leveraged_etf_rules",
    "analyst_output_structure",
    "analyst_artifact_schema",
    "research_artifact_schema",
    "trade_intent_schema",
    "risk_constraints_schema",
    "final_validation_schema",
    "portfolio_allocation_schema",
    "side",
    "side_label",
    "opponent",
    "opponent_label",
    "stance",
    "stance_label",
    "stance_intro",
    "stance_rules",
    "stance_schema_extra",
    "date",
    "lang",
    "window_days",
    "role",
    "phase",
    "kind",
    "round",
    "topic_id",
    "topic",
    "blocked_repeats",
    "next_agenda",
    "analyst_reports",
    "research_plan",
    "trader_plan",
    "risk_history",
    "portfolio_decision",
    "allocation_context",
    "workflow_pattern",
    "researcher_body",
    "risk_analyst_body",
];

/// Schema placeholders backed by a schema function in `orchestrator-core::artifact`.
pub const SCHEMA_PLACEHOLDERS: &[&str] = &[
    "analyst_artifact_schema",
    "research_artifact_schema",
    "trade_intent_schema",
    "risk_constraints_schema",
    "final_validation_schema",
    "portfolio_allocation_schema",
];

/// Shared component files under `prompts/common/`.
#[allow(dead_code)]
pub const COMMON_COMPONENTS: &[&str] = &[
    "anti_injection.md",
    "analyst_output_contract.md",
    "leveraged_etf_rules.md",
    "analyst_output_structure.md",
    "research_calibration.md",
    "research_drivers.md",
    "risk_analyst.md",
];

pub fn run_all_checks(prompts_dir: &Path) -> Result<LintReport> {
    let config = orchestrator_cli::cli_config::load_default_config()
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
    let mut issues = Vec::new();
    let mut files_checked = 0;

    let mut prompt_files: Vec<(PathBuf, String)> = Vec::new();
    collect_md_files(prompts_dir, &mut prompt_files)?;
    let component_registry = ComponentRegistry::discover(prompts_dir)?;
    component_registry.validate_required_variables()?;

    for (path, content) in &prompt_files {
        files_checked += 1;
        let role = infer_role_from_path(path, prompts_dir);
        checks::check_placeholder_completeness(path, content, &component_registry, &mut issues);
        checks::check_schema_references(path, content, &mut issues);
        checks::check_common_components(
            path,
            content,
            prompts_dir,
            &component_registry,
            &mut issues,
        );
        if !role.is_empty() {
            checks::check_orphan_placeholders(
                path,
                content,
                &role,
                &component_registry,
                &mut issues,
            );
        }
        checks::check_file_size(path, content, &mut issues);
        checks::check_anti_injection(path, content, &role, &config, &mut issues);
    }
    checks::check_duplicate_content(&prompt_files, &mut issues);

    // Sort issues for deterministic output (by file, then line, then severity).
    issues.sort_by(|a, b| {
        a.file
            .cmp(&b.file)
            .then(a.line.cmp(&b.line))
            .then(a.severity.cmp(&b.severity))
    });

    let errors = issues.iter().filter(|i| i.severity == "error").count();
    let warnings = issues.iter().filter(|i| i.severity == "warning").count();
    let info = issues.iter().filter(|i| i.severity == "info").count();

    Ok(LintReport {
        summary: LintSummary {
            files_checked,
            errors,
            warnings,
            info,
        },
        issues,
    })
}

pub fn print_text_report(report: &LintReport) {
    let s = &report.summary;
    println!(
        "Prompt lint: {} files checked, {} errors, {} warnings, {} info",
        s.files_checked, s.errors, s.warnings, s.info
    );
    if report.issues.is_empty() {
        println!("No issues found.");
        return;
    }
    for issue in &report.issues {
        let loc = match issue.line {
            Some(line) => format!("{}:{}", issue.file, line),
            None => issue.file.clone(),
        };
        println!(
            "[{}] {} ({}): {}",
            issue.severity.to_uppercase(),
            loc,
            issue.check,
            issue.message
        );
    }
}

fn collect_md_files(dir: &Path, out: &mut Vec<(PathBuf, String)>) -> Result<()> {
    if !dir.exists() {
        anyhow::bail!("prompts directory does not exist: {}", dir.display());
    }
    let mut entries: Vec<PathBuf> = Vec::new();
    collect_paths(dir, &mut entries);
    entries.sort();
    for path in entries {
        if path.extension().is_none_or(|ext| ext != "md") {
            continue;
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;
        out.push((path, content));
    }
    Ok(())
}

fn collect_paths(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_paths(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Map a prompt file path to the role string the renderer would use for it.
/// Common component files map to an empty string (no standalone role).
fn infer_role_from_path(path: &Path, prompts_dir: &Path) -> String {
    let rel = path.strip_prefix(prompts_dir).unwrap_or(path);
    let category = rel
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .unwrap_or("");
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    match category {
        "analysts" => format!("analyst.{stem}"),
        "researchers" => {
            let mut tokens: Vec<&str> = stem.split('_').collect();
            if tokens.last() == Some(&"monitor") {
                tokens.pop();
            }
            if tokens.len() >= 2 {
                format!("researcher.{}.{}", tokens[0], tokens[1..].join("."))
            } else {
                format!("researcher.{stem}")
            }
        }
        "managers" => match stem {
            "research_manager" => "manager.research".to_string(),
            "portfolio_manager" => "portfolio.manager".to_string(),
            other => format!("manager.{other}"),
        },
        "mediators" => match stem {
            "topic_generation" => "mediator.topic".to_string(),
            "topic_controller" => "mediator.topic_controller".to_string(),
            other => format!("mediator.{other}"),
        },
        "risk" => format!("risk.{stem}"),
        "traders" => "trader".to_string(),
        "allocation" => "allocation.manager".to_string(),
        _ => String::new(),
    }
}
