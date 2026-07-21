//! Prompt evaluation suite CLI.
//!
//! Loads test cases from a directory of JSON files, runs each case in mock
//! (or live) mode, scores the resulting artifact, and compares against a
//! baseline. Exits with code 1 if any regression is detected.
//!
//! Usage:
//!   cargo run -p orchestrator-cli --bin orchestrator-eval -- [options]
//!
//! Options:
//!   --cases-dir <PATH>      Directory of test case JSONs (default: tests/eval/cases)
//!   --baseline <PATH>       Baseline file path (default: tests/eval/baseline.json)
//!   --update-baseline       Write current scores as the new baseline
//!   --live                  Run in live LLM mode (requires LLM_GATEWAY_API_KEY)
//!   --filter <TEST_ID>      Run only the matching test case

use anyhow::{bail, Context, Result};
use clap::Parser;
use orchestrator_cli::eval::baseline::{check_regression, load_baseline, save_baseline};
use orchestrator_cli::eval::runner::{CaseResult, EvalRunner};
use orchestrator_cli::eval::{EvalCase, EvalMode};
use std::path::Path;

#[derive(Parser)]
struct Args {
    /// Path to eval cases directory.
    #[arg(long, default_value = "tests/eval/cases")]
    cases_dir: String,

    /// Path to baseline file.
    #[arg(long, default_value = "tests/eval/baseline.json")]
    baseline: String,

    /// Update baseline with current scores.
    #[arg(long)]
    update_baseline: bool,

    /// Run in live mode (requires LLM_GATEWAY_API_KEY).
    #[arg(long)]
    live: bool,

    /// Filter to specific test ID.
    #[arg(long)]
    filter: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cases_dir = Path::new(&args.cases_dir);

    let mut cases = load_cases(cases_dir, args.filter.as_deref())?;
    if cases.is_empty() {
        bail!("no test cases found in {}", cases_dir.display());
    }

    // Override mode if --live is set
    if args.live {
        for case in &mut cases {
            case.mode = EvalMode::Live;
        }
    }

    let runner = EvalRunner::new();
    let results = runner.run_all(&cases)?;

    if args.update_baseline {
        let baseline_path = Path::new(&args.baseline);
        save_baseline(baseline_path, &results)?;
        println!("Baseline updated at {}", baseline_path.display());
        print_summary(&results);
        return Ok(());
    }

    let baseline_path = Path::new(&args.baseline);
    if !baseline_path.exists() {
        // No baseline yet — print results without regression check
        println!(
            "No baseline file at {}; run with --update-baseline to create one.",
            baseline_path.display()
        );
        print_summary(&results);
        return Ok(());
    }

    let baseline = load_baseline(baseline_path)?;
    let regressions = check_regression(&results, &baseline);

    if !regressions.is_empty() {
        for r in &regressions {
            eprintln!(
                "REGRESSION: {} score {} -> {} (delta {:.1})",
                r.test_id, r.baseline_score, r.current_score, r.delta
            );
        }
        print_summary(&results);
        std::process::exit(1);
    }

    println!("All {} test cases passed (no regressions).", results.len());
    print_summary(&results);
    Ok(())
}

/// Load all JSON test cases from a directory, optionally filtered by test_id.
fn load_cases(dir: &Path, filter: Option<&str>) -> Result<Vec<EvalCase>> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read cases directory: {}", dir.display()))?;

    let mut cases = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read case file: {}", path.display()))?;
        let case: EvalCase = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse case JSON: {}", path.display()))?;
        if let Some(filter_id) = filter {
            if case.test_id != filter_id {
                continue;
            }
        }
        cases.push(case);
    }

    // Sort by test_id for deterministic output
    cases.sort_by(|a, b| a.test_id.cmp(&b.test_id));
    Ok(cases)
}

fn print_summary(results: &[CaseResult]) {
    println!("\n{:-<80}", "");
    println!(
        "{:<32} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "TEST_ID", "AGG", "JSON", "SCHMA", "FIELD", "DIR", "EVID"
    );
    println!("{:-<80}", "");

    for r in results {
        let dims = &r.score.dimensions;
        println!(
            "{:<32} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>10.1}",
            r.test_id,
            r.score.aggregate,
            dims.get("json_validity").copied().unwrap_or(0.0),
            dims.get("schema_compliance").copied().unwrap_or(0.0),
            dims.get("field_completeness").copied().unwrap_or(0.0),
            dims.get("direction_reasonableness").copied().unwrap_or(0.0),
            dims.get("evidence_quality").copied().unwrap_or(0.0),
        );
    }
    println!("{:-<80}", "");
}
