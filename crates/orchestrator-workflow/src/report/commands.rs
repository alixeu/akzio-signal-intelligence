use anyhow::{bail, Context, Result};
use chrono::Local;
use clap::{Args, ValueEnum};
use html_escape::encode_text;
use orchestrator_core::{config_float, config_str, config_strings, display_ticker, run_slug};
use serde_json::{json, Value};
use std::{fs, path::PathBuf, process::Command};

#[derive(Debug, Clone, ValueEnum)]
pub enum ReportMode {
    Build,
    Send,
    BuildAndSend,
}

#[derive(Debug, Clone, Args)]
pub struct ReportArgs {
    #[arg(long, value_enum, default_value_t = ReportMode::BuildAndSend)]
    pub mode: ReportMode,
}

pub fn run(args: ReportArgs) -> Result<Value> {
    let config = super::config::load_default_config();
    let tickers = config_strings(&config, "orchestrator.analysis_universe", &[]);
    let slug = if tickers.is_empty() {
        "TQQQ".to_string()
    } else {
        run_slug(&tickers)
    };
    let today = Local::now().date_naive().to_string();
    let payload = build_payload(&today, &slug, &tickers)?;
    let mut result = json!({
        "subject": payload.get("subject").and_then(Value::as_str).unwrap_or("Daily TQQQ Strategy Report"),
        "orchestrator_status": payload.get("orchestrator_status").and_then(Value::as_str).unwrap_or("unknown")
    });
    if matches!(args.mode, ReportMode::Send | ReportMode::BuildAndSend) {
        let html = build_html(&payload);
        let email = send_report(&config, &slug, &payload, &html)?;
        result["email_status"] = email["status"].clone();
        result["email_detail"] = email["detail"].clone();
    }
    Ok(result)
}

fn build_payload(today: &str, slug: &str, tickers: &[String]) -> Result<Value> {
    let run_dir = if let Ok(value) = std::env::var("CODEX_ORCH_RUN_DIR") {
        PathBuf::from(value)
    } else {
        super::config::project_path_from_config(format!("outputs/{}/{}_exec", slug, today))
    };
    let state_path = run_dir.join("state.json");
    let final_summary_path = run_dir.join("final_summary.md");
    let state = if state_path.exists() {
        serde_json::from_str(&fs::read_to_string(&state_path)?)?
    } else {
        json!({})
    };
    let report_markdown = super::builder::build_human_readable_report(&state);
    let report_html = super::builder::report_to_html(&report_markdown);
    let final_summary = fs::read_to_string(&final_summary_path).unwrap_or_default();

    Ok(json!({
        "subject": format!("{} strategy report {}", display_ticker(tickers), today),
        "today": today,
        "orchestrator_status": if state_path.exists() { "complete" } else { "missing" },
        "run_dir": run_dir,
        "orchestrator_state": state,
        "report_markdown": report_markdown,
        "report_html": report_html,
        "final_summary": final_summary
    }))
}

fn build_html(payload: &Value) -> String {
    let subject = payload
        .get("subject")
        .and_then(Value::as_str)
        .unwrap_or("Daily Report");
    if let Some(report_html) = payload.get("report_html").and_then(Value::as_str) {
        if !report_html.is_empty() {
            return report_html.to_string();
        }
    }
    let summary = payload
        .get("final_summary")
        .and_then(Value::as_str)
        .unwrap_or("");
    let allocation_html = allocation_html(payload);
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><h1>{}</h1>{}<pre>{}</pre></body></html>",
        encode_text(subject),
        encode_text(subject),
        allocation_html,
        encode_text(summary)
    )
}

fn allocation_html(payload: &Value) -> String {
    let allocation = payload
        .get("orchestrator_state")
        .and_then(|state| state.get("portfolio_allocation"))
        .unwrap_or(&Value::Null);
    let Some(weights) = allocation.get("weights").and_then(Value::as_object) else {
        return String::new();
    };
    let mut rows = String::new();
    for (asset, payload) in weights {
        let weight = payload.get("weight").and_then(Value::as_f64).unwrap_or(0.0);
        let rationale = payload
            .get("rationale")
            .and_then(Value::as_str)
            .unwrap_or("");
        rows.push_str(&format!(
            "<tr><td>{}</td><td>{:.1}%</td><td>{}</td></tr>",
            encode_text(asset),
            weight * 100.0,
            encode_text(rationale)
        ));
    }
    let summary = allocation
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("");
    format!(
        "<section><h2>Portfolio Allocation</h2><p><strong>VIX regime:</strong> {} &nbsp; <strong>Total equity exposure:</strong> {} &nbsp; <strong>Correlation:</strong> {}</p><table border=\"1\" cellspacing=\"0\" cellpadding=\"6\"><thead><tr><th>Asset</th><th>Weight</th><th>Rationale</th></tr></thead><tbody>{}</tbody></table><p>{}</p></section>",
        encode_text(allocation.get("vix_regime").and_then(Value::as_str).unwrap_or("")),
        encode_text(&allocation.get("total_equity_exposure").map(Value::to_string).unwrap_or_default()),
        encode_text(allocation.get("correlation_note").and_then(Value::as_str).unwrap_or("")),
        rows,
        encode_text(summary)
    )
}

fn send_report(config: &Value, slug: &str, payload: &Value, html: &str) -> Result<Value> {
    if config_str(config, "report.email.enabled", "true") == "false" {
        return Ok(json!({"status": "skipped", "detail": "report.email.enabled=false"}));
    }
    let state_path = super::config::project_path_from_config(format!(
        "outputs/{}/report_email_state.json",
        slug
    ));
    let state = fs::read_to_string(&state_path)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .unwrap_or_else(|| json!({}));
    let probability_threshold = config_float(config, "report.email.probability_threshold", 0.68);
    let decision = email_decision(payload, &state, probability_threshold);
    if !decision.should_send {
        return Ok(json!({"status": "skipped", "detail": decision.reason}));
    }
    let subject = payload
        .get("subject")
        .and_then(Value::as_str)
        .unwrap_or("Daily Report");
    let smtp_url = config_str(config, "report.email.smtp_url", "");
    let username = config_str(config, "report.email.username", "");
    let password = config_str(config, "report.email.password", "");
    let from = config_str(config, "report.email.from", &username);
    let to = config_str(config, "report.email.to", "");
    if [
        smtp_url.as_str(),
        username.as_str(),
        password.as_str(),
        from.as_str(),
        to.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        bail!("report.email requires smtp_url, username, password, from, and to");
    }
    let eml = format!(
        "From: {from}\nTo: {to}\nSubject: {subject}\nMIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n\n{html}"
    );
    let temp_dir = tempfile::tempdir()?;
    let message_path = temp_dir.path().join("message.eml");
    fs::write(&message_path, &eml)?;
    let status = Command::new("curl")
        .arg("--ssl-reqd")
        .arg("--url")
        .arg(&smtp_url)
        .arg("--user")
        .arg(format!("{username}:{password}"))
        .arg("--mail-from")
        .arg(&from)
        .arg("--mail-rcpt")
        .arg(&to)
        .arg("--upload-file")
        .arg(&message_path)
        .status()
        .context("failed to invoke curl smtp")?;
    if status.success() {
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &state_path,
            serde_json::to_string_pretty(&json!({
                "last_sent_date": decision.date,
                "last_direction": decision.direction,
                "last_probability": decision.probability,
                "last_reason": decision.reason,
                "last_subject": subject,
                "sent_at": Local::now().to_rfc3339(),
            }))?,
        )?;
    }
    Ok(
        json!({"status": if status.success() { "sent" } else { "failed" }, "detail": to, "reason": decision.reason}),
    )
}

struct EmailDecision {
    should_send: bool,
    reason: &'static str,
    date: String,
    direction: &'static str,
    probability: f64,
}

fn email_decision(payload: &Value, state: &Value, probability_threshold: f64) -> EmailDecision {
    let date = payload
        .get("today")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| Local::now().date_naive().to_string());
    let (direction, probability) = payload_direction(payload);
    if state
        .get("last_sent_date")
        .and_then(Value::as_str)
        .is_none_or(|last_date| last_date != date)
    {
        return EmailDecision {
            should_send: true,
            reason: "first_send_today",
            date,
            direction,
            probability,
        };
    }
    let last_direction = state.get("last_direction").and_then(Value::as_str);
    let reversed = matches!(
        (last_direction, direction),
        (Some("long"), "short") | (Some("short"), "long")
    );
    let should_send = reversed && probability >= probability_threshold;
    EmailDecision {
        should_send,
        reason: if should_send {
            "high_probability_reversal"
        } else {
            "already_sent_today"
        },
        date,
        direction,
        probability,
    }
}

fn payload_direction(payload: &Value) -> (&'static str, f64) {
    let state = payload.get("orchestrator_state").unwrap_or(payload);
    let research = state.get("research_plan").unwrap_or(state);
    let long = normalize_probability(research.get("long_probability")).unwrap_or(0.0);
    let short = normalize_probability(research.get("short_probability")).unwrap_or(0.0);
    if long > short {
        ("long", long)
    } else if short > long {
        ("short", short)
    } else {
        ("neutral", long)
    }
}

fn normalize_probability(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64().map(|n| if n > 1.0 { n / 100.0 } else { n }),
        Value::String(text) => {
            let trimmed = text.trim();
            if let Some(percent) = trimmed.strip_suffix('%') {
                percent.trim().parse::<f64>().ok().map(|n| n / 100.0)
            } else {
                trimmed
                    .parse::<f64>()
                    .ok()
                    .map(|n| if n > 1.0 { n / 100.0 } else { n })
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_decision_sends_first_daily_report() {
        let payload = json!({"today": "2026-06-19", "orchestrator_state": {"long_probability": 0.51, "short_probability": 0.49}});
        let decision = email_decision(&payload, &json!({}), 0.68);
        assert!(decision.should_send);
        assert_eq!(decision.reason, "first_send_today");
    }

    #[test]
    fn build_payload_reads_state_from_run_dir() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run-dir");
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(run_dir.join("state.json"), "{}").unwrap();
        std::env::set_var("CODEX_ORCH_RUN_DIR", &run_dir);
        let payload = build_payload("2026-06-19", "TQQQ", &[]).unwrap();
        std::env::remove_var("CODEX_ORCH_RUN_DIR");
        assert_eq!(payload["orchestrator_status"], "complete");
    }

    #[test]
    fn email_decision_sends_high_probability_reversal_only() {
        let state = json!({"last_sent_date": "2026-06-19", "last_direction": "long"});
        let weak = json!({"today": "2026-06-19", "orchestrator_state": {"long_probability": 0.4, "short_probability": 0.6}});
        assert!(!email_decision(&weak, &state, 0.68).should_send);

        let strong = json!({"today": "2026-06-19", "orchestrator_state": {"long_probability": 0.2, "short_probability": 0.8}});
        let decision = email_decision(&strong, &state, 0.68);
        assert!(decision.should_send);
        assert_eq!(decision.reason, "high_probability_reversal");
    }
}
