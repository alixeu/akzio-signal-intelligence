//! Technical indicator CSV snapshots (Yahoo-derived, not SQLite).
//!
//! File naming: `{symbol_lower}_{interval_label}.csv`
//! e.g. `qqq_day.csv`, `qqq_3h.csv`, `vix_20min.csv`.
//!
//! Each file holds the latest N bars (default 60) with feature columns.

use anyhow::{bail, Context, Result};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_TECHNICAL_CSV_DIR: &str = "outputs/technical";
pub const DEFAULT_TECHNICAL_BARS: usize = 60;

/// Canonical storage/query interval names used in code and CSV metadata.
pub fn storage_interval(interval: &str) -> Option<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1d" | "day" | "daily" => Some("daily"),
        "3h" | "three_hour" | "three-hour" => Some("3h"),
        "20min" | "20m" | "twenty_minute" | "twenty-minute" => Some("20min"),
        _ => None,
    }
}

/// Short label used in CSV filenames (`day` / `3h` / `20min`).
pub fn interval_file_label(interval: &str) -> Option<&'static str> {
    match storage_interval(interval)? {
        "daily" => Some("day"),
        "3h" => Some("3h"),
        "20min" => Some("20min"),
        _ => None,
    }
}

pub fn technical_csv_filename(symbol: &str, interval: &str) -> Option<String> {
    let label = interval_file_label(interval)?;
    Some(format!(
        "{}_{}.csv",
        symbol.trim().to_ascii_lowercase(),
        label
    ))
}

pub fn technical_csv_path(dir: &Path, symbol: &str, interval: &str) -> Option<PathBuf> {
    technical_csv_filename(symbol, interval).map(|name| dir.join(name))
}

pub fn default_technical_csv_dir() -> PathBuf {
    if let Ok(path) = std::env::var("ORCHESTRATOR_TECHNICAL_CSV_DIR") {
        let path = path.trim();
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    crate::project_path(DEFAULT_TECHNICAL_CSV_DIR)
}

#[derive(Debug, Clone)]
pub struct TechnicalCsvRow {
    pub date: String,
    pub values: HashMap<String, f64>,
}

/// Write feature rows as CSV. `rows` should already be trimmed to the keep window
/// and ordered oldest → newest.
pub fn write_technical_csv(path: &Path, rows: &[TechnicalCsvRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create technical csv dir {}", parent.display()))?;
    }
    let mut columns: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        columns.extend(row.values.keys().cloned());
    }
    // Prefer a stable, human-friendly column order for the common fields.
    let preferred = [
        "Close",
        "Return",
        "LogReturn",
        "Gap",
        "Body",
        "UpperShadow",
        "LowerShadow",
    ];
    let mut ordered: Vec<String> = Vec::new();
    for key in preferred {
        if columns.remove(key) {
            ordered.push(key.to_string());
        }
    }
    ordered.extend(columns);

    let mut out = String::new();
    out.push_str("date");
    for col in &ordered {
        out.push(',');
        out.push_str(col);
    }
    out.push('\n');
    for row in rows {
        out.push_str(&escape_csv_field(&row.date));
        for col in &ordered {
            out.push(',');
            if let Some(value) = row.values.get(col) {
                out.push_str(&format_csv_number(*value));
            }
        }
        out.push('\n');
    }
    fs::write(path, out)
        .with_context(|| format!("failed to write technical csv {}", path.display()))?;
    Ok(())
}

pub fn read_technical_csv(path: &Path) -> Result<Vec<TechnicalCsvRow>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read technical csv {}", path.display()))?;
    parse_technical_csv(&raw)
}

pub fn parse_technical_csv(raw: &str) -> Result<Vec<TechnicalCsvRow>> {
    let mut lines = raw.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("technical csv missing header"))?;
    let columns = split_csv_line(header);
    if columns.is_empty() || columns[0] != "date" {
        bail!("technical csv header must start with date");
    }
    let mut rows = Vec::new();
    for line in lines {
        let fields = split_csv_line(line);
        if fields.is_empty() {
            continue;
        }
        let date = fields[0].clone();
        let mut values = HashMap::new();
        for (idx, col) in columns.iter().enumerate().skip(1) {
            let Some(cell) = fields.get(idx).map(String::as_str) else {
                continue;
            };
            if cell.trim().is_empty() {
                continue;
            }
            if let Ok(value) = cell.parse::<f64>() {
                if value.is_finite() {
                    values.insert(col.clone(), value);
                }
            }
        }
        rows.push(TechnicalCsvRow { date, values });
    }
    Ok(rows)
}

pub fn latest_indicator(rows: &[TechnicalCsvRow], indicator: &str) -> Option<f64> {
    rows.iter()
        .rev()
        .find_map(|row| row.values.get(indicator).copied())
}

pub fn latest_close(rows: &[TechnicalCsvRow]) -> Option<(String, f64)> {
    rows.iter().rev().find_map(|row| {
        row.values
            .get("Close")
            .copied()
            .map(|close| (row.date.clone(), close))
    })
}

pub fn closes_for_correlation(rows: &[TechnicalCsvRow], limit: usize) -> Vec<(String, f64)> {
    let mut closes: Vec<(String, f64)> = rows
        .iter()
        .filter_map(|row| {
            row.values
                .get("Close")
                .copied()
                .map(|close| (row.date.clone(), close))
        })
        .collect();
    if closes.len() > limit {
        closes = closes.split_off(closes.len() - limit);
    }
    closes
}

pub fn close_on_or_before(rows: &[TechnicalCsvRow], date: &str) -> Option<(String, f64)> {
    let target = date.get(..10).unwrap_or(date);
    rows.iter()
        .rev()
        .filter_map(|row| {
            let day = row.date.get(..10).unwrap_or(row.date.as_str());
            row.values
                .get("Close")
                .copied()
                .filter(|_| day <= target)
                .map(|close| (row.date.clone(), close))
        })
        .next()
}

pub fn close_on_or_after(rows: &[TechnicalCsvRow], date: &str) -> Option<(String, f64)> {
    let target = date.get(..10).unwrap_or(date);
    rows.iter()
        .filter_map(|row| {
            let day = row.date.get(..10).unwrap_or(row.date.as_str());
            row.values
                .get("Close")
                .copied()
                .filter(|_| day >= target)
                .map(|close| (row.date.clone(), close))
        })
        .next()
}

/// Compact latest-bar snapshot for tool/context consumers.
pub fn latest_snapshot(
    symbol: &str,
    interval: &str,
    rows: &[TechnicalCsvRow],
    keep_keys: &[&str],
) -> Option<serde_json::Value> {
    let row = rows.last()?;
    let indicators: serde_json::Map<String, serde_json::Value> = if keep_keys.is_empty() {
        row.values
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect()
    } else {
        keep_keys
            .iter()
            .filter_map(|key| {
                row.values
                    .get(*key)
                    .map(|value| ((*key).to_string(), serde_json::json!(value)))
            })
            .collect()
    };
    if indicators.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "ticker": symbol.to_ascii_uppercase(),
        "interval": storage_interval(interval).unwrap_or(interval),
        "kline_time": row.date,
        "source": "technical_csv",
        "indicators": indicators
    }))
}

/// Render CSV bodies as `<file id: name>` blocks for prompt injection.
/// OpenAI Responses API maps non-PDF documents to text; this matches that shape.
pub fn render_csv_file_blocks(
    dir: &Path,
    symbols: &[String],
    intervals: &[&str],
) -> Result<String> {
    let mut blocks = Vec::new();
    for symbol in symbols {
        for interval in intervals {
            let Some(path) = technical_csv_path(dir, symbol, interval) else {
                continue;
            };
            if !path.exists() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("technical.csv");
            let body = fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            blocks.push(format!("<file id: {name}>\n{body}</file>\n"));
        }
    }
    Ok(blocks.join("\n"))
}

fn format_csv_number(value: f64) -> String {
    // Compact but stable; avoid scientific notation for typical prices.
    let text = format!("{value:.10}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn escape_csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current);
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn filename_uses_day_label_for_daily() {
        assert_eq!(
            technical_csv_filename("QQQ", "1d").as_deref(),
            Some("qqq_day.csv")
        );
        assert_eq!(
            technical_csv_filename("SOXX", "3h").as_deref(),
            Some("soxx_3h.csv")
        );
    }

    #[test]
    fn round_trip_csv_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("qqq_day.csv");
        let rows = vec![
            TechnicalCsvRow {
                date: "2026-01-01".into(),
                values: HashMap::from([("Close".into(), 100.0), ("Return".into(), 0.01)]),
            },
            TechnicalCsvRow {
                date: "2026-01-02".into(),
                values: HashMap::from([("Close".into(), 101.0), ("Return".into(), 0.01)]),
            },
        ];
        write_technical_csv(&path, &rows).unwrap();
        let loaded = read_technical_csv(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].date, "2026-01-02");
        assert_eq!(latest_close(&loaded).unwrap().1, 101.0);
    }
}
