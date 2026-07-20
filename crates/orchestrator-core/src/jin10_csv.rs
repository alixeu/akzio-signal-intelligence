//! Jin10 flash news CSV storage.
//!
//! File naming: `{date}.csv` in the jin10 output directory.
//! e.g. `2026-07-20.csv`.
//!
//! Each file holds flash news items fetched for that date with columns: id, time, content.

use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_JIN10_CSV_DIR: &str = "outputs/jin10";

pub fn default_jin10_csv_dir() -> PathBuf {
    if let Ok(path) = std::env::var("ORCHESTRATOR_JIN10_CSV_DIR") {
        let path = path.trim();
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    crate::project_path(DEFAULT_JIN10_CSV_DIR)
}

pub fn jin10_csv_path(dir: &Path, date: &str) -> PathBuf {
    dir.join(format!("{date}.csv"))
}

#[derive(Debug, Clone)]
pub struct Jin10CsvRow {
    pub id: String,
    pub time: String,
    pub content: String,
}

pub fn write_jin10_csv(path: &Path, rows: &[Jin10CsvRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create jin10 csv dir {}", parent.display()))?;
    }
    let mut lines = Vec::with_capacity(rows.len() + 1);
    lines.push("id,time,content".to_string());
    for row in rows {
        lines.push(format!(
            "{},{},{}",
            csv_escape(&row.id),
            csv_escape(&row.time),
            csv_escape(&row.content)
        ));
    }
    fs::write(path, lines.join("\n"))
        .with_context(|| format!("failed to write jin10 csv {}", path.display()))?;
    Ok(())
}

pub fn read_jin10_csv(path: &Path) -> Result<Vec<Jin10CsvRow>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read jin10 csv {}", path.display()))?;
    parse_jin10_csv(&raw)
}

pub fn parse_jin10_csv(raw: &str) -> Result<Vec<Jin10CsvRow>> {
    let mut lines = raw.lines().filter(|line| !line.trim().is_empty());
    let header = lines.next().unwrap_or("");
    if !header.starts_with("id,") {
        anyhow::bail!("jin10 csv header must start with 'id,'");
    }
    let mut rows = Vec::new();
    for line in lines {
        let fields = parse_csv_line(line);
        if fields.len() < 3 {
            continue;
        }
        rows.push(Jin10CsvRow {
            id: fields[0].clone(),
            time: fields[1].clone(),
            content: fields[2..].join(","),
        });
    }
    Ok(rows)
}

/// Load all jin10 CSV rows from the default directory for a given date.
pub fn load_jin10_csv(date: &str) -> Vec<Jin10CsvRow> {
    let csv_dir = default_jin10_csv_dir();
    let path = jin10_csv_path(&csv_dir, date);
    read_jin10_csv(&path).unwrap_or_default()
}

/// Load jin10 CSV rows from all recent files in the directory.
pub fn load_jin10_csv_recent(max_files: usize) -> Vec<Jin10CsvRow> {
    let csv_dir = default_jin10_csv_dir();
    let Ok(entries) = fs::read_dir(&csv_dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".csv")))
        .map(|e| e.path())
        .collect();
    paths.sort();
    paths.reverse();
    paths.truncate(max_files);

    let mut all_rows = Vec::new();
    for path in paths {
        if let Ok(rows) = read_jin10_csv(&path) {
            all_rows.extend(rows);
        }
    }
    all_rows
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' if !in_quotes && current.is_empty() => {
                in_quotes = true;
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

    #[test]
    fn roundtrip_jin10_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("2026-07-20.csv");
        let rows = vec![
            Jin10CsvRow {
                id: "abc123".into(),
                time: "2026-07-20 09:00:00".into(),
                content: "Fed rate decision pending".into(),
            },
            Jin10CsvRow {
                id: "def456".into(),
                time: "2026-07-20 09:05:00".into(),
                content: "Oil prices surge, OPEC cuts".into(),
            },
        ];
        write_jin10_csv(&path, &rows).unwrap();
        let loaded = read_jin10_csv(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "abc123");
        assert_eq!(loaded[0].content, "Fed rate decision pending");
        assert_eq!(loaded[1].id, "def456");
    }

    #[test]
    fn csv_with_commas_in_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let rows = vec![Jin10CsvRow {
            id: "x1".into(),
            time: "2026-07-20 10:00:00".into(),
            content: "GDP growth 3.2%, beating expectations of 2.8%".into(),
        }];
        write_jin10_csv(&path, &rows).unwrap();
        let loaded = read_jin10_csv(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].content,
            "GDP growth 3.2%, beating expectations of 2.8%"
        );
    }
}
