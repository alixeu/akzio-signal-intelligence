//! Pure validation of observation rows in governed FRED evidence and read projections.

use std::collections::{btree_map::Entry, BTreeMap};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const OBSERVATION_QUALITY_VERSION: u32 = 1;
// FRED's documented maximum page size for series/observations.
const MAX_OBSERVATION_ROWS: usize = 100_000;

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Pagination {
    Complete,
    Truncated,
    #[default]
    Unreported,
    Invalid,
}

#[derive(Default, Deserialize, Serialize)]
pub struct ObservationQuality {
    version: u32,
    raw_rows: usize,
    usable_rows: usize,
    missing_rows: usize,
    malformed_rows: usize,
    duplicate_rows: usize,
    out_of_window_rows: usize,
    wrong_vintage_rows: usize,
    // Ordering is independent of the row disposition counts. New normalized rows
    // are sorted after validation; an old unnormalized payload retains source order.
    out_of_order_rows: usize,
    pagination: Pagination,
}

impl ObservationQuality {
    pub fn completeness_ppm(&self) -> u32 {
        ((self.usable_rows as u64 * 1_000_000) / self.raw_rows.max(1) as u64) as u32
    }

    fn counts_match(&self) -> bool {
        self.raw_rows <= MAX_OBSERVATION_ROWS
            && self.out_of_order_rows <= self.raw_rows
            && [
                self.usable_rows,
                self.missing_rows,
                self.malformed_rows,
                self.duplicate_rows,
                self.out_of_window_rows,
                self.wrong_vintage_rows,
            ]
            .into_iter()
            .try_fold(0_usize, usize::checked_add)
                == Some(self.raw_rows)
    }
}

pub struct ObservationScope {
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    vintage: NaiveDate,
}

impl ObservationScope {
    pub fn for_response(
        value: &Value,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
        vintage: Option<NaiveDate>,
    ) -> Option<Self> {
        let point = fred_date(value, "realtime_start")?;
        (fred_date(value, "realtime_end") == Some(point) && vintage.is_none_or(|v| v == point))
            .then_some(Self {
                start,
                end,
                vintage: point,
            })
    }

    fn for_vintage_series(resource: &str, value: &Value) -> Option<Self> {
        let mut parts = resource.split(':');
        if parts.next()? != "series" {
            return None;
        }
        let series = parts.next()?;
        if series.is_empty()
            || series.len() > 64
            || !series
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return None;
        }
        let start = parse_date(parts.next()?)?;
        let end = parse_date(parts.next()?)?;
        let vintage = parse_date(parts.next()?)?;
        if parts.next().is_some() || end < start || (end - start).num_days() > 366 {
            return None;
        }
        Self::for_response(value, Some(start), Some(end), Some(vintage))
    }
}

fn fred_date(row: &Value, key: &str) -> Option<NaiveDate> {
    parse_date(row.get(key)?.as_str()?)
}

fn parse_date(value: &str) -> Option<NaiveDate> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn pagination(value: &Value, raw_rows: usize) -> Pagination {
    let fields = [value.get("count"), value.get("offset"), value.get("limit")];
    if fields == [None, None, None] {
        return Pagination::Unreported;
    }
    let [Some(count), Some(offset), Some(limit)] =
        fields.map(|field| field.and_then(Value::as_u64))
    else {
        return Pagination::Invalid;
    };
    let raw_rows = raw_rows as u64;
    let Some(page_end) = offset.checked_add(raw_rows) else {
        return Pagination::Invalid;
    };
    if limit == 0 || raw_rows > limit || page_end > count {
        Pagination::Invalid
    } else if offset != 0 || page_end < count {
        Pagination::Truncated
    } else {
        Pagination::Complete
    }
}

fn assess_rows<'a>(
    rows: &'a [Value],
    scope: &ObservationScope,
) -> (Vec<&'a Value>, ObservationQuality) {
    let mut quality = ObservationQuality {
        version: OBSERVATION_QUALITY_VERSION,
        raw_rows: rows.len(),
        ..ObservationQuality::default()
    };
    let mut by_date = BTreeMap::new();
    let mut previous_date = None;
    for row in rows {
        let Some(date) = fred_date(row, "date") else {
            quality.malformed_rows += 1;
            continue;
        };
        if previous_date.is_some_and(|previous| date < previous) {
            quality.out_of_order_rows += 1;
        }
        previous_date = Some(date);
        match by_date.entry(date) {
            Entry::Vacant(entry) => {
                entry.insert(Some(row));
            }
            Entry::Occupied(mut entry) => {
                // Reject every row on an ambiguous date, regardless of row order or value.
                quality.duplicate_rows += if entry.insert(None).is_some() { 2 } else { 1 };
            }
        }
    }

    let mut usable = Vec::new();
    for (date, row) in by_date {
        let Some(row) = row else { continue };
        if scope.start.is_some_and(|start| date < start)
            || scope.end.is_some_and(|end| date > end)
            || date > scope.vintage
        {
            quality.out_of_window_rows += 1;
            continue;
        }
        if !matches!((fred_date(row, "realtime_start"), fred_date(row, "realtime_end")),
            (Some(start), Some(end)) if (start..=end).contains(&scope.vintage))
        {
            quality.wrong_vintage_rows += 1;
            continue;
        }
        match row.get("value") {
            None | Some(Value::Null) => quality.missing_rows += 1,
            Some(Value::String(value)) if value == "." => quality.missing_rows += 1,
            Some(Value::String(value)) if value.parse::<f64>().is_ok_and(f64::is_finite) => {
                usable.push(row)
            }
            _ => quality.malformed_rows += 1,
        }
    }
    quality.usable_rows = usable.len();
    (usable, quality)
}

fn observation_rows(value: &Value) -> Option<&[Value]> {
    let rows = value.get("observations")?.as_array()?;
    (rows.len() <= MAX_OBSERVATION_ROWS).then_some(rows.as_slice())
}

/// Validate provider observation rows for a parsed request and count its reported page coverage.
pub fn assess_response<'a>(
    value: &'a Value,
    scope: &ObservationScope,
) -> Option<(Vec<&'a Value>, ObservationQuality)> {
    let rows = observation_rows(value)?;
    let (usable, mut quality) = assess_rows(rows, scope);
    quality.pagination = pagination(value, rows.len());
    Some((usable, quality))
}

/// Reassess the unchanged vendor rows in an old normalized payload for its read projection.
/// The projection carries usable dates and derived coverage without altering the stored capture.
pub fn legacy_fred_observations<'a>(
    resource: &str,
    value: &'a Value,
) -> Option<(Vec<&'a Value>, ObservationQuality)> {
    let scope = ObservationScope::for_vintage_series(resource, value)?;
    assess_response(value, &scope)
}

/// Diagnose a series that cannot supply directional macro evidence. Other FRED
/// resources and other evidence sources have independent qualification rules.
/// This evaluates row quality and pagination, leaving freshness and time authority
/// to the caller's EvidenceTimeBasis validation.
pub fn fred_series_direction_gap(resource: &str, value: &Value) -> Option<&'static str> {
    if !resource.starts_with("series:") {
        return None;
    }
    const UNVERIFIED: &str = "fred_observation_rows_unverified";
    let Some((scope, rows)) =
        ObservationScope::for_vintage_series(resource, value).zip(observation_rows(value))
    else {
        return Some(UNVERIFIED);
    };
    let (_, checked) = assess_rows(rows, &scope);
    let page = if let Some(metadata) = value.get("observation_quality") {
        let Ok(quality) = ObservationQuality::deserialize(metadata) else {
            return Some(UNVERIFIED);
        };
        let page = pagination(value, quality.raw_rows);
        if quality.version != OBSERVATION_QUALITY_VERSION
            || !quality.counts_match()
            || quality.usable_rows != checked.usable_rows
            || checked.usable_rows != rows.len()
            || checked.out_of_order_rows != 0
            || quality.pagination != page
        {
            return Some(UNVERIFIED);
        }
        page
    } else {
        // Legacy direct captures paired the exact-vintage envelope with unchanged
        // observation rows. Ordinary FRED missing values can coexist with known
        // facts; any other defect makes the old displayed payload unsuitable.
        if checked.usable_rows + checked.missing_rows != rows.len()
            || checked.out_of_order_rows != 0
        {
            return Some(UNVERIFIED);
        }
        pagination(value, rows.len())
    };
    match page {
        Pagination::Complete if checked.usable_rows == 0 => Some("fred_no_usable_observations"),
        Pagination::Complete => None,
        Pagination::Truncated => Some("fred_observation_page_truncated"),
        Pagination::Unreported => Some("fred_observation_pagination_unreported"),
        Pagination::Invalid => Some("fred_observation_pagination_invalid"),
    }
}
