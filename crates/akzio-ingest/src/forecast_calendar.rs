use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use akzio_domain::{ArtifactKind, ArtifactRef, Asset, ContextManifestPayload};
use akzio_store::Store;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::{EvidenceAdapterError, EvidenceRuntimeResult, EvidenceSource, GovernedResource};

pub(crate) const BAR_AVAILABILITY_LAG_MINUTES: i64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledSession {
    pub date: NaiveDate,
    pub close_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CalendarMetadata {
    version: u32,
    availability_lag_minutes: i64,
    session_closes: BTreeMap<NaiveDate, DateTime<Utc>>,
}

/// Scheduled closes in the exact immutable evidence selected for a proposal.
#[derive(Debug, Default)]
pub struct ForecastCalendars {
    scheduled: Vec<(Asset, CalendarMetadata)>,
    legacy: BTreeMap<String, BTreeMap<String, DateTime<Utc>>>,
}

impl ForecastCalendars {
    /// Follow the accepted DecisionContext's exact research Manifest. Legacy
    /// contexts can lack this reference or use an unversioned bar snapshot.
    pub fn for_decision_context(
        store: &Store,
        context_ref: &ArtifactRef,
    ) -> EvidenceRuntimeResult<Option<Self>> {
        let context = store.artifact(&context_ref.artifact_id)?;
        if context_ref.kind != ArtifactKind::DecisionContext || context.kind != context_ref.kind {
            return Err(invalid_calendar("forecast calendar requires a DecisionContext").into());
        }
        let mut manifests = context
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest);
        let Some(reference) = manifests.next() else {
            return Ok(None);
        };
        if manifests.next().is_some() {
            return Err(invalid_calendar("DecisionContext has multiple research manifests").into());
        }
        let artifact = store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(
                invalid_calendar("DecisionContext research manifest has unexpected kind").into(),
            );
        }
        let manifest = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let calendars = Self::read_selections(store, &manifest, false)?;
        Ok(calendars.has_frozen_schedule().then_some(calendars))
    }

    pub fn read(store: &Store, manifest: &ContextManifestPayload) -> EvidenceRuntimeResult<Self> {
        Self::read_selections(store, manifest, true)
    }

    fn read_selections(
        store: &Store,
        manifest: &ContextManifestPayload,
        read_legacy_closes: bool,
    ) -> EvidenceRuntimeResult<Self> {
        let mut calendars = Self::default();
        for selection in &manifest.selections {
            if selection.artifact.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let artifact = store.artifact(&selection.artifact.artifact_id)?;
            if artifact.kind != ArtifactKind::NormalizedEvidence {
                return Err(
                    invalid_calendar("manifest calendar reference has unexpected kind").into(),
                );
            }
            let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            if let Some(calendar) = payload.pointer("/value/forecast_calendar") {
                let resource = payload["resource"].as_str().ok_or_else(|| {
                    invalid_calendar("forecast calendar is missing its governed resource")
                })?;
                let GovernedResource::AlpacaBars {
                    asset,
                    raw_prices: false,
                    ..
                } = GovernedResource::parse(EvidenceSource::Alpaca, resource)?
                else {
                    return Err(invalid_calendar(
                        "forecast calendar must come from Alpaca research bars",
                    )
                    .into());
                };
                if payload["source"] != "alpaca" || artifact.provenance.source_family != "alpaca" {
                    return Err(
                        invalid_calendar("forecast calendar lacks Alpaca provenance").into(),
                    );
                }
                calendars
                    .scheduled
                    .push((asset, serde_json::from_value(calendar.clone())?));
                continue;
            }
            if !read_legacy_closes {
                continue;
            }
            let Some(symbol) = payload["resource"]
                .as_str()
                .and_then(|resource| resource.strip_prefix("bars:"))
                .and_then(|resource| resource.split(':').next())
            else {
                continue;
            };
            if let Some(closes) = payload.pointer("/value/forecast_session_closes") {
                let closes = serde_json::from_value(closes.clone())?;
                if let Some(previous) = calendars.legacy.insert(symbol.to_owned(), closes) {
                    if previous != calendars.legacy[symbol] {
                        return Err(
                            invalid_calendar("conflicting legacy exchange calendars").into()
                        );
                    }
                }
            }
        }
        Ok(calendars)
    }

    pub fn has_frozen_schedule(&self) -> bool {
        !self.scheduled.is_empty()
    }

    pub fn legacy_closes(&self) -> &BTreeMap<String, BTreeMap<String, DateTime<Utc>>> {
        &self.legacy
    }

    /// Require all four calendars to agree on the next five dates and closes.
    /// A missing or contradictory intermediate date cannot move a horizon.
    pub fn sessions_after(
        &self,
        baseline: NaiveDate,
    ) -> EvidenceRuntimeResult<[ScheduledSession; 5]> {
        let mut assets = BTreeSet::new();
        let mut common = None;
        for (asset, calendar) in &self.scheduled {
            if calendar.version != 1
                || calendar.availability_lag_minutes != BAR_AVAILABILITY_LAG_MINUTES
            {
                return Err(invalid_calendar(
                    "unsupported forecast calendar version or publication lag",
                )
                .into());
            }
            let next = calendar
                .session_closes
                .range((Excluded(baseline), Unbounded))
                .take(5)
                .map(|(&date, &close_at)| {
                    let available_at = close_at
                        .checked_add_signed(Duration::minutes(calendar.availability_lag_minutes))
                        .ok_or_else(|| {
                            invalid_calendar("invalid forecast calendar availability")
                        })?;
                    if crate::market_session_day(close_at) != date {
                        return Err(invalid_calendar(
                            "forecast close differs from its market date",
                        ));
                    }
                    Ok(ScheduledSession {
                        date,
                        close_at,
                        available_at,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next: [ScheduledSession; 5] = next.try_into().map_err(|_| {
                invalid_calendar("forecast calendar does not cover the next five sessions")
            })?;
            if common.is_some_and(|previous| previous != next) {
                return Err(invalid_calendar(
                    "conflicting scheduled exchange dates, closes or publication lag",
                )
                .into());
            }
            common = Some(next);
            assets.insert(*asset);
        }
        if !Asset::EXECUTABLE.iter().all(|asset| assets.contains(asset)) {
            return Err(invalid_calendar(
                "forecast timing requires all four governed exchange calendars",
            )
            .into());
        }
        common.ok_or_else(|| invalid_calendar("forecast calendar missing").into())
    }
}

fn invalid_calendar(message: &str) -> EvidenceAdapterError {
    EvidenceAdapterError::DataQuality(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_calendar(closes: &[(&str, &str)]) -> ForecastCalendars {
        ForecastCalendars {
            scheduled: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| {
                    (
                        asset,
                        CalendarMetadata {
                            version: 1,
                            availability_lag_minutes: 20,
                            session_closes: closes
                                .iter()
                                .map(|(day, time)| (day.parse().unwrap(), time.parse().unwrap()))
                                .collect(),
                        },
                    )
                })
                .collect(),
            legacy: BTreeMap::new(),
        }
    }

    /// WHY THIS TEST EXISTS (Task #9): The horizon dates use the frozen venue calendar across a holiday, weekend and shortened session.
    /// Do not weaken or delete this assertion.
    #[test]
    fn frozen_dates_keep_holidays_early_close_and_publication_lag() {
        let calendar = fixture_calendar(&[
            ("2026-11-25", "2026-11-25T21:00:00Z"),
            ("2026-11-27", "2026-11-27T18:00:00Z"),
            ("2026-11-30", "2026-11-30T21:00:00Z"),
            ("2026-12-01", "2026-12-01T21:00:00Z"),
            ("2026-12-02", "2026-12-02T21:00:00Z"),
            ("2026-12-03", "2026-12-03T21:00:00Z"),
            ("2026-12-04", "2026-12-04T21:00:00Z"),
        ]);
        let before_holiday = calendar
            .sessions_after("2026-11-25".parse().unwrap())
            .unwrap();
        assert_eq!(
            before_holiday[0].close_at,
            "2026-11-27T18:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            before_holiday[0].available_at,
            "2026-11-27T18:20:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            before_holiday[2].date,
            "2026-12-01".parse::<NaiveDate>().unwrap()
        );
        assert_eq!(
            before_holiday[4].date,
            "2026-12-03".parse::<NaiveDate>().unwrap()
        );
        let weekend = calendar
            .sessions_after("2026-11-28".parse().unwrap())
            .unwrap();
        assert_eq!(weekend[0].date, "2026-11-30".parse::<NaiveDate>().unwrap());

        let mut invalid = calendar;
        for (_, metadata) in &mut invalid.scheduled {
            metadata.availability_lag_minutes = 0;
        }
        let error = invalid
            .sessions_after("2026-11-25".parse().unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("publication lag"));
    }

    /// WHY THIS TEST EXISTS (Task #9): DST changes UTC availability without advancing a horizon when one asset lacks a scheduled date.
    /// Do not weaken or delete this assertion.
    #[test]
    fn frozen_dates_preserve_dst_and_reject_a_disputed_intermediary() {
        let mut calendar = fixture_calendar(&[
            ("2026-03-06", "2026-03-06T21:00:00Z"),
            ("2026-03-09", "2026-03-09T20:00:00Z"),
            ("2026-03-10", "2026-03-10T20:00:00Z"),
            ("2026-03-11", "2026-03-11T20:00:00Z"),
            ("2026-03-12", "2026-03-12T20:00:00Z"),
            ("2026-03-13", "2026-03-13T20:00:00Z"),
        ]);
        let baseline = "2026-03-05".parse().unwrap();
        let sessions = calendar.sessions_after(baseline).unwrap();
        assert_eq!(
            sessions[0].available_at,
            "2026-03-06T21:20:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            sessions[1].available_at,
            "2026-03-09T20:20:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        calendar.scheduled[3]
            .1
            .session_closes
            .remove(&"2026-03-09".parse().unwrap());
        assert!(calendar.sessions_after(baseline).is_err());
    }
}
