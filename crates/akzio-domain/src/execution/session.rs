/// Provider-calendar-derived session. `is_open` remains the broker's regular-hours flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingSession {
    PreMarket,
    Regular,
    AfterHours,
    Overnight,
    Closed,
}

/// Trading API currently encodes these flags in `attributes`; accept the
/// documented boolean fields too. Explicit false and halt always fail closed.
pub fn alpaca_overnight_asset_available(value: &serde_json::Value, asset: Asset) -> bool {
    let flag = |name: &str| -> Option<bool> {
        if let Some(value) = value.get(name) {
            return value.as_bool();
        }
        let attributes = value.get("attributes")?.as_array()?;
        if attributes.iter().any(|v| !v.is_string()) {
            return None;
        }
        Some(attributes.iter().any(|v| v.as_str() == Some(name)))
    };
    value["symbol"] == asset.symbol()
        && value["status"] == "active"
        && value["tradable"] == true
        && flag("overnight_tradable") == Some(true)
        && flag("overnight_halted") == Some(false)
}

impl TradingSession {
    pub fn extended_hours(self) -> bool {
        matches!(self, Self::PreMarket | Self::AfterHours | Self::Overnight)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingSessionSnapshot {
    pub kind: TradingSession,
    pub trade_date: chrono::NaiveDate,
    pub next_open: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    /// Assets explicitly reported overnight-tradable and not overnight-halted.
    pub overnight_assets: BTreeSet<Asset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeSession {
    pub date: chrono::NaiveDate,
    pub open: chrono::NaiveTime,
    pub close: chrono::NaiveTime,
}

impl ExchangeSession {
    pub fn from_alpaca(value: &serde_json::Value) -> Result<Vec<Self>, DomainError> {
        let invalid = || DomainError::InvalidBudget {
            field: "trading_session.calendar",
        };
        value
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .map(|row| {
                let text = |field| {
                    row.get(field)
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(invalid)
                };
                let time = |field| {
                    chrono::NaiveTime::parse_from_str(text(field)?, "%H:%M")
                        .or_else(|_| {
                            chrono::NaiveTime::parse_from_str(text(field).unwrap_or(""), "%H:%M:%S")
                        })
                        .map_err(|_| invalid())
                };
                Ok(Self {
                    date: chrono::NaiveDate::parse_from_str(text("date")?, "%Y-%m-%d")
                        .map_err(|_| invalid())?,
                    open: time("open")?,
                    close: time("close")?,
                })
            })
            .collect()
    }
}

impl TradingSessionSnapshot {
    pub fn from_calendar(
        now: DateTime<Utc>,
        regular_open: bool,
        calendar: &[ExchangeSession],
    ) -> Result<Self, DomainError> {
        use chrono::TimeZone;
        let ny = chrono_tz::America::New_York;
        let local_date = now.with_timezone(&ny).date_naive();
        let at = |date: chrono::NaiveDate, time| {
            ny.from_local_datetime(&date.and_time(time))
                .single()
                .map(|value| value.with_timezone(&Utc))
                .ok_or(DomainError::InvalidBudget {
                    field: "trading_session.calendar_time",
                })
        };
        let hour = |h| chrono::NaiveTime::from_hms_opt(h, 0, 0).expect("fixed hour");
        let mut next_open = None;
        for day in calendar {
            if day.open >= day.close || day.open < hour(4) || day.close > hour(20) {
                return Err(DomainError::InvalidBudget {
                    field: "trading_session.calendar",
                });
            }
            let prior = day.date.pred_opt().ok_or(DomainError::InvalidBudget {
                field: "trading_session.date",
            })?;
            let overnight_start = at(prior, hour(20))?;
            let pre_start = at(day.date, hour(4))?;
            let open = at(day.date, day.open)?;
            let close = at(day.date, day.close)?;
            let end = at(day.date, hour(20))?;
            if overnight_start > now {
                next_open = Some(
                    next_open.map_or(overnight_start, |v: DateTime<Utc>| v.min(overnight_start)),
                );
            }
            let active = if now >= overnight_start && now < pre_start {
                Some((TradingSession::Overnight, pre_start))
            } else if now >= pre_start && now < open {
                Some((TradingSession::PreMarket, open))
            } else if now >= open && now < close && regular_open {
                Some((TradingSession::Regular, close))
            } else if now >= close && now < end {
                Some((TradingSession::AfterHours, end))
            } else {
                None
            };
            if let Some((kind, ends_at)) = active {
                return Ok(Self {
                    kind,
                    trade_date: day.date,
                    next_open: None,
                    ends_at: Some(ends_at),
                    overnight_assets: BTreeSet::new(),
                });
            }
        }
        Ok(Self {
            kind: TradingSession::Closed,
            trade_date: local_date,
            next_open,
            ends_at: None,
            overnight_assets: BTreeSet::new(),
        })
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;

    #[test]
    fn provider_calendar_drives_sessions_holidays_early_close_and_trade_date() {
        let calendar = ExchangeSession::from_alpaca(&serde_json::json!([
            {"date":"2026-09-04", "open":"09:30", "close":"16:00"},
            {"date":"2026-09-08", "open":"09:30", "close":"16:00"},
            {"date":"2026-11-27", "open":"09:30", "close":"13:00"}
        ]))
        .unwrap();
        for (time, regular_open, expected, date) in [
            (
                "2026-09-04T03:59:59-04:00",
                false,
                TradingSession::Overnight,
                "2026-09-04",
            ),
            (
                "2026-09-04T04:00:00-04:00",
                false,
                TradingSession::PreMarket,
                "2026-09-04",
            ),
            (
                "2026-09-04T09:30:00-04:00",
                true,
                TradingSession::Regular,
                "2026-09-04",
            ),
            (
                "2026-09-04T16:00:00-04:00",
                false,
                TradingSession::AfterHours,
                "2026-09-04",
            ),
            (
                "2026-09-04T20:00:00-04:00",
                false,
                TradingSession::Closed,
                "2026-09-04",
            ),
            (
                "2026-09-05T12:00:00-04:00",
                false,
                TradingSession::Closed,
                "2026-09-05",
            ),
            (
                "2026-09-06T20:00:00-04:00",
                false,
                TradingSession::Closed,
                "2026-09-06",
            ),
            (
                "2026-09-07T20:00:00-04:00",
                false,
                TradingSession::Overnight,
                "2026-09-08",
            ),
            (
                "2026-11-27T03:59:59-05:00",
                false,
                TradingSession::Overnight,
                "2026-11-27",
            ),
            (
                "2026-11-27T13:00:00-05:00",
                false,
                TradingSession::AfterHours,
                "2026-11-27",
            ),
        ] {
            let session = TradingSessionSnapshot::from_calendar(
                time.parse().unwrap(),
                regular_open,
                &calendar,
            )
            .unwrap();
            assert_eq!(session.kind, expected, "{time}");
            assert_eq!(session.trade_date.to_string(), date, "{time}");
        }
        let closed = TradingSessionSnapshot::from_calendar(
            "2026-09-05T12:00:00-04:00".parse().unwrap(),
            false,
            &calendar,
        )
        .unwrap();
        assert_eq!(
            closed.next_open.unwrap(),
            "2026-09-07T20:00:00-04:00"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        let no_calendar = TradingSessionSnapshot::from_calendar(Utc::now(), true, &[]).unwrap();
        assert_eq!(no_calendar.kind, TradingSession::Closed);
    }

    #[test]
    fn legacy_order_hash_fields_remain_unchanged() {
        let old = serde_json::json!({"asset":"QQQ","side":"buy","notional":1000000,"limit_price":1000000});
        let decoded: OrderIntent = serde_json::from_value(old.clone()).unwrap();
        assert!(!decoded.extended_hours);
        assert_eq!(serde_json::to_value(decoded).unwrap(), old);
    }

    #[test]
    fn overnight_asset_flags_match_actual_trading_api_and_reject_halts() {
        let mut value = serde_json::json!({"symbol":"QQQ","status":"active","tradable":true,
            "attributes":["fractional_eh_enabled","overnight_tradable"]});
        assert!(alpaca_overnight_asset_available(&value, Asset::Qqq));
        value["attributes"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!("overnight_halted"));
        assert!(!alpaca_overnight_asset_available(&value, Asset::Qqq));
        value.as_object_mut().unwrap().remove("attributes");
        assert!(!alpaca_overnight_asset_available(&value, Asset::Qqq));
        value["overnight_tradable"] = serde_json::json!(true);
        value["overnight_halted"] = serde_json::json!(false);
        assert!(alpaca_overnight_asset_available(&value, Asset::Qqq));
        assert!(!alpaca_overnight_asset_available(&value, Asset::Soxl));
        value["overnight_tradable"] = serde_json::json!(false);
        assert!(!alpaca_overnight_asset_available(&value, Asset::Qqq));
    }
}
