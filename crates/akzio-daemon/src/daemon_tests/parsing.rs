#[test]
fn daily_bar_parser_is_decimal_safe_and_rejects_duplicate_dates() {
    let observed_at = DateTime::parse_from_rfc3339("2026-08-11T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let bars = parse_daily_bars(
        &serde_json::json!({
            "bars": [
                {"t": "2026-08-10T20:00:00Z", "c": 100.25},
                {"t": "2026-08-11T20:00:00Z", "c": "-0.5"}
            ]
        }),
        observed_at,
    )
    .unwrap();
    assert_eq!(
        bars[&NaiveDate::from_ymd_opt(2026, 8, 10).unwrap()],
        MoneyMicros(100_250_000)
    );
    assert_eq!(
        bars[&NaiveDate::from_ymd_opt(2026, 8, 11).unwrap()],
        MoneyMicros(-500_000)
    );

    let duplicate = parse_daily_bars(
        &serde_json::json!({
            "bars": [
                {"t": "2026-08-10T20:00:00Z", "c": 100.25},
                {"t": "2026-08-10T20:00:00Z", "c": 101.25}
            ]
        }),
        observed_at,
    );
    assert!(matches!(
        duplicate,
        Err(PaperDecodeError::Unavailable(message)) if message == "daily bar date is duplicated"
    ));
}
