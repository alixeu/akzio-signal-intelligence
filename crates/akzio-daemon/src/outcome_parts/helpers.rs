use super::*;

pub(super) fn next_outcome_check_at(now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let today = DateTime::from_naive_utc_and_offset(
        now.date_naive()
            .and_hms_opt(22, 0, 0)
            .expect("22:00 UTC is a valid time"),
        Utc,
    );
    if today > now {
        return Ok(today);
    }
    let tomorrow = now
        .date_naive()
        .succ_opt()
        .ok_or_else(|| DaemonError::Unavailable("outcome check date overflow".to_owned()))?;
    Ok(DateTime::from_naive_utc_and_offset(
        tomorrow
            .and_hms_opt(22, 0, 0)
            .expect("22:00 UTC is a valid time"),
        Utc,
    ))
}
