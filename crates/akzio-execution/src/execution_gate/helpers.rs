fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn outside_freshness_window(
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: i64,
    max_future_skew_secs: i64,
) -> bool {
    let age = now.signed_duration_since(observed_at);
    age > Duration::seconds(max_age_secs) || age < -Duration::seconds(max_future_skew_secs)
}

fn snapshot_skewed(observed_at: [DateTime<Utc>; 3], max_skew_secs: i64) -> bool {
    let oldest = observed_at.into_iter().min().expect("three snapshots");
    let newest = observed_at.into_iter().max().expect("three snapshots");
    newest.signed_duration_since(oldest) > Duration::seconds(max_skew_secs)
}
