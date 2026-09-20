use super::*;
use akzio_domain::{AccountSnapshot, DecisionValidity, MarketClockSnapshot, QuoteSnapshot};

/// Read projection of the existing governed normalized payload, not a second
/// persisted evidence format. Remaining provider content is irrelevant here.
#[derive(Debug, Deserialize)]
pub(super) struct FrozenAccountComponent {
    pub source: String,
    pub resource: String,
    pub observed_at: DateTime<Utc>,
    pub need: ArtifactRef,
    pub raw: ArtifactRef,
}

pub(super) fn frozen_account_observations(
    snapshot: &Artifact,
    account: &AccountSnapshot,
    components: &[(Artifact, FrozenAccountComponent)],
) -> Option<Vec<DateTime<Utc>>> {
    if snapshot.producer != "execution.snapshot.account" {
        return None;
    }
    if snapshot.kind != ArtifactKind::NormalizedEvidence
        || snapshot.lifecycle != ArtifactLifecycle::Canonical
        || snapshot.provenance.source_family != "alpaca"
        || snapshot.origin.is_none()
    {
        return None;
    }
    let references = snapshot
        .source_refs
        .iter()
        .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
        .collect::<Vec<_>>();
    if references.len() != components.len() {
        return None;
    }
    // The governed single-account snapshot is still produced by
    // materialize_paper_single_snapshot. A composed snapshot has four sources
    // and no single URI; neither path may silently fall back to aggregate time.
    let expected: std::collections::BTreeSet<String> = match components.len() {
        1 if snapshot.provenance.source_uri.is_some() => ["paper.account".into()].into(),
        4 => [
            "paper.account".into(),
            "paper.positions".into(),
            "paper.open_orders".into(),
            format!("paper.fills:{}", account.broker_session),
        ]
        .into(),
        _ => return None,
    };
    let mut resources = std::collections::BTreeSet::new();
    let mut observed = vec![account.observed_at];
    for (artifact, payload) in components {
        if artifact.kind != ArtifactKind::NormalizedEvidence
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
            || artifact.producer != "akzio.ingest.alpaca.normalized"
            || artifact.origin != snapshot.origin
            || artifact.provenance.source_family != "alpaca"
            || artifact.provenance.observed_at != Some(payload.observed_at)
            || payload.source != "alpaca"
            || payload.need.kind != ArtifactKind::EvidenceNeed
            || payload.raw.kind != ArtifactKind::RawEvidence
            || !artifact.source_refs.contains(&payload.need)
            || !artifact.source_refs.contains(&payload.raw)
            || !snapshot.source_refs.contains(&payload.raw)
            || !references
                .iter()
                .any(|reference| reference.artifact_id == artifact.artifact_id)
            || !resources.insert(payload.resource.clone())
        {
            return None;
        }
        observed.push(payload.observed_at);
    }
    (resources == expected).then_some(observed)
}

/// Ephemeral permission to send an order, derived from the immutable plan's
/// sources. It is deliberately not serialized into or added to old plan hashes.
/// An absent window permits recovery reads only, never a new broker effect.
#[derive(Debug, Clone)]
pub struct PaperSubmissionAuthorization {
    plan_hash: ContentHash,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    session: Option<akzio_domain::TradingSessionSnapshot>,
}

impl PaperSubmissionAuthorization {
    pub(super) fn matches_session(
        &self,
        session: &akzio_domain::TradingSessionSnapshot,
        legacy_session: &str,
    ) -> bool {
        self.session.as_ref().map_or_else(
            || {
                session.kind == akzio_domain::TradingSession::Regular
                    && session.trade_date.to_string() == legacy_session
            },
            |frozen| frozen.kind == session.kind && frozen.trade_date == session.trade_date,
        )
    }
    pub(super) fn restrict_account_observations(
        &mut self,
        observations: Option<&[DateTime<Utc>]>,
        policy: &crate::ExecutionPolicy,
    ) {
        let Some(observations) = observations else {
            self.window = None;
            return;
        };
        let Some((mut start, mut end)) = self.window else {
            return;
        };
        for observed_at in observations {
            let bounds = chrono::Duration::try_seconds(policy.max_account_age_secs)
                .and_then(|age| observed_at.checked_add_signed(age))
                .zip(
                    chrono::Duration::try_seconds(policy.max_future_skew_secs)
                        .and_then(|skew| observed_at.checked_sub_signed(skew)),
                );
            let Some((expiry, earliest)) = bounds else {
                self.window = None;
                return;
            };
            start = start.max(earliest);
            end = end.min(expiry);
        }
        self.window = (start <= end).then_some((start, end));
    }

    pub(super) fn from_frozen_sources(
        plan: &ExecutionPlan,
        policy: &crate::ExecutionPolicy,
        validity: Option<&DecisionValidity>,
        approval_expires_at: Option<DateTime<Utc>>,
        account: &AccountSnapshot,
        quotes: &QuoteSnapshot,
        clock: &MarketClockSnapshot,
    ) -> Result<Self> {
        let mut authorization = Self {
            plan_hash: plan.plan_hash.clone(),
            window: None,
            session: clock.session.clone(),
        };
        let Some(validity) = validity else {
            return Ok(authorization);
        };
        let Some(approval_expires_at) = approval_expires_at else {
            return Ok(authorization);
        };
        validity.validate()?;
        account.validate()?;
        quotes.validate()?;
        clock.validate()?;
        let policy_hash = policy
            .policy_hash()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        if policy_hash != plan.policy_hash
            || account.broker_session != plan.broker_session
            || quotes.broker_session != plan.broker_session
            || clock.broker_session != plan.broker_session
            || !clock.tradable()
        {
            return Ok(authorization);
        }
        let mut starts_at = validity.generated_at;
        let mut expires_at = validity.valid_until.min(approval_expires_at);
        if let Some(end) = clock.session.as_ref().and_then(|s| s.ends_at) {
            expires_at = expires_at.min(end);
        }
        let mut observations = vec![
            (account.observed_at, policy.max_account_age_secs),
            (quotes.observed_at, policy.max_quote_age_secs),
            (clock.observed_at, policy.max_clock_age_secs),
        ];
        for order in &plan.orders {
            let Some(quote) = quotes.quotes.get(&order.asset) else {
                return Ok(authorization);
            };
            observations.push((quote.observed_at, policy.max_quote_age_secs));
        }
        for (observed_at, max_age) in observations {
            let Some(expiry) = chrono::Duration::try_seconds(max_age)
                .and_then(|age| observed_at.checked_add_signed(age))
            else {
                return Ok(authorization);
            };
            let Some(start) = chrono::Duration::try_seconds(policy.max_future_skew_secs)
                .and_then(|skew| observed_at.checked_sub_signed(skew))
            else {
                return Ok(authorization);
            };
            starts_at = starts_at.max(start);
            expires_at = expires_at.min(expiry);
        }
        if starts_at <= expires_at {
            authorization.window = Some((starts_at, expires_at));
        }
        Ok(authorization)
    }

    pub(super) fn assert_current(&self, plan_hash: &ContentHash, now: DateTime<Utc>) -> Result<()> {
        if self.plan_hash != *plan_hash
            || !self
                .window
                .is_some_and(|(start, end)| start <= now && now <= end)
        {
            return Err(PaperError::SubmissionUnauthorized);
        }
        Ok(())
    }

    pub(super) fn assert_replacement_current(&self, now: DateTime<Utc>) -> Result<()> {
        self.assert_current(&self.plan_hash, now)
    }
}
