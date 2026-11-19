use akzio_domain::{ContentHash, DecisionClock};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AcquiredEvidence, EvidenceRequest, EvidenceSource, FredDirectTransport, GovernedResource,
    NormalizedEvidencePayload,
};

const AVAILABILITY_PROOF_VERSION: u32 = 1;

/// Receipt and source-specific proof sealed alongside the normalized value and its raw reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAvailabilityProof {
    version: u32,
    captured_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bars: Option<BarRequestBasis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fred_snapshot: Option<FredPointSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BarRequestBasis {
    requested_cutoff: Option<DateTime<Utc>>,
    requested_feed: Option<String>,
    requested_adjustment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FredPointSnapshot {
    vintage: NaiveDate,
    request_uri: String,
    raw_hash: ContentHash,
}

impl EvidenceAvailabilityProof {
    pub(crate) fn seal(
        request: &EvidenceRequest,
        governed: &GovernedResource,
        acquired: &AcquiredEvidence,
    ) -> Self {
        let bars = matches!(governed, GovernedResource::AlpacaBars { .. }).then(|| {
            let uri = Url::parse(&acquired.source_uri).ok();
            let query_param = |name: &str| {
                uri.as_ref()?
                    .query_pairs()
                    .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
            };
            BarRequestBasis {
                requested_cutoff: acquired
                    .normalized
                    .get("decision_cutoff")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok()),
                requested_feed: query_param("feed"),
                requested_adjustment: query_param("adjustment"),
            }
        });
        let fred_snapshot = FredDirectTransport::verify_point_snapshot(request, acquired).map(
            |(vintage, raw_hash)| FredPointSnapshot {
                vintage,
                request_uri: acquired.source_uri.clone(),
                raw_hash,
            },
        );
        Self {
            version: AVAILABILITY_PROOF_VERSION,
            captured_at: acquired.observed_at,
            bars,
            fred_snapshot,
        }
    }
}

/// Why the frozen evidence can supply facts at a particular cutoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceAvailabilityBasis {
    Captured { captured_at: DateTime<Utc> },
    FredPointSnapshot { vintage: NaiveDate },
}

/// Stable reasons for withholding historical qualification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceAvailabilityUnknown {
    MissingProof,
    UnsupportedProofVersion,
    ReceiptMismatch,
    SourceTimeOutsideCutoff,
    LateCaptureWithoutVersion,
    FredPointSnapshotUnverified,
}

impl EvidenceAvailabilityUnknown {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingProof => "availability_proof_missing",
            Self::UnsupportedProofVersion => "availability_proof_version_unsupported",
            Self::ReceiptMismatch => "availability_receipt_mismatch",
            Self::SourceTimeOutsideCutoff => "availability_source_time_outside_cutoff",
            Self::LateCaptureWithoutVersion => "availability_late_capture_without_version",
            Self::FredPointSnapshotUnverified => "availability_fred_point_snapshot_unverified",
        }
    }
}

impl std::fmt::Display for EvidenceAvailabilityUnknown {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl NormalizedEvidencePayload {
    /// Qualify a sealed evidence version at an explicit cutoff for historical or supplemental use.
    pub fn availability_at(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<EvidenceAvailabilityBasis, EvidenceAvailabilityUnknown> {
        use EvidenceAvailabilityUnknown as Unknown;

        let proof = self
            .availability_proof
            .as_ref()
            .ok_or(Unknown::MissingProof)?;
        if proof.version != AVAILABILITY_PROOF_VERSION {
            return Err(Unknown::UnsupportedProofVersion);
        }
        let captured_at = proof.captured_at;
        if captured_at != self.observed_at
            || captured_at != self.provenance.observed_at
            || captured_at != self.time_basis.retrieved_at
        {
            return Err(Unknown::ReceiptMismatch);
        }
        let clock = DecisionClock {
            decision_cutoff: cutoff,
        };
        self.time_basis
            .validate_at(&clock)
            .map_err(|_| Unknown::SourceTimeOutsideCutoff)?;
        if self.provenance.source_uri.starts_with("fixture://")
            && proof.bars.is_some()
            && self.value.get("session_closes").is_none()
            && self
                .time_basis
                .event_time
                .is_some_and(|event| !clock.contains_completed_daily_bar(event))
        {
            return Err(Unknown::SourceTimeOutsideCutoff);
        }
        if clock.contains(captured_at) {
            return Ok(EvidenceAvailabilityBasis::Captured { captured_at });
        }
        if self.source != EvidenceSource::Fred {
            return Err(Unknown::LateCaptureWithoutVersion);
        }
        // This attestation is generated only by rerunning the direct adapter from raw bytes
        // before sealing the normalized CAS artifact. The source date alone proves nothing.
        let snapshot = proof
            .fred_snapshot
            .as_ref()
            .filter(|snapshot| {
                Some(snapshot.vintage) == self.time_basis.vintage
                    && snapshot.request_uri == self.provenance.source_uri
                    && self.provenance.dedupe_key == format!("fred:{}", snapshot.raw_hash)
            })
            .ok_or(Unknown::FredPointSnapshotUnverified)?;
        Ok(EvidenceAvailabilityBasis::FredPointSnapshot {
            vintage: snapshot.vintage,
        })
    }
}
