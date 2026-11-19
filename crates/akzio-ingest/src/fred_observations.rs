use akzio_domain::fred_observations::{assess_response, ObservationScope};
use serde_json::Value;

use crate::{EvidenceAdapterError, EvidenceQuality, GovernedResource};

pub(crate) fn normalize_observations(
    governed: &GovernedResource,
    value: &mut Value,
) -> Result<EvidenceQuality, EvidenceAdapterError> {
    let GovernedResource::Fred {
        window_start,
        window_end,
        vintage,
        ..
    } = governed
    else {
        return Err(EvidenceAdapterError::SourceMismatch);
    };
    let scope = ObservationScope::for_response(value, *window_start, *window_end, *vintage)
        .ok_or_else(|| {
            EvidenceAdapterError::DataQuality(
                "FRED observations have no valid point vintage".to_owned(),
            )
        })?;
    let (usable, coverage) = assess_response(value, &scope).ok_or_else(|| {
        EvidenceAdapterError::DataQuality("FRED observation page exceeds its row bound".to_owned())
    })?;
    let quality = EvidenceQuality {
        completeness_ppm: coverage.completeness_ppm(),
        ..EvidenceQuality::default()
    };
    let usable = usable.into_iter().cloned().collect();
    value["observations"] = Value::Array(usable);
    value["observation_quality"] = serde_json::to_value(coverage).map_err(|_| {
        EvidenceAdapterError::DataQuality("FRED observation coverage encoding failed".to_owned())
    })?;
    Ok(quality)
}
