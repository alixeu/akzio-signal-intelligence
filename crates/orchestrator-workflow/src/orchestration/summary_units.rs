//! Stable identifiers for Rust-owned Phase Summary indexes.

use orchestrator_core::md5_3;

const INDEX_ID_DOMAIN: &[u8] = b"akzio.phase_summary.index.v1\0";

/// Stable Index ID for one Rust-owned summary. Length-prefixed fields keep the
/// preimage unambiguous and preserve the distinction between `None` and `""`.
pub fn derive_summary_index_id(
    run_id: &str,
    source_phase: u8,
    role: &str,
    ticker: Option<&str>,
    topic_id: Option<&str>,
    unit_key: &str,
    source_payload_hash: &str,
) -> String {
    let mut preimage = INDEX_ID_DOMAIN.to_vec();
    push_field(&mut preimage, run_id.as_bytes());
    push_field(&mut preimage, source_phase.to_string().as_bytes());
    push_field(&mut preimage, role.as_bytes());
    push_optional_field(&mut preimage, ticker);
    push_optional_field(&mut preimage, topic_id);
    push_field(&mut preimage, unit_key.as_bytes());
    push_field(&mut preimage, source_payload_hash.as_bytes());
    format!("idx-{}", md5_3(preimage))
}

fn push_field(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn push_optional_field(output: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            output.push(1);
            push_field(output, value.as_bytes());
        }
        None => output.push(0),
    }
}

#[cfg(test)]
mod tests {
    use super::derive_summary_index_id;

    #[test]
    fn aggregate_identity_is_stable_and_distinguishes_optional_fields() {
        let aggregate = derive_summary_index_id(
            "run-1",
            3,
            "manager.research",
            None,
            None,
            "phase3:manager.research:artifact:aggregate:none:0",
            "sha256:source",
        );
        assert_eq!(aggregate.len(), 10);
        assert!(aggregate.starts_with("idx-"));
        assert_ne!(
            aggregate,
            derive_summary_index_id(
                "run-1",
                3,
                "manager.research",
                Some(""),
                None,
                "phase3:manager.research:artifact:aggregate:none:0",
                "sha256:source",
            )
        );
    }
}
