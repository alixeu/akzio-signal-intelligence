//! Explicitly opted-in network test. The normal suite never spends LLM tokens.
use super::*;
use akzio_domain::EvidenceAcquisitionMode;
use serde_json::Value;

#[tokio::test]
#[ignore = "requires AKZIO_REAL_LLM_CONFIG and AKZIO_REAL_LLM_OUTPUT; calls deployment LLM routes"]
async fn real_news_uses_deployment_routes_and_verifies_sources() -> Result<()> {
    let config_path = PathBuf::from(
        std::env::var("AKZIO_REAL_LLM_CONFIG").context("set the deployment config path")?,
    );
    let output = PathBuf::from(
        std::env::var("AKZIO_REAL_LLM_OUTPUT").context("set a new workspace output directory")?,
    );
    fs::create_dir(&output)?;
    let mut config = read_config_file(&config_path)?;
    // Explicit local-test correction for a deployment file with transposed
    // endpoint/key fields. Never changes that file or silently falls back.
    let swapped = std::env::var("AKZIO_REAL_LLM_SWAP_ENDPOINT_FIELDS").as_deref() == Ok("1");
    if swapped {
        let model = config
            .model
            .as_mut()
            .context("deployment model is required")?;
        std::mem::swap(&mut model.base_url, &mut model.api_key);
    }
    resolve_model_configuration(&mut config)?;
    let model = config
        .model
        .as_ref()
        .context("deployment model is required")?;
    let discovery = model
        .routes
        .get("evidence.news_web")
        .map(|r| model.for_route(r))
        .unwrap_or_else(|| model.clone());
    let reviewer = model
        .routes
        .get("research.critic")
        .map(|r| model.for_route(r))
        .unwrap_or_else(|| model.clone());
    fs::write(
        output.join("identity.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "config_path":config_path,"config_hash":akzio_domain::ContentHash::of_bytes(&fs::read(&config_path)?),
            "endpoint_key_fields_swapped_in_memory":swapped,
            "discovery":{"model":discovery.model,"reasoning_effort":discovery.reasoning_effort},
            "reviewer":{"model":reviewer.model,"reasoning_effort":reviewer.reasoning_effort},
            "broker_calls":0,"scope":"production news router and source review; no Store or execution"
        }))?,
    )?;
    let adapter = akzio_ingest::configured_news_evidence_transport(model)?;
    let end = chrono::Utc::now().date_naive();
    let start = end - ChronoDuration::days(7);
    let need = akzio_domain::EvidenceNeed {
        schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
        source_family: "news_web".into(),
        resource: format!("news:QQQ:{start}:{end}:market"),
        max_age_secs: 604800,
    };
    let request = EvidenceRequest {
        source: EvidenceSource::NewsWeb,
        resource: need.resource.clone(),
        max_age: ChronoDuration::days(7),
        acquisition_mode: akzio_domain::evidence_acquisition_mode(RunPurpose::PositionPlan, &need),
    };
    let result = adapter.acquire(&request).await;
    if let Err(error) = &result {
        fs::write(
            output.join("failure.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"stage":"acquire","error":error.to_string()}),
            )?,
        )?;
    }
    let acquired = result?;
    fs::write(output.join("raw.ndjson"), &acquired.raw)?;
    fs::write(
        output.join("normalized.json"),
        serde_json::to_vec_pretty(&acquired.normalized)?,
    )?;
    fs::write(
        output.join("quality.json"),
        serde_json::to_vec_pretty(&acquired.quality)?,
    )?;
    assert_eq!(
        request.acquisition_mode,
        EvidenceAcquisitionMode::ModelReviewed
    );
    assert_eq!(
        acquired
            .normalized
            .pointer("/source_review/reviewer/model")
            .and_then(Value::as_str),
        Some(reviewer.model.as_str())
    );
    assert_eq!(
        acquired
            .normalized
            .pointer("/source_review/status")
            .and_then(Value::as_str),
        Some("model_reviewed"),
        "inspect normalized.json and raw.ndjson; an HTTP success is not enough"
    );
    assert!(acquired.quality.citations_complete);
    assert!(!acquired.normalized["reviewed_facts"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        acquired.normalized["source_review"]["human_review"],
        "not_performed"
    );
    assert_eq!(acquired.normalized["source_document"]["fetch_count"], 0);
    let time_basis = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(
        &request,
        &acquired,
        chrono::Utc::now(),
    )?;
    fs::write(
        output.join("validated-time-basis.json"),
        serde_json::to_vec_pretty(&time_basis)?,
    )?;
    println!(
        "real news source review passed; evidence: {}",
        output.display()
    );
    Ok(())
}
