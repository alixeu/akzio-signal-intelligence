use orchestrator_core::{
    default_project_root, extract_json_artifact, normalize_probability, parse_tickers,
    replace_placeholders, run_slug,
};
use serde_json::json;

#[test]
fn core_public_helpers_match_python_compatibility_expectations() {
    let tickers = parse_tickers(" qqq, VIX, soxx,qqq ");
    assert_eq!(tickers, vec!["QQQ", "VIX", "SOXX"]);
    assert_eq!(run_slug(&tickers), "QQQ_VIX_SOXX");
    assert_eq!(normalize_probability(&json!("71%")), Some(0.71));
    assert_eq!(
        replace_placeholders("{ticker}:{lang}", &json!({"ticker": "QQQ", "lang": "zh"})),
        "QQQ:zh"
    );
    assert_eq!(
        extract_json_artifact("{\"ok\":true}").unwrap(),
        json!({"ok": true})
    );
}

#[test]
fn default_project_root_points_at_workspace_root() {
    assert!(default_project_root().join("Cargo.toml").exists());
    assert!(default_project_root().join("prompts").exists());
}
