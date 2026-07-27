#![allow(dead_code)] // monitor prompt selection remains a supported renderer mode.

use anyhow::{bail, Context, Result};
use orchestrator_core::{render_template, ComponentPlugin};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::lifecycle::{research_plan_to_trade_intent, tickers_from_state, topic_state};
use orchestrator_core::ComponentRegistry;

pub(crate) fn mode_prompt_path(base: &std::path::Path, state: &Value) -> PathBuf {
    if state.get("mode").and_then(Value::as_str) != Some("monitor") {
        return base.to_path_buf();
    }
    let Some(stem) = base.file_stem().and_then(|value| value.to_str()) else {
        return base.to_path_buf();
    };
    let candidate = base.with_file_name(format!("{stem}_monitor.md"));
    if candidate.exists() {
        candidate
    } else {
        base.to_path_buf()
    }
}

/// Load a shared prompt component from `prompts/common/<file_name>` relative to
/// the role prompt path. Missing components resolve to an empty string so a role
/// prompt that does not reference the placeholder is unaffected.
fn prompts_dir_from_prompt_path(prompt_path: Option<&std::path::Path>) -> Option<PathBuf> {
    let path = prompt_path?;
    for ancestor in path.ancestors() {
        if ancestor.join("common").is_dir() || ancestor.join("roles").is_dir() {
            return Some(ancestor.to_path_buf());
        }
    }
    path.parent()?.parent().map(PathBuf::from)
}

fn prompt_component(prompt_path: Option<&std::path::Path>, relative_path: &str) -> Result<String> {
    let Some(prompts_dir) = prompts_dir_from_prompt_path(prompt_path) else {
        return Ok(String::new());
    };
    let component_path = prompts_dir.join(relative_path);
    if component_path.exists() {
        std::fs::read_to_string(&component_path).with_context(|| {
            format!(
                "failed to read prompt template {}",
                component_path.display()
            )
        })
    } else {
        Ok(String::new())
    }
}

/// Return lower-snake-case placeholders from the original template only.
///
/// This intentionally mirrors `render_template`: JSON braces and replacement
/// text are not placeholders. The renderer uses this before materialising
/// values so unused context and components are never loaded or serialised.
pub(crate) fn raw_template_placeholders(template: &str) -> BTreeSet<String> {
    let bytes = template.as_bytes();
    let mut placeholders = BTreeSet::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'{' {
            cursor += 1;
            continue;
        }
        let start = cursor + 1;
        let Some(end_offset) = bytes[start..].iter().position(|byte| *byte == b'}') else {
            break;
        };
        let end = start + end_offset;
        let name = &template[start..end];
        if !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            placeholders.insert(name.to_string());
        }
        cursor = end + 1;
    }
    placeholders
}

fn add_template_placeholders(placeholders: &mut BTreeSet<String>, template: &str) {
    placeholders.extend(raw_template_placeholders(template));
}

fn referenced_components<'a>(
    component_registry: Option<&'a ComponentRegistry>,
    role: &str,
    placeholders: &mut BTreeSet<String>,
) -> Vec<&'a ComponentPlugin> {
    let Some(registry) = component_registry else {
        return Vec::new();
    };
    let candidates = registry.for_role(role);
    let mut selected = Vec::new();
    loop {
        let mut changed = false;
        for plugin in &candidates {
            if !placeholders.contains(&plugin.manifest.placeholder_key)
                || selected.iter().any(|selected: &&ComponentPlugin| {
                    selected.manifest.name == plugin.manifest.name
                })
            {
                continue;
            }
            selected.push(*plugin);
            placeholders.extend(plugin.manifest.required_variables.iter().cloned());
            add_template_placeholders(placeholders, &plugin.template);
            changed = true;
        }
        if !changed {
            break;
        }
    }
    selected
}

fn insert_if_referenced(
    values: &mut serde_json::Map<String, Value>,
    placeholders: &BTreeSet<String>,
    key: &str,
    value: impl FnOnce() -> Result<Value>,
) -> Result<()> {
    if placeholders.contains(key) {
        values.insert(key.to_string(), value()?);
    }
    Ok(())
}

fn phase3_context(state: &Value) -> Value {
    let input_tickers = tickers_from_state(state);
    let primary_ticker = input_tickers.first().cloned();
    let probability_base = state
        .get("weighted_probability_base")
        .filter(|value| !value.is_null())
        .cloned();

    json!({
        "status": if probability_base.is_some() { "available" } else { "not_loaded" },
        "item_count": probability_base.as_ref().and_then(Value::as_object).map(|items| items.len()).unwrap_or(0),
        "source": "rust_deterministic_probability_baseline",
        "input_tickers": input_tickers,
        "primary_ticker": primary_ticker,
        "weighted_probability_base": probability_base.unwrap_or(Value::Null),
        "analyst_weights": state.get("analyst_weights").cloned().unwrap_or(Value::Null)
    })
}

fn contains_leveraged_etf(tickers: &[String]) -> bool {
    tickers.iter().any(|ticker| {
        matches!(
            ticker.trim().to_ascii_uppercase().as_str(),
            "TQQQ" | "SQQQ" | "SOXL" | "SOXS" | "UPRO" | "SPXU"
        )
    })
}

fn retrieval_bootstrap(state: &Value, current_phase: i64) -> Value {
    let mut counts = serde_json::Map::new();
    let mut total = 0usize;
    if let Some(completed) = state.get("phase_compress").and_then(Value::as_object) {
        for (phase, summary) in completed {
            let Ok(phase) = phase.parse::<i64>() else {
                continue;
            };
            if phase >= current_phase {
                continue;
            }
            let count = summary
                .get("index_ids")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            counts.insert(phase.to_string(), json!(count));
            total += count;
        }
    }
    let conflict_count = state
        .get("phase1_index")
        .and_then(|value| value.get("per_ticker"))
        .and_then(Value::as_object)
        .map(|items| {
            items
                .values()
                .filter_map(|item| {
                    item.get("cross_analyst_conflicts")
                        .and_then(Value::as_array)
                })
                .map(Vec::len)
                .sum::<usize>()
        })
        .unwrap_or(0);
    json!({
        "status": if total == 0 { "empty" } else { "available" },
        "item_count": total,
        "source_phase_counts": counts,
        "source_roles_present": Vec::<String>::new(),
        "phase1_completed": state.get("phase1_index").is_some_and(|value| !value.is_null()),
        "direction_or_evidence_conflict_count": conflict_count,
        "source": "file_store_index_metadata",
        "directly_injected": true,
        "semantic_content_included": false,
        "retrievable_via_tools": true
    })
}

fn phase4_control_context(state: &Value) -> Value {
    let research_plan = state.get("research_plan").filter(|value| !value.is_null());
    let candidate = research_plan
        .map(research_plan_to_trade_intent)
        .unwrap_or(Value::Null);
    json!({
        "status": if research_plan.is_some() { "available" } else { "not_loaded" },
        "item_count": usize::from(research_plan.is_some()),
        "candidate_action": candidate.get("candidate_action"),
        "allowed_direction": candidate.get("candidate_action"),
        "semantic_source": "rust_deterministic_rating_mapping",
        "phase3_semantics_included": false
    })
}

fn phase5_control_context(state: &Value) -> Value {
    let scenario = state.get("overnight_gap_scenario");
    json!({
        "status": "available",
        "overnight_gap_scenario": {
            "status": if scenario.is_some() { "available" } else { "not_loaded" },
            "item_count": usize::from(scenario.is_some()),
            "data": scenario.cloned().unwrap_or(Value::Null),
            "source": if scenario.is_some() { "runtime" } else { "none" }
        },
        "hard_position_cap": state.pointer("/allocation_context/max_single_position")
            .cloned().unwrap_or(Value::Null),
        "prior_phase_semantics_included": false
    })
}

fn phase6_control_context(state: &Value) -> Value {
    let weights = state
        .get("current_portfolio_weights")
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "status": "available",
        "investable_assets": state.get("investable_assets").cloned().unwrap_or_else(|| json!([])),
        "current_weights": {
            "status": if weights.is_null() { "not_loaded" } else { "available" },
            "item_count": weights.as_object().map(|items| items.len()).unwrap_or(0),
            "data": weights,
            "source": "rust_runtime"
        },
        "hard_position_cap": state.pointer("/allocation_context/max_single_position")
            .cloned().unwrap_or(Value::Null),
        "allowed_execution_status": ["execute", "wait", "downgrade"],
        "prior_phase_semantics_included": false
    })
}

fn context_manifest_item(name: &str, value: Value, source: &str, retrievable: bool) -> Value {
    let item_count = value
        .get("item_count")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            value
                .as_object()
                .map(|items| items.len() as u64)
                .unwrap_or(u64::from(!value.is_null()))
        });
    json!({
        "name": name,
        "status": value.get("status").and_then(Value::as_str).unwrap_or(if value.is_null() { "not_loaded" } else { "available" }),
        "item_count": item_count,
        "character_count": serde_json::to_string(&value).map(|text| text.chars().count()).unwrap_or(0),
        "source": source,
        "directly_injected": true,
        "retrievable_via_tools": retrievable
    })
}

pub(crate) fn direct_context_manifest(state: &Value, phase: i64) -> Value {
    let mut contexts = Vec::new();
    if phase == 0 || (2..=6).contains(&phase) {
        contexts.push(context_manifest_item(
            "retrieval_bootstrap",
            retrieval_bootstrap(state, phase),
            "phase_summary_index_metadata",
            true,
        ));
    }
    match phase {
        3 => contexts.push(context_manifest_item(
            "phase3_context",
            phase3_context(state),
            "rust_deterministic_probability_baseline",
            false,
        )),
        4 => contexts.push(context_manifest_item(
            "phase4_control_context",
            phase4_control_context(state),
            "rust_deterministic_rating_mapping",
            false,
        )),
        5 => contexts.push(context_manifest_item(
            "phase5_control_context",
            phase5_control_context(state),
            "rust_runtime_constraints",
            false,
        )),
        6 => contexts.push(context_manifest_item(
            "phase6_control_context",
            phase6_control_context(state),
            "rust_runtime_constraints",
            false,
        )),
        _ => {}
    }
    json!({
        "status": if contexts.is_empty() { "not_applicable" } else { "available" },
        "context_count": contexts.len(),
        "contexts": contexts
    })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
    component_registry: Option<&ComponentRegistry>,
) -> Result<String> {
    let discovered_registry = if component_registry.is_none() {
        prompt_path
            .and_then(|_| prompts_dir_from_prompt_path(prompt_path))
            .map(|prompts_dir| ComponentRegistry::discover(&prompts_dir))
            .transpose()?
    } else {
        None
    };
    render_prompt_with_plugins(
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        prompt_path,
        component_registry.or(discovered_registry.as_ref()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt_with_plugins(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
    component_registry: Option<&ComponentRegistry>,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let ticker = state
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| tickers.first().map(String::as_str))
        .unwrap_or("")
        .to_string();
    let path = prompt_path.with_context(|| format!("missing prompt path for role {role}"))?;
    let template = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read prompt template {}", path.display()))?;
    let mut placeholders = raw_template_placeholders(&template);
    let stance_role_label = role.strip_prefix("risk.").unwrap_or("");
    let (side, side_label, opponent, opponent_label, side_strategy_path) =
        if role.contains(".bull.") {
            (
                "bull",
                "看多",
                "bear",
                "看空",
                Some("phase2/researcher/side_bull.md"),
            )
        } else if role.contains(".bear.") {
            (
                "bear",
                "看空",
                "bull",
                "看多",
                Some("phase2/researcher/side_bear.md"),
            )
        } else {
            ("", "", "", "", None)
        };
    let mut component_templates = BTreeMap::new();
    let mut risk_analyst_template = None;
    let mut side_strategy = None;
    let mut selected_components = Vec::new();
    loop {
        let mut changed = false;
        for (key, relative_path) in [
            (
                "analyst_output_contract",
                "common/analyst_output_contract.md",
            ),
            ("retrieval_policy", "common/retrieval_policy.md"),
            ("research_calibration", "common/research_calibration.md"),
            ("research_drivers", "common/research_drivers.md"),
        ] {
            if placeholders.contains(key) && !component_templates.contains_key(key) {
                let component = prompt_component(prompt_path, relative_path)?;
                add_template_placeholders(&mut placeholders, &component);
                component_templates.insert(key, component);
                changed = true;
            }
        }
        if placeholders.contains("leveraged_etf_rules")
            && contains_leveraged_etf(&tickers)
            && !component_templates.contains_key("leveraged_etf_rules")
        {
            let component = prompt_component(prompt_path, "common/leveraged_etf_rules.md")?;
            add_template_placeholders(&mut placeholders, &component);
            component_templates.insert("leveraged_etf_rules", component);
            changed = true;
        }
        if placeholders.contains("risk_analyst_body") && risk_analyst_template.is_none() {
            let component = prompt_component(prompt_path, "phase5/risk_analyst.md")?;
            add_template_placeholders(&mut placeholders, &component);
            risk_analyst_template = Some(component);
            changed = true;
        }
        if placeholders.contains("side_strategy") && side_strategy.is_none() {
            side_strategy = Some(
                side_strategy_path
                    .map(|path| prompt_component(prompt_path, path))
                    .transpose()?
                    .unwrap_or_default(),
            );
            changed = true;
        }
        let referenced = referenced_components(component_registry, role, &mut placeholders);
        if referenced.len() != selected_components.len() {
            selected_components = referenced;
            changed = true;
        }
        if !changed {
            break;
        }
    }

    let mut values = serde_json::Map::new();
    insert_if_referenced(&mut values, &placeholders, "ticker", || {
        Ok(Value::String(ticker.clone()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "tickers", || {
        Ok(Value::String(tickers.join(",")))
    })?;
    insert_if_referenced(&mut values, &placeholders, "role", || {
        Ok(Value::String(role.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "phase", || Ok(json!(phase)))?;
    insert_if_referenced(&mut values, &placeholders, "kind", || {
        Ok(Value::String(kind.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "lang", || {
        Ok(Value::String(
            state
                .get("lang")
                .and_then(Value::as_str)
                .unwrap_or("zh")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, &placeholders, "side", || {
        Ok(Value::String(side.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "side_label", || {
        Ok(Value::String(side_label.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "opponent", || {
        Ok(Value::String(opponent.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "opponent_label", || {
        Ok(Value::String(opponent_label.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "stance", || {
        Ok(Value::String(stance_role_label.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "stance_label", || {
        Ok(Value::String(stance_role_label.to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "workflow_pattern", || {
        Ok(Value::String(
            "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, &placeholders, "run_id", || {
        Ok(Value::String(
            state
                .get("run_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, &placeholders, "date", || {
        Ok(Value::String(
            state
                .get("current_date")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, &placeholders, "window_days", || {
        Ok(state.get("window_days").cloned().unwrap_or(Value::Null))
    })?;
    insert_if_referenced(&mut values, &placeholders, "round", || {
        Ok(json!(round.unwrap_or_default()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "topic_id", || {
        Ok(Value::String(topic_id.unwrap_or("").to_string()))
    })?;
    insert_if_referenced(&mut values, &placeholders, "topic", || {
        let current_topic = topic_id
            .and_then(|id| topic_state(state, id))
            .and_then(|topic| topic.get("topic").cloned())
            .unwrap_or(Value::Null);
        Ok(Value::String(serde_json::to_string_pretty(&current_topic)?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "portfolio_decision", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("final_trade_decision")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "allocation_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("allocation_context")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "alpaca_mode", || {
        Ok(Value::String(
            if state.get("mock").and_then(Value::as_bool) == Some(true)
                || state.get("debug").and_then(Value::as_bool) == Some(true)
            {
                "disabled"
            } else {
                "live"
            }
            .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, &placeholders, "phase3_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase3_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "retrieval_bootstrap", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &retrieval_bootstrap(state, phase),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "phase4_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase4_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "phase5_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase5_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "phase6_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase6_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "common_ground", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state.get("common_ground").cloned().unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "reflection_task", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state.get("reflection_task").cloned().unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, &placeholders, "summary_source_payload", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("_summary_source_payload")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;

    let mut values = Value::Object(values);
    let rendered_components = component_templates
        .iter()
        .map(|(key, component)| Ok((*key, render_template(component, &values)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let values_map = values
        .as_object_mut()
        .expect("renderer values are constructed as an object");
    for (key, component) in rendered_components {
        values_map.insert(key.to_string(), Value::String(component));
    }
    for key in [
        "common_ticker_prompt",
        "analyst_output_contract",
        "retrieval_policy",
        "anti_injection",
        "research_calibration",
        "research_drivers",
        "analysis_trace_contract",
        "experience_contract",
        "leveraged_etf_rules",
        "risk_analyst_body",
        "side_strategy",
        "stance_intro",
        "stance_rules",
        "stance_schema_extra",
        "researcher_body",
    ] {
        if placeholders.contains(key) {
            values_map
                .entry(key.to_string())
                .or_insert_with(|| Value::String(String::new()));
        }
    }
    if let Some(side_strategy) = side_strategy {
        values_map.insert("side_strategy".to_string(), Value::String(side_strategy));
    }
    for plugin in selected_components {
        let rendered = render_template(&plugin.template, &values).with_context(|| {
            format!(
                "failed to render component plugin {} at {}",
                plugin.manifest.name,
                plugin.path.display()
            )
        })?;
        values
            .as_object_mut()
            .expect("renderer values are constructed as an object")
            .insert(
                plugin.manifest.placeholder_key.clone(),
                Value::String(rendered),
            );
    }
    if let Some(risk_analyst_template) = risk_analyst_template {
        let risk_analyst_body = render_template(&risk_analyst_template, &values)?;
        values
            .as_object_mut()
            .expect("renderer values are constructed as an object")
            .insert(
                "risk_analyst_body".to_string(),
                Value::String(risk_analyst_body),
            );
    }
    if placeholders.contains("common_ticker_prompt")
        && values
            .get("common_ticker_prompt")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        let path = prompt_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<inline prompt>".to_string());
        bail!(
            "prompt {path} references {{common_ticker_prompt}} but no enabled ticker component injected it for role {role}"
        );
    }
    render_template(&template, &values).map_err(|error| {
        anyhow::anyhow!(
            "failed to render prompt {}: {error}",
            prompt_path
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<inline prompt>".to_string())
        )
    })
}

/// Public lint entry point. The CLI deliberately shares the production
/// renderer so placeholder checks cannot drift from runtime composition.
#[allow(clippy::too_many_arguments)]
pub fn render_prompt_for_lint(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: &std::path::Path,
    component_registry: &ComponentRegistry,
) -> Result<String> {
    render_prompt_with_plugins(
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        Some(prompt_path),
        Some(component_registry),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::config::resolve_versioned_prompt_path;
    use orchestrator_core::ComponentRegistry;
    use serde_json::json;
    use tempfile::TempDir;

    fn write_ticker_component(prompts: &std::path::Path, body: &str) {
        std::fs::create_dir_all(prompts.join("common/components/ticker")).unwrap();
        std::fs::write(
            prompts.join("common/components/ticker/manifest.toml"),
            r#"name = "ticker"
injection_points = ["*"]
priority = 10
placeholder_key = "common_ticker_prompt"
required_variables = ["ticker", "tickers"]
"#,
        )
        .unwrap();
        std::fs::write(prompts.join("common/components/ticker/component.md"), body).unwrap();
    }

    fn write_component(
        prompts: &std::path::Path,
        name: &str,
        injection_points: &str,
        placeholder_key: &str,
        body: &str,
    ) {
        let dir = prompts.join("common/components").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("manifest.toml"),
            format!(
                "name = \"{name}\"\ninjection_points = {injection_points}\nplaceholder_key = \"{placeholder_key}\"\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join("component.md"), body).unwrap();
    }

    #[test]
    fn render_prompt_injects_common_ticker_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        write_ticker_component(&prompts, "Ticker boundary: {ticker}; all: {tickers}");
        let prompt_path = prompts.join("phase1/test.md");
        std::fs::write(&prompt_path, "Role prompt\n{common_ticker_prompt}").unwrap();
        let state = json!({"ticker": "TQQQ", "tickers": ["TQQQ", "VIX"]});

        let prompt = render_prompt(
            &state,
            "analyst.test",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("Ticker boundary: TQQQ; all: TQQQ,VIX"));
    }

    #[test]
    fn render_prompt_rejects_unresolved_placeholder() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        let prompt_path = prompts.join("phase1/test.md");
        std::fs::write(&prompt_path, "Role prompt {missing_variable}").unwrap();

        let error = render_prompt(
            &json!({"ticker": "QQQ", "tickers": ["QQQ"]}),
            "analyst.test",
            1,
            "artifact",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing_variable"));
    }

    #[test]
    fn raw_template_placeholder_discovery_ignores_json_and_unclosed_braces() {
        assert_eq!(
            raw_template_placeholders("{ticker} {not-a-placeholder} {} {\"ticker\": 1} {later"),
            std::collections::BTreeSet::from(["ticker".to_string()])
        );
    }

    #[test]
    fn unused_shared_component_is_not_loaded_or_rendered() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        std::fs::write(
            prompts.join("common/retrieval_policy.md"),
            "unused {missing_variable}",
        )
        .unwrap();
        let prompt_path = prompts.join("phase1/technical.md");
        std::fs::write(&prompt_path, "ticker={ticker}").unwrap();

        let rendered = render_prompt(
            &json!({"ticker": "QQQ", "tickers": ["QQQ"]}),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert_eq!(rendered, "ticker=QQQ");
    }

    #[test]
    fn unused_plugin_component_is_not_rendered() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        write_component(
            &prompts,
            "unused",
            "[\"analyst.technical\"]",
            "unused_component",
            "unused {missing_variable}",
        );
        let prompt_path = prompts.join("phase1/technical.md");
        std::fs::write(&prompt_path, "ticker={ticker}").unwrap();
        let registry = ComponentRegistry::discover(&prompts).unwrap();

        let rendered = render_prompt_with_plugins(
            &json!({"ticker": "QQQ", "tickers": ["QQQ"]}),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&prompt_path),
            Some(&registry),
        )
        .unwrap();

        assert_eq!(rendered, "ticker=QQQ");
    }

    #[test]
    fn render_prompt_with_plugins_overrides_legacy_component() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "LEGACY {ticker}").unwrap();
        write_ticker_component(&prompts, "PLUGIN {ticker}");
        let prompt_path = prompts.join("phase1/technical.md");
        std::fs::write(&prompt_path, "{common_ticker_prompt}").unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});
        let registry = ComponentRegistry::discover(&prompts).unwrap();

        let prompt = render_prompt_with_plugins(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            Some(&registry),
        )
        .unwrap();

        assert!(prompt.contains("PLUGIN QQQ"));
        assert!(!prompt.contains("LEGACY QQQ"));
    }

    #[test]
    fn render_prompt_injects_shared_components() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "CONTRACT for {ticker}",
        )
        .unwrap();
        write_component(
            &prompts,
            "anti_injection",
            "[\"*\"]",
            "anti_injection",
            "NO-INJECT boundary",
        );
        let prompt_path = prompts.join("phase1/technical.md");
        std::fs::write(
            &prompt_path,
            "{common_ticker_prompt}\n{anti_injection}\n{analyst_output_contract}",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ", "SOXX"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("TICK QQQ"));
        assert!(prompt.contains("CONTRACT for QQQ"));
        assert!(prompt.contains("NO-INJECT boundary"));
    }

    #[test]
    fn missing_component_expands_to_empty() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        // No analyst_output_contract.md / anti_injection.md on disk.
        let prompt_path = prompts.join("phase1/technical.md");
        std::fs::write(
            &prompt_path,
            "start\n{anti_injection}\n{analyst_output_contract}\nend",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("start"));
        assert!(prompt.contains("end"));
        assert!(!prompt.contains("{anti_injection}"));
        assert!(!prompt.contains("{analyst_output_contract}"));
    }

    #[test]
    fn bull_and_bear_initial_prompts_are_standalone() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase2")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(
            prompts.join("common/researcher_seed.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bull_initial.md"),
            "看多研究员\n{common_ticker_prompt}\nrole=researcher.bull.initial artifact=bull_seed_packet field=known_bear_constraint",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bear_initial.md"),
            "看空研究员\n{common_ticker_prompt}\nrole=researcher.bear.initial artifact=bear_seed_packet field=known_bull_constraint",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let bull = render_prompt(
            &state,
            "researcher.bull.initial",
            2,
            "bull_seed",
            None,
            None,
            Some(&prompts.join("phase2/bull_initial.md")),
            None,
        )
        .unwrap();
        let bear = render_prompt(
            &state,
            "researcher.bear.initial",
            2,
            "bear_seed",
            None,
            None,
            Some(&prompts.join("phase2/bear_initial.md")),
            None,
        )
        .unwrap();

        assert!(bull.contains("看多研究员"));
        assert!(bull.contains("role=researcher.bull.initial"));
        assert!(bull.contains("artifact=bull_seed_packet"));
        assert!(bull.contains("field=known_bear_constraint"));
        assert!(bear.contains("看空研究员"));
        assert!(bear.contains("role=researcher.bear.initial"));
        assert!(bear.contains("artifact=bear_seed_packet"));
        assert!(bear.contains("field=known_bull_constraint"));
        for prompt in [&bull, &bear] {
            assert!(prompt.contains("TICK QQQ"));
            assert!(!prompt.contains("{researcher_body}"));
            assert!(!prompt.contains("{side}"));
            assert!(!prompt.contains("SHOULD NOT LOAD"));
        }
    }

    #[test]
    fn interaction_role_uses_standalone_interaction_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase2")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(prompts.join("common/researcher_seed.md"), "SEED {side}").unwrap();
        std::fs::write(
            prompts.join("common/researcher_interaction.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bull_interaction.md"),
            "看多研究员\n{common_ticker_prompt}\nrole=researcher.bull.interaction artifact=bull_debate_packet target=看空 claim",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let out = render_prompt(
            &state,
            "researcher.bull.interaction",
            2,
            "bull_packet",
            Some(2),
            None,
            Some(&prompts.join("phase2/bull_interaction.md")),
            None,
        )
        .unwrap();

        assert!(out.contains("看多研究员"));
        assert!(out.contains("TICK QQQ"));
        assert!(out.contains("role=researcher.bull.interaction"));
        assert!(out.contains("artifact=bull_debate_packet"));
        assert!(out.contains("看空 claim"));
        assert!(!out.contains("SEED"));
        assert!(!out.contains("SHOULD NOT LOAD"));
        assert!(!out.contains("{researcher_body}"));
        assert!(!out.contains("{side}"));
    }

    #[test]
    fn integrated_risk_review_expands_shared_body() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase5")).unwrap();
        std::fs::write(
            prompts.join("phase5/risk_analyst.md"),
            "shared body {phase5_control_context} {retrieval_bootstrap}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase5/conservative.md"),
            "保守风险分析师\n{risk_analyst_body}\n\"key_risks\": [\"主要风险\"]",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let conservative = render_prompt(
            &state,
            "risk.conservative",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("phase5/conservative.md")),
            None,
        )
        .unwrap();

        assert!(conservative.contains("保守风险分析师"));
        assert!(conservative.contains("\"key_risks\""));
        for placeholder in [
            "{risk_analyst_body}",
            "{stance_label}",
            "{stance_intro}",
            "{stance_rules}",
            "{stance_schema_extra}",
        ] {
            assert!(!conservative.contains(placeholder));
        }
    }

    // ---- Golden regression over the real prompt pack -----------------------
    //
    // Renders every shipped role prompt against a representative mock state and
    // asserts that no known placeholder token survives and the output is
    // non-trivial. This catches (a) a role prompt referencing a placeholder the
    // renderer never sets, and (b) a shared component that fails to expand.
    // Literal `{` from JSON examples is fine — we only look for the specific
    // `{token}` names the renderer is responsible for.

    fn project_prompts_dir() -> std::path::PathBuf {
        // render.rs -> orchestration -> src -> orchestrator-workflow -> crates -> repo
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("prompts")
    }

    /// Placeholder tokens the renderer owns. If any survive a render, either the
    /// prompt referenced an unknown key or a component failed to expand.
    const KNOWN_PLACEHOLDERS: &[&str] = &[
        "{common_ticker_prompt}",
        "{analyst_output_contract}",
        "{anti_injection}",
        "{research_calibration}",
        "{research_drivers}",
        "{analysis_trace_contract}",
        "{experience_contract}",
        "{side}",
        "{side_label}",
        "{opponent}",
        "{opponent_label}",
        "{side_strategy}",
        "{leveraged_etf_rules}",
        "{risk_analyst_body}",
        "{date}",
        "{window_days}",
        "{topic_id}",
        "{topic}",
        "{round}",
        "{retrieval_bootstrap}",
        "{phase4_control_context}",
        "{phase5_control_context}",
        "{phase6_control_context}",
        "{allocation_context}",
        "{reflection_task}",
        "{summary_source_payload}",
        "{phase3_context}",
        "{common_ground}",
    ];

    fn golden_mock_state() -> Value {
        golden_mock_state_with_date("2026-07-03")
    }

    fn golden_mock_state_with_date(date: &str) -> Value {
        json!({
            "ticker": "QQQ",
            "tickers": ["QQQ", "SOXX", "VIX"],
            "current_date": date,
            "window_days": 5,
            "lang": "zh",
            "run_id": "golden-run",
            "analyst_reports": {"analyst.technical": {"per_ticker": {}}},
            "research_plan": {"rating": "Hold"},
            "trader_investment_plan": {"action": "Hold"},
            "risk_debate_state": {"history": []},
            "final_trade_decision": {"rating": "Hold"},
            "allocation_context": {"investable_assets": ["QQQ", "SOXX"]}
        })
    }

    #[test]
    fn phase3_context_only_injects_deterministic_inputs() {
        let context = phase3_context(&json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.5}},
            "analyst_weights": {"analyst.technical": 0.5, "analyst.news_macro": 0.5},
            "phase1_index": {
                "status": "insufficient",
                "weighted_probability_base": {"QQQ": {"long_probability": 0.5}},
                "per_ticker": {"QQQ": {
                    "decision_hinges": ["price confirmation"],
                    "role_summaries": [{
                        "role": "analyst.technical",
                        "status": "ready",
                        "stance": "neutral",
                        "confidence": 0.5,
                        "summary": "full analyst report must not be forwarded",
                        "key_evidence": [
                            {"claim": "one", "evidence_type": "fact", "report": "drop"},
                            {"claim": "two", "evidence_type": "opinion"},
                            {"claim": "three", "evidence_type": "fact"},
                            {"claim": "four", "evidence_type": "fact"}
                        ]
                    }]
                }}
            },
            "debate_state_artifact": {
                "status": "skipped_no_actionable_evidence",
                "topic_briefs": [{"topic_id": "QQQ-gap"}],
                "debate_turns": [{"should_not": "be forwarded"}]
            },
            "prior_experience": {"items": []},
            "track_record": {"sample_size": 2},
            "agent_accuracy": {"analyst.technical": 0.7}
        }));

        assert!(context.get("phase1").is_none());
        assert!(context.get("phase2_5").is_none());
        assert!(context.get("phase_summary_tables").is_none());
        assert_eq!(
            context["weighted_probability_base"]["QQQ"]["long_probability"],
            0.5
        );
        assert!(context.get("prior_experience").is_none());
        assert!(context.get("track_record").is_none());
        assert!(context.get("agent_accuracy").is_none());
    }

    #[test]
    fn context_manifest_records_source_size_and_visibility() {
        let manifest = direct_context_manifest(
            &json!({
                "phase_summary_indexes": {
                    "phases": {
                        "3": {"summaries": [{"role": "manager.research"}], "details": []}
                    }
                },
                "research_plan": {"rating": "Buy"}
            }),
            4,
        );
        assert_eq!(manifest["context_count"], 2);
        assert_eq!(manifest["contexts"][0]["name"], "retrieval_bootstrap");
        assert_eq!(manifest["contexts"][0]["retrievable_via_tools"], true);
        assert_eq!(
            manifest["contexts"][1]["source"],
            "rust_deterministic_rating_mapping"
        );
        assert!(manifest["contexts"][1]["character_count"].as_u64().unwrap() > 0);
    }

    #[test]
    fn downstream_control_contexts_exclude_prior_phase_semantics() {
        let state = json!({
            "research_plan": {
                "rating": "Hold",
                "long_probability": 0.5,
                "short_probability": 0.5,
                "report": "DROP_FULL_RESEARCH_REPORT",
                "per_ticker": {"QQQ": {
                    "rating": "Hold",
                    "long_probability": 0.5,
                    "short_probability": 0.5,
                    "report": "DROP_TICKER_REPORT"
                }}
            },
            "trader_investment_plan": {"action": "Hold", "position_size_pct_max": 0.0},
            "analyst_reports": {"analyst.technical": {"report": "DROP_ANALYST_REPORT"}},
            "risk_debate_state": {"history": [{"artifact": {
                "role": "risk.aggressive",
                "stance": "aggressive",
                "argument": "compact argument",
                "raw_context": "DROP_RISK_CONTEXT"
            }}]}
        });

        let trader = serde_json::to_string(&phase4_control_context(&state)).unwrap();
        let risk = serde_json::to_string(&phase5_control_context(&state)).unwrap();
        let portfolio = serde_json::to_string(&phase6_control_context(&state)).unwrap();
        for context in [&trader, &risk, &portfolio] {
            assert!(!context.contains("DROP_FULL_RESEARCH_REPORT"));
            assert!(!context.contains("DROP_TICKER_REPORT"));
            assert!(!context.contains("DROP_ANALYST_REPORT"));
            assert!(!context.contains("DROP_RISK_CONTEXT"));
        }
    }

    #[test]
    fn retrieval_first_prompts_do_not_embed_prior_phase_payloads() {
        let prompts = project_prompts_dir();
        let state = json!({
            "ticker": "QQQ",
            "tickers": ["QQQ"],
            "run_id": "retrieval-first",
            "current_date": "2026-07-24",
            "window_days": 60,
            "phase1_index": {"report": "FULL_PHASE1_SENTINEL"},
            "research_plan": {
                "rating": "Hold",
                "report": "FULL_RESEARCH_SENTINEL"
            },
            "trader_investment_plan": {
                "action": "Hold",
                "report": "FULL_TRADER_SENTINEL"
            },
            "risk_debate_state": {
                "history": [{"artifact": {"argument": "FULL_RISK_SENTINEL"}}]
            },
            "investable_assets": ["QQQ"]
        });
        for (role, phase, relative) in [
            ("mediator.topic", 2, "phase2/topic_generator.md"),
            ("mediator.topic", 2, "phase2/researcher/warmup.md"),
            ("trader", 4, "phase4/trader.md"),
            ("risk.aggressive", 5, "phase5/aggressive.md"),
            ("portfolio.manager", 6, "phase6/portfolio_manager.md"),
        ] {
            let rendered = render_prompt(
                &state,
                role,
                phase,
                "artifact",
                None,
                None,
                Some(&prompts.join(relative)),
                None,
            )
            .unwrap();
            for sentinel in [
                "FULL_PHASE1_SENTINEL",
                "FULL_RESEARCH_SENTINEL",
                "FULL_TRADER_SENTINEL",
                "FULL_RISK_SENTINEL",
            ] {
                assert!(!rendered.contains(sentinel), "{relative} leaked {sentinel}");
            }
        }
    }

    #[test]
    fn bootstrap_is_materially_smaller_than_direct_semantic_context() {
        let large = "x".repeat(20_000);
        let state = json!({
            "ticker": "QQQ",
            "tickers": ["QQQ"],
            "phase1_index": {"report": large},
            "research_plan": {"report": large},
            "risk_debate_state": {"history": [{"artifact": {"argument": large}}]},
            "phase_summary_indexes": {
                "run_id": "run",
                "phases": {}
            }
        });
        let direct_chars = state["phase1_index"].to_string().len()
            + state["research_plan"].to_string().len()
            + state["risk_debate_state"].to_string().len();
        let bootstrap_chars = retrieval_bootstrap(&state, 6).to_string().len()
            + phase6_control_context(&state).to_string().len();
        assert!(
            bootstrap_chars * 20 < direct_chars,
            "bootstrap={bootstrap_chars}, direct={direct_chars}"
        );
    }

    #[test]
    fn shipped_analyst_contract_delegates_shape_to_runtime_validation() {
        let contract = std::fs::read_to_string(
            project_prompts_dir().join("common/analyst_output_contract.md"),
        )
        .unwrap();

        assert!(contract.contains("Rust finalizer"));
        assert!(!contract.contains("{analyst_artifact_schema}"));
        assert!(!contract.contains("顶层结构"));
        assert!(contract.contains("finalize_analyst_report"));
        assert!(!contract.contains("输出必须是单个 JSON 对象"));
        assert!(!contract.contains("```json"));
    }

    #[test]
    fn static_prefix_is_stable_across_dynamic_changes() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let path = prompts.join("phase1/technical.md");
        let prompt_a = render_prompt(
            &golden_mock_state_with_date("2026-07-01"),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        let prompt_b = render_prompt(
            &golden_mock_state_with_date("2026-07-06"),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        let split_marker = "<!-- DYNAMIC SUFFIX";
        let prefix_a = prompt_a.split(split_marker).next().unwrap_or("");
        let prefix_b = prompt_b.split(split_marker).next().unwrap_or("");

        assert_eq!(
            prefix_a, prefix_b,
            "Static prefix must be identical across calls with different dates"
        );
    }

    #[test]
    fn versioned_prompt_path_resolves_correctly() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts/phase1");
        std::fs::create_dir_all(&prompts).unwrap();
        std::fs::write(prompts.join("technical.md"), "v1 content").unwrap();
        std::fs::write(prompts.join("technical_v2.md"), "v2 content").unwrap();

        let base = prompts.join("technical.md");
        let v1 = resolve_versioned_prompt_path(&base, Some("v1")).unwrap();
        let absent = resolve_versioned_prompt_path(&base, None).unwrap();
        let v2 = resolve_versioned_prompt_path(&base, Some("v2")).unwrap();
        let v3_fallback = resolve_versioned_prompt_path(&base, Some("v3")).unwrap();
        assert_eq!(v1, base);
        assert_eq!(absent, base);
        assert_eq!(v2, prompts.join("technical_v2.md"));
        assert_eq!(v3_fallback, base);
    }

    #[test]
    fn golden_all_role_prompts_render_without_unresolved_placeholders() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            // Skip in environments without the prompt pack (e.g. packaged crate).
            return;
        }
        let state = golden_mock_state();
        let plugin_registry = ComponentRegistry::discover(&prompts).unwrap();
        let mut known_placeholders = KNOWN_PLACEHOLDERS
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        known_placeholders.extend(
            plugin_registry
                .placeholder_keys()
                .into_iter()
                .map(|key| format!("{{{key}}}")),
        );
        // (role, relative prompt path, kind)
        let cases: &[(&str, &str, &str)] = &[
            ("analyst.technical", "phase1/technical.md", "artifact"),
            ("analyst.news_macro", "phase1/news_macro.md", "artifact"),
            (
                "mediator.topic",
                "phase2/topic_generator.md",
                "topic_generation",
            ),
            (
                "researcher.bull.initial",
                "phase2/researcher/debate.md",
                "bull_seed",
            ),
            (
                "researcher.bear.initial",
                "phase2/researcher/debate.md",
                "bear_seed",
            ),
            (
                "researcher.bull.interaction",
                "phase2/researcher/debate.md",
                "bull_packet",
            ),
            (
                "researcher.bear.interaction",
                "phase2/researcher/debate.md",
                "bear_packet",
            ),
            (
                "mediator.topic_controller",
                "phase2/topic_controller.md",
                "controller_packet",
            ),
            ("manager.research", "phase3/research_manager.md", "artifact"),
            ("trader", "phase4/trader.md", "artifact"),
            ("risk.aggressive", "phase5/aggressive.md", "risk_argument"),
            ("risk.neutral", "phase5/neutral.md", "risk_argument"),
            (
                "risk.conservative",
                "phase5/conservative.md",
                "risk_argument",
            ),
            (
                "portfolio.manager",
                "phase6/portfolio_manager.md",
                "artifact",
            ),
        ];

        for (role, rel, kind) in cases {
            let path = prompts.join(rel);
            assert!(path.exists(), "missing prompt file {}", path.display());
            let prompt = render_prompt(
                &state,
                role,
                1,
                kind,
                Some(2),
                Some("QQQ-aggregate"),
                Some(&path),
                None,
            )
            .unwrap_or_else(|e| panic!("render failed for {role} ({rel}): {e}"));

            assert!(
                prompt.trim().len() > 40,
                "rendered prompt for {role} ({rel}) is suspiciously short"
            );
            for token in &known_placeholders {
                assert!(
                    !prompt.contains(token),
                    "unresolved placeholder {token} in {role} ({rel})"
                );
            }
        }
    }

    #[test]
    fn only_analytical_execution_and_summary_roles_receive_trace_components() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        let cases: &[(&str, i64, &str, &str)] = &[
            (
                "compressor.phase_summary",
                0,
                "phase_summary",
                "phase_summary/phase_summary.md",
            ),
            (
                "mediator.topic",
                2,
                "topic_generation",
                "phase2/topic_generator.md",
            ),
            (
                "manager.research",
                3,
                "artifact",
                "phase3/research_manager.md",
            ),
            ("trader", 4, "artifact", "phase4/trader.md"),
            (
                "portfolio.manager",
                6,
                "artifact",
                "phase6/portfolio_manager.md",
            ),
        ];

        for (role, phase, kind, relative) in cases {
            let prompt = render_prompt(
                &state,
                role,
                *phase,
                kind,
                Some(1),
                Some("QQQ-aggregate"),
                Some(&prompts.join(relative)),
                None,
            )
            .unwrap_or_else(|error| panic!("render failed for {role} ({relative}): {error}"));
            assert!(
                prompt.contains("审计轨迹"),
                "{relative} did not receive its role-specific trace component"
            );
            assert!(
                !prompt.contains("{analysis_trace_contract}"),
                "{relative} retained an unresolved analysis trace placeholder"
            );
        }

        for (role, phase, kind, relative) in [
            (
                "researcher.bull.initial",
                2,
                "bull_seed",
                "phase2/researcher/debate.md",
            ),
            (
                "mediator.topic_controller",
                2,
                "controller_packet",
                "phase2/topic_controller.md",
            ),
            ("risk.neutral", 5, "risk_argument", "phase5/neutral.md"),
        ] {
            let prompt = render_prompt(
                &state,
                role,
                phase,
                kind,
                Some(1),
                Some("QQQ-aggregate"),
                Some(&prompts.join(relative)),
                None,
            )
            .unwrap_or_else(|error| panic!("render failed for {role} ({relative}): {error}"));
            assert!(
                !prompt.contains("审计轨迹"),
                "{relative} received an inapplicable trace component"
            );
        }

        let summary =
            std::fs::read_to_string(prompts.join("phase_summary/phase_summary.md")).unwrap();
        assert!(summary.contains("create_index(kind=phase_summary)"));
        assert!(summary.contains("append_index_detail"));
        assert!(summary.contains("finalize_index"));
        assert!(summary.contains("source_phase >= 2"));
    }

    #[test]
    fn golden_analyst_prompts_carry_runtime_contract_and_boundaries() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        for rel in ["phase1/technical.md", "phase1/news_macro.md"] {
            let path = prompts.join(rel);
            let role = format!(
                "analyst.{}",
                rel.trim_start_matches("phase1/").trim_end_matches(".md")
            );
            let prompt =
                render_prompt(&state, &role, 1, "artifact", None, None, Some(&path), None).unwrap();
            // The model still receives the key behavioral contract, while the
            // runtime validator remains the only source of structural truth.
            assert!(
                prompt.contains("direction"),
                "{rel} missing direction field"
            );
            assert!(
                prompt.contains("confidence"),
                "{rel} missing confidence field"
            );
            assert!(!prompt.contains("顶层结构"), "{rel} embeds a JSON shape");
            assert!(
                !prompt.contains("{analyst_artifact_schema}"),
                "{rel} contains a schema placeholder"
            );
            // Anti-injection boundary must be present for external-content roles.
            assert!(
                prompt.contains("外部内容边界") || prompt.contains("不是给你的指令"),
                "{rel} missing anti-injection boundary"
            );
        }
    }

    #[test]
    fn research_manager_injects_calibration_and_semantic_drivers() {
        let prompts = project_prompts_dir();
        let rendered = render_prompt(
            &golden_mock_state(),
            "manager.research",
            3,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase3/research_manager.md")),
            None,
        )
        .unwrap();

        assert!(rendered.contains("duplicate_evidence_discount"));
        assert!(rendered.contains("probability_drivers"));
        assert!(rendered.contains("direction`: `increase | decrease | neutral"));
        assert!(!rendered.contains("{research_calibration}"));
        assert!(!rendered.contains("{research_drivers}"));
        assert!(!rendered.contains("```json"));
    }

    #[test]
    fn leveraged_etf_rules_are_injected_only_for_leveraged_output_scope() {
        let prompts = project_prompts_dir();
        let path = prompts.join("phase1/technical.md");
        let ordinary = render_prompt(
            &golden_mock_state(),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        assert!(!ordinary.contains("## 杠杆 ETF 补充规则"));
        assert!(!ordinary.contains("SQQQ 与 QQQ 反向"));

        let mut leveraged = golden_mock_state();
        leveraged["ticker"] = json!("TQQQ");
        leveraged["tickers"] = json!(["TQQQ"]);
        let leveraged = render_prompt(
            &leveraged,
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        assert!(leveraged.contains("## 杠杆 ETF 补充规则"));
        assert!(leveraged.contains("SQQQ 与 QQQ 反向"));
    }

    #[test]
    fn technical_prompt_lists_the_runtime_direction_and_source_tier_enums() {
        let prompts = project_prompts_dir();
        let prompt = render_prompt(
            &golden_mock_state(),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase1/technical.md")),
            None,
        )
        .unwrap();

        assert!(prompt.contains("`bullish`、`bearish`、`neutral`、`mixed` 或 `unobserved`"));
        assert!(prompt.contains("不得输出组合标签（例如 `neutral_bullish`）"));
        assert!(prompt.contains("一律填写 `unknown`"));
        assert!(prompt.contains("绝不填写 `T1_reference`"));
        assert!(
            prompt.contains("`priced_in` 只能为文本 `already_priced`、`under_priced` 或 `unclear`")
        );
        assert!(prompt.contains("`key_evidence` 中的 `claim`、`source` 与 `timestamp` 均为必填"));
    }

    #[test]
    fn downstream_prompts_enforce_single_authority_chain() {
        let prompts = project_prompts_dir();
        let trader = render_prompt(
            &golden_mock_state(),
            "trader",
            4,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase4/trader.md")),
            None,
        )
        .unwrap();
        assert!(trader.contains("Phase 3 Summary 是唯一市场结论"));

        for role in ["risk.aggressive", "risk.neutral", "risk.conservative"] {
            let stance = role.strip_prefix("risk.").unwrap();
            let risk = render_prompt(
                &golden_mock_state(),
                role,
                5,
                "risk_argument",
                Some(1),
                None,
                Some(&prompts.join(format!("phase5/{stance}.md"))),
                None,
            )
            .unwrap();
            assert!(risk.contains(stance));
            assert!(risk.contains("风险委员会"));
            assert!(risk.contains("overnight_gap_scenario"));
        }
    }

    #[test]
    fn topic_controller_uses_only_canonical_control_fields() {
        let content =
            std::fs::read_to_string(project_prompts_dir().join("phase2/topic_controller.md"))
                .unwrap();
        assert!(content.contains("set_claim_status"));
        assert!(content.contains("route_debate_steer"));
        assert!(!content.contains("blocked_repeats"));
        assert!(!content.contains("next_agenda"));
    }

    #[test]
    fn active_prompts_do_not_reference_removed_social_sources() {
        let prompts = project_prompts_dir();
        for relative in [
            "phase1/technical.md",
            "phase1/news_macro.md",
            "common/analyst_output_contract.md",
            "phase3/research_manager.md",
            "phase2/researcher/warmup.md",
            "phase2/researcher/debate.md",
            "phase2/researcher/side_bull.md",
            "phase2/researcher/side_bear.md",
        ] {
            let content = std::fs::read_to_string(prompts.join(relative)).unwrap();
            for removed in ["YouTube", "Reddit", "Twitter"] {
                assert!(!content.contains(removed), "{relative} mentions {removed}");
            }
        }
    }

    #[test]
    fn phase2_kind_templates_keep_the_researcher_audit_boundaries() {
        let prompts = project_prompts_dir();
        let warmup = std::fs::read_to_string(prompts.join("phase2/researcher/warmup.md")).unwrap();
        let debate = std::fs::read_to_string(prompts.join("phase2/researcher/debate.md")).unwrap();

        assert!(warmup.contains("多空双方研究员共用的预热模式"));
        assert!(warmup.contains("必须真实调用 `read_phase_summaries(source_phase=1)`"));
        assert!(warmup.contains("1-2 个 summary"));
        assert!(debate.contains("raw Jin10"));
        assert!(debate.contains("create_debate_claim"));
        assert!(debate.contains("respond_to_debate_claim"));
        assert!(debate.contains("next_steers"));
        assert!(debate.contains("blocked_claims"));
        assert!(debate.contains("reply_to_claim_id"));
        assert!(debate.contains("no_new_info"));
        assert!(debate.contains("steelman"));
    }
}
