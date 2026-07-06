use anyhow::{Context, Result};
use orchestrator_core::{
    analyst_artifact_schema, final_validation_schema, portfolio_allocation_schema,
    replace_placeholders, research_artifact_schema, risk_constraints_schema, trade_intent_schema,
};
use serde_json::{json, Value};
use std::path::PathBuf;

use super::state::{tickers_from_state, topic_state};

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
fn common_component(prompt_path: Option<&std::path::Path>, file_name: &str) -> Result<String> {
    let Some(path) = prompt_path else {
        return Ok(String::new());
    };
    let Some(prompts_dir) = path.parent().and_then(|parent| parent.parent()) else {
        return Ok(String::new());
    };
    let common_path = prompts_dir.join("common").join(file_name);
    if common_path.exists() {
        std::fs::read_to_string(&common_path)
            .with_context(|| format!("failed to read prompt template {}", common_path.display()))
    } else {
        Ok(String::new())
    }
}

/// Map a researcher role to (side, side_label, opponent, opponent_label) so the
/// bull and bear prompts can share one template body parameterized by `{side}`.
/// Non-researcher roles get empty strings (their prompts don't use the keys).
fn researcher_side_params(role: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    if role.starts_with("researcher.bull") {
        ("bull", "看多", "bear", "看空")
    } else if role.starts_with("researcher.bear") {
        ("bear", "看空", "bull", "看多")
    } else {
        ("", "", "", "")
    }
}

/// Map a risk role (`risk.aggressive` / `risk.neutral` / `risk.conservative`) to
/// its stance token so the three risk prompts can share one template body.
fn risk_stance_label(role: &str) -> &'static str {
    match role {
        "risk.aggressive" => "aggressive",
        "risk.conservative" => "conservative",
        "risk.neutral" => "neutral",
        _ => "",
    }
}

/// Stance-specific fragments for the shared risk analyst template:
/// (label, intro, stance-specific numbered rules, extra JSON schema field).
fn risk_stance_fragments(role: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    match role {
        "risk.aggressive" => (
            "激进风险分析师",
            "你的任务是为高回报路径辩护，指出保守和中性视角可能错失的机会，但不能无视已知风险或编造新催化。",
            "2. 指出支持更高风险的 1-3 个最强依据，并说明它们是否已在 analyst_reports 中独立出现。\n3. 明确列出愿意接受的风险，不把风险淡化成机会；若 trader_plan 已很激进，优先建议保持而非继续加码。",
            "\n  \"key_risks_accepted\": [\"接受的风险\"],",
        ),
        "risk.conservative" => (
            "保守风险分析师",
            "你的任务是保护资产、降低波动，指出拟议方案中过度冒险的部分，但不能因为天然保守就否定所有机会。",
            "2. `key_risks` 只列 2-5 个真正会改变执行的风险，区分“必须降风险”与“只需监控”。\n3. 若 trader_plan 已经保守，指出无需进一步收缩，避免过度防御。",
            "\n  \"key_risks\": [\"主要风险\"],",
        ),
        "risk.neutral" => (
            "中性风险分析师",
            "你的任务是在激进与保守之间给出平衡观点，评估收益与风险，并给出最少改动的折中方案；既不因单一利好追高，也不因单一风险完全否定方案。",
            "2. `balanced_view` 列出 2-4 条平衡观察，每条都连接到 trader_plan 或 analyst_reports。\n3. 如果证据不足以支持执行，明确建议转为观察，而不是模糊折中。",
            "\n  \"balanced_view\": [\"平衡观察\"],",
        ),
        _ => ("", "", "", ""),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let ticker = state
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| tickers.first().map(String::as_str))
        .unwrap_or("");
    let template = if let Some(path) = prompt_path {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt template {}", path.display()))?
    } else {
        "Return only artifact JSON for role {role}, kind {kind}, phase {phase}, and tickers {tickers}. Include per_ticker for every ticker.".to_string()
    };
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let current_controller = current_topic_state
        .get("controller_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked_repeats = current_controller
        .get("blocked_repeats")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let next_agenda = current_controller
        .get("next_agenda")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let common_ticker_prompt_template = common_component(prompt_path, "ticker.md")?;
    let analyst_output_contract_template =
        common_component(prompt_path, "analyst_output_contract.md")?;
    let anti_injection_template = common_component(prompt_path, "anti_injection.md")?;
    let researcher_seed_template = common_component(prompt_path, "researcher_seed.md")?;
    let researcher_interaction_template =
        common_component(prompt_path, "researcher_interaction.md")?;
    let risk_analyst_template = common_component(prompt_path, "risk_analyst.md")?;
    // Render components with the schema in scope so a `{analyst_artifact_schema}`
    // placeholder inside the contract is resolved before the component text is
    // spliced into the role prompt (the outer pass runs once, in key order, and
    // would otherwise leave the nested placeholder untouched).
    let component_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
    });
    let common_ticker_prompt =
        replace_placeholders(&common_ticker_prompt_template, &component_values);
    let analyst_output_contract =
        replace_placeholders(&analyst_output_contract_template, &component_values);
    let anti_injection = replace_placeholders(&anti_injection_template, &component_values);
    let (side, side_label, opponent, opponent_label) = researcher_side_params(role);
    let stance_label = risk_stance_label(role);
    let (stance_role_label, stance_intro, stance_rules, stance_schema_extra) =
        risk_stance_fragments(role);
    let values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "ticker": ticker,
        "tickers": tickers.join(","),
        "common_ticker_prompt": common_ticker_prompt,
        "analyst_output_contract": analyst_output_contract,
        "anti_injection": anti_injection,
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
        "side": side,
        "side_label": side_label,
        "opponent": opponent,
        "opponent_label": opponent_label,
        "stance": stance_label,
        "stance_label": stance_role_label,
        "stance_intro": stance_intro,
        "stance_rules": stance_rules,
        "stance_schema_extra": stance_schema_extra,
        "date": state.get("current_date").and_then(Value::as_str).unwrap_or(""),
        "lang": state.get("lang").and_then(Value::as_str).unwrap_or("zh"),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "role": role,
        "phase": phase,
        "kind": kind,
        "round": round.unwrap_or_default(),
        "topic_id": topic_id.unwrap_or(""),
        "topic": serde_json::to_string_pretty(&current_topic)?,
        "blocked_repeats": serde_json::to_string_pretty(&blocked_repeats)?,
        "next_agenda": serde_json::to_string_pretty(&next_agenda)?,
        "analyst_reports": serde_json::to_string_pretty(&state.get("analyst_reports").cloned().unwrap_or(Value::Null))?,
        "research_plan": serde_json::to_string_pretty(&state.get("research_plan").cloned().unwrap_or(Value::Null))?,
        "trader_plan": serde_json::to_string_pretty(&state.get("trader_investment_plan").cloned().unwrap_or(Value::Null))?,
        "risk_history": serde_json::to_string_pretty(&state.get("risk_debate_state").and_then(|v| v.get("history")).cloned().unwrap_or_else(|| json!([])))?,
        "portfolio_decision": serde_json::to_string_pretty(&state.get("final_trade_decision").cloned().unwrap_or(Value::Null))?,
        "allocation_context": serde_json::to_string_pretty(&state.get("allocation_context").cloned().unwrap_or(Value::Null))?,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    // Researcher (bull/bear) and risk-tier bodies live in shared components,
    // parameterized by {side}/{stance}. Render them against the value set, then
    // expose the result so the thin role file includes one placeholder each.
    let researcher_body_template = if side.is_empty() {
        String::new()
    } else if role.ends_with(".interaction") {
        researcher_interaction_template
    } else {
        researcher_seed_template
    };
    let researcher_body = replace_placeholders(&researcher_body_template, &values);
    let risk_analyst_body = replace_placeholders(&risk_analyst_template, &values);
    let mut values = values;
    if let Some(map) = values.as_object_mut() {
        map.insert(
            "researcher_body".to_string(),
            Value::String(researcher_body),
        );
        map.insert(
            "risk_analyst_body".to_string(),
            Value::String(risk_analyst_body),
        );
    }
    Ok(replace_placeholders(&template, &values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn render_prompt_injects_common_ticker_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(
            prompts.join("common/ticker.md"),
            "Ticker boundary: {ticker}; all: {tickers}",
        )
        .unwrap();
        let prompt_path = prompts.join("analysts/test.md");
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
        )
        .unwrap();

        assert!(prompt.contains("Ticker boundary: TQQQ; all: TQQQ,VIX"));
    }

    #[test]
    fn render_prompt_injects_shared_components() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "TICK {ticker}").unwrap();
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "CONTRACT for {ticker}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("common/anti_injection.md"),
            "NO-INJECT boundary",
        )
        .unwrap();
        let prompt_path = prompts.join("analysts/technical.md");
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
        )
        .unwrap();

        assert!(prompt.contains("TICK QQQ"));
        assert!(prompt.contains("CONTRACT for QQQ"));
        assert!(prompt.contains("NO-INJECT boundary"));
    }

    #[test]
    fn schema_placeholder_resolves_inside_component() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "TICK").unwrap();
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "schema:\n{analyst_artifact_schema}",
        )
        .unwrap();
        let prompt_path = prompts.join("analysts/technical.md");
        std::fs::write(&prompt_path, "{analyst_output_contract}").unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
        )
        .unwrap();

        // The nested schema placeholder must be expanded, not left literal.
        assert!(!prompt.contains("{analyst_artifact_schema}"));
        assert!(prompt.contains("direction"));
        assert!(prompt.contains("confidence"));
    }

    #[test]
    fn missing_component_expands_to_empty() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "TICK").unwrap();
        // No analyst_output_contract.md / anti_injection.md on disk.
        let prompt_path = prompts.join("analysts/technical.md");
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
        )
        .unwrap();

        assert!(prompt.contains("start"));
        assert!(prompt.contains("end"));
        assert!(!prompt.contains("{anti_injection}"));
        assert!(!prompt.contains("{analyst_output_contract}"));
    }

    #[test]
    fn researcher_side_params_map_correctly() {
        assert_eq!(researcher_side_params("researcher.bull.initial").0, "bull");
        assert_eq!(
            researcher_side_params("researcher.bear.interaction").0,
            "bear"
        );
        assert_eq!(researcher_side_params("analyst.technical").0, "");
    }

    #[test]
    fn bull_and_bear_share_body_with_swapped_side() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("researchers")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "").unwrap();
        std::fs::write(
            prompts.join("common/researcher_seed.md"),
            "side={side} label={side_label} opp={opponent} role=researcher.{side}.initial",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bull_initial.md"),
            "{researcher_body}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bear_initial.md"),
            "{researcher_body}",
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
            Some(&prompts.join("researchers/bull_initial.md")),
        )
        .unwrap();
        let bear = render_prompt(
            &state,
            "researcher.bear.initial",
            2,
            "bear_seed",
            None,
            None,
            Some(&prompts.join("researchers/bear_initial.md")),
        )
        .unwrap();

        assert!(bull.contains("side=bull label=看多 opp=bear role=researcher.bull.initial"));
        assert!(bear.contains("side=bear label=看空 opp=bull role=researcher.bear.initial"));
        assert!(!bull.contains("{researcher_body}"));
    }

    #[test]
    fn interaction_role_selects_interaction_body() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("researchers")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "").unwrap();
        std::fs::write(prompts.join("common/researcher_seed.md"), "SEED {side}").unwrap();
        std::fs::write(
            prompts.join("common/researcher_interaction.md"),
            "INTERACTION {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bull_interaction.md"),
            "{researcher_body}",
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
            Some(&prompts.join("researchers/bull_interaction.md")),
        )
        .unwrap();

        assert!(out.contains("INTERACTION bull"));
        assert!(!out.contains("SEED"));
    }

    #[test]
    fn risk_tiers_share_body_with_stance_fragments() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("risk")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "").unwrap();
        std::fs::write(
            prompts.join("common/risk_analyst.md"),
            "stance={stance} label={stance_label}{stance_schema_extra}",
        )
        .unwrap();
        std::fs::write(prompts.join("risk/aggressive.md"), "{risk_analyst_body}").unwrap();
        std::fs::write(prompts.join("risk/conservative.md"), "{risk_analyst_body}").unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let aggressive = render_prompt(
            &state,
            "risk.aggressive",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("risk/aggressive.md")),
        )
        .unwrap();
        let conservative = render_prompt(
            &state,
            "risk.conservative",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("risk/conservative.md")),
        )
        .unwrap();

        assert!(aggressive.contains("stance=aggressive label=激进风险分析师"));
        assert!(aggressive.contains("key_risks_accepted"));
        assert!(conservative.contains("stance=conservative label=保守风险分析师"));
        assert!(conservative.contains("\"key_risks\""));
        assert!(!aggressive.contains("{risk_analyst_body}"));
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
        "{analyst_artifact_schema}",
        "{research_artifact_schema}",
        "{trade_intent_schema}",
        "{risk_constraints_schema}",
        "{final_validation_schema}",
        "{portfolio_allocation_schema}",
        "{researcher_body}",
        "{risk_analyst_body}",
        "{side}",
        "{side_label}",
        "{opponent}",
        "{opponent_label}",
        "{stance}",
        "{stance_label}",
        "{stance_intro}",
        "{stance_rules}",
        "{stance_schema_extra}",
        "{date}",
        "{window_days}",
        "{topic_id}",
        "{topic}",
        "{round}",
        "{trader_plan}",
        "{research_plan}",
        "{analyst_reports}",
        "{risk_history}",
        "{allocation_context}",
    ];

    fn golden_mock_state() -> Value {
        json!({
            "ticker": "QQQ",
            "tickers": ["QQQ", "SOXX", "VIX"],
            "current_date": "2026-07-03",
            "window_days": 5,
            "lang": "zh",
            "run_id": "golden-run",
            "analyst_reports": {"analyst.technical": {"per_ticker": {}}},
            "research_plan": {"rating": "Hold"},
            "trader_investment_plan": {"action": "Hold"},
            "risk_debate_state": {"history": []},
            "final_trade_decision": {"rating": "Hold"},
            "allocation_context": {"investable_tickers": ["QQQ", "SOXX"]}
        })
    }

    #[test]
    fn golden_all_role_prompts_render_without_unresolved_placeholders() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            // Skip in environments without the prompt pack (e.g. packaged crate).
            return;
        }
        let state = golden_mock_state();
        // (role, relative prompt path, kind)
        let cases: &[(&str, &str, &str)] = &[
            ("analyst.technical", "analysts/technical.md", "artifact"),
            ("analyst.news_macro", "analysts/news_macro.md", "artifact"),
            ("analyst.youtube", "analysts/youtube.md", "artifact"),
            ("analyst.reddit", "analysts/reddit.md", "artifact"),
            ("analyst.x", "analysts/x.md", "artifact"),
            (
                "researcher.bull.initial",
                "researchers/bull_initial.md",
                "bull_seed",
            ),
            (
                "researcher.bear.initial",
                "researchers/bear_initial.md",
                "bear_seed",
            ),
            (
                "researcher.bull.interaction",
                "researchers/bull_interaction.md",
                "bull_packet",
            ),
            (
                "researcher.bear.interaction",
                "researchers/bear_interaction.md",
                "bear_packet",
            ),
            (
                "researcher.bull.initial",
                "researchers/bull_initial_monitor.md",
                "bull_seed",
            ),
            (
                "researcher.bear.initial",
                "researchers/bear_initial_monitor.md",
                "bear_seed",
            ),
            (
                "mediator.topic",
                "mediators/topic_generation.md",
                "topic_generation",
            ),
            (
                "mediator.topic_controller",
                "mediators/topic_controller.md",
                "controller_packet",
            ),
            (
                "manager.research",
                "managers/research_manager.md",
                "artifact",
            ),
            ("trader", "traders/trader.md", "artifact"),
            ("risk.aggressive", "risk/aggressive.md", "risk_argument"),
            ("risk.neutral", "risk/neutral.md", "risk_argument"),
            ("risk.conservative", "risk/conservative.md", "risk_argument"),
            (
                "portfolio.manager",
                "managers/portfolio_manager.md",
                "artifact",
            ),
            ("allocation.manager", "allocation/manager.md", "artifact"),
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
            )
            .unwrap_or_else(|e| panic!("render failed for {role} ({rel}): {e}"));

            assert!(
                prompt.trim().len() > 40,
                "rendered prompt for {role} ({rel}) is suspiciously short"
            );
            for token in KNOWN_PLACEHOLDERS {
                assert!(
                    !prompt.contains(token),
                    "unresolved placeholder {token} in {role} ({rel})"
                );
            }
        }
    }

    #[test]
    fn golden_analyst_prompts_carry_schema_and_boundaries() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        for rel in [
            "analysts/technical.md",
            "analysts/news_macro.md",
            "analysts/reddit.md",
            "analysts/x.md",
            "analysts/youtube.md",
        ] {
            let path = prompts.join(rel);
            let role = format!(
                "analyst.{}",
                rel.trim_start_matches("analysts/").trim_end_matches(".md")
            );
            let prompt =
                render_prompt(&state, &role, 1, "artifact", None, None, Some(&path)).unwrap();
            // Machine-readable contract fields must reach the model.
            assert!(
                prompt.contains("direction"),
                "{rel} missing direction field"
            );
            assert!(
                prompt.contains("confidence"),
                "{rel} missing confidence field"
            );
            // Anti-injection boundary must be present for external-content roles.
            assert!(
                prompt.contains("外部内容边界") || prompt.contains("不是给你的指令"),
                "{rel} missing anti-injection boundary"
            );
        }
    }
}
