//! Rust-owned, deterministic planning of Phase Summary units.
//!
//! Summary agents never choose how many indexes to create.  The workflow
//! derives the complete set from finalized source artifacts, then gives each
//! agent exactly one [`SummaryUnit`]. This module does not read or write a store.

use anyhow::{bail, ensure, Result};
use sha2::{Digest, Sha256};

const INDEX_ID_DOMAIN: &[u8] = b"akzio.phase_summary.index.v1\0";

/// One Rust-owned source shape for a completed business phase.
///
/// Collection members are source artifact identifiers, not model-provided
/// choices.  The planner canonicalizes their order and rejects duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummaryUnitScope {
    Phase1 {
        analyst_roles: Vec<String>,
        tickers: Vec<String>,
    },
    Phase2 {
        final_controller_topic_ids: Vec<String>,
    },
    Phase3 {
        tickers: Vec<String>,
    },
    Phase4 {
        tickers: Vec<String>,
    },
    Phase5 {
        risk_roles: Vec<String>,
        tickers: Vec<String>,
    },
    Phase6 {
        investable_assets: Vec<String>,
    },
    Phase7,
}

impl SummaryUnitScope {
    pub const fn source_phase(&self) -> u8 {
        match self {
            Self::Phase1 { .. } => 1,
            Self::Phase2 { .. } => 2,
            Self::Phase3 { .. } => 3,
            Self::Phase4 { .. } => 4,
            Self::Phase5 { .. } => 5,
            Self::Phase6 { .. } => 6,
            Self::Phase7 => 7,
        }
    }
}

/// Inputs that Rust passes to the deterministic planner after source
/// artifacts have finalized.  `source_payload_hash` must be the canonical
/// `sha256:<lowercase-hex>` hash of the exact source payload for this phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryUnitPlanRequest {
    pub run_id: String,
    pub source_payload_hash: String,
    pub max_units: usize,
    pub scope: SummaryUnitScope,
}

/// A fixed summary/index creation unit.  `index_id` is independent of any
/// text later produced by an LLM.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SummaryUnit {
    pub source_phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub index_id: String,
}

/// Produces the full, fixed list of summary work for one completed phase.
pub struct SummaryUnitPlanner;

impl SummaryUnitPlanner {
    pub fn plan(request: SummaryUnitPlanRequest) -> Result<Vec<SummaryUnit>> {
        validate_request(&request)?;
        let source_phase = request.scope.source_phase();
        let mut units = match request.scope {
            SummaryUnitScope::Phase1 {
                analyst_roles,
                tickers,
            } => cross_product_units(analyst_roles, tickers, |role, ticker| SummaryUnitSeed {
                unit_key: format!("phase1:analyst:{role}:ticker:{ticker}"),
                role,
                ticker: Some(ticker),
                topic_id: None,
            })?,
            SummaryUnitScope::Phase2 {
                final_controller_topic_ids,
            } => {
                let mut units = vec![SummaryUnitSeed {
                    role: "mediator.topic".to_string(),
                    ticker: None,
                    topic_id: None,
                    unit_key: "phase2:topic-generation:aggregate".to_string(),
                }];
                for topic_id in canonical_optional_items(
                    final_controller_topic_ids,
                    "phase2 final controller topic id",
                )? {
                    units.push(SummaryUnitSeed {
                        role: "mediator.topic_controller".to_string(),
                        ticker: None,
                        unit_key: format!("phase2:topic-controller:{topic_id}"),
                        topic_id: Some(topic_id),
                    });
                }
                units.push(SummaryUnitSeed {
                    role: "reducer.debate_final".to_string(),
                    ticker: None,
                    topic_id: None,
                    unit_key: "phase2:final-reducer:aggregate".to_string(),
                });
                units
            }
            SummaryUnitScope::Phase3 { tickers } => ticker_units(
                tickers,
                "phase3 ticker",
                "manager.research",
                "phase3:research-decision",
            )?,
            SummaryUnitScope::Phase4 { tickers } => {
                ticker_units(tickers, "phase4 ticker", "trader", "phase4:trade-intent")?
            }
            SummaryUnitScope::Phase5 {
                risk_roles,
                tickers,
            } => cross_product_units(
                canonical_items(risk_roles, "phase5 risk role")?,
                canonical_items(tickers, "phase5 ticker")?,
                |role, ticker| SummaryUnitSeed {
                    unit_key: format!("phase5:risk:{role}:ticker:{ticker}"),
                    role,
                    ticker: Some(ticker),
                    topic_id: None,
                },
            )?,
            SummaryUnitScope::Phase6 { investable_assets } => {
                let mut units = optional_ticker_units(
                    investable_assets,
                    "phase6 investable asset",
                    "portfolio.manager",
                    "phase6:portfolio-asset",
                )?;
                units.push(SummaryUnitSeed {
                    role: "portfolio.manager".to_string(),
                    ticker: None,
                    topic_id: None,
                    unit_key: "phase6:portfolio:aggregate".to_string(),
                });
                units
            }
            SummaryUnitScope::Phase7 => vec![SummaryUnitSeed {
                role: "allocator".to_string(),
                ticker: None,
                topic_id: None,
                unit_key: "phase7:allocation:aggregate".to_string(),
            }],
        };

        // Use the unit key as the primary presentation order. It includes the
        // phase-specific category and raw source identity, so independent
        // source collection ordering cannot change the plan.
        units.sort_by(|left, right| left.unit_key.cmp(&right.unit_key));
        ensure!(
            units.len() <= request.max_units,
            "phase {source_phase} needs {} summary units, exceeding configured maximum {}",
            units.len(),
            request.max_units
        );

        let planned = units
            .into_iter()
            .map(|seed| {
                SummaryUnit::from_seed(
                    request.run_id.as_str(),
                    source_phase,
                    seed,
                    request.source_payload_hash.as_str(),
                )
            })
            .collect::<Vec<_>>();
        ensure_unique_units(&planned)?;
        Ok(planned)
    }
}

#[derive(Debug)]
struct SummaryUnitSeed {
    role: String,
    ticker: Option<String>,
    topic_id: Option<String>,
    unit_key: String,
}

impl SummaryUnit {
    fn from_seed(
        run_id: &str,
        source_phase: u8,
        seed: SummaryUnitSeed,
        source_payload_hash: &str,
    ) -> Self {
        let index_id = derive_summary_index_id(
            run_id,
            source_phase,
            &seed.role,
            seed.ticker.as_deref(),
            seed.topic_id.as_deref(),
            &seed.unit_key,
            source_payload_hash,
        );
        Self {
            source_phase,
            role: seed.role,
            ticker: seed.ticker,
            topic_id: seed.topic_id,
            unit_key: seed.unit_key,
            source_payload_hash: source_payload_hash.to_string(),
            index_id,
        }
    }
}

/// Stable Index ID for a single Rust-owned unit.  Length-prefixed fields keep
/// the preimage unambiguous even when a role, ticker, or topic contains a
/// separator character. `None` differs from an empty string.
pub fn derive_summary_index_id(
    run_id: &str,
    source_phase: u8,
    role: &str,
    ticker: Option<&str>,
    topic_id: Option<&str>,
    unit_key: &str,
    source_payload_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(INDEX_ID_DOMAIN);
    hash_field(&mut hasher, run_id.as_bytes());
    hash_field(&mut hasher, source_phase.to_string().as_bytes());
    hash_field(&mut hasher, role.as_bytes());
    hash_optional_field(&mut hasher, ticker);
    hash_optional_field(&mut hasher, topic_id);
    hash_field(&mut hasher, unit_key.as_bytes());
    hash_field(&mut hasher, source_payload_hash.as_bytes());
    format!("idx-{:x}", hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_optional_field(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_field(hasher, value.as_bytes());
        }
        None => hasher.update([0]),
    }
}

fn validate_request(request: &SummaryUnitPlanRequest) -> Result<()> {
    ensure_non_empty("run_id", &request.run_id)?;
    ensure!(
        request.max_units > 0,
        "summary unit maximum must be greater than zero"
    );
    ensure!(
        is_content_hash(&request.source_payload_hash),
        "source_payload_hash must be canonical sha256:<lowercase-hex>"
    );
    Ok(())
}

fn is_content_hash(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn ticker_units(
    tickers: Vec<String>,
    field: &str,
    role: &str,
    unit_prefix: &str,
) -> Result<Vec<SummaryUnitSeed>> {
    canonical_items(tickers, field)?
        .into_iter()
        .map(|ticker| {
            Ok(SummaryUnitSeed {
                role: role.to_string(),
                ticker: Some(ticker.clone()),
                topic_id: None,
                unit_key: format!("{unit_prefix}:ticker:{ticker}"),
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn optional_ticker_units(
    tickers: Vec<String>,
    field: &str,
    role: &str,
    unit_prefix: &str,
) -> Result<Vec<SummaryUnitSeed>> {
    let units = canonical_optional_items(tickers, field)?
        .into_iter()
        .map(|ticker| SummaryUnitSeed {
            role: role.to_string(),
            ticker: Some(ticker.clone()),
            topic_id: None,
            unit_key: format!("{unit_prefix}:ticker:{ticker}"),
        })
        .collect::<Vec<_>>();
    Ok(units)
}

fn cross_product_units<F>(
    roles: Vec<String>,
    tickers: Vec<String>,
    make_unit: F,
) -> Result<Vec<SummaryUnitSeed>>
where
    F: FnMut(String, String) -> SummaryUnitSeed,
{
    let roles = canonical_items(roles, "role")?;
    let tickers = canonical_items(tickers, "ticker")?;
    let mut make_unit = make_unit;
    Ok(roles
        .into_iter()
        .flat_map(|role| {
            tickers
                .iter()
                .cloned()
                .map(move |ticker| (role.clone(), ticker))
        })
        .map(|(role, ticker)| make_unit(role, ticker))
        .collect())
}

fn canonical_items(items: Vec<String>, field: &str) -> Result<Vec<String>> {
    ensure!(!items.is_empty(), "{field} collection must not be empty");
    canonical_optional_items(items, field)
}

fn canonical_optional_items(mut items: Vec<String>, field: &str) -> Result<Vec<String>> {
    for item in &items {
        ensure_non_empty(field, item)?;
    }
    items.sort();
    if let Some(duplicate) = items.windows(2).find(|pair| pair[0] == pair[1]) {
        bail!("duplicate {field}: {}", duplicate[0]);
    }
    Ok(items)
}

fn ensure_non_empty(field: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{field} must not be empty");
    Ok(())
}

fn ensure_unique_units(units: &[SummaryUnit]) -> Result<()> {
    for pair in units.windows(2) {
        if pair[0].unit_key == pair[1].unit_key {
            bail!("duplicate summary unit key: {}", pair[0].unit_key);
        }
    }
    let mut ids = units
        .iter()
        .map(|unit| unit.index_id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    if let Some(duplicate) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
        bail!("summary index id collision: {}", duplicate[0]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        derive_summary_index_id, SummaryUnitPlanRequest, SummaryUnitPlanner, SummaryUnitScope,
    };

    const SOURCE_HASH: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn request(scope: SummaryUnitScope) -> SummaryUnitPlanRequest {
        SummaryUnitPlanRequest {
            run_id: "run-2026-07-27".to_string(),
            source_payload_hash: SOURCE_HASH.to_string(),
            max_units: 32,
            scope,
        }
    }

    #[test]
    fn phase1_plans_one_unit_per_role_and_ticker_in_canonical_order() {
        let units = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase1 {
            analyst_roles: vec!["analyst.news_macro".into(), "analyst.technical".into()],
            tickers: vec!["SOXX".into(), "QQQ".into()],
        }))
        .unwrap();

        assert_eq!(units.len(), 4);
        assert_eq!(
            units
                .iter()
                .map(|unit| unit.unit_key.as_str())
                .collect::<Vec<_>>(),
            vec![
                "phase1:analyst:analyst.news_macro:ticker:QQQ",
                "phase1:analyst:analyst.news_macro:ticker:SOXX",
                "phase1:analyst:analyst.technical:ticker:QQQ",
                "phase1:analyst:analyst.technical:ticker:SOXX",
            ]
        );
        assert!(units.iter().all(|unit| unit.topic_id.is_none()));
        assert!(units.iter().all(|unit| unit.index_id.starts_with("idx-")));
    }

    #[test]
    fn phase2_has_only_topic_generation_final_controllers_and_final_reducer() {
        let units = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase2 {
            final_controller_topic_ids: vec!["rates".into(), "ai".into()],
        }))
        .unwrap();

        assert_eq!(units.len(), 4);
        assert_eq!(
            units
                .iter()
                .map(|unit| unit.unit_key.as_str())
                .collect::<Vec<_>>(),
            vec![
                "phase2:final-reducer:aggregate",
                "phase2:topic-controller:ai",
                "phase2:topic-controller:rates",
                "phase2:topic-generation:aggregate",
            ]
        );
        assert_eq!(units[1].role, "mediator.topic_controller");
        assert_eq!(units[1].topic_id.as_deref(), Some("ai"));
        assert!(units.iter().all(|unit| unit.ticker.is_none()));

        let no_topics = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase2 {
            final_controller_topic_ids: vec![],
        }))
        .unwrap();
        assert_eq!(no_topics.len(), 2);
        assert!(no_topics
            .iter()
            .all(|unit| unit.topic_id.is_none() && unit.ticker.is_none()));
    }

    #[test]
    fn phase5_is_risk_role_by_ticker_and_phase6_adds_portfolio_aggregate() {
        let phase5 = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase5 {
            risk_roles: vec!["risk.neutral".into(), "risk.aggressive".into()],
            tickers: vec!["QQQ".into(), "SOXX".into()],
        }))
        .unwrap();
        assert_eq!(phase5.len(), 4);
        assert!(phase5.iter().all(|unit| unit.role.starts_with("risk.")));
        assert!(phase5.iter().all(|unit| unit.ticker.is_some()));

        let phase6 = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase6 {
            investable_assets: vec!["SOXX".into(), "QQQ".into()],
        }))
        .unwrap();
        assert_eq!(phase6.len(), 3);
        assert!(phase6
            .iter()
            .any(|unit| unit.unit_key == "phase6:portfolio:aggregate"));
        assert_eq!(
            phase6.iter().filter(|unit| unit.ticker.is_some()).count(),
            2
        );

        let no_investable_assets = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase6 {
            investable_assets: vec![],
        }))
        .unwrap();
        assert_eq!(no_investable_assets.len(), 1);
        assert_eq!(
            no_investable_assets[0].unit_key,
            "phase6:portfolio:aggregate"
        );
    }

    #[test]
    fn phase3_phase4_and_phase7_are_exactly_ticker_or_aggregate_units() {
        let phase3 = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase3 {
            tickers: vec!["QQQ".into()],
        }))
        .unwrap();
        assert_eq!(phase3.len(), 1);
        assert_eq!(phase3[0].role, "manager.research");

        let phase4 = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase4 {
            tickers: vec!["QQQ".into()],
        }))
        .unwrap();
        assert_eq!(phase4.len(), 1);
        assert_eq!(phase4[0].role, "trader");

        let phase7 = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase7)).unwrap();
        assert_eq!(phase7.len(), 1);
        assert_eq!(phase7[0].role, "allocator");
        assert_eq!(phase7[0].ticker, None);
    }

    #[test]
    fn ids_change_with_source_payload_but_not_source_collection_order() {
        let ordered = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase1 {
            analyst_roles: vec!["analyst.technical".into()],
            tickers: vec!["QQQ".into(), "SOXX".into()],
        }))
        .unwrap();
        let reversed = SummaryUnitPlanner::plan(request(SummaryUnitScope::Phase1 {
            analyst_roles: vec!["analyst.technical".into()],
            tickers: vec!["SOXX".into(), "QQQ".into()],
        }))
        .unwrap();
        assert_eq!(ordered, reversed);

        let mut changed_source = request(SummaryUnitScope::Phase1 {
            analyst_roles: vec!["analyst.technical".into()],
            tickers: vec!["QQQ".into(), "SOXX".into()],
        });
        changed_source.source_payload_hash =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into();
        let changed = SummaryUnitPlanner::plan(changed_source).unwrap();
        assert_ne!(ordered[0].index_id, changed[0].index_id);
    }

    #[test]
    fn validates_maximum_duplicates_and_hash_format() {
        let mut too_small = request(SummaryUnitScope::Phase1 {
            analyst_roles: vec!["analyst.technical".into()],
            tickers: vec!["QQQ".into(), "SOXX".into()],
        });
        too_small.max_units = 1;
        assert!(SummaryUnitPlanner::plan(too_small)
            .unwrap_err()
            .to_string()
            .contains("exceeding configured maximum"));

        let duplicate = request(SummaryUnitScope::Phase3 {
            tickers: vec!["QQQ".into(), "QQQ".into()],
        });
        assert!(SummaryUnitPlanner::plan(duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let mut invalid_hash = request(SummaryUnitScope::Phase7);
        invalid_hash.source_payload_hash = "not-a-hash".into();
        assert!(SummaryUnitPlanner::plan(invalid_hash)
            .unwrap_err()
            .to_string()
            .contains("source_payload_hash"));
    }

    #[test]
    fn id_preimage_distinguishes_none_empty_and_separator_values() {
        let none = derive_summary_index_id(
            "run",
            2,
            "role:with:separator",
            None,
            Some("topic"),
            "unit",
            SOURCE_HASH,
        );
        let empty = derive_summary_index_id(
            "run",
            2,
            "role",
            Some(""),
            Some("topic"),
            "unit",
            SOURCE_HASH,
        );
        assert_ne!(none, empty);
    }
}
