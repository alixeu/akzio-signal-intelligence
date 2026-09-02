//! Versioned, Rust-owned instrument and evidence requirement registry.

use std::collections::BTreeSet;

use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::{Asset, ContentHash, EvidenceNeed, DOMAIN_SCHEMA_VERSION};

pub const INSTRUMENT_EVIDENCE_REGISTRY_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Etf,
    DailyResetLeveragedEtf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentEvidenceProfile {
    pub asset: Asset,
    pub kind: InstrumentKind,
    pub issuer: &'static str,
    pub benchmark: &'static str,
    pub daily_leverage_multiplier: u8,
}

impl InstrumentEvidenceProfile {
    pub const fn daily_reset(self) -> bool {
        matches!(self.kind, InstrumentKind::DailyResetLeveragedEtf)
    }
}

pub const INSTRUMENT_EVIDENCE_REGISTRY: [InstrumentEvidenceProfile; 4] = [
    InstrumentEvidenceProfile {
        asset: Asset::Qqq,
        kind: InstrumentKind::Etf,
        issuer: "Invesco",
        benchmark: "Nasdaq-100 Index",
        daily_leverage_multiplier: 1,
    },
    InstrumentEvidenceProfile {
        asset: Asset::Tqqq,
        kind: InstrumentKind::DailyResetLeveragedEtf,
        issuer: "ProShares",
        benchmark: "Nasdaq-100 Index",
        daily_leverage_multiplier: 3,
    },
    InstrumentEvidenceProfile {
        asset: Asset::Soxx,
        kind: InstrumentKind::Etf,
        issuer: "iShares",
        benchmark: "NYSE Semiconductor Index",
        daily_leverage_multiplier: 1,
    },
    InstrumentEvidenceProfile {
        asset: Asset::Soxl,
        kind: InstrumentKind::DailyResetLeveragedEtf,
        issuer: "Direxion",
        benchmark: "NYSE Semiconductor Index",
        daily_leverage_multiplier: 3,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentEvidenceCategory {
    EtfHoldings,
    IndexMetadata,
    LeveragedEtfTerms,
    CorporateActions,
    EarningsEventCalendar,
    MacroReleaseCalendar,
    ImpliedVolatilityTermStructure,
    LiquiditySpread,
}

impl InstrumentEvidenceCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EtfHoldings => "etf_holdings",
            Self::IndexMetadata => "index_metadata",
            Self::LeveragedEtfTerms => "leveraged_etf_terms",
            Self::CorporateActions => "corporate_actions",
            Self::EarningsEventCalendar => "earnings_event_calendar",
            Self::MacroReleaseCalendar => "macro_release_calendar",
            Self::ImpliedVolatilityTermStructure => "implied_volatility_term_structure",
            Self::LiquiditySpread => "liquidity_spread",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstrumentEvidenceKey {
    pub asset: Option<Asset>,
    pub category: InstrumentEvidenceCategory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentEvidenceRequirement {
    pub registry_version: u32,
    pub key: InstrumentEvidenceKey,
    pub need: EvidenceNeed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentEvidenceBlocker {
    pub registry_version: u32,
    pub key: InstrumentEvidenceKey,
    pub expected_source_family: String,
    pub expected_resource: String,
}

pub fn instrument_evidence_registry_hash() -> ContentHash {
    let identity = INSTRUMENT_EVIDENCE_REGISTRY
        .iter()
        .map(|profile| {
            format!(
                "{}:{:?}:{}:{}:{}:{}",
                profile.asset,
                profile.kind,
                profile.issuer,
                profile.benchmark,
                profile.daily_leverage_multiplier,
                profile.daily_reset()
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    ContentHash::of_bytes(
        format!(
            "akzio.instrument_evidence_registry:v{INSTRUMENT_EVIDENCE_REGISTRY_VERSION}|{identity}"
        )
        .as_bytes(),
    )
}

pub fn instrument_evidence_requirements_for_session(
    session_key: &str,
) -> Vec<InstrumentEvidenceRequirement> {
    let session = NaiveDate::parse_from_str(session_key, "%Y-%m-%d").ok();
    let shifted = |days: i64| {
        session
            .and_then(|date| date.checked_add_signed(Duration::days(days)))
            .map(|date| date.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| session_key.to_owned())
    };
    let corporate_actions_start = shifted(-366);
    let option_expiration_end = shifted(120);
    let macro_release_end = shifted(45);
    let macro_vintage = shifted(-1);

    let mut requirements = Vec::new();
    for profile in INSTRUMENT_EVIDENCE_REGISTRY {
        let symbol = profile.asset.symbol();
        for category in [
            InstrumentEvidenceCategory::EtfHoldings,
            InstrumentEvidenceCategory::IndexMetadata,
            InstrumentEvidenceCategory::EarningsEventCalendar,
        ] {
            requirements.push(requirement(
                Some(profile.asset),
                category,
                "news_web",
                format!("research:{}:{symbol}:{session_key}", category.as_str()),
                24 * 60 * 60,
            ));
        }
        if profile.daily_reset() {
            requirements.push(requirement(
                Some(profile.asset),
                InstrumentEvidenceCategory::LeveragedEtfTerms,
                "news_web",
                format!("research:leveraged_etf_terms:{symbol}:{session_key}"),
                7 * 24 * 60 * 60,
            ));
        }
        requirements.push(requirement(
            Some(profile.asset),
            InstrumentEvidenceCategory::CorporateActions,
            "alpaca",
            format!("corporate_actions:{symbol}:{corporate_actions_start}:{session_key}"),
            24 * 60 * 60,
        ));
        requirements.push(requirement(
            Some(profile.asset),
            InstrumentEvidenceCategory::ImpliedVolatilityTermStructure,
            "alpaca",
            format!("option_chain:{symbol}:{session_key}:{option_expiration_end}"),
            5 * 60,
        ));
        requirements.push(requirement(
            Some(profile.asset),
            InstrumentEvidenceCategory::LiquiditySpread,
            "alpaca",
            "paper.quotes".to_owned(),
            5 * 60,
        ));
    }
    requirements.push(requirement(
        None,
        InstrumentEvidenceCategory::MacroReleaseCalendar,
        "fred",
        format!("release_calendar:{session_key}:{macro_release_end}:{macro_vintage}"),
        24 * 60 * 60,
    ));
    requirements
}

pub fn instrument_evidence_needs_for_session(session_key: &str) -> Vec<EvidenceNeed> {
    instrument_evidence_requirements_for_session(session_key)
        .into_iter()
        .map(|requirement| requirement.need)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn instrument_evidence_required_keys_for_asset(
    asset: Asset,
) -> BTreeSet<InstrumentEvidenceKey> {
    let daily_reset = INSTRUMENT_EVIDENCE_REGISTRY
        .iter()
        .find(|profile| profile.asset == asset)
        .is_some_and(|profile| profile.daily_reset());
    let mut categories = BTreeSet::from([
        InstrumentEvidenceCategory::EtfHoldings,
        InstrumentEvidenceCategory::IndexMetadata,
        InstrumentEvidenceCategory::CorporateActions,
        InstrumentEvidenceCategory::EarningsEventCalendar,
        InstrumentEvidenceCategory::ImpliedVolatilityTermStructure,
        InstrumentEvidenceCategory::LiquiditySpread,
    ]);
    if daily_reset {
        categories.insert(InstrumentEvidenceCategory::LeveragedEtfTerms);
    }
    let mut keys = categories
        .into_iter()
        .map(|category| InstrumentEvidenceKey {
            asset: Some(asset),
            category,
        })
        .collect::<BTreeSet<_>>();
    keys.insert(InstrumentEvidenceKey {
        asset: None,
        category: InstrumentEvidenceCategory::MacroReleaseCalendar,
    });
    keys
}

pub fn instrument_evidence_blockers_for_session<'a>(
    session_key: &str,
    resources: impl IntoIterator<Item = &'a str>,
) -> Vec<InstrumentEvidenceBlocker> {
    let actual = resources.into_iter().collect::<BTreeSet<_>>();
    instrument_evidence_requirements_for_session(session_key)
        .into_iter()
        .filter(|requirement| !actual.contains(requirement.need.resource.as_str()))
        .map(|requirement| InstrumentEvidenceBlocker {
            registry_version: INSTRUMENT_EVIDENCE_REGISTRY_VERSION,
            key: requirement.key,
            expected_source_family: requirement.need.source_family,
            expected_resource: requirement.need.resource,
        })
        .collect()
}

pub fn instrument_evidence_keys_for_resource(resource: &str) -> BTreeSet<InstrumentEvidenceKey> {
    if resource == "paper.quotes" {
        return Asset::EXECUTABLE
            .into_iter()
            .map(|asset| InstrumentEvidenceKey {
                asset: Some(asset),
                category: InstrumentEvidenceCategory::LiquiditySpread,
            })
            .collect();
    }
    if resource.starts_with("release_calendar:") {
        return BTreeSet::from([InstrumentEvidenceKey {
            asset: None,
            category: InstrumentEvidenceCategory::MacroReleaseCalendar,
        }]);
    }
    let parts = resource.split(':').collect::<Vec<_>>();
    let (category, symbol) = match parts.as_slice() {
        ["research", category, symbol, _] => (*category, *symbol),
        ["corporate_actions", symbol, _, _] => ("corporate_actions", *symbol),
        ["option_chain", symbol, _, _] => ("implied_volatility_term_structure", *symbol),
        _ => return BTreeSet::new(),
    };
    let Ok(asset) = Asset::try_from(symbol) else {
        return BTreeSet::new();
    };
    let category = match category {
        "etf_holdings" => InstrumentEvidenceCategory::EtfHoldings,
        "index_metadata" => InstrumentEvidenceCategory::IndexMetadata,
        "leveraged_etf_terms" => InstrumentEvidenceCategory::LeveragedEtfTerms,
        "earnings_event_calendar" => InstrumentEvidenceCategory::EarningsEventCalendar,
        "corporate_actions" => InstrumentEvidenceCategory::CorporateActions,
        "implied_volatility_term_structure" => {
            InstrumentEvidenceCategory::ImpliedVolatilityTermStructure
        }
        _ => return BTreeSet::new(),
    };
    BTreeSet::from([InstrumentEvidenceKey {
        asset: Some(asset),
        category,
    }])
}

fn requirement(
    asset: Option<Asset>,
    category: InstrumentEvidenceCategory,
    source_family: &str,
    resource: String,
    max_age_secs: u64,
) -> InstrumentEvidenceRequirement {
    InstrumentEvidenceRequirement {
        registry_version: INSTRUMENT_EVIDENCE_REGISTRY_VERSION,
        key: InstrumentEvidenceKey { asset, category },
        need: EvidenceNeed {
            schema_version: DOMAIN_SCHEMA_VERSION,
            source_family: source_family.to_owned(),
            resource,
            max_age_secs,
        },
    }
}
