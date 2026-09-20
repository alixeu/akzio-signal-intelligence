//! Evidence acquisition wording and typed resource rendering.
use crate::{EvidenceAdapterError, EvidenceSource, GovernedResource};

pub(crate) const WEB_GOVERNANCE: &str =
    "只使用 Rust 批准的 native web tool。为每个实质性事实返回可核验的 citations。\n";

pub(crate) fn research_intent(
    source: EvidenceSource,
    resource: &str,
) -> Result<String, EvidenceAdapterError> {
    let research_intent = match GovernedResource::parse(source, resource).map_err(|error| {
            EvidenceAdapterError::Policy {
                evidence_source: source,
                resource: resource.to_owned(),
                reason: error.to_string(),
            }
        })? {
            GovernedResource::NewsWeb { query } => query,
            GovernedResource::RecentNews {
                asset,
                window_start,
                window_end,
                topic,
            } => format!(
                "资产 {} 的 {} 近期新闻和事件，时间范围从 {} 到 {}。搜索该 ETF、其基础指数、主要成分股，以及行业或货币政策事件。区分报道事实和推断出的 ETF 影响。最多使用三个来自允许 domain 的相关文章；报告发布日期。产品机制不是近期新闻。\n",
                asset.symbol(),
                topic,
                window_start,
                window_end
            ),
            GovernedResource::OfficialFundHoldings { asset, as_of } => {
                format!(
                    "官方完整 {} ETF 持仓，截至 {}\n",
                    asset.symbol(),
                    as_of
                )
            }
            GovernedResource::OfficialIndexMetadata { asset, as_of } => {
                format!(
                    "官方 {} 基准指数截至 {} 的元数据\n",
                    asset.symbol(),
                    as_of
                )
            }
            GovernedResource::OfficialLeveragedEtfTerms { asset, as_of } => format!(
                "官方 {} 每日重置杠杆条款和风险，截至 {}\n",
                asset.symbol(),
                as_of
            ),
            GovernedResource::OfficialEarningsEventCalendar { asset, as_of } => format!(
                "官方 {} 成分公司截至 {} 的财报日历\n",
                asset.symbol(),
                as_of
            ),
            _ => resource.to_owned(),
        };
    Ok(research_intent)
}
