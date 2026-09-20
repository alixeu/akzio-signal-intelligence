//! Native capability-probe instructions owned by the model client.
pub(super) const CALL: &str = "调用所需的 capability probe function 一次。\n";
pub(super) const FUNCTION_GOVERNANCE: &str =
    "只执行所需的 Akzio capability probe function call。\n";
pub(super) const WEB_GOVERNANCE: &str =
    "使用 Rust 批准的 native web search 一次，并返回来源 URL。\n";
pub(super) const WEB_QUERY: &str =
    "在 fred.stlouisfed.org 查找官方 FRED VIXCLS series 页面并引用其 URL。\n";
