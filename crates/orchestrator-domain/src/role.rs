use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnalystRole {
    Technical,
    NewsMacro,
    Youtube,
    Reddit,
    X,
}

impl AnalystRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            AnalystRole::Technical => "analyst.technical",
            AnalystRole::NewsMacro => "analyst.news_macro",
            AnalystRole::Youtube => "analyst.youtube",
            AnalystRole::Reddit => "analyst.reddit",
            AnalystRole::X => "analyst.x",
        }
    }

    pub fn parse(key: &str) -> Option<Self> {
        match key.trim() {
            "technical" | "analyst.technical" => Some(AnalystRole::Technical),
            "news" | "news_macro" | "analyst.news_macro" => Some(AnalystRole::NewsMacro),
            "youtube" | "analyst.youtube" => Some(AnalystRole::Youtube),
            "reddit" | "analyst.reddit" => Some(AnalystRole::Reddit),
            "x" | "analyst.x" => Some(AnalystRole::X),
            _ => None,
        }
    }

    pub fn preflight_tool(&self) -> &'static str {
        match self {
            AnalystRole::Technical => "run_technical_indicators",
            AnalystRole::NewsMacro => "fetch_jin10_flash",
            AnalystRole::Youtube => "fetch_social_youtube",
            AnalystRole::Reddit => "fetch_social_reddit",
            AnalystRole::X => "fetch_social_x",
        }
    }

    pub fn phase1_short_name(&self) -> &'static str {
        match self {
            AnalystRole::Technical => "technical",
            AnalystRole::NewsMacro => "news",
            AnalystRole::Youtube => "youtube",
            AnalystRole::Reddit => "reddit",
            AnalystRole::X => "x",
        }
    }
}
