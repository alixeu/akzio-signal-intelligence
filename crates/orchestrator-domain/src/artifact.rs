use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleJobResult {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub tickers: Vec<String>,
    pub artifact: Option<Value>,
    pub error: Option<String>,
    pub timed_out: bool,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentTurnInfo {
    pub turn_id: String,
    pub session_id: String,
    pub run_id: String,
    pub phase: i64,
    pub role: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rating {
    Buy,
    Overweight,
    Hold,
    Underweight,
    Sell,
}

impl Rating {
    pub fn as_str(&self) -> &'static str {
        match self {
            Rating::Buy => "Buy",
            Rating::Overweight => "Overweight",
            Rating::Hold => "Hold",
            Rating::Underweight => "Underweight",
            Rating::Sell => "Sell",
        }
    }
}
