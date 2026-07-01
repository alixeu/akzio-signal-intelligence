use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Phase {
    Phase1 = 1,
    Phase1Reducer = 15,
    Phase2 = 2,
    Phase2TopicGen = 21,
    Phase2Reducer = 25,
    Phase3 = 3,
    Phase3MemoryReflector = 35,
}

impl Phase {
    pub fn as_i64(self) -> i64 {
        self as i64
    }
}
