//! Shared phase_summary compress gate: background jobs + sync wait for tool readers.
//!
//! After each business phase, compress runs concurrently with the next phase.
//! When a tool needs the phase_summary index while compress is still in flight, it waits
//! here until the relevant job(s) complete, then reads the updated memory index.

use crate::phase_index::{PhaseSummaryMemoryIndex, PhaseSummaryPhaseBatch};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

/// Process-wide registry so tool runtime can find the gate by `run_id`.
static REGISTRY: Mutex<Option<HashMap<String, Arc<PhaseSummaryGate>>>> = Mutex::new(None);

fn registry() -> std::sync::MutexGuard<'static, Option<HashMap<String, Arc<PhaseSummaryGate>>>> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn register_phase_summary_gate(run_id: &str, gate: Arc<PhaseSummaryGate>) {
    let mut reg = registry();
    let map = reg.get_or_insert_with(HashMap::new);
    map.insert(run_id.to_string(), gate);
}

pub fn phase_summary_gate(run_id: &str) -> Option<Arc<PhaseSummaryGate>> {
    registry().as_ref().and_then(|m| m.get(run_id).cloned())
}

pub fn unregister_phase_summary_gate(run_id: &str) {
    if let Some(map) = registry().as_mut() {
        map.remove(run_id);
    }
}

#[derive(Debug, Default)]
struct GateStatus {
    /// source_phase values currently compressing.
    inflight: HashSet<i64>,
    /// Last error per source_phase (if any).
    errors: HashMap<i64, String>,
}

/// Concurrent phase_summary compress coordination for one run.
pub struct PhaseSummaryGate {
    run_id: String,
    memory: RwLock<PhaseSummaryMemoryIndex>,
    status: Mutex<GateStatus>,
    cvar: Condvar,
}

impl std::fmt::Debug for PhaseSummaryGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhaseSummaryGate")
            .field("run_id", &self.run_id)
            .field("inflight", &self.inflight_phases())
            .finish()
    }
}

impl PhaseSummaryGate {
    pub fn new(run_id: impl Into<String>) -> Self {
        let run_id = run_id.into();
        Self {
            memory: RwLock::new(PhaseSummaryMemoryIndex::new(run_id.clone())),
            run_id,
            status: Mutex::new(GateStatus::default()),
            cvar: Condvar::new(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Mark compress for `source_phase` as in-flight (call before spawning job).
    pub fn mark_inflight(&self, source_phase: i64) {
        let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
        st.inflight.insert(source_phase);
        st.errors.remove(&source_phase);
    }

    /// Merge finished batch into memory and wake waiters.
    pub fn complete(&self, source_phase: i64, batch: PhaseSummaryPhaseBatch) {
        {
            let mut mem = self.memory.write().unwrap_or_else(|e| e.into_inner());
            if mem.run_id.is_empty() {
                mem.run_id = self.run_id.clone();
            }
            mem.merge(batch);
        }
        {
            let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
            st.inflight.remove(&source_phase);
            st.errors.remove(&source_phase);
        }
        self.cvar.notify_all();
    }

    /// Mark job failed and wake waiters (they may still read partial memory).
    pub fn fail(&self, source_phase: i64, error: impl Into<String>) {
        let error = error.into();
        {
            let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
            st.inflight.remove(&source_phase);
            st.errors.insert(source_phase, error);
        }
        self.cvar.notify_all();
    }

    /// Wait until compress jobs for phases `<= max_source_phase` (or all if None) are done.
    ///
    /// Used by tools that need the phase_summary index while the next business phase is running.
    pub fn wait_until_ready(&self, max_source_phase: Option<i64>, timeout: Duration) -> bool {
        self.wait_until_ready_checked(max_source_phase, timeout)
            .is_ok()
    }

    /// Wait for relevant compressor jobs and propagate compressor errors or timeout.
    pub fn wait_until_ready_checked(
        &self,
        max_source_phase: Option<i64>,
        timeout: Duration,
    ) -> Result<()> {
        let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let mut errors: Vec<_> = st
                .errors
                .iter()
                .filter(|(phase, _)| max_source_phase.is_none_or(|max| **phase <= max))
                .map(|(phase, error)| (*phase, error.clone()))
                .collect();
            errors.sort_by_key(|(phase, _)| *phase);
            if let Some((phase, error)) = errors.into_iter().next() {
                anyhow::bail!("phase_summary compressor failed for source_phase {phase}: {error}");
            }
            let busy = st.inflight.iter().any(|&p| match max_source_phase {
                Some(max) => p <= max,
                None => true,
            });
            if !busy {
                return Ok(());
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                let mut phases: Vec<_> = st
                    .inflight
                    .iter()
                    .filter(|phase| max_source_phase.is_none_or(|max| **phase <= max))
                    .copied()
                    .collect();
                phases.sort_unstable();
                anyhow::bail!("timed out waiting for phase_summary source phases {phases:?}");
            }
            let wait = deadline.saturating_duration_since(now);
            let (guard, timeout_result) = self
                .cvar
                .wait_timeout(st, wait)
                .unwrap_or_else(|e| e.into_inner());
            st = guard;
            if timeout_result.timed_out() {
                // re-check once more after timeout
                continue;
            }
        }
    }

    /// Snapshot current memory index (call after wait_until_ready when possible).
    pub fn snapshot(&self) -> PhaseSummaryMemoryIndex {
        self.memory
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether any compress job is still running.
    pub fn has_inflight(&self) -> bool {
        !self
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inflight
            .is_empty()
    }

    pub fn inflight_phases(&self) -> Vec<i64> {
        let mut v: Vec<i64> = self
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inflight
            .iter()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase_index::PhaseSummaryInput;
    use crate::AGGREGATE_TICKER;
    use serde_json::json;
    use std::thread;

    #[test]
    fn wait_blocks_until_complete() {
        let gate = Arc::new(PhaseSummaryGate::new("run-wait"));
        gate.mark_inflight(1);
        let g2 = gate.clone();
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let mut batch = PhaseSummaryPhaseBatch {
                source_phase: 1,
                ..Default::default()
            };
            batch.push_summary(&PhaseSummaryInput {
                run_id: "run-wait".into(),
                source_phase: 1,
                role: "compressor".into(),
                ticker: AGGREGATE_TICKER.into(),
                topic_id: None,
                summary: "ok".into(),
                summary_json: json!({}),
                confidence: 0.5,
            });
            g2.complete(1, batch);
        });
        assert!(gate.wait_until_ready(Some(1), Duration::from_secs(2)));
        assert_eq!(gate.snapshot().phases.len(), 1);
        h.join().unwrap();
    }

    #[test]
    fn checked_wait_propagates_compressor_error() {
        let gate = PhaseSummaryGate::new("run-error");
        gate.mark_inflight(1);
        gate.fail(1, "invalid summary bundle");

        let error = gate
            .wait_until_ready_checked(Some(1), Duration::from_secs(1))
            .unwrap_err();
        assert!(error.to_string().contains("invalid summary bundle"));
    }
}
