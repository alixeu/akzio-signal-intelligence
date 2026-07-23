pub mod alpaca;
pub mod archive;
pub mod candidate;
pub mod context;
pub mod importers;
pub mod maintenance;
pub mod memory;
pub mod outcome;
pub mod phase_index;
pub mod phase_summary_gate;
pub mod prediction;
pub mod reflection;
pub mod schema;
pub mod technical_store;
pub mod write;

pub use alpaca::{
    import_legacy_executions, record_account_snapshot, record_exact_execution, ExecutionRecord,
};
pub use context::{
    context_count, handle_read_command, messages_for_run, messages_text, read_run_context,
    session_history_items, sqlite_context, turn_history_items, RunContextReadRequest,
    RuntimeContext,
};
pub use importers::{
    import_jin10_payload, import_scored_jin10_items, jin10_item_id, record_jin10_attention,
    record_jin10_attention_for_turn, Jin10Attention,
};
pub use maintenance::{
    cleanup_database, core_query_plans, database_doctor, open_read_only, vacuum, wal_checkpoint,
    RetentionPolicy,
};
pub use phase_index::{
    clear_phase_compress, compressor_debug_snapshot, expand_attention_subjects, list_attention,
    list_phase_details_for_phase, list_phase_summaries, list_phase_summaries_for_phase,
    list_phase_summary_details, persist_phase_summary_batch, phase_detail_id, phase_summary_id,
    record_attention, record_attention_batch, upsert_phase_summary, upsert_phase_summary_detail,
    AttentionEvent, PhaseSummaryDetailInput, PhaseSummaryDetailRow, PhaseSummaryInput,
    PhaseSummaryMemoryIndex, PhaseSummaryPhaseBatch, PhaseSummaryRow,
};
pub use phase_summary_gate::{
    phase_summary_gate, register_phase_summary_gate, unregister_phase_summary_gate,
    PhaseSummaryGate,
};
pub use reflection::{
    pending_reflection_tasks, persist_reflection_artifact, read_experience,
    reflection_source_context, score_mature_predictions, set_reflection_task_status,
    upsert_decision_snapshot, DecisionSnapshotInput, PendingReflectionTask, ReflectionScoreSummary,
    ReflectionThresholds, PREDICTION_HORIZON_TRADING_DAYS,
};
pub use schema::{connect, ensure_schema, AGGREGATE_TICKER};
pub use technical_store::{
    close_after_trading_days as technical_close_after_trading_days,
    close_on_or_after as technical_close_on_or_after,
    close_on_or_before as technical_close_on_or_before, import_technical_csv, latest_technical_bar,
    load_technical_range, load_technical_series,
    minimum_close_between as technical_minimum_close_between, technical_row_count,
};
pub use write::{
    clear_agent_loop_history, set_run_current_phase, update_run_status, upsert_agent_turn,
    write_agent_message_scoped, write_role_turn_summary, write_run_record, AgentMessageInput,
    AgentTurnInput, RoleTurnSummaryInput, RunRecordInput, Scope,
};
