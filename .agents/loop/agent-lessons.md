# Akzio Agent Loop — Mechanism-Enforced Lessons Ledger (`agent-lessons.md`)

> **Rule (adapted from `glows777/Vela` and Geoffrey Huntley's Backpressure principle)**
>
> When an agent miss, hallucination, hollow test, or review bounce is confirmed, record the trigger path, the layer that should have caught it, and the mechanism patch. Close a lesson after the corresponding deterministic check is in place.

## Lesson 001 — Hollow Error-Mapping Tests (Tautological Unit Test)

- **Source** Akzio Round 1 (`P2-6`, `crates/akzio-daemon/src/launch.rs` / `scheduler_tick.rs`).
- **Trigger path** The initial `post_reservation_run_log_failure_is_not_unknown_submission` test constructed `SchedulerError::RunLogUnavailable` directly in the test body and asserted that it did not match `SubmissionUnknown`, missing the production mapping function.
- **Missed layer** Fresh-Context Verifier and Worker TDD.
- **Mechanism patch** Worker and Verifier require every regression test to invoke the production entry point or extracted pure production mapper, such as `classify_scheduler_error`, and assert an independently known result. `issue_hub.py verify-worktree` checks that every new `#[test]` and `#[tokio::test]` has a `/// WHY THIS TEST EXISTS (Task #<id>): ...` comment with the local task ID.

## Lesson 002 — `cargo fmt` Blind Spot on Legacy `include!` Fragments

- **Source** Akzio Audit `P2-10` (`crates/akzio-store/src/store/prelude.rs` and `crates/akzio-store/src/store.rs:49-66`).
- **Trigger path** `cargo fmt --all -- --check` can exit `0` when files pulled in via `include!(...)` are unformatted. Running `rustfmt --edition 2021 --check` unconditionally on all 11 legacy fragments would force mass reformatting of untouched lines.
- **Missed layer** Pre-Commit Backpressure.
- **Mechanism patch** `verify-worktree` checks the frozen base version of each modified fragment first. A formatted baseline requires the modified file to pass `rustfmt`. The diff check guards new whitespace errors in unformatted fragments, and the Verifier checks that the surrounding code keeps its existing formatting.

## Lesson 003 — Optimizing for Checker Output Instead of Fixing Behavior

- **Source** `glows777/Vela` (`Issue #137` and `PR #136`) and research on agent backpressure.
- **Trigger path** A Worker can silence compiler, Clippy, or test failures by adding `#[allow(...)]` or `#[ignore]`, changing visibility across architectural seams, or altering test expectations to match buggy output.
- **Missed layer** Pre-Commit Backpressure and Fresh-Context Verifier.
- **Mechanism patch** `verify-worktree` detects changed files outside the design and checks added Rust diff lines for lint and test suppressions. The Verifier reviews the fixed base-to-head diff for altered expectations and weakened assertions. Landing requires both results for the same SHA.

## Lesson 004 — Main Worktree Contamination and Stale Agent Actions

- **Source** Akzio workspace safety rules and Prompt Interference Audit (`2026-11-17`).
- **Trigger path** Git mutations in the main worktree can mix or overwrite the owner's uncommitted changes. Generic role markers and continuation hooks previously allowed the Leader and its children to interfere with one another. Mounting interactive TDD or multi-agent review skills also interrupted isolated roles. Interrupted launches can leave work and commands tied to an expired wave.
- **Missed layer** Hook Guard and Agent Isolation.
- **Mechanism patch** `claim` assigns an isolated `outputs/loop/worktrees/task-N` worktree. Role headers and hub actions bind each session to the current SQLite lease, task attempt, or verified SHA as appropriate. The blocking launcher releases the current lease when the Leader and its children finish and AGY confirms completion. An interrupted wave can be recovered after its lease expires, using the saved worktree and task state. The Worker and Verifier prompts include their own test and review protocols.
