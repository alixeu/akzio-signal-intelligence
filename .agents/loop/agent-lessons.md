# Akzio Agent Loop — Mechanism-Enforced Lessons Ledger (`agent-lessons.md`)

> **Rule (adapted from `glows777/Vela` & Geoffrey Huntley's Backpressure principle)**:
> Every time an agent miss, hallucination, hollow test, or review bounce is confirmed, record a lesson here with:
> 1. **Trigger Path**: Who did what, in which crate/file, and what went wrong.
> 2. **Missed Layer**: Which layer should have caught it (`Pre-Commit Backpressure`, `Fresh-Context Verifier`, `CI`, or `Leader Design`).
> 3. **Mechanism Patch**: Which deterministic script check in `.agents/loop/bin/issue_hub.py`, `.agents/loop/bin/hook_guard.py`, or rule in `.agents/agents/*.md` was updated.
> **A lesson without a deterministic mechanism patch is NOT closed.**

---

## Lesson 001 — Hollow Error-Mapping Tests (Tautological Unit Test)
- **Source**: Akzio Round 1 (`P2-6`, `crates/akzio-daemon/src/launch.rs` / `scheduler_tick.rs`)
- **Trigger Path**: An initial test `post_reservation_run_log_failure_is_not_unknown_submission` constructed `SchedulerError::RunLogUnavailable` directly in the test body and asserted it did not match `SubmissionUnknown`, without calling any production mapping function.
- **Missed Layer**: `Fresh-Context Verifier` & `Worker TDD`
- **Mechanism Patch**:
  - Added explicit rule in `.agents/agents/rust-worker.md` and `.agents/agents/rust-verifier.md`: Every regression test must invoke the **production entry point or extracted pure production mapper** (e.g., `classify_scheduler_error`), never assert on a locally constructed struct/enum variant in isolation.
  - Every new `#[test]` / `#[tokio::test]` in `.rs` diffs must carry a preceding `/// WHY THIS TEST EXISTS (Issue #<N>): ...` doc comment, checked per-test by `.agents/loop/bin/issue_hub.py verify-worktree`.

## Lesson 002 — `cargo fmt` Blindspot on Pre-Existing Unformatted `include!` Fragments
- **Source**: Akzio Audit `P2-10` (`crates/akzio-store/src/store/prelude.rs` and `crates/akzio-store/src/store.rs:49-66`)
- **Trigger Path**: `cargo fmt --all -- --check` exits `0` even when files pulled in via `include!(...)` are unformatted, while running `rustfmt --edition 2021 --check` unconditionally on all 11 legacy `include!` files causes a guaranteed deadlock unless untouched lines are mass-reformatted.
- **Missed Layer**: `Pre-Commit Backpressure`
- **Mechanism Patch**:
  - `.agents/loop/bin/issue_hub.py verify-worktree` inspects `git show <base_ref>:<file>` via `rustfmt --edition 2021 --check` first: if the base file was already formatted, the modified file must pass `rustfmt`; if the base file was legacy-unformatted, it avoids forcing mass reformatting and enforces `git diff --check` + clean formatting on newly touched lines.

## Lesson 003 — Optimizing for Checker Output Instead of Fixing Behavior (Reward-Hacking)
- **Source**: `glows777/Vela` (`Issue #137 / PR #136`) & X Agent Backpressure research
- **Trigger Path**: Cheap worker models sometimes silence compiler/clippy/test failures by adding `#[allow(...)]`, `#[ignore]`, changing visibility across architectural seams, or altering test expectations to match buggy output.
- **Missed Layer**: `Pre-Commit Backpressure` & `Fresh-Context Verifier`
- **Mechanism Patch**:
  - `.agents/loop/bin/issue_hub.py verify-worktree` deterministically scans added `.rs` diff lines and hard-fails if `#[allow(`, `#![allow(`, `#[ignore]`, or unauthorized file edits outside the `[LOOP-DESIGN]` `allowed_files` whitelist appear.

## Lesson 004 — Main Working Tree Contamination & Subagent Hook Interference
- **Source**: Akzio Workspace Safety Rule (`AGENTS.md` §5) & Prompt Interference Audit (`2026-11-17`)
- **Trigger Path**: (1) Running mutating `git` commands in the primary working directory mixes or wipes uncommitted human work; (2) using a single `[AKZIO-AUTONOMOUS-LOOP]` token for both Leader and Subagents causes `Stop` hooks to hijack subagents or `PreToolUse` to stall subagents on `"ask"`; (3) mounting interactive `tdd` / multi-subagent `code-review` skills onto isolated Flash subagents causes premature human prompts or nested subagent loops.
- **Missed Layer**: `Hook Guard` & `Agent Isolation`
- **Mechanism Patch**:
  - `.agents/loop/bin/issue_hub.py worktree-create` creates isolated Git worktrees under `outputs/loop/worktrees/issue-<N>` branched from `loop/nightly` and checks fast-forward ancestry in `worktree-land`.
  - `.agents/loop/bin/hook_guard.py` separates `LEADER_TOKEN` (`[AKZIO-AUTONOMOUS-LOOP]`) and `SUBAGENT_TOKEN` (`[AKZIO-LOOP-SUBAGENT]`), enforces root-workspace git & `~/.akzio` path safety only in loop mode without breaking interactive sessions, and keeps `queue_cache.json` synchronized across state transitions.
  - `.agents/agents/rust-worker.md` and `.agents/agents/rust-verifier.md` inline self-contained Red-Green & dual-axis verification protocols instead of mounting conflicting interactive skills, and `AGENTS.md` stays under Antigravity's 24 KB rule limit.
