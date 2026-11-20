#!/usr/bin/env bash
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"
ISSUE_HUB="$REPO_ROOT/.agents/loop/bin/issue_hub.py"
ISSUE_BRIDGE="$REPO_ROOT/.agents/loop/bin/issue_bridge.py"
AGY_BIN="${AGY_BIN:-agy}"
LEADER_MODEL="${LEADER_MODEL:-}"
WAVE_INTERVAL_SECS="${WAVE_INTERVAL_SECS:-900}"
LOG_DIR="$REPO_ROOT/outputs/loop/logs"

usage() {
  cat <<'EOF'
Usage: bash .agents/loop/bin/loop_daemon.sh <command> [options]

Commands:
  init            Initialize local state and print the queue JSON
  status          Print the local queue JSON
  wave            Run one Leader wave and perform configured delivery
  daemon          Run waves, deliver accepted work, and generate due reports
  deliver         Retry delivery to origin/main and the configured GitHub issue
  sync-issues     Process authorized Issue commands, decision replies, and PR deliveries
  daily-report    Generate a local report now (or only if due with --if-due)
  publish-log     Append pending audit events to an existing GitHub issue
                  Usage: publish-log --repo OWNER/REPO --issue N

Environment overrides:
  AGY_BIN=agy                  Path or command name of the Antigravity CLI
  LEADER_MODEL=<model>         Optional AGY model slug; otherwise use the agent configuration
  WAVE_INTERVAL_SECS=900       Seconds between daemon ticks
  AKZIO_LOOP_AUDIT_REPO         GitHub OWNER/REPO (Git config: akzioLoop.auditRepo)
  AKZIO_LOOP_AUDIT_ISSUE        Existing issue number (Git config: akzioLoop.auditIssue)
  Configure both values to enable automatic delivery to origin/main and the issue.
  akzioLoop.operatorId         Numeric GitHub user ID, set in local Git config to enable Issue sync
EOF
}

run_single_wave() {
  local agy_bin wave_json wave_info wave expected_task event_checkpoint timestamp log prompt
  if ! agy_bin="$(type -P "$AGY_BIN")"; then
    printf 'ERROR: Antigravity CLI %s is unavailable. Set AGY_BIN to its executable.\n' "$AGY_BIN" >&2
    return 1
  fi
  mkdir -p "$LOG_DIR" || return
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)" || return

  wave_json="$(python3 "$ISSUE_HUB" start-wave)" || return
  wave_info="$(python3 -c '
import json
import re
import sys

result = json.load(sys.stdin)
if isinstance(result, dict) and result.get("status") == "busy":
    print("busy")
elif isinstance(result, dict) and result.get("status") == "started":
    wave = result.get("wave")
    if not isinstance(wave, str) or not re.fullmatch(r"[0-9a-f]{32}", wave):
        sys.exit("Invalid wave token from issue_hub.py start-wave")
    task, checkpoint = result.get("expected_task"), result.get("event_checkpoint")
    if (task is not None and (type(task) is not int or task < 1)) or type(checkpoint) is not int or checkpoint < 0:
        sys.exit("Invalid task checkpoint from issue_hub.py start-wave")
    print(wave, task or 0, checkpoint)
else:
    sys.exit("Unexpected response from issue_hub.py start-wave")
' <<< "$wave_json")" || return
  if [[ "$wave_info" == busy ]]; then
    printf 'Another wave owns the SQLite lease. Skipping this tick.\n'
    return 0
  fi

  IFS=' ' read -r wave expected_task event_checkpoint <<< "$wave_info"
  log="$LOG_DIR/wave-$timestamp-$$.jsonl"
  prompt="[AKZIO-LOOP-LEADER] $wave
请从仓库根目录按 .agents/agents/loop-leader.md 和 autonomous-loop 技能执行本轮的一个队列动作。
读取规程后，先调用终端工具执行 python3 .agents/loop/bin/issue_hub.py poll。若工具调用失败，以实际错误为准。
等本轮子智能体结束并确认任务状态已写入 SQLite 后再返回。"
  # The default SQLite lease lasts two hours. Bound the blocking CLI turn within it.
  local -a agy_args=(--agent loop-leader --sandbox --print-timeout 110m --output-format stream-json)
  if [[ -n "$LEADER_MODEL" ]]; then
    agy_args+=(--model "$LEADER_MODEL")
  fi
  printf '[%s] Running Leader wave\n' "$timestamp"
  if ! "$agy_bin" "${agy_args[@]}" -p "$prompt" 2> >(tee -a "$log.stderr" >&2) | tee "$log"; then
    printf 'WARN: Leader exited unsuccessfully. The lease will expire.\n' >&2
    return 1
  fi
  if ! python3 - "$REPO_ROOT" "$log" <<'AGY_RESULT'
import json
from pathlib import Path
import sys

root, output = map(Path, sys.argv[1:])
with output.open(encoding="utf-8") as stream:
    events = [json.loads(line) for line in stream if line.strip()]
starts = [event for event in events if event.get("event") == "init"]
ends = [event["result"] for event in events if event.get("event") == "result"]
if len(starts) != 1 or len(ends) != 1:
    sys.exit("Expected one Antigravity init and one final result.")
start, result = starts[0], ends[0]
metadata = start["init"]
if (
    Path(metadata["cwd"]).resolve() != root.resolve()
    or metadata.get("agent") != "loop-leader"
    or not start.get("conversation_id")
    or start["conversation_id"] != result.get("conversation_id")
    or result.get("status") != "SUCCESS"
):
    sys.exit("The Antigravity result did not confirm a completed Leader turn here.")
AGY_RESULT
  then
    printf 'WARN: Leader completion could not be confirmed. The lease will expire.\n' >&2
    return 1
  fi
  local -a finish_args=(finish-wave --wave "$wave")
  if [[ "$expected_task" != 0 ]]; then
    finish_args+=(--expect-task "$expected_task" --after-event "$event_checkpoint")
  fi
  python3 "$ISSUE_HUB" "${finish_args[@]}"
}

deliver_configured() {
  local repo="${AKZIO_LOOP_AUDIT_REPO:-}"
  local issue="${AKZIO_LOOP_AUDIT_ISSUE:-}"
  local required="${1:-false}"
  if [[ -z "$repo" ]]; then
    repo="$(git config --local --get akzioLoop.auditRepo || true)"
  fi
  if [[ -z "$issue" ]]; then
    issue="$(git config --local --get akzioLoop.auditIssue || true)"
  fi
  if [[ -z "$repo" && -z "$issue" && "$required" == false ]]; then
    return 0
  fi
  if [[ ! "$repo" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ || ! "$issue" =~ ^0*[1-9][0-9]*$ ]]; then
    printf 'ERROR: Set a GitHub repository (OWNER/REPO) and issue number to enable Loop delivery.\n' >&2
    return 1
  fi

  local failed=0
  python3 "$ISSUE_HUB" deliver-main || failed=1
  python3 "$ISSUE_HUB" publish-log --repo "$repo" --issue "$issue" || failed=1
  return "$failed"
}

sync_configured_issues() {
  local operator repo issue required="${1:-false}"
  operator="$(git config --local --get akzioLoop.operatorId || true)"
  if [[ -z "$operator" && "$required" == false ]]; then
    return 0
  fi
  repo="${AKZIO_LOOP_AUDIT_REPO:-$(git config --local --get akzioLoop.auditRepo || true)}"
  issue="${AKZIO_LOOP_AUDIT_ISSUE:-$(git config --local --get akzioLoop.auditIssue || true)}"
  if [[ -z "$operator" || -z "$repo" || -z "$issue" ]]; then
    printf 'ERROR: Configure akzioLoop.operatorId, the GitHub repository, and the audit Issue.\n' >&2
    return 1
  fi
  python3 "$ISSUE_BRIDGE" sync --repo "$repo" --operator "$operator" --audit-issue "$issue"
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  init)
    python3 "$ISSUE_HUB" init "$@"
    ;;
  status)
    python3 "$ISSUE_HUB" poll "$@"
    ;;
  wave)
    failed=0
    sync_configured_issues || failed=1
    run_single_wave || failed=1
    sync_configured_issues || failed=1
    deliver_configured || failed=1
    exit "$failed"
    ;;
  deliver)
    deliver_configured true
    ;;
  sync-issues)
    sync_configured_issues true
    ;;
  daily-report)
    python3 "$ISSUE_HUB" daily-report "$@"
    ;;
  publish-log)
    python3 "$ISSUE_HUB" publish-log "$@"
    ;;
  daemon)
    printf 'Starting Akzio Loop Daemon (interval=%ss)\n' "$WAVE_INTERVAL_SECS"
    while true; do
      if ! python3 "$ISSUE_HUB" daily-report --if-due; then
        printf 'WARN: Local daily report failed. The next tick will retry.\n' >&2
      fi
      if ! sync_configured_issues; then
        printf 'WARN: Issue synchronization is pending. The next tick will retry.\n' >&2
      fi
      if ! run_single_wave; then
        printf 'WARN: Wave failed. The daemon will try again on its next tick.\n' >&2
      fi
      if ! sync_configured_issues; then
        printf 'WARN: Issue synchronization is pending. The next tick will retry.\n' >&2
      fi
      if ! deliver_configured; then
        printf 'WARN: Delivery is pending. The next tick will retry.\n' >&2
      fi
      sleep "$WAVE_INTERVAL_SECS"
    done
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
