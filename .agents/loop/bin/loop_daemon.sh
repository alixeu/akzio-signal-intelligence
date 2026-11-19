#!/usr/bin/env bash
set -euo pipefail

# Akzio 24h Unattended Agent Loop Runner
# Coordinates fresh-context Leader waves via Antigravity `agentapi new-conversation`
# and bridges everything through GitHub Issues (`issue_hub.py`).

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
ISSUE_HUB="$REPO_ROOT/.agents/loop/bin/issue_hub.py"
AGENTAPI_BIN="${AGENTAPI_BIN:-$HOME/.gemini/antigravity/bin/agentapi}"
LEADER_MODEL="${LEADER_MODEL:-pro}"
WAVE_INTERVAL_SECS="${WAVE_INTERVAL_SECS:-900}"
STATE_DIR="$REPO_ROOT/outputs/loop/state"
LOG_DIR="$REPO_ROOT/outputs/loop/logs"
LOCK_DIR="$STATE_DIR/wave.lock"
mkdir -p "$STATE_DIR" "$LOG_DIR"

usage() {
  cat <<'EOF'
Usage: bash .agents/loop/bin/loop_daemon.sh <command> [options]

Commands:
  init            Initialize GitHub labels (loop:inbox, loop:ready, loop:done, loop:blocked, etc.)
  status          Poll GitHub Issues queue and print current state JSON
  wave            Launch a single fresh-context Leader wave via Antigravity `agentapi`
  daemon          Run the 24h continuous unattended loop (wakes Leader on each interval + daily report)
  daily-report    Generate the 24h daily report immediately and publish to GitHub Issues

Environment overrides:
  LEADER_MODEL=pro              Model tier or explicit model ID (e.g. default/gemini-4-hydrogen:xhigh)
                                Note: `agentapi` falls back to `flash` if a specified model ID is unavailable.
  WAVE_INTERVAL_SECS=900        Seconds between waves in `daemon` mode (default: 900s = 15m)
  AGENTAPI_BIN=/path/agentapi   Path to Antigravity `agentapi` binary
EOF
}

acquire_wave_lock() {
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    local old_pid=""
    if [[ -f "$LOCK_DIR/pid" ]]; then
      old_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    fi
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "[SKIP] Another loop wave (pid=$old_pid) is still running; skipping overlapping tick." >&2
      return 1
    fi

    echo "[WARN] Removing stale wave lock (pid=${old_pid:-unknown})." >&2
    rm -rf "$LOCK_DIR" || return
    mkdir "$LOCK_DIR" || return
  fi

  trap 'rm -rf "$LOCK_DIR"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  printf '%s\n' "${BASHPID:-$$}" >"$LOCK_DIR/pid"
}

build_wave_prompt() {
  local poll_json="$1"
  cat <<EOF
[AKZIO-AUTONOMOUS-LOOP]
你是 Akzio 24h 无人值守研发循环的 Loop Leader。请严格按照 \`.agents/agents/loop-leader.md\` 与 \`.agents/skills/autonomous-loop/SKILL.md\` 执行本轮波次（One Issue Per Wave Step）：

当前 \`issue_hub.py poll\` 快照：
\`\`\`json
$poll_json
\`\`\`

执行要求：
1. 派发任何子智能体（\`issue-scout\` / \`rust-worker\` / \`rust-verifier\`）时，必须在子智能体 Prompt 首行写入 \`[AKZIO-LOOP-SUBAGENT]\`（**禁止**把 \`[AKZIO-AUTONOMOUS-LOOP]\` 传给子智能体，以免误触发 Leader 的 \`Stop\` 续跑钩子）。
2. 若 \`recommended_action == "design_inbox"\`：对目标 Issue 执行 Stage 1 设计，核查源码与红线（参考 \`dirty_main_files\`，避免与主工作区未提交修改冲突），通过 \`python3 .agents/loop/bin/issue_hub.py post-design\` 发布 \`[LOOP-DESIGN]\`（或遇到产品决策项通过 \`issue_hub.py block\` 标记 \`loop:blocked\`）。
3. 若 \`recommended_action == "execute_ready"\`：先运行 \`issue_hub.py claim\` 与 \`issue_hub.py worktree-create\` 创建隔离工作区 \`outputs/loop/worktrees/issue-<N>\`（绝不改动主工作区未提交文件），再通过 \`invoke_subagent(TypeName="rust-worker", Model="flash")\` 派发实现；随后运行 \`issue_hub.py verify-worktree\` 确定性反压检查，并派发 \`invoke_subagent(TypeName="rust-verifier", Model="flash")\` 双轴盲审；通过后执行 \`worktree-land\` 与 \`complete --close\` 发布 \`[LOOP-REPORT]\`，连续 2 轮不过则触发熔断 \`worktree-discard\` + \`block\`。
4. 若 \`recommended_action == "scout_needed"\`：唤醒 \`invoke_subagent(TypeName="issue-scout", Model="flash")\` 按巡检指南核对代码库和可用的本地审计台账，排除已有修复并调用 \`issue_hub.py propose\` 提交 1~3 个带去重指纹的新 Issue，随后为首个新 Issue 完成 Stage 1 \`[LOOP-DESIGN]\`。
5. 若 \`daily_report_due == true\`（仅当外部未提前发布时为 true）：在本轮末尾运行 \`python3 .agents/loop/bin/issue_hub.py daily-report --hours 24 --publish-github\`。
EOF
}

# Run each wave in a subshell so its EXIT trap releases the lock before the next wave.
run_single_wave() (
  acquire_wave_lock || return 0

  local ts
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  local wave_log="$LOG_DIR/wave-$ts.log"

  echo "[$ts] Polling GitHub Issues queue..." | tee -a "$wave_log"
  local poll_json
  poll_json="$(python3 "$ISSUE_HUB" poll)" || return
  echo "$poll_json" >>"$wave_log"

  local action
  action="$(printf '%s' "$poll_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("recommended_action",""))')" || return
  local daily_due
  daily_due="$(printf '%s' "$poll_json" | python3 -c 'import sys,json; print(str(json.load(sys.stdin).get("daily_report_due",False)).lower())')" || return

  if [[ "$daily_due" == "true" ]]; then
    echo "[$ts] 24h Daily Report is due; generating and publishing to GitHub Issues..." | tee -a "$wave_log"
    python3 "$ISSUE_HUB" daily-report --hours 24 --publish-github | tee -a "$wave_log" || return
    # Refresh poll snapshot so Leader does not emit a duplicate daily report in the same wave
    poll_json="$(python3 "$ISSUE_HUB" poll)" || return
    echo "$poll_json" >>"$wave_log"
  fi

  local prompt
  prompt="$(build_wave_prompt "$poll_json")" || return

  if [[ ! -x "$AGENTAPI_BIN" ]]; then
    echo "ERROR: agentapi binary not found at $AGENTAPI_BIN" | tee -a "$wave_log" >&2
    return 1
  fi

  echo "[$ts] Launching fresh-context Leader wave (profile=loop-leader, action=$action, model=$LEADER_MODEL)..." | tee -a "$wave_log"
  "$AGENTAPI_BIN" new-conversation \
    --profile="loop-leader" \
    --model="$LEADER_MODEL" \
    --title="Akzio Loop Wave ($ts · $action)" \
    -- "$prompt" | tee -a "$wave_log"
)

cmd="${1:-help}"
shift || true

case "$cmd" in
  init)
    python3 "$ISSUE_HUB" init-labels "$@"
    ;;
  status)
    python3 "$ISSUE_HUB" poll "$@"
    ;;
  wave)
    run_single_wave "$@"
    ;;
  daily-report)
    python3 "$ISSUE_HUB" daily-report --hours 24 --publish-github "$@"
    ;;
  daemon)
    echo "Starting Akzio 24h Unattended Loop Daemon (interval=${WAVE_INTERVAL_SECS}s, leader=${LEADER_MODEL})..."
    while true; do
      if ! run_single_wave; then
        echo "[WARN] Wave exited non-zero; backing off for 120s..." >&2
        sleep 120
      fi
      sleep "$WAVE_INTERVAL_SECS"
    done
    ;;
  *)
    usage
    exit 1
    ;;
esac
