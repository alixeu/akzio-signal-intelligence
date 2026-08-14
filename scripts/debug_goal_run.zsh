#!/usr/bin/env zsh

set -euo pipefail
umask 077

script_dir="${0:A:h}"
repo_root="${script_dir:h}"
cd "${repo_root}"

goal_config_input="${1:-config/akzio.debug-goal.local.toml}"
goal_stage="${2:-real-debug}"
goal_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
goal_bundle="${repo_root}/report/debug-goal/${goal_stamp}-${goal_stage}"
goal_target="$(mktemp -d "${TMPDIR:-/tmp}/akzio-goal-target.XXXXXX")"
goal_store="${goal_bundle}/store"
goal_export="${goal_bundle}/export"
goal_runtime_config="${goal_bundle}/config.toml"
goal_pid=""
goal_run_id=""

mkdir -p "${goal_bundle}/commands"
trap 'if [[ -d "${goal_target}" ]]; then rm -rf -- "${goal_target}"; fi' EXIT

if [[ "${goal_config_input}" = /* ]]; then
  goal_config="${goal_config_input}"
else
  goal_config="${repo_root}/${goal_config_input}"
fi

[[ -f "${goal_config}" ]] || {
  print -u2 -- "debug config does not exist: ${goal_config}"
  exit 2
}

command -v cargo >/dev/null || { print -u2 -- "cargo is required"; exit 2; }
command -v jq >/dev/null || { print -u2 -- "jq is required"; exit 2; }
command -v openssl >/dev/null || { print -u2 -- "openssl is required"; exit 2; }

if [[ -z "${LLM_GATEWAY_BASE_URL:-}" || -z "${LLM_GATEWAY_API_KEY:-}" ]]; then
  print -u2 -- "real Debug requires LLM_GATEWAY_BASE_URL and LLM_GATEWAY_API_KEY"
  exit 2
fi

if [[ -z "${AKZIO_DAEMON_TOKEN:-}" ]]; then
  export AKZIO_DAEMON_TOKEN="$(openssl rand -hex 32)"
fi

write_manifest() {
  local llm_gateway_configured=false
  local alpaca_configured=false
  [[ -n "${LLM_GATEWAY_BASE_URL:-}" && -n "${LLM_GATEWAY_API_KEY:-}" ]] && llm_gateway_configured=true
  [[ -n "${ALPACA_API_KEY:-}" && -n "${ALPACA_API_SECRET:-}" ]] && alpaca_configured=true

  jq -n \
    --arg stage "${goal_stage}" \
    --arg created_at "${goal_stamp}" \
    --arg config "${goal_config}" \
    --arg runtime_config "${goal_runtime_config}" \
    --arg target_dir "${goal_target}" \
    --arg store_root "${goal_store}" \
    --arg commit "$(git rev-parse HEAD)" \
    --arg branch "$(git branch --show-current)" \
    --arg rustc "$(rustc --version)" \
    --arg cargo "$(cargo --version)" \
    --arg model "gpt-5.6-luna" \
    --argjson llm_gateway_configured "${llm_gateway_configured}" \
    --argjson alpaca_configured "${alpaca_configured}" \
    '{stage:$stage,created_at:$created_at,config:$config,runtime_config:$runtime_config,target_dir:$target_dir,store_root:$store_root,commit:$commit,branch:$branch,rustc:$rustc,cargo:$cargo,model:$model,llm_gateway_configured:$llm_gateway_configured,alpaca_configured:$alpaca_configured}' \
    > "${goal_bundle}/manifest.json"
}

write_runtime_config() {
  sed \
    -e "s|^store_root[[:space:]]*=.*$|store_root = \"${goal_store}\"|" \
    "${goal_config}" > "${goal_runtime_config}"
}

cleanup() {
  local exit_code=$?
  set +e
  if [[ -n "${goal_pid}" ]] && kill -0 "${goal_pid}" 2>/dev/null; then
    kill -TERM "${goal_pid}" 2>/dev/null
    wait "${goal_pid}" 2>/dev/null
  fi
  if [[ -d "${goal_target}" ]]; then
    cargo clean --target-dir "${goal_target}" > "${goal_bundle}/commands/cargo-clean.log" 2>&1
    rm -rf -- "${goal_target}"
  fi
  print -r -- "${exit_code}" > "${goal_bundle}/commands/script-exit-code.txt"
  exit "${exit_code}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

write_runtime_config
write_manifest
du -sh target outputs report > "${goal_bundle}/commands/disk-before.txt" 2>&1 || true

CARGO_TARGET_DIR="${goal_target}" cargo build -p akzio-cli --offline \
  > "${goal_bundle}/commands/cargo-build.log" 2>&1

"${goal_target}/debug/akzio" --config "${goal_runtime_config}" daemon serve \
  > "${goal_bundle}/commands/daemon.log" 2>&1 &
goal_pid=$!

ready=false
for attempt in {1..60}; do
  if [[ -n "${goal_pid}" ]] && ! kill -0 "${goal_pid}" 2>/dev/null; then
    print -u2 -- "daemon exited before readiness; see ${goal_bundle}/commands/daemon.log"
    exit 12
  fi
  if "${goal_target}/debug/akzio" --config "${goal_runtime_config}" daemon ready \
    > "${goal_bundle}/commands/ready-${attempt}.json" 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  print -u2 -- "daemon did not become ready; see ${goal_bundle}/commands/daemon.log"
  exit 10
fi

"${goal_target}/debug/akzio" --config "${goal_runtime_config}" run submit debug \
  > "${goal_bundle}/commands/run-submit.json" 2>&1
goal_run_id="$(jq -er '.run_id' "${goal_bundle}/commands/run-submit.json")"

terminal=false
goal_status=""
for attempt in {1..300}; do
  if [[ -n "${goal_pid}" ]] && ! kill -0 "${goal_pid}" 2>/dev/null; then
    print -u2 -- "daemon exited while waiting for run ${goal_run_id}; see ${goal_bundle}/commands/daemon.log"
    exit 12
  fi
  if "${goal_target}/debug/akzio" --config "${goal_runtime_config}" run replay "${goal_run_id}" \
    > "${goal_bundle}/commands/replay.json" 2>&1; then
    goal_status="$(jq -r '.status // empty' "${goal_bundle}/commands/replay.json" 2>/dev/null || true)"
    case "${goal_status:l}" in
      completed|completed_with_execution_rejection|failed|cancelled)
        terminal=true
        break
        ;;
    esac
  fi
  sleep 1
done
if [[ "${terminal}" != true ]]; then
  print -u2 -- "run did not reach a terminal status within 300 seconds"
  exit 11
fi

"${goal_target}/debug/akzio" --config "${goal_runtime_config}" run trajectory "${goal_run_id}" \
  > "${goal_bundle}/commands/trajectory.json" 2>&1
"${goal_target}/debug/akzio" --config "${goal_runtime_config}" run retrospectives "${goal_run_id}" \
  > "${goal_bundle}/commands/retrospectives.json" 2>&1
"${goal_target}/debug/akzio" --config "${goal_runtime_config}" store metrics \
  > "${goal_bundle}/commands/store-metrics.json" 2>&1
"${goal_target}/debug/akzio" --config "${goal_runtime_config}" store alerts \
  > "${goal_bundle}/commands/store-alerts.json" 2>&1
"${goal_target}/debug/akzio" --config "${goal_runtime_config}" store doctor \
  > "${goal_bundle}/commands/store-doctor.json" 2>&1
"${goal_target}/debug/akzio" --config "${goal_runtime_config}" store export-run "${goal_run_id}" \
  "${goal_export}" --include-raw-model \
  > "${goal_bundle}/commands/export-run.json" 2>&1

jq -n \
  --arg run_id "${goal_run_id}" \
  --arg status "${goal_status}" \
  --arg bundle "${goal_bundle}" \
  '{run_id:$run_id,status:$status,bundle:$bundle,terminal:true,raw_model_export:true}' \
  > "${goal_bundle}/summary.json"

du -sh target outputs report > "${goal_bundle}/commands/disk-after.txt" 2>&1 || true
print -r -- "${goal_run_id}"

if [[ "${goal_status:l}" != completed ]]; then
  print -u2 -- "real Debug did not complete successfully: ${goal_status}"
  exit 20
fi
