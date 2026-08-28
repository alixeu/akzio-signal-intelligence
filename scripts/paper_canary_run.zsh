#!/usr/bin/env zsh
set -euo pipefail
umask 077

script_dir="${0:A:h}"
repo_root="${script_dir:h}"
cd "${repo_root}"

config_input="${1:-config/akzio.local.toml}"
if [[ "${config_input}" = /* ]]; then
  config="${config_input}"
else
  config="${repo_root}/${config_input}"
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
bundle="${repo_root}/report/debug-goal/${stamp}-paper-canary"
target_dir="$(mktemp -d "${TMPDIR:-/tmp}/akzio-paper-target.XXXXXX")"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/akzio-paper-preflight.XXXXXX")"
store_root="${bundle}/store"
runtime_config="${bundle}/config.toml"
export_root="${bundle}/export"
daemon_pid=""
run_id=""
session_key=""
summary_status="failed"
eligible=false
scheduler_epoch=""
commitment_id=""
committed_at=""
run_status=""
run_purpose=""
task_count=""
terminal_task_count=""
paper_kinds=false
real_model=false
collect_failed=false
order_receipts=0
export_checked=false
export_raw_model=false
secret_scan_clean=false

mkdir -p "${bundle}/commands"

write_summary() {
  jq -n \
    --arg status "${summary_status}" \
    --arg bundle "${bundle}" \
    --arg run_id "${run_id}" \
    --arg session_key "${session_key}" \
    --arg scheduler_epoch "${scheduler_epoch}" \
    --arg commitment_id "${commitment_id}" \
    --arg committed_at "${committed_at}" \
    --arg run_status "${run_status}" \
    --arg run_purpose "${run_purpose}" \
    --arg task_count "${task_count}" \
    --arg terminal_task_count "${terminal_task_count}" \
    --argjson eligible "${eligible}" \
    --argjson real_model "${real_model}" \
    --argjson order_receipts "${order_receipts}" \
    --argjson export_checked "${export_checked}" \
    --argjson export_raw_model "${export_raw_model}" \
    --argjson secret_scan_clean "${secret_scan_clean}" \
    --argjson paper_kinds "${paper_kinds}" \
    --argjson collect_failed "${collect_failed}" \
    '{status:$status,bundle:$bundle,run_id:(if $run_id == "" then null else $run_id end),session_key:(if $session_key == "" then null else $session_key end),scheduler_epoch:(if $scheduler_epoch == "" then null else ($scheduler_epoch|tonumber) end),commitment_artifact_id:(if $commitment_id == "" then null else $commitment_id end),committed_at:(if $committed_at == "" then null else $committed_at end),run_status:(if $run_status == "" then null else $run_status end),run_purpose:(if $run_purpose == "" then null else $run_purpose end),task_count:(if $task_count == "" then null else ($task_count|tonumber) end),terminal_task_count:(if $terminal_task_count == "" then null else ($terminal_task_count|tonumber) end),eligible:$eligible,real_model:$real_model,order_receipts:$order_receipts,export_checked:$export_checked,export_raw_model:$export_raw_model,secret_scan_clean:$secret_scan_clean,paper_kinds:$paper_kinds,collect_failed:$collect_failed,raw_model_export:false}' \
    > "${bundle}/summary.json"
}

cleanup() {
  local original_exit=$?
  set +e
  local cargo_clean_exit=0
  local rm_exit=0
  local target_exists=false
  local cleanup_ok=false
  local final_exit=${original_exit}

  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill -TERM "${daemon_pid}" 2>/dev/null
    wait "${daemon_pid}" 2>/dev/null
  fi

  if [[ -d "${target_dir}" ]]; then
    CARGO_TARGET_DIR="${target_dir}" cargo clean --target-dir "${target_dir}" \
      > "${bundle}/commands/cargo-clean.log" 2>&1
    cargo_clean_exit=$?
    rm -rf -- "${target_dir}"
    rm_exit=$?
  fi
  [[ -d "${target_dir}" ]] && target_exists=true

  {
    for disk_path in target outputs report; do
      if [[ -e "${disk_path}" ]]; then
        du -sh "${disk_path}"
      fi
    done
  } > "${bundle}/commands/disk-after.txt" 2>&1

  if (( cargo_clean_exit == 0 && rm_exit == 0 )) && [[ "${target_exists}" == false ]]; then
    cleanup_ok=true
  else
    final_exit=21
  fi

  jq -n \
    --arg target_dir "${target_dir}" \
    --argjson original_exit "${original_exit}" \
    --argjson cargo_clean_exit "${cargo_clean_exit}" \
    --argjson rm_exit "${rm_exit}" \
    --argjson target_exists "${target_exists}" \
    --argjson cleanup_ok "${cleanup_ok}" \
    '{target_dir:$target_dir,original_exit:$original_exit,cargo_clean_exit:$cargo_clean_exit,rm_exit:$rm_exit,target_exists:$target_exists,cleanup_ok:$cleanup_ok}' \
    > "${bundle}/commands/cleanup.json"

  rm -rf -- "${tmp_dir}"
  if [[ ! -s "${bundle}/summary.json" ]] || ! jq -e . "${bundle}/summary.json" >/dev/null 2>&1; then
    write_summary
  fi
  print -r -- "${final_exit}" > "${bundle}/commands/script-exit-code.txt"
  exit "${final_exit}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[[ -f "${config}" ]] || { print -u2 -- "missing config: ${config}"; exit 2; }
for command_name in cargo curl jq openssl rg; do
  command -v "${command_name}" >/dev/null || { print -u2 -- "missing command: ${command_name}"; exit 2; }
done

[[ -n "${LLM_GATEWAY_BASE_URL:-}" && -n "${LLM_GATEWAY_API_KEY:-}" ]] || {
  print -u2 -- "real Paper canary requires LLM_GATEWAY_BASE_URL and LLM_GATEWAY_API_KEY"
  exit 2
}
[[ -n "${ALPACA_API_KEY:-}" && -n "${ALPACA_API_SECRET:-}" ]] || {
  print -u2 -- "Paper canary requires ALPACA_API_KEY and ALPACA_API_SECRET"
  exit 2
}
[[ -n "${FRED_API_KEY:-}" ]] || {
  print -u2 -- "Paper canary requires FRED_API_KEY"
  exit 2
}
[[ "${ALPACA_PAPER_BASE_URL:-https://paper-api.alpaca.markets}" == "https://paper-api.alpaca.markets" ]] || {
  print -u2 -- "Paper canary rejects non-Paper ALPACA_PAPER_BASE_URL"
  exit 2
}
if [[ -z "${AKZIO_DAEMON_TOKEN:-}" ]]; then
  export AKZIO_DAEMON_TOKEN="$(openssl rand -hex 32)"
fi

rg -q '^[[:space:]]*auto_paper[[:space:]]*=[[:space:]]*true' "${config}" || {
  print -u2 -- "Paper canary requires auto_paper=true"
  exit 2
}
rg -q '^[[:space:]]*assets[[:space:]]*=.*TQQQ.*QQQ.*SOXX.*SOXL' "${config}" || {
  print -u2 -- "Paper canary requires the four-asset universe"
  exit 2
}

market_data_feed="$(sed -nE 's/^[[:space:]]*market_data_feed[[:space:]]*=[[:space:]]*"(iex|sip)"[[:space:]]*$/\1/p' "${config}" | head -n 1)"
[[ -n "${market_data_feed}" ]] || {
  print -u2 -- "Paper canary requires market_data_feed=iex or sip"
  exit 2
}

git rev-parse HEAD > "${bundle}/commands/commit.txt"
git status --short --untracked-files=all > "${bundle}/commands/git-status.txt"
{
  print -- "mode=paper-canary"
  print -- "model=gpt-5.6-luna"
  print -- "paper_endpoint=https://paper-api.alpaca.markets"
  print -- "market_data_endpoint=https://data.alpaca.markets"
  print -- "market_data_feed=${market_data_feed}"
  print -- "raw_model_export=false"
} > "${bundle}/manifest.txt"
jq -n \
  --arg commit "$(git rev-parse HEAD)" \
  --arg config "${config}" \
  --arg model "gpt-5.6-luna" \
  --arg paper_endpoint "https://paper-api.alpaca.markets" \
  --arg market_data_endpoint "https://data.alpaca.markets" \
  --arg market_data_feed "${market_data_feed}" \
  '{mode:"paper-canary",commit:$commit,config:$config,model:$model,paper_endpoint:$paper_endpoint,market_data_endpoint:$market_data_endpoint,market_data_feed:$market_data_feed,assets:["TQQQ","QQQ","SOXX","SOXL"],raw_model_export:false}' \
  > "${bundle}/manifest.json"

{
  for disk_path in target outputs report; do
    if [[ -e "${disk_path}" ]]; then
      du -sh "${disk_path}"
    fi
  done
} > "${bundle}/commands/disk-before.txt" 2>&1

alpaca_get() {
  local name="$1"
  local url="$2"
  local output="${tmp_dir}/${name}.json"
  local attempt
  for attempt in {1..6}; do
    : > "${output}"
    if {
      print -r -- "header = \"APCA-API-KEY-ID: ${ALPACA_API_KEY}\""
      print -r -- "header = \"APCA-API-SECRET-KEY: ${ALPACA_API_SECRET}\""
    } | curl --ipv4 --http1.1 --silent --show-error --fail-with-body \
      --config - --connect-timeout 15 --max-time 45 \
      "${url}" > "${output}" 2>> "${bundle}/commands/preflight-${name}.stderr" \
      && jq -e . "${output}" >/dev/null; then
      print -r -- "${output}"
      return 0
    fi
    sleep "${attempt}"
  done
  cp "${output}" "${bundle}/commands/preflight-${name}.error.json" 2>/dev/null || true
  return 1
}

account_file="$(alpaca_get account https://paper-api.alpaca.markets/v2/account)" || {
  summary_status=preflight_error
  write_summary
  exit 10
}
clock_file="$(alpaca_get clock https://paper-api.alpaca.markets/v2/clock)" || {
  summary_status=preflight_error
  write_summary
  exit 10
}
quotes_file="$(alpaca_get quotes "https://data.alpaca.markets/v2/stocks/quotes/latest?symbols=TQQQ%2CQQQ%2CSOXX%2CSOXL&feed=${market_data_feed}")" || {
  summary_status=preflight_error
  write_summary
  exit 10
}
orders_file="$(alpaca_get open-orders 'https://paper-api.alpaca.markets/v2/orders?status=open&limit=500&direction=desc')" || {
  summary_status=preflight_error
  write_summary
  exit 10
}

jq '{status,account_blocked,trading_blocked}' "${account_file}" \
  > "${bundle}/commands/preflight-account.json"
jq '{is_open,timestamp,next_open,next_close}' "${clock_file}" \
  > "${bundle}/commands/preflight-clock.json"
jq '{quotes:(.quotes // {} | with_entries(.value |= {ap,as,bp,bs,t}))}' "${quotes_file}" \
  > "${bundle}/commands/preflight-quotes.json"
jq '[.[] | select(.symbol == "TQQQ" or .symbol == "QQQ" or .symbol == "SOXX" or .symbol == "SOXL") | {id,client_order_id,symbol,side,status,qty,filled_qty,created_at,updated_at}]' "${orders_file}" \
  > "${bundle}/commands/preflight-open-orders.json"

account_ok="$(jq -r '(.status == "ACTIVE" and .account_blocked == false and .trading_blocked == false)' "${account_file}")"
clock_open="$(jq -r '.is_open // false' "${clock_file}")"
quotes_executable=true
for asset in TQQQ QQQ SOXX SOXL; do
  if ! jq -e --arg asset "${asset}" \
    '(.quotes[$asset] != null and (.quotes[$asset].ap // 0) > 0 and (.quotes[$asset].as // 0) > 0 and (.quotes[$asset].bp // 0) > 0 and (.quotes[$asset].bs // 0) > 0)' \
    "${quotes_file}" >/dev/null; then
    quotes_executable=false
  fi
done

open_order_count="$(jq 'length' "${bundle}/commands/preflight-open-orders.json")"
jq -n \
  --arg account_status "$(jq -r '.status // "unknown"' "${account_file}")" \
  --argjson account_ok "${account_ok}" \
  --argjson is_open "${clock_open}" \
  --argjson quotes_executable "${quotes_executable}" \
  --argjson open_order_count "${open_order_count}" \
  '{account_status:$account_status,account_ok:$account_ok,is_open:$is_open,quotes_executable:$quotes_executable,open_order_count:$open_order_count,orders_submitted:0,live_trading:false}' \
  > "${bundle}/commands/preflight-summary.json"

if [[ "${clock_open}" != true ]]; then
  summary_status=market_closed
  write_summary
  exit 0
fi
if [[ "${account_ok}" != true ]]; then
  summary_status=account_blocked
  write_summary
  exit 20
fi
if [[ "${quotes_executable}" != true ]]; then
  summary_status=quotes_not_executable
  write_summary
  exit 0
fi

eligible=true
session_key="$(jq -r '.timestamp[0:10] // empty' "${clock_file}")"
if [[ ! "${session_key}" =~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' ]]; then
  summary_status=invalid_broker_session_date
  write_summary
  exit 2
fi
goal_port=$((7344 + (RANDOM % 500)))
sed \
  -e "s|^[[:space:]]*store_root[[:space:]]*=.*$|store_root = \"${store_root}\"|" \
  -e "s|^[[:space:]]*http_addr[[:space:]]*=.*$|http_addr = \"127.0.0.1:${goal_port}\"|" \
  "${config}" > "${runtime_config}"

if ! rg -q '^[[:space:]]*auto_paper[[:space:]]*=[[:space:]]*true' "${runtime_config}"; then
  summary_status=runtime_config_invalid
  write_summary
  exit 2
fi

{
  print -- "session_key=${session_key}"
  print -- "http_addr=127.0.0.1:${goal_port}"
  print -- "store_root=${store_root}"
  print -- "raw_model_export=false"
} > "${bundle}/commands/runtime-inputs.txt"

if ! CARGO_TARGET_DIR="${target_dir}" cargo build --offline -p akzio-cli \
  > "${bundle}/commands/cargo-build.log" 2>&1; then
  summary_status=build_failed
  write_summary
  exit 11
fi
binary="${target_dir}/debug/akzio"

paper_approver="${AKZIO_PAPER_APPROVER:-${USER:-local-operator}}"
paper_max_notional_usd_cents="${AKZIO_PAPER_MAX_NOTIONAL_USD_CENTS:-100000}"

"${binary}" --config "${runtime_config}" daemon serve \
  > "${bundle}/commands/daemon.log" 2>&1 &
daemon_pid=$!

ready=false
for attempt in {1..60}; do
  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    summary_status=daemon_exited
    write_summary
    exit 12
  fi
  if "${binary}" --config "${runtime_config}" daemon ready \
    > "${bundle}/commands/ready-${attempt}.json" 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  summary_status=daemon_not_ready
  write_summary
  exit 13
fi
"${binary}" --config "${runtime_config}" daemon health \
  > "${bundle}/commands/health-ready.json" 2>&1 || true

if ! "${binary}" --config "${runtime_config}" store approve-paper "${session_key}" \
  --operator "${paper_approver}" \
  --reason "one-session scheduler-owned Paper canary" \
  --max-notional-usd-cents "${paper_max_notional_usd_cents}" \
  --valid-hours 8 \
  > "${bundle}/commands/paper-approval.json" 2> "${bundle}/commands/paper-approval.stderr"; then
  summary_status=paper_approval_failed
  write_summary
  exit 12
fi

session_found=false
for attempt in {1..180}; do
  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    summary_status=daemon_exited_waiting_session
    write_summary
    exit 14
  fi
  session_file="${bundle}/commands/session-${attempt}.json"
  "${binary}" --config "${runtime_config}" store paper-session "${session_key}" \
    > "${session_file}" 2>&1 || true
  if jq -e '.workflow.run_id' "${session_file}" >/dev/null 2>&1; then
    cp "${session_file}" "${bundle}/commands/session.json"
    run_id="$(jq -r '.workflow.run_id' "${session_file}")"
    scheduler_epoch="$(jq -r '.scheduler_epoch // empty' "${session_file}")"
    session_found=true
    break
  fi
  sleep 2
done
if [[ "${session_found}" != true ]]; then
  summary_status=session_not_reserved
  write_summary
  exit 15
fi

terminal=false
run_status=""
for attempt in {1..180}; do
  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    summary_status=daemon_exited_waiting_run
    write_summary
    exit 16
  fi
  replay_file="${bundle}/commands/replay-${attempt}.json"
  "${binary}" --config "${runtime_config}" run replay "${run_id}" \
    > "${replay_file}" 2>&1 || true
  if jq -e . "${replay_file}" >/dev/null 2>&1; then
    jq -c . "${replay_file}" >> "${bundle}/commands/replay-history.jsonl"
    run_status="$(jq -r '.status // empty' "${replay_file}")"
    run_purpose="$(jq -r '.purpose // empty' "${replay_file}")"
    task_count="$(jq -r '.task_count // empty' "${replay_file}")"
    terminal_task_count="$(jq -r '.terminal_task_count // empty' "${replay_file}")"
    cp "${replay_file}" "${bundle}/commands/replay.json"
    case "${run_status}" in
      completed|completed_with_execution_rejection|failed|cancelled)
        terminal=true
        break
        ;;
    esac
  fi
  sleep 2
done
if [[ "${terminal}" != true ]]; then
  summary_status=run_timeout
  write_summary
  exit 17
fi

"${binary}" --config "${runtime_config}" store paper-session "${session_key}" \
  > "${bundle}/commands/session-final.json" 2>&1 || true
if jq -e '.workflow.run_id' "${bundle}/commands/session-final.json" >/dev/null 2>&1; then
  cp "${bundle}/commands/session-final.json" "${bundle}/commands/session.json"
  scheduler_epoch="$(jq -r '.scheduler_epoch // empty' "${bundle}/commands/session-final.json")"
fi

collect_failed=false
if ! "${binary}" --config "${runtime_config}" run trajectory "${run_id}" \
  > "${bundle}/commands/trajectory.json" 2>&1; then collect_failed=true; fi
if ! "${binary}" --config "${runtime_config}" run retrospectives "${run_id}" \
  > "${bundle}/commands/retrospectives.json" 2>&1; then collect_failed=true; fi
if ! "${binary}" --config "${runtime_config}" store metrics \
  > "${bundle}/commands/store-metrics.json" 2>&1; then collect_failed=true; fi
if ! "${binary}" --config "${runtime_config}" store alerts \
  > "${bundle}/commands/store-alerts.json" 2>&1; then collect_failed=true; fi
if ! "${binary}" --config "${runtime_config}" store doctor \
  > "${bundle}/commands/store-doctor.json" 2>&1; then collect_failed=true; fi
if ! "${binary}" --config "${runtime_config}" store export-run "${run_id}" "${export_root}" \
  > "${bundle}/commands/export-run.json" 2>&1; then collect_failed=true; fi

if [[ -f "${export_root}/akzio-export.sqlite3" ]]; then
  export_checked=true
  export_raw_model="$(jq -r '.include_raw_model // true' "${bundle}/commands/export-run.json" 2>/dev/null || print true)"
  order_receipts="$(jq '[.artifacts[]? | select(.artifact.kind == "order_receipt")] | length' "${bundle}/commands/export-run.json" 2>/dev/null || print 0)"
else
  collect_failed=true
fi

print -r -- "${LLM_GATEWAY_API_KEY:-}" > "${tmp_dir}/secret-patterns.txt"
print -r -- "${ALPACA_API_KEY:-}" >> "${tmp_dir}/secret-patterns.txt"
print -r -- "${ALPACA_API_SECRET:-}" >> "${tmp_dir}/secret-patterns.txt"
print -r -- "${AKZIO_DAEMON_TOKEN:-}" >> "${tmp_dir}/secret-patterns.txt"
secret_scan_clean=true
if rg -n -F -f "${tmp_dir}/secret-patterns.txt" "${bundle}" >/dev/null 2>&1; then
  secret_scan_clean=false
fi
jq -n \
  --argjson export_raw_model "${export_raw_model}" \
  --argjson order_receipts "${order_receipts}" \
  --argjson secret_scan_clean "${secret_scan_clean}" \
  '{include_raw_model:$export_raw_model,order_receipts:$order_receipts,secret_scan_clean:$secret_scan_clean}' \
  > "${bundle}/commands/validation.json"

if jq -e '[.[] | .artifact_kind // empty] | index("execution_commitment") != null and index("reconciliation") != null' \
  "${bundle}/commands/trajectory.json" >/dev/null 2>&1; then
  paper_kinds=true
fi
if jq -e 'any(.[]; .event_type == "agent.turn_completed" and .model.model_id == "gpt-5.6-luna" and .model.provider_id == "responses")' \
  "${bundle}/commands/trajectory.json" >/dev/null 2>&1; then
  real_model=true
fi
commitment_id="$(jq -r '.commitment_artifact_id // empty' "${bundle}/commands/session.json" 2>/dev/null || true)"
committed_at="$(jq -r '.committed_at // empty' "${bundle}/commands/session.json" 2>/dev/null || true)"
if [[ "${run_status}" == completed && "${run_purpose}" == paper && -n "${task_count}" && "${task_count}" == "${terminal_task_count}" && -n "${commitment_id}" && -n "${committed_at}" && "${scheduler_epoch}" != "" && "${paper_kinds}" == true && "${real_model}" == true && "${order_receipts}" -gt 0 && "${export_checked}" == true && "${export_raw_model}" == false && "${secret_scan_clean}" == true && "${collect_failed}" == false ]]; then
  summary_status=completed
else
  summary_status=completed_with_validation_failure
fi
write_summary

if [[ "${summary_status}" != completed ]]; then
  exit 30
fi
