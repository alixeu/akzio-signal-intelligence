#!/usr/bin/env bash
# 两个正式运行触发。只提交一次，冷启动、Gate 和 Outcome 均由 Core 处理。
# 不依赖 Python/curl/jq；缺 Core 时由 Rust CLI 启动并保持运行，不修改配置、Policy 或审批。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CALLER_DIR="$PWD"
CONFIG="${HOME:?HOME is required}/.akzio/config.toml"

usage() {
  cat <<'HELP'
用法：bash scripts/run_core.sh [--config FILE] position-plan|paper

  position-plan  真实 LLM 生成仓位计划，不提交订单。
  paper          真实 LLM 生成计划，通过审批与 Gate 后提交 Alpaca Paper 订单。

已有正式 Core 时复用；未运行则自动启动并等待 ready。
默认使用 ~/.akzio/config.toml、store/ 与 logs/core-*.log。
冷启动由 Core 自动判断：缺 Policy 时保留研究计划，正式目标为零，Paper 为 NoOrder。
两种模式都包含 T1/T3/T5 预测；仅 Paper 在流程完成后安排后续跨交易日 Outcome。
只触发一次并返回 Run ID。若 Core 由脚本启动，将持续后台运行以推进 Outcome。
脚本启动的 Core 用 Rust CLI 的 run replay/detail 查询，不重复启动第二个 Core。
无参数或 --help 仅显示帮助。提交结果不确定时先查询原 Run，勿直接重复执行。
HELP
}

die() { printf 'error: %s\n' "$*" >&2; exit 2; }

if [[ "${1:-}" == "--config" ]]; then
  [[ -n "${2:-}" ]] || die "--config 需要路径"
  CONFIG="$2"
  shift 2
fi

MODE="${1:-help}"
[[ $# -le 1 ]] || die "只接受 position-plan 或 paper；--config 放在模式前"
case "$MODE" in
  help|--help|-h) usage; exit 0 ;;
  position-plan|paper) ;;
  *) die "未知模式：${MODE}；使用 help 查看全部模式" ;;
esac

[[ "$CONFIG" = /* ]] || CONFIG="$CALLER_DIR/$CONFIG"
cd "$ROOT"
# 使用当前源码的薄客户端；与正式 HTTP 客户端调用同一个 POST /runs，绝不自动重试提交。
exec cargo run --locked --quiet -p akzio-cli -- --config "$CONFIG" run start --purpose "$MODE" --auto-serve
