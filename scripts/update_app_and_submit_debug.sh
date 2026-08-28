#!/usr/bin/env bash

# Rebuild the distributable Observatory app, remove repository build products,
# then submit one Debug run using an isolated temporary Cargo target.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="$ROOT/apps/AkzioMac"
BUILD_SCRIPT="$APP_ROOT/Scripts/build_app.sh"
SIGN_SCRIPT="$APP_ROOT/Scripts/sign_app.sh"
APP_BUNDLE="$APP_ROOT/dist/Akzio Observatory.app"
RUN_TARGET="$(mktemp -d "${TMPDIR:-/tmp}/akzio-cli-submit-target.XXXXXX")"

cleanup() {
  local exit_status=$?
  trap - EXIT
  if [[ -d "$RUN_TARGET" ]]; then
    rm -rf -- "$RUN_TARGET"
  fi
  exit "$exit_status"
}
trap cleanup EXIT

echo "==> rebuilding Observatory app"
"$BUILD_SCRIPT"

if [[ ! -x "$APP_BUNDLE/Contents/MacOS/AkzioObservatory" ]]; then
  echo "error: rebuilt Observatory executable is missing: $APP_BUNDLE" >&2
  exit 1
fi
if [[ ! -x "$APP_BUNDLE/Contents/MacOS/akzio-core" ]]; then
  echo "error: rebuilt Rust core is missing: $APP_BUNDLE" >&2
  exit 1
fi

echo "==> applying ad-hoc self-use signature"
"$SIGN_SCRIPT" "$APP_BUNDLE"

echo "==> removing repository build intermediates"
for intermediate in \
  "$ROOT/target" \
  "$APP_ROOT/.build" \
  "$APP_ROOT/dist/dmg-stage"
do
  if [[ -e "$intermediate" || -L "$intermediate" ]]; then
    echo "remove: $intermediate"
    rm -rf -- "$intermediate"
  fi
done

echo "==> submitting Debug run"
# cargo run compiles the CLI itself. Keep that unavoidable compilation in a
# temporary target so the repository remains clean after this command exits.
CARGO_TARGET_DIR="$RUN_TARGET" cargo run -p akzio-cli -- run submit debug
