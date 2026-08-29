#!/usr/bin/env bash

# Rebuild the distributable Observatory app and remove repository build products.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="$ROOT/apps/AkzioMac"
BUILD_SCRIPT="$APP_ROOT/Scripts/build_app.sh"
SIGN_SCRIPT="$APP_ROOT/Scripts/sign_app.sh"
APP_BUNDLE="$APP_ROOT/dist/Akzio Observatory.app"

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

echo "packaged: $APP_BUNDLE"
