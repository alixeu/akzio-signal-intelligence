#!/usr/bin/env bash

# Build and sign the distributable Observatory app, preserving build products.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 所有路径都从脚本位置派生；调用者的当前目录不会改变构建产物或签名目标。
APP_ROOT="$ROOT/apps"
BUILD_SCRIPT="$APP_ROOT/Scripts/build_app.sh"
SIGN_SCRIPT="$APP_ROOT/Scripts/sign_app.sh"
APP_BUNDLE="${AKZIO_APP_BUNDLE:-$APP_ROOT/dist/akzio.app}"

echo "==> rebuilding Observatory app"
# build_app 自身会拒绝覆盖既有 bundle；因此这里不会在旧包上增量拼接文件。
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
# 两个可执行文件和外层 bundle 都必须存在后才进入签名阶段。
"$SIGN_SCRIPT" "$APP_BUNDLE"

echo "packaged: $APP_BUNDLE"
