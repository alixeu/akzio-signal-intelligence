#!/usr/bin/env bash

# 文件职责：编译当前 checkout 的 Swift Observatory 和 release Rust Core，组装并签名新的 App。
# 不启动 App/Core、不提交 Paper 请求；默认使用唯一 Bundle 路径，绝不覆盖既有 Bundle。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="$ROOT/apps"
BUILD_SCRIPT="$APP_ROOT/Scripts/build_app.sh"
SIGN_SCRIPT="$APP_ROOT/Scripts/sign_app.sh"

if [[ $# -ne 0 ]]; then
  if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
    echo "usage: $0"
    echo "Build and ad-hoc sign a new App with the current release Rust Core."
    echo "Set AKZIO_APP_BUNDLE to choose an unused output path; existing Bundles are never overwritten."
    exit 0
  fi
  echo "error: unknown arguments (use --help)" >&2
  exit 2
fi

# 时间戳和 PID 只用于为这次构建选择新路径，不表示业务 Run 或应用版本。
# 显式 AKZIO_APP_BUNDLE 保持调用者选择；相对路径以调用时的工作目录为基准。
APP_BUNDLE="${AKZIO_APP_BUNDLE:-$APP_ROOT/dist/akzio-$(date -u +%Y%m%dT%H%M%SZ)-$$.app}"
[[ "$APP_BUNDLE" = /* ]] || APP_BUNDLE="$PWD/$APP_BUNDLE"
export AKZIO_APP_BUNDLE="$APP_BUNDLE"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
[[ "$TARGET_DIR" = /* ]] || TARGET_DIR="$ROOT/$TARGET_DIR"
RUST_BINARY="$TARGET_DIR/release/akzio"

echo "==> building current Observatory and release Rust Core"
# build_app 会先拒绝已有目标，再编译 Swift/Cargo 并组装 Bundle。
"$BUILD_SCRIPT"

if [[ ! -x "$APP_BUNDLE/Contents/MacOS/AkzioObservatory" ]]; then
  echo "error: Observatory executable is missing: $APP_BUNDLE" >&2
  exit 1
fi
if [[ ! -x "$APP_BUNDLE/Contents/MacOS/akzio-core" ]]; then
  echo "error: embedded Rust Core is missing: $APP_BUNDLE" >&2
  exit 1
fi
if [[ ! -x "$RUST_BINARY" ]]; then
  echo "error: release Rust binary is missing: $RUST_BINARY" >&2
  exit 1
fi

echo "==> applying ad-hoc self-use signature"
"$SIGN_SCRIPT" "$APP_BUNDLE"

printf 'APP_BUNDLE=%s\nRUST_BINARY=%s\nEMBEDDED_CORE=%s\n' \
  "$APP_BUNDLE" "$RUST_BINARY" "$APP_BUNDLE/Contents/MacOS/akzio-core"
