#!/usr/bin/env bash
# 文件职责：在 SwiftPM 与 Cargo 都成功后组装新的 macOS .app bundle。
# 它拒绝覆盖已有 bundle，复制 Rust CLI 作为 Core，并生成版本化 Info.plist；不启动业务。
# scripts/build_app_and_core.sh 会先调用本脚本再验收可执行文件并签名；手动调用也会写入新的 apps/dist 或指定目标。
# Swift/Cargo 构建产物留在各自构建目录，只有编译均成功后才开始创建 Bundle；本脚本自身不做 codesign/notarization。
# Assemble a double-clickable .app from the SwiftPM build product.
# No Xcode required: pure `swift build` + bundle layout.
# 严格模式拒绝未定义变量，并让未被条件结构接住的失败命令/管道传播退出；脚本没有回滚 trap，Bundle 创建后失败可能留下部分目录。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 先把脚本位置解析为 apps 根目录，再固定仓库根目录；后续相对路径不依赖调用者的当前目录。
# `ROOT` is `<repo>/apps`; one parent is the repository root.
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
CONFIG="${CONFIG:-release}"
APP_NAME="akzio"
VERSION="${VERSION:-1.0.0}"
BUILD_NUMBER="${BUILD_NUMBER:-1}"
DIST="$ROOT/dist"
BUNDLE="${AKZIO_APP_BUNDLE:-$DIST/$APP_NAME.app}"
[[ "$BUNDLE" = /* ]] || BUNDLE="$PWD/$BUNDLE"
# 构建脚本不会覆盖已有 bundle；冲突直接失败，避免把旧包和本次构建结果混在一起。
if [[ -e "$BUNDLE" || -L "$BUNDLE" ]]; then
    echo "error: bundle already exists; choose a new AKZIO_APP_BUNDLE destination: $BUNDLE" >&2
    exit 1
fi

# Resolve relative Cargo target paths from the same directory for build and copy.
cd "$REPO_ROOT"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"

# Swift 产物和 Rust core 必须都成功生成，任一步失败都会因 set -e 终止打包。
echo "==> swift build ($CONFIG)"
swift build --package-path "$ROOT" -c "$CONFIG" --product AkzioObservatory
BIN_PATH="$(swift build --package-path "$ROOT" -c "$CONFIG" --show-bin-path)"

echo "==> cargo build (release)"
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --locked --release -p akzio-cli

echo "==> laying out bundle"
# 只在两个独立编译步骤成功后创建 bundle 目录，并复制实际可执行文件。
# 若此后的复制、权限设置或 plist 写入失败，已创建内容会保留；下次运行会按前面的防覆盖检查要求换新目标。
mkdir -p "$BUNDLE/Contents/MacOS" "$BUNDLE/Contents/Resources"
cp "$BIN_PATH/AkzioObservatory" "$BUNDLE/Contents/MacOS/AkzioObservatory"
cp "${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release/akzio" "$BUNDLE/Contents/MacOS/akzio-core"
cp "$REPO_ROOT/config/akzio.observatory.toml" \
    "$BUNDLE/Contents/Resources/akzio.observatory.toml"
chmod 755 "$BUNDLE/Contents/MacOS/AkzioObservatory" "$BUNDLE/Contents/MacOS/akzio-core"

sed -e "s/__VERSION__/$VERSION/" -e "s/__BUILD__/$BUILD_NUMBER/" \
    "$ROOT/Resources/Info.plist.in" > "$BUNDLE/Contents/Info.plist"
printf 'APPL????' > "$BUNDLE/Contents/PkgInfo"

# SwiftPM 资源 bundle 和图标都是可选输入；存在才复制，不会伪造缺失资源。
shopt -s nullglob
for resource_bundle in "$BIN_PATH"/*.bundle; do
    cp -R "$resource_bundle" "$BUNDLE/Contents/Resources/"
done
shopt -u nullglob
if [ -f "$ROOT/Resources/AppIcon.icns" ]; then
    cp "$ROOT/Resources/AppIcon.icns" "$BUNDLE/Contents/Resources/AppIcon.icns"
fi
if [ -d "$ROOT/Resources/AppIcon.icon" ]; then
    cp -R "$ROOT/Resources/AppIcon.icon" "$BUNDLE/Contents/Resources/AppIcon.icon"
fi

echo "built: $BUNDLE"
