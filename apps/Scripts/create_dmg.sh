#!/usr/bin/env bash
# Package the signed bundle into a self-use .dmg via hdiutil.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 所有输出默认落在 apps/dist；环境变量只覆盖目标，不改变签名和打包顺序。
APP_NAME="akzio"
VERSION="${VERSION:-1.0.0}"
DIST="$ROOT/dist"
BUNDLE="${AKZIO_APP_BUNDLE:-$DIST/$APP_NAME.app}"
DMG="${AKZIO_DMG_PATH:-$DIST/akzio-$VERSION.dmg}"

if [[ -e "$DMG" || -L "$DMG" ]]; then
    # 不覆盖既有磁盘映像，调用者需要显式选择新的目标路径。
    echo "error: disk image already exists; choose a new AKZIO_DMG_PATH: $DMG" >&2
    exit 1
fi

if [ ! -d "$BUNDLE" ]; then
    # DMG 必须从已组装的 bundle 产生，缺包时不继续创建半成品。
    echo "error: bundle not found: $BUNDLE (run build_app.sh first)" >&2
    exit 1
fi

# 先对将被复制的原始 bundle 签名并验证，保证 DMG 中的内容就是已检查的版本。
"$ROOT/Scripts/sign_app.sh" "$BUNDLE" >/dev/null

mkdir -p "$DIST"
STAGE="$(mktemp -d "$DIST/dmg-stage.XXXXXX")"
# EXIT 时删除暂存目录；hdiutil 失败也会经过 trap，避免留下临时 bundle 副本。
trap 'rm -rf -- "$STAGE"' EXIT
cp -R "$BUNDLE" "$STAGE/"
ln -s /Applications "$STAGE/Applications"

echo "==> hdiutil create"
hdiutil create -volname "$APP_NAME" -srcfolder "$STAGE" -format UDZO "$DMG" >/dev/null
echo "packaged: $DMG"
