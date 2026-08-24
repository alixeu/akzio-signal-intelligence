#!/usr/bin/env bash
# Package the signed bundle into a self-use .dmg via hdiutil.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="Akzio Observatory"
VERSION="${VERSION:-1.0.0}"
DIST="$ROOT/dist"
BUNDLE="$DIST/$APP_NAME.app"
STAGE="$DIST/dmg-stage"
DMG="$DIST/AkzioObservatory-$VERSION.dmg"

if [ ! -d "$BUNDLE" ]; then
    echo "error: bundle not found: $BUNDLE (run build_app.sh first)" >&2
    exit 1
fi

# Seal and verify the exact bundle copied into the disk image.
"$ROOT/Scripts/sign_app.sh" "$BUNDLE" >/dev/null

rm -rf "$STAGE" "$DMG"
mkdir -p "$STAGE"
cp -R "$BUNDLE" "$STAGE/"
ln -s /Applications "$STAGE/Applications"

echo "==> hdiutil create"
hdiutil create -volname "$APP_NAME" -srcfolder "$STAGE" -ov -format UDZO "$DMG" >/dev/null
rm -rf "$STAGE"
echo "packaged: $DMG"
