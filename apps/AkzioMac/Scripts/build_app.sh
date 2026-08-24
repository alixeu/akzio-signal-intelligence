#!/usr/bin/env bash
# Assemble a double-clickable .app from the SwiftPM build product.
# No Xcode required: pure `swift build` + bundle layout.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
CONFIG="${CONFIG:-release}"
APP_NAME="Akzio Observatory"
VERSION="${VERSION:-1.0.0}"
BUILD_NUMBER="${BUILD_NUMBER:-1}"
DIST="$ROOT/dist"
BUNDLE="$DIST/$APP_NAME.app"

echo "==> swift build ($CONFIG)"
swift build --package-path "$ROOT" -c "$CONFIG" --product AkzioObservatory
BIN_PATH="$(swift build --package-path "$ROOT" -c "$CONFIG" --show-bin-path)"

echo "==> cargo build (release)"
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --release -p akzio-cli

echo "==> laying out bundle"
rm -rf "$BUNDLE"
mkdir -p "$BUNDLE/Contents/MacOS" "$BUNDLE/Contents/Resources"
cp "$BIN_PATH/AkzioObservatory" "$BUNDLE/Contents/MacOS/AkzioObservatory"
cp "$REPO_ROOT/target/release/akzio" "$BUNDLE/Contents/MacOS/akzio-core"
cp "$REPO_ROOT/config/akzio.observatory.toml" \
    "$BUNDLE/Contents/Resources/akzio.observatory.toml"
chmod 755 "$BUNDLE/Contents/MacOS/AkzioObservatory" "$BUNDLE/Contents/MacOS/akzio-core"

sed -e "s/__VERSION__/$VERSION/" -e "s/__BUILD__/$BUILD_NUMBER/" \
    "$ROOT/Resources/Info.plist.in" > "$BUNDLE/Contents/Info.plist"
printf 'APPL????' > "$BUNDLE/Contents/PkgInfo"

# Carry SwiftPM resource bundles and the app icon when present.
shopt -s nullglob
for resource_bundle in "$BIN_PATH"/*.bundle; do
    cp -R "$resource_bundle" "$BUNDLE/Contents/Resources/"
done
shopt -u nullglob
if [ -f "$ROOT/Resources/AppIcon.icns" ]; then
    cp "$ROOT/Resources/AppIcon.icns" "$BUNDLE/Contents/Resources/AppIcon.icns"
fi

echo "built: $BUNDLE"
