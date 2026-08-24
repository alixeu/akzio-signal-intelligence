#!/usr/bin/env bash
# Ad-hoc sign the assembled bundle. Self-use only: no Developer ID, no notarization.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE="${1:-$ROOT/dist/Akzio Observatory.app}"

if [ ! -d "$BUNDLE" ]; then
    echo "error: bundle not found: $BUNDLE (run build_app.sh first)" >&2
    exit 1
fi

echo "==> codesign (ad-hoc)"
codesign --force --sign - --options runtime "$BUNDLE/Contents/MacOS/akzio-core"
codesign --force --sign - --options runtime "$BUNDLE/Contents/MacOS/AkzioObservatory"
codesign --force --sign - --options runtime "$BUNDLE"
codesign --verify --deep --strict --verbose=2 "$BUNDLE"
echo "signed: $BUNDLE"
