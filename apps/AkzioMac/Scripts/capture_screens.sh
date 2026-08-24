#!/usr/bin/env bash
# Batch screenshot evidence for Akzio Observatory.
#
# Renders every page for the default scenario, plus the scenarios that exercise the
# rules most likely to regress (Critic Not Triggered, data Unavailable, Reduce
# Motion, No Order, blocked gates). Output lands in outputs/observatory-shots/.
#
# Usage:
#   Scripts/capture_screens.sh              # release binary, default matrix
#   SIZE=1280x800 Scripts/capture_screens.sh  # the narrow, compact-layout pass
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${CONFIG:-release}"
SIZE="${SIZE:-1512x982}"
SCALE="${SCALE:-2}"
OUT_DIR="${OUT_DIR:-../../outputs/observatory-shots}"

echo "building ($CONFIG)…"
swift build -c "$CONFIG" --product AkzioObservatory >/dev/null
BIN="$(swift build -c "$CONFIG" --show-bin-path)/AkzioObservatory"

mkdir -p "$OUT_DIR"

ROUTES=(overview workflow intelligence portfolio outcome learning runArchive scenarioGallery)

capture() {
  local scenario="$1" route="$2" suffix="${3:-}"
  local name="${scenario}-${route}${suffix}"
  "$BIN" --capture \
    --scenario "$scenario" \
    --route "$route" \
    --size "$SIZE" \
    --scale "$SCALE" \
    ${suffix:+--settings} \
    --out "$OUT_DIR/${name}.png" >/dev/null
  echo "  $name.png"
}

echo "default scenario — all eight pages:"
for route in "${ROUTES[@]}"; do
  capture 01 "$route"
done

echo "settings layer:"
"$BIN" --capture --scenario 01 --route overview --settings \
  --size "$SIZE" --scale "$SCALE" \
  --out "$OUT_DIR/01-settings.png" >/dev/null
echo "  01-settings.png"

echo "rule-bearing scenarios:"
# 03 critic not triggered · 06 no order · 08 blocked gate · 12 unsealed horizon
# 16 non-paper paper commit · 17 reduce motion · 18 data unavailable · 19 canary
declare -a MATRIX=(
  "03 workflow"
  "06 portfolio"
  "08 workflow"
  "12 outcome"
  "16 portfolio"
  "17 overview"
  "18 overview"
  "19 learning"
  "20 runArchive"
)
for pair in "${MATRIX[@]}"; do
  capture ${pair}
done

echo
echo "wrote $(ls -1 "$OUT_DIR"/*.png | wc -l | tr -d ' ') PNGs to $OUT_DIR"
