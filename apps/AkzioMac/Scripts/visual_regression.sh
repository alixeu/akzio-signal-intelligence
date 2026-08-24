#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

manifest="Scripts/visual-baseline.sha256"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/akzio-visual.XXXXXX")"
trap 'rm -r "$tmp_dir"' EXIT

swift build -c release --product AkzioObservatory >/dev/null
binary="$(swift build -c release --show-bin-path)/AkzioObservatory"
failures=0

while read -r expected key; do
    mode="${key%%/*}"
    filename="${key#*/}"
    route="${filename#01-}"
    route="${route%.png}"
    size="1512x982"
    extra=()
    if [[ "$mode" == "compact" ]]; then
        size="1280x800"
        extra+=(--compact)
    fi
    if [[ "$route" == "settings" ]]; then
        route="overview"
        extra+=(--settings)
    fi

    output="$tmp_dir/$mode-$filename"
    "$binary" --capture --scenario 01 --route "$route" \
        --size "$size" --scale 2 ${extra[@]+"${extra[@]}"} --out "$output" >/dev/null
    actual="$(shasum -a 256 "$output" | cut -d' ' -f1)"
    if [[ "$actual" != "$expected" ]]; then
        printf 'changed: %s\n' "$key"
        failures=$((failures + 1))
    fi
done < "$manifest"

if (( failures > 0 )); then
    printf '%d visual baseline(s) changed\n' "$failures" >&2
    exit 1
fi

printf '18 visual baselines match\n'
