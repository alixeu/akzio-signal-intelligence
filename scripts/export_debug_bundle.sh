#!/bin/sh
# 文件职责：调用 Rust 的只读 debug export，把已脱敏的 bundle 打成不可覆盖的归档。
# 本脚本只负责参数、临时目录、归档和退出码；业务完整性由 EXPORT_STATUS 决定。
# POSIX sh entrypoint for a read-only Akzio diagnostic bundle.
set -eu

usage() {
    # usage 同时承担帮助和参数错误出口；调用者传入 0 才表示显式请求帮助。
    # usage 同时承担帮助和参数错误出口，调用者传入 0 才表示显式请求帮助。
    echo "usage: $0 --config CONFIG --run-id RUN_ID --out OUTPUT_DIR [--store STORE_ROOT]" >&2
    exit "${1:-1}"
}

config=
run_id=
out=
store=

while [ "$#" -gt 0 ]; do
    # 参数解析只消费成对的 option/value；未知参数立即失败，不选择隐式最新 Run。
    case "$1" in
        --config)
            [ "$#" -ge 2 ] || usage
            config=$2
            shift 2
            ;;
        --run-id)
            [ "$#" -ge 2 ] || usage
            run_id=$2
            shift 2
            ;;
        --out)
            [ "$#" -ge 2 ] || usage
            out=$2
            shift 2
            ;;
        --store)
            [ "$#" -ge 2 ] || usage
            store=$2
            shift 2
            ;;
        --help|-h)
            usage 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage
            ;;
    esac
done

[ -n "$config" ] && [ -f "$config" ] || {
    echo "--config must name an existing file" >&2
    exit 1
}
[ -n "$run_id" ] || {
    echo "--run-id is required; this script never selects an implicit latest Run" >&2
    exit 1
}
[ -n "$out" ] || {
    echo "--out is required" >&2
    exit 1
}
case "$run_id" in
    *[!A-Za-z0-9._-]*)
        echo "--run-id contains unsupported path characters" >&2
        exit 1
        ;;
esac

command -v mktemp >/dev/null 2>&1 || { echo "missing dependency: mktemp" >&2; exit 1; }
command -v tar >/dev/null 2>&1 || { echo "missing dependency: tar" >&2; exit 1; }
command -v mkdir >/dev/null 2>&1 || { echo "missing dependency: mkdir" >&2; exit 1; }
command -v mv >/dev/null 2>&1 || { echo "missing dependency: mv" >&2; exit 1; }

umask 077
# 从脚本位置定位仓库，确保从任意当前目录调用时仍使用同一 Cargo manifest。
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)

if [ -e "$out" ] && [ -L "$out" ]; then
    echo "--out must not be a symlink" >&2
    exit 1
fi
mkdir -p -- "$out"

tmp_root=$(mktemp -d "${TMPDIR:-/tmp}/akzio-debug-export.XXXXXX")
cleanup() {
    # trap 覆盖正常退出和信号退出；临时目录只保存中间 bundle，不承担最终交付。
    if [ -n "${tmp_root:-}" ] && [ -d "$tmp_root" ]; then
        rm -rf -- "$tmp_root"
    fi
}
trap cleanup EXIT HUP INT TERM

bundle_tmp=$tmp_root/bundle
cli_output=$tmp_root/cli-output.json
binary=${AKZIO_BIN:-$repo_dir/target/debug/akzio}

if [ -x "$binary" ]; then
    # 已有可执行文件时直接复用；否则只允许离线 Cargo fallback，不能拉取依赖或启动服务。
    set -- "$binary" --config "$config" debug export-bundle "$run_id" --out "$bundle_tmp"
else
    command -v cargo >/dev/null 2>&1 || {
        echo "akzio binary not found and cargo is unavailable; set AKZIO_BIN" >&2
        exit 1
    }
    # Offline Cargo fallback only builds the already checked-out workspace; it
    # cannot fetch crates or start a daemon/model during export.
    set -- cargo run --manifest-path "$repo_dir/Cargo.toml" --locked --offline --quiet -p akzio-cli -- --config "$config" debug export-bundle "$run_id" --out "$bundle_tmp"
fi
if [ -n "$store" ]; then
    set -- "$@" --store "$store"
fi

if ! "$@" >"$cli_output"; then
    echo "Rust export command failed; no archive was created" >&2
    cat "$cli_output" >&2 || true
    exit 1
fi
[ -d "$bundle_tmp" ] || {
    echo "Rust export returned success but did not create the bundle directory" >&2
    exit 1
}
[ -f "$bundle_tmp/EXPORT_STATUS" ] || {
    echo "bundle is missing EXPORT_STATUS" >&2
    exit 1
}

timestamp=$(date -u '+%Y%m%dT%H%M%SZ')
name="akzio-debug-${run_id}-${timestamp}-$$"
final_dir=$out/$name
final_tar=$out/$name.tar.gz
if [ -e "$final_dir" ] || [ -e "$final_tar" ]; then
    echo "refusing to overwrite an existing output: $final_dir or $final_tar" >&2
    exit 1
fi
mv -- "$bundle_tmp" "$final_dir"
# 先搬到最终目录，再打 tar；tar 失败会删除不可用归档但保留 bundle 目录供诊断。
if ! tar -czf "$final_tar" -C "$out" "$name"; then
    echo "tar creation failed; no archive is usable" >&2
    rm -f -- "$final_tar"
    exit 1
fi

status=$(sed -n '1p' "$final_dir/EXPORT_STATUS" 2>/dev/null || echo partial)
printf 'bundle_dir=%s\n' "$final_dir"
printf 'bundle_tar=%s\n' "$final_tar"
printf 'status=%s\n' "$status"

if [ "$status" = partial ]; then
    # partial 是已生成但未完整导出的结果，用退出码 2 与真正失败区分。
    exit 2
fi
exit 0
