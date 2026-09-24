#!/usr/bin/env bash
# 文件职责：以 ad-hoc 身份依次签名 Rust Core、Swift App 和外层 bundle，再执行 deep verify。
# 这是本地自用检查，不提供 Developer ID、notarization 或生产发布证明。
# build_app_and_core.sh 与 create_dmg.sh 会把新建的 Bundle 路径传给本脚本；成功只表示本机 codesign 验证通过。
# codesign 会修改 Bundle 内的签名元数据；本脚本不编译、不创建 Bundle，也不启动 App。
# Ad-hoc sign the assembled bundle. Self-use only: no Developer ID, no notarization.
# 严格模式会在未被条件结构接住的 codesign/verify 失败时退出；此前已经写入的签名不会自动撤销。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 参数为空时使用默认 bundle；相对路径只在这里解析，后续 codesign 使用同一个目标。
BUNDLE="${1:-$ROOT/dist/akzio.app}"

if [ ! -d "$BUNDLE" ]; then
    # 签名不会创建或猜测 bundle，缺少组装产物时直接返回错误。
    echo "error: bundle not found: $BUNDLE (run build_app.sh first)" >&2
    exit 1
fi

echo "==> codesign (ad-hoc)"
# 先签名两个可执行文件，再签名外层 bundle，最后对整个嵌套结构做严格验证。
codesign --force --sign - --options runtime "$BUNDLE/Contents/MacOS/akzio-core"
codesign --force --sign - --options runtime "$BUNDLE/Contents/MacOS/AkzioObservatory"
codesign --force --sign - --options runtime "$BUNDLE"
codesign --verify --deep --strict --verbose=2 "$BUNDLE"
echo "signed: $BUNDLE"
