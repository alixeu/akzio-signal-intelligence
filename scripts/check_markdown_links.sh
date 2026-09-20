#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# 只有真实 Git 工作区能提供 tracked + untracked 的交付清单；归档目录不允许伪造空成功。
# An archive has no Git inventory. Report that limitation instead of letting a
# failed process substitution produce an empty file list and a false success.
git_root="$(git rev-parse --show-toplevel)"
if [[ "${git_root}" != "${repo_root}" ]]; then
  printf 'markdown check requires the repository Git inventory: %s\n' "${repo_root}" >&2
  exit 1
fi

failed=0
while IFS= read -r -d '' markdown; do
  # Unstaged deletions still appear in the index, but have no content to check.
  [[ -e "${markdown}" ]] || continue
  while IFS= read -r match; do
    # 外部、mailto 和 Codex 链接不映射到本地文件；其余相对路径按 Markdown 所在目录解析。
    target="${match#*](}"
    target="${target%)}"
    target="${target%%#*}"
    case "${target}" in
      ""|http://*|https://*|mailto:*|codex://*) continue ;;
    esac
    if [[ "${target}" = /* ]]; then
      printf 'absolute local markdown link is not reproducible: %s -> %s\n' \
        "${markdown}" "${target}" >&2
      failed=1
      continue
    fi
    candidate="$(dirname "${markdown}")/${target}"
    if [[ ! -e "${candidate}" ]]; then
      printf 'broken markdown link: %s -> %s\n' "${markdown}" "${target}" >&2
      failed=1
    fi
  done < <(grep -oE '\[[^][]+\]\([^)]+\)' "${markdown}" || true)
# Validate the deliverable tree, including new documentation before git add.
# Ignored Stores, build outputs and local evidence are not repository docs.
done < <(git ls-files --cached --others --exclude-standard -z -- '*.md')

exit "${failed}"
