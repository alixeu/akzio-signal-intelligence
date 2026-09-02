#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

failed=0
while IFS= read -r markdown; do
  while IFS= read -r match; do
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
done < <(find . -type f -name '*.md' -not -path './target/*' -not -path './.git/*' | sort)

exit "${failed}"
